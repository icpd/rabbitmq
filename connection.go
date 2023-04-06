package rabbitmq

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrRabbitmqClosed = errors.New("rabbitmq closed")

var (
	defaultHeartbeat  = 5 * time.Second
	defaultAmqpConfig = amqp.Config{
		Heartbeat: defaultHeartbeat,
	}
)

// ExchangeType 交换机类型
type ExchangeType string

const (
	ExchangeTypeFanout ExchangeType = "fanout"
	ExchangeTypeTopic               = "topic"
	ExchangeTypeDirect              = "direct"
)

type rabbitmqConn struct {
	// rabbitmq section
	Connection      *amqp.Connection
	Channel         *rabbitmqChannel
	url             string
	withoutExchange bool // 不需要交换机

	// 内部状态维护 section
	sync.Mutex
	connected       bool
	close           chan struct{}
	waitConnection  chan struct{}
	connCloseNotify chan *amqp.Error
	chanCloseNotify chan *amqp.Error
}

// ExchangeOptions rabbitmq 交换机
type ExchangeOptions struct {
	// 交换机明
	Name string
	// 交换机类型
	Type ExchangeType
	// 是否持久化
	Durable bool
}

func newRabbitmqConn(url string) *rabbitmqConn {
	ret := &rabbitmqConn{
		url:            url,
		waitConnection: make(chan struct{}),
		close:          make(chan struct{}),
	}
	close(ret.waitConnection)

	return ret
}

func (r *rabbitmqConn) Connect(config *amqp.Config) error {
	r.Lock()
	if r.connected {
		r.Unlock()
		return nil
	}

	r.Unlock()

	if config == nil {
		config = &defaultAmqpConfig
	}
	return r.connect(config)
}

func (r *rabbitmqConn) connect(config *amqp.Config) error {
	if err := r.tryConnect(config); err != nil {
		return err
	}

	r.Lock()
	r.connected = true
	r.Unlock()

	// 创建重连协程
	go r.reconnect(config)

	return nil
}

func (r *rabbitmqConn) tryConnect(config *amqp.Config) (err error) {
	if r.Connection == nil || r.Connection.IsClosed() {
		r.Connection, err = amqp.DialConfig(r.url, *config)
		if err != nil {
			return
		}

		r.connCloseNotify = make(chan *amqp.Error, 1)
		r.Connection.NotifyClose(r.connCloseNotify)
	}

	r.Channel, err = newRabbitChannel(r.Connection)
	if err != nil {
		return
	}

	r.chanCloseNotify = make(chan *amqp.Error, 1)
	r.Channel.channel.NotifyClose(r.chanCloseNotify)
	return
}

func (r *rabbitmqConn) reconnect(config *amqp.Config) {
	// recover panic
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Panic: rabbitmq reconnect: %v", err)
		}
	}()

	for {
		if !r.connected { // 第一次 connected 为 true, 不需要重连
			b := NewForeverBackoff()
			for {
				err := r.tryConnect(config)
				if err == nil {
					break
				}

				// 等待重试
				log.Printf("Error: rabbitmq reconnect: %v", err)
				b.Wait()
				continue
			}

			r.Lock()
			r.connected = true
			r.Unlock()

			// 通知等待的协程链接创建好了
			close(r.waitConnection)
			log.Println("Info: rabbitmq reconnect success")
		}

		// 监听关闭事件
		select {
		case <-r.close:
			return

		case err := <-r.connCloseNotify: // 连接关闭通知
			log.Printf("Warning: connection notify close: %v", err)
			r.Lock()
			r.connected = false
			r.waitConnection = make(chan struct{})
			r.Unlock()
			r.connCloseNotify = nil

		case err := <-r.chanCloseNotify: // channel关闭通知
			log.Printf("Warning: channel notify close: %v", err)
			r.Lock()
			r.connected = false
			r.waitConnection = make(chan struct{})
			r.Unlock()
			r.chanCloseNotify = nil
		}
	}
}

func (r *rabbitmqConn) Close() error {
	r.Lock()
	defer r.Unlock()

	select {
	case <-r.close: // 已经关闭了直接返回
		return nil
	default:
		close(r.close)
		r.connected = false
	}

	return r.Connection.Close()
}

func (r *rabbitmqConn) Consume(opts Options) (deliveries <-chan amqp.Delivery, err error) {
	// 创建队列
	if opts.DurableQueue {
		err = r.Channel.DeclareDurableQueue(opts.Queue, nil)
	} else {
		err = r.Channel.DeclareQueue(opts.Queue, nil)
	}
	if err != nil {
		return nil, err
	}

	// 绑定消费者
	deliveries, err = r.Channel.ConsumeQueue(opts.Queue, opts.AutoAck)
	if err != nil {
		return nil, err
	}

	if !opts.WithoutExchange {
		// 创建交换机
		if err = r.Channel.DeclareExchange(opts.Exchange); err != nil {
			return nil, err
		}

		// 队列与交换机绑定
		err = r.Channel.BindQueue(opts.Queue, opts.Key, opts.Exchange.Name, nil)
		if err != nil {
			return nil, err
		}
	}

	return deliveries, nil
}

func (r *rabbitmqConn) Publish(ctx context.Context, msg amqp.Publishing, opts Options) error {
	select {
	case <-r.close:
		return ErrRabbitmqClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-r.waitConnection:
	}

	if r.withoutExchange {
		return r.Channel.Publish(ctx, "", opts.Key, msg)
	}

	return r.Channel.Publish(ctx, opts.Exchange.Name, opts.Key, msg)
}
