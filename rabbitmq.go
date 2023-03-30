package rabbitmq

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// DefaultDeliveryCloseDelay 默认 delivery 关闭后重建 consume 延迟时间
	DefaultDeliveryCloseDelay = 100 * time.Millisecond

	// DefaultPublishMaxTries 默认发送失败最大重试次数
	DefaultPublishMaxTries uint64 = 3
	// DefaultPublishRetryDelay 默认发送失败延迟时间
	DefaultPublishRetryDelay = 300 * time.Millisecond
)

type Rabbitmq struct {
	conn   *rabbitmqConn
	url    string
	config *amqp.Config

	lock sync.Mutex

	// delivery 关闭后重建 consume 延迟时间
	DeliveryCloseDelay time.Duration

	// 发送失败重试最大重试次数
	PublishMaxTries uint64
	// 发送失败重试延迟时间
	PublishRetryDelay time.Duration
}

type Handler func([]byte) error

func (r *Rabbitmq) Connect() error {
	config := defaultAmqpConfig
	if r.config != nil {
		config = *r.config
	}

	r.conn = newRabbitmqConn(r.url)
	return r.conn.Connect(&config)
}

func (r *Rabbitmq) Close() error {
	if r.conn == nil {
		return errors.New("not connected")
	}

	return r.conn.Close()
}

// Exchange 创建交换机
func (r *Rabbitmq) Exchange(opts ExchangeOptions) error {
	return r.conn.Channel.DeclareExchange(opts)
}

// Publish 发布消息，如果发送失败会自动重试
//
// 注意：发消息前一定确保交换机成功创建。交换机不存在 amqp 并不会返回错误，而是直接关闭 rabbitmq channel
func (r *Rabbitmq) Publish(ctx context.Context, msg []byte, options ...Option) error {
	opts := newOptions(options...)

	var b backoff.BackOff
	b = backoff.NewConstantBackOff(r.PublishRetryDelay)
	b = backoff.WithMaxRetries(b, r.PublishMaxTries)

	err := backoff.Retry(func() error {
		return r.conn.Publish(ctx, amqp.Publishing{
			Body: msg,
		}, opts)
	}, b)

	return err
}

// Subscribe 订阅消息
func (r *Rabbitmq) Subscribe(handler Handler, options ...Option) error {
	if r.conn == nil {
		return errors.New("not connected")
	}

	// 初始订阅相关配置。默认开启自动ack
	opts := newOptions(options...)

	fn := func(msg amqp.Delivery) {
		err := handler(msg.Body)
		if err != nil {
			log.Printf("Waring: rabbitmq subscribe handle: %v", err)
		}

		// 自动ack直接返回
		if opts.AutoAck {
			return
		}

		// 消费成功ack
		if err == nil {
			if err = msg.Ack(false); err != nil {
				log.Printf("Warning: rabbitmq ack msg: %v", err)
			}
			return
		}

		// 消费失败，根据配置决定是否重入队列
		if err = msg.Nack(false, opts.Requeue); err != nil {
			log.Printf("Warning: rabbitmq nack msg: %v", err)
		}
	}

	go r.subscribe(opts, fn)

	return nil
}

func (r *Rabbitmq) subscribe(opts Options, fn func(amqp.Delivery)) {
	// recover panic
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Panic: rabbitmq subscribe: %v", err)
		}
	}()

	b := NewForeverBackoff()
	for {
		select {
		case <-r.conn.close: // 我们主动关闭，退出程序
			return
		case <-time.After(time.Second): // 收到断连通知后 r.conn.waitConnection 会被重新赋值，这里是为了防止出现死锁
			continue
		case <-r.conn.waitConnection: // 如果在正在重连，等待重连成功
		}

		r.lock.Lock()
		if !r.conn.connected {
			r.lock.Unlock()
			continue
		}

		deliveries, err := r.conn.Consume(opts)
		r.lock.Unlock()
		if err != nil {
			log.Printf("Error: rabbitmq subscribe consume: %v", err)
			b.Wait()
			continue
		}

		// 重置退避时间
		b.Reset()

		log.Printf("Info: rabbitmq start consume queue: %s", opts.Queue)

		// 当 rabbitmq conn 和 channel 关闭时 deliveries 都会被关闭，
		// 所以这里会退出循环，等待重连后重新订阅。
		// 除以上两个情况外，就是队列被删除了，deliveries 也会被关闭，但是并不会收任何关闭通知消息。
		for d := range deliveries {
			fn(d)
		}

		// 观察日志发现在这里触发的事件比 chanNotifyClose 和 connNotifyClose 更快
		// 导致在上面的 select 语句中，r.conn.waitConnection 的状态仍然是关闭状态，连接还是旧的，因此 consume 报错 continue 后正常。
		// 所以这里休眠一会儿，减少没必要的错误
		log.Printf("Info: rabbitmq deliverie channel closed, sleep %s and wait reconnect", r.DeliveryCloseDelay)
		time.Sleep(r.DeliveryCloseDelay)
	}
}

func NewRabbitmq(url string, config *amqp.Config) *Rabbitmq {
	return &Rabbitmq{
		url:    url,
		config: config,

		DeliveryCloseDelay: DefaultDeliveryCloseDelay,
		PublishMaxTries:    DefaultPublishMaxTries,
		PublishRetryDelay:  DefaultPublishRetryDelay,
	}
}
