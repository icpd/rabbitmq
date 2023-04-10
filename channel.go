package rabbitmq

import (
	"context"
	"errors"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitmqChannel struct {
	connection *amqp.Connection
	rawChannel *amqp.Channel
}

func newRabbitChannel(conn *amqp.Connection) (*rabbitmqChannel, error) {
	rabbitCh := &rabbitmqChannel{
		connection: conn,
	}

	if err := rabbitCh.Connect(); err != nil {
		return nil, err
	}

	return rabbitCh, nil
}

func (r *rabbitmqChannel) Connect() (err error) {
	r.rawChannel, err = r.connection.Channel()
	return
}

func (r *rabbitmqChannel) DeclareExchange(ex ExchangeOptions) error {
	return r.rawChannel.ExchangeDeclare(
		ex.Name,         // name
		string(ex.Type), // kind
		ex.Durable,      // durable
		false,           // autoDelete
		false,           // internal
		false,           // noWait
		nil,             // args
	)
}

// DeclareDurableQueue  持久化队列
// rabbitmq服务重启后，队列数据不会丢失；消费者连接时，队列也不会被删除
func (r *rabbitmqChannel) DeclareDurableQueue(queue string, args amqp.Table) (err error) {
	_, err = r.rawChannel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		args,  // args
	)

	return
}

// DeclareQueue 非持久化队列
// rabbitmq服务重启后，队列数据会丢失；消费者连接时，队列会自动删除
func (r *rabbitmqChannel) DeclareQueue(queue string, args amqp.Table) (err error) {
	_, err = r.rawChannel.QueueDeclare(
		queue, // name
		false, // durable
		true,  // autoDelete
		false, // exclusive
		false, // noWait
		args,  // args
	)

	return
}

func (r *rabbitmqChannel) ConsumeQueue(queue string, autoAck bool) (<-chan amqp.Delivery, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	return r.rawChannel.Consume(
		queue,       // queue
		id.String(), // consumer
		autoAck,     // autoAck
		false,       // exclusive
		false,       // noLocal
		false,       // nowait
		nil,         // args
	)
}

func (r *rabbitmqChannel) BindQueue(queue, key, exchange string, args amqp.Table) error {
	return r.rawChannel.QueueBind(
		queue,    // name
		key,      // key
		exchange, // exchange
		false,    // noWait
		args,     // args
	)
}

func (r *rabbitmqChannel) Publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	if r.rawChannel == nil {
		return errors.New("rawChannel is nil")
	}

	err := r.rawChannel.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		return err
	}

	return nil
}
