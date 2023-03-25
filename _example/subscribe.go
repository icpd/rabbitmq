package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.test.com/common/rabbitmq"
)

func main() {
	// 初始化rabbitmq对象
	config := amqp.Config{
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("subscriber") // 给连接设置个名字，方便在rabbitmq管理后台查看
	r := rabbitmq.NewRabbitmq(
		"amqp://admin:admin@192.168.2.239:5672",
		&config, // 创建 rabbitmq 连接的一些配置项，没有什么特殊要求直接传nil也可以
	)

	// 连接rabbitmq，必须调用。内部会起一个协程维护连接，如果连接断开会自动重连
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}

	// 交换机配置
	exchange1 := rabbitmq.ExchangeOptions{
		Name:    "rabbitmq_test_exchange1",   // 交换机名
		Type:    rabbitmq.ExchangeTypeFanout, // 交换机类型
		Durable: true,                        // 是否持久化
	}

	// 创建订阅并消费，内部会起一个协程消费数据
	err := r.Subscribe(
		func(msg []byte) error {
			log.Println("receive1:", string(msg))
			return nil
		},
		rabbitmq.Queue("rabbitmq_test_queue1"), // 设置队列名，如果不设置队列，rabbitmq服务器会自动生成一个唯一的队列名
		rabbitmq.Exchange(exchange1),           // 设置交换机配置
		rabbitmq.DurableQueue(),                // 设置队列为持久化队列，默认为非持久化队列
		rabbitmq.DisableAutoAck(),              // 禁用自动ack。默认是开启了自动ack
	)
	if err != nil {
		log.Fatal(err)
	}

	// 也可以创建多个订阅消费
	exchange2 := rabbitmq.ExchangeOptions{
		Name: "rabbitmq_test_exchange2",   // 交换机名
		Type: rabbitmq.ExchangeTypeFanout, // 交换机类型
	}
	err = r.Subscribe(func(msg []byte) error {
		log.Println("receive2:", string(msg))
		return nil
	},
		rabbitmq.Queue("rabbitmq_test_queue2"), // 设置队列名，如果不设置队列，rabbitmq服务器会自动生成一个唯一的队列名
		rabbitmq.Exchange(exchange2),           // 设置交换机配置
	)
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
