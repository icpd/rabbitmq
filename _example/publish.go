package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.test.com/common/rabbitmq"
)

func main() {
	// 初始化rabbitmq对象
	config := amqp.Config{
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("publisher") // 给连接设置个名字，方便在rabbitmq管理后台查看
	r := rabbitmq.NewRabbitmq(
		"amqp://admin:admin@192.168.2.239:5672",
		&config, // 创建 rabbitmq 连接的一些配置项，没有什么特殊要求直接传nil也可以
	)

	// 连接rabbitmq，必须调用。内部会起一个协程维护连接，如果连接断开会自动重连
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}

	// 创建交换机
	exchange1 := rabbitmq.ExchangeOptions{
		Name:    "rabbitmq_test_exchange1",   // 交换机名
		Type:    rabbitmq.ExchangeTypeFanout, // 交换机类型
		Durable: true,                        // 是否持久化
	}
	if err := r.Exchange(exchange1); err != nil {
		log.Fatal(err)
	}

	exchange2 := rabbitmq.ExchangeOptions{
		Name: "rabbitmq_test_exchange2",   // 交换机名
		Type: rabbitmq.ExchangeTypeFanout, // 交换机类型
	}
	if err := r.Exchange(exchange2); err != nil {
		log.Fatal(err)
	}

	// 发送消息前，***请确保你已经创建好了交换机**
	for i := 0; i < 10; i++ {
		var ex rabbitmq.ExchangeOptions
		if i%2 == 0 {
			ex = exchange1
		} else {
			ex = exchange2
		}

		err := r.Publish(context.Background(),
			[]byte(fmt.Sprintf("hello world %d", i)),
			rabbitmq.Exchange(ex),
		)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("send %d to %s\n", i, ex.Name)

		time.Sleep(time.Second)
	}

	select {}
}
