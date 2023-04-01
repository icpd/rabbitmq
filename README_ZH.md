# rabbitmq

针对`github.com/rabbitmq/amqp091-go`使用封装。  

## 目的

- **添加自动重连处理**
- 发送消息失败主动重试
- 屏蔽`github.com/rabbitmq/amqp091-go`的使用细节，减小使用者的心智负担

## 使用


### Publish
```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/icpd/rabbitmq"
)

func main() {
	// 1.初始化rabbitmq对象
	r := rabbitmq.NewRabbitmq(
		"amqp://admin:admin@192.168.2.239:5672",
		nil,
	)

	// 2.创建与 rabbitmq 服务的连接
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}

	// 3.向 rabbitmq 服务申名一个交换机
	exchange := rabbitmq.ExchangeOptions{
		Name:    "example_exchange",          // 交换机名
		Type:    rabbitmq.ExchangeTypeFanout, // 交换机类型
		Durable: true,                        // 是否持久化
	}
	if err := r.Exchange(exchange); err != nil {
		log.Fatal(err)
	}

	// 4.向交换机发送消息，同步不阻塞
	// notice: 发布消息前，请确保交换机已经创建
	err := r.Publish(
		context.Background(),
		[]byte(fmt.Sprintf("hello %d", i)), // 要发送的消息体 
		rabbitmq.Exchange(exchange),        // 设置消息发目标交换机
	)
	if err != nil {
		log.Fatal(err)
	}

	select {}
}

```

### Subscribe

```go
package main

import (
	"log"

	"github.com/icpd/rabbitmq"
)

func main() {
	// 1.初始化rabbitmq对象
	r := rabbitmq.NewRabbitmq(
		"amqp://admin:admin@192.168.2.239:5672",
		nil,
	)

	// 2.创建与 rabbitmq 服务的连接
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}

	// 3.交换机配置
	exchange := rabbitmq.ExchangeOptions{
		Name:    "example_exchange",          // 交换机名
		Type:    rabbitmq.ExchangeTypeFanout, // 交换机类型
		Durable: true,                        // 是否持久化
	}

	// 4.创建订阅
	// 订阅并消费内部会起一个协程消费数据所以不会阻塞主协程
	err := r.Subscribe(func(msg []byte) error {
		log.Println("receive:", string(msg))
		return nil
	},
		rabbitmq.Queue("example_queue"), // 设置消费队列名
		rabbitmq.Exchange(exchange),     // 设置队列需要绑定的交换机
	)
	if err != nil {
		log.Fatal(err)
	}

	select {}
}

```

更多使用案例请查看`_example`目录。
