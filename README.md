# rabbitmq

Encapsulation for `github.com/rabbitmq/amqp091-go`.

## Purpose

- Add automatic reconnection handling
- Retry sending messages when failed
- Shield the usage details of `github.com/rabbitmq/amqp091-go` and reduce users' cognitive load.

## Usage


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
	// 1. Initialize a rabbitmq object
	r := rabbitmq.NewRabbitmq(
		"amqp://admin:admin@192.168.2.239:5672",
		nil,
	)

	// 2. Create a connection to the rabbitmq service
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}

	// 3. Declare an exchange with the rabbitmq service
	exchange := rabbitmq.ExchangeOptions{
		Name:    "example_exchange",          // Exchange name
		Type:    rabbitmq.ExchangeTypeFanout, // Exchange type
		Durable: true,                        // Whether it is durable
	}
	if err := r.Exchange(exchange); err != nil {
		log.Fatal(err)
	}

	// 4. Send a message to the exchange, not blocking
	// Notice: Please ensure that the exchange has been created before publishing a message
	err := r.Publish(
		context.Background(),
		[]byte(fmt.Sprintf("hello %d", i)), // Message body to be sent 
		rabbitmq.Exchange(exchange),        // Set the message exchange target
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
	// 1. Initialize a rabbitmq object
	r := rabbitmq.NewRabbitmq(
		"amqp://admin:admin@192.168.2.239:5672",
		nil,
	)

	// 2. Create a connection to the rabbitmq service
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}

	// 3. Exchange configuration
	exchange := rabbitmq.ExchangeOptions{
		Name:    "example_exchange",          // Exchange name
		Type:    rabbitmq.ExchangeTypeFanout, // Exchange type
		Durable: true,                        // Whether it is durable
	}

	// 4. Create a subscription
	// Subscribing and consuming internally starts a goroutine that consumes data so it won't block the main goroutine.
	err := r.Subscribe(func(msg []byte) error {
		log.Println("receive:", string(msg))
		return nil
	},
		rabbitmq.Queue("example_queue"), // Set the name of the consumption queue
		rabbitmq.Exchange(exchange),     // Set the exchange that the queue needs to bind to
	)
	if err != nil {
		log.Fatal(err)
	}

	select {}
}

```

For more usage examples, please refer to the `_example` directory.
