# rabbitmq-go
Golang RabbitMQ high level library

```go
package main

import (
	"github.com/labstack/gommon/log"
	"github.com/trueifnotfalse/rabbitmq-go"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	logger := log.New("-")
	config := rabbitmq.Config{
		Host:     "127.0.0.1",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
	}
	con := rabbitmq.NewConnector(logger, &config, true)
	con.Connect()
	qc := rabbitmq.NewDurableQueueConfig("hello")
	err := con.QueueDeclare(qc)
	failOnError(err, "Failed to declare a queue")

	message := struct {
		CreatedAt string `json:"created_at"`
		Text      string `json:"text"`
	}{
		CreatedAt: time.Now().Format("2006-01-02 15:04:05"),
		Text:      "Hello World!",
	}

	err = con.PublishStructToQueue("hello", message)
	failOnError(err, "Failed to publish a message")
}
```
