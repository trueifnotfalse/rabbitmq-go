package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"time"
)

const reconnectTimeOut = 5

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type Connector struct {
	logger     *log.Logger
	connection *amqp.Connection
	dsn        string
	reConnect  bool
}

func NewConnector(logger *log.Logger, c *Config, reConnect bool) *Connector {
	dsn := fmt.Sprintf("amqp://%s:%s@%s:%s/", c.User, c.Password, c.Host, c.Port)
	connector := Connector{
		logger:    logger,
		dsn:       dsn,
		reConnect: reConnect,
	}

	return &connector
}

func (c *Connector) GetChannel() (*amqp.Channel, error) {
	channel, err := c.connection.Channel()
	if nil != err {
		return nil, fmt.Errorf("RabbitMQ: channel opening error: %v", err)
	}

	return channel, nil
}

func (c *Connector) connect() error {
	var err error
	c.logger.Info("RabbitMQ: opening connection")
	c.connection, err = amqp.Dial(c.dsn)
	if nil != err {
		return fmt.Errorf("RabbitMQ: connection setup error : %v", err)
	}

	return nil
}

func (c *Connector) Connect() *Connector {
	if c.isConnected() {
		return c
	}

	var err error
	err = c.connect()
	if nil == err || !c.reConnect {
		return c
	}
	c.logger.Error(err.Error())

	for {
		err = c.connect()
		if nil == err {
			return c
		}
		c.logger.Error(err.Error())
		c.logger.Info("RabbitMQ: reconnect...")
		time.Sleep(reconnectTimeOut * time.Second)
	}
}

func (c *Connector) isConnected() bool {
	return !(nil == c.connection || c.connection.IsClosed())
}

func (c *Connector) QueueDeclare(qc *QueueConfig) error {
	c.Connect()
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()
	_, err = channel.QueueDeclare(
		qc.Name,
		qc.Durable,
		qc.AutoDelete,
		qc.Exclusive,
		qc.NoWait,
		qc.Args,
	)

	return err
}

func (c *Connector) Close() error {
	var err error
	if nil != c {
		if nil != c.connection {
			return c.connection.Close()
		}
	}

	return err
}

func (c *Connector) PublishStructToQueue(name string, obj interface{}) error {
	var msg []byte
	var err  error
	objWithMarshalJSON, ok := obj.(json.Marshaler)
	if ok {
		msg, err = objWithMarshalJSON.MarshalJSON()
	} else {
		msg, err = json.Marshal(obj)
	}

	if nil != err {
		return err
	}
	return c.PublishToQueue(name, msg)
}

func (c *Connector) PublishToQueue(name string, body []byte) error {
	c.Connect()
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()
	return channel.Publish(
		"",
		name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
}

func NewDurableQueueConfig(name string) *QueueConfig {
	return &QueueConfig{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

func NewTransientQueueConfig(name string) *QueueConfig {
	return &QueueConfig{
		Name:       name,
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}
