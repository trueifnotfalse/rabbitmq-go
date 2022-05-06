package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"time"
)

const defaultReconnectTimeOut = 5

type Config struct {
	Host             string
	Port             string
	User             string
	Password         string
	ReConnect        bool
	ReconnectTimeOut time.Duration
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type ConsumeConfig struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type Connector struct {
	logger           *log.Logger
	connection       *amqp.Connection
	dsn              string
	reConnect        bool
	reconnectTimeOut time.Duration
}

type MessageHandler func(<-chan amqp.Delivery)

func NewConnector(logger *log.Logger, c *Config) *Connector {
	dsn := fmt.Sprintf("amqp://%s:%s@%s:%s/", c.User, c.Password, c.Host, c.Port)
	reconnectTimeout := c.ReconnectTimeOut
	if 0 >= reconnectTimeout {
		reconnectTimeout = defaultReconnectTimeOut * time.Second
	}

	connector := Connector{
		logger:           logger,
		dsn:              dsn,
		reConnect:        c.ReConnect,
		reconnectTimeOut: reconnectTimeout,
	}

	return &connector
}

func (c *Connector) GetChannel() (*amqp.Channel, error) {
	c.Connect()
	connection := c.connection
	if nil == connection {
		return nil, fmt.Errorf("RabbitMQ: connection closed")
	}
	channel, err := connection.Channel()
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
		time.Sleep(c.reconnectTimeOut)
	}
}

func (c *Connector) isConnected() bool {
	return !(nil == c.connection || c.connection.IsClosed())
}

func (c *Connector) QueueDeclare(qc *QueueConfig) error {
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
	var err error
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

func (c Connector) Qos(prefetchCount int, prefetchSize int, global bool) error {
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}

	return channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *Connector) Consume(cc *ConsumeConfig, handler MessageHandler) error {
	delivery, err := c.consume(cc)
	if nil != err {
		return err
	}
	handler(delivery)

	return nil
}

func (c *Connector) consume(cc *ConsumeConfig) (<-chan amqp.Delivery, error) {
	channel, err := c.GetChannel()
	if nil != err {
		return nil, err
	}

	delivery, err := channel.Consume(
		cc.Queue,
		cc.Consumer,
		cc.AutoAck,
		cc.Exclusive,
		cc.NoLocal,
		cc.NoWait,
		cc.Args,
	)

	if nil != err {
		return nil, err
	}

	return delivery, nil
}

func NewManualAckConsumeConfig(name string, consumer string) *ConsumeConfig {
	return &ConsumeConfig{
		Queue:     name,
		Consumer:  consumer,
		AutoAck:   false,
		NoLocal:   false,
		Exclusive: false,
		NoWait:    false,
		Args:      nil,
	}
}

func NewAutoAckConsumeConfig(name string, consumer string) *ConsumeConfig {
	return &ConsumeConfig{
		Queue:     name,
		Consumer:  consumer,
		AutoAck:   false,
		NoLocal:   false,
		Exclusive: false,
		NoWait:    false,
		Args:      nil,
	}
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
