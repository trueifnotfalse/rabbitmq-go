package rabbitmq

import (
	"fmt"
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

const defaultReconnectTimeOut = 5

type Connector struct {
	sync.Mutex
	logger           *log.Logger
	connection       *amqp.Connection
	dsn              string
	reConnect        bool
	reconnectTimeOut time.Duration
}

type MessageHandler func(<-chan amqp.Delivery)

func NewConnector(logger *log.Logger, config *Config) *Connector {
	dsn := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", config.User, config.Password, config.Host, config.Port, config.Vhost)
	reconnectTimeout := config.ReconnectTimeOut
	if 0 >= reconnectTimeout {
		reconnectTimeout = defaultReconnectTimeOut * time.Second
	}

	connector := Connector{
		logger:           logger,
		dsn:              dsn,
		reConnect:        config.ReConnect,
		reconnectTimeOut: reconnectTimeout,
	}

	return &connector
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
	defer c.Unlock()
	c.Lock()
	if c.isConnected() {
		return c
	}

	var err error
	err = c.connect()
	if nil == err || !c.reConnect {
		return c
	}
	c.logger.Error(err)

	for {
		err = c.connect()
		if nil == err {
			return c
		}
		c.logger.Error(err)
		c.logger.Info("RabbitMQ: reconnect...")
		time.Sleep(c.reconnectTimeOut)
	}
}

func (c *Connector) GetChannel() (*amqp.Channel, error) {
	c.Connect()
	if nil == c.connection {
		return nil, fmt.Errorf("RabbitMQ: connection closed")
	}

	channel, err := c.connection.Channel()
	if nil != err {
		return nil, fmt.Errorf("RabbitMQ: channel opening error: %v", err)
	}

	return channel, nil
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

func (c *Connector) QueueDelete(qc *QueueConfig) error {
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()
	_, err = channel.QueueDelete(
		qc.Name,
		false,
		false,
		qc.NoWait,
	)

	return err
}

func (c *Connector) QueueBind(qbc *QueueBindConfig) error {
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()

	return channel.QueueBind(
		qbc.QueueName,
		qbc.RoutingKey,
		qbc.ExchangeName,
		qbc.NoWait,
		qbc.Args,
	)
}

func (c *Connector) QueueUnBind(qbc *QueueBindConfig) error {
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()

	return channel.QueueUnbind(
		qbc.QueueName,
		qbc.RoutingKey,
		qbc.ExchangeName,
		qbc.Args,
	)
}

func (c *Connector) ExchangeDeclare(ec *ExchangeConfig) error {
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()

	return channel.ExchangeDeclare(
		ec.Name,
		ec.Type,
		ec.Durable,
		ec.AutoDelete,
		ec.Internal,
		ec.NoWait,
		ec.Args,
	)
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

func (c *Connector) Consume(consumeConfig *ConsumeConfig, handler MessageHandler) error {
	channel, err := c.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()

	err = channel.Qos(consumeConfig.PrefetchCount, 0, false)
	if nil != err {
		return err
	}

	delivery, err := channel.Consume(
		consumeConfig.Queue,
		consumeConfig.Consumer,
		consumeConfig.AutoAck,
		consumeConfig.Exclusive,
		consumeConfig.NoLocal,
		consumeConfig.NoWait,
		consumeConfig.Args,
	)

	if nil != err {
		return err
	}

	handler(delivery)

	return nil
}

func (c *Connector) Publish() *Publisher {
	return newPublisher(c)
}
