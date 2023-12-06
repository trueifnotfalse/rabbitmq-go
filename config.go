package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
)

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

type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type QueueBindConfig struct {
	QueueName    string
	ExchangeName string
	RoutingKey   string
	NoWait       bool
	Args         amqp.Table
}

type ConsumeConfig struct {
	Queue         string
	Consumer      string
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          amqp.Table
	PrefetchCount int
}

func NewManualAckConsumeConfig(name string, consumer string) *ConsumeConfig {
	return &ConsumeConfig{
		Queue:         name,
		Consumer:      consumer,
		AutoAck:       false,
		NoLocal:       false,
		Exclusive:     false,
		NoWait:        false,
		Args:          nil,
		PrefetchCount: 0,
	}
}

func NewAutoAckConsumeConfig(name string, consumer string) *ConsumeConfig {
	return &ConsumeConfig{
		Queue:         name,
		Consumer:      consumer,
		AutoAck:       true,
		NoLocal:       false,
		Exclusive:     false,
		NoWait:        false,
		Args:          nil,
		PrefetchCount: 0,
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

func NewDurableExchangeConfig(name, typeName string) *ExchangeConfig {
	return &ExchangeConfig{
		Name:       name,
		Type:       typeName,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

func NewTransientExchangeConfig(name, typeName string) *ExchangeConfig {
	return &ExchangeConfig{
		Name:       name,
		Type:       typeName,
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

func NewQueueBindConfig(queueName, exchangeName string) *QueueBindConfig {
	return &QueueBindConfig{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   "",
		NoWait:       false,
		Args:         nil,
	}
}
