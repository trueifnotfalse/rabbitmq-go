package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type Publisher struct {
	connector    *Connector
	exchangeName string
	routingKey   string
	deliveryMode uint8
	contentType  string
	body         []byte
	obj          interface{}
	expiration   string
}

func newPublisher(connector *Connector) *Publisher {
	return &Publisher{
		connector:    connector,
		deliveryMode: amqp.Persistent,
		contentType:  "text/plain",
	}
}

func (p *Publisher) ToExchange(name string) *Publisher {
	p.exchangeName = name

	return p
}

func (p *Publisher) ToQueue(name string) *Publisher {
	p.routingKey = name

	return p
}

func (p *Publisher) AsPersistent() *Publisher {
	p.deliveryMode = amqp.Persistent

	return p
}

func (p *Publisher) AsTransient() *Publisher {
	p.deliveryMode = amqp.Transient

	return p
}

func (p *Publisher) WithExpiration(expiration string) *Publisher {
	p.expiration = expiration

	return p
}

func (p *Publisher) WithContentType(contentType string) *Publisher {
	p.contentType = contentType

	return p
}

func (p *Publisher) WithData(data []byte) *Publisher {
	p.body = data

	return p
}

func (p *Publisher) WithStruct(obj interface{}) *Publisher {
	p.obj = obj

	return p
}

func (p *Publisher) Do() error {
	channel, err := p.connector.GetChannel()
	if nil != err {
		return err
	}
	defer channel.Close()
	body, err := p.getBody()
	if nil != err {
		return err
	}

	return channel.Publish(
		p.exchangeName,
		p.routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  p.contentType,
			Body:         body,
			DeliveryMode: p.deliveryMode,
			Expiration:   p.expiration,
		})
}

func (p *Publisher) getBody() ([]byte, error) {
	if nil != p.obj {
		return p.unmarshalStruct()
	}

	return p.body, nil
}

func (p *Publisher) unmarshalStruct() ([]byte, error) {
	objWithMarshalJSON, ok := p.obj.(json.Marshaler)
	if ok {
		return objWithMarshalJSON.MarshalJSON()
	}

	return json.Marshal(p.obj)
}
