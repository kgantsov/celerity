package broker

import (
	"context"

	"github.com/kgantsov/celerity/internal/task"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQDelivery struct {
	delivery amqp.Delivery
}

func (d *RabbitMQDelivery) Ack(multiple bool) error {
	return d.delivery.Ack(multiple)
}

func (d *RabbitMQDelivery) Nack(multiple bool) error {
	return d.delivery.Nack(multiple, true)
}

type RawMessage struct {
	Queue         string
	Headers       map[string]any
	Body          []byte
	ContentType   string
	ReplyTo       string
	CorrelationID string
	Delivery      task.Deliverable
}

type BrokerConfig struct {
	URL           string
	Queues        []string
	PrefetchCount int
}

// Publisher is the minimal interface for sending tasks to a broker.
// Implementations only need a connection — no consumer goroutines required.
type Publisher interface {
	Connect() error
	PublishMessage(msg *RawMessage) error
	Close()
}

// Broker is the full consumer+publisher interface used by the worker.
// Start establishes the connection and spawns consumer goroutines;
// Close tears everything down.
type Broker interface {
	Start()
	GetMessage(ctx context.Context) (*RawMessage, error)
	PublishMessage(message *RawMessage) error
	Close()
}
