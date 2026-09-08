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

type Broker interface {
	// Start starts the broker and begins consuming tasks from the queue.
	Start()
	// GetMessage retrieves a task from the broker. It returns a pointer to a task.
	// Task and an error if any occurred during retrieval. Block until a task is
	// available or the context is canceled.
	GetMessage(ctx context.Context) (*RawMessage, error)
	// PublishMessage publishes a task to the broker. It takes a pointer to a task.Task
	// and returns an error if any occurred during publishing.
	PublishMessage(message *RawMessage) error
	// PublishResult publishes the result of a task to the broker. It takes a replyTo string,
	// a correlationID string, and a body byte slice. It returns an error if any
	// occurred during publishing.
	Close()
}
