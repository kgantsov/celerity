package celerity

import (
	"context"
	"fmt"
	"log/slog"
	neturl "net/url"

	"github.com/google/uuid"
	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/protocol/celery"
	internaltask "github.com/kgantsov/celerity/internal/task"
)

// PublishOptions controls how a task is enqueued.
type PublishOptions struct {
	// Queue is the destination queue. Defaults to "celery" if empty.
	Queue string
	// Args are positional arguments passed to the task handler.
	Args []any
	// Kwargs are keyword arguments passed to the task handler.
	Kwargs map[string]any
	// TaskID is the Celery task ID. A UUID is generated when empty.
	TaskID string
}

// Client publishes tasks to a broker. It does not consume any queues.
// Create one with NewClient, call Connect, Publish tasks, then Close.
type Client struct {
	brokerURL string
	publisher broker.Publisher
	proto     celery.Protocol
	logger    *slog.Logger
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClientLogger sets the logger used by the Client.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// NewClient creates a Client that will publish to brokerURL.
// The URL scheme determines the broker implementation (e.g. amqp://).
// Call Connect before Publish and Close when done.
func NewClient(brokerURL string, opts ...ClientOption) *Client {
	c := &Client{
		brokerURL: brokerURL,
		logger:    slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.proto, _ = celery.NewProtocol(c.logger.With("component", "protocol"), "2.0")
	return c
}

// Connect establishes the broker connection. Must be called before Publish.
func (c *Client) Connect() error {
	pub, err := newPublisherForURL(c.brokerURL, c.logger)
	if err != nil {
		return err
	}
	if err := pub.Connect(); err != nil {
		return err
	}
	c.publisher = pub
	return nil
}

// Close tears down the broker connection.
func (c *Client) Close() {
	if c.publisher != nil {
		c.publisher.Close()
	}
}

// Publish enqueues taskName with the given options and returns the task ID.
func (c *Client) Publish(
	ctx context.Context, taskName string, opts PublishOptions,
) (string, error) {
	taskID := opts.TaskID
	if taskID == "" {
		taskID = uuid.NewString()
	}
	queue := opts.Queue
	if queue == "" {
		queue = "celery"
	}

	t := &internaltask.Task{
		ID:        taskID,
		Task:      taskName,
		QueueName: queue,
		Args:      opts.Args,
		Kwargs:    opts.Kwargs,
	}

	raw, err := c.proto.ToRawMessage(t)
	if err != nil {
		return "", fmt.Errorf("serialize task: %w", err)
	}

	if err := c.publisher.PublishMessage(raw); err != nil {
		return "", fmt.Errorf("publish task: %w", err)
	}

	c.logger.InfoContext(
		ctx, "task published", "task", taskName, "id", taskID, "queue", queue,
	)
	return taskID, nil
}

// newPublisherForURL selects a Publisher implementation based on the URL scheme.
// This is the extension point for future broker backends (redis://, mongodb://, etc.).
func newPublisherForURL(brokerURL string, logger *slog.Logger) (broker.Publisher, error) {
	u, err := neturl.Parse(brokerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid broker URL: %w", err)
	}
	switch u.Scheme {
	case "amqp", "amqps":
		return broker.NewRabbitMQPublisher(brokerURL, logger), nil
	default:
		return nil, fmt.Errorf("unsupported broker scheme %q", u.Scheme)
	}
}
