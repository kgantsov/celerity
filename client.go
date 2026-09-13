package celerity

import (
	"context"
	"fmt"
	"log/slog"
	neturl "net/url"
	"time"

	"github.com/google/uuid"
	"github.com/kgantsov/celerity/internal/backend"
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
	// ReplyTo is the queue the worker should publish the result to.
	// When a backend URL is configured it defaults to the task ID.
	ReplyTo string
}

// Task is returned by Publish and carries enough context to fetch the result.
type Task struct {
	ID      string
	backend backend.Backend
}

// Client publishes tasks to a broker. It does not consume any queues.
// Create one with NewClient, call Connect, Publish tasks, then Close.
type Client struct {
	brokerURL  string
	backendURL string
	publisher  broker.Publisher
	backend    backend.Backend
	proto      celery.Protocol
	logger     *slog.Logger
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClientLogger sets the logger used by the Client.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithClientBackendURL sets the result backend URL. The connection is established in Connect.
func WithClientBackendURL(backendURL string) ClientOption {
	return func(c *Client) {
		c.backendURL = backendURL
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

// Connect establishes the broker and (if configured) backend connections.
// Must be called before Publish.
func (c *Client) Connect() error {
	pub, err := newPublisherForURL(c.brokerURL, c.logger)
	if err != nil {
		return err
	}
	c.publisher = pub

	if c.backendURL != "" {
		b, err := newBackendForURL(
			context.Background(),
			c.logger.With("component", "backend"),
			broker.BackendConfig{URL: c.backendURL},
		)
		if err != nil {
			return fmt.Errorf("connect to backend: %w", err)
		}
		c.backend = b
	} else {
		c.backend = noopBackend{}
	}

	return nil
}

// GetResult waits for the result of t and unmarshals it into T.
// Requires WithClientBackendURL to have been set when creating the client.
func GetResult[T any](ctx context.Context, t *Task) (T, error) {
	return backend.NewAsyncResult[T](t.ID, t.backend).Get(ctx)
}

// Close tears down the broker connection.
func (c *Client) Close() {
	if c.publisher != nil {
		c.publisher.Close()
	}
}

// Publish enqueues taskName and returns a Task that can be used to fetch the result.
func (c *Client) Publish(
	ctx context.Context, taskName string, opts PublishOptions,
) (*Task, error) {
	taskID := opts.TaskID
	if taskID == "" {
		taskID = uuid.NewString()
	}
	queue := opts.Queue
	if queue == "" {
		queue = "celery"
	}

	t := &internaltask.Task{
		ID:            taskID,
		Task:          taskName,
		QueueName:     queue,
		Args:          opts.Args,
		Kwargs:        opts.Kwargs,
		ReplyTo:       opts.ReplyTo,
		CorrelationId: taskID,
	}
	if t.ReplyTo == "" && c.backendURL != "" {
		t.ReplyTo = taskID
	}

	if t.ReplyTo != "" {
		if err := c.backend.PrepareResult(ctx, t.ReplyTo, 24*time.Hour); err != nil {
			return nil, fmt.Errorf("prepare result queue: %w", err)
		}
	}

	raw, err := c.proto.ToRawMessage(t)
	if err != nil {
		return nil, fmt.Errorf("serialize task: %w", err)
	}

	if err := c.publisher.PublishMessage(raw); err != nil {
		return nil, fmt.Errorf("publish task: %w", err)
	}

	c.logger.InfoContext(
		ctx, "task published", "task", taskName, "id", taskID, "queue", queue,
	)
	return &Task{ID: taskID, backend: c.backend}, nil
}

// newPublisherForURL creates a publish-only broker connection for the given URL.
func newPublisherForURL(brokerURL string, logger *slog.Logger) (broker.Publisher, error) {
	u, err := neturl.Parse(brokerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid broker URL: %w", err)
	}
	switch u.Scheme {
	case "amqp", "amqps":
		b := broker.NewRabbitMQBroker(
			context.Background(), logger, broker.BrokerConfig{URL: brokerURL},
		)
		if err := b.Connect(); err != nil {
			return nil, fmt.Errorf("connect to broker: %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported broker scheme %q", u.Scheme)
	}
}
