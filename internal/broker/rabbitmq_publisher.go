package broker

import (
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQPublisher is a publish-only AMQP client. It holds a single
// connection and opens a fresh channel per publish. On connection failure it
// reconnects once and retries before returning an error.
type RabbitMQPublisher struct {
	url    string
	connMu sync.RWMutex
	conn   *amqp.Connection
	logger *slog.Logger
}

func NewRabbitMQPublisher(url string, logger *slog.Logger) *RabbitMQPublisher {
	return &RabbitMQPublisher{url: url, logger: logger}
}

func (p *RabbitMQPublisher) Connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	p.connMu.Lock()
	p.conn = conn
	p.connMu.Unlock()

	p.logger.Info("connected to RabbitMQ", "url", p.url)

	return nil
}

func (p *RabbitMQPublisher) PublishMessage(msg *RawMessage) error {
	err := p.publish(msg)
	if err == nil {
		return nil
	}

	p.connMu.RLock()
	closed := p.conn == nil || p.conn.IsClosed()
	p.connMu.RUnlock()

	if !closed {
		return err
	}

	p.logger.Warn("connection lost, reconnecting", "err", err)
	if reconnErr := p.Connect(); reconnErr != nil {
		return fmt.Errorf("reconnect failed: %w", reconnErr)
	}

	return p.publish(msg)
}

func (p *RabbitMQPublisher) publish(msg *RawMessage) error {
	p.connMu.RLock()
	conn := p.conn
	p.connMu.RUnlock()

	if conn == nil {
		return fmt.Errorf("not connected: call Connect() first")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	return ch.Publish(
		"",
		msg.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Headers:     msg.Headers,
			Body:        msg.Body,
		},
	)
}

func (p *RabbitMQPublisher) Close() {
	p.connMu.Lock()
	conn := p.conn
	p.conn = nil
	p.connMu.Unlock()

	if conn != nil {
		conn.Close()
		p.logger.Info("disconnected from RabbitMQ")
	}
}
