package backend

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQBackend struct {
	url       string
	connMu    sync.RWMutex
	conn      *amqp.Connection
	logger    *slog.Logger
	parentCtx context.Context
	// ctx is derived from Background, not parentCtx, so the connection stays
	// alive after parentCtx is canceled — in-flight SetResult/GetResult calls
	// can still complete. Only Close() cancels ctx.
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func NewRabbitMQBackend(
	parentCtx context.Context, url string, logger *slog.Logger,
) *RabbitMQBackend {
	ctx, cancel := context.WithCancel(context.Background())
	return &RabbitMQBackend{
		url:       url,
		logger:    logger,
		parentCtx: parentCtx,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (b *RabbitMQBackend) Connect() error {
	conn, err := amqp.Dial(b.url)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	b.connMu.Lock()
	b.conn = conn
	b.connMu.Unlock()
	b.logger.Info("connected to RabbitMQ", "url", b.url)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.manageConnection(conn)
	}()
	return nil
}

// manageConnection watches for connection drops and reconnects indefinitely,
// mirroring the same pattern used by RabbitMQBroker.
func (b *RabbitMQBackend) manageConnection(conn *amqp.Connection) {
	for {
		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-b.ctx.Done():
			// Hard close via Close() — tear down immediately.
			b.connMu.Lock()
			b.conn = nil
			b.connMu.Unlock()
			conn.CloseDeadline(time.Now().Add(2 * time.Second))
			return
		case <-b.parentCtx.Done():
			// Graceful shutdown: stop reconnecting but hold the connection
			// open so in-flight SetResult/GetResult calls can still complete.
			// Wait for Close() to signal the hard shutdown.
			<-b.ctx.Done()
			b.connMu.Lock()
			b.conn = nil
			b.connMu.Unlock()
			conn.CloseDeadline(time.Now().Add(2 * time.Second))
			return
		case err := <-notifyClose:
			b.logger.Warn("connection lost, reconnecting", "err", err)
			b.connMu.Lock()
			b.conn = nil
			b.connMu.Unlock()
		}

		for {
			select {
			case <-b.ctx.Done():
				return
			case <-b.parentCtx.Done():
				// Shutdown signaled during reconnect — don't bother.
				<-b.ctx.Done()
				return
			case <-time.After(5 * time.Second):
			}

			newConn, dialErr := amqp.Dial(b.url)
			if dialErr != nil {
				b.logger.Error("reconnect failed, retrying in 5s", "err", dialErr)
				continue
			}
			b.connMu.Lock()
			b.conn = newConn
			b.connMu.Unlock()
			b.logger.Info("reconnected to RabbitMQ")
			conn = newConn
			break
		}
	}
}

func (b *RabbitMQBackend) withChannel(fn func(*amqp.Channel) error) error {
	b.connMu.RLock()
	conn := b.conn
	b.connMu.RUnlock()
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	return fn(ch)
}

func (b *RabbitMQBackend) PrepareResult(
	ctx context.Context, taskID string, ttl time.Duration,
) error {
	return b.withChannel(func(ch *amqp.Channel) error {
		_, err := ch.QueueDeclare(taskID, false, true, false, false, amqp.Table{
			"x-expires": int32(ttl.Milliseconds()),
		})
		return err
	})
}

func (b *RabbitMQBackend) SetResult(
	ctx context.Context, taskID string, data []byte, ttl time.Duration,
) error {
	return b.withChannel(func(ch *amqp.Channel) error {
		return ch.PublishWithContext(ctx, "", taskID, false, false, amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: taskID,
			Body:          data,
			Expiration:    strconv.FormatInt(ttl.Milliseconds(), 10),
		})
	})
}

// GetResult blocks until the result message arrives, the context expires,
// or the AMQP channel is closed (e.g. due to a connection drop).
func (b *RabbitMQBackend) GetResult(ctx context.Context, taskID string) ([]byte, error) {
	b.connMu.RLock()
	conn := b.conn
	b.connMu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclarePassive(taskID, false, true, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("result queue not found (was PrepareResult called?): %w", err)
	}
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}

	chClose := ch.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case msg, ok := <-msgs:
		if !ok {
			return nil, fmt.Errorf("channel closed while waiting for result")
		}
		return msg.Body, nil
	case amqpErr := <-chClose:
		return nil, fmt.Errorf("channel closed: %w", amqpErr)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *RabbitMQBackend) Close(ctx context.Context) {
	b.closeOnce.Do(func() {
		b.logger.Info("closing backend")
		b.cancel()

		done := make(chan struct{})
		go func() {
			b.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			b.logger.Info("backend closed")
		case <-ctx.Done():
			b.logger.Warn("timed out waiting for backend goroutine, forcing close")
			b.connMu.Lock()
			conn := b.conn
			b.conn = nil
			b.connMu.Unlock()
			if conn != nil {
				conn.CloseDeadline(time.Now().Add(2 * time.Second))
			}
		}
	})
}
