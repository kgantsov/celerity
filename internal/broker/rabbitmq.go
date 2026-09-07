package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kgantsov/celerity/internal/protocol/celeryv2"
	"github.com/kgantsov/celerity/internal/task"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Message wraps the RabbitMQ delivery to allow manual Ack/Nack by the caller.
type Message struct {
	Queue    string
	Body     []byte
	delivery amqp.Delivery
}

// Ack acknowledges the message, removing it from the queue.
func (m *Message) Ack() error {
	return m.delivery.Ack(false)
}

// Nack rejects the message. If requeue is true, it goes back in the queue.
func (m *Message) Nack(requeue bool) error {
	return m.delivery.Nack(false, requeue)
}

// RabbitMQBroker manages the connection and workers for RabbitMQ.
type RabbitMQBroker struct {
	config   BrokerConfig
	taskChan chan *task.Task
	// parentCtx signals that the caller wants to stop consuming new messages
	// (e.g. Ctrl+C). Consumer goroutines exit when it fires, but the AMQP
	// connection is kept alive so in-flight tasks can still ack/nack their
	// deliveries. Only Close() — which cancels ctx — actually tears down the
	// connection.
	parentCtx context.Context
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	connMu    sync.RWMutex
	conn      *amqp.Connection
}

// NewRabbitMQBroker initializes a new broker instance. parentCtx signals when
// to stop consuming (e.g. on Ctrl+C); call Close() separately to actually tear
// down the AMQP connection once all in-flight tasks have finished.
func NewRabbitMQBroker(parentCtx context.Context, config BrokerConfig) *RabbitMQBroker {
	// Derive ctx from Background, not from parentCtx. This keeps the AMQP
	// connection alive even after parentCtx is canceled, so workers can still
	// ack deliveries for tasks they picked up before the shutdown signal.
	ctx, cancel := context.WithCancel(context.Background())
	return &RabbitMQBroker{
		config:    config,
		taskChan:  make(chan *task.Task),
		parentCtx: parentCtx,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start launches the background connection manager and workers.
func (b *RabbitMQBroker) Start() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.manageConnection()
	}()
}

// GetTask blocks until a task is available on ANY of the configured queues,
// or until the broker is closed.
func (b *RabbitMQBroker) GetTask(ctx context.Context) (*task.Task, error) {
	select {
	case task, ok := <-b.taskChan:
		if !ok {
			return nil, fmt.Errorf("broker is closed")
		}
		return task, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.parentCtx.Done():
		return nil, fmt.Errorf("broker stopped consuming")
	case <-b.ctx.Done():
		return nil, fmt.Errorf("broker is closed")
	}
}

func (b *RabbitMQBroker) PublishTask(task *task.Task) error {
	b.connMu.RLock()
	conn := b.conn
	b.connMu.RUnlock()

	if conn == nil {
		return fmt.Errorf("no active connection")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	body, err := json.Marshal([]any{task.Args, task.Kwargs, map[string]any{}})
	if err != nil {
		return fmt.Errorf("failed to serialize task: %w", err)
	}

	return ch.Publish(
		"",             // default exchange
		task.QueueName, // routing key == queue name
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Headers: amqp.Table{
				"id":      task.ID,
				"task":    task.Task,
				"retries": task.RetryCount,
			},
			Body: body,
		},
	)
}

func (b *RabbitMQBroker) PublishResult(replyTo string, correlationID string, body []byte) error {
	b.connMu.RLock()
	conn := b.conn
	b.connMu.RUnlock()

	if conn == nil {
		return fmt.Errorf("no active connection")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	return ch.Publish(
		"",      // default exchange
		replyTo, // routing key == queue name
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			Headers:       amqp.Table{},
			CorrelationId: correlationID,
			Body:          body,
		},
	)
}

// closeTimeout bounds how long Close() will wait for background goroutines
// (including the AMQP connection/channel close handshakes) to finish before
// giving up, so a stuck broker connection can never hang the whole process.
const closeTimeout = 5 * time.Second

// Close gracefully shuts down the broker, stopping all workers and active connections.
// It never blocks longer than closeTimeout, even if the underlying AMQP
// connection is stuck performing its close handshake with the server.
func (b *RabbitMQBroker) Close() {
	b.closeOnce.Do(func() {
		log.Println("[Broker] Closing broker...")
		b.cancel() // Signal all loops and workers to stop

		done := make(chan struct{})
		go func() {
			b.wg.Wait() // Wait for background goroutines to exit cleanly
			close(done)
		}()

		select {
		case <-done:
			// All goroutines exited on their own; safe to close taskChan since
			// nothing can still be sending on it.
			close(b.taskChan)
			log.Println("[Broker] Broker closed successfully.")
		case <-time.After(closeTimeout):
			// Something (most likely the AMQP connection/channel close
			// handshake) is stuck. Give up waiting instead of hanging the
			// whole process forever. We deliberately do NOT close taskChan
			// here: a goroutine may still be alive and could send on it,
			// which would panic if the channel were closed.
			log.Println("[Broker] Timed out waiting for broker goroutines to stop; forcing shutdown.")
		}
	})
}

func (b *RabbitMQBroker) manageConnection() {
	for {
		log.Println("[Broker] Attempting to connect to RabbitMQ...")
		conn, err := amqp.Dial(b.config.URL)
		if err != nil {
			log.Printf("[Broker] Connection failed: %v. Retrying in 5s...", err)
			select {
			case <-b.ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		log.Println("[Broker] Connected!")
		b.connMu.Lock()
		b.conn = conn
		b.connMu.Unlock()

		connCtx, cancelConn := context.WithCancel(b.ctx)
		var workerWg sync.WaitGroup

		for _, q := range b.config.Queues {
			workerWg.Add(1)
			go b.consumeWorker(connCtx, &workerWg, conn, q)
		}

		select {
		case <-b.ctx.Done():
			// Explicit Close() — tear down everything.
			cancelConn()
			workerWg.Wait()
			b.connMu.Lock()
			b.conn = nil
			b.connMu.Unlock()
			conn.CloseDeadline(time.Now().Add(2 * time.Second))
			return
		case <-b.parentCtx.Done():
			// Caller signaled stop (e.g. Ctrl+C). Consumer goroutines will
			// exit on their own via parentCtx; wait for them, then hold the
			// connection open until Close() is called so in-flight tasks can
			// still ack their deliveries.
			workerWg.Wait()
			<-b.ctx.Done()
			b.connMu.Lock()
			b.conn = nil
			b.connMu.Unlock()
			conn.CloseDeadline(time.Now().Add(2 * time.Second))
			return
		case err := <-conn.NotifyClose(make(chan *amqp.Error, 1)):
			log.Printf("[Broker] Connection lost: %v. Reconnecting...", err)
			cancelConn()
			workerWg.Wait()
			b.connMu.Lock()
			b.conn = nil
			b.connMu.Unlock()
		}
	}
}

func (b *RabbitMQBroker) consumeWorker(ctx context.Context, wg *sync.WaitGroup, conn *amqp.Connection, queueName string) {
	defer wg.Done()

	for {
		// Check stop/close before opening a new channel.
		select {
		case <-ctx.Done():
			return
		case <-b.parentCtx.Done():
			return
		default:
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("[%s] Failed to open channel: %v. Retrying in 2s...", queueName, err)
			select {
			case <-ctx.Done():
				return
			case <-b.parentCtx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		ch.Qos(b.config.PrefetchCount, 0, false)

		msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		chClose := ch.NotifyClose(make(chan *amqp.Error, 1))
		log.Printf("[%s] Worker listening...", queueName)

	consumeLoop:
		for {
			select {
			case <-ctx.Done():
				// Explicit Close() — shut down the channel.
				ch.Close()
				return
			case <-b.parentCtx.Done():
				// Caller is stopping consumption (e.g. Ctrl+C). Do NOT close
				// the channel: it must stay open so workers can ack/nack the
				// deliveries they already picked up.
				return
			case err := <-chClose:
				log.Printf("[%s] Channel closed: %v. Rebuilding...", queueName, err)
				break consumeLoop
			case msg, ok := <-msgs:
				if !ok {
					break consumeLoop
				}
				log.Printf("[%s] Received a message: %s\n", queueName, msg.Body)

				task, err := celeryv2.ParseCeleryDelivery(msg)
				if err != nil {
					log.Printf("[%s] Error parsing task: %v. Nacking...", queueName, err)
					msg.Nack(false, false)
					continue
				}
				select {
				case b.taskChan <- task:
				case <-ctx.Done():
					msg.Nack(false, true)
					ch.Close()
					return
				case <-b.parentCtx.Done():
					// Nack this specific message so RabbitMQ requeues it;
					// don't close the channel (same reason as above).
					msg.Nack(false, true)
					return
				case <-chClose:
					break consumeLoop
				}
			}
		}
	}
}
