package celerity

import (
	"context"
	"log"
	"sync"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/protocol/celery"
	"github.com/kgantsov/celerity/internal/registry"
	"github.com/kgantsov/celerity/internal/worker"
)

type Config struct {
	Broker broker.BrokerConfig
	Worker worker.WorkerConfig
}

type Celerity struct {
	broker       broker.Broker
	dispatcher   *worker.Dispatcher
	registry     *registry.TaskRegistry
	config       Config
	wg           sync.WaitGroup
	protoVersion string
	proto        celery.Protocol
}

// Option defines a function type that modifies the Server config
type Option func(*Celerity)

// WithWorkers sets the number of workers for the Celerity server
func WithWorkers(workers int) Option {
	return func(s *Celerity) {
		s.config.Worker.Count = workers
	}
}

// WithPrefetchCount sets the prefetch count for the Celerity server
func WithPrefetchCount(prefetchCount int) Option {
	return func(s *Celerity) {
		s.config.Broker.PrefetchCount = prefetchCount
	}
}

// WithAcksLate sets the acksLate option for the Celerity server
func WithAcksLate(acksLate bool) Option {
	return func(s *Celerity) {
		s.config.Worker.AcksLate = acksLate
	}
}

// NewCelerity creates a new Celerity server with the given broker URL, queues,
// and optional configurations.
func NewCelerity(brokerURL string, queues []string, opts ...Option) *Celerity {
	registry := registry.NewTaskRegistry()
	server := &Celerity{
		registry: registry,
		config: Config{
			Broker: broker.BrokerConfig{
				URL:           brokerURL,
				Queues:        queues,
				PrefetchCount: 5, // default prefetch count
			},
			Worker: worker.WorkerConfig{
				Count:    5,
				AcksLate: false,
			},
		},
		protoVersion: "2.0",
	}

	// Apply any provided options
	for _, opt := range opts {
		opt(server)
	}

	proto, _ := celery.NewProtocol(server.protoVersion)
	server.proto = proto

	return server
}

// Start launches the broker, dispatcher and the task processing loop. All of
// them respect ctx: canceling it (e.g. via signal.NotifyContext on Ctrl+C)
// will cause every component to begin shutting down on its own, without
// requiring Stop() to be called. Call Stop() afterwards to wait for that
// shutdown to complete (with a bound on how long to wait).
func (c *Celerity) Start(ctx context.Context) {
	JobQueue := make(chan worker.Job)

	if c.broker == nil {
		c.broker = broker.NewRabbitMQBroker(ctx, c.config.Broker)
	}

	// Start the broker background routines
	c.broker.Start()

	c.dispatcher = worker.NewDispatcher(
		c.registry, JobQueue, c.config.Worker, c.broker, c.proto,
	)
	c.dispatcher.Run(ctx)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		var jobWg sync.WaitGroup
		defer jobWg.Wait()
		for {
			t, err := c.broker.GetMessage(ctx)
			if err != nil {
				log.Printf("Broker stopped: %v", err)
				return
			}

			log.Printf("Dispatching task: %+v\n", t)

			jobWg.Add(1)
			job := worker.NewJob(t, &jobWg)
			select {
			case JobQueue <- job:
			case <-ctx.Done():
				jobWg.Done()
				return
			}
		}
	}()
}

// Stop gracefully shuts down the broker, dispatcher/workers and the task
// processing loop, and waits for them to finish. It never waits past ctx's
// deadline/cancellation, so passing a context.WithTimeout guarantees Stop
// returns promptly even if something is stuck.
//
// Shutdown order matters for correctness: the AMQP connection must stay open
// until all in-flight tasks have acked/nacked their deliveries, so we close
// the broker only after the task loop (and its jobWg) have fully drained.
func (c *Celerity) Stop(ctx context.Context) {
	log.Println("Stopping Celerity...")

	c.dispatcher.Stop()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Celerity stopped cleanly.")
	case <-ctx.Done():
		log.Println("Timed out waiting for Celerity to stop; forcing shutdown.")
	}

	// Close the broker after all tasks have acked so the AMQP connection is
	// still alive when worker.go calls task.Delivery.Ack().
	c.broker.Close()
}

func (c *Celerity) RegisterTask(name string, fn any, paramNames []string) error {
	return c.registry.Register(name, fn, paramNames)
}
