package worker

import (
	"context"

	"github.com/kgantsov/celerity/internal/registry"
)

type Dispatcher struct {
	registry   *registry.TaskRegistry
	WorkerPool chan chan Job
	MaxWorkers int
	JobQueue   chan Job
	ctx        context.Context
	cancel     context.CancelFunc
	Workers    []*Worker
}

func NewDispatcher(registry *registry.TaskRegistry, JobQueue chan Job, maxWorkers int) *Dispatcher {
	WorkerPool := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		registry:   registry,
		WorkerPool: WorkerPool,
		MaxWorkers: maxWorkers,
		JobQueue:   JobQueue,
		Workers:    []*Worker{},
	}
}

// Run starts the dispatcher and its worker pool. It derives its own
// cancelable context from ctx, so everything stops as soon as either ctx is
// canceled or Stop() is called, whichever happens first. Workers derive
// their contexts from the dispatcher's, so canceling the dispatcher also
// cancels every worker in the pool.
func (d *Dispatcher) Run(ctx context.Context) {
	d.ctx, d.cancel = context.WithCancel(ctx)

	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.registry, d.WorkerPool)
		worker.Start(d.ctx)
		d.Workers = append(d.Workers, worker)
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job, ok := <-d.JobQueue:
			if !ok {
				return
			}
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle, or until we're
				// shutting down.
				select {
				case jobChannel := <-d.WorkerPool:
					select {
					case jobChannel <- job:
					case <-d.ctx.Done():
					}
				case <-d.ctx.Done():
				}
			}(job)
		case <-d.ctx.Done():
			return
		}
	}
}

// Stop stops the dispatch loop and, transitively, all workers. It is safe
// to call multiple times, and safe to call even if Run was never called.
func (d *Dispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
}
