package worker

import (
	"context"
	"log"

	"github.com/kgantsov/celerity/internal/registry"
)

type WorkerConfig struct {
	Count    int
	AcksLate bool
}

// Worker represents the worker that executes the job
type Worker struct {
	config     WorkerConfig
	registry   *registry.TaskRegistry
	WorkerPool chan chan Job
	JobChannel chan Job
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewWorker(registry *registry.TaskRegistry, workerPool chan chan Job, config WorkerConfig) *Worker {
	return &Worker{
		config:     config,
		registry:   registry,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
	}
}

// Start method starts the run loop for the worker. It derives its own
// cancelable context from ctx, so the worker stops as soon as either ctx is
// canceled or Stop() is called, whichever happens first.
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)

	log.Println("Worker started")

	go func() {
		for {
			// register the current worker into the worker queue.
			select {
			case w.WorkerPool <- w.JobChannel:
			case <-w.ctx.Done():
				return
			}

			select {
			case job := <-w.JobChannel:
				task := job.GetTask()
				log.Printf("Processing a task: %+v", task)

				result, err := w.registry.Execute(task.Task, task.Args, task.Kwargs)

				if err != nil {
					log.Printf("Error executing task: %s", err.Error())
					switch err {
					case registry.ErrTaskNotFound, registry.ErrTooManyArguments, registry.ErrInvalidArgumentType, registry.ErrMissingArgument:
						task.Delivery.Ack(false)
					default:
						log.Printf("Republishing failed task: %s", err.Error())
						task.Delivery.Nack(false)
					}
				} else {
					log.Printf("Task result: %v\n", result)
					task.Delivery.Ack(false)
				}

				job.GetWaitGroup().Done()

			case <-w.ctx.Done():
				// context was canceled (e.g. Ctrl+C, or Stop() was called);
				// stop picking up work
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests. It is safe
// to call multiple times, and safe to call even if Start was never called.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
