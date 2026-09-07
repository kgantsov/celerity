package worker

import (
	"context"
	"errors"
	"log"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/protocol/celeryv1"
	"github.com/kgantsov/celerity/internal/registry"
	"github.com/kgantsov/celerity/internal/task"
)

type WorkerConfig struct {
	Count    int
	AcksLate bool
}

// Worker represents the worker that executes the job
type Worker struct {
	config     WorkerConfig
	broker     broker.Broker
	registry   *registry.TaskRegistry
	WorkerPool chan chan Job
	JobChannel chan Job
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewWorker(
	registry *registry.TaskRegistry,
	workerPool chan chan Job,
	config WorkerConfig,
	broker broker.Broker,
) *Worker {
	return &Worker{
		config:     config,
		broker:     broker,
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
				tk := job.GetTask()
				log.Printf("Processing a task: %+v", tk)

				if !w.config.AcksLate {
					tk.Delivery.Ack(false)
				}

				result, err := w.registry.Execute(tk.Task, tk.Args, tk.Kwargs)

				if err != nil {
					log.Printf("Error executing task: %s", err.Error())
					if retryable, ok := errors.AsType[task.Retryable](err); ok {
						if tk.RetryCount < retryable.GetMaxRetries() {
							log.Printf("Retrying task, attempt %d", tk.RetryCount+1)
							tk.RetryCount++
							if pubErr := w.broker.PublishTask(tk); pubErr != nil {
								log.Printf("Failed to republish task: %s", pubErr.Error())
							}
						} else {
							log.Printf("Max retries reached for task: %+v", tk)
							w.replyToResultQueue(tk, "FAILURE", result)
						}
					}
					if w.config.AcksLate {
						tk.Delivery.Ack(false)
					}
				} else {
					log.Printf("Task result: %v\n", result)

					w.replyToResultQueue(tk, "SUCCESS", result)

					if w.config.AcksLate {
						tk.Delivery.Ack(false)
					}
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

func (w *Worker) replyToResultQueue(tk *task.Task, status string, result any) {
	if tk.ReplyTo == "" {
		return
	}
	resultBytes, err := celeryv1.BuildCeleryReplyPayload(tk.CorrelationId, status, result)
	if err != nil {
		log.Printf("Failed to serialize result: %s", err.Error())
		return
	}
	w.broker.PublishResult(tk.ReplyTo, tk.CorrelationId, resultBytes)
}

// Stop signals the worker to stop listening for work requests. It is safe
// to call multiple times, and safe to call even if Start was never called.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
