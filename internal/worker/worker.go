package worker

import (
	"context"
	"errors"
	"log/slog"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/ctxlog"
	"github.com/kgantsov/celerity/internal/protocol/celery"
	"github.com/kgantsov/celerity/internal/registry"
	"github.com/kgantsov/celerity/internal/task"
)

type WorkerConfig struct {
	Count    int
	AcksLate bool
}

// Worker represents the worker that executes the job
type Worker struct {
	logger     *slog.Logger
	config     WorkerConfig
	broker     broker.Broker
	registry   *registry.TaskRegistry
	WorkerPool chan chan Job
	JobChannel chan Job
	ctx        context.Context
	cancel     context.CancelFunc
	proto      celery.Protocol
}

func NewWorker(
	logger *slog.Logger,
	registry *registry.TaskRegistry,
	workerPool chan chan Job,
	config WorkerConfig,
	broker broker.Broker,
	proto celery.Protocol,
) *Worker {
	return &Worker{
		logger:     logger,
		config:     config,
		broker:     broker,
		registry:   registry,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		proto:      proto,
	}
}

// Start method starts the run loop for the worker. It derives its own
// cancelable context from ctx, so the worker stops as soon as either ctx is
// canceled or Stop() is called, whichever happens first.
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)

	w.logger.Debug("worker started")

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
				msg := job.GetMessage()

				tk, err := w.proto.ToTask(msg)
				if err != nil {
					w.logger.Error("failed to parse message", "err", err)
					job.GetWaitGroup().Done()
					continue
				}

				logger := w.logger.With("task", tk.Task, "id", tk.ID)
				logger.Debug("processing task")

				if !w.config.AcksLate {
					tk.Delivery.Ack(false)
				}

				execCtx := ctxlog.With(w.ctx, logger)
				result, err := w.registry.Execute(execCtx, tk.Task, tk.Args, tk.Kwargs)

				if err != nil {
					logger.Error("task execution failed", "err", err)
					if retryable, ok := errors.AsType[task.Retryable](err); ok {
						if tk.RetryCount < retryable.GetMaxRetries() {
							logger.Info("retrying task", "attempt", tk.RetryCount+1)
							tk.RetryCount++

							msg, err := w.proto.ToRawMessage(tk)
							if err != nil {
								logger.Error("failed to serialize task for retry", "err", err)
								w.replyToResultQueue(logger, tk, "FAILURE", result)
								continue
							}

							if pubErr := w.broker.PublishMessage(msg); pubErr != nil {
								logger.Error("failed to republish task", "err", pubErr)
							}
						} else {
							logger.Warn("max retries reached")
							w.replyToResultQueue(logger, tk, "FAILURE", result)
						}
					}
					if w.config.AcksLate {
						tk.Delivery.Ack(false)
					}
				} else {
					logger.Debug("task succeeded", "result", result)

					w.replyToResultQueue(logger, tk, "SUCCESS", result)

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

func (w *Worker) replyToResultQueue(logger *slog.Logger, tk *task.Task, status string, result any) {
	if tk.ReplyTo == "" {
		logger.Debug("no reply_to queue, skipping result publish")
		return
	}
	msg, err := w.proto.BuildReplyMessage(tk, status, result)
	if err != nil {
		logger.Error("failed to build reply message", "err", err)
		return
	}

	w.broker.PublishMessage(msg)
}

// Stop signals the worker to stop listening for work requests. It is safe
// to call multiple times, and safe to call even if Start was never called.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
