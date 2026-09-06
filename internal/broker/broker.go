package broker

import (
	"context"

	"github.com/kgantsov/celerity/internal/task"
)

type Broker interface {
	// Start starts the broker and begins consuming tasks from the queue.
	Start()
	// GetTask retrieves a task from the broker. It returns a pointer to a task.
	// Task and an error if any occurred during retrieval. Block until a task is
	// available or the context is canceled.
	GetTask(ctx context.Context) (*task.Task, error)
	// Stop stops the broker and cleans up any resources. It should be called
	// when the broker is no longer needed.
	Close()
}
