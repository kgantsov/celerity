package backend

import (
	"context"
	"time"

	"github.com/kgantsov/celerity/internal/task"
)

type Backend interface {
	PrepareResult(ctx context.Context, taskID string, ttl time.Duration) error
	SetResult(ctx context.Context, taskID string, data []byte, ttl time.Duration) error
	GetResult(ctx context.Context, taskID string) ([]byte, error)
	// ResultKey derives the id SetResult/GetResult should use for tk. Each
	// backend picks its own convention: an AMQP-style backend replies to the
	// queue Celery names in ReplyTo, while a database-style backend (Redis,
	// ...) is looked up by the task's own id regardless of ReplyTo.
	ResultKey(tk *task.Task) string
	Close(ctx context.Context)
}
