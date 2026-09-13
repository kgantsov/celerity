package backend

import (
	"context"
	"time"
)

type Backend interface {
	PrepareResult(ctx context.Context, taskID string, ttl time.Duration) error
	SetResult(ctx context.Context, taskID string, data []byte, ttl time.Duration) error
	GetResult(ctx context.Context, taskID string) ([]byte, error)
	Close()
}
