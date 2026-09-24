package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/kgantsov/celerity/internal/task"
)

type RedisBackend struct {
	url    string
	rdb    *redis.Client
	logger *slog.Logger
}

func NewRedisBackend(
	parentCtx context.Context, url string, logger *slog.Logger,
) *RedisBackend {
	return &RedisBackend{
		url:    url,
		logger: logger,
	}
}

func (b *RedisBackend) Connect() error {
	opts, err := redis.ParseURL(b.url)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}

	b.rdb = rdb
	b.logger.Info("connected to Redis", "addr", opts.Addr)
	return nil
}

// PrepareResult is a no-op for Redis: unlike the RabbitMQ backend, there is
// no queue to declare ahead of time — SetResult's SET creates the key.
func (b *RedisBackend) PrepareResult(
	ctx context.Context, taskID string, ttl time.Duration,
) error {
	return nil
}

func (b *RedisBackend) SetResult(
	ctx context.Context, taskID string, data []byte, ttl time.Duration,
) error {
	key := fmt.Sprintf("celery-task-meta-%s", taskID)
	return b.rdb.Set(ctx, key, data, ttl).Err()
}

// GetResult blocks until the result key appears in Redis or ctx is done.
// Redis has no blocking "wait for key" primitive, so this polls.
func (b *RedisBackend) GetResult(ctx context.Context, taskID string) ([]byte, error) {
	key := fmt.Sprintf("celery-task-meta-%s", taskID)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		val, err := b.rdb.Get(ctx, key).Result()
		if err == nil {
			return []byte(val), nil
		}
		if !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("get result: %w", err)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// ResultKey looks results up by the task's own id: Celery always sets
// correlation id to the task id regardless of backend, while ReplyTo is
// only meaningful to AMQP-style backends (see RabbitMQBackend.ResultKey).
func (b *RedisBackend) ResultKey(tk *task.Task) string {
	if tk.CorrelationId != "" {
		return tk.CorrelationId
	}
	return tk.ID
}

func (b *RedisBackend) Close(ctx context.Context) {
	if b.rdb == nil {
		return
	}
	if err := b.rdb.Close(); err != nil {
		b.logger.Warn("error closing redis backend", "err", err)
	}
}
