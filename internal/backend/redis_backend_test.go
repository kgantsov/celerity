package backend

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestRedisBackend(t *testing.T) *RedisBackend {
	t.Helper()

	mr := miniredis.RunT(t)

	b := NewRedisBackend(context.Background(), "redis://"+mr.Addr(), slog.Default())
	require.NoError(t, b.Connect())
	t.Cleanup(func() { b.Close(context.Background()) })

	return b
}

func TestRedisBackend_SetAndGetResult(t *testing.T) {
	b := newTestRedisBackend(t)
	ctx := context.Background()

	err := b.SetResult(ctx, "task-1", []byte(`{"status":"SUCCESS"}`), time.Minute)
	require.NoError(t, err)

	val, err := b.GetResult(ctx, "task-1")
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"SUCCESS"}`, string(val))
}

func TestRedisBackend_PrepareResult_NoOp(t *testing.T) {
	b := newTestRedisBackend(t)
	ctx := context.Background()

	// PrepareResult must succeed even though no key has been set yet: Redis
	// needs no upfront declaration, unlike the RabbitMQ backend's queue.
	err := b.PrepareResult(ctx, "task-1", time.Minute)
	require.NoError(t, err)
}

func TestRedisBackend_GetResult_WaitsForKey(t *testing.T) {
	b := newTestRedisBackend(t)
	ctx := context.Background()

	done := make(chan struct{})
	var setErr error
	go func() {
		defer close(done)
		time.Sleep(50 * time.Millisecond)
		setErr = b.SetResult(ctx, "task-2", []byte(`{"status":"SUCCESS"}`), time.Minute)
	}()

	val, err := b.GetResult(ctx, "task-2")
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"SUCCESS"}`, string(val))

	<-done
	require.NoError(t, setErr)
}

func TestRedisBackend_ResultKey(t *testing.T) {
	b := &RedisBackend{}

	// Correlation id (the real task id) wins even when ReplyTo is set, since
	// a real Celery client using redis:// as its backend always sets ReplyTo
	// to its own thread_oid, never the task id.
	assert.Equal(t, "corr-1", b.ResultKey(&task.Task{
		ID: "id-1", CorrelationId: "corr-1", ReplyTo: "some-thread-oid",
	}))
	// Falls back to ID when CorrelationId is unset.
	assert.Equal(t, "id-1", b.ResultKey(&task.Task{ID: "id-1"}))
}

func TestRedisBackend_GetResult_ContextCanceled(t *testing.T) {
	b := newTestRedisBackend(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := b.GetResult(ctx, "never-set")
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}
