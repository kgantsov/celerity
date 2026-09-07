package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kgantsov/celerity/internal/registry"
	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func runWorkerJob(t *testing.T, reg *registry.TaskRegistry, tk *task.Task, config WorkerConfig, b *MockBroker) {
	t.Helper()
	pool := make(chan chan Job, 1)
	w := NewWorker(reg, pool, config, b)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	job := NewJob(tk, &wg)

	jobCh := <-pool
	jobCh <- job
	wg.Wait()
}

func TestWorker_successAcks(t *testing.T) {
	tests := []struct {
		name   string
		args   []any
		kwargs map[string]any
	}{
		{name: "positional args", args: []any{float64(1), float64(2)}, kwargs: map[string]any{}},
		{name: "kwargs", args: []any{}, kwargs: map[string]any{"a": float64(3), "b": float64(4)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := registry.NewTaskRegistry()
			assert.NoError(
				t, reg.Register(
					"add", func(a, b int) (int, error) { return a + b, nil }, []string{"a", "b"},
				),
			)

			delivery := &MockDelivery{}
			delivery.On("Ack", false).Return(nil)

			tk := &task.Task{Task: "add", Args: tt.args, Kwargs: tt.kwargs, Delivery: delivery}
			runWorkerJob(t, reg, tk, WorkerConfig{Count: 1}, &MockBroker{})

			delivery.AssertCalled(t, "Ack", false)
			delivery.AssertNotCalled(t, "Nack", mock.Anything)
		})
	}
}

func TestWorker_registryErrorAcks(t *testing.T) {
	tests := []struct {
		name     string
		taskName string
		args     []any
		kwargs   map[string]any
	}{
		{
			name:     "task not found",
			taskName: "missing",
			args:     []any{},
			kwargs:   map[string]any{},
		},
		{
			name:     "too many args",
			taskName: "add",
			args:     []any{float64(1), float64(2), float64(3)},
			kwargs:   map[string]any{},
		},
		{
			name:     "missing kwarg",
			taskName: "add",
			args:     []any{float64(1)},
			kwargs:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := registry.NewTaskRegistry()
			assert.NoError(
				t, reg.Register(
					"add", func(a, b int) (int, error) { return a + b, nil }, []string{"a", "b"},
				),
			)

			delivery := &MockDelivery{}
			delivery.On("Ack", false).Return(nil)

			tk := &task.Task{Task: tt.taskName, Args: tt.args, Kwargs: tt.kwargs, Delivery: delivery}
			runWorkerJob(t, reg, tk, WorkerConfig{Count: 1}, &MockBroker{})

			delivery.AssertCalled(t, "Ack", false)
			delivery.AssertNotCalled(t, "Nack", mock.Anything)
		})
	}
}

// AcksLate=true: transient errors republish for retry then ack the original delivery.
func TestWorker_businessErrorRetries(t *testing.T) {
	reg := registry.NewTaskRegistry()
	retryErr := &testRetryError{err: errors.New("transient"), maxRetries: 3}
	assert.NoError(
		t, reg.Register("fail", func() error { return retryErr }, []string{}),
	)

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	b := &MockBroker{}
	b.On("PublishTask", mock.Anything).Return(nil)

	tk := &task.Task{Task: "fail", Args: []any{}, Kwargs: map[string]any{}, Delivery: delivery}
	runWorkerJob(t, reg, tk, WorkerConfig{Count: 1, AcksLate: true}, b)

	b.AssertCalled(t, "PublishTask", mock.Anything)
	delivery.AssertCalled(t, "Ack", false)
	delivery.AssertNotCalled(t, "Nack", mock.Anything)
}

// AcksLate=false: delivery is acked before execution regardless of outcome.
func TestWorker_acksLateFalse_businessErrorAcks(t *testing.T) {
	reg := registry.NewTaskRegistry()
	assert.NoError(
		t, reg.Register("fail", func() error { return errors.New("transient") }, []string{}),
	)

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	tk := &task.Task{Task: "fail", Args: []any{}, Kwargs: map[string]any{}, Delivery: delivery}
	runWorkerJob(t, reg, tk, WorkerConfig{Count: 1, AcksLate: false}, &MockBroker{})

	delivery.AssertCalled(t, "Ack", false)
	delivery.AssertNotCalled(t, "Nack", mock.Anything)
}

func TestWorker_stopWithoutStart(t *testing.T) {
	pool := make(chan chan Job, 1)
	w := NewWorker(registry.NewTaskRegistry(), pool, WorkerConfig{Count: 1}, &MockBroker{})
	assert.NotPanics(t, func() { w.Stop() })
}

func TestWorker_publishesSuccessResult(t *testing.T) {
	reg := registry.NewTaskRegistry()
	require.NoError(t, reg.Register(
		"add", func(a, b int) (int, error) { return a + b, nil }, []string{"a", "b"},
	))

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	var capturedBody []byte
	b := &MockBroker{}
	b.On("PublishResult", "reply-queue", "corr-id-123", mock.Anything).
		Run(func(args mock.Arguments) { capturedBody = args[2].([]byte) }).
		Return(nil)

	tk := &task.Task{
		Task:          "add",
		Args:          []any{float64(3), float64(4)},
		Kwargs:        map[string]any{},
		Delivery:      delivery,
		ReplyTo:       "reply-queue",
		CorrelationId: "corr-id-123",
	}
	runWorkerJob(t, reg, tk, WorkerConfig{Count: 1}, b)

	b.AssertCalled(t, "PublishResult", "reply-queue", "corr-id-123", mock.Anything)

	var reply map[string]any
	require.NoError(t, json.Unmarshal(capturedBody, &reply))
	assert.Equal(t, "corr-id-123", reply["task_id"])
	assert.Equal(t, "SUCCESS", reply["status"])
	assert.Equal(t, float64(7), reply["result"])
}

func TestWorker_doesNotPublishResultWithoutReplyTo(t *testing.T) {
	reg := registry.NewTaskRegistry()
	require.NoError(t, reg.Register(
		"add", func(a, b int) (int, error) { return a + b, nil }, []string{"a", "b"},
	))

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	b := &MockBroker{}

	tk := &task.Task{
		Task:     "add",
		Args:     []any{float64(1), float64(2)},
		Kwargs:   map[string]any{},
		Delivery: delivery,
	}
	runWorkerJob(t, reg, tk, WorkerConfig{Count: 1}, b)

	b.AssertNotCalled(t, "PublishResult", mock.Anything, mock.Anything, mock.Anything)
}

func TestWorker_publishesFailureOnMaxRetries(t *testing.T) {
	reg := registry.NewTaskRegistry()
	retryErr := &testRetryError{err: errors.New("boom"), maxRetries: 2}
	require.NoError(t, reg.Register(
		"fail", func() error { return retryErr }, []string{},
	))

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	var capturedBody []byte
	b := &MockBroker{}
	b.On("PublishResult", "reply-queue", "corr-id-456", mock.Anything).
		Run(func(args mock.Arguments) { capturedBody = args[2].([]byte) }).
		Return(nil)

	tk := &task.Task{
		Task:          "fail",
		Args:          []any{},
		Kwargs:        map[string]any{},
		Delivery:      delivery,
		ReplyTo:       "reply-queue",
		CorrelationId: "corr-id-456",
		RetryCount:    2, // already at max
	}
	runWorkerJob(t, reg, tk, WorkerConfig{Count: 1, AcksLate: true}, b)

	b.AssertCalled(t, "PublishResult", "reply-queue", "corr-id-456", mock.Anything)
	b.AssertNotCalled(t, "PublishTask", mock.Anything)

	var reply map[string]any
	require.NoError(t, json.Unmarshal(capturedBody, &reply))
	assert.Equal(t, "corr-id-456", reply["task_id"])
	assert.Equal(t, "FAILURE", reply["status"])
}

func TestWorker_doesNotPublishResultDuringRetry(t *testing.T) {
	reg := registry.NewTaskRegistry()
	retryErr := &testRetryError{err: errors.New("transient"), maxRetries: 3}
	require.NoError(t, reg.Register(
		"fail", func() error { return retryErr }, []string{},
	))

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	b := &MockBroker{}
	b.On("PublishTask", mock.Anything).Return(nil)

	tk := &task.Task{
		Task:          "fail",
		Args:          []any{},
		Kwargs:        map[string]any{},
		Delivery:      delivery,
		ReplyTo:       "reply-queue",
		CorrelationId: "corr-id-789",
		RetryCount:    0,
	}
	runWorkerJob(t, reg, tk, WorkerConfig{Count: 1, AcksLate: true}, b)

	b.AssertCalled(t, "PublishTask", mock.Anything)
	b.AssertNotCalled(t, "PublishResult", mock.Anything, mock.Anything, mock.Anything)
}
