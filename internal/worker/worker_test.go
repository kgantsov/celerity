package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kgantsov/celerity/internal/broker"
	celery "github.com/kgantsov/celerity/internal/protocol/celery"
	"github.com/kgantsov/celerity/internal/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestMsg(
	t *testing.T,
	taskName string,
	args []any,
	kwargs map[string]any,
	delivery broker.Delivery,
	replyTo, corrID string,
	retryCount int8,
) *broker.RawMessage {
	t.Helper()
	body, err := json.Marshal([]any{args, kwargs, nil})
	require.NoError(t, err)
	return &broker.RawMessage{
		Headers:       map[string]any{"id": "test-id", "task": taskName, "retries": retryCount},
		Body:          body,
		ReplyTo:       replyTo,
		CorrelationID: corrID,
		Delivery:      delivery,
	}
}

func runWorkerJob(t *testing.T, reg *registry.TaskRegistry, msg *broker.RawMessage, config WorkerConfig, b *MockBroker) {
	t.Helper()
	proto, err := celery.NewProtocol("2.0")
	require.NoError(t, err)
	pool := make(chan chan Job, 1)
	w := NewWorker(reg, pool, config, b, proto)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	job := NewJob(msg, &wg)

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

			msg := newTestMsg(t, "add", tt.args, tt.kwargs, delivery, "", "", 0)
			runWorkerJob(t, reg, msg, WorkerConfig{Count: 1}, &MockBroker{})

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

			msg := newTestMsg(t, tt.taskName, tt.args, tt.kwargs, delivery, "", "", 0)
			runWorkerJob(t, reg, msg, WorkerConfig{Count: 1}, &MockBroker{})

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
	b.On("PublishMessage", mock.Anything).Return(nil)

	msg := newTestMsg(t, "fail", []any{}, map[string]any{}, delivery, "", "", 0)
	runWorkerJob(t, reg, msg, WorkerConfig{Count: 1, AcksLate: true}, b)

	b.AssertCalled(t, "PublishMessage", mock.Anything)
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

	msg := newTestMsg(t, "fail", []any{}, map[string]any{}, delivery, "", "", 0)
	runWorkerJob(t, reg, msg, WorkerConfig{Count: 1, AcksLate: false}, &MockBroker{})

	delivery.AssertCalled(t, "Ack", false)
	delivery.AssertNotCalled(t, "Nack", mock.Anything)
}

func TestWorker_stopWithoutStart(t *testing.T) {
	pool := make(chan chan Job, 1)
	proto, err := celery.NewProtocol("2.0")
	require.NoError(t, err)
	w := NewWorker(registry.NewTaskRegistry(), pool, WorkerConfig{Count: 1}, &MockBroker{}, proto)
	assert.NotPanics(t, func() { w.Stop() })
}

func TestWorker_publishesSuccessResult(t *testing.T) {
	reg := registry.NewTaskRegistry()
	require.NoError(t, reg.Register(
		"add", func(a, b int) (int, error) { return a + b, nil }, []string{"a", "b"},
	))

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	var capturedMsg *broker.RawMessage
	b := &MockBroker{}
	b.On("PublishMessage", mock.Anything).
		Run(func(args mock.Arguments) { capturedMsg = args[0].(*broker.RawMessage) }).
		Return(nil)

	msg := newTestMsg(t, "add", []any{float64(3), float64(4)}, map[string]any{}, delivery, "reply-queue", "corr-id-123", 0)
	runWorkerJob(t, reg, msg, WorkerConfig{Count: 1}, b)

	b.AssertCalled(t, "PublishMessage", mock.Anything)
	require.NotNil(t, capturedMsg)
	assert.Equal(t, "reply-queue", capturedMsg.Queue)

	var reply map[string]any
	require.NoError(t, json.Unmarshal(capturedMsg.Body, &reply))
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

	msg := newTestMsg(t, "add", []any{float64(1), float64(2)}, map[string]any{}, delivery, "", "", 0)
	runWorkerJob(t, reg, msg, WorkerConfig{Count: 1}, b)

	b.AssertNotCalled(t, "PublishMessage", mock.Anything)
}

func TestWorker_publishesFailureOnMaxRetries(t *testing.T) {
	reg := registry.NewTaskRegistry()
	retryErr := &testRetryError{err: errors.New("boom"), maxRetries: 2}
	require.NoError(t, reg.Register(
		"fail", func() error { return retryErr }, []string{},
	))

	delivery := &MockDelivery{}
	delivery.On("Ack", false).Return(nil)

	var capturedMsg *broker.RawMessage
	b := &MockBroker{}
	b.On("PublishMessage", mock.Anything).
		Run(func(args mock.Arguments) { capturedMsg = args[0].(*broker.RawMessage) }).
		Return(nil)

	msg := newTestMsg(t, "fail", []any{}, map[string]any{}, delivery, "reply-queue", "corr-id-456", 2)
	runWorkerJob(t, reg, msg, WorkerConfig{Count: 1, AcksLate: true}, b)

	b.AssertNumberOfCalls(t, "PublishMessage", 1)
	require.NotNil(t, capturedMsg)
	assert.Equal(t, "reply-queue", capturedMsg.Queue)

	var reply map[string]any
	require.NoError(t, json.Unmarshal(capturedMsg.Body, &reply))
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
	b.On("PublishMessage", mock.Anything).Return(nil)

	msg := newTestMsg(t, "fail", []any{}, map[string]any{}, delivery, "reply-queue", "corr-id-789", 0)
	runWorkerJob(t, reg, msg, WorkerConfig{Count: 1, AcksLate: true}, b)

	b.AssertCalled(t, "PublishMessage", mock.Anything)
	b.AssertNumberOfCalls(t, "PublishMessage", 1)
}
