package worker

import (
	"context"
	"time"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/mock"
)

type MockDelivery struct {
	mock.Mock
}

func (m *MockDelivery) Ack(multiple bool) error {
	args := m.Called(multiple)
	return args.Error(0)
}

func (m *MockDelivery) Nack(multiple bool) error {
	args := m.Called(multiple)
	return args.Error(0)
}

// testRetryError implements task.Retryable for use in worker tests, avoiding
// a circular import on the celerity root package.
type testRetryError struct {
	err        error
	maxRetries int8
}

func (e *testRetryError) Error() string       { return e.err.Error() }
func (e *testRetryError) GetMaxRetries() int8 { return e.maxRetries }

type MockBroker struct {
	mock.Mock
}

func (m *MockBroker) Start() {
	m.Called()
}

func (m *MockBroker) GetMessage(ctx context.Context) (*broker.RawMessage, error) {
	args := m.Called(ctx)
	msg, _ := args.Get(0).(*broker.RawMessage)
	return msg, args.Error(1)
}

func (m *MockBroker) PublishMessage(msg *broker.RawMessage) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockBroker) Close(ctx context.Context) {
	m.Called(ctx)
}

type MockBackend struct {
	mock.Mock
}

func (m *MockBackend) PrepareResult(ctx context.Context, taskID string, ttl time.Duration) error {
	return m.Called(ctx, taskID, ttl).Error(0)
}

func (m *MockBackend) SetResult(ctx context.Context, taskID string, data []byte, ttl time.Duration) error {
	args := m.Called(ctx, taskID, data, ttl)
	return args.Error(0)
}

func (m *MockBackend) GetResult(ctx context.Context, taskID string) ([]byte, error) {
	args := m.Called(ctx, taskID)
	result, _ := args.Get(0).([]byte)
	return result, args.Error(1)
}

func (m *MockBackend) Close(ctx context.Context) {
	m.Called(ctx)
}

// ResultKey mimics a database-style backend (correlation id, falling back to
// task id) rather than going through testify's expectation machinery: it's
// pure derivation with no side effect worth asserting on, and requiring
// every test to stub it would be noise. Backend-specific ResultKey behavior
// (e.g. RabbitMQBackend staying on ReplyTo) is covered by that backend's own
// tests.
func (m *MockBackend) ResultKey(tk *task.Task) string {
	if tk.CorrelationId != "" {
		return tk.CorrelationId
	}
	return tk.ID
}
