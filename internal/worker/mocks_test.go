package worker

import (
	"context"

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

func (e *testRetryError) Error() string      { return e.err.Error() }
func (e *testRetryError) GetMaxRetries() int8 { return e.maxRetries }

type MockBroker struct {
	mock.Mock
}

func (m *MockBroker) Start() {
	m.Called()
}

func (m *MockBroker) GetTask(ctx context.Context) (*task.Task, error) {
	args := m.Called(ctx)
	t, _ := args.Get(0).(*task.Task)
	return t, args.Error(1)
}

func (m *MockBroker) PublishTask(t *task.Task) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockBroker) Close() {
	m.Called()
}

