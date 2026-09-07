package worker

import (
	"context"

	"github.com/kgantsov/celerity/internal/broker"
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

func (m *MockBroker) Close() {
	m.Called()
}
