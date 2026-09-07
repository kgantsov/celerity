package broker

import (
	"context"

	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/mock"
)

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

func (m *MockBroker) PublishResult(replyTo string, correlationID string, body []byte) error {
	args := m.Called(replyTo, correlationID, body)
	return args.Error(0)
}

func (m *MockBroker) Close() {
	m.Called()
}
