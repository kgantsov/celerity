package broker

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Connect() error {
	return m.Called().Error(0)
}

func (m *MockPublisher) PublishMessage(msg *RawMessage) error {
	return m.Called(msg).Error(0)
}

func (m *MockPublisher) Close() {
	m.Called()
}

type MockBroker struct {
	mock.Mock
}

func (m *MockBroker) Start() {
	m.Called()
}

func (m *MockBroker) GetMessage(ctx context.Context) (*RawMessage, error) {
	args := m.Called(ctx)
	t, _ := args.Get(0).(*RawMessage)
	return t, args.Error(1)
}

func (m *MockBroker) PublishMessage(t *RawMessage) error {
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
