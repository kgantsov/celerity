package worker

import (
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
