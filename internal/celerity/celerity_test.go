package celerity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewCelerity_defaults(t *testing.T) {
	c := NewCelerity("amqp://guest:guest@localhost", []string{"default"})
	assert.Equal(t, 5, c.workers)
	assert.Equal(t, 5, c.prefetchCount)
	assert.Equal(t, []string{"default"}, c.queues)
}

func TestNewCelerity_options(t *testing.T) {
	tests := []struct {
		name               string
		opts               []Option
		wantWorkers        int
		wantPrefetchCount  int
	}{
		{
			name:              "default values",
			opts:              nil,
			wantWorkers:       5,
			wantPrefetchCount: 5,
		},
		{
			name:              "custom workers",
			opts:              []Option{WithWorkers(10)},
			wantWorkers:       10,
			wantPrefetchCount: 5,
		},
		{
			name:              "custom prefetch",
			opts:              []Option{WithPrefetchCount(2)},
			wantWorkers:       5,
			wantPrefetchCount: 2,
		},
		{
			name:              "both options",
			opts:              []Option{WithWorkers(3), WithPrefetchCount(1)},
			wantWorkers:       3,
			wantPrefetchCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCelerity("amqp://localhost", []string{"q"}, tt.opts...)
			assert.Equal(t, tt.wantWorkers, c.workers)
			assert.Equal(t, tt.wantPrefetchCount, c.prefetchCount)
		})
	}
}

func TestRegisterTask(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		fn         any
		paramNames []string
		wantErr    bool
	}{
		{
			name:       "valid function",
			taskName:   "add",
			fn:         func(a, b int) (int, error) { return a + b, nil },
			paramNames: []string{"a", "b"},
		},
		{
			name:       "not a function",
			taskName:   "bad",
			fn:         42,
			paramNames: []string{},
			wantErr:    true,
		},
		{
			name:       "param count mismatch",
			taskName:   "mismatch",
			fn:         func(a int) error { return nil },
			paramNames: []string{"a", "b"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCelerity("amqp://localhost", []string{"q"})
			err := c.RegisterTask(tt.taskName, tt.fn, tt.paramNames...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

type mockDelivery struct {
	mock.Mock
}

func (m *mockDelivery) Ack(multiple bool) error {
	return m.Called(multiple).Error(0)
}

func (m *mockDelivery) Nack(multiple bool) error {
	return m.Called(multiple).Error(0)
}

func TestCelerity_StartStop(t *testing.T) {
	acked := make(chan struct{})
	delivery := &mockDelivery{}
	delivery.On("Ack", false).Return(nil).Run(func(args mock.Arguments) {
		select {
		case acked <- struct{}{}:
		default:
		}
	})

	b := &MockBroker{}
	b.On("Start").Return()
	b.On("Close").Return()
	b.On("GetTask", mock.Anything).Once().Return(&task.Task{
		Task:     "add",
		Args:     []any{float64(1), float64(2)},
		Kwargs:   map[string]any{},
		Delivery: delivery,
	}, nil)
	b.On("GetTask", mock.Anything).Return((*task.Task)(nil), context.Canceled)

	c := NewCelerity("amqp://localhost", []string{"q"}, WithWorkers(2))
	c.broker = b
	require.NoError(t, c.RegisterTask("add", func(a, b int) (int, error) { return a + b, nil }, "a", "b"))

	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)

	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task ack")
	}

	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	c.Stop(stopCtx)

	b.AssertCalled(t, "Start")
	b.AssertCalled(t, "Close")
}

func TestCelerity_BrokerError(t *testing.T) {
	broker := &MockBroker{}
	broker.On("Start").Return()
	broker.On("Close").Return()
	broker.On("GetTask", mock.Anything).Return(nil, errors.New("connection lost"))

	c := NewCelerity("amqp://localhost", []string{"q"})
	c.broker = broker

	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)

	// broker error should cause the processing loop to exit cleanly
	time.Sleep(50 * time.Millisecond)
	cancel()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	c.Stop(stopCtx)

	broker.AssertCalled(t, "Start")
}
