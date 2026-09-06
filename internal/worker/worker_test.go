package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kgantsov/celerity/internal/registry"
	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func runWorkerJob(t *testing.T, reg *registry.TaskRegistry, tk *task.Task) {
	t.Helper()
	pool := make(chan chan Job, 1)
	w := NewWorker(reg, pool)
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
			assert.NoError(t, reg.Register("add", func(a, b int) (int, error) { return a + b, nil }, "a", "b"))

			delivery := &MockDelivery{}
			delivery.On("Ack", false).Return(nil)

			tk := &task.Task{Task: "add", Args: tt.args, Kwargs: tt.kwargs, Delivery: delivery}
			runWorkerJob(t, reg, tk)

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
			assert.NoError(t, reg.Register("add", func(a, b int) (int, error) { return a + b, nil }, "a", "b"))

			delivery := &MockDelivery{}
			delivery.On("Ack", false).Return(nil)

			tk := &task.Task{Task: tt.taskName, Args: tt.args, Kwargs: tt.kwargs, Delivery: delivery}
			runWorkerJob(t, reg, tk)

			delivery.AssertCalled(t, "Ack", false)
			delivery.AssertNotCalled(t, "Nack", mock.Anything)
		})
	}
}

func TestWorker_businessErrorNacks(t *testing.T) {
	reg := registry.NewTaskRegistry()
	assert.NoError(t, reg.Register("fail", func() error { return errors.New("transient") }))

	delivery := &MockDelivery{}
	delivery.On("Nack", false).Return(nil)

	tk := &task.Task{Task: "fail", Args: []any{}, Kwargs: map[string]any{}, Delivery: delivery}
	runWorkerJob(t, reg, tk)

	delivery.AssertCalled(t, "Nack", false)
	delivery.AssertNotCalled(t, "Ack", mock.Anything)
}

func TestWorker_stopWithoutStart(t *testing.T) {
	pool := make(chan chan Job, 1)
	w := NewWorker(registry.NewTaskRegistry(), pool)
	assert.NotPanics(t, func() { w.Stop() })
}
