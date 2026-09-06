package worker

import (
	"context"
	"testing"
	"time"

	"github.com/kgantsov/celerity/internal/registry"
	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTaskScheduler_ScheduleAndStop(t *testing.T) {
	tests := []struct {
		name     string
		taskName string
		args     []any
		kwargs   map[string]any
	}{
		{name: "no args", taskName: "noop", args: []any{}, kwargs: map[string]any{}},
		{name: "with args", taskName: "add", args: []any{float64(1), float64(2)}, kwargs: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := registry.NewTaskRegistry()
			assert.NoError(t, reg.Register("noop", func() error { return nil }))
			assert.NoError(t, reg.Register("add", func(a, b int) (int, error) { return a + b, nil }, "a", "b"))

			jobQueue := make(chan Job, 10)
			d := NewDispatcher(reg, jobQueue, 2)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			d.Run(ctx)
			defer d.Stop()

			scheduler := NewTaskScheduler(jobQueue)

			delivery := &MockDelivery{}
			delivery.On("Ack", false).Return(nil)

			tk := &task.Task{Task: tt.taskName, Args: tt.args, Kwargs: tt.kwargs, Delivery: delivery}
			assert.NoError(t, scheduler.Schedule(tk))

			assert.NoError(t, scheduler.Stop())

			delivery.AssertCalled(t, "Ack", false)
			delivery.AssertNotCalled(t, "Nack", mock.Anything)
		})
	}
}
