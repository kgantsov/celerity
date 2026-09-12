package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	celery "github.com/kgantsov/celerity/internal/protocol/celery"
	"github.com/kgantsov/celerity/internal/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
			reg := registry.NewTaskRegistry(slog.Default())
			assert.NoError(t, reg.Register("noop", func() error { return nil }, []string{}))
			assert.NoError(
				t, reg.Register(
					"add", func(a, b int) (int, error) { return a + b, nil }, []string{"a", "b"},
				),
			)

			proto, err := celery.NewProtocol(slog.Default(), "2.0")
			require.NoError(t, err)
			jobQueue := make(chan Job, 10)
			d := NewDispatcher(slog.Default(), reg, jobQueue, WorkerConfig{Count: 2}, &MockBroker{}, &MockBroker{}, proto)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			d.Run(ctx)
			defer d.Stop()

			scheduler := NewTaskScheduler(jobQueue)

			delivery := &MockDelivery{}
			delivery.On("Ack", false).Return(nil)

			msg := newTestMsg(t, tt.taskName, tt.args, tt.kwargs, delivery, "", "", 0)
			assert.NoError(t, scheduler.Schedule(msg))

			assert.NoError(t, scheduler.Stop())

			delivery.AssertCalled(t, "Ack", false)
			delivery.AssertNotCalled(t, "Nack", mock.Anything)
		})
	}
}
