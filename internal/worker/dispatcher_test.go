package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	celery "github.com/kgantsov/celerity/internal/protocol/celery"
	"github.com/kgantsov/celerity/internal/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDispatcher_StopWithoutRun(t *testing.T) {
	reg := registry.NewTaskRegistry()
	jobQueue := make(chan Job, 1)
	proto, err := celery.NewProtocol("2.0")
	require.NoError(t, err)
	d := NewDispatcher(reg, jobQueue, WorkerConfig{Count: 2}, &MockBroker{}, proto)
	assert.NotPanics(t, func() { d.Stop() })
}

func TestDispatcher_RunStop(t *testing.T) {
	tests := []struct {
		name       string
		maxWorkers int
	}{
		{name: "single worker", maxWorkers: 1},
		{name: "multiple workers", maxWorkers: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := registry.NewTaskRegistry()
			jobQueue := make(chan Job, 1)
			proto, err := celery.NewProtocol("2.0")
			require.NoError(t, err)
			d := NewDispatcher(reg, jobQueue, WorkerConfig{Count: tt.maxWorkers}, &MockBroker{}, proto)

			ctx, cancel := context.WithCancel(context.Background())
			d.Run(ctx)
			cancel()
			d.Stop()

			assert.Len(t, d.Workers, tt.maxWorkers)
		})
	}
}

func TestDispatcher_DispatchesJobsToWorkers(t *testing.T) {
	tests := []struct {
		name     string
		jobCount int
		workers  int
	}{
		{name: "one job one worker", jobCount: 1, workers: 1},
		{name: "many jobs many workers", jobCount: 5, workers: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := registry.NewTaskRegistry()
			assert.NoError(t, reg.Register("noop", func() error { return nil }, []string{}))

			proto, err := celery.NewProtocol("2.0")
			require.NoError(t, err)
			jobQueue := make(chan Job, tt.jobCount)
			d := NewDispatcher(reg, jobQueue, WorkerConfig{Count: tt.workers}, &MockBroker{}, proto)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			d.Run(ctx)
			defer d.Stop()

			var wg sync.WaitGroup
			deliveries := make([]*MockDelivery, tt.jobCount)
			for i := 0; i < tt.jobCount; i++ {
				del := &MockDelivery{}
				del.On("Ack", false).Return(nil)
				deliveries[i] = del

				wg.Add(1)
				msg := newTestMsg(t, "noop", []any{}, map[string]any{}, del, "", "", 0)
				jobQueue <- NewJob(msg, &wg)
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()

			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("timed out waiting for jobs to complete")
			}

			for _, del := range deliveries {
				del.AssertCalled(t, "Ack", false)
				del.AssertNotCalled(t, "Nack", mock.Anything)
			}
		})
	}
}
