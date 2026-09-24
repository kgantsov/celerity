package celerity

import (
	"context"
	"time"

	"github.com/kgantsov/celerity/internal/task"
)

type noopBackend struct{}

func (noopBackend) PrepareResult(_ context.Context, _ string, _ time.Duration) error { return nil }
func (noopBackend) SetResult(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}
func (noopBackend) GetResult(_ context.Context, _ string) ([]byte, error) { return nil, nil }
func (noopBackend) ResultKey(_ *task.Task) string                         { return "" }
func (noopBackend) Close(_ context.Context)                               {}
