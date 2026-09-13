package celerity

import (
	"context"
	"time"
)

type noopBackend struct{}

func (noopBackend) PrepareResult(_ context.Context, _ string, _ time.Duration) error { return nil }
func (noopBackend) SetResult(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}
func (noopBackend) GetResult(_ context.Context, _ string) ([]byte, error) { return nil, nil }
func (noopBackend) Close(_ context.Context)                                {}
