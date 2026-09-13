package backend

import (
	"context"
	"encoding/json"
	"fmt"
)

type AsyncResult[T any] struct {
	taskID  string
	backend Backend
}

func NewAsyncResult[T any](taskID string, b Backend) *AsyncResult[T] {
	return &AsyncResult[T]{taskID: taskID, backend: b}
}

func (r *AsyncResult[T]) Get(ctx context.Context) (T, error) {
	var zero T
	raw, err := r.backend.GetResult(ctx, r.taskID)
	if err != nil {
		return zero, err
	}

	var envelope struct {
		Status string          `json:"status"`
		Result json.RawMessage `json:"result"`
	}

	if err := json.Unmarshal(raw, &envelope); err != nil {
		return zero, fmt.Errorf("decode result envelope: %w", err)
	}

	if envelope.Status != "SUCCESS" {
		return zero, fmt.Errorf("task status: %s", envelope.Status)
	}

	var result T
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		return zero, fmt.Errorf("decode result: %w", err)
	}

	return result, nil
}
