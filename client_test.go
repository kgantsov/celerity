package celerity

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestClient_Publish(t *testing.T) {
	tests := []struct {
		name        string
		taskName    string
		opts        PublishOptions
		publishErr  error
		wantErr     bool
		wantQueue   string
		wantTaskHdr string
		wantArgs    []any
		wantKwargs  map[string]any
	}{
		{
			name:     "explicit queue and task ID",
			taskName: "hello.add",
			opts: PublishOptions{
				Queue:  "myqueue",
				Args:   []any{5, 3},
				TaskID: "abc-123",
			},
			wantQueue:   "myqueue",
			wantTaskHdr: "hello.add",
			wantArgs:    []any{float64(5), float64(3)},
		},
		{
			name:        "empty queue defaults to celery",
			taskName:    "hello.add",
			opts:        PublishOptions{Args: []any{1}},
			wantQueue:   "celery",
			wantTaskHdr: "hello.add",
			wantArgs:    []any{float64(1)},
		},
		{
			name:     "kwargs only",
			taskName: "hello.greet",
			opts: PublishOptions{
				Kwargs: map[string]any{"name": "world"},
			},
			wantQueue:   "celery",
			wantTaskHdr: "hello.greet",
			wantKwargs:  map[string]any{"name": "world"},
		},
		{
			name:        "no args or kwargs",
			taskName:    "hello.ping",
			opts:        PublishOptions{},
			wantQueue:   "celery",
			wantTaskHdr: "hello.ping",
		},
		{
			name:       "publisher error propagates",
			taskName:   "hello.add",
			opts:       PublishOptions{},
			publishErr: errors.New("connection lost"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &broker.MockPublisher{}

			var captured *broker.RawMessage
			pub.On("PublishMessage", mock.Anything).
				Run(func(args mock.Arguments) {
					captured = args.Get(0).(*broker.RawMessage)
				}).
				Return(tt.publishErr)

			client := NewClient("amqp://localhost")
			client.publisher = pub

			id, err := client.Publish(context.Background(), tt.taskName, tt.opts)

			if tt.wantErr {
				require.Error(t, err)
				pub.AssertExpectations(t)
				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, id)
			if tt.opts.TaskID != "" {
				assert.Equal(t, tt.opts.TaskID, id)
			}

			require.NotNil(t, captured)
			assert.Equal(t, tt.wantQueue, captured.Queue)
			assert.Equal(t, tt.wantTaskHdr, captured.Headers["task"])
			assert.Equal(t, id, captured.Headers["id"])

			var raw []json.RawMessage
			require.NoError(t, json.Unmarshal(captured.Body, &raw))
			require.GreaterOrEqual(t, len(raw), 2)
			var args []any
			var kwargs map[string]any
			require.NoError(t, json.Unmarshal(raw[0], &args))
			require.NoError(t, json.Unmarshal(raw[1], &kwargs))
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, args)
			}
			if tt.wantKwargs != nil {
				assert.Equal(t, tt.wantKwargs, kwargs)
			}

			pub.AssertExpectations(t)
		})
	}
}

func TestClient_Connect_unsupportedScheme(t *testing.T) {
	client := NewClient("redis://localhost:6379")
	err := client.Connect()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported broker scheme "redis"`)
}

func TestClient_Connect_invalidURL(t *testing.T) {
	client := NewClient("://bad-url")
	err := client.Connect()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid broker URL")
}

func TestClient_Publish_generatesUniqueIDs(t *testing.T) {
	pub := &broker.MockPublisher{}
	pub.On("PublishMessage", mock.Anything).Return(nil)

	client := NewClient("amqp://localhost")
	client.publisher = pub
	ctx := context.Background()

	id1, err := client.Publish(ctx, "task", PublishOptions{})
	require.NoError(t, err)
	id2, err := client.Publish(ctx, "task", PublishOptions{})
	require.NoError(t, err)

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)
}

