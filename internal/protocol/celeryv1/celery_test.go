package celeryv1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestCeleryV1Payload_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantArgs   []any
		wantKwargs map[string]any
		wantErr    bool
	}{
		{
			name:       "args and kwargs",
			input:      `[[1, 2], {"key": "val"}, {}]`,
			wantArgs:   []any{float64(1), float64(2)},
			wantKwargs: map[string]any{"key": "val"},
		},
		{
			name:       "empty args and kwargs",
			input:      `[[], {}, {}]`,
			wantArgs:   []any{},
			wantKwargs: map[string]any{},
		},
		{
			name:       "only args",
			input:      `[[1, 2]]`,
			wantArgs:   []any{float64(1), float64(2)},
			wantKwargs: nil,
		},
		{
			name:    "invalid json",
			input:   `not json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p CeleryV1Payload
			err := json.Unmarshal([]byte(tt.input), &p)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantArgs, p.Args)
			assert.Equal(t, tt.wantKwargs, p.Kwargs)
		})
	}
}

func TestParseCeleryDelivery(t *testing.T) {
	tests := []struct {
		name       string
		headers    amqp.Table
		body       string
		wantID     string
		wantTask   string
		wantArgs   []any
		wantKwargs map[string]any
		wantErr    bool
	}{
		{
			name:       "full valid delivery",
			headers:    amqp.Table{"id": "abc-123", "task": "hello.add"},
			body:       `[[5, 3], {}, {}]`,
			wantID:     "abc-123",
			wantTask:   "hello.add",
			wantArgs:   []any{float64(5), float64(3)},
			wantKwargs: map[string]any{},
		},
		{
			name:       "kwargs only",
			headers:    amqp.Table{"id": "xyz", "task": "hello.greet"},
			body:       `[[], {"name": "Alice"}, {}]`,
			wantID:     "xyz",
			wantTask:   "hello.greet",
			wantArgs:   []any{},
			wantKwargs: map[string]any{"name": "Alice"},
		},
		{
			name:    "invalid body",
			headers: amqp.Table{"id": "1", "task": "bad"},
			body:    `not json`,
			wantErr: true,
		},
		{
			name:       "missing headers",
			headers:    amqp.Table{},
			body:       `[[], {}, {}]`,
			wantID:     "",
			wantTask:   "",
			wantArgs:   []any{},
			wantKwargs: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := amqp.Delivery{
				Headers: tt.headers,
				Body:    []byte(tt.body),
			}

			task, err := ParseCeleryDelivery(d)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantID, task.ID)
			assert.Equal(t, tt.wantTask, task.Task)
			assert.Equal(t, tt.wantArgs, task.Args)
			assert.Equal(t, tt.wantKwargs, task.Kwargs)
			assert.NotNil(t, task.Delivery)
		})
	}
}
