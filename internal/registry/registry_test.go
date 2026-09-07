package registry

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegister(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		fn         any
		paramNames []string
		wantErr    bool
	}{
		{
			name:       "valid function no params",
			taskName:   "no.params",
			fn:         func() error { return nil },
			paramNames: []string{},
		},
		{
			name:       "valid function with params",
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
			fn:         func(a, b int) error { return nil },
			paramNames: []string{"a"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewTaskRegistry()
			err := r.Register(tt.taskName, tt.fn, tt.paramNames)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExecute_positionalArgs(t *testing.T) {
	tests := []struct {
		name       string
		taskName   string
		fn         any
		paramNames []string
		args       []any
		kwargs     map[string]any
		wantResult []any
		wantErr    error
	}{
		{
			name:       "int addition",
			taskName:   "add",
			fn:         func(a, b int) (int, error) { return a + b, nil },
			paramNames: []string{"a", "b"},
			args:       []any{float64(3), float64(4)},
			kwargs:     map[string]any{},
			wantResult: []any{7, nil},
		},
		{
			name:       "string concat",
			taskName:   "concat",
			fn:         func(s1, s2 string) (string, error) { return s1 + s2, nil },
			paramNames: []string{"s1", "s2"},
			args:       []any{"hello", " world"},
			kwargs:     map[string]any{},
			wantResult: []any{"hello world", nil},
		},
		{
			name:       "float64 to float32 coercion",
			taskName:   "sum",
			fn:         func(a float32) (float32, error) { return a * 2, nil },
			paramNames: []string{"a"},
			args:       []any{float64(1.5)},
			kwargs:     map[string]any{},
			wantResult: []any{float32(3.0), nil},
		},
		{
			name:       "too many positional args",
			taskName:   "add",
			fn:         func(a int) (int, error) { return a, nil },
			paramNames: []string{"a"},
			args:       []any{float64(1), float64(2)},
			kwargs:     map[string]any{},
			wantErr:    ErrTooManyArguments,
		},
		{
			name:       "task not found",
			taskName:   "missing",
			fn:         nil,
			paramNames: nil,
			args:       []any{},
			kwargs:     map[string]any{},
			wantErr:    ErrTaskNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewTaskRegistry()
			if tt.fn != nil {
				require.NoError(t, r.Register(tt.taskName, tt.fn, tt.paramNames))
			}

			result, err := r.Execute(tt.taskName, tt.args, tt.kwargs)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantResult, result)
			}
		})
	}
}

func TestExecute_kwargs(t *testing.T) {
	tests := []struct {
		name       string
		fn         any
		paramNames []string
		args       []any
		kwargs     map[string]any
		wantResult []any
		wantErr    error
	}{
		{
			name:       "all kwargs",
			fn:         func(a, b int) (int, error) { return a - b, nil },
			paramNames: []string{"a", "b"},
			args:       []any{},
			kwargs:     map[string]any{"a": float64(10), "b": float64(3)},
			wantResult: []any{7, nil},
		},
		{
			name:       "mixed positional and kwargs",
			fn:         func(a, b int) (int, error) { return a * b, nil },
			paramNames: []string{"a", "b"},
			args:       []any{float64(5)},
			kwargs:     map[string]any{"b": float64(4)},
			wantResult: []any{20, nil},
		},
		{
			name:       "missing kwarg",
			fn:         func(a, b int) (int, error) { return a + b, nil },
			paramNames: []string{"a", "b"},
			args:       []any{float64(1)},
			kwargs:     map[string]any{},
			wantErr:    ErrMissingArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.Register("task", tt.fn, tt.paramNames))

			result, err := r.Execute("task", tt.args, tt.kwargs)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantResult, result)
			}
		})
	}
}

func TestExecute_taskReturnsError(t *testing.T) {
	sentinel := errors.New("task failed")
	r := NewTaskRegistry()
	require.NoError(t, r.Register("fail", func() error { return sentinel }, []string{}))

	result, err := r.Execute("fail", []any{}, map[string]any{})
	assert.ErrorIs(t, err, sentinel)
	assert.NotNil(t, result)
}
