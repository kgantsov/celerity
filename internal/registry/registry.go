package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var ErrTaskNotFound = errors.New("task not found in registry")
var ErrTooManyArguments = errors.New("too many arguments provided for task")
var ErrInvalidArgumentType = errors.New("invalid argument type for task")
var ErrMissingArgument = errors.New("missing required argument for task")

type Task struct {
	fn         reflect.Value
	paramNames []string // Maps kwargs keys -> positional index
}

type TaskRegistry struct {
	tasks map[string]Task
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{tasks: make(map[string]Task)}
}

// Register registers a function along with its expected parameter names (in order)
func (r *TaskRegistry) Register(name string, fn any, paramNames []string) error {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return fmt.Errorf("task %q is not a function", name)
	}

	if v.Type().NumIn() != len(paramNames) {
		return fmt.Errorf(
			"task %q expects %d params, but %d names were provided",
			name,
			v.Type().NumIn(),
			len(paramNames),
		)
	}

	r.tasks[name] = Task{
		fn:         v,
		paramNames: paramNames,
	}
	return nil
}

func (r *TaskRegistry) Execute(
	taskName string, rawArgs []any, rawKwargs map[string]any,
) ([]any, error) {
	task, exists := r.tasks[taskName]
	if !exists {
		return nil, ErrTaskNotFound
	}

	fnType := task.fn.Type()
	numArgs := fnType.NumIn()
	inArgs := make([]reflect.Value, numArgs)

	// Fill positional arguments first
	for i := 0; i < len(rawArgs); i++ {
		if i >= numArgs {
			return nil, ErrTooManyArguments
		}
		targetType := fnType.In(i)
		val, err := convertValue(rawArgs[i], targetType)
		if err != nil {
			return nil, ErrInvalidArgumentType
		}
		inArgs[i] = val
	}

	// Fill keyword arguments by matching parameter names
	for i := len(rawArgs); i < numArgs; i++ {
		paramName := task.paramNames[i]
		kwVal, found := rawKwargs[paramName]
		if !found {
			return nil, ErrMissingArgument
		}

		targetType := fnType.In(i)
		val, err := convertValue(kwVal, targetType)
		if err != nil {
			return nil, ErrInvalidArgumentType
		}
		inArgs[i] = val
	}

	results := task.fn.Call(inArgs)

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	var out []any
	var taskErr error
	for _, res := range results {
		if res.Type().Implements(errorType) {
			if !res.IsNil() {
				taskErr = res.Interface().(error)
			}
		} else {
			out = append(out, res.Interface())
		}
	}

	return out, taskErr
}

// convertValue converts dynamic JSON values (float64, maps, slices) to target reflect.Types
func convertValue(val any, targetType reflect.Type) (reflect.Value, error) {
	if val == nil {
		return reflect.Zero(targetType), nil
	}

	srcVal := reflect.ValueOf(val)

	// Exact type match
	if srcVal.Type().AssignableTo(targetType) {
		return srcVal, nil
	}

	// JSON unmarshals numbers as float64. Convert numbers to Go integer/float types.
	if srcVal.Kind() == reflect.Float64 {
		f := srcVal.Float()
		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return reflect.ValueOf(int(f)).Convert(targetType), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return reflect.ValueOf(uint(f)).Convert(targetType), nil
		case reflect.Float32, reflect.Float64:
			return srcVal.Convert(targetType), nil
		}
	}

	// Fallback to JSON re-encoding for complex types (structs, maps, slices)
	byteData, err := json.Marshal(val)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("cannot marshal argument for conversion: %w", err)
	}

	newPtr := reflect.New(targetType)
	if err := json.Unmarshal(byteData, newPtr.Interface()); err != nil {
		return reflect.Value{}, fmt.Errorf("cannot unmarshal %T into target type %s", val, targetType)
	}

	return newPtr.Elem(), nil
}
