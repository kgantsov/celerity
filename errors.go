package celerity

type RetryError struct {
	Err        error
	MaxRetries int8
}

func (e *RetryError) Error() string {
	return e.Err.Error()
}

func (e *RetryError) GetMaxRetries() int8 {
	return e.MaxRetries
}
