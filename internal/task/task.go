package task

type Delivery interface {
	Ack(multiple bool) error
	Nack(multiple bool) error
}

// Retryable is implemented by errors that want to be retried. The worker
// checks for this interface; plain errors are not retried.
type Retryable interface {
	error
	GetMaxRetries() int8
}

type Task struct {
	QueueName     string
	ReplyTo       string
	CorrelationId string
	ID            string
	Task          string
	Args          []any
	Kwargs        map[string]any
	Delivery      Delivery
	RetryCount    int8
}
