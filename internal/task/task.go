package task

type Delivery interface {
	Ack(multiple bool) error
	Nack(multiple bool) error
	SetAcksLate(acksLate bool)
}

type Task struct {
	ID       string
	Task     string
	Args     []any
	Kwargs   map[string]any
	Delivery Delivery
}
