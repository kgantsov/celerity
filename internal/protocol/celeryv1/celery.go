package celeryv1

import (
	"encoding/json"
	"fmt"

	"github.com/kgantsov/celerity/internal/task"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CeleryDelivery struct {
	delivery amqp.Delivery
	acksLate bool
}

func (d *CeleryDelivery) Ack(multiple bool) error {
	// Only ack if acksLate is true, otherwise the broker will auto-ack the message
	if d.acksLate {
		return d.delivery.Ack(multiple)
	}

	return nil
}

func (d *CeleryDelivery) Nack(multiple bool) error {
	// Only nack if acksLate is true, otherwise the broker will auto-nack the message
	if d.acksLate {
		return d.delivery.Nack(multiple, true)
	}

	return nil
}

func (d *CeleryDelivery) SetAcksLate(acksLate bool) {
	d.acksLate = acksLate
}

// CeleryV1Payload represents the body array: [args, kwargs, embed]
type CeleryV1Payload struct {
	Args   []any          `json:"0"`
	Kwargs map[string]any `json:"1"`
}

// UnmarshalJSON handles decoding the top-level JSON array format
func (p *CeleryV1Payload) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if len(raw) > 0 {
		if err := json.Unmarshal(raw[0], &p.Args); err != nil {
			return err
		}
	}
	if len(raw) > 1 {
		if err := json.Unmarshal(raw[1], &p.Kwargs); err != nil {
			return err
		}
	}
	return nil
}

// ParseCeleryDelivery parses an AMQP delivery into a Task struct
func ParseCeleryDelivery(d amqp.Delivery) (*task.Task, error) {
	task := &task.Task{
		Delivery: &CeleryDelivery{delivery: d},
	}

	if id, ok := d.Headers["id"].(string); ok {
		task.ID = id
	}
	if taskName, ok := d.Headers["task"].(string); ok {
		task.Task = taskName
	}

	var payload CeleryV1Payload
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode v1 payload: %w", err)
	}

	task.Args = payload.Args
	task.Kwargs = payload.Kwargs

	return task, nil
}
