package celeryv2

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/kgantsov/celerity/internal/task"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CeleryDelivery struct {
	delivery amqp.Delivery
}

func (d *CeleryDelivery) Ack(multiple bool) error {
	return d.delivery.Ack(multiple)
}

func (d *CeleryDelivery) Nack(multiple bool) error {
	return d.delivery.Nack(multiple, true)
}

// CeleryV2Payload represents the body array: [args, kwargs, embed]
type CeleryV2Payload struct {
	Args   []any          `json:"0"`
	Kwargs map[string]any `json:"1"`
}

// UnmarshalJSON handles decoding the top-level JSON array format
func (p *CeleryV2Payload) UnmarshalJSON(data []byte) error {
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
		QueueName:     d.RoutingKey,
		Delivery:      &CeleryDelivery{delivery: d},
		ReplyTo:       d.ReplyTo,
		CorrelationId: d.CorrelationId,
	}

	if id, ok := d.Headers["id"].(string); ok {
		task.ID = id
	}
	if taskName, ok := d.Headers["task"].(string); ok {
		task.Task = taskName
	}

	log.Printf("HEADERS: %v\n", d.Headers)

	// read retry count and max retries from headers if present
	if retryCount, ok := d.Headers["retries"].(int8); ok {
		task.RetryCount = int8(retryCount)
	}

	var payload CeleryV2Payload
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode v1 payload: %w", err)
	}

	task.Args = payload.Args
	task.Kwargs = payload.Kwargs

	return task, nil
}

type Reply struct {
	TaskID    string        `json:"task_id"`
	Status    string        `json:"status"`
	Result    any           `json:"result"`
	Traceback interface{}   `json:"traceback"`
	Children  []interface{} `json:"children"`
}

func BuildCeleryReplyPayload(taskID string, status string, result any) ([]byte, error) {
	if s, ok := result.([]any); ok && len(s) == 1 {
		result = s[0]
	}
	reply := Reply{
		TaskID:    taskID,
		Status:    status,
		Result:    result,
		Traceback: nil,
		Children:  []interface{}{},
	}
	resultBytes, err := json.Marshal(reply)

	if err != nil {
		return nil, err
	}

	return resultBytes, nil
}
