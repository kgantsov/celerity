package celery

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/task"
)

type CeleryPtotocolV2 struct{}

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

type Reply struct {
	TaskID    string        `json:"task_id"`
	Status    string        `json:"status"`
	Result    any           `json:"result"`
	Traceback interface{}   `json:"traceback"`
	Children  []interface{} `json:"children"`
}

// ParseCeleryDelivery parses an AMQP delivery into a Task struct
func (p *CeleryPtotocolV2) ToTask(msg *broker.RawMessage) (*task.Task, error) {
	task := &task.Task{
		QueueName:     msg.Queue,
		Delivery:      msg.Delivery,
		ReplyTo:       msg.ReplyTo,
		CorrelationId: msg.CorrelationID,
	}

	if id, ok := msg.Headers["id"].(string); ok {
		task.ID = id
	}
	if taskName, ok := msg.Headers["task"].(string); ok {
		task.Task = taskName
	}

	log.Printf("HEADERS: %v\n", msg.Headers)

	// read retry count and max retries from headers if present
	if retryCount, ok := msg.Headers["retries"].(int8); ok {
		task.RetryCount = int8(retryCount)
	}

	var payload CeleryV2Payload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode v1 payload: %w", err)
	}

	task.Args = payload.Args
	task.Kwargs = payload.Kwargs

	return task, nil
}

func (p *CeleryPtotocolV2) ToRawMessage(tk *task.Task) (*broker.RawMessage, error) {
	payload := CeleryV2Payload{
		Args:   tk.Args,
		Kwargs: tk.Kwargs,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode v2 payload: %w", err)
	}

	headers := map[string]any{
		"id":      tk.ID,
		"task":    tk.Task,
		"retries": tk.RetryCount,
	}

	return &broker.RawMessage{
		Queue:   tk.QueueName,
		Body:    body,
		Headers: headers,
	}, nil
}

func (p *CeleryPtotocolV2) BuildReplyMessage(
	tk *task.Task, status string, result any,
) (*broker.RawMessage, error) {
	if s, ok := result.([]any); ok && len(s) == 1 {
		result = s[0]
	}
	reply := Reply{
		TaskID:    tk.CorrelationId,
		Status:    status,
		Result:    result,
		Traceback: nil,
		Children:  []interface{}{},
	}
	resultBytes, err := json.Marshal(reply)

	if err != nil {
		return nil, err
	}

	return &broker.RawMessage{
		Queue: tk.ReplyTo,
		Body:  resultBytes,
	}, nil
}
