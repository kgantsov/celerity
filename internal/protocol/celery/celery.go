package celery

import (
	"errors"

	"github.com/kgantsov/celerity/internal/broker"
	"github.com/kgantsov/celerity/internal/task"
)

var ErrUnsupportedProtocolVersion = errors.New("unsupported protocol version")

type Protocol interface {
	ToTask(msg *broker.RawMessage) (*task.Task, error)
	ToRawMessage(*task.Task) (*broker.RawMessage, error)
	BuildReplyMessage(tk *task.Task, status string, result any) (*broker.RawMessage, error)
}

func NewProtocol(version string) (Protocol, error) {
	switch version {
	case "2.0":
		return &CeleryPtotocolV2{}, nil
	default:
		return nil, ErrUnsupportedProtocolVersion
	}
}
