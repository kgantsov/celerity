package backend

import (
	"testing"

	"github.com/kgantsov/celerity/internal/task"
	"github.com/stretchr/testify/assert"
)

// ResultKey must stay ReplyTo for the AMQP backend: SetResult publishes
// with routing key = ResultKey, and a real rpc://-style Celery client only
// consumes from the reply queue it declared itself, named by ReplyTo (its
// thread_oid) — not by the task/correlation id.
func TestRabbitMQBackend_ResultKey(t *testing.T) {
	b := &RabbitMQBackend{}

	assert.Equal(t, "reply-queue", b.ResultKey(&task.Task{
		ID: "task-1", CorrelationId: "task-1", ReplyTo: "reply-queue",
	}))
	assert.Equal(t, "", b.ResultKey(&task.Task{
		ID: "task-1", CorrelationId: "task-1", ReplyTo: "",
	}))
}
