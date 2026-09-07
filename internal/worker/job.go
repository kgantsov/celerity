package worker

import (
	"sync"

	"github.com/kgantsov/celerity/internal/broker"
)

// Job represents the job to be run
type Job struct {
	msg *broker.RawMessage
	wg  *sync.WaitGroup
}

func NewJob(msg *broker.RawMessage, wg *sync.WaitGroup) Job {
	return Job{msg: msg, wg: wg}
}

func (j *Job) GetMessage() *broker.RawMessage {
	return j.msg
}

func (j *Job) GetWaitGroup() *sync.WaitGroup {
	return j.wg
}
