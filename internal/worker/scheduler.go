package worker

import (
	"sync"

	"github.com/kgantsov/celerity/internal/broker"
)

type Delivery interface {
	Ack(multiple bool) error
	Body() []byte
}

type Scheduler interface {
	Schedule(msg *broker.RawMessage) error
	Stop() error
}

type TaskScheduler struct {
	JobQueue chan Job
	mut      sync.RWMutex
	wg       *sync.WaitGroup
}

func NewTaskScheduler(JobQueue chan Job) *TaskScheduler {
	var wg sync.WaitGroup

	return &TaskScheduler{JobQueue: JobQueue, wg: &wg}
}

func (s *TaskScheduler) Schedule(msg *broker.RawMessage) error {
	s.wg.Add(1)

	s.JobQueue <- Job{wg: s.wg, msg: msg}
	return nil
}

func (s *TaskScheduler) Stop() error {
	s.wg.Wait()
	return nil
}
