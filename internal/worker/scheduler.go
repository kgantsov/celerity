package worker

import (
	"sync"

	"github.com/kgantsov/celerity/internal/task"
)

type Delivery interface {
	Ack(multiple bool) error
	Body() []byte
}

type Scheduler interface {
	Schedule(task *task.Task) error
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

func (s *TaskScheduler) Schedule(task *task.Task) error {
	s.wg.Add(1)

	s.JobQueue <- Job{wg: s.wg, task: task}
	return nil
}

func (s *TaskScheduler) Stop() error {
	s.wg.Wait()
	return nil
}
