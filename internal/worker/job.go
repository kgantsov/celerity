package worker

import (
	"sync"

	"github.com/kgantsov/celerity/internal/task"
)

// Job represents the job to be run
type Job struct {
	task *task.Task
	wg   *sync.WaitGroup
}

func NewJob(t *task.Task, wg *sync.WaitGroup) Job {
	return Job{task: t, wg: wg}
}

func (j *Job) GetTask() *task.Task {
	return j.task
}

func (j *Job) GetWaitGroup() *sync.WaitGroup {
	return j.wg
}
