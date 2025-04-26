package future

import (
	v1 "github.com/5krotov/task-resolver-pkg/api/v1"
)

type TaskFuture struct {
	Task   v1.StartTaskRequest
	Err    chan error
	Result chan v1.UpdateStatusRequest
}

func NewTaskFuture(task v1.StartTaskRequest) *TaskFuture {
	return &TaskFuture{Task: task, Err: make(chan error), Result: make(chan v1.UpdateStatusRequest)}
}
