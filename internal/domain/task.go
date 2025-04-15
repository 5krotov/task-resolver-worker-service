package domain

import (
	entity "github.com/5krotov/task-resolver-pkg/entity/v1"
)

const (
	StatusCreated    = 0
	StatusPending    = 1
	StatusInProgress = 2
	StatusDone       = 3
)

// Re-alias the external v1.Task and entity.Status
type Task = entity.Task
type Status = entity.Status
