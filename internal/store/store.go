package store

import (
	"errors"
	"taskforge/pkg/task"
)

var ErrTaskNotFound = errors.New("task not found")

type TaskStore interface {
	Save(t *task.Task) error
	Get(id string) (*task.Task, error)
	UpdateStatus(id string, status task.Status, errMessage string) error
	Close() error
}
