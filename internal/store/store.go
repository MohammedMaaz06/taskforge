package store

import (
"errors"

"taskforge/pkg/task"
)

var ErrTaskNotFound = errors.New("task not found")

type Store interface {
Save(t *task.Task) error
Get(id string) (*task.Task, error)
List(statusFilter ...string) ([]*task.Task, error)
UpdateStatus(id string, status task.Status, lastErr string) error
Close() error
}

