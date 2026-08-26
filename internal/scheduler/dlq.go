package scheduler

import (
"sync"

"taskforge/pkg/task"
)

type DeadLetterQueue struct {
mu    sync.Mutex
tasks []*task.Task
}

func NewDeadLetterQueue() *DeadLetterQueue {
return &DeadLetterQueue{
tasks: make([]*task.Task, 0),
}
}

func NewDLQ() *DeadLetterQueue {
return NewDeadLetterQueue()
}

func (dlq *DeadLetterQueue) Store(t *task.Task, reason string) {
dlq.mu.Lock()
defer dlq.mu.Unlock()
if t != nil {
t.LastError = reason
t.Status = task.StatusDLQ
}
dlq.tasks = append(dlq.tasks, t)
}

func (dlq *DeadLetterQueue) Add(t *task.Task) {
dlq.Store(t, "")
}

func (dlq *DeadLetterQueue) GetTasks() []*task.Task {
dlq.mu.Lock()
defer dlq.mu.Unlock()
result := make([]*task.Task, len(dlq.tasks))
copy(result, dlq.tasks)
return result
}

func (dlq *DeadLetterQueue) GetAll() []*task.Task {
return dlq.GetTasks()
}

func (dlq *DeadLetterQueue) Size() int {
dlq.mu.Lock()
defer dlq.mu.Unlock()
return len(dlq.tasks)
}

func (dlq *DeadLetterQueue) Remove(id string) (*task.Task, bool) {
dlq.mu.Lock()
defer dlq.mu.Unlock()

for i, t := range dlq.tasks {
if t != nil && t.ID == id {
removedTask := dlq.tasks[i]
dlq.tasks = append(dlq.tasks[:i], dlq.tasks[i+1:]...)
return removedTask, true
}
}
return nil, false
}

