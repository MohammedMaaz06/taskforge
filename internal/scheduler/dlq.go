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

func (dlq *DeadLetterQueue) Add(t *task.Task) {
dlq.mu.Lock()
defer dlq.mu.Unlock()
t.Status = task.StatusDLQ
dlq.tasks = append(dlq.tasks, t)
}

func (dlq *DeadLetterQueue) List() []*task.Task {
dlq.mu.Lock()
defer dlq.mu.Unlock()
copied := make([]*task.Task, len(dlq.tasks))
copy(copied, dlq.tasks)
return copied
}

func (dlq *DeadLetterQueue) Remove(taskID string) *task.Task {
dlq.mu.Lock()
defer dlq.mu.Unlock()
for i, t := range dlq.tasks {
if t.ID == taskID {
dlq.tasks = append(dlq.tasks[:i], dlq.tasks[i+1:]...)
return t
}
}
return nil
}

func (dlq *DeadLetterQueue) Clear() {
dlq.mu.Lock()
defer dlq.mu.Unlock()
dlq.tasks = make([]*task.Task, 0)
}

func (dlq *DeadLetterQueue) Size() int {
dlq.mu.Lock()
defer dlq.mu.Unlock()
return len(dlq.tasks)
}

