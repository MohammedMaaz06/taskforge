package scheduler

import (
"fmt"
"sync"
"taskforge/pkg/task"
)

type DeadLetterQueue struct {
mu    sync.Mutex
tasks []*task.Task
}

func NewDLQ() *DeadLetterQueue {
return &DeadLetterQueue{
tasks: make([]*task.Task, 0),
}
}

func (dlq *DeadLetterQueue) Store(t *task.Task, reason string) {
dlq.mu.Lock()
defer dlq.mu.Unlock()

t.Status = task.StatusDLQ
t.LastError = reason
dlq.tasks = append(dlq.tasks, t)
fmt.Printf("[DLQ ALERT] Task ID: %s permanently failed after %d retries. Reason: %s\n", t.ID, t.CurrentRetry, reason)
}

func (dlq *DeadLetterQueue) Size() int {
dlq.mu.Lock()
defer dlq.mu.Unlock()
return len(dlq.tasks)
}
