package scheduler

import (
"sync"
"taskforge/pkg/task"
)

type DeadLetterQueue struct {
mu    sync.Mutex
tasks map[string]*task.Task
}

func NewDLQ() *DeadLetterQueue {
return &DeadLetterQueue{
tasks: make(map[string]*task.Task),
}
}

func (d *DeadLetterQueue) Add(t *task.Task) {
d.mu.Lock()
defer d.mu.Unlock()
d.tasks[t.ID] = t
}

func (d *DeadLetterQueue) Store(t *task.Task, reason string) {
if t != nil && reason != "" {
t.LastError = reason
}
d.Add(t)
}

func (d *DeadLetterQueue) GetAll() []*task.Task {
d.mu.Lock()
defer d.mu.Unlock()
list := make([]*task.Task, 0, len(d.tasks))
for _, t := range d.tasks {
list = append(list, t)
}
return list
}

func (d *DeadLetterQueue) Remove(id string) (*task.Task, bool) {
d.mu.Lock()
defer d.mu.Unlock()
t, exists := d.tasks[id]
if exists {
delete(d.tasks, id)
}
return t, exists
}
