package store

import (
"fmt"
"sync"
"taskforge/pkg/task"
)

type MemoryStore struct {
mu    sync.RWMutex
tasks map[string]*task.Task
}

func NewMemoryStore() *MemoryStore {
return &MemoryStore{
tasks: make(map[string]*task.Task),
}
}

func (s *MemoryStore) Save(t *task.Task) {
s.mu.Lock()
defer s.mu.Unlock()
s.tasks[t.ID] = t
}

func (s *MemoryStore) Get(id string) (*task.Task, error) {
s.mu.RLock()
defer s.mu.RUnlock()
t, exists := s.tasks[id]
if !exists {
return nil, fmt.Errorf("task with id %s not found", id)
}
return t, nil
}
