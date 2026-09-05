package store

import (
"context"
"sync"
"testing"
"time"
)

type SharedLockState struct {
mu    sync.Mutex
locks map[string]string
}

type MemoryLocker struct {
state *SharedLockState
owner string
}

func NewMemoryLocker(state *SharedLockState, owner string) *MemoryLocker {
return &MemoryLocker{
state: state,
owner: owner,
}
}

func (m *MemoryLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
m.state.mu.Lock()
defer m.state.mu.Unlock()
if _, exists := m.state.locks[key]; exists {
return false, nil
}
m.state.locks[key] = m.owner
return true, nil
}

func (m *MemoryLocker) Release(ctx context.Context, key string) error {
m.state.mu.Lock()
defer m.state.mu.Unlock()
if owner, exists := m.state.locks[key]; exists && owner == m.owner {
delete(m.state.locks, key)
return nil
}
return ErrLockAcquireFailed
}

func TestMemoryLocker_Concurrency(t *testing.T) {
shared := &SharedLockState{locks: make(map[string]string)}
l1 := NewMemoryLocker(shared, "worker-1")
l2 := NewMemoryLocker(shared, "worker-2")
ctx := context.Background()

acquired, err := l1.Acquire(ctx, "task-123", 5*time.Second)
if err != nil || !acquired {
t.Fatalf("worker-1 failed to acquire lock: %v", err)
}

acquired2, _ := l2.Acquire(ctx, "task-123", 5*time.Second)
if acquired2 {
t.Fatalf("worker-2 should not acquire locked key")
}

if err := l1.Release(ctx, "task-123"); err != nil {
t.Fatalf("worker-1 failed to release lock: %v", err)
}

acquired2Again, _ := l2.Acquire(ctx, "task-123", 5*time.Second)
if !acquired2Again {
t.Fatalf("worker-2 failed to acquire lock after release")
}
}

