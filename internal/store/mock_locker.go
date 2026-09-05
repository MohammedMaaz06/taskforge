package store

import (
"context"
"sync"
"time"
)

type SharedLockState struct {
mu    sync.Mutex
locks map[string]string
}

func NewSharedLockState() *SharedLockState {
return &SharedLockState{
locks: make(map[string]string),
}
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

