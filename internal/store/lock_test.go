package store

import (
"context"
"testing"
"time"
)

func TestMemoryLocker_Concurrency(t *testing.T) {
shared := NewSharedLockState()
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

