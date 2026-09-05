package worker

import (
"testing"
"time"

"taskforge/internal/store"
"taskforge/pkg/task"
)

func TestPool_TaskExecution(t *testing.T) {
pool := NewPool(2, nil, nil, nil, nil, nil)
pool.Start()

tk := &task.Task{ID: "test-task-1", Status: task.StatusPending}
if !pool.Submit(tk) {
t.Fatalf("failed to submit task")
}

time.Sleep(150 * time.Millisecond)
pool.Stop()

if tk.Status != task.StatusCompleted {
t.Fatalf("expected status completed, got %s", tk.Status)
}
}

func TestPool_DistributedLocking(t *testing.T) {
sharedState := store.NewSharedLockState()
locker := store.NewMemoryLocker(sharedState, "worker-instance-1")
pool := NewPool(2, nil, nil, nil, nil, locker)
pool.Start()

tk := &task.Task{ID: "locked-task", Status: task.StatusPending}
if !pool.Submit(tk) {
t.Fatalf("failed to submit task")
}

time.Sleep(150 * time.Millisecond)
pool.Stop()

if tk.Status != task.StatusCompleted {
t.Fatalf("expected task status completed with lock acquired, got %s", tk.Status)
}
}

