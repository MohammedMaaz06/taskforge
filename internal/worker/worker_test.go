package worker

import (
"testing"
"time"

"taskforge/internal/scheduler"
"taskforge/pkg/task"
)

func TestWorkerPool_SuccessfulTask(t *testing.T) {
dlq := scheduler.NewDLQ()
wp := NewWorkerPool(2, 5, dlq)
wp.Start()

tsk := task.NewTask("task-w1", "process_images", []byte(`{}`), 1)
wp.Submit(tsk)

time.Sleep(300 * time.Millisecond)
wp.Stop()

if tsk.Status != task.StatusCompleted {
t.Errorf("expected task status %q, got %q", task.StatusCompleted, tsk.Status)
}
}

func TestWorkerPool_DLQRouting(t *testing.T) {
dlq := scheduler.NewDLQ()
wp := NewWorkerPool(1, 5, dlq)
wp.Start()

tsk := task.NewTask("task-w2", "flaky_third_party_api", []byte(`{}`), 1)
tsk.MaxRetries = 1

wp.Submit(tsk)

time.Sleep(1500 * time.Millisecond)
wp.Stop()

if tsk.Status != task.StatusDLQ {
t.Errorf("expected task status %q, got %q", task.StatusDLQ, tsk.Status)
}

if dlq.Size() != 1 {
t.Errorf("expected DLQ size 1, got %d", dlq.Size())
}
}
