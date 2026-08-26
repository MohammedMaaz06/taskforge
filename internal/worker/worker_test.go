package worker

import (
"testing"
"time"

"taskforge/internal/scheduler"
"taskforge/pkg/task"
)

func TestWorkerPool(t *testing.T) {
dlq := scheduler.NewDeadLetterQueue()
// Pass nil for SQLiteStore in unit tests
pool := NewWorkerPool(2, 10, dlq, nil)
pool.Start()
defer pool.Stop()

t1 := task.NewTask("1", "test-task", []byte(`{}`), 5)
pool.Submit(t1)

time.Sleep(100 * time.Millisecond)
}

