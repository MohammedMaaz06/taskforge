package worker

import (
"testing"
"time"

"taskforge/internal/scheduler"
"taskforge/pkg/task"
)

func TestWorkerPool(t *testing.T) {
sched := scheduler.NewTaskScheduler()
dlq := scheduler.NewDeadLetterQueue()

pool := NewPool(2, sched, nil, dlq, nil)
pool.Start()

testTask := &task.Task{
ID:         "1",
Name:       "test-task",
Priority:   5,
MaxRetries: 3,
}

sched.Push(testTask)

time.Sleep(200 * time.Millisecond)

pool.Stop()

if testTask.Status != task.StatusCompleted {
t.Errorf("Expected status COMPLETED, got %s", testTask.Status)
}
}

