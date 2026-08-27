package scheduler

import (
"testing"

"taskforge/pkg/task"
)

func TestPriorityQueue(t *testing.T) {
ts := NewTaskScheduler()

t1 := &task.Task{ID: "1", Priority: 1}
t2 := &task.Task{ID: "2", Priority: 10}
t3 := &task.Task{ID: "3", Priority: 5}

ts.Push(t1)
ts.Push(t2)
ts.Push(t3)

if ts.Size() != 3 {
t.Errorf("Expected queue size 3, got %d", ts.Size())
}

popped, err := ts.Pop()
if err != nil {
t.Fatalf("Unexpected error: %v", err)
}

if popped.ID != "2" {
t.Errorf("Expected highest priority task 2, got %s", popped.ID)
}
}

