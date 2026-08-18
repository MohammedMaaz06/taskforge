package task

import (
"testing"
"time"
)

func TestNewTask(t *testing.T) {
id := "task-1"
name := "send_email"
payload := []byte(`{"to":"user@example.com"}`)
priority := 1

task := NewTask(id, name, payload, priority)

if task.ID != id {
t.Errorf("expected ID %q, got %q", id, task.ID)
}
if task.Name != name {
t.Errorf("expected Name %q, got %q", name, task.Name)
}
if string(task.Payload) != string(payload) {
t.Errorf("expected Payload %s, got %s", payload, task.Payload)
}
if task.Status != StatusPending {
t.Errorf("expected status %q, got %q", StatusPending, task.Status)
}
if task.Priority != priority {
t.Errorf("expected priority %d, got %d", priority, task.Priority)
}
if task.MaxRetries != 3 {
t.Errorf("expected MaxRetries 3, got %d", task.MaxRetries)
}
}

func TestNextRetryDelay(t *testing.T) {
task := NewTask("task-2", "process_data", []byte(`{}`), 0)

// Retry 0: 2^0 = 1 second
if delay := task.NextRetryDelay(); delay != 1*time.Second {
t.Errorf("expected delay 1s for retry 0, got %v", delay)
}

// Retry 1: 2^1 = 2 seconds
task.CurrentRetry = 1
if delay := task.NextRetryDelay(); delay != 2*time.Second {
t.Errorf("expected delay 2s for retry 1, got %v", delay)
}

// Retry 2: 2^2 = 4 seconds
task.CurrentRetry = 2
if delay := task.NextRetryDelay(); delay != 4*time.Second {
t.Errorf("expected delay 4s for retry 2, got %v", delay)
}
}
