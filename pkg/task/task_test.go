package task

import (
"testing"
)

func TestNewTask(t *testing.T) {
title := "Process Batch Job"
payload := `{"batch_id": 42}`

task := New(title, payload)

if task.Title != title {
t.Errorf("expected title %q, got %q", title, task.Title)
}
if task.Payload != payload {
t.Errorf("expected payload %q, got %q", payload, task.Payload)
}
if task.Status != StatusPending {
t.Errorf("expected initial status %q, got %q", StatusPending, task.Status)
}
if task.ID == "" {
t.Error("expected non-empty task ID")
}
}

func TestTaskStatusTransitions(t *testing.T) {
task := New("Sample Task", "{}")

task.MarkProcessing()
if task.Status != StatusProcessing {
t.Errorf("expected status %q, got %q", StatusProcessing, task.Status)
}

task.MarkCompleted()
if task.Status != StatusCompleted {
t.Errorf("expected status %q, got %q", StatusCompleted, task.Status)
}
if task.CompletedAt == nil {
t.Error("expected CompletedAt to be set")
}
}

func TestTaskValidation(t *testing.T) {
tests := []struct {
name    string
title   string
wantErr bool
}{
{"valid task", "Valid Title", false},
{"empty title", "", true},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
task := New(tt.title, "{}")
err := task.Validate()

if tt.wantErr && err == nil {
t.Error("expected error for invalid task, got nil")
}
if !tt.wantErr && err != nil {
t.Errorf("unexpected error: %v", err)
}
})
}
}
