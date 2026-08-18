package store

import (
"testing"

"taskforge/pkg/task"
)

func setupTestStore(t *testing.T) *SQLiteStore {
store, err := NewSQLiteStore(":memory:")
if err != nil {
t.Fatalf("failed to create in-memory sqlite store: %v", err)
}
t.Cleanup(func() {
store.Close()
})
return store
}

func TestSQLiteStore_SaveAndGet(t *testing.T) {
s := setupTestStore(t)

tsk := task.NewTask("task-101", "process_payment", []byte(`{"amount":100}`), 1)

if err := s.Save(tsk); err != nil {
t.Fatalf("failed to save task: %v", err)
}

fetched, err := s.Get("task-101")
if err != nil {
t.Fatalf("failed to get task: %v", err)
}

if fetched.ID != tsk.ID {
t.Errorf("expected ID %q, got %q", tsk.ID, fetched.ID)
}
if fetched.Name != tsk.Name {
t.Errorf("expected Name %q, got %q", tsk.Name, fetched.Name)
}
if fetched.Status != task.StatusPending {
t.Errorf("expected Status %q, got %q", task.StatusPending, fetched.Status)
}
}

func TestSQLiteStore_GetNotFound(t *testing.T) {
s := setupTestStore(t)

_, err := s.Get("non-existent-id")
if err != ErrTaskNotFound {
t.Errorf("expected ErrTaskNotFound, got %v", err)
}
}

func TestSQLiteStore_UpdateStatus(t *testing.T) {
s := setupTestStore(t)

tsk := task.NewTask("task-102", "send_webhook", []byte(`{}`), 2)
if err := s.Save(tsk); err != nil {
t.Fatalf("failed to save initial task: %v", err)
}

if err := s.UpdateStatus("task-102", task.StatusFailed, "timeout connection"); err != nil {
t.Fatalf("failed to update status: %v", err)
}

updated, err := s.Get("task-102")
if err != nil {
t.Fatalf("failed to fetch updated task: %v", err)
}

if updated.Status != task.StatusFailed {
t.Errorf("expected status %q, got %q", task.StatusFailed, updated.Status)
}
if updated.LastError != "timeout connection" {
t.Errorf("expected last error %q, got %q", "timeout connection", updated.LastError)
}
}
