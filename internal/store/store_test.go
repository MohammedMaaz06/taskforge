package store

import (
"os"
"testing"

"taskforge/pkg/task"
)

func setupTestStore(t *testing.T) (*SQLiteStore, func()) {
tmpFile := "test_tasks.db"
st, err := NewSQLiteStore(tmpFile)
if err != nil {
t.Fatalf("Failed to create test store: %v", err)
}

cleanup := func() {
st.Close()
os.Remove(tmpFile)
}

return st, cleanup
}

func TestSQLiteStore_SaveAndGet(t *testing.T) {
st, cleanup := setupTestStore(t)
defer cleanup()

t1 := task.NewTask("task-1", "test_job", []byte("hello"), 5)
if err := st.Save(t1); err != nil {
t.Fatalf("Failed to save task: %v", err)
}

got, err := st.Get("task-1")
if err != nil {
t.Fatalf("Failed to get task: %v", err)
}

if got.ID != t1.ID || got.Name != t1.Name || got.Priority != t1.Priority {
t.Errorf("Mismatch. Expected %+v, got %+v", t1, got)
}
}

func TestSQLiteStore_GetNotFound(t *testing.T) {
st, cleanup := setupTestStore(t)
defer cleanup()

_, err := st.Get("nonexistent")
if err == nil {
t.Error("Expected error for nonexistent task, got nil")
}
}

func TestSQLiteStore_UpdateStatus(t *testing.T) {
st, cleanup := setupTestStore(t)
defer cleanup()

t1 := task.NewTask("task-2", "update_job", []byte("data"), 1)
_ = st.Save(t1)

err := st.UpdateStatus("task-2", task.StatusCompleted, "")
if err != nil {
t.Fatalf("Failed to update status: %v", err)
}

got, _ := st.Get("task-2")
if got.Status != task.StatusCompleted {
t.Errorf("Expected status COMPLETED, got %s", got.Status)
}
}

func TestSQLiteStore_List(t *testing.T) {
st, cleanup := setupTestStore(t)
defer cleanup()

t1 := task.NewTask("task-10", "job_10", []byte("1"), 1)
t2 := task.NewTask("task-11", "job_11", []byte("2"), 2)

_ = st.Save(t1)
_ = st.Save(t2)

tasks, err := st.List()
if err != nil {
t.Fatalf("Failed to list tasks: %v", err)
}

if len(tasks) != 2 {
t.Errorf("Expected 2 tasks, got %d", len(tasks))
}
}

