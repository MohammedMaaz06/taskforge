package store

import (
"path/filepath"
"testing"

"taskforge/pkg/task"
)

func setupTestStore(t *testing.T) *SQLiteStore {
tmpDir := t.TempDir()
dbPath := filepath.Join(tmpDir, "test.db")

st, err := NewSQLiteStore(dbPath)
if err != nil {
t.Fatalf("failed to create sqlite store: %v", err)
}

return st
}

func TestSQLiteStore_SaveAndGet(t *testing.T) {
st := setupTestStore(t)
defer st.Close()

tsk := task.NewTask("task-1", "test_job", []byte("hello"), 1)

if err := st.Save(tsk); err != nil {
t.Fatalf("failed to save task: %v", err)
}

retrieved, err := st.Get("task-1")
if err != nil {
t.Fatalf("failed to get task: %v", err)
}

if retrieved == nil {
t.Fatal("expected task, got nil")
}

if retrieved.ID != tsk.ID || retrieved.Name != tsk.Name {
t.Errorf("mismatch: got %+v, want %+v", retrieved, tsk)
}
}

func TestSQLiteStore_GetNotFound(t *testing.T) {
st := setupTestStore(t)
defer st.Close()

_, err := st.Get("non-existent-id")
if err != ErrTaskNotFound {
t.Errorf("expected ErrTaskNotFound, got %v", err)
}
}

func TestSQLiteStore_UpdateStatus(t *testing.T) {
st := setupTestStore(t)
defer st.Close()

tsk := task.NewTask("task-2", "update_job", []byte("data"), 2)
_ = st.Save(tsk)

if err := st.UpdateStatus("task-2", task.StatusCompleted, ""); err != nil {
t.Fatalf("failed to update status: %v", err)
}

retrieved, _ := st.Get("task-2")
if retrieved.Status != task.StatusCompleted {
t.Errorf("expected status %s, got %s", task.StatusCompleted, retrieved.Status)
}
}

func TestSQLiteStore_List(t *testing.T) {
st := setupTestStore(t)
defer st.Close()

t1 := task.NewTask("task-10", "job_10", []byte("1"), 1)
t2 := task.NewTask("task-11", "job_11", []byte("2"), 1)
t2.Status = task.StatusCompleted

_ = st.Save(t1)
_ = st.Save(t2)

// Test listing all
allTasks, err := st.List("")
if err != nil {
t.Fatalf("failed to list all tasks: %v", err)
}
if len(allTasks) != 2 {
t.Errorf("expected 2 tasks, got %d", len(allTasks))
}

// Test filtering by status
pendingTasks, err := st.List(string(task.StatusPending))
if err != nil {
t.Fatalf("failed to list pending tasks: %v", err)
}
if len(pendingTasks) != 1 {
t.Fatalf("expected 1 pending task, got %d", len(pendingTasks))
}
if pendingTasks[0].ID != "task-10" {
t.Errorf("expected task-10, got %s", pendingTasks[0].ID)
}

completedTasks, err := st.List(string(task.StatusCompleted))
if err != nil {
t.Fatalf("failed to list completed tasks: %v", err)
}
if len(completedTasks) != 1 {
t.Fatalf("expected 1 completed task, got %d", len(completedTasks))
}
if completedTasks[0].ID != "task-11" {
t.Errorf("expected task-11, got %s", completedTasks[0].ID)
}
}
