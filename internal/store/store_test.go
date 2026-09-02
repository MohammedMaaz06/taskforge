package store

import (
"os"
"testing"

"taskforge/pkg/task"
)

func TestSQLiteStore(t *testing.T) {
dbPath := "test_taskforge.db"
defer os.Remove(dbPath)

st, err := NewSQLiteStore(dbPath)
if err != nil {
t.Fatalf("Failed to create store: %v", err)
}
defer st.Close()

// Test Save & Get
tsk := task.NewTask("test-job", 1, 3)
tsk.Payload = "hello world"

if err := st.Save(tsk); err != nil {
t.Fatalf("Failed to save task: %v", err)
}

retrieved, err := st.Get(tsk.ID)
if err != nil {
t.Fatalf("Failed to get task: %v", err)
}

if retrieved.Name != "test-job" {
t.Errorf("Expected name test-job, got %s", retrieved.Name)
}

// Test List
tasks, err := st.List()
if err != nil || len(tasks) != 1 {
t.Fatalf("Expected 1 task in store, got %d", len(tasks))
}
}

