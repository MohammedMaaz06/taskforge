package worker

import (
"testing"
"time"

"taskforge/internal/dag"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/pkg/task"
)

func TestWorkerPoolExecution(t *testing.T) {
st, err := store.NewSQLiteStore("test_worker.db")
if err != nil {
t.Fatalf("Failed to init db: %v", err)
}
defer st.Close()

sched := scheduler.NewTaskScheduler()
dlq := scheduler.NewDeadLetterQueue()
dagMgr := dag.NewManager(st)

pool := NewPool(2, sched, st, dlq, dagMgr, nil)
pool.Start()
defer pool.Stop()

tsk := task.NewTask("unit-test-task", 1, 3)
st.Save(tsk)
sched.Push(tsk)

time.Sleep(1 * time.Second)

res, err := st.Get(tsk.ID)
if err != nil {
t.Fatalf("Failed to fetch task: %v", err)
}

if res.Status != task.StatusCompleted {
t.Errorf("Expected task status COMPLETED, got %s", res.Status)
}
}

