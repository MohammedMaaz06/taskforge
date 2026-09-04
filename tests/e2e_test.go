package tests

import (
"bytes"
"encoding/json"
"net/http"
"net/http/httptest"
"os"
"testing"
"time"

"taskforge/internal/auth"
"taskforge/internal/dag"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

func TestE2E_TaskLifecycleAndAuth(t *testing.T) {
dbFile := "e2e_test.db"
defer os.Remove(dbFile)

st, err := store.NewSQLiteStore(dbFile)
if err != nil {
t.Fatalf("failed to create store: %v", err)
}
defer st.Close()

dagMgr := dag.NewManager(st)
dlq := scheduler.NewDeadLetterQueue()
sched := scheduler.NewTaskScheduler()

pool := worker.NewPool(2, sched, st, dlq, dagMgr, nil)
pool.Start()
defer pool.Stop()

authGuard := auth.NewAuthGuard("e2e-secret-key", map[string]string{
"valid-api-key": "test-client",
})

submitHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
return
}
var tsk task.Task
if err := json.NewDecoder(r.Body).Decode(&tsk); err != nil {
http.Error(w, err.Error(), http.StatusBadRequest)
return
}
if tsk.ID == "" {
tsk.ID = "task-e2e-1"
}
tsk.Status = task.StatusPending
if err := st.Save(&tsk); err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}
sched.Push(&tsk)

w.WriteHeader(http.StatusCreated)
_ = json.NewEncoder(w).Encode(tsk)
})

mux := http.NewServeMux()
mux.Handle("/tasks", authGuard.Middleware(submitHandler))

testServer := httptest.NewServer(mux)
defer testServer.Close()

t.Run("Unauthorized Request Blocked", func(t *testing.T) {
req, _ := http.NewRequest(http.MethodPost, testServer.URL+"/tasks", nil)
resp, err := http.DefaultClient.Do(req)
if err != nil {
t.Fatalf("failed request: %v", err)
}
defer resp.Body.Close()
if resp.StatusCode != http.StatusUnauthorized {
t.Fatalf("expected status 401 Unauthorized, got %d", resp.StatusCode)
}
})

t.Run("Authorized Task Submission and Execution", func(t *testing.T) {
newTask := task.Task{
ID:       "task-e2e-1",
Name:     "e2e-task",
Status:   task.StatusPending,
Priority: 1,
}
body, _ := json.Marshal(newTask)

req, _ := http.NewRequest(http.MethodPost, testServer.URL+"/tasks", bytes.NewBuffer(body))
req.Header.Set("X-API-Key", "valid-api-key")

resp, err := http.DefaultClient.Do(req)
if err != nil {
t.Fatalf("failed request: %v", err)
}
defer resp.Body.Close()

if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
t.Fatalf("expected status 201/200, got %d", resp.StatusCode)
}

// Wait for worker loop execution to complete (executeTask sleeps 500ms on success)
time.Sleep(800 * time.Millisecond)

processed, err := st.Get("task-e2e-1")
if err != nil {
t.Fatalf("failed to retrieve task from store: %v", err)
}
if processed.Status != task.StatusCompleted {
t.Fatalf("expected task status COMPLETED, got %s", processed.Status)
}
})
}

