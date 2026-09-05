package tests

import (
"fmt"
"io"
"net/http"
"net/http/httptest"
"strings"
"testing"
"time"

"taskforge/internal/dag"
"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

func TestE2E_PrometheusMetricsTracking(t *testing.T) {
metrics.InitMetrics()

st, err := store.NewSQLiteStore("test_metrics_e2e.db")
if err != nil {
t.Fatalf("Failed to initialize SQLite store: %v", err)
}
defer st.Close()

sched := scheduler.NewTaskScheduler()
dlq := scheduler.NewDeadLetterQueue()
dagMgr := dag.NewManager(st)

pool := worker.NewPool(3, sched, st, dlq, dagMgr, nil)
pool.Start()
defer pool.Stop()

mux := http.NewServeMux()
mux.Handle("/metrics", metrics.MetricsHandler())
ts := httptest.NewServer(mux)
defer ts.Close()

// Submit 3 tasks through the scheduler
taskIDs := []string{"metric-task-1", "metric-task-2", "metric-task-3"}
for _, id := range taskIDs {
tsk := task.NewTask(id, 1, 3)
if err := st.Save(tsk); err != nil {
t.Fatalf("failed to save task %s: %v", id, err)
}
sched.Push(tsk)
}

// Allow workers to process all submitted tasks
time.Sleep(1 * time.Second)

// Fetch exposed metrics endpoint
resp, err := http.Get(fmt.Sprintf("%s/metrics", ts.URL))
if err != nil {
t.Fatalf("failed to fetch /metrics endpoint: %v", err)
}
defer resp.Body.Close()

if resp.StatusCode != http.StatusOK {
t.Fatalf("expected HTTP 200 from /metrics, got %d", resp.StatusCode)
}

bodyBytes, err := io.ReadAll(resp.Body)
if err != nil {
t.Fatalf("failed to read response body: %v", err)
}
body := string(bodyBytes)

// Assert metric counter increments
if !strings.Contains(body, "taskforge_tasks_processed_total") {
t.Errorf("metrics missing taskforge_tasks_processed_total metric:\n%s", body)
}

if !strings.Contains(body, `taskforge_tasks_processed_total{status="COMPLETED"}`) {
t.Errorf("expected COMPLETED status counter in metrics:\n%s", body)
}

if !strings.Contains(body, "taskforge_task_execution_duration_seconds_bucket") {
t.Errorf("expected task execution latency histogram in metrics:\n%s", body)
}
}

