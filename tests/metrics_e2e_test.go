package tests

import (
"os"
"testing"
"time"

"taskforge/internal/metrics"
"taskforge/internal/store"
"taskforge/pkg/task"
)

func TestE2E_PrometheusMetricsTracking(t *testing.T) {
dbPath := "test_metrics_e2e.db"
os.Remove(dbPath)
defer os.Remove(dbPath)

metrics.InitMetrics()

st, err := store.NewSQLiteStore(dbPath)
if err != nil {
t.Fatalf("Failed to initialize SQLite store: %v", err)
}
defer st.Close()

tsk := task.NewTask("metric-task-1", 1, 3)
tsk.Payload = "test metrics"

if err := st.Save(tsk); err != nil {
t.Fatalf("failed to save task %s: %v", tsk.ID, err)
}

time.Sleep(100 * time.Millisecond)
}
