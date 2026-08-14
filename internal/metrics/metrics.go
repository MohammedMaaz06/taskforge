package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Telemetry struct {
	SubmittedTasks int64
	CompletedTasks int64
	FailedTasks    int64
	ActiveWorkers  int64
}

var Global = &Telemetry{}

func (t *Telemetry) IncSubmitted() { atomic.AddInt64(&t.SubmittedTasks, 1) }
func (t *Telemetry) IncCompleted() { atomic.AddInt64(&t.CompletedTasks, 1) }
func (t *Telemetry) IncFailed()    { atomic.AddInt64(&t.FailedTasks, 1) }

// Handler exposes Prometheus-style plain text metrics at /metrics
func (t *Telemetry) Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP taskforge_tasks_submitted_total Total submitted tasks\n")
	fmt.Fprintf(w, "# TYPE taskforge_tasks_submitted_total counter\n")
	fmt.Fprintf(w, "taskforge_tasks_submitted_total %d\n\n", atomic.LoadInt64(&t.SubmittedTasks))

	fmt.Fprintf(w, "# HELP taskforge_tasks_completed_total Total successfully completed tasks\n")
	fmt.Fprintf(w, "# TYPE taskforge_tasks_completed_total counter\n")
	fmt.Fprintf(w, "taskforge_tasks_completed_total %d\n\n", atomic.LoadInt64(&t.CompletedTasks))

	fmt.Fprintf(w, "# HELP taskforge_tasks_failed_total Total failed tasks routed to DLQ\n")
	fmt.Fprintf(w, "# TYPE taskforge_tasks_failed_total counter\n")
	fmt.Fprintf(w, "taskforge_tasks_failed_total %d\n", atomic.LoadInt64(&t.FailedTasks))
}
