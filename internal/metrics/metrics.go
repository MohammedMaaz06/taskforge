package metrics

import (
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
TasksProcessed = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "taskforge_tasks_processed_total",
Help: "The total number of processed tasks",
},
[]string{"status"},
)

TaskDuration = promauto.NewHistogramVec(
prometheus.HistogramOpts{
Name:    "taskforge_task_duration_seconds",
Help:    "Task execution duration in seconds",
Buckets: prometheus.DefBuckets,
},
[]string{"task_name"},
)

ActiveWorkers = promauto.NewGauge(
prometheus.GaugeOpts{
Name: "taskforge_active_workers",
Help: "Current number of active worker goroutines",
},
)
)

type Metrics struct {
TasksProcessed *prometheus.CounterVec
TaskDuration   *prometheus.HistogramVec
ActiveWorkers  prometheus.Gauge
}

func (m *Metrics) IncFailed() {
m.TasksProcessed.WithLabelValues("failed").Inc()
}

func (m *Metrics) IncCompleted() {
m.TasksProcessed.WithLabelValues("completed").Inc()
}

func (m *Metrics) IncSubmitted() {
m.TasksProcessed.WithLabelValues("submitted").Inc()
}

var Global = &Metrics{
TasksProcessed: TasksProcessed,
TaskDuration:   TaskDuration,
ActiveWorkers:  ActiveWorkers,
}
