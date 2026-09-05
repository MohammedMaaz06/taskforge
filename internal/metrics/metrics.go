package metrics

import (
"net/http"

"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
TaskQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
Name: "taskforge_queue_depth",
Help: "Current number of tasks pending in the scheduler queue",
})

TaskExecutionDuration = prometheus.NewHistogramVec(
prometheus.HistogramOpts{
Name:    "taskforge_task_execution_duration_seconds",
Help:    "Histogram of task execution latencies in seconds",
Buckets: prometheus.DefBuckets,
},
[]string{"status"},
)

TasksProcessedTotal = prometheus.NewCounterVec(
prometheus.CounterOpts{
Name: "taskforge_tasks_processed_total",
Help: "Total count of tasks processed by workers",
},
[]string{"status"},
)
)

func InitMetrics() {
prometheus.MustRegister(TaskQueueDepth)
prometheus.MustRegister(TaskExecutionDuration)
prometheus.MustRegister(TasksProcessedTotal)
}

func MetricsHandler() http.Handler {
return promhttp.Handler()
}

