package metrics

import (
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
TasksProcessedTotal = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "taskforge_tasks_processed_total",
Help: "Total number of tasks processed, partitioned by status.",
},
[]string{"status"},
)

TaskExecutionDuration = promauto.NewHistogramVec(
prometheus.HistogramOpts{
Name:    "taskforge_task_execution_duration_seconds",
Help:    "Histogram of task execution duration in seconds.",
Buckets: prometheus.DefBuckets,
},
[]string{"task_name"},
)

QueueDepth = promauto.NewGauge(
prometheus.GaugeOpts{
Name: "taskforge_queue_depth",
Help: "Current number of tasks waiting in the scheduler priority queue.",
},
)

DLQCount = promauto.NewGauge(
prometheus.GaugeOpts{
Name: "taskforge_dlq_count",
Help: "Current number of failed tasks residing in the Dead Letter Queue.",
},
)
)

