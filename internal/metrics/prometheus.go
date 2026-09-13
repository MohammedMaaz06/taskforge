package metrics

import (
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
TasksProcessedTotal = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "taskforge_tasks_processed_total",
Help: "Total number of processed tasks by status",
},
[]string{"status"},
)

TaskProcessingDuration = promauto.NewHistogramVec(
prometheus.HistogramOpts{
Name:    "taskforge_task_duration_seconds",
Help:    "Task execution latency in seconds",
Buckets: prometheus.DefBuckets,
},
[]string{"task_type"},
)

QueueDepthGauge = promauto.NewGaugeVec(
prometheus.GaugeOpts{
Name: "taskforge_queue_depth",
Help: "Current depth of Redis task queues",
},
[]string{"queue_name"},
)
)
