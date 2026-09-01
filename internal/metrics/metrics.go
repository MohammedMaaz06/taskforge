package metrics

import (
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
TasksCompleted = promauto.NewCounter(prometheus.CounterOpts{
Name: "taskforge_tasks_completed_total",
Help: "The total number of successfully completed tasks",
})

TaskFailures = promauto.NewCounter(prometheus.CounterOpts{
Name: "taskforge_tasks_failed_total",
Help: "The total number of failed tasks",
})

DLQCount = promauto.NewGauge(prometheus.GaugeOpts{
Name: "taskforge_dlq_tasks_total",
Help: "Current number of tasks in Dead Letter Queue",
})

QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
Name: "taskforge_queue_depth",
Help: "Current depth of pending task scheduler queue",
})

TaskDuration = promauto.NewHistogram(prometheus.HistogramOpts{
Name:    "taskforge_task_duration_seconds",
Help:    "Execution time for tasks in seconds",
Buckets: prometheus.DefBuckets,
})
)

