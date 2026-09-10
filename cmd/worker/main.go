package main

import (
"context"
"crypto/sha256"
"encoding/json"
"errors"
"fmt"
"log"
"net/http"
"os"
"time"

"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/redis/go-redis/v9"
"taskforge/internal/store"
"taskforge/internal/task"
)

var (
tasksProcessed = prometheus.NewCounterVec(
prometheus.CounterOpts{
Name: "taskforge_worker_tasks_processed_total",
Help: "Total number of tasks processed by the worker",
},
[]string{"status", "type"},
)
taskDuration = prometheus.NewHistogramVec(
prometheus.HistogramOpts{
Name:    "taskforge_worker_task_duration_seconds",
Help:    "Histogram of task processing latency",
Buckets: prometheus.DefBuckets,
},
[]string{"type"},
)
dlqTasksTotal = prometheus.NewCounter(
prometheus.CounterOpts{
Name: "taskforge_worker_dlq_tasks_total",
Help: "Total tasks routed to Dead Letter Queue",
},
)
)

func init() {
prometheus.MustRegister(tasksProcessed)
prometheus.MustRegister(taskDuration)
prometheus.MustRegister(dlqTasksTotal)
}

func executeTask(t task.Task) error {
if t.Type == "failing_task" {
return errors.New("simulated execution failure")
}
if t.Type == "cpu_bound" {
h := sha256.New()
for i := 0; i < 3000000; i++ {
h.Write([]byte(fmt.Sprintf("work-%d", i)))
}
return nil
}
time.Sleep(200 * time.Millisecond)
return nil
}

func main() {
redisAddr := os.Getenv("REDIS_ADDR")
if redisAddr == "" {
redisAddr = "redis-master.default.svc.cluster.local:6379"
}

rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
workerID := fmt.Sprintf("worker-%d", os.Getpid())
locker := store.NewRedisLocker(rdb, workerID)
ctx := context.Background()

go func() {
http.Handle("/metrics", promhttp.Handler())
log.Println("Exposing Prometheus metrics on :8081/metrics")
_ = http.ListenAndServe(":8081", nil)
}()

log.Printf("Worker %s initialized with Retries, Exponential Backoff, and DLQ handling...", workerID)

for {
result, err := rdb.BLPop(ctx, 0, task.QueueMain).Result()
if err != nil {
time.Sleep(1 * time.Second)
continue
}

var t task.Task
if err := json.Unmarshal([]byte(result[1]), &t); err != nil {
tasksProcessed.WithLabelValues("unmarshal_error", "unknown").Inc()
continue
}

if t.MaxRetries == 0 {
t.MaxRetries = 3
}

taskKey := fmt.Sprintf("task:%s", t.ID)
acquired, err := locker.Acquire(ctx, taskKey, 30*time.Second)
if err != nil || !acquired {
tasksProcessed.WithLabelValues("skipped", t.Type).Inc()
continue
}

startTime := time.Now()
execErr := executeTask(t)
duration := time.Since(startTime).Seconds()
taskDuration.WithLabelValues(t.Type).Observe(duration)

_ = locker.Release(ctx, taskKey)

if execErr != nil {
log.Printf("[FAILURE] Task %s failed (Attempt %d/%d): %v", t.ID, t.RetryCount+1, t.MaxRetries, execErr)

if t.RetryCount < t.MaxRetries {
t.RetryCount++
t.LastError = execErr.Error()
backoff := task.CalculateBackoff(t.RetryCount)

log.Printf("[RETRY BACKOFF] Re-queuing task %s after %v delay...", t.ID, backoff)
time.Sleep(backoff)

data, _ := json.Marshal(t)
rdb.RPush(ctx, task.QueueMain, data)
tasksProcessed.WithLabelValues("retried", t.Type).Inc()
} else {
log.Printf("[DLQ ROUTE] Task %s exhausted retries. Moving to %s", t.ID, task.QueueDLQ)
if err := task.PushToDLQ(ctx, rdb, t, execErr.Error()); err != nil {
log.Printf("Error pushing task %s to DLQ: %v", t.ID, err)
}
dlqTasksTotal.Inc()
tasksProcessed.WithLabelValues("dlq_exhausted", t.Type).Inc()
}
} else {
log.Printf("[SUCCESS] Task %s completed successfully", t.ID)
tasksProcessed.WithLabelValues("success", t.Type).Inc()
}
}
}
