package main

import (
"context"
"encoding/json"
"fmt"
"log"
"net/http"
"os"
"time"

"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/redis/go-redis/v9"
"taskforge/internal/store"
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
)

func init() {
prometheus.MustRegister(tasksProcessed)
prometheus.MustRegister(taskDuration)
}

type Task struct {
ID   string `json:"id"`
Type string `json:"type"`
}

func main() {
redisAddr := os.Getenv("REDIS_ADDR")
if redisAddr == "" {
redisAddr = "redis-master.default.svc.cluster.local:6379"
}

rdb := redis.NewClient(&redis.Options{
Addr: redisAddr,
})

workerID := fmt.Sprintf("worker-%d", os.Getpid())
locker := store.NewRedisLocker(rdb, workerID)
ctx := context.Background()

// Expose Prometheus metrics endpoint
go func() {
http.Handle("/metrics", promhttp.Handler())
log.Println("Exposing Prometheus metrics on :8081/metrics")
if err := http.ListenAndServe(":8081", nil); err != nil {
log.Printf("Metrics HTTP server failed: %v", err)
}
}()

fmt.Printf("Worker %s starting with metrics and distributed locking...\n", workerID)

for {
result, err := rdb.BLPop(ctx, 0, "task_queue").Result()
if err != nil {
log.Printf("Error popping task: %v", err)
time.Sleep(1 * time.Second)
continue
}

var task Task
if err := json.Unmarshal([]byte(result[1]), &task); err != nil {
log.Printf("Failed to unmarshal task payload: %v", err)
tasksProcessed.WithLabelValues("unmarshal_error", "unknown").Inc()
continue
}

taskKey := fmt.Sprintf("task:%s", task.ID)
lockTTL := 30 * time.Second

acquired, err := locker.Acquire(ctx, taskKey, lockTTL)
if err != nil || !acquired {
log.Printf("[SKIP] Task %s is locked or failed acquisition", task.ID)
tasksProcessed.WithLabelValues("skipped", task.Type).Inc()
continue
}

startTime := time.Now()
log.Printf("[WORKER %s] [LOCK ACQUIRED] Processing task: ID=%s, Type=%s", workerID, task.ID, task.Type)

time.Sleep(500 * time.Millisecond) // Simulated workload

duration := time.Since(startTime).Seconds()
taskDuration.WithLabelValues(task.Type).Observe(duration)

if err := locker.Release(ctx, taskKey); err != nil {
log.Printf("[LOCK RELEASE WARN] Could not release lock for task %s: %v", task.ID, err)
tasksProcessed.WithLabelValues("release_warn", task.Type).Inc()
} else {
log.Printf("[WORKER %s] [LOCK RELEASED] Task %s completed successfully", workerID, task.ID)
tasksProcessed.WithLabelValues("success", task.Type).Inc()
}
}
}
