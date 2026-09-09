package main

import (
"context"
"encoding/json"
"fmt"
"log"
"os"
"time"

"github.com/redis/go-redis/v9"
"taskforge/internal/store"
)

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

fmt.Printf("Worker %s starting with distributed locking at %s...\n", workerID, redisAddr)

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
continue
}

taskKey := fmt.Sprintf("task:%s", task.ID)
lockTTL := 30 * time.Second

acquired, err := locker.Acquire(ctx, taskKey, lockTTL)
if err != nil {
log.Printf("[LOCKED] Error acquiring lock for task %s: %v", task.ID, err)
continue
}
if !acquired {
log.Printf("[SKIP] Task %s is already locked by another worker", task.ID)
continue
}

log.Printf("[WORKER %s] [LOCK ACQUIRED] Processing task: ID=%s, Type=%s", workerID, task.ID, task.Type)

time.Sleep(500 * time.Millisecond)

if err := locker.Release(ctx, taskKey); err != nil {
log.Printf("[LOCK RELEASE WARN] Could not release lock for task %s: %v", task.ID, err)
} else {
log.Printf("[WORKER %s] [LOCK RELEASED] Task %s completed successfully", workerID, task.ID)
}
}
}
