package main

import (
"context"
"fmt"
"log"
"os"
"time"

"github.com/redis/go-redis/v9"
)

func main() {
redisAddr := os.Getenv("REDIS_ADDR")
if redisAddr == "" {
redisAddr = "redis-master-0.redis-headless.default.svc.cluster.local:6379"
}

rdb := redis.NewClient(&redis.Options{
Addr: redisAddr,
})

ctx := context.Background()
fmt.Printf("Worker starting, listening on Redis at %s...\n", redisAddr)

for {
// BLPop blocks until a task is available on task_queue
result, err := rdb.BLPop(ctx, 0*time.Second, "task_queue").Result()
if err != nil {
log.Printf("Error popping task from Redis: %v", err)
time.Sleep(2 * time.Second)
continue
}

// result[0] is key name ("task_queue"), result[1] is payload
taskData := result[1]
fmt.Printf("[WORKER] Processed task payload: %s\n", taskData)
}
}
