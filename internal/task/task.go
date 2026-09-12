package task

import (
"context"
"fmt"

"github.com/redis/go-redis/v9"
)

const (
QueueMain     = "taskforge:queue:main"
QueueDLQ      = "taskforge:queue:dlq"
KeyTaskStatus = "taskforge:status:"
)

type TaskStatus string

const (
StatusPending   TaskStatus = "PENDING"
StatusCompleted TaskStatus = "COMPLETED"
StatusFailed    TaskStatus = "FAILED"
)

type Task struct {
ID         string                 `json:"id"`
Payload    map[string]interface{} `json:"payload"`
RetryCount int                    `json:"retry_count"`
Status     TaskStatus             `json:"status,omitempty"`
Error      string                 `json:"error,omitempty"`
}

func ReplayDLQ(ctx context.Context, rdb *redis.Client, limit int) (int, error) {
replayed := 0
for i := 0; i < limit; i++ {
_, err := rdb.RPopLPush(ctx, QueueDLQ, QueueMain).Result()
if err == redis.Nil {
break
}
if err != nil {
return replayed, fmt.Errorf("failed during replay: %w", err)
}
replayed++
}
return replayed, nil
}

func SetTaskStatus(ctx context.Context, rdb *redis.Client, id string, status TaskStatus) error {
return rdb.Set(ctx, KeyTaskStatus+id, string(status), 0).Err()
}

func GetTaskStatus(ctx context.Context, rdb *redis.Client, id string) (string, error) {
val, err := rdb.Get(ctx, KeyTaskStatus+id).Result()
if err == redis.Nil {
return "UNKNOWN", nil
}
return val, err
}

func GetMetrics(ctx context.Context, rdb *redis.Client) (map[string]int64, error) {
mainCount, err := rdb.LLen(ctx, QueueMain).Result()
if err != nil {
return nil, err
}
dlqCount, err := rdb.LLen(ctx, QueueDLQ).Result()
if err != nil {
return nil, err
}
return map[string]int64{
"main_queue": mainCount,
"dlq_queue":  dlqCount,
}, nil
}

func PurgeDLQ(ctx context.Context, rdb *redis.Client) (int64, error) {
return rdb.Del(ctx, QueueDLQ).Result()
}
