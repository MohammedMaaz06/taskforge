package task

import (
"context"
"encoding/json"
"fmt"
"math"
"math/rand"
"time"

"github.com/redis/go-redis/v9"
)

type Task struct {
ID         string    `json:"id"`
Type       string    `json:"type"`
Payload    string    `json:"payload,omitempty"`
MaxRetries int       `json:"max_retries"`
RetryCount int       `json:"retry_count"`
LastError  string    `json:"last_error,omitempty"`
FailedAt   time.Time `json:"failed_at,omitempty"`
}

const (
QueueMain = "task_queue"
QueueDLQ  = "task_queue_dlq"
)

func CalculateBackoff(retryCount int) time.Duration {
base := 100 * time.Millisecond
maxBackoff := 10 * time.Second

backoff := float64(base) * math.Pow(2, float64(retryCount))
if backoff > float64(maxBackoff) {
backoff = float64(maxBackoff)
}

jitter := rand.Float64() * backoff
return time.Duration(jitter)
}

func PushToDLQ(ctx context.Context, rdb *redis.Client, t Task, errReason string) error {
t.LastError = errReason
t.FailedAt = time.Now()

data, err := json.Marshal(t)
if err != nil {
return fmt.Errorf("failed to marshal DLQ task: %w", err)
}

return rdb.RPush(ctx, QueueDLQ, data).Err()
}

func ReplayDLQ(ctx context.Context, rdb *redis.Client, count int64) (int64, error) {
var replayed int64
for i := int64(0); i < count; i++ {
rawTask, err := rdb.LPop(ctx, QueueDLQ).Result()
if err == redis.Nil {
break
} else if err != nil {
return replayed, err
}

var t Task
if err := json.Unmarshal([]byte(rawTask), &t); err == nil {
t.RetryCount = 0
t.LastError = ""
data, _ := json.Marshal(t)
if err := rdb.RPush(ctx, QueueMain, data).Err(); err == nil {
replayed++
}
}
}
return replayed, nil
}
