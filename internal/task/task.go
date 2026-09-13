package task

import (
"context"
"encoding/json"
"math"
"time"

"github.com/redis/go-redis/v9"
)

const (
QueueMain = "main"
QueueDLQ  = "dlq"
)

type Task struct {
ID         string    `json:"id"`
Type       string    `json:"type"`
Status     string    `json:"status"`
Payload    string    `json:"payload"`
MaxRetries int       `json:"max_retries"`
RetryCount int       `json:"retry_count"`
LastError  string    `json:"last_error"`
CreatedAt  time.Time `json:"created_at"`
UpdatedAt  time.Time `json:"updated_at"`
}

func CalculateBackoff(retryCount int) time.Duration {
backoff := math.Pow(2, float64(retryCount))
return time.Duration(backoff) * time.Second
}

func PushToDLQ(ctx context.Context, rdb *redis.Client, t Task, reason string) error {
t.Status = "DLQ"
t.LastError = reason
t.UpdatedAt = time.Now()

data, err := json.Marshal(t)
if err != nil {
return err
}
return rdb.RPush(ctx, QueueDLQ, data).Err()
}
