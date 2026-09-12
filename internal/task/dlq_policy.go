package task

import (
"context"
"encoding/json"
"fmt"
"time"

"github.com/redis/go-redis/v9"
)

type ExpiringDLQTask struct {
Task
FailedAt time.Time `json:"failed_at"`
}

func AddToDLQWithTimestamp(ctx context.Context, rdb *redis.Client, t Task) error {
expTask := ExpiringDLQTask{
Task:     t,
FailedAt: time.Now(),
}
data, err := json.Marshal(expTask)
if err != nil {
return fmt.Errorf("failed to marshal task for DLQ: %w", err)
}
return rdb.RPush(ctx, QueueDLQ, data).Err()
}

func CleanExpiredDLQ(ctx context.Context, rdb *redis.Client, maxAge time.Duration) (int64, error) {
tasksRaw, err := rdb.LRange(ctx, QueueDLQ, 0, -1).Result()
if err != nil {
return 0, err
}

var removed int64
cutoff := time.Now().Add(-maxAge)

for _, raw := range tasksRaw {
var expTask ExpiringDLQTask
if err := json.Unmarshal([]byte(raw), &expTask); err == nil {
if !expTask.FailedAt.IsZero() && expTask.FailedAt.Before(cutoff) {
rdb.LRem(ctx, QueueDLQ, 1, raw)
removed++
}
}
}
return removed, nil
}
