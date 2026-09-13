package task

import (
"context"
"encoding/json"
"fmt"

"github.com/redis/go-redis/v9"
)

const QueuePriority = "queue:priority"

type PriorityTask struct {
Task
Priority int `json:"priority"`
}

func EnqueuePriority(ctx context.Context, rdb *redis.Client, t PriorityTask) error {
data, err := json.Marshal(t)
if err != nil {
return fmt.Errorf("failed to marshal priority task: %w", err)
}

score := float64(-t.Priority)
return rdb.ZAdd(ctx, QueuePriority, redis.Z{Score: score, Member: data}).Err()
}

func DequeuePriority(ctx context.Context, rdb *redis.Client) (*PriorityTask, error) {
results, err := rdb.ZPopMin(ctx, QueuePriority, 1).Result()
if err != nil {
return nil, err
}
if len(results) == 0 {
return nil, redis.Nil
}

var pt PriorityTask
if err := json.Unmarshal([]byte(results[0].Member.(string)), &pt); err != nil {
return nil, err
}
return &pt, nil
}
