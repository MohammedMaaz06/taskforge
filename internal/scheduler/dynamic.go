package scheduler

import (
"context"
"fmt"
"time"

"github.com/redis/go-redis/v9"
)

type DynamicScheduler struct {
rdb *redis.Client
key string
}

func NewDynamicScheduler(rdb *redis.Client, queueKey string) *DynamicScheduler {
return &DynamicScheduler{
rdb: rdb,
key: fmt.Sprintf("scheduler:delayed:%s", queueKey),
}
}

func (s *DynamicScheduler) ScheduleTask(ctx context.Context, taskID string, executeAt time.Time) error {
score := float64(executeAt.Unix())
return s.rdb.ZAdd(ctx, s.key, redis.Z{
Score:  score,
Member: taskID,
}).Err()
}

func (s *DynamicScheduler) FetchReadyTasks(ctx context.Context, now time.Time) ([]string, error) {
maxScore := fmt.Sprintf("%d", now.Unix())

// Fetch task IDs whose scheduled execution time is <= now
taskIDs, err := s.rdb.ZRangeByScore(ctx, s.key, &redis.ZRangeBy{
Min: "-inf",
Max: maxScore,
}).Result()
if err != nil {
return nil, err
}

if len(taskIDs) > 0 {
// Remove fetched IDs from the sorted set
err = s.rdb.ZRem(ctx, s.key, taskIDs).Err()
if err != nil {
return nil, err
}
}

return taskIDs, nil
}
