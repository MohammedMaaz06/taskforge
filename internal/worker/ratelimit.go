package worker

import (
"context"
"fmt"
"time"

"github.com/redis/go-redis/v9"
)

type RateLimiter struct {
rdb *redis.Client
}

func NewRateLimiter(rdb *redis.Client) *RateLimiter {
return &RateLimiter{rdb: rdb}
}

// Allow checks if a task type can execute based on token bucket limits
func (r *RateLimiter) Allow(ctx context.Context, taskType string, limit int, window time.Duration) (bool, error) {
key := fmt.Sprintf("ratelimit:%s", taskType)
now := time.Now().UnixNano()
clearBefore := now - window.Nanoseconds()

pipe := r.rdb.TxPipeline()
pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", clearBefore))
pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: now})
countCmd := pipe.ZCard(ctx, key)
pipe.Expire(ctx, key, window)

_, err := pipe.Exec(ctx)
if err != nil {
return false, err
}

return countCmd.Val() <= int64(limit), nil
}
