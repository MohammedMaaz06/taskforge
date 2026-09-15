package worker

import (
"context"
"testing"
"time"

"github.com/alicebob/miniredis/v2"
"github.com/redis/go-redis/v9"
)

func TestRateLimiter_Allow(t *testing.T) {
mr, err := miniredis.Run()
if err != nil {
t.Fatalf("failed to start miniredis: %v", err)
}
defer mr.Close()

rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
limiter := NewRateLimiter(rdb)
ctx := context.Background()

taskType := "webhook_dispatch"
limit := 2
window := 1 * time.Second

// First two execution requests should pass
for i := 0; i < limit; i++ {
allowed, err := limiter.Allow(ctx, taskType, limit, window)
if err != nil || !allowed {
t.Fatalf("expected request %d to be allowed, got allowed=%v, err=%v", i+1, allowed, err)
}
}

// Third request within the window should be throttled
allowed, err := limiter.Allow(ctx, taskType, limit, window)
if err != nil {
t.Fatalf("unexpected error: %v", err)
}
if allowed {
t.Fatalf("expected 3rd request to be rate limited")
}
}
