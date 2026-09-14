package scheduler

import (
"context"
"testing"
"time"

"github.com/alicebob/miniredis/v2"
"github.com/redis/go-redis/v9"
)

func TestDynamicScheduler_ScheduleAndFetch(t *testing.T) {
mr, err := miniredis.Run()
if err != nil {
t.Fatalf("failed to start miniredis: %v", err)
}
defer mr.Close()

rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
ds := NewDynamicScheduler(rdb, "default")
ctx := context.Background()

now := time.Now()
pastTask := "task-past"
futureTask := "task-future"

// Schedule one past task and one future task
_ = ds.ScheduleTask(ctx, pastTask, now.Add(-5*time.Second))
_ = ds.ScheduleTask(ctx, futureTask, now.Add(10*time.Minute))

// Fetch tasks ready right now
ready, err := ds.FetchReadyTasks(ctx, now)
if err != nil {
t.Fatalf("failed to fetch ready tasks: %v", err)
}

if len(ready) != 1 || ready[0] != pastTask {
t.Fatalf("expected [%s], got %v", pastTask, ready)
}
}
