package worker

import (
"context"
"fmt"
"time"

"github.com/redis/go-redis/v9"
)

type HeartbeatManager struct {
rdb        *redis.Client
workerID   string
interval   time.Duration
ttl        time.Duration
}

func NewHeartbeatManager(rdb *redis.Client, workerID string, interval, ttl time.Duration) *HeartbeatManager {
return &HeartbeatManager{
rdb:      rdb,
workerID: workerID,
interval: interval,
ttl:      ttl,
}
}

func (h *HeartbeatManager) Start(ctx context.Context) {
ticker := time.NewTicker(h.interval)
defer ticker.Stop()

// Initial heartbeat immediately
_ = h.SendHeartbeat(ctx)

for {
select {
case <-ctx.Done():
_ = h.ClearHeartbeat(context.Background())
return
case <-ticker.C:
_ = h.SendHeartbeat(ctx)
}
}
}

func (h *HeartbeatManager) SendHeartbeat(ctx context.Context) error {
key := fmt.Sprintf("worker:heartbeat:%s", h.workerID)
return h.rdb.Set(ctx, key, time.Now().Unix(), h.ttl).Err()
}

func (h *HeartbeatManager) ClearHeartbeat(ctx context.Context) error {
key := fmt.Sprintf("worker:heartbeat:%s", h.workerID)
return h.rdb.Del(ctx, key).Err()
}
