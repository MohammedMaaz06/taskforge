package worker

import (
"context"
"testing"
"time"

"github.com/alicebob/miniredis/v2"
"github.com/redis/go-redis/v9"
)

func TestHeartbeatManager_SendAndClear(t *testing.T) {
mr, err := miniredis.Run()
if err != nil {
t.Fatalf("failed to start miniredis: %v", err)
}
defer mr.Close()

rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
hb := NewHeartbeatManager(rdb, "worker-node-1", 100*time.Millisecond, 1*time.Second)
ctx := context.Background()

// Send heartbeat
if err := hb.SendHeartbeat(ctx); err != nil {
t.Fatalf("failed to send heartbeat: %v", err)
}

if !mr.Exists("worker:heartbeat:worker-node-1") {
t.Fatalf("expected heartbeat key to exist")
}

// Clear heartbeat on shutdown
if err := hb.ClearHeartbeat(ctx); err != nil {
t.Fatalf("failed to clear heartbeat: %v", err)
}

if mr.Exists("worker:heartbeat:worker-node-1") {
t.Fatalf("expected heartbeat key to be deleted")
}
}
