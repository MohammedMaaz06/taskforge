package task

import (
"context"
"net/http"
"net/http/httptest"
"testing"

"github.com/alicebob/miniredis/v2"
"github.com/redis/go-redis/v9"
)

func TestDLQManager_Replay(t *testing.T) {
mr, err := miniredis.Run()
if err != nil {
t.Fatalf("failed to start miniredis: %v", err)
}
defer mr.Close()

rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
dlq := NewDLQManager(rdb, "default")

ctx := context.Background()
_ = rdb.RPush(ctx, "queue:dlq:default", "task-failed-1", "task-failed-2").Err()

req := httptest.NewRequest(http.MethodPost, "/v1/dlq/replay", nil)
rr := httptest.NewRecorder()

dlq.HandleReplay().ServeHTTP(rr, req)

if rr.Code != http.StatusOK {
t.Fatalf("expected 200 OK, got %d", rr.Code)
}

dlqLen, _ := rdb.LLen(ctx, "queue:dlq:default").Result()
activeLen, _ := rdb.LLen(ctx, "queue:active:default").Result()

if dlqLen != 0 || activeLen != 2 {
t.Fatalf("expected DLQ len 0 and active len 2, got DLQ=%d, Active=%d", dlqLen, activeLen)
}
}
