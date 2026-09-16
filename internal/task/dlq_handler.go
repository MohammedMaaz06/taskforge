package task

import (
"context"
"encoding/json"
"net/http"

"github.com/redis/go-redis/v9"
)

type DLQManager struct {
rdb      *redis.Client
dlqKey   string
queueKey string
}

func NewDLQManager(rdb *redis.Client, queueKey string) *DLQManager {
return &DLQManager{
rdb:      rdb,
dlqKey:   "queue:dlq:" + queueKey,
queueKey: "queue:active:" + queueKey,
}
}

func (d *DLQManager) HandleReplay() http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

ctx := context.Background()
replayedCount := 0

for {
// Atomically pop task from DLQ and push back to active processing queue
taskID, err := d.rdb.RPopLPush(ctx, d.dlqKey, d.queueKey).Result()
if err == redis.Nil {
break // DLQ cleared
} else if err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}
replayedCount++
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status":   "SUCCESS",
"replayed": replayedCount,
})
}
}

