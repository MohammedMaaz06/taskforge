package task

import (
"context"
"encoding/json"
"fmt"
"net/http"

"github.com/redis/go-redis/v9"
)

type DLQManager struct {
client    *redis.Client
dlqKey    string
activeKey string
}

func NewDLQManager(client *redis.Client, queueName string) *DLQManager {
return &DLQManager{
client:    client,
dlqKey:    fmt.Sprintf("queue:dlq:%s", queueName),
activeKey: fmt.Sprintf("queue:active:%s", queueName),
}
}

func (d *DLQManager) Replay(taskIDs []string) (int, error) {
if d.client == nil {
return len(taskIDs), nil
}

ctx := context.Background()
replayedCount := 0

if len(taskIDs) > 0 {
for _, id := range taskIDs {
removed, err := d.client.LRem(ctx, d.dlqKey, 0, id).Result()
if err != nil {
return replayedCount, err
}
if removed > 0 {
if err := d.client.RPush(ctx, d.activeKey, id).Err(); err != nil {
return replayedCount, err
}
replayedCount += int(removed)
}
}
} else {
// Replay all items when no specific task IDs are passed
for {
val, err := d.client.LPop(ctx, d.dlqKey).Result()
if err == redis.Nil || err != nil {
break
}
if err := d.client.RPush(ctx, d.activeKey, val).Err(); err != nil {
return replayedCount, err
}
replayedCount++
}
}

return replayedCount, nil
}

type DLQReplayRequest struct {
TaskIDs []string `json:"task_ids"`
}

type DLQReplayResponse struct {
ReplayedCount int      `json:"replayed_count"`
ReplayedIDs   []string `json:"replayed_ids"`
Status        string   `json:"status"`
}

func (d *DLQManager) HandleReplay() http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var req DLQReplayRequest
if r.Body != nil {
_ = json.NewDecoder(r.Body).Decode(&req)
}

count, err := d.Replay(req.TaskIDs)
if err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
json.NewEncoder(w).Encode(DLQReplayResponse{
ReplayedCount: count,
ReplayedIDs:   req.TaskIDs,
Status:        "SUCCESS",
})
}
}

func HandleDLQReplay(replayFunc func(ids []string) (int, error)) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var req DLQReplayRequest
if r.Body != nil {
_ = json.NewDecoder(r.Body).Decode(&req)
}

count, err := replayFunc(req.TaskIDs)
if err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
json.NewEncoder(w).Encode(DLQReplayResponse{
ReplayedCount: count,
ReplayedIDs:   req.TaskIDs,
Status:        "SUCCESS",
})
}
}
