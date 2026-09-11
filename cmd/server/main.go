package main

import (
"encoding/json"
"fmt"
"log"
"net/http"
"os"
"time"

"github.com/redis/go-redis/v9"
"taskforge/internal/task"
)

func authenticate(r *http.Request) bool {
apiKey := r.Header.Get("X-API-Key")
expectedKey := os.Getenv("API_KEY")
if expectedKey == "" {
expectedKey = "tf-worker-secret-key"
}
return apiKey == expectedKey
}

func main() {
redisAddr := os.Getenv("REDIS_ADDR")
if redisAddr == "" {
redisAddr = "redis-master.default.svc.cluster.local:6379"
}

rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
mux := http.NewServeMux()

mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
w.Write([]byte("OK"))
})

mux.HandleFunc("/api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var t task.Task
if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
http.Error(w, "Invalid payload", http.StatusBadRequest)
return
}
if t.ID == "" {
t.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
}

data, err := json.Marshal(t)
if err != nil {
http.Error(w, "Task processing error", http.StatusInternalServerError)
return
}

if err := rdb.RPush(r.Context(), task.QueueMain, data).Err(); err != nil {
http.Error(w, "Failed to enqueue task", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusAccepted)
json.NewEncoder(w).Encode(map[string]string{
"status":  "enqueued",
"message": "Task sent to worker queue",
"id":      t.ID,
})
})

mux.HandleFunc("/api/v1/dlq", func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}
if r.Method != http.MethodGet {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

tasksRaw, err := rdb.LRange(r.Context(), task.QueueDLQ, 0, 50).Result()
if err != nil {
http.Error(w, "Failed to query DLQ", http.StatusInternalServerError)
return
}

var dlqTasks []task.Task
for _, raw := range tasksRaw {
var t task.Task
if err := json.Unmarshal([]byte(raw), &t); err == nil {
dlqTasks = append(dlqTasks, t)
}
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"dlq_count": len(dlqTasks),
"tasks":     dlqTasks,
})
})

mux.HandleFunc("/api/v1/dlq/replay", func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

replayed, err := task.ReplayDLQ(r.Context(), rdb, 100)
if err != nil {
http.Error(w, "Failed to replay DLQ tasks", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status":            "success",
"replayed_count":    replayed,
"destination_queue": task.QueueMain,
})
})

// Feature 1: System Metrics Endpoint
mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}

mainLen, _ := rdb.LLen(r.Context(), task.QueueMain).Result()
dlqLen, _ := rdb.LLen(r.Context(), task.QueueDLQ).Result()

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"main_queue_depth": mainLen,
"dlq_depth":        dlqLen,
"timestamp":        time.Now().Format(time.RFC3339),
})
})

// Feature 2: DLQ Purge Endpoint
mux.HandleFunc("/api/v1/dlq/purge", func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}
if r.Method != http.MethodDelete {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

deleted, err := rdb.Del(r.Context(), task.QueueDLQ).Result()
if err != nil {
http.Error(w, "Failed to purge DLQ", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status":        "purged",
"keys_removed":  deleted,
"target_queue":  task.QueueDLQ,
})
})

log.Println("API Gateway running on :8080...")
log.Fatal(http.ListenAndServe(":8080", mux))
}
