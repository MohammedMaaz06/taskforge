package main

import (
"encoding/json"
"fmt"
"log"
"net/http"
"os"
"time"

"github.com/redis/go-redis/v9"
"taskforge/internal/middleware"
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

// Rate limited API endpoint (100 req / minute window)
mux.HandleFunc("/api/v1/tasks/priority", middleware.RateLimit(rdb, 100, time.Minute, func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var pt task.PriorityTask
if err := json.NewDecoder(r.Body).Decode(&pt); err != nil {
http.Error(w, "Invalid payload", http.StatusBadRequest)
return
}
if pt.ID == "" {
pt.ID = fmt.Sprintf("task-prio-%d", time.Now().UnixNano())
}

if err := task.EnqueuePriority(r.Context(), rdb, pt); err != nil {
http.Error(w, "Failed to enqueue priority task", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusAccepted)
json.NewEncoder(w).Encode(map[string]interface{}{
"status":   "priority_enqueued",
"id":       pt.ID,
"priority": pt.Priority,
})
}))

mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}

mainLen, _ := rdb.LLen(r.Context(), task.QueueMain).Result()
dlqLen, _ := rdb.LLen(r.Context(), task.QueueDLQ).Result()
prioLen, _ := rdb.ZCard(r.Context(), task.QueuePriority).Result()

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"main_queue_depth":     mainLen,
"dlq_depth":            dlqLen,
"priority_queue_depth": prioLen,
"timestamp":            time.Now().Format(time.RFC3339),
})
})

log.Println("Advanced API Gateway running on :8080...")
log.Fatal(http.ListenAndServe(":8080", mux))
}
