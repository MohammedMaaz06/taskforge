package main

import (
"context"
"fmt"
"io"
"log"
"net/http"
"os"

"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/redis/go-redis/v9"
"taskforge/internal/auth"
)

func main() {
port := os.Getenv("PORT")
if port == "" {
port = "8080"
}

redisAddr := os.Getenv("REDIS_ADDR")
if redisAddr == "" {
redisAddr = "redis-master.default.svc.cluster.local:6379"
}

rdb := redis.NewClient(&redis.Options{
Addr: redisAddr,
})

jwtSecret := os.Getenv("JWT_SECRET")
if jwtSecret == "" {
jwtSecret = "taskforge-dev-secret-key"
}
apiKeys := map[string]string{
"tf-worker-secret-key": "internal-worker-node",
}
authGuard := auth.NewAuthGuard(jwtSecret, apiKeys)

mux := http.NewServeMux()

mux.Handle("/metrics", promhttp.Handler())

healthHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
w.Write([]byte(`{"status":"UP"}`))
})
mux.Handle("/health", healthHandler)
mux.Handle("/healthz", healthHandler)

// Task submission handler pushing to Redis
taskHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
ctx := context.Background()
body, err := io.ReadAll(r.Body)
if err != nil || len(body) == 0 {
body = []byte(`{"id":"api-generated-task","type":"default"}`)
}

err = rdb.RPush(ctx, "task_queue", string(body)).Err()
if err != nil {
http.Error(w, fmt.Sprintf(`{"error":"failed to enqueue task: %v"}`, err), http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusAccepted)
w.Write([]byte(`{"status":"enqueued","message":"Task sent to worker queue"}`))
})

mux.Handle("/api/tasks", authGuard.Middleware(taskHandler))
mux.Handle("/api/v1/tasks", authGuard.Middleware(taskHandler))

fileServer := http.FileServer(http.Dir("./static"))
mux.Handle("/", fileServer)

fmt.Printf("TaskForge Server starting on port %s...\n", port)
if err := http.ListenAndServe(":"+port, mux); err != nil {
log.Fatalf("Server failed: %v", err)
}
}
