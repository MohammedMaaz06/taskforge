package main

import (
"encoding/json"
"fmt"
"log/slog"
"net/http"
"os"
"sync/atomic"
"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

type Server struct {
queue  *scheduler.SafeQueue
pool   *worker.WorkerPool
store  *store.MemoryStore
dlq    *scheduler.DeadLetterQueue
idSeq  int64
logger *slog.Logger
}

func main() {
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
logger.Info("Starting TaskForge Server v0.5 with Telemetry & JSON Logging...")

dlq := scheduler.NewDLQ()
queue := scheduler.NewSafeQueue()
memStore := store.NewMemoryStore()
pool := worker.NewWorkerPool(3, 10, dlq)
pool.Start()

srv := &Server{
queue:  queue,
pool:   pool,
store:  memStore,
dlq:    dlq,
logger: logger,
}

// Routes
http.HandleFunc("/api/v1/tasks/submit", srv.handleSubmit)
http.HandleFunc("/api/v1/tasks/status", srv.handleStatus)
http.HandleFunc("/metrics", metrics.Global.Handler)

logger.Info("Server listening", "address", "http://localhost:8080")
if err := http.ListenAndServe(":8080", nil); err != nil {
logger.Error("server crashed", "error", err)
}
}

type TaskPayload struct {
Name     string `json:"name"`
Payload  string `json:"payload"`
Priority int    `json:"priority"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var req TaskPayload
if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
return
}

id := fmt.Sprintf("task-%d", atomic.AddInt64(&s.idSeq, 1))
t := task.NewTask(id, req.Name, []byte(req.Payload), req.Priority)

s.store.Save(t)
s.pool.Submit(t)

s.logger.Info("task submitted via REST", "task_id", t.ID, "priority", t.Priority)

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]string{
"task_id": t.ID,
"status":  string(t.Status),
"message": "Task accepted for execution",
})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
id := r.URL.Query().Get("id")
if id == "" {
http.Error(w, "Missing task id query parameter", http.StatusBadRequest)
return
}

t, err := s.store.Get(id)
if err != nil {
http.Error(w, err.Error(), http.StatusNotFound)
return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(t)
}
