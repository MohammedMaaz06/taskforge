package main

import (
"encoding/json"
"errors"
"fmt"
"net/http"
"os"
"strings"
"time"

"github.com/prometheus/client_golang/prometheus/promhttp"

"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

func main() {
dbPath := os.Getenv("DATABASE_PATH")
if dbPath == "" {
dbPath = "taskforge.db"
}

st, err := store.NewSQLiteStore(dbPath)
if err != nil {
fmt.Printf("Failed to initialize database: %v\n", err)
os.Exit(1)
}
defer st.Close()

dlq := scheduler.NewDLQ()
wp := worker.NewWorkerPool(3, 10, dlq)
wp.Start()
defer wp.Stop()

// Expose Prometheus metrics endpoint
http.Handle("/metrics", promhttp.Handler())

http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
})

http.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
if r.Method == http.MethodGet {
statusFilter := strings.ToUpper(r.URL.Query().Get("status"))
tasks, err := st.List(statusFilter)
if err != nil {
http.Error(w, "Failed to retrieve tasks", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_ = json.NewEncoder(w).Encode(tasks)
return
}

if r.Method == http.MethodPost {
var req struct {
Name     string `json:"name"`
Payload  string `json:"payload"`
Priority int    `json:"priority"`
}

if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, "Invalid payload", http.StatusBadRequest)
return
}

tsk := task.NewTask(fmt.Sprintf("task-%d", time.Now().UnixNano()), req.Name, []byte(req.Payload), req.Priority)
if err := st.Save(tsk); err != nil {
http.Error(w, "Failed to save task", http.StatusInternalServerError)
return
}

wp.Submit(tsk)
metrics.TasksProcessed.WithLabelValues("submitted").Inc()

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusCreated)
_ = json.NewEncoder(w).Encode(tsk)
return
}

http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

http.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {
path := strings.TrimPrefix(r.URL.Path, "/tasks/")
if path == "" {
http.Error(w, "Task ID required", http.StatusBadRequest)
return
}

parts := strings.Split(path, "/")
id := parts[0]

if len(parts) == 2 && parts[1] == "cancel" {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

tsk, err := st.Get(id)
if errors.Is(err, store.ErrTaskNotFound) {
http.Error(w, "Task not found", http.StatusNotFound)
return
}
if err != nil {
http.Error(w, "Database error", http.StatusInternalServerError)
return
}

if tsk.Status != task.StatusPending {
http.Error(w, fmt.Sprintf("cannot cancel task in %s status", tsk.Status), http.StatusBadRequest)
return
}

if err := st.UpdateStatus(id, task.StatusFailed, "task cancelled by user"); err != nil {
http.Error(w, "Failed to cancel task", http.StatusInternalServerError)
return
}

tsk.Status = task.StatusFailed
tsk.LastError = "task cancelled by user"
metrics.TasksProcessed.WithLabelValues("cancelled").Inc()

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_ = json.NewEncoder(w).Encode(tsk)
return
}

if len(parts) == 1 {
if r.Method == http.MethodGet {
tsk, err := st.Get(id)
if errors.Is(err, store.ErrTaskNotFound) {
http.Error(w, "Task not found", http.StatusNotFound)
return
}
if err != nil {
http.Error(w, "Database error", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_ = json.NewEncoder(w).Encode(tsk)
return
}
}

http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

port := os.Getenv("PORT")
if port == "" {
port = "8080"
}

fmt.Printf("TaskForge server starting on port %s...\n", port)
if err := http.ListenAndServe(":"+port, nil); err != nil {
fmt.Printf("Server failed: %v\n", err)
}
}
