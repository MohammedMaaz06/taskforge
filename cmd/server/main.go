package main

import (
"context"
"encoding/json"
"errors"
"fmt"
"net/http"
"os"
"os/signal"
"strings"
"syscall"
"time"

"github.com/prometheus/client_golang/prometheus/promhttp"

"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/websocket"
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

hub := websocket.NewHub()
go hub.Run()

dlq := scheduler.NewDLQ()
wp := worker.NewWorkerPool(3, 10, dlq, st)
wp.Start()

mux := http.NewServeMux()

mux.Handle("/", http.FileServer(http.Dir("./web")))
mux.Handle("/metrics", promhttp.Handler())
mux.HandleFunc("/ws", hub.HandleWS)

mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
})

mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
if r.Method == http.MethodGet {
w.Header().Set("Content-Type", "application/json")
_ = json.NewEncoder(w).Encode(wp.GetWorkerStats())
return
}
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
if r.Method == http.MethodGet {
w.Header().Set("Content-Type", "application/json")
_ = json.NewEncoder(w).Encode(dlq.GetAll())
return
}
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

mux.HandleFunc("/dlq/retry/", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
id := strings.TrimPrefix(r.URL.Path, "/dlq/retry/")
t, ok := dlq.Remove(id)
if !ok {
http.Error(w, "Task not found in DLQ", http.StatusNotFound)
return
}
t.Status = task.StatusPending
t.CurrentRetry = 0
_ = st.UpdateStatus(t.ID, task.StatusPending, "")
wp.Submit(t)
hub.Broadcast("TASK_RETRIED", t)
w.WriteHeader(http.StatusOK)
})

mux.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
if r.Method == http.MethodGet {
statusFilter := strings.ToUpper(r.URL.Query().Get("status"))
tasks, err := st.List(statusFilter)
if err != nil {
http.Error(w, "Failed to retrieve tasks", http.StatusInternalServerError)
return
}
w.Header().Set("Content-Type", "application/json")
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
hub.Broadcast("TASK_CREATED", tsk)

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusCreated)
_ = json.NewEncoder(w).Encode(tsk)
return
}
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

mux.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {
path := strings.TrimPrefix(r.URL.Path, "/tasks/")
parts := strings.Split(path, "/")
id := parts[0]

if id == "" {
http.Error(w, "Task ID required", http.StatusBadRequest)
return
}

if len(parts) == 1 && r.Method == http.MethodGet {
tsk, err := st.Get(id)
if err != nil {
http.Error(w, "Task not found", http.StatusNotFound)
return
}
w.Header().Set("Content-Type", "application/json")
_ = json.NewEncoder(w).Encode(tsk)
return
}

if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
tsk, err := st.Get(id)
if err != nil {
http.Error(w, "Task not found", http.StatusNotFound)
return
}
_ = st.UpdateStatus(id, task.StatusFailed, "task cancelled by user")
tsk.Status = task.StatusFailed
tsk.LastError = "task cancelled by user"
dlq.Add(tsk)
metrics.TasksProcessed.WithLabelValues("cancelled").Inc()
hub.Broadcast("TASK_CANCELLED", tsk)
w.WriteHeader(http.StatusOK)
return
}

http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

port := os.Getenv("PORT")
if port == "" {
port = "8080"
}
server := &http.Server{Addr: ":" + port, Handler: mux}

stop := make(chan os.Signal, 1)
signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

go func() {
fmt.Printf("TaskForge server starting on port %s...\n", port)
if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
fmt.Printf("Server unexpected error: %v\n", err)
}
}()

<-stop
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = server.Shutdown(ctx)
wp.Stop()
_ = st.Close()
}
