package main

import (
"encoding/json"
"fmt"
"net/http"
"time"

"github.com/prometheus/client_golang/prometheus/promhttp"

"taskforge/internal/metrics"
"taskforge/internal/notifier"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

func main() {
st, err := store.NewSQLiteStore("taskforge.db")
if err != nil {
fmt.Printf("Failed to initialize database: %v\n", err)
return
}
defer st.Close()

sched := scheduler.NewTaskScheduler()
dlq := scheduler.NewDeadLetterQueue()
notif := notifier.NewNotifier()

listener := func(t *task.Task, status task.Status) {
switch status {
case task.StatusCompleted:
notif.Notify(notifier.EventTaskCompleted, t)
case task.StatusFailed:
notif.Notify(notifier.EventTaskFailed, t)
case task.StatusDLQ:
notif.Notify(notifier.EventTaskDLQ, t)
}
}

wp := worker.NewPool(3, sched, st, dlq, listener)
wp.Start()
defer wp.Stop()

// Task Endpoint
http.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
switch r.Method {
case http.MethodPost:
var req struct {
Name            string `json:"name"`
Priority        int    `json:"priority"`
MaxRetries      int    `json:"max_retries"`
DelaySeconds    int    `json:"delay_seconds"`
IntervalSeconds int    `json:"interval_seconds"`
}
if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, err.Error(), http.StatusBadRequest)
return
}

if req.MaxRetries == 0 {
req.MaxRetries = 3
}

t := task.NewTask(req.Name, req.Priority, req.MaxRetries)
if req.DelaySeconds > 0 {
t.ScheduledAt = time.Now().Add(time.Duration(req.DelaySeconds) * time.Second)
}
if req.IntervalSeconds > 0 {
t.IntervalSeconds = req.IntervalSeconds
}

if err := st.Save(t); err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}
sched.Push(t)
metrics.QueueDepth.Set(float64(sched.Size()))

w.WriteHeader(http.StatusCreated)
json.NewEncoder(w).Encode(t)

case http.MethodGet:
tasks, err := st.List()
if err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}
json.NewEncoder(w).Encode(tasks)

default:
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}
})

// Webhook Endpoints
http.HandleFunc("/webhooks", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
switch r.Method {
case http.MethodPost:
var req struct {
URL string `json:"url"`
}
if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
http.Error(w, "Invalid webhook URL", http.StatusBadRequest)
return
}

notif.Register(req.URL)
json.NewEncoder(w).Encode(map[string]interface{}{"status": "registered", "url": req.URL})

case http.MethodGet:
json.NewEncoder(w).Encode(map[string]interface{}{"webhooks": notif.List()})

default:
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}
})

// Task Cancellation Endpoint
http.HandleFunc("/tasks/cancel", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

taskID := r.URL.Query().Get("id")
if taskID == "" {
http.Error(w, "Missing task id query parameter", http.StatusBadRequest)
return
}

success := wp.CancelTask(taskID)
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"task_id": taskID, "cancelled": success})
})

// DLQ Endpoints
http.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
if r.Method == http.MethodGet {
json.NewEncoder(w).Encode(dlq.List())
return
}
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
})

http.HandleFunc("/dlq/retry", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

taskID := r.URL.Query().Get("id")
t := dlq.Remove(taskID)
if t == nil {
http.Error(w, "Task not found in DLQ", http.StatusNotFound)
return
}

t.CurrentRetry = 0
t.Status = task.StatusPending
st.Save(t)
sched.Push(t)

metrics.DLQCount.Set(float64(dlq.Size()))
metrics.QueueDepth.Set(float64(sched.Size()))

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"task_id": taskID, "retried": true})
})

http.HandleFunc("/dlq/purge", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

dlq.Clear()
metrics.DLQCount.Set(0)

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"purged": true})
})

// Dynamic Worker Scaling Endpoint
http.HandleFunc("/workers/scale", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
switch r.Method {
case http.MethodGet:
json.NewEncoder(w).Encode(map[string]int{"workers": wp.GetWorkerCount()})

case http.MethodPost:
var req struct {
Workers int `json:"workers"`
}
if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, err.Error(), http.StatusBadRequest)
return
}

if req.Workers <= 0 {
http.Error(w, "Invalid worker count", http.StatusBadRequest)
return
}

newCount := wp.Scale(req.Workers)
json.NewEncoder(w).Encode(map[string]int{"workers": newCount})

default:
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}
})

http.Handle("/metrics", promhttp.Handler())
http.Handle("/", http.FileServer(http.Dir("./static")))

fmt.Println("TaskForge server running on port 8080...")
if err := http.ListenAndServe(":8080", nil); err != nil {
fmt.Printf("Server error: %v\n", err)
}
}

