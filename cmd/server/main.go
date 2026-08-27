package main

import (
"encoding/json"
"log"
"net/http"
"os"

"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/internal/websocket"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

type Server struct {
scheduler *scheduler.TaskScheduler
store     store.Store
dlq       *scheduler.DeadLetterQueue
wsHub     *websocket.Hub
pool      *worker.Pool
}

func main() {
st, err := store.NewSQLiteStore("taskforge.db")
if err != nil {
log.Fatalf("Failed to initialize database: %v", err)
}
defer st.Close()

sched := scheduler.NewTaskScheduler()
dlq := scheduler.NewDeadLetterQueue()
wsHub := websocket.NewHub()
go wsHub.Run()

listener := func(t *task.Task, status task.Status) {
wsHub.BroadcastTaskUpdate(t, string(status))
}

pool := worker.NewPool(3, sched, st, dlq, listener)
pool.Start()
defer pool.Stop()

server := &Server{
scheduler: sched,
store:     st,
dlq:       dlq,
wsHub:     wsHub,
pool:      pool,
}

http.HandleFunc("/tasks", server.handleTasks)
http.HandleFunc("/dlq", server.handleDLQ)
http.HandleFunc("/ws", wsHub.ServeHTTP)
http.Handle("/", http.FileServer(http.Dir("./static")))

port := os.Getenv("PORT")
if port == "" {
port = "8080"
}

log.Printf("TaskForge server running on port %s...", port)
if err := http.ListenAndServe(":"+port, nil); err != nil {
log.Fatalf("Server error: %v", err)
}
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")

switch r.Method {
case http.MethodPost:
var t task.Task
if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
http.Error(w, err.Error(), http.StatusBadRequest)
return
}
if t.ID == "" {
t.ID = task.GenerateID()
}
t.Status = task.StatusPending

if err := s.store.Save(&t); err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}

s.scheduler.Push(&t)
s.wsHub.BroadcastTaskUpdate(&t, string(task.StatusPending))

w.WriteHeader(http.StatusCreated)
json.NewEncoder(w).Encode(t)

case http.MethodGet:
tasks, err := s.store.List()
if err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}
json.NewEncoder(w).Encode(tasks)

default:
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
if r.Method != http.MethodGet {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
json.NewEncoder(w).Encode(s.dlq.GetAll())
}

