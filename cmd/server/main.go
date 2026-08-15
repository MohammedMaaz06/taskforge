package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"taskforge/internal/metrics"
	"taskforge/internal/scheduler"
	"taskforge/internal/store"
	"taskforge/internal/worker"
	"taskforge/pkg/task"
)

type Server struct {
	queue  *scheduler.SafeQueue
	pool   *worker.WorkerPool
	store  store.TaskStore
	dlq    *scheduler.DeadLetterQueue
	idSeq  int64
	logger *slog.Logger
}

type TaskPayload struct {
	Name     string `json:"name"`
	Payload  string `json:"payload"`
	Priority int    `json:"priority"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Starting TaskForge Server v0.7 with SQLite Persistence...")

	persistentStore, err := store.NewSQLiteStore("taskforge.db")
	if err != nil {
		logger.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer persistentStore.Close()

	dlq := scheduler.NewDLQ()
	queue := scheduler.NewSafeQueue()
	pool := worker.NewWorkerPool(3, 10, dlq)
	pool.Start()

	srv := &Server{
		queue:  queue,
		pool:   pool,
		store:  persistentStore,
		dlq:    dlq,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tasks/submit", srv.handleSubmit)
	mux.HandleFunc("/api/v1/tasks/status", srv.handleStatus)
	mux.HandleFunc("/metrics", metrics.Global.Handler)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Info("Server listening", "address", "http://localhost:8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	sig := <-stopChan
	logger.Info("Shutdown signal received", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP server forced to shutdown", "error", err)
	} else {
		logger.Info("HTTP listener stopped accepting new requests")
	}

	logger.Info("Draining active task workers...")
	pool.Stop()

	logger.Info("TaskForge engine shut down cleanly. Bye!")
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

	if err := s.store.Save(t); err != nil {
		s.logger.Error("failed to save task", "error", err)
		http.Error(w, "Failed to persist task", http.StatusInternalServerError)
		return
	}

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
		if err == store.ErrTaskNotFound {
			http.Error(w, "Task not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(t)
}
