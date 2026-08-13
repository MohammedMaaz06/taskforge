package worker

import (
"log/slog"
"os"
"sync"
"time"
"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/pkg/task"
)

type WorkerPool struct {
NumWorkers int
TaskStream chan *task.Task
DLQ        *scheduler.DeadLetterQueue
wg         sync.WaitGroup
logger     *slog.Logger
}

func NewWorkerPool(numWorkers int, bufferSize int, dlq *scheduler.DeadLetterQueue) *WorkerPool {
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
return &WorkerPool{
NumWorkers: numWorkers,
TaskStream: make(chan *task.Task, bufferSize),
DLQ:        dlq,
logger:     logger,
}
}

func (wp *WorkerPool) Start() {
for i := 1; i <= wp.NumWorkers; i++ {
wp.wg.Add(1)
go wp.worker(i)
}
wp.logger.Info("worker pool started", "num_workers", wp.NumWorkers)
}

func (wp *WorkerPool) worker(id int) {
defer wp.wg.Done()
for t := range wp.TaskStream {
wp.executeTask(id, t)
}
wp.logger.Info("worker thread stopped", "worker_id", id)
}

func (wp *WorkerPool) executeTask(workerID int, t *task.Task) {
t.Status = task.StatusRunning
t.UpdatedAt = time.Now().UTC()

wp.logger.Info("processing task",
"worker_id", workerID,
"task_id", t.ID,
"task_name", t.Name,
"priority", t.Priority,
"attempt", t.CurrentRetry+1,
)

// Simulate transient failure check
if t.Name == "flaky_third_party_api" {
t.CurrentRetry++
if t.CurrentRetry < t.MaxRetries {
delay := t.NextRetryDelay()
wp.logger.Warn("task execution failed, retrying",
"task_id", t.ID,
"retry_delay", delay.String(),
)
time.Sleep(delay)
wp.executeTask(workerID, t)
return
} else {
wp.logger.Error("task dead-lettered", "task_id", t.ID, "max_retries", t.MaxRetries)
wp.DLQ.Store(t, "exceeded max execution retries")
metrics.Global.IncFailed()
return
}
}

time.Sleep(150 * time.Millisecond)
t.Status = task.StatusCompleted
t.UpdatedAt = time.Now().UTC()
metrics.Global.IncCompleted()

wp.logger.Info("task completed successfully", "worker_id", workerID, "task_id", t.ID)
}

func (wp *WorkerPool) Submit(t *task.Task) {
metrics.Global.IncSubmitted()
wp.TaskStream <- t
}

func (wp *WorkerPool) Stop() {
close(wp.TaskStream)
wp.wg.Wait()
wp.logger.Info("all workers stopped cleanly")
}
