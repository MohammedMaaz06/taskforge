package worker

import (
"log/slog"
"os"
"sync"
"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/pkg/task"
"time"
)

type WorkerState struct {
ID        int    `json:"id"`
Busy      bool   `json:"busy"`
CurrentID string `json:"current_task_id,omitempty"`
}

type WorkerPool struct {
NumWorkers int
TaskStream chan *task.Task
DLQ        *scheduler.DeadLetterQueue
Store      *store.SQLiteStore
states     map[int]*WorkerState
stateMu    sync.RWMutex
wg         sync.WaitGroup
logger     *slog.Logger
}

func NewWorkerPool(numWorkers int, bufferSize int, dlq *scheduler.DeadLetterQueue, st *store.SQLiteStore) *WorkerPool {
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
states := make(map[int]*WorkerState)
for i := 1; i <= numWorkers; i++ {
states[i] = &WorkerState{ID: i, Busy: false}
}

return &WorkerPool{
NumWorkers: numWorkers,
TaskStream: make(chan *task.Task, bufferSize),
DLQ:        dlq,
Store:      st,
states:     states,
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

func (wp *WorkerPool) setWorkerBusy(id int, busy bool, taskID string) {
wp.stateMu.Lock()
defer wp.stateMu.Unlock()
if w, ok := wp.states[id]; ok {
w.Busy = busy
w.CurrentID = taskID
}
}

func (wp *WorkerPool) GetWorkerStats() map[string]interface{} {
wp.stateMu.RLock()
defer wp.stateMu.RUnlock()

activeCount := 0
workerDetails := make([]WorkerState, 0, len(wp.states))
for _, w := range wp.states {
if w.Busy {
activeCount++
}
workerDetails = append(workerDetails, *w)
}

return map[string]interface{}{
"total_workers":  wp.NumWorkers,
"active_workers": activeCount,
"queue_depth":    len(wp.TaskStream),
"workers":        workerDetails,
}
}

func (wp *WorkerPool) worker(id int) {
defer wp.wg.Done()
for t := range wp.TaskStream {
wp.setWorkerBusy(id, true, t.ID)
wp.executeTask(id, t)
wp.setWorkerBusy(id, false, "")
}
wp.logger.Info("worker thread stopped", "worker_id", id)
}

func (wp *WorkerPool) executeTask(workerID int, t *task.Task) {
t.Status = task.StatusRunning
t.UpdatedAt = time.Now().UTC()
if wp.Store != nil {
_ = wp.Store.UpdateStatus(t.ID, task.StatusRunning, "")
}

wp.logger.Info("processing task",
"worker_id", workerID,
"task_id", t.ID,
"task_name", t.Name,
"priority", t.Priority,
"attempt", t.CurrentRetry+1,
)

if t.Name == "flaky_third_party_api" || string(t.Payload) == "fail" {
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
t.Status = task.StatusFailed
t.LastError = "exceeded max execution retries"
if wp.Store != nil {
_ = wp.Store.UpdateStatus(t.ID, task.StatusFailed, t.LastError)
}
t.Status = task.StatusDLQ; if wp.Store != nil { _ = wp.Store.Save(t) }; wp.DLQ.Store(t, "exceeded max execution retries")
metrics.Global.IncFailed()
return
}
}

time.Sleep(150 * time.Millisecond)
t.Status = task.StatusCompleted
t.UpdatedAt = time.Now().UTC()
if wp.Store != nil {
_ = wp.Store.UpdateStatus(t.ID, task.StatusCompleted, "")
}
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
