package worker

import (
"fmt"
"sync"
"sync/atomic"
"time"
"taskforge/internal/scheduler"
"taskforge/pkg/task"
)

type WorkerPool struct {
NumWorkers int
TaskStream chan *task.Task
DLQ        *scheduler.DeadLetterQueue
wg         sync.WaitGroup
TotalDone  int64
TotalDLQ   int64
}

func NewWorkerPool(numWorkers int, bufferSize int, dlq *scheduler.DeadLetterQueue) *WorkerPool {
return &WorkerPool{
NumWorkers: numWorkers,
TaskStream: make(chan *task.Task, bufferSize),
DLQ:        dlq,
}
}

func (wp *WorkerPool) Start() {
for i := 1; i <= wp.NumWorkers; i++ {
wp.wg.Add(1)
go wp.worker(i)
}
fmt.Printf("[%d Workers Initialized and Ready]\n", wp.NumWorkers)
}

func (wp *WorkerPool) worker(id int) {
defer wp.wg.Done()
for t := range wp.TaskStream {
wp.executeTask(id, t)
}
fmt.Printf("Worker %d shut down cleanly.\n", id)
}

func (wp *WorkerPool) executeTask(workerID int, t *task.Task) {
t.Status = task.StatusRunning
t.UpdatedAt = time.Now().UTC()

fmt.Printf("Worker %d processing Task ID: %s (%s) [Attempt %d/%d]\n", 
workerID, t.ID, t.Name, t.CurrentRetry+1, t.MaxRetries)

// Simulate failure for specific flaky tasks
if t.Name == "flaky_third_party_api" {
t.CurrentRetry++
if t.CurrentRetry < t.MaxRetries {
delay := t.NextRetryDelay()
fmt.Printf("Worker %d: Task ID %s failed. Retrying in %v (Exponential Backoff)...\n", workerID, t.ID, delay)
time.Sleep(delay)
wp.executeTask(workerID, t) // Retry
return
} else {
wp.DLQ.Store(t, "exceeded max execution retries")
atomic.AddInt64(&wp.TotalDLQ, 1)
return
}
}

// Normal task execution
time.Sleep(100 * time.Millisecond)
t.Status = task.StatusCompleted
t.UpdatedAt = time.Now().UTC()
atomic.AddInt64(&wp.TotalDone, 1)

fmt.Printf("Worker %d completed Task ID: %s successfully.\n", workerID, t.ID)
}

func (wp *WorkerPool) Submit(t *task.Task) {
wp.TaskStream <- t
}

func (wp *WorkerPool) Stop() {
close(wp.TaskStream)
wp.wg.Wait()
fmt.Println("All workers stopped gracefully.")
}
