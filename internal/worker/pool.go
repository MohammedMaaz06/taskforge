package worker

import (
"fmt"
"sync"
"sync/atomic"
"time"
"taskforge/pkg/task"
)

type WorkerPool struct {
NumWorkers int
TaskStream chan *task.Task
wg         sync.WaitGroup
TotalDone  int64
}

func NewWorkerPool(numWorkers int, bufferSize int) *WorkerPool {
return &WorkerPool{
NumWorkers: numWorkers,
TaskStream: make(chan *task.Task, bufferSize),
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
t.Status = task.StatusRunning
t.UpdatedAt = time.Now().UTC()

fmt.Printf("Worker %d processing Task ID: %s (%s) [Priority: %d]\n", id, t.ID, t.Name, t.Priority)

// Simulate execution time
time.Sleep(200 * time.Millisecond)

t.Status = task.StatusCompleted
t.UpdatedAt = time.Now().UTC()
atomic.AddInt64(&wp.TotalDone, 1)

fmt.Printf("Worker %d completed Task ID: %s\n", id, t.ID)
}
fmt.Printf("Worker %d shut down cleanly.\n", id)
}

func (wp *WorkerPool) Submit(t *task.Task) {
wp.TaskStream <- t
}

func (wp *WorkerPool) Stop() {
close(wp.TaskStream) // Close channel to signal workers to exit loop
wp.wg.Wait()        // Wait for all worker goroutines to finish
fmt.Println("All workers stopped gracefully.")
}
