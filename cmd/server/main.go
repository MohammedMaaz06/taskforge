package main

import (
"fmt"
"log"
"taskforge/internal/scheduler"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

func main() {
log.Println("Starting TaskForge Engine v0.3 (Fault Tolerant)...")

// 1. Initialize DLQ and Queue
dlq := scheduler.NewDLQ()
queue := scheduler.NewSafeQueue()

// 2. Add normal tasks and a failing task
queue.Push(task.NewTask("201", "payment_settlement", []byte("pay_data"), 10))
queue.Push(task.NewTask("202", "flaky_third_party_api", []byte("api_data"), 5))
queue.Push(task.NewTask("203", "cache_invalidation", []byte("cache_data"), 2))

// 3. Initialize Worker Pool with DLQ link
pool := worker.NewWorkerPool(2, 10, dlq)
pool.Start()

// 4. Dispatch tasks
for queue.Len() > 0 {
t := queue.Pop()
pool.Submit(t)
}

// 5. Shutdown and display resilient metrics
pool.Stop()

fmt.Println("\n=== TaskForge Execution Summary ===")
fmt.Printf("Successfully Processed Tasks : %d\n", pool.TotalDone)
fmt.Printf("Dead Letter Queue (DLQ) Tasks : %d\n", dlq.Size())
}
