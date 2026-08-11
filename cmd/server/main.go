package main

import (
"fmt"
"log"
"taskforge/internal/scheduler"
"taskforge/internal/worker"
"taskforge/pkg/task"
)

func main() {
log.Println("Starting TaskForge Engine v0.2...")

// 1. Initialize Safe Priority Queue
queue := scheduler.NewSafeQueue()

// 2. Enqueue sample workload
queue.Push(task.NewTask("101", "export_pdf_report", []byte("pdf_data"), 1))
queue.Push(task.NewTask("102", "process_payment_gate", []byte("payment_data"), 10))
queue.Push(task.NewTask("103", "send_welcome_email", []byte("email_data"), 5))
queue.Push(task.NewTask("104", "database_backup", []byte("backup_data"), 8))

// 3. Initialize Worker Pool with 2 parallel workers
pool := worker.NewWorkerPool(2, 10)
pool.Start()

// 4. Dispatch tasks from Queue to Worker Pool
for queue.Len() > 0 {
t := queue.Pop()
pool.Submit(t)
}

// 5. Graceful shutdown
pool.Stop()
fmt.Printf("Total tasks executed successfully: %d\n", pool.TotalDone)
}
