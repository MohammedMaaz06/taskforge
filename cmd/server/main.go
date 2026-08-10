package main

import (
"fmt"
"log"
"taskforge/internal/scheduler"
"taskforge/pkg/task"
)

func main() {
log.Println("Starting TaskForge Engine...")

queue := scheduler.NewSafeQueue()

// Enqueue tasks with varying priorities
queue.Push(task.NewTask("1", "low_priority_job", []byte("data1"), 1))
queue.Push(task.NewTask("2", "critical_job", []byte("data2"), 10))
queue.Push(task.NewTask("3", "medium_priority_job", []byte("data3"), 5))

fmt.Printf("Queued %d tasks.\n", queue.Len())

// Dequeue - Should process critical_job (10) -> medium (5) -> low (1)
for queue.Len() > 0 {
t := queue.Pop()
fmt.Printf("Processing Task ID: %s | Name: %s | Priority: %d\n", t.ID, t.Name, t.Priority)
}
}
