package task

import (
"crypto/rand"
"fmt"
"time"
)

type Status string

const (
StatusPending   Status = "PENDING"
StatusRunning   Status = "RUNNING"
StatusCompleted Status = "COMPLETED"
StatusFailed    Status = "FAILED"
StatusCancelled Status = "CANCELLED"
StatusDLQ       Status = "DLQ"
)

type Task struct {
ID           string    `json:"id"`
Name         string    `json:"name"`
Payload      string    `json:"payload"`
Status       Status    `json:"status"`
Priority     int       `json:"priority"`
MaxRetries   int       `json:"max_retries"`
CurrentRetry int       `json:"current_retry"`
LastError    string    `json:"last_error,omitempty"`
ScheduledAt  time.Time `json:"scheduled_at"`
CreatedAt    time.Time `json:"created_at"`
UpdatedAt    time.Time `json:"updated_at"`
}

func NewTask(name string, priority, maxRetries int) *Task {
now := time.Now()
return &Task{
ID:           GenerateID(),
Name:         name,
Status:       StatusPending,
Priority:     priority,
MaxRetries:   maxRetries,
CurrentRetry: 0,
ScheduledAt:  now,
CreatedAt:    now,
UpdatedAt:    now,
}
}

func GenerateID() string {
b := make([]byte, 8)
rand.Read(b)
return fmt.Sprintf("task-%x", b)
}

