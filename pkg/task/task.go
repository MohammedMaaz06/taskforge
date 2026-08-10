package task

import (
"time"
)

type Status string

const (
StatusPending   Status = "PENDING"
StatusRunning   Status = "RUNNING"
StatusCompleted Status = "COMPLETED"
StatusFailed    Status = "FAILED"
)

// Task represents a unit of work within TaskForge
type Task struct {
ID           string    `json:"id"`
Name         string    `json:"name"`
Payload      []byte    `json:"payload"`
Status       Status    `json:"status"`
Priority     int       `json:"priority"` // Higher value = higher priority
MaxRetries   int       `json:"max_retries"`
CurrentRetry int       `json:"current_retry"`
ScheduledAt  time.Time `json:"scheduled_at"`
CreatedAt    time.Time `json:"created_at"`
UpdatedAt    time.Time `json:"updated_at"`
}

func NewTask(id, name string, payload []byte, priority int) *Task {
now := time.Now().UTC()
return &Task{
ID:           id,
Name:         name,
Payload:      payload,
Status:       StatusPending,
Priority:     priority,
MaxRetries:   3,
CurrentRetry: 0,
ScheduledAt:  now,
CreatedAt:    now,
UpdatedAt:    now,
}
}
