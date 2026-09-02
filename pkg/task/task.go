package task

import (
"crypto/rand"
"encoding/hex"
"time"
)

type Status string

const (
StatusPending   Status = "PENDING"
StatusWaiting   Status = "WAITING"
StatusRunning   Status = "RUNNING"
StatusCompleted Status = "COMPLETED"
StatusFailed    Status = "FAILED"
StatusDLQ       Status = "DLQ"
StatusBlocked   Status = "BLOCKED"
)

type Task struct {
ID              string    `json:"id"`
Name            string    `json:"name"`
Payload         string    `json:"payload"`
Status          Status    `json:"status"`
Priority        int       `json:"priority"`
MaxRetries      int       `json:"max_retries"`
CurrentRetry    int       `json:"current_retry"`
LastError       string    `json:"last_error,omitempty"`
ScheduledAt     time.Time `json:"scheduled_at"`
IntervalSeconds int       `json:"interval_seconds,omitempty"`
DependsOn       []string  `json:"depends_on,omitempty"`
CreatedAt       time.Time `json:"created_at"`
UpdatedAt       time.Time `json:"updated_at"`
}

func generateID() string {
b := make([]byte, 8)
rand.Read(b)
return "task-" + hex.EncodeToString(b)
}

func NewTask(name string, priority int, maxRetries int) *Task {
now := time.Now()
return &Task{
ID:           generateID(),
Name:         name,
Status:       StatusPending,
Priority:     priority,
MaxRetries:   maxRetries,
CurrentRetry: 0,
ScheduledAt:  now,
DependsOn:    []string{},
CreatedAt:    now,
UpdatedAt:    now,
}
}

