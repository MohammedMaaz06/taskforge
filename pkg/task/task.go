package task

import (
"crypto/rand"
"encoding/hex"
"fmt"
"time"
)

type Status string

const (
StatusPending   Status = "PENDING"
StatusRunning   Status = "RUNNING"
StatusCompleted Status = "COMPLETED"
StatusFailed    Status = "FAILED"
StatusDLQ       Status = "DLQ"
)

type Task struct {
ID           string    `json:"id"`
Name         string    `json:"name"`
Payload      []byte    `json:"payload"`
Priority     int       `json:"priority"`
Status       Status    `json:"status"`
MaxRetries   int       `json:"max_retries"`
CurrentRetry int       `json:"current_retry"`
LastError    string    `json:"last_error,omitempty"`
ScheduledAt  time.Time `json:"scheduled_at"`
CreatedAt    time.Time `json:"created_at"`
UpdatedAt    time.Time `json:"updated_at"`
}

func GenerateID() string {
b := make([]byte, 8)
_, err := rand.Read(b)
if err != nil {
return fmt.Sprintf("task-%d", time.Now().UnixNano())
}
return hex.EncodeToString(b)
}

func NewTask(id string, name string, payload []byte, priority int) *Task {
now := time.Now()
if id == "" {
id = GenerateID()
}
return &Task{
ID:           id,
Name:         name,
Payload:      payload,
Priority:     priority,
Status:       StatusPending,
MaxRetries:   3,
CurrentRetry: 0,
ScheduledAt:  now,
CreatedAt:    now,
UpdatedAt:    now,
}
}

