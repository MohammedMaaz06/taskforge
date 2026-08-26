package task

import "time"

type Status string

const (
StatusPending   Status = "PENDING"
StatusRunning   Status = "RUNNING"
StatusCompleted Status = "COMPLETED"
StatusFailed    Status = "FAILED"
StatusDLQ       Status = "DLQ"
)

type Task struct {
ID           string        `json:"id"`
Name         string        `json:"name"`
Payload      []byte        `json:"payload"`
Priority     int           `json:"priority"`
Status       Status        `json:"status"`
MaxRetries   int           `json:"max_retries"`
CurrentRetry int           `json:"current_retry"`
LastError    string        `json:"last_error"`
ScheduledAt  time.Time     `json:"scheduled_at"`
CreatedAt    time.Time     `json:"created_at"`
UpdatedAt    time.Time     `json:"updated_at"`
Index        int           `json:"index"`
}

func NewTask(id, name string, payload []byte, priority int) *Task {
now := time.Now()
return &Task{
ID:         id,
Name:       name,
Payload:    payload,
Priority:   priority,
Status:     StatusPending,
MaxRetries: 3,
CreatedAt:  now,
UpdatedAt:  now,
}
}

func (t *Task) NextRetryDelay() time.Duration {
return time.Duration(1<<t.CurrentRetry) * time.Second
}

