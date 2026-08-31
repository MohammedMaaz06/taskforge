package notifier

import (
"bytes"
"encoding/json"
"log"
"net/http"
"sync"
"time"

"taskforge/pkg/task"
)

type EventType string

const (
EventTaskCompleted EventType = "task.completed"
EventTaskFailed    EventType = "task.failed"
EventTaskDLQ       EventType = "task.dlq"
)

type WebhookPayload struct {
Event     EventType  `json:"event"`
Task      *task.Task `json:"task"`
Timestamp time.Time  `json:"timestamp"`
}

type Notifier struct {
mu        sync.RWMutex
webhooks  []string
client    *http.Client
}

func NewNotifier() *Notifier {
return &Notifier{
webhooks: make([]string, 0),
client:   &http.Client{Timeout: 5 * time.Second},
}
}

func (n *Notifier) Register(url string) {
n.mu.Lock()
defer n.mu.Unlock()
n.webhooks = append(n.webhooks, url)
log.Printf("INFO webhook registered url=%s", url)
}

func (n *Notifier) List() []string {
n.mu.RLock()
defer n.mu.RUnlock()
copied := make([]string, len(n.webhooks))
copy(copied, n.webhooks)
return copied
}

func (n *Notifier) Notify(event EventType, t *task.Task) {
n.mu.RLock()
urls := make([]string, len(n.webhooks))
copy(urls, n.webhooks)
n.mu.RUnlock()

if len(urls) == 0 {
return
}

payload := WebhookPayload{
Event:     event,
Task:      t,
Timestamp: time.Now(),
}

data, err := json.Marshal(payload)
if err != nil {
log.Printf("ERROR failed to marshal webhook payload: %v", err)
return
}

for _, url := range urls {
go func(webhookURL string, body []byte) {
resp, err := n.client.Post(webhookURL, "application/json", bytes.NewBuffer(body))
if err != nil {
log.Printf("ERROR failed to send webhook to %s: %v", webhookURL, err)
return
}
defer resp.Body.Close()
log.Printf("INFO webhook delivered url=%s status=%d", webhookURL, resp.StatusCode)
}(url, data)
}
}

