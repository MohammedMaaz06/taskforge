package websocket

import (
"net/http/httptest"
"strings"
"testing"

"github.com/gorilla/websocket"
"taskforge/pkg/task"
)

func TestHub_BroadcastTaskUpdate(t *testing.T) {
hub := NewHub()
go hub.Run()

server := httptest.NewServer(hub)
defer server.Close()

url := "ws" + strings.TrimPrefix(server.URL, "http")

ws, _, err := websocket.DefaultDialer.Dial(url, nil)
if err != nil {
t.Fatalf("Failed to connect to WebSocket server: %v", err)
}
defer ws.Close()

testTask := &task.Task{
ID:   "task-123",
Name: "test-broadcast",
}

hub.BroadcastTaskUpdate(testTask, "RUNNING")

var event Event
err = ws.ReadJSON(&event)
if err != nil {
t.Fatalf("Failed to read WebSocket message: %v", err)
}

if event.Type != "TASK_STATUS_UPDATE" {
t.Errorf("Expected event type TASK_STATUS_UPDATE, got %s", event.Type)
}

payload, ok := event.Payload.(map[string]interface{})
if !ok {
t.Fatalf("Expected map payload, got %T", event.Payload)
}

if payload["task_id"] != "task-123" {
t.Errorf("Expected task_id task-123, got %v", payload["task_id"])
}

if payload["status"] != "RUNNING" {
t.Errorf("Expected status RUNNING, got %v", payload["status"])
}
}

