package websocket

import (
"log"
"net/http"
"sync"

"github.com/gorilla/websocket"
"taskforge/pkg/task"
)

var upgrader = websocket.Upgrader{
ReadBufferSize:  1024,
WriteBufferSize: 1024,
CheckOrigin: func(r *http.Request) bool {
return true // Allow all connections for local dev testing
},
}

type Event struct {
Type    string      `json:"type"`
Payload interface{} `json:"payload"`
}

type Hub struct {
clients    map[*websocket.Conn]bool
broadcast  chan Event
register   chan *websocket.Conn
unregister chan *websocket.Conn
mu         sync.Mutex
}

func NewHub() *Hub {
return &Hub{
clients:    make(map[*websocket.Conn]bool),
broadcast:  make(chan Event, 256),
register:   make(chan *websocket.Conn),
unregister: make(chan *websocket.Conn),
}
}

func (h *Hub) Run() {
for {
select {
case conn := <-h.register:
h.mu.Lock()
h.clients[conn] = true
h.mu.Unlock()
log.Println("WebSocket client connected")

case conn := <-h.unregister:
h.mu.Lock()
if _, ok := h.clients[conn]; ok {
delete(h.clients, conn)
conn.Close()
log.Println("WebSocket client disconnected")
}
h.mu.Unlock()

case event := <-h.broadcast:
h.mu.Lock()
for conn := range h.clients {
err := conn.WriteJSON(event)
if err != nil {
log.Printf("WebSocket write error: %v", err)
conn.Close()
delete(h.clients, conn)
}
}
h.mu.Unlock()
}
}
}

func (h *Hub) BroadcastTaskUpdate(t *task.Task, status string) {
h.broadcast <- Event{
Type: "TASK_STATUS_UPDATE",
Payload: map[string]interface{}{
"task_id": t.ID,
"name":    t.Name,
"status":  status,
"error":   t.LastError,
},
}
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
conn, err := upgrader.Upgrade(w, r, nil)
if err != nil {
log.Printf("Failed to upgrade connection: %v", err)
return
}
h.register <- conn

go func() {
defer func() {
h.unregister <- conn
}()
for {
_, _, err := conn.ReadMessage()
if err != nil {
break
}
}
}()
}

