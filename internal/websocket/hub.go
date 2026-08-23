package websocket

import (
"net/http"
"sync"

"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
CheckOrigin: func(r *http.Request) bool { return true },
}

type Event struct {
Type    string      `json:"type"`
Payload interface{} `json:"payload"`
}

type Hub struct {
clients   map[*websocket.Conn]bool
broadcast chan Event
mu        sync.Mutex
}

func NewHub() *Hub {
return &Hub{
clients:   make(map[*websocket.Conn]bool),
broadcast: make(chan Event, 100),
}
}

func (h *Hub) Run() {
for event := range h.broadcast {
h.mu.Lock()
for client := range h.clients {
err := client.WriteJSON(event)
if err != nil {
client.Close()
delete(h.clients, client)
}
}
h.mu.Unlock()
}
}

func (h *Hub) Broadcast(eventType string, payload interface{}) {
h.broadcast <- Event{Type: eventType, Payload: payload}
}

func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
conn, err := upgrader.Upgrade(w, r, nil)
if err != nil {
return
}

h.mu.Lock()
h.clients[conn] = true
h.mu.Unlock()
}
