package task

import (
"encoding/json"
"net/http"
"time"
)

type BatchTaskRequest struct {
Tasks []Task `json:"tasks"`
}

type BatchTaskResponse struct {
Ingested int      `json:"ingested"`
IDs      []string `json:"ids"`
Status   string   `json:"status"`
}

func HandleBatchIngest(tasks []*Task, saveBatchFunc func([]*Task) error) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var req BatchTaskRequest
if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
return
}

now := time.Now()
batch := make([]*Task, 0, len(req.Tasks))
ids := make([]string, 0, len(req.Tasks))

for i := range req.Tasks {
t := &req.Tasks[i]
if t.Status == "" {
t.Status = "PENDING"
}
t.CreatedAt = now
t.UpdatedAt = now
batch = append(batch, t)
ids = append(ids, t.ID)
}

if err := saveBatchFunc(batch); err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusCreated)
json.NewEncoder(w).Encode(BatchTaskResponse{
Ingested: len(batch),
IDs:      ids,
Status:   "SUCCESS",
})
}
}
