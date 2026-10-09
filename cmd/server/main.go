package main

import (
"encoding/json"
"fmt"
"html/template"
"log"
"net/http"
"os"
"sync/atomic"
"time"

"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/redis/go-redis/v9"
"taskforge/internal/middleware"
"taskforge/internal/task"
)

var activeWorkerCount int64 = 2
var clusterPaused int32 = 0

const dashboardHTML = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TaskForge | Control Center</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        slate: { 850: '#111827', 900: '#0f172a', 950: '#020617' }
                    }
                }
            }
        }
    </script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased">
    <div class="max-w-7xl mx-auto px-6 py-8">
        <!-- Header -->
        <header class="flex items-center justify-between pb-6 mb-8 border-b border-slate-800">
            <div class="flex items-center space-x-3">
                <div class="h-8 w-8 rounded-lg bg-sky-500/10 border border-sky-500/20 flex items-center justify-center text-sky-400 font-bold">TF</div>
                <div>
                    <h1 class="text-xl font-bold tracking-tight text-white">TaskForge Engine</h1>
                    <p class="text-xs text-slate-400">Distributed Orchestration & Telemetry</p>
                </div>
            </div>
            <div class="flex items-center space-x-3">
                <button id="pause-btn" onclick="togglePauseCluster()" class="px-3.5 py-1.5 text-xs font-semibold rounded-lg bg-amber-600/80 hover:bg-amber-500 text-white transition-colors flex items-center space-x-1.5">
                    <span id="pause-btn-text">Pause Cluster</span>
                </button>
                <button onclick="purgeDLQ()" class="px-3.5 py-1.5 text-xs font-semibold rounded-lg bg-rose-600/80 hover:bg-rose-500 text-white transition-colors flex items-center space-x-1.5">
                    <span>Purge DLQ</span>
                </button>
                <button onclick="document.getElementById('ingest-modal').classList.remove('hidden')" class="px-3.5 py-1.5 text-xs font-semibold rounded-lg bg-sky-600 hover:bg-sky-500 text-white transition-colors flex items-center space-x-1.5">
                    <span>+ Dispatch New Task</span>
                </button>
                <span id="cluster-status-pill" class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                    <span id="cluster-pulse" class="w-1.5 h-1.5 rounded-full bg-emerald-400 mr-1.5 animate-pulse"></span>
                    <span id="cluster-status-text">Cluster Active</span>
                </span>
            </div>
        </header>

        <!-- Metric Cards -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-5 mb-8">
            <div class="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Main Queue</p>
                <div class="mt-2 flex items-baseline justify-between">
                    <span class="text-3xl font-extrabold text-white" id="val-main">{{.MainDepth}}</span>
                    <span class="text-xs text-slate-500">pending</span>
                </div>
            </div>
            <div class="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Priority Queue</p>
                <div class="mt-2 flex items-baseline justify-between">
                    <span class="text-3xl font-extrabold text-sky-400" id="val-prio">{{.PrioDepth}}</span>
                    <span class="text-xs text-sky-500/80">high priority</span>
                </div>
            </div>
            <div class="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">DLQ Quarantined</p>
                <div class="mt-2 flex items-baseline justify-between">
                    <span class="text-3xl font-extrabold text-rose-400" id="val-dlq">{{.DLQDepth}}</span>
                    <span class="text-xs text-rose-500/80">isolated</span>
                </div>
            </div>
            <div class="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Active Workers</p>
                <div class="mt-2 flex items-baseline justify-between">
                    <span class="text-3xl font-extrabold text-emerald-400" id="val-workers">{{.WorkerCount}}</span>
                    <div class="flex items-center space-x-1">
                        <button onclick="scaleWorkers('down')" class="px-2 py-0.5 text-xs font-bold rounded bg-slate-800 hover:bg-slate-700 text-slate-300">-</button>
                        <button onclick="scaleWorkers('up')" class="px-2 py-0.5 text-xs font-bold rounded bg-slate-800 hover:bg-slate-700 text-slate-300">+</button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Task Table -->
        <div class="bg-slate-900/60 border border-slate-800/80 rounded-xl overflow-hidden backdrop-blur-sm">
            <div class="px-6 py-4 border-b border-slate-800/80 flex items-center justify-between">
                <h2 class="text-sm font-semibold text-slate-200">Live Task Stream</h2>
                <span class="text-xs text-slate-500">Auto-polling via /ui/data</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-slate-950/50 text-slate-400 text-xs uppercase tracking-wider border-b border-slate-800/80">
                        <tr>
                            <th class="px-6 py-3 font-semibold">Task Identifier</th>
                            <th class="px-6 py-3 font-semibold">Queue Tier</th>
                            <th class="px-6 py-3 font-semibold">State</th>
                            <th class="px-6 py-3 font-semibold text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-800/60 text-slate-300" id="task-table-body">
                        <tr class="hover:bg-slate-800/30 transition-colors">
                            <td class="px-6 py-4 font-mono text-xs text-sky-400">task-prio-1789552325125866500</td>
                            <td class="px-6 py-4"><span class="px-2 py-1 text-xs rounded bg-sky-500/10 text-sky-400 border border-sky-500/20">High Priority</span></td>
                            <td class="px-6 py-4"><span class="px-2 py-1 text-xs rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">priority_enqueued</span></td>
                            <td class="px-6 py-4 text-right">
                                <button onclick="replayDLQ()" class="px-3 py-1.5 text-xs font-semibold rounded-lg bg-sky-600 hover:bg-sky-500 text-white transition-colors">
                                    Replay DLQ Task
                                </button>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Task Ingestion Modal -->
    <div id="ingest-modal" class="hidden fixed inset-0 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 rounded-xl max-w-md w-full p-6 shadow-2xl">
            <div class="flex justify-between items-center pb-4 mb-4 border-b border-slate-800">
                <h3 class="text-base font-bold text-white">Dispatch New Task</h3>
                <button onclick="document.getElementById('ingest-modal').classList.add('hidden')" class="text-slate-400 hover:text-white">&times;</button>
            </div>
            <form id="ingest-form" onsubmit="submitTask(event)" class="space-y-4">
                <div>
                    <label class="block text-xs font-semibold text-slate-400 uppercase mb-1">Queue Tier</label>
                    <select id="queue-tier" class="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-sky-500">
                        <option value="main">Main Queue (Standard)</option>
                        <option value="priority">Priority Queue (Urgent)</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs font-semibold text-slate-400 uppercase mb-1">Task Type / Name</label>
                    <input type="text" id="task-type" value="process_image_job" required class="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-sky-500" />
                </div>
                <div>
                    <label class="block text-xs font-semibold text-slate-400 uppercase mb-1">Payload JSON</label>
                    <textarea id="task-payload" rows="3" class="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-xs font-mono text-slate-200 focus:outline-none focus:border-sky-500">{"user_id": "usr_99", "action": "resize"}</textarea>
                </div>
                <div class="flex justify-end space-x-2 pt-2">
                    <button type="button" onclick="document.getElementById('ingest-modal').classList.add('hidden')" class="px-4 py-2 text-xs font-semibold rounded-lg bg-slate-800 text-slate-300 hover:bg-slate-700">Cancel</button>
                    <button type="submit" class="px-4 py-2 text-xs font-semibold rounded-lg bg-sky-600 hover:bg-sky-500 text-white">Submit Task</button>
                </div>
            </form>
        </div>
    </div>

    <script>
        let isPaused = false;

        async function togglePauseCluster() {
            const endpoint = isPaused ? '/api/v1/cluster/resume' : '/api/v1/cluster/pause';
            try {
                const res = await fetch(endpoint, { method: 'POST' });
                if (res.ok) {
                    const data = await res.json();
                    isPaused = data.paused;
                    updatePauseUI();
                }
            } catch (err) {
                alert('Failed to toggle cluster pause state');
            }
        }

        function updatePauseUI() {
            const btnText = document.getElementById('pause-btn-text');
            const btn = document.getElementById('pause-btn');
            const pill = document.getElementById('cluster-status-pill');
            const text = document.getElementById('cluster-status-text');
            const pulse = document.getElementById('cluster-pulse');

            if (isPaused) {
                btnText.innerText = 'Resume Cluster';
                btn.className = 'px-3.5 py-1.5 text-xs font-semibold rounded-lg bg-emerald-600/80 hover:bg-emerald-500 text-white transition-colors flex items-center space-x-1.5';
                pill.className = 'inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20';
                pulse.className = 'w-1.5 h-1.5 rounded-full bg-amber-400 mr-1.5 animate-pulse';
                text.innerText = 'Cluster Paused';
            } else {
                btnText.innerText = 'Pause Cluster';
                btn.className = 'px-3.5 py-1.5 text-xs font-semibold rounded-lg bg-amber-600/80 hover:bg-amber-500 text-white transition-colors flex items-center space-x-1.5';
                pill.className = 'inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
                pulse.className = 'w-1.5 h-1.5 rounded-full bg-emerald-400 mr-1.5 animate-pulse';
                text.innerText = 'Cluster Active';
            }
        }

        async function scaleWorkers(direction) {
            try {
                const res = await fetch('/api/v1/workers/scale', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: direction })
                });
                const data = await res.json();
                if (res.ok) {
                    document.getElementById('val-workers').innerText = data.workers;
                }
            } catch (err) {
                console.error('Scale error:', err);
            }
        }

        async function submitTask(e) {
            e.preventDefault();
            const tier = document.getElementById('queue-tier').value;
            const type = document.getElementById('task-type').value;
            const payloadRaw = document.getElementById('task-payload').value;

            try {
                const res = await fetch('/api/v1/tasks/ingest', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ tier, type, payload: JSON.parse(payloadRaw) })
                });
                const data = await res.json();
                if (res.ok) {
                    alert('Task Dispatched Successfully: ' + data.id);
                    document.getElementById('ingest-modal').classList.add('hidden');
                } else {
                    alert('Error: ' + data.error);
                }
            } catch (err) {
                alert('Invalid JSON payload or network error');
            }
        }

        async function replayDLQ() {
            try {
                const res = await fetch('/api/v1/dlq/replay', { method: 'POST' });
                const data = await res.json();
                if (res.ok) {
                    alert('Success: ' + data.message);
                } else {
                    alert('Error: ' + data.error);
                }
            } catch (err) {
                alert('Failed to execute DLQ replay request');
            }
        }

        async function purgeDLQ() {
            if (!confirm('Are you sure you want to purge all items from the Dead Letter Queue?')) return;
            try {
                const res = await fetch('/api/v1/dlq/purge', { method: 'DELETE' });
                const data = await res.json();
                if (res.ok) {
                    alert('DLQ Purged: ' + data.message);
                } else {
                    alert('Error: ' + data.error);
                }
            } catch (err) {
                alert('Failed to purge DLQ');
            }
        }

        setInterval(async () => {
            try {
                const res = await fetch('/ui/data');
                if (res.ok) {
                    const data = await res.json();
                    document.getElementById('val-main').innerText = data.main_depth;
                    document.getElementById('val-prio').innerText = data.prio_depth;
                    document.getElementById('val-dlq').innerText = data.dlq_depth;
                    document.getElementById('val-workers').innerText = data.workers;
                    isPaused = data.paused;
                    updatePauseUI();
                }
            } catch (err) {
                console.error('Polling error:', err);
            }
        }, 3000);
    </script>
</body>
</html>`

func authenticate(r *http.Request) bool {
apiKey := r.Header.Get("X-API-Key")
expectedKey := os.Getenv("API_KEY")
if expectedKey == "" {
expectedKey = "tf-worker-secret-key"
}
return apiKey == expectedKey
}

func main() {
redisAddr := os.Getenv("REDIS_ADDR")
if redisAddr == "" {
redisAddr = "localhost:6379"
}

rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
mux := http.NewServeMux()

mux.Handle("/metrics", promhttp.Handler())

mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
w.Write([]byte("OK"))
})

mux.HandleFunc("/ui/data", func(w http.ResponseWriter, r *http.Request) {
mainLen, _ := rdb.LLen(r.Context(), task.QueueMain).Result()
dlqLen, _ := rdb.LLen(r.Context(), task.QueueDLQ).Result()
prioLen, _ := rdb.ZCard(r.Context(), task.QueuePriority).Result()

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"main_depth": mainLen,
"dlq_depth":  dlqLen,
"prio_depth": prioLen,
"workers":    atomic.LoadInt64(&activeWorkerCount),
"paused":     atomic.LoadInt32(&clusterPaused) == 1,
})
})

// Cluster Circuit Breaker Endpoints
mux.HandleFunc("/api/v1/cluster/pause", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
atomic.StoreInt32(&clusterPaused, 1)
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status": "paused",
"paused": true,
})
})

mux.HandleFunc("/api/v1/cluster/resume", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
atomic.StoreInt32(&clusterPaused, 0)
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status": "resumed",
"paused": false,
})
})

// Dynamic Worker Scale Endpoint
mux.HandleFunc("/api/v1/workers/scale", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var req struct {
Action string `json:"action"`
}
if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, "Invalid payload", http.StatusBadRequest)
return
}

if req.Action == "up" {
atomic.AddInt64(&activeWorkerCount, 1)
} else if req.Action == "down" && atomic.LoadInt64(&activeWorkerCount) > 1 {
atomic.AddInt64(&activeWorkerCount, -1)
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status":  "scaled",
"workers": atomic.LoadInt64(&activeWorkerCount),
})
})

// Live Ingestion Endpoint
mux.HandleFunc("/api/v1/tasks/ingest", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

if atomic.LoadInt32(&clusterPaused) == 1 {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusServiceUnavailable)
json.NewEncoder(w).Encode(map[string]string{"error": "Cluster is currently paused"})
return
}

var req struct {
Tier    string                 `json:"tier"`
Type    string                 `json:"type"`
Payload map[string]interface{} `json:"payload"`
}

if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusBadRequest)
json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
return
}

taskID := fmt.Sprintf("task-%s-%d", req.Tier, time.Now().UnixNano())
taskData, _ := json.Marshal(map[string]interface{}{
"id":      taskID,
"type":    req.Type,
"payload": req.Payload,
"created": time.Now().Unix(),
})

w.Header().Set("Content-Type", "application/json")

if req.Tier == "priority" {
pt := task.PriorityTask{
ID:       taskID,
Priority: 10,
Payload:  string(taskData),
}
if err := task.EnqueuePriority(r.Context(), rdb, pt); err != nil {
w.WriteHeader(http.StatusInternalServerError)
json.NewEncoder(w).Encode(map[string]string{"error": "Failed to enqueue priority task"})
return
}
} else {
if err := rdb.LPush(r.Context(), task.QueueMain, taskData).Err(); err != nil {
w.WriteHeader(http.StatusInternalServerError)
json.NewEncoder(w).Encode(map[string]string{"error": "Failed to enqueue main task"})
return
}
}

w.WriteHeader(http.StatusAccepted)
json.NewEncoder(w).Encode(map[string]interface{}{
"status": "enqueued",
"id":     taskID,
"tier":   req.Tier,
})
})

// DLQ Replay Endpoint
mux.HandleFunc("/api/v1/dlq/replay", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

item, err := rdb.RPop(r.Context(), task.QueueDLQ).Result()
if err == redis.Nil {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusBadRequest)
json.NewEncoder(w).Encode(map[string]string{"error": "DLQ is currently empty"})
return
} else if err != nil {
http.Error(w, "Failed to fetch from DLQ", http.StatusInternalServerError)
return
}

if err := rdb.LPush(r.Context(), task.QueueMain, item).Err(); err != nil {
http.Error(w, "Failed to re-enqueue item into main queue", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]string{
"status":  "replayed",
"message": "Task successfully moved from DLQ to Main Queue",
})
})

// DLQ Purge Endpoint
mux.HandleFunc("/api/v1/dlq/purge", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodDelete {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

deleted, err := rdb.Del(r.Context(), task.QueueDLQ).Result()
if err != nil {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusInternalServerError)
json.NewEncoder(w).Encode(map[string]string{"error": "Failed to purge DLQ"})
return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"status":  "purged",
"message": fmt.Sprintf("DLQ successfully cleared (%d queue key reset)", deleted),
})
})

mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
mainLen, _ := rdb.LLen(r.Context(), task.QueueMain).Result()
dlqLen, _ := rdb.LLen(r.Context(), task.QueueDLQ).Result()
prioLen, _ := rdb.ZCard(r.Context(), task.QueuePriority).Result()

tmpl, err := template.New("dashboard").Parse(dashboardHTML)
if err != nil {
http.Error(w, "Template error", http.StatusInternalServerError)
return
}

tmpl.Execute(w, map[string]interface{}{
"MainDepth":   mainLen,
"DLQDepth":    dlqLen,
"PrioDepth":   prioLen,
"WorkerCount": atomic.LoadInt64(&activeWorkerCount),
})
})

mux.HandleFunc("/api/v1/tasks/priority", middleware.RateLimit(rdb, 100, time.Minute, func(w http.ResponseWriter, r *http.Request) {
if !authenticate(r) {
http.Error(w, "Unauthorized", http.StatusUnauthorized)
return
}
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}

var pt task.PriorityTask
if err := json.NewDecoder(r.Body).Decode(&pt); err != nil {
http.Error(w, "Invalid payload", http.StatusBadRequest)
return
}
if pt.ID == "" {
pt.ID = fmt.Sprintf("task-prio-%d", time.Now().UnixNano())
}

if err := task.EnqueuePriority(r.Context(), rdb, pt); err != nil {
http.Error(w, "Failed to enqueue priority task", http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusAccepted)
json.NewEncoder(w).Encode(map[string]interface{}{
"status":   "priority_enqueued",
"id":       pt.ID,
"priority": pt.Priority,
})
}))

log.Println("TaskForge Control Server running on :8080...")
log.Fatal(http.ListenAndServe(":8080", mux))
}
