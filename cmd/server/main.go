package main

import (
"bytes"
"crypto/hmac"
"crypto/sha256"
"encoding/hex"
"encoding/json"
"fmt"
"html/template"
"log"
"net/http"
"os"
"runtime"
"sync"
"sync/atomic"
"time"

"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/redis/go-redis/v9"
"taskforge/internal/middleware"
"taskforge/internal/task"
)

var activeWorkerCount int64 = 2
var clusterPaused int32 = 0

var (
webhookMu       sync.RWMutex
registeredHooks = []string{"http://localhost:9000/webhook/receiver"}
)

func dispatchWebhook(event string, payload interface{}) {
webhookMu.RLock()
hooks := make([]string, len(registeredHooks))
copy(hooks, registeredHooks)
webhookMu.RUnlock()

bodyMap := map[string]interface{}{
"event":     event,
"timestamp": time.Now().Unix(),
"data":      payload,
}
bodyBytes, _ := json.Marshal(bodyMap)
secret := "tf-webhook-secret-key"

for _, hookURL := range hooks {
go func(url string) {
req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
if err != nil {
return
}
req.Header.Set("Content-Type", "application/json")
mac := hmac.New(sha256.New, []byte(secret))
mac.Write(bodyBytes)
signature := hex.EncodeToString(mac.Sum(nil))
req.Header.Set("X-TaskForge-Signature", "sha256="+signature)

client := &http.Client{Timeout: 2 * time.Second}
_, _ = client.Do(req)
}(hookURL)
}
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TaskForge | Enterprise Control Center</title>
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
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased selection:bg-sky-500 selection:text-white">
    <div class="max-w-7xl mx-auto px-6 py-8">
        <!-- Header -->
        <header class="flex flex-col md:flex-row items-start md:items-center justify-between pb-6 mb-8 border-b border-slate-800/80 gap-4">
            <div class="flex items-center space-x-3.5">
                <div class="h-10 w-10 rounded-xl bg-gradient-to-br from-sky-500/20 to-indigo-500/20 border border-sky-500/30 flex items-center justify-center text-sky-400 font-bold shadow-lg shadow-sky-950/50">TF</div>
                <div>
                    <h1 class="text-xl font-extrabold tracking-tight text-white flex items-center gap-2">
                        TaskForge <span class="text-xs px-2 py-0.5 rounded-full bg-sky-500/10 text-sky-400 border border-sky-500/20 font-medium">Enterprise v2.5</span>
                    </h1>
                    <p class="text-xs text-slate-400">Distributed Orchestration & Real-Time Filtering</p>
                </div>
            </div>
            <div class="flex flex-wrap items-center gap-2.5">
                <button onclick="testWebhook()" class="px-3.5 py-2 text-xs font-semibold rounded-xl bg-slate-900/80 hover:bg-slate-800 text-purple-400 border border-purple-500/20 hover:border-purple-500/40 transition-all shadow-sm flex items-center space-x-1.5">
                    <span>⚡ Test Webhook</span>
                </button>
                <button id="pause-btn" onclick="togglePauseCluster()" class="px-3.5 py-2 text-xs font-semibold rounded-xl bg-slate-900/80 hover:bg-slate-800 text-amber-400 border border-amber-500/20 hover:border-amber-500/40 transition-all shadow-sm flex items-center space-x-1.5">
                    <span id="pause-btn-text">⏸ Pause Cluster</span>
                </button>
                <button onclick="purgeDLQ()" class="px-3.5 py-2 text-xs font-semibold rounded-xl bg-slate-900/80 hover:bg-slate-800 text-rose-400 border border-rose-500/20 hover:border-rose-500/40 transition-all shadow-sm flex items-center space-x-1.5">
                    <span>🗑 Purge DLQ</span>
                </button>
                <button onclick="document.getElementById('ingest-modal').classList.remove('hidden')" class="px-4 py-2 text-xs font-semibold rounded-xl bg-gradient-to-r from-sky-600 to-indigo-600 hover:from-sky-500 hover:to-indigo-500 text-white transition-all shadow-lg shadow-sky-900/20 flex items-center space-x-1.5">
                    <span>+ Dispatch Task</span>
                </button>
                <span id="cluster-status-pill" class="inline-flex items-center px-3 py-1.5 rounded-xl text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 shadow-sm">
                    <span id="cluster-pulse" class="w-2 h-2 rounded-full bg-emerald-400 mr-2 animate-pulse"></span>
                    <span id="cluster-status-text">Cluster Active</span>
                </span>
            </div>
        </header>

        <!-- Metric Cards -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-5 mb-8">
            <div class="bg-slate-900/40 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-md shadow-xl">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Main Queue</p>
                <div class="mt-3 flex items-baseline justify-between">
                    <span class="text-3xl font-black text-white tracking-tight" id="val-main">{{.MainDepth}}</span>
                    <span class="text-xs text-slate-500 font-medium">pending items</span>
                </div>
            </div>
            <div class="bg-slate-900/40 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-md shadow-xl">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Priority Queue</p>
                <div class="mt-3 flex items-baseline justify-between">
                    <span class="text-3xl font-black text-sky-400 tracking-tight" id="val-prio">{{.PrioDepth}}</span>
                    <span class="text-xs text-sky-500/80 font-medium">high priority</span>
                </div>
            </div>
            <div class="bg-slate-900/40 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-md shadow-xl">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">DLQ Quarantined</p>
                <div class="mt-3 flex items-baseline justify-between">
                    <span class="text-3xl font-black text-rose-400 tracking-tight" id="val-dlq">{{.DLQDepth}}</span>
                    <span class="text-xs text-rose-500/80 font-medium">isolated</span>
                </div>
            </div>
            <div class="bg-slate-900/40 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-md shadow-xl">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Active Workers</p>
                <div class="mt-3 flex items-baseline justify-between">
                    <span class="text-3xl font-black text-emerald-400 tracking-tight" id="val-workers">{{.WorkerCount}}</span>
                    <div class="flex items-center space-x-1.5 bg-slate-950/60 p-1 rounded-xl border border-slate-800">
                        <button onclick="scaleWorkers('down')" class="px-2.5 py-1 text-xs font-bold rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors">-</button>
                        <button onclick="scaleWorkers('up')" class="px-2.5 py-1 text-xs font-bold rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors">+</button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Telemetry Grid -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-5 mb-8">
            <div class="bg-slate-900/30 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Heap Memory Alloc</p>
                <p class="text-2xl font-black text-sky-400 mt-2 tracking-tight" id="val-mem">-- MB</p>
                <p class="text-xs text-slate-500 mt-1">Runtime RAM Footprint</p>
            </div>
            <div class="bg-slate-900/30 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Active Goroutines</p>
                <p class="text-2xl font-black text-emerald-400 mt-2 tracking-tight" id="val-goroutines">--</p>
                <p class="text-xs text-slate-500 mt-1">Lightweight Execution Threads</p>
            </div>
            <div class="bg-slate-900/30 border border-slate-800/80 rounded-2xl p-5 backdrop-blur-sm">
                <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">GC Cycles</p>
                <p class="text-2xl font-black text-purple-400 mt-2 tracking-tight" id="val-gc">--</p>
                <p class="text-xs text-slate-500 mt-1">Garbage Collector Passes</p>
            </div>
        </div>

        <!-- Task Table with Search & Filter Bar -->
        <div class="bg-slate-900/40 border border-slate-800/80 rounded-2xl overflow-hidden backdrop-blur-md shadow-2xl">
            <div class="px-6 py-4 border-b border-slate-800/80 flex flex-col md:flex-row items-center justify-between bg-slate-900/60 gap-3">
                <h2 class="text-sm font-bold text-slate-200">Live Task Stream</h2>
                <div class="flex items-center space-x-3 w-full md:w-auto">
                    <input type="text" id="task-search" oninput="filterTasks()" placeholder="Search tasks by ID..." class="bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-sky-500 w-full md:w-64" />
                    <select id="task-filter" onchange="filterTasks()" class="bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-slate-300 focus:outline-none focus:border-sky-500">
                        <option value="all">All Tiers</option>
                        <option value="priority">High Priority</option>
                        <option value="main">Main Queue</option>
                    </select>
                </div>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-slate-950/60 text-slate-400 text-xs uppercase tracking-wider border-b border-slate-800/80">
                        <tr>
                            <th class="px-6 py-3.5 font-semibold">Task Identifier</th>
                            <th class="px-6 py-3.5 font-semibold">Queue Tier</th>
                            <th class="px-6 py-3.5 font-semibold">State</th>
                            <th class="px-6 py-3.5 font-semibold text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-800/60 text-slate-300" id="task-table-body">
                        <tr class="task-row hover:bg-slate-800/30 transition-colors" data-id="task-prio-1789552325125866500" data-tier="priority">
                            <td class="px-6 py-4 font-mono text-xs text-sky-400">task-prio-1789552325125866500</td>
                            <td class="px-6 py-4"><span class="px-2.5 py-1 text-xs font-medium rounded-lg bg-sky-500/10 text-sky-400 border border-sky-500/20">High Priority</span></td>
                            <td class="px-6 py-4"><span class="px-2.5 py-1 text-xs font-medium rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">priority_enqueued</span></td>
                            <td class="px-6 py-4 text-right">
                                <button onclick="replayDLQ()" class="px-3.5 py-1.5 text-xs font-semibold rounded-xl bg-sky-600 hover:bg-sky-500 text-white transition-all shadow-sm">
                                    Replay Task
                                </button>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Task Ingestion Modal -->
    <div id="ingest-modal" class="hidden fixed inset-0 bg-slate-950/80 backdrop-blur-md flex items-center justify-center p-4 z-50">
        <div class="bg-slate-900 border border-slate-800 rounded-2xl max-w-md w-full p-6 shadow-2xl">
            <div class="flex justify-between items-center pb-4 mb-4 border-b border-slate-800">
                <h3 class="text-base font-bold text-white">Dispatch New Task</h3>
                <button onclick="document.getElementById('ingest-modal').classList.add('hidden')" class="text-slate-400 hover:text-white text-lg">&times;</button>
            </div>
            <form id="ingest-form" onsubmit="submitTask(event)" class="space-y-4">
                <div>
                    <label class="block text-xs font-semibold text-slate-400 uppercase mb-1.5">Queue Tier</label>
                    <select id="queue-tier" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3.5 py-2.5 text-sm text-slate-200 focus:outline-none focus:border-sky-500">
                        <option value="main">Main Queue (Standard)</option>
                        <option value="priority">Priority Queue (Urgent)</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs font-semibold text-slate-400 uppercase mb-1.5">Task Type / Name</label>
                    <input type="text" id="task-type" value="process_image_job" required class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3.5 py-2.5 text-sm text-slate-200 focus:outline-none focus:border-sky-500" />
                </div>
                <div>
                    <label class="block text-xs font-semibold text-slate-400 uppercase mb-1.5">Payload JSON</label>
                    <textarea id="task-payload" rows="3" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3.5 py-2.5 text-xs font-mono text-slate-200 focus:outline-none focus:border-sky-500">{"user_id": "usr_99", "action": "resize"}</textarea>
                </div>
                <div class="flex justify-end space-x-2 pt-2">
                    <button type="button" onclick="document.getElementById('ingest-modal').classList.add('hidden')" class="px-4 py-2 text-xs font-semibold rounded-xl bg-slate-800 text-slate-300 hover:bg-slate-700 transition-colors">Cancel</button>
                    <button type="submit" class="px-4 py-2 text-xs font-semibold rounded-xl bg-sky-600 hover:bg-sky-500 text-white transition-colors shadow-lg shadow-sky-900/20">Submit Task</button>
                </div>
            </form>
        </div>
    </div>

    <script>
        let isPaused = false;

        function filterTasks() {
            const query = document.getElementById('task-search').value.toLowerCase();
            const tierFilter = document.getElementById('task-filter').value;
            const rows = document.querySelectorAll('.task-row');

            rows.forEach(row => {
                const id = row.getAttribute('data-id').toLowerCase();
                const tier = row.getAttribute('data-tier');
                const matchesSearch = id.includes(query);
                const matchesTier = (tierFilter === 'all' || tier === tierFilter);

                if (matchesSearch && matchesTier) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        }

        async function testWebhook() {
            try {
                const res = await fetch('/api/v1/webhooks/test', { method: 'POST' });
                const data = await res.json();
                alert(data.message || 'Test webhook dispatched');
            } catch (err) {
                alert('Failed to trigger webhook test');
            }
        }

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
            const pill = document.getElementById('cluster-status-pill');
            const text = document.getElementById('cluster-status-text');
            const pulse = document.getElementById('cluster-pulse');

            if (isPaused) {
                btnText.innerText = '▶ Resume Cluster';
                pill.className = 'inline-flex items-center px-3 py-1.5 rounded-xl text-xs font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20 shadow-sm';
                pulse.className = 'w-2 h-2 rounded-full bg-amber-400 mr-2 animate-pulse';
                text.innerText = 'Cluster Paused';
            } else {
                btnText.innerText = '⏸ Pause Cluster';
                pill.className = 'inline-flex items-center px-3 py-1.5 rounded-xl text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 shadow-sm';
                pulse.className = 'w-2 h-2 rounded-full bg-emerald-400 mr-2 animate-pulse';
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
                } else if (res.status === 429) {
                    alert('Rate Limit Exceeded: Too many ingestion requests.');
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

                const tRes = await fetch('/api/v1/telemetry');
                if (tRes.ok) {
                    const tData = await tRes.json();
                    document.getElementById('val-mem').innerText = (tData.alloc_bytes / (1024 * 1024)).toFixed(2) + ' MB';
                    document.getElementById('val-goroutines').innerText = tData.goroutines;
                    document.getElementById('val-gc').innerText = tData.gc_cycles;
                }
            } catch (err) {
                console.error('Polling error:', err);
            }
        }, 2000);
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

mux.HandleFunc("/api/v1/webhooks/test", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
dispatchWebhook("task.test", map[string]string{"message": "Hello from TaskForge Webhook Engine!"})
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]string{
"status":  "dispatched",
"message": "Test webhook event successfully queued for broadcast",
})
})

mux.HandleFunc("/api/v1/telemetry", func(w http.ResponseWriter, r *http.Request) {
var m runtime.MemStats
runtime.ReadMemStats(&m)

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{
"goroutines":  runtime.NumGoroutine(),
"alloc_bytes": m.Alloc,
"sys_bytes":   m.Sys,
"gc_cycles":   m.NumGC,
})
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

mux.HandleFunc("/api/v1/cluster/pause", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
atomic.StoreInt32(&clusterPaused, 1)
dispatchWebhook("cluster.paused", map[string]bool{"paused": true})
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"status": "paused", "paused": true})
})

mux.HandleFunc("/api/v1/cluster/resume", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
atomic.StoreInt32(&clusterPaused, 0)
dispatchWebhook("cluster.resumed", map[string]bool{"paused": false})
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"status": "resumed", "paused": false})
})

mux.HandleFunc("/api/v1/workers/scale", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
var req struct {
Action string `json:"action"`
}
json.NewDecoder(r.Body).Decode(&req)
if req.Action == "up" {
atomic.AddInt64(&activeWorkerCount, 1)
} else if req.Action == "down" && atomic.LoadInt64(&activeWorkerCount) > 1 {
atomic.AddInt64(&activeWorkerCount, -1)
}
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"status": "scaled", "workers": atomic.LoadInt64(&activeWorkerCount)})
})

mux.HandleFunc("/api/v1/tasks/ingest", middleware.RateLimit(rdb, 250, time.Minute, func(w http.ResponseWriter, r *http.Request) {
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
json.NewEncoder(w).Encode(map[string]string{"error": "Invalid payload"})
return
}

taskID := fmt.Sprintf("task-%s-%d", req.Tier, time.Now().UnixNano())
taskData, _ := json.Marshal(map[string]interface{}{
"id":      taskID,
"type":    req.Type,
"payload": req.Payload,
"created": time.Now().Unix(),
})

if req.Tier == "priority" {
pt := task.PriorityTask{ID: taskID, Priority: 10, Payload: string(taskData)}
task.EnqueuePriority(r.Context(), rdb, pt)
} else {
rdb.LPush(r.Context(), task.QueueMain, taskData)
}

dispatchWebhook("task.enqueued", map[string]string{"id": taskID, "tier": req.Tier})

w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusAccepted)
json.NewEncoder(w).Encode(map[string]interface{}{"status": "enqueued", "id": taskID, "tier": req.Tier})
}))

mux.HandleFunc("/api/v1/dlq/replay", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodPost {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
item, err := rdb.RPop(r.Context(), task.QueueDLQ).Result()
if err == redis.Nil {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusBadRequest)
json.NewEncoder(w).Encode(map[string]string{"error": "DLQ is empty"})
return
}
rdb.LPush(r.Context(), task.QueueMain, item)
dispatchWebhook("task.replayed", map[string]string{"source": "dlq"})
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]string{"status": "replayed", "message": "Task moved from DLQ to Main"})
})

mux.HandleFunc("/api/v1/dlq/purge", func(w http.ResponseWriter, r *http.Request) {
if r.Method != http.MethodDelete {
http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
return
}
rdb.Del(r.Context(), task.QueueDLQ)
dispatchWebhook("dlq.purged", map[string]bool{"purged": true})
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]interface{}{"status": "purged", "message": "DLQ cleared"})
})

mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
mainLen, _ := rdb.LLen(r.Context(), task.QueueMain).Result()
dlqLen, _ := rdb.LLen(r.Context(), task.QueueDLQ).Result()
prioLen, _ := rdb.ZCard(r.Context(), task.QueuePriority).Result()

tmpl, _ := template.New("dashboard").Parse(dashboardHTML)
tmpl.Execute(w, map[string]interface{}{
"MainDepth":   mainLen,
"DLQDepth":    dlqLen,
"PrioDepth":   prioLen,
"WorkerCount": atomic.LoadInt64(&activeWorkerCount),
})
})

log.Println("TaskForge Next-Gen Server running on :8080...")
log.Fatal(http.ListenAndServe(":8080", mux))
}
