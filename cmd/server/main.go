package main

import (
"encoding/json"
"fmt"
"html/template"
"log"
"net/http"
"os"
"time"

"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/redis/go-redis/v9"
"taskforge/internal/middleware"
"taskforge/internal/task"
)

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
                <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                    <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 mr-1.5 animate-pulse"></span>
                    Cluster Active
                </span>
                <span class="px-2.5 py-1 rounded-md text-xs font-semibold bg-slate-800 text-slate-300 border border-slate-700">v7.0 Pro</span>
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
                    <span class="text-3xl font-extrabold text-emerald-400">2</span>
                    <span class="text-xs text-emerald-500/80">online</span>
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
                    <tbody class="divide-y divide-slate-800/60 text-slate-300">
                        <tr class="hover:bg-slate-800/30 transition-colors">
                            <td class="px-6 py-4 font-mono text-xs text-sky-400">task-prio-1789552325125866500</td>
                            <td class="px-6 py-4"><span class="px-2 py-1 text-xs rounded bg-sky-500/10 text-sky-400 border border-sky-500/20">High Priority</span></td>
                            <td class="px-6 py-4"><span class="px-2 py-1 text-xs rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">priority_enqueued</span></td>
                            <td class="px-6 py-4 text-right">
                                <button onclick="alert('Triggering DLQ Replay...')" class="px-3 py-1.5 text-xs font-semibold rounded-lg bg-sky-600 hover:bg-sky-500 text-white transition-colors">
                                    Replay Task
                                </button>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        setInterval(async () => {
            try {
                const res = await fetch('/ui/data');
                if (res.ok) {
                    const data = await res.json();
                    document.getElementById('val-main').innerText = data.main_depth;
                    document.getElementById('val-prio').innerText = data.prio_depth;
                    document.getElementById('val-dlq').innerText = data.dlq_depth;
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
"MainDepth": mainLen,
"DLQDepth":  dlqLen,
"PrioDepth": prioLen,
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
