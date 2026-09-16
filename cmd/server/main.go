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
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TaskForge Control Center</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 2rem; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 1rem; margin-bottom: 2rem; }
        h1 { margin: 0; color: #38bdf8; font-size: 1.5rem; }
        h2 { color: #94a3b8; font-size: 1.1rem; margin-top: 2rem; margin-bottom: 1rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; }
        .card { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 1.5rem; }
        .card h3 { margin: 0 0 0.5rem 0; font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }
        .card .value { font-size: 2.25rem; font-weight: bold; color: #38bdf8; }
        .badge { background: #0284c7; color: white; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
        .badge-status { background: #065f46; color: #34d399; }
        table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 8px; overflow: hidden; border: 1px solid #334155; }
        th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #334155; font-size: 0.875rem; }
        th { background: #0f172a; color: #94a3b8; font-size: 0.75rem; text-transform: uppercase; }
        tr:last-child td { border-bottom: none; }
        .btn { background: #0284c7; color: white; border: none; padding: 0.35rem 0.75rem; border-radius: 4px; cursor: pointer; font-size: 0.75rem; font-weight: 600; }
        .btn:hover { background: #0369a1; }
    </style>
</head>
<body>
    <div class="header">
        <h1>TaskForge Engine Control</h1>
        <span class="badge">v7 Pro</span>
    </div>

    <!-- Telemetry Cards -->
    <div class="grid">
        <div class="card">
            <h3>Main Queue Depth</h3>
            <div class="value" id="val-main">{{.MainDepth}}</div>
        </div>
        <div class="card">
            <h3>Priority Queue Depth</h3>
            <div class="value" id="val-prio">{{.PrioDepth}}</div>
        </div>
        <div class="card">
            <h3>DLQ Items</h3>
            <div class="value" id="val-dlq">{{.DLQDepth}}</div>
        </div>
        <div class="card">
            <h3>Active Workers</h3>
            <div class="value" id="val-workers">2</div>
        </div>
    </div>

    <!-- Recent Task Activity -->
    <h2>Engine Task Telemetry</h2>
    <table>
        <thead>
            <tr>
                <th>Task ID</th>
                <th>Queue Tier</th>
                <th>Status</th>
                <th>Engine Action</th>
            </tr>
        </thead>
        <tbody id="task-rows">
            <tr>
                <td><code>task-prio-1789552325125866500</code></td>
                <td>High Priority</td>
                <td><span class="badge badge-status">priority_enqueued</span></td>
                <td><button class="btn" onclick="alert('Triggering DLQ Replay...')">Replay Task</button></td>
            </tr>
        </tbody>
    </table>

    <script>
        // Auto-refresh stats every 3 seconds
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
                console.error('Failed to update dashboard stats:', err);
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

	// JSON endpoint for dashboard live polling
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

	// Main UI Dashboard Handler
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