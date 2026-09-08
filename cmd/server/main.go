package main

import (
"fmt"
"log"
"net/http"
"os"

"github.com/prometheus/client_golang/prometheus/promhttp"
"taskforge/internal/auth"
)

func main() {
port := os.Getenv("PORT")
if port == "" {
port = "8080"
}

jwtSecret := os.Getenv("JWT_SECRET")
if jwtSecret == "" {
jwtSecret = "taskforge-dev-secret-key"
}
apiKeys := map[string]string{
"tf-worker-secret-key": "internal-worker-node",
}
authGuard := auth.NewAuthGuard(jwtSecret, apiKeys)

mux := http.NewServeMux()

// Metrics & Health Checks
mux.Handle("/metrics", promhttp.Handler())

healthHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
w.Write([]byte(`{"status":"UP"}`))
})

mux.Handle("/health", healthHandler)
mux.Handle("/healthz", healthHandler)

// Protected API Routes
taskHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
w.Write([]byte(`{"status":"success","message":"Authenticated access granted"}`))
})

apiMux := http.NewServeMux()
apiMux.Handle("/api/tasks", taskHandler)
apiMux.Handle("/api/v1/tasks", taskHandler)

// Route /api and /api/ to the protected mux
mux.Handle("/api", authGuard.Middleware(apiMux))
mux.Handle("/api/", authGuard.Middleware(apiMux))

// Serve Static Assets
fileServer := http.FileServer(http.Dir("./static"))
mux.Handle("/", fileServer)

fmt.Printf("TaskForge Server starting on port %s...\n", port)
if err := http.ListenAndServe(":"+port, mux); err != nil {
log.Fatalf("Server failed: %v", err)
}
}
