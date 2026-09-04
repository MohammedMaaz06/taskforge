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

// Initialize Auth Guard
jwtSecret := os.Getenv("JWT_SECRET")
if jwtSecret == "" {
jwtSecret = "taskforge-dev-secret-key"
}
apiKeys := map[string]string{
"tf-worker-secret-key": "internal-worker-node",
}
authGuard := auth.NewAuthGuard(jwtSecret, apiKeys)

mux := http.NewServeMux()

// Public Routes
mux.Handle("/metrics", promhttp.Handler())
mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
w.Write([]byte(`{"status":"UP"}`))
})

// Protected API Routes
apiMux := http.NewServeMux()
apiMux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.Write([]byte(`{"status":"success","message":"Authenticated access granted"}`))
})

// Wrap protected routes with auth middleware
mux.Handle("/api/", authGuard.Middleware(apiMux))

// Serve Static Assets
fileServer := http.FileServer(http.Dir("./static"))
mux.Handle("/", fileServer)

fmt.Printf("TaskForge Server starting on port %s...\n", port)
if err := http.ListenAndServe(":"+port, mux); err != nil {
log.Fatalf("Server failed: %v", err)
}
}

