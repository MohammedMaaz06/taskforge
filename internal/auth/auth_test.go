package auth

import (
"net/http"
"net/http/httptest"
"testing"
"time"
)

func TestAuthGuard(t *testing.T) {
guard := NewAuthGuard("super-secret-key", map[string]string{
"valid-api-key-123": "worker-service",
})

protectedHandler := guard.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
w.Write([]byte("success"))
}))

// Test 1: Missing Auth -> 401
req := httptest.NewRequest("GET", "/api/tasks", nil)
rec := httptest.NewRecorder()
protectedHandler.ServeHTTP(rec, req)
if rec.Code != http.StatusUnauthorized {
t.Fatalf("expected 401, got %d", rec.Code)
}

// Test 2: Valid API Key -> 200
req = httptest.NewRequest("GET", "/api/tasks", nil)
req.Header.Set(APIKeyHeader, "valid-api-key-123")
rec = httptest.NewRecorder()
protectedHandler.ServeHTTP(rec, req)
if rec.Code != http.StatusOK {
t.Fatalf("expected 200 for valid API key, got %d", rec.Code)
}

// Test 3: Valid JWT Token -> 200
token, err := guard.GenerateJWT("admin_user", "admin", time.Hour)
if err != nil {
t.Fatalf("failed to generate token: %v", err)
}

req = httptest.NewRequest("GET", "/api/tasks", nil)
req.Header.Set("Authorization", "Bearer "+token)
rec = httptest.NewRecorder()
protectedHandler.ServeHTTP(rec, req)
if rec.Code != http.StatusOK {
t.Fatalf("expected 200 for valid JWT, got %d", rec.Code)
}
}

