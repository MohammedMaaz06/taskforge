package metrics

import (
"net/http"
"net/http/httptest"
"testing"
)

func TestMetricsHandler(t *testing.T) {
InitMetrics()

TaskQueueDepth.Set(5)
TasksProcessedTotal.WithLabelValues("completed").Inc()

req := httptest.NewRequest("GET", "/metrics", nil)
rr := httptest.NewRecorder()

handler := MetricsHandler()
handler.ServeHTTP(rr, req)

if rr.Code != http.StatusOK {
t.Fatalf("expected status 200, got %d", rr.Code)
}

body := rr.Body.String()
if !contains(body, "taskforge_queue_depth 5") {
t.Errorf("metrics response missing expected gauge value: %s", body)
}
}

func contains(str, substr string) bool {
return len(str) >= len(substr) && (str == substr || len(substr) == 0 || (len(str) > 0 && searchSubstring(str, substr)))
}

func searchSubstring(str, substr string) bool {
for i := 0; i <= len(str)-len(substr); i++ {
if str[i:i+len(substr)] == substr {
return true
}
}
return false
}

