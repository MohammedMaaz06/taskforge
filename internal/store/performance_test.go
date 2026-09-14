package store

import (
"testing"
"taskforge/internal/task"
)

func BenchmarkSQLiteStore_Save(b *testing.B) {
s, err := NewSQLiteStore(":memory:")
if err != nil {
b.Fatalf("failed to create sqlite store: %v", err)
}
defer s.Close()

_ = ConfigureSQLitePerformance(s.db)

b.ResetTimer()
for i := 0; i < b.N; i++ {
t := &task.Task{
ID:     "bench-task",
Type:   "benchmark",
Status: "PENDING",
}
_ = s.Save(t)
}
}
