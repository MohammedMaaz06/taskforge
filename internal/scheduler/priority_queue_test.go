package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkTaskScheduler(b *testing.B) {
	ts := NewTaskScheduler()
	b.ResetTimer()

	go func() {
		for i := 0; i < b.N; i++ {
			ts.Submit(&Task{
				ID: fmt.Sprintf("task-%d", i),
				Priority: i % 10,
				CreatedAt: time.Now(),
			})
		}
	}()

	for i := 0; i < b.N; i++ {
		ts.PopNext()
	}
}
