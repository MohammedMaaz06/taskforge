package benchmark

import (
"context"
"sync"
"testing"
"time"

"github.com/redis/go-redis/v9"
)

func BenchmarkTaskIngestionThroughput(b *testing.B) {
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
ctx := context.Background()

b.ResetTimer()
b.RunParallel(func(pb *testing.PB) {
for pb.Next() {
_, _ = rdb.LPush(ctx, "benchmark:queue", "test-payload-data").Result()
}
})
_ = rdb.Del(ctx, "benchmark:queue").Err()
}

func BenchmarkConcurrentWorkerDispatch(b *testing.B) {
var wg sync.WaitGroup
workers := 50
tasksPerWorker := 100

b.ResetTimer()
for i := 0; i < b.N; i++ {
for w := 0; w < workers; w++ {
wg.Add(1)
go func() {
defer wg.Done()
for t := 0; t < tasksPerWorker; t++ {
time.Sleep(1 * time.Microsecond)
}
}()
}
wg.Wait()
}
}
