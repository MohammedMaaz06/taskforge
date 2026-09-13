package middleware

import (
"fmt"
"net/http"
"time"

"github.com/redis/go-redis/v9"
)

func RateLimit(rdb *redis.Client, limit int, window time.Duration, next http.HandlerFunc) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
clientIP := r.RemoteAddr
key := fmt.Sprintf("rate:%s", clientIP)
now := time.Now().UnixNano()
clearBefore := now - window.Nanoseconds()

pipe := rdb.Pipeline()
pipe.ZRemRangeByScore(r.Context(), key, "0", fmt.Sprintf("%d", clearBefore))
pipe.ZAdd(r.Context(), key, redis.Z{Score: float64(now), Member: now})
pipe.ZCard(r.Context(), key)
pipe.Expire(r.Context(), key, window)

cmds, err := pipe.Exec(r.Context())
if err != nil {
http.Error(w, "Rate limiting check failed", http.StatusInternalServerError)
return
}

reqCount := cmds[2].(*redis.IntCmd).Val()
if reqCount > int64(limit) {
w.Header().Set("Retry-After", "60")
http.Error(w, "Rate limit exceeded. Try again later.", http.StatusTooManyRequests)
return
}

next(w, r)
}
}
