package store

import (
"context"
"errors"
"fmt"
"time"

"github.com/redis/go-redis/v9"
)

var ErrLockAcquireFailed = errors.New("failed to acquire distributed lock")

type DistributedLocker interface {
Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
Release(ctx context.Context, key string) error
}

type RedisLocker struct {
client *redis.Client
owner  string
}

func NewRedisLocker(client *redis.Client, owner string) *RedisLocker {
return &RedisLocker{
client: client,
owner:  owner,
}
}

func (r *RedisLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
lockKey := fmt.Sprintf("lock:%s", key)
ok, err := r.client.SetNX(ctx, lockKey, r.owner, ttl).Result()
if err != nil {
return false, fmt.Errorf("redis setnx error: %w", err)
}
return ok, nil
}

func (r *RedisLocker) Release(ctx context.Context, key string) error {
lockKey := fmt.Sprintf("lock:%s", key)
luaScript := `
if redis.call("get", KEYS[1]) == ARGV[1] then
return redis.call("del", KEYS[1])
else
return 0
end
`
res, err := r.client.Eval(ctx, luaScript, []string{lockKey}, r.owner).Result()
if err != nil {
return fmt.Errorf("lua unlock error: %w", err)
}
if res.(int64) == 0 {
return errors.New("lock lost or owned by another instance")
}
return nil
}

