package worker

import (
"context"
"fmt"
"log"
"sync"
"time"

"taskforge/internal/dag"
"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/pkg/task"
)

type Pool struct {
numWorkers int
taskQueue  chan *task.Task
scheduler  *scheduler.TaskScheduler
store      *store.SQLiteStore
dlq        *scheduler.DeadLetterQueue
dagManager *dag.Manager
locker     store.DistributedLocker
wg         sync.WaitGroup
ctx        context.Context
cancel     context.CancelFunc
}

func NewPool(
numWorkers int,
s *scheduler.TaskScheduler,
st *store.SQLiteStore,
dlq *scheduler.DeadLetterQueue,
dagMgr *dag.Manager,
locker store.DistributedLocker,
) *Pool {
ctx, cancel := context.WithCancel(context.Background())
return &Pool{
numWorkers: numWorkers,
taskQueue:  make(chan *task.Task, 100),
scheduler:  s,
store:      st,
dlq:        dlq,
dagManager: dagMgr,
locker:     locker,
ctx:        ctx,
cancel:     cancel,
}
}

func (p *Pool) Start() {
log.Printf("INFO worker pool started num_workers=%d", p.numWorkers)
for i := 0; i < p.numWorkers; i++ {
p.wg.Add(1)
go p.workerLoop(i)
}
}

func (p *Pool) Submit(t *task.Task) bool {
select {
case p.taskQueue <- t:
metrics.TaskQueueDepth.Set(float64(len(p.taskQueue)))
return true
default:
return false
}
}

func (p *Pool) Stop() {
p.cancel()
close(p.taskQueue)
p.wg.Wait()
}

func (p *Pool) workerLoop(id int) {
defer p.wg.Done()
ticker := time.NewTicker(20 * time.Millisecond)
defer ticker.Stop()

for {
select {
case <-p.ctx.Done():
return
case t, ok := <-p.taskQueue:
if ok && t != nil {
metrics.TaskQueueDepth.Set(float64(len(p.taskQueue)))
p.processTask(t)
}
case <-ticker.C:
if p.scheduler != nil {
if t, err := p.scheduler.Pop(); err == nil && t != nil {
p.processTask(t)
}
}
}
}
}

func (p *Pool) processTask(t *task.Task) {
start := time.Now()
if p.locker != nil {
lockKey := fmt.Sprintf("task:%s", t.ID)
acquired, err := p.locker.Acquire(p.ctx, lockKey, 30*time.Second)
if err != nil {
log.Printf("ERROR failed to acquire lock for task %s: %v", t.ID, err)
metrics.TasksProcessedTotal.WithLabelValues("failed_lock").Inc()
return
}
if !acquired {
log.Printf("INFO task %s skipped, claimed by another instance", t.ID)
metrics.TasksProcessedTotal.WithLabelValues("skipped_lock").Inc()
return
}
defer func() {
if err := p.locker.Release(p.ctx, lockKey); err != nil {
log.Printf("WARN failed to release lock for task %s: %v", t.ID, err)
}
}()
}

t.Status = task.StatusRunning
if p.store != nil {
_ = p.store.UpdateStatus(t.ID, task.StatusRunning, "")
}

time.Sleep(50 * time.Millisecond)

t.Status = task.StatusCompleted
if p.store != nil {
_ = p.store.UpdateStatus(t.ID, task.StatusCompleted, "")
}

duration := time.Since(start).Seconds()
metrics.TaskExecutionDuration.WithLabelValues(string(task.StatusCompleted)).Observe(duration)
metrics.TasksProcessedTotal.WithLabelValues(string(task.StatusCompleted)).Inc()
}

