package worker

import (
"context"
"fmt"
"log"
"sync"
"sync/atomic"
"time"

"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/pkg/task"
)

type TaskListener func(t *task.Task, status task.Status)

type Pool struct {
mu           sync.Mutex
numWorkers   int32
scheduler    *scheduler.TaskScheduler
store        store.Store
dlq          *scheduler.DeadLetterQueue
listener     TaskListener
stopChan     chan struct{}
cancelMap    sync.Map
workerCancel []chan struct{}
baseBackoff  time.Duration
}

func NewPool(numWorkers int, sched *scheduler.TaskScheduler, st store.Store, dlq *scheduler.DeadLetterQueue, listener TaskListener) *Pool {
return &Pool{
numWorkers:   int32(numWorkers),
scheduler:    sched,
store:        st,
dlq:          dlq,
listener:     listener,
stopChan:     make(chan struct{}),
workerCancel: make([]chan struct{}, 0),
baseBackoff:  1 * time.Second,
}
}

func (p *Pool) Start() {
p.mu.Lock()
defer p.mu.Unlock()
log.Printf("INFO worker pool started num_workers=%d", p.numWorkers)
for i := 0; i < int(p.numWorkers); i++ {
p.startWorker()
}
}

func (p *Pool) startWorker() {
stop := make(chan struct{})
p.workerCancel = append(p.workerCancel, stop)
go p.worker(stop)
}

func (p *Pool) Scale(targetWorkers int) int {
p.mu.Lock()
defer p.mu.Unlock()

current := int(atomic.LoadInt32(&p.numWorkers))
if targetWorkers <= 0 || targetWorkers == current {
return current
}

if targetWorkers > current {
diff := targetWorkers - current
for i := 0; i < diff; i++ {
p.startWorker()
}
} else {
diff := current - targetWorkers
for i := 0; i < diff; i++ {
idx := len(p.workerCancel) - 1
close(p.workerCancel[idx])
p.workerCancel = p.workerCancel[:idx]
}
}

atomic.StoreInt32(&p.numWorkers, int32(targetWorkers))
log.Printf("INFO worker pool scaled from %d to %d", current, targetWorkers)
return targetWorkers
}

func (p *Pool) CancelTask(taskID string) bool {
if cancel, ok := p.cancelMap.Load(taskID); ok {
cancel.(context.CancelFunc)()
p.cancelMap.Delete(taskID)
return true
}

t, err := p.store.Get(taskID)
if err == nil && t != nil && t.Status == task.StatusPending {
t.Status = task.StatusCancelled
p.store.Save(t)
metrics.TasksProcessedTotal.WithLabelValues("cancelled").Inc()
return true
}

return false
}

func (p *Pool) GetWorkerCount() int {
return int(atomic.LoadInt32(&p.numWorkers))
}

func (p *Pool) Stop() {
close(p.stopChan)
}

func (p *Pool) worker(stopChan chan struct{}) {
for {
select {
case <-p.stopChan:
return
case <-stopChan:
return
default:
t, err := p.scheduler.Pop()
if err != nil || t == nil {
time.Sleep(100 * time.Millisecond)
continue
}

metrics.QueueDepth.Set(float64(p.scheduler.Size()))
p.processTask(t)
}
}
}

func (p *Pool) processTask(t *task.Task) {
ctx, cancel := context.WithCancel(context.Background())
p.cancelMap.Store(t.ID, cancel)
defer p.cancelMap.Delete(t.ID)

startTime := time.Now()

t.Status = task.StatusRunning
p.store.Save(t)
if p.listener != nil {
p.listener(t, task.StatusRunning)
}

workDone := make(chan struct{})
go func() {
time.Sleep(500 * time.Millisecond)
close(workDone)
}()

select {
case <-ctx.Done():
t.Status = task.StatusCancelled
t.LastError = "task cancelled by user"
p.store.Save(t)
metrics.TasksProcessedTotal.WithLabelValues("cancelled").Inc()
log.Printf("WARN task cancelled id=%s name=%s", t.ID, t.Name)
if p.listener != nil {
p.listener(t, task.StatusCancelled)
}
return
case <-workDone:
}

var err error
if t.Name == "fail-task" {
err = fmt.Errorf("simulated failure for task %s", t.ID)
}

duration := time.Since(startTime).Seconds()
metrics.TaskExecutionDuration.WithLabelValues(t.Name).Observe(duration)

if err != nil {
t.CurrentRetry++
t.LastError = err.Error()

if t.CurrentRetry >= t.MaxRetries {
t.Status = task.StatusFailed
p.dlq.Add(t)

metrics.TasksProcessedTotal.WithLabelValues("failed").Inc()
metrics.DLQCount.Set(float64(p.dlq.Size()))

log.Printf("ERROR task failed permanently id=%s name=%s retries=%d/%d", t.ID, t.Name, t.CurrentRetry, t.MaxRetries)
} else {
backoffDelay := t.CalculateBackoff(p.baseBackoff)
t.Status = task.StatusPending
t.ScheduledAt = time.Now().Add(backoffDelay)

log.Printf("WARN task scheduled for retry id=%s name=%s retry=%d/%d backoff=%v", t.ID, t.Name, t.CurrentRetry, t.MaxRetries, backoffDelay)

go func(retryTask *task.Task, delay time.Duration) {
time.Sleep(delay)
p.scheduler.Push(retryTask)
metrics.TasksProcessedTotal.WithLabelValues("retry").Inc()
metrics.QueueDepth.Set(float64(p.scheduler.Size()))
}(t, backoffDelay)
}
} else {
t.Status = task.StatusCompleted
metrics.TasksProcessedTotal.WithLabelValues("completed").Inc()

log.Printf("INFO task completed successfully id=%s name=%s duration=%.2fs", t.ID, t.Name, duration)
}

p.store.Save(t)
if p.listener != nil {
p.listener(t, t.Status)
}
}

