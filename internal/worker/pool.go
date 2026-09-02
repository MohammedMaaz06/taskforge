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

type TaskListener func(t *task.Task, status task.Status)

type Pool struct {
numWorkers int
sched      *scheduler.TaskScheduler
store      store.Store
dlq        *scheduler.DeadLetterQueue
dagMgr     *dag.Manager
listener   TaskListener
cancelMap  map[string]context.CancelFunc
mu         sync.Mutex
wg         sync.WaitGroup
quit       chan struct{}
}

func NewPool(numWorkers int, sched *scheduler.TaskScheduler, store store.Store, dlq *scheduler.DeadLetterQueue, dagMgr *dag.Manager, listener TaskListener) *Pool {
return &Pool{
numWorkers: numWorkers,
sched:      sched,
store:      store,
dlq:        dlq,
dagMgr:     dagMgr,
listener:   listener,
cancelMap:  make(map[string]context.CancelFunc),
quit:       make(chan struct{}),
}
}

func (p *Pool) Start() {
p.mu.Lock()
defer p.mu.Unlock()

for i := 0; i < p.numWorkers; i++ {
p.wg.Add(1)
go p.workerLoop(i)
}
log.Printf("INFO worker pool started num_workers=%d", p.numWorkers)
}

func (p *Pool) Scale(newSize int) int {
p.mu.Lock()
defer p.mu.Unlock()

if newSize > p.numWorkers {
diff := newSize - p.numWorkers
for i := 0; i < diff; i++ {
p.wg.Add(1)
go p.workerLoop(p.numWorkers + i)
}
} else if newSize < p.numWorkers {
diff := p.numWorkers - newSize
for i := 0; i < diff; i++ {
p.quit <- struct{}{}
}
}

p.numWorkers = newSize
log.Printf("INFO worker pool scaled to num_workers=%d", p.numWorkers)
return p.numWorkers
}

func (p *Pool) GetWorkerCount() int {
p.mu.Lock()
defer p.mu.Unlock()
return p.numWorkers
}

func (p *Pool) CancelTask(id string) bool {
p.mu.Lock()
defer p.mu.Unlock()

if cancel, exists := p.cancelMap[id]; exists {
cancel()
delete(p.cancelMap, id)
log.Printf("INFO task cancellation signal sent id=%s", id)
return true
}
return false
}

func (p *Pool) workerLoop(id int) {
defer p.wg.Done()

for {
select {
case <-p.quit:
return
default:
t, err := p.sched.Pop()
if err != nil || t == nil {
time.Sleep(100 * time.Millisecond)
continue
}

if time.Now().Before(t.ScheduledAt) {
time.Sleep(time.Until(t.ScheduledAt))
}

p.executeTask(id, t)
}
}
}

func (p *Pool) executeTask(workerID int, t *task.Task) {
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

p.mu.Lock()
p.cancelMap[t.ID] = cancel
p.mu.Unlock()

defer func() {
p.mu.Lock()
delete(p.cancelMap, t.ID)
p.mu.Unlock()
}()

t.Status = task.StatusRunning
t.UpdatedAt = time.Now()
p.store.Save(t)

startTime := time.Now()

select {
case <-ctx.Done():
if ctx.Err() == context.Canceled {
t.Status = task.StatusFailed
t.LastError = "cancelled by user request"
p.store.Save(t)
metrics.TaskFailures.Inc()
if p.listener != nil {
p.listener(t, task.StatusFailed)
}
p.dagMgr.EvaluateDependents(t.ID, func(dt *task.Task) { p.sched.Push(dt) })
return
}
default:
}

// Simulated Task Failure
if t.Name == "fail-task" {
t.LastError = fmt.Sprintf("simulated failure for task %s", t.ID)
t.CurrentRetry++
t.UpdatedAt = time.Now()

if t.CurrentRetry >= t.MaxRetries {
t.Status = task.StatusDLQ
p.store.Save(t)
p.dlq.Add(t)
metrics.DLQCount.Inc()
metrics.TaskFailures.Inc()
if p.listener != nil {
p.listener(t, task.StatusDLQ)
}
p.dagMgr.EvaluateDependents(t.ID, func(dt *task.Task) { p.sched.Push(dt) })
} else {
t.Status = task.StatusPending
p.store.Save(t)
p.sched.Push(t)
metrics.TaskFailures.Inc()
if p.listener != nil {
p.listener(t, task.StatusFailed)
}
}
return
}

time.Sleep(500 * time.Millisecond)

t.Status = task.StatusCompleted
t.UpdatedAt = time.Now()
p.store.Save(t)

duration := time.Since(startTime).Seconds()
metrics.TaskDuration.Observe(duration)
metrics.TasksCompleted.Inc()

if p.listener != nil {
p.listener(t, task.StatusCompleted)
}

// Trigger dependent waiting tasks in DAG
p.dagMgr.EvaluateDependents(t.ID, func(dt *task.Task) {
p.sched.Push(dt)
})

if t.IntervalSeconds > 0 {
nextTask := task.NewTask(t.Name, t.Priority, t.MaxRetries)
nextTask.IntervalSeconds = t.IntervalSeconds
nextTask.ScheduledAt = time.Now().Add(time.Duration(t.IntervalSeconds) * time.Second)
p.store.Save(nextTask)
p.sched.Push(nextTask)
}
}

func (p *Pool) Stop() {
close(p.quit)
p.wg.Wait()
}

