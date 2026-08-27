package worker

import (
"context"
"log/slog"
"sync"
"time"

"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/pkg/task"
)

type StatusListener func(t *task.Task, status task.Status)

type Pool struct {
numWorkers     int
scheduler      *scheduler.TaskScheduler
store          store.Store
dlq            *scheduler.DeadLetterQueue
statusListener StatusListener
tasks          chan *task.Task
wg             sync.WaitGroup
ctx            context.Context
cancel         context.CancelFunc
}

func NewPool(numWorkers int, sched *scheduler.TaskScheduler, st store.Store, dlq *scheduler.DeadLetterQueue, listener StatusListener) *Pool {
ctx, cancel := context.WithCancel(context.Background())
return &Pool{
numWorkers:     numWorkers,
scheduler:      sched,
store:          st,
dlq:            dlq,
statusListener: listener,
tasks:          make(chan *task.Task, numWorkers*2),
ctx:            ctx,
cancel:         cancel,
}
}

func (p *Pool) Start() {
slog.Info("worker pool started", "num_workers", p.numWorkers)
for i := 1; i <= p.numWorkers; i++ {
p.wg.Add(1)
go p.worker(i)
}
}

func (p *Pool) worker(id int) {
defer p.wg.Done()
for {
select {
case <-p.ctx.Done():
slog.Info("worker thread stopped", "worker_id", id)
return
default:
t, err := p.scheduler.Pop()
if err != nil {
time.Sleep(100 * time.Millisecond)
continue
}
if t == nil {
time.Sleep(50 * time.Millisecond)
continue
}

p.processTask(id, t)
}
}
}

func (p *Pool) processTask(workerID int, t *task.Task) {
t.Status = task.StatusRunning
t.CurrentRetry++
if p.store != nil {
_ = p.store.UpdateStatus(t.ID, task.StatusRunning, "")
}
p.notifyStatus(t, task.StatusRunning)

slog.Info("processing task", "worker_id", workerID, "task_id", t.ID, "task_name", t.Name, "priority", t.Priority, "attempt", t.CurrentRetry)

err := p.execute(t)

if err != nil {
slog.Error("task execution failed", "worker_id", workerID, "task_id", t.ID, "error", err)
t.LastError = err.Error()

if t.CurrentRetry < t.MaxRetries {
t.Status = task.StatusPending
p.scheduler.Push(t)
if p.store != nil {
_ = p.store.UpdateStatus(t.ID, task.StatusPending, t.LastError)
}
p.notifyStatus(t, task.StatusPending)
} else {
t.Status = task.StatusDLQ
if p.dlq != nil {
p.dlq.Store(t, err.Error())
}
if p.store != nil {
_ = p.store.UpdateStatus(t.ID, task.StatusDLQ, t.LastError)
}
p.notifyStatus(t, task.StatusDLQ)
}
return
}

t.Status = task.StatusCompleted
if p.store != nil {
_ = p.store.UpdateStatus(t.ID, task.StatusCompleted, "")
}
p.notifyStatus(t, task.StatusCompleted)
slog.Info("task completed successfully", "worker_id", workerID, "task_id", t.ID)
}

func (p *Pool) execute(t *task.Task) error {
time.Sleep(100 * time.Millisecond)
return nil
}

func (p *Pool) notifyStatus(t *task.Task, status task.Status) {
if p.statusListener != nil {
p.statusListener(t, status)
}
}

func (p *Pool) Stop() {
p.cancel()
p.wg.Wait()
slog.Info("all workers stopped cleanly")
}

