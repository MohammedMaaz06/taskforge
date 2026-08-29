package worker

import (
"fmt"
"log"
"time"

"taskforge/internal/metrics"
"taskforge/internal/scheduler"
"taskforge/internal/store"
"taskforge/pkg/task"
)

type TaskListener func(t *task.Task, status task.Status)

type Pool struct {
numWorkers int
scheduler  *scheduler.TaskScheduler
store      store.Store
dlq        *scheduler.DeadLetterQueue
listener   TaskListener
stopChan   chan struct{}
}

func NewPool(numWorkers int, sched *scheduler.TaskScheduler, st store.Store, dlq *scheduler.DeadLetterQueue, listener TaskListener) *Pool {
return &Pool{
numWorkers: numWorkers,
scheduler:  sched,
store:      st,
dlq:        dlq,
listener:   listener,
stopChan:   make(chan struct{}),
}
}

func (p *Pool) Start() {
log.Printf("INFO worker pool started num_workers=%d", p.numWorkers)
for i := 0; i < p.numWorkers; i++ {
go p.worker(i)
}
}

func (p *Pool) Stop() {
close(p.stopChan)
}

func (p *Pool) worker(id int) {
for {
select {
case <-p.stopChan:
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
startTime := time.Now()

t.Status = task.StatusRunning
p.store.Save(t)
if p.listener != nil {
p.listener(t, task.StatusRunning)
}

time.Sleep(500 * time.Millisecond)

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

log.Printf("ERROR task failed permanently id=%s name=%s retries=%d", t.ID, t.Name, t.CurrentRetry)
} else {
t.Status = task.StatusPending
p.scheduler.Push(t)

metrics.TasksProcessedTotal.WithLabelValues("retry").Inc()
metrics.QueueDepth.Set(float64(p.scheduler.Size()))

log.Printf("WARN task retrying id=%s name=%s retry=%d/%d", t.ID, t.Name, t.CurrentRetry, t.MaxRetries)
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

