package scheduler

import (
"container/heap"
"sync"
"time"
)

type Task struct {
ID           string    `json:"id"`
Name         string    `json:"name"`
Payload      string    `json:"payload"`
Priority     int       `json:"priority"`
Status       string    `json:"status"`
MaxRetries   int       `json:"max_retries"`
CurrentRetry int       `json:"current_retry"`
CreatedAt    time.Time `json:"created_at"`
index        int
}

type PriorityQueue []*Task

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
if pq[i].Priority == pq[j].Priority {
return pq[i].CreatedAt.Before(pq[j].CreatedAt)
}
return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
pq[i], pq[j] = pq[j], pq[i]
pq[i].index = i
pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
n := len(*pq)
item := x.(*Task)
item.index = n
*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
old := *pq
n := len(old)
item := old[n-1]
old[n-1] = nil
item.index = -1
*pq = old[0 : n-1]
return item
}

type TaskScheduler struct {
mu    sync.Mutex
pq    PriorityQueue
cond  *sync.Cond
tasks map[string]*Task
}

func NewTaskScheduler() *TaskScheduler {
ts := &TaskScheduler{
pq:    make(PriorityQueue, 0),
tasks: make(map[string]*Task),
}
ts.cond = sync.NewCond(&ts.mu)
heap.Init(&ts.pq)
return ts
}

func (ts *TaskScheduler) Submit(task *Task) {
ts.mu.Lock()
defer ts.mu.Unlock()

task.Status = "PENDING"
task.CreatedAt = time.Now()
ts.tasks[task.ID] = task
heap.Push(&ts.pq, task)
ts.cond.Signal()
}

func (ts *TaskScheduler) PopNext() *Task {
ts.mu.Lock()
defer ts.mu.Unlock()

for ts.pq.Len() == 0 {
ts.cond.Wait()
}

task := heap.Pop(&ts.pq).(*Task)
task.Status = "RUNNING"
return task
}

func (ts *TaskScheduler) GetTask(id string) (*Task, bool) {
ts.mu.Lock()
defer ts.mu.Unlock()
task, exists := ts.tasks[id]
return task, exists
}

func (ts *TaskScheduler) GetAllTasks() []*Task {
ts.mu.Lock()
defer ts.mu.Unlock()
all := make([]*Task, 0, len(ts.tasks))
for _, t := range ts.tasks {
all = append(all, t)
}
return all
}
