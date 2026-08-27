package scheduler

import (
"container/heap"
"errors"
"sync"

"taskforge/pkg/task"
)

var ErrQueueEmpty = errors.New("queue is empty")

type TaskHeap []*task.Task

func (h TaskHeap) Len() int           { return len(h) }
func (h TaskHeap) Less(i, j int) bool { return h[i].Priority > h[j].Priority }
func (h TaskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *TaskHeap) Push(x interface{}) {
*h = append(*h, x.(*task.Task))
}

func (h *TaskHeap) Pop() interface{} {
old := *h
n := len(old)
x := old[n-1]
*h = old[0 : n-1]
return x
}

type TaskScheduler struct {
mu sync.Mutex
pq TaskHeap
}

func NewTaskScheduler() *TaskScheduler {
ts := &TaskScheduler{
pq: make(TaskHeap, 0),
}
heap.Init(&ts.pq)
return ts
}

func (ts *TaskScheduler) Push(t *task.Task) {
ts.mu.Lock()
defer ts.mu.Unlock()
heap.Push(&ts.pq, t)
}

func (ts *TaskScheduler) PushTask(t *task.Task) {
ts.Push(t)
}

func (ts *TaskScheduler) Pop() (*task.Task, error) {
ts.mu.Lock()
defer ts.mu.Unlock()
if len(ts.pq) == 0 {
return nil, ErrQueueEmpty
}
item := heap.Pop(&ts.pq).(*task.Task)
return item, nil
}

func (ts *TaskScheduler) PopTask() (*task.Task, error) {
return ts.Pop()
}

func (ts *TaskScheduler) Size() int {
ts.mu.Lock()
defer ts.mu.Unlock()
return len(ts.pq)
}

