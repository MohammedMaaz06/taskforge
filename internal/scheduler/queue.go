package scheduler

import (
	"container/heap"
	"sync"
	"taskforge/pkg/task"
)

type PriorityQueue []*task.Task

func (pq PriorityQueue) Len() int           { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].Priority > pq[j].Priority } // Max Heap
func (pq PriorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*task.Task)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// SafeQueue wraps PriorityQueue with thread-safe mutex operations
type SafeQueue struct {
	mu sync.Mutex
	pq PriorityQueue
}

func NewSafeQueue() *SafeQueue {
	sq := &SafeQueue{pq: make(PriorityQueue, 0)}
	heap.Init(&sq.pq)
	return sq
}

func (sq *SafeQueue) Push(t *task.Task) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	heap.Push(&sq.pq, t)
}

func (sq *SafeQueue) Pop() *task.Task {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if len(sq.pq) == 0 {
		return nil
	}
	return heap.Pop(&sq.pq).(*task.Task)
}

func (sq *SafeQueue) Len() int {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.pq.Len()
}
