package dag

import (
"sync"
"testing"

"taskforge/internal/store"
"taskforge/pkg/task"
)

type mockStore struct {
mu    sync.Mutex
tasks map[string]*task.Task
}

func newMockStore() *mockStore {
return &mockStore{tasks: make(map[string]*task.Task)}
}

func (m *mockStore) Save(t *task.Task) error {
m.mu.Lock()
defer m.mu.Unlock()
m.tasks[t.ID] = t
return nil
}

func (m *mockStore) Get(id string) (*task.Task, error) {
m.mu.Lock()
defer m.mu.Unlock()
t, exists := m.tasks[id]
if !exists {
return nil, store.ErrTaskNotFound
}
return t, nil
}

func (m *mockStore) List(statusFilter ...string) ([]*task.Task, error) {
m.mu.Lock()
defer m.mu.Unlock()
list := make([]*task.Task, 0, len(m.tasks))
filter := ""
if len(statusFilter) > 0 {
filter = statusFilter[0]
}

for _, t := range m.tasks {
if filter == "" || string(t.Status) == filter {
list = append(list, t)
}
}
return list, nil
}

func (m *mockStore) UpdateStatus(id string, status task.Status, lastErr string) error {
m.mu.Lock()
defer m.mu.Unlock()
t, exists := m.tasks[id]
if !exists {
return store.ErrTaskNotFound
}
t.Status = status
t.LastError = lastErr
return nil
}

func (m *mockStore) Close() error {
return nil
}

func TestDAG_CanExecute(t *testing.T) {
st := newMockStore()
mgr := NewManager(st)

t.Run("No Dependencies Can Always Execute", func(t *testing.T) {
tk := &task.Task{ID: "task-1", Status: task.StatusPending}
canRun, err := mgr.CanExecute(tk)
if err != nil || !canRun {
t.Fatalf("expected canRun=true, err=nil, got canRun=%v, err=%v", canRun, err)
}
})

t.Run("Parent Not Completed Returns False", func(t *testing.T) {
parent := &task.Task{ID: "parent-1", Status: task.StatusRunning}
_ = st.Save(parent)

child := &task.Task{ID: "child-1", DependsOn: []string{"parent-1"}, Status: task.StatusWaiting}
canRun, err := mgr.CanExecute(child)
if err != nil || canRun {
t.Fatalf("expected canRun=false, err=nil, got canRun=%v, err=%v", canRun, err)
}
})

t.Run("Parent Completed Returns True", func(t *testing.T) {
parent := &task.Task{ID: "parent-2", Status: task.StatusCompleted}
_ = st.Save(parent)

child := &task.Task{ID: "child-2", DependsOn: []string{"parent-2"}, Status: task.StatusWaiting}
canRun, err := mgr.CanExecute(child)
if err != nil || !canRun {
t.Fatalf("expected canRun=true, err=nil, got canRun=%v, err=%v", canRun, err)
}
})

t.Run("Parent Failed Returns Error", func(t *testing.T) {
parent := &task.Task{ID: "parent-3", Status: task.StatusFailed}
_ = st.Save(parent)

child := &task.Task{ID: "child-3", DependsOn: []string{"parent-3"}, Status: task.StatusWaiting}
canRun, err := mgr.CanExecute(child)
if err == nil || canRun {
t.Fatalf("expected error for failed parent, got canRun=%v, err=%v", canRun, err)
}
})
}

func TestDAG_EvaluateDependents(t *testing.T) {
st := newMockStore()
mgr := NewManager(st)

parent := &task.Task{ID: "p-1", Status: task.StatusCompleted}
child := &task.Task{ID: "c-1", DependsOn: []string{"p-1"}, Status: task.StatusWaiting}
_ = st.Save(parent)
_ = st.Save(child)

pushed := false
schedPush := func(tk *task.Task) {
if tk.ID == "c-1" {
pushed = true
}
}

mgr.EvaluateDependents("p-1", schedPush)

if !pushed {
t.Fatalf("expected child task c-1 to be pushed to scheduler")
}

updatedChild, _ := st.Get("c-1")
if updatedChild.Status != task.StatusPending {
t.Fatalf("expected child status StatusPending, got %s", updatedChild.Status)
}
}

