package dag

import (
"sync"
"testing"

"taskforge/internal/store"
"taskforge/pkg/task"
)

type mockStore struct {
mu        sync.Mutex
tasks     map[string]*task.Task
failList  bool
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
if m.failList {
return nil, store.ErrTaskNotFound
}
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

t.Run("Parent Missing Returns Error", func(t *testing.T) {
child := &task.Task{ID: "child-0", DependsOn: []string{"missing-parent"}, Status: task.StatusWaiting}
canRun, err := mgr.CanExecute(child)
if err == nil || canRun {
t.Fatalf("expected error for missing parent, got canRun=%v, err=%v", canRun, err)
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

t.Run("Parent Failed/DLQ/Blocked Returns Error", func(t *testing.T) {
statuses := []task.Status{task.StatusFailed, task.StatusDLQ, task.StatusBlocked}

for idx, stt := range statuses {
pID := "parent-unrecoverable-" + string(stt)
cID := "child-unrecoverable-" + string(stt) + string(rune(idx))

parent := &task.Task{ID: pID, Status: stt}
_ = st.Save(parent)

child := &task.Task{ID: cID, DependsOn: []string{pID}, Status: task.StatusWaiting}
canRun, err := mgr.CanExecute(child)
if err == nil || canRun {
t.Fatalf("expected error for parent status %s, got canRun=%v, err=%v", stt, canRun, err)
}
}
})
}

func TestDAG_EvaluateDependents(t *testing.T) {
t.Run("Store List Error Handled Gracefully", func(t *testing.T) {
st := newMockStore()
st.failList = true
mgr := NewManager(st)

pushed := false
mgr.EvaluateDependents("p-1", func(tk *task.Task) { pushed = true })
if pushed {
t.Fatal("expected no push on store list failure")
}
})

t.Run("Ignores Non-Waiting Tasks and Unrelated Dependencies", func(t *testing.T) {
st := newMockStore()
mgr := NewManager(st)

parent := &task.Task{ID: "p-1", Status: task.StatusCompleted}
runningTask := &task.Task{ID: "c-running", DependsOn: []string{"p-1"}, Status: task.StatusRunning}
unrelatedTask := &task.Task{ID: "c-other", DependsOn: []string{"p-99"}, Status: task.StatusWaiting}

_ = st.Save(parent)
_ = st.Save(runningTask)
_ = st.Save(unrelatedTask)

pushedCount := 0
mgr.EvaluateDependents("p-1", func(tk *task.Task) { pushedCount++ })

if pushedCount != 0 {
t.Fatalf("expected 0 pushes, got %d", pushedCount)
}
})

t.Run("Blocks Dependent Task When Parent Fails", func(t *testing.T) {
st := newMockStore()
mgr := NewManager(st)

parent := &task.Task{ID: "p-failed", Status: task.StatusFailed}
child := &task.Task{ID: "c-failed-dep", DependsOn: []string{"p-failed"}, Status: task.StatusWaiting}

_ = st.Save(parent)
_ = st.Save(child)

pushed := false
mgr.EvaluateDependents("p-failed", func(tk *task.Task) { pushed = true })

if pushed {
t.Fatal("expected failed parent child NOT to be pushed")
}

updatedChild, _ := st.Get("c-failed-dep")
if updatedChild.Status != task.StatusBlocked {
t.Fatalf("expected child status StatusBlocked, got %s", updatedChild.Status)
}
})

t.Run("Pushes Dependent Task When Parent Completes", func(t *testing.T) {
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
})
}

