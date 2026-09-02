package dag

import (
"fmt"
"taskforge/internal/store"
"taskforge/pkg/task"
)

type Manager struct {
store store.Store
}

func NewManager(st store.Store) *Manager {
return &Manager{store: st}
}

// CanExecute checks if all parent dependencies for a task are COMPLETED.
func (m *Manager) CanExecute(t *task.Task) (bool, error) {
if len(t.DependsOn) == 0 {
return true, nil
}

for _, parentID := range t.DependsOn {
parent, err := m.store.Get(parentID)
if err != nil || parent == nil {
return false, fmt.Errorf("parent task %s not found", parentID)
}

if parent.Status == task.StatusFailed || parent.Status == task.StatusDLQ || parent.Status == task.StatusBlocked {
return false, fmt.Errorf("parent task %s in unrecoverable state: %s", parentID, parent.Status)
}

if parent.Status != task.StatusCompleted {
return false, nil
}
}

return true, nil
}

// EvaluateDependents checks all waiting tasks across the system when a task completes or fails.
func (m *Manager) EvaluateDependents(completedParentID string, schedPush func(*task.Task)) {
tasks, err := m.store.List()
if err != nil {
return
}

for _, t := range tasks {
if t.Status != task.StatusWaiting {
continue
}

// Check if this waiting task depends on the completed parent
isDependent := false
for _, depID := range t.DependsOn {
if depID == completedParentID {
isDependent = true;
break
}
}

if !isDependent {
continue
}

canRun, err := m.CanExecute(t)
if err != nil {
// Parent failed/blocked -> mark dependent as BLOCKED
t.Status = task.StatusBlocked
t.LastError = fmt.Sprintf("dependency error: %v", err)
t.UpdatedAt = t.UpdatedAt
m.store.Save(t)
continue
}

if canRun {
t.Status = task.StatusPending
m.store.Save(t)
schedPush(t)
}
}
}

