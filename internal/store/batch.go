package store

import (
"fmt"
"taskforge/internal/task"
)

func (s *SQLiteStore) SaveBatch(tasks []*task.Task) error {
tx, err := s.db.Begin()
if err != nil {
return fmt.Errorf("failed to begin transaction: %w", err)
}
defer tx.Rollback()

stmt, err := tx.Prepare(`
INSERT INTO tasks (id, type, status, payload, max_retries, retry_count, last_error, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
status=excluded.status,
payload=excluded.payload,
retry_count=excluded.retry_count,
last_error=excluded.last_error,
updated_at=excluded.updated_at
`)
if err != nil {
return fmt.Errorf("failed to prepare statement: %w", err)
}
defer stmt.Close()

for _, t := range tasks {
_, err := stmt.Exec(t.ID, t.Type, t.Status, t.Payload, t.MaxRetries, t.RetryCount, t.LastError, t.CreatedAt, t.UpdatedAt)
if err != nil {
return fmt.Errorf("failed to insert task %s: %w", t.ID, err)
}
}

return tx.Commit()
}
