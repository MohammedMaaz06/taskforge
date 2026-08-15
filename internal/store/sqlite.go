package store

import (
"database/sql"
"fmt"
"time"

_ "modernc.org/sqlite"
"taskforge/pkg/task"
)

type SQLiteStore struct {
db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)")
if err != nil {
return nil, fmt.Errorf("failed to open sqlite db: %w", err)
}

schema := `
CREATE TABLE IF NOT EXISTS tasks (
id TEXT PRIMARY KEY,
name TEXT NOT NULL,
payload BLOB,
status TEXT NOT NULL,
priority INTEGER NOT NULL,
max_retries INTEGER NOT NULL,
current_retry INTEGER NOT NULL,
last_error TEXT,
scheduled_at DATETIME,
created_at DATETIME,
updated_at DATETIME
);`

if _, err := db.Exec(schema); err != nil {
return nil, fmt.Errorf("failed to initialize schema: %w", err)
}

return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Save(t *task.Task) error {
query := `
INSERT INTO tasks (id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
status = excluded.status,
current_retry = excluded.current_retry,
last_error = excluded.last_error,
updated_at = excluded.updated_at;`

_, err := s.db.Exec(query,
t.ID, t.Name, t.Payload, string(t.Status), t.Priority,
t.MaxRetries, t.CurrentRetry, t.LastError,
t.ScheduledAt, t.CreatedAt, time.Now(),
)
return err
}

func (s *SQLiteStore) Get(id string) (*task.Task, error) {
query := `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE id = ?`

row := s.db.QueryRow(query, id)

var t task.Task
var statusStr string

err := row.Scan(
&t.ID, &t.Name, &t.Payload, &statusStr, &t.Priority,
&t.MaxRetries, &t.CurrentRetry, &t.LastError,
&t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt,
)
if err == sql.ErrNoRows {
return nil, ErrTaskNotFound
} else if err != nil {
return nil, err
}

t.Status = task.Status(statusStr)
return &t, nil
}

func (s *SQLiteStore) UpdateStatus(id string, status task.Status, errMessage string) error {
query := `UPDATE tasks SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`
_, err := s.db.Exec(query, string(status), errMessage, time.Now(), id)
return err
}

func (s *SQLiteStore) Close() error {
return s.db.Close()
}
