package store

import (
"database/sql"
"errors"
"fmt"

_ "modernc.org/sqlite"
"taskforge/pkg/task"
)

type SQLiteStore struct {
db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
db, err := sql.Open("sqlite", dbPath)
if err != nil {
return nil, fmt.Errorf("failed to open sqlite database: %w", err)
}

if err := db.Ping(); err != nil {
return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
}

s := &SQLiteStore{db: db}
if err := s.initSchema(); err != nil {
return nil, fmt.Errorf("failed to initialize schema: %w", err)
}

return s, nil
}

func (s *SQLiteStore) initSchema() error {
query := `
CREATE TABLE IF NOT EXISTS tasks (
id TEXT PRIMARY KEY,
name TEXT,
payload BLOB,
priority INTEGER,
status TEXT,
max_retries INTEGER,
current_retry INTEGER,
last_error TEXT,
scheduled_at DATETIME,
created_at DATETIME,
updated_at DATETIME
);`
_, err := s.db.Exec(query)
return err
}

func (s *SQLiteStore) Save(t *task.Task) error {
query := `
INSERT INTO tasks (id, name, payload, priority, status, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
name=excluded.name,
payload=excluded.payload,
priority=excluded.priority,
status=excluded.status,
max_retries=excluded.max_retries,
current_retry=excluded.current_retry,
last_error=excluded.last_error,
scheduled_at=excluded.scheduled_at,
updated_at=excluded.updated_at;`

_, err := s.db.Exec(query, t.ID, t.Name, t.Payload, t.Priority, string(t.Status), t.MaxRetries, t.CurrentRetry, t.LastError, t.ScheduledAt, t.CreatedAt, t.UpdatedAt)
return err
}

func (s *SQLiteStore) Get(id string) (*task.Task, error) {
query := `SELECT id, name, payload, priority, status, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE id = ?`
row := s.db.QueryRow(query, id)

var t task.Task
var statusStr string
err := row.Scan(&t.ID, &t.Name, &t.Payload, &t.Priority, &statusStr, &t.MaxRetries, &t.CurrentRetry, &t.LastError, &t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt)
if err != nil {
if errors.Is(err, sql.ErrNoRows) {
return nil, ErrTaskNotFound
}
return nil, err
}
t.Status = task.Status(statusStr)
return &t, nil
}

func (s *SQLiteStore) List(statusFilter ...string) ([]*task.Task, error) {
var query string
var args []interface{}

if len(statusFilter) > 0 && statusFilter[0] != "" {
query = `SELECT id, name, payload, priority, status, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE status = ?`
args = append(args, statusFilter[0])
} else {
query = `SELECT id, name, payload, priority, status, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks`
}

rows, err := s.db.Query(query, args...)
if err != nil {
return nil, err
}
defer rows.Close()

var tasks []*task.Task
for rows.Next() {
var t task.Task
var statusStr string
if err := rows.Scan(&t.ID, &t.Name, &t.Payload, &t.Priority, &statusStr, &t.MaxRetries, &t.CurrentRetry, &t.LastError, &t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
return nil, err
}
t.Status = task.Status(statusStr)
tasks = append(tasks, &t)
}
return tasks, nil
}

func (s *SQLiteStore) UpdateStatus(id string, status task.Status, lastErr string) error {
query := `UPDATE tasks SET status = ?, last_error = ? WHERE id = ?`
_, err := s.db.Exec(query, string(status), lastErr, id)
return err
}

func (s *SQLiteStore) Close() error {
if s.db != nil {
return s.db.Close()
}
return nil
}

