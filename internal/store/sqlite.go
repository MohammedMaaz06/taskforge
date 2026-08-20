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
db, err := sql.Open("sqlite", dbPath)
if err != nil {
return nil, fmt.Errorf("failed to open database: %w", err)
}

query := `
CREATE TABLE IF NOT EXISTS tasks (
id TEXT PRIMARY KEY,
name TEXT NOT NULL,
payload BLOB,
status TEXT NOT NULL,
priority INTEGER NOT NULL,
max_retries INTEGER NOT NULL,
current_retry INTEGER NOT NULL,
last_error TEXT,
scheduled_at DATETIME NOT NULL,
created_at DATETIME NOT NULL,
updated_at DATETIME NOT NULL
);`

if _, err := db.Exec(query); err != nil {
return nil, fmt.Errorf("failed to create tasks table: %w", err)
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

_, err := s.db.Exec(query, t.ID, t.Name, t.Payload, t.Status, t.Priority, t.MaxRetries, t.CurrentRetry, t.LastError, t.ScheduledAt, t.CreatedAt, t.UpdatedAt)
return err
}

func (s *SQLiteStore) Get(id string) (*task.Task, error) {
query := `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE id = ?`
row := s.db.QueryRow(query, id)

var t task.Task
err := row.Scan(&t.ID, &t.Name, &t.Payload, &t.Status, &t.Priority, &t.MaxRetries, &t.CurrentRetry, &t.LastError, &t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt)
if err == sql.ErrNoRows {
return nil, ErrTaskNotFound
}
if err != nil {
return nil, err
}
return &t, nil
}

func (s *SQLiteStore) GetByID(id string) (*task.Task, error) {
return s.Get(id)
}

func (s *SQLiteStore) List(statusFilter string) ([]*task.Task, error) {
var query string
var args []interface{}

if statusFilter != "" {
query = `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE status = ? ORDER BY created_at DESC`
args = append(args, statusFilter)
} else {
query = `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks ORDER BY created_at DESC`
}

rows, err := s.db.Query(query, args...)
if err != nil {
return nil, err
}
defer rows.Close()

tasks := []*task.Task{}
for rows.Next() {
var t task.Task
if err := rows.Scan(&t.ID, &t.Name, &t.Payload, &t.Status, &t.Priority, &t.MaxRetries, &t.CurrentRetry, &t.LastError, &t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
return nil, err
}
tasks = append(tasks, &t)
}

return tasks, rows.Err()
}

func (s *SQLiteStore) UpdateStatus(id string, status task.Status, errMsg string) error {
query := `UPDATE tasks SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`
_, err := s.db.Exec(query, status, errMsg, time.Now().UTC(), id)
return err
}

func (s *SQLiteStore) Close() error {
return s.db.Close()
}
