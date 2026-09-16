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
dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", dbPath)
db, err := sql.Open("sqlite", dsn)
if err != nil {
return nil, fmt.Errorf("failed to open sqlite db: %w", err)
}

db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
db.SetConnMaxLifetime(time.Hour)

store := &SQLiteStore{db: db}
if err := store.initSchema(); err != nil {
db.Close()
return nil, err
}

return store, nil
}

func (s *SQLiteStore) initSchema() error {
query := `
CREATE TABLE IF NOT EXISTS tasks (
id TEXT PRIMARY KEY,
type TEXT,
status TEXT,
payload TEXT,
max_retries INTEGER,
current_retry INTEGER,
last_error TEXT,
created_at DATETIME,
updated_at DATETIME
);`
_, err := s.db.Exec(query)
return err
}

func (s *SQLiteStore) Save(t *task.Task) error {
query := `
INSERT INTO tasks (id, type, status, payload, max_retries, current_retry, last_error, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
type=excluded.type,
status=excluded.status,
payload=excluded.payload,
current_retry=excluded.current_retry,
last_error=excluded.last_error,
updated_at=excluded.updated_at`
_, err := s.db.Exec(query, t.ID, t.Name, t.Status, t.Payload, t.MaxRetries, t.CurrentRetry, t.LastError, t.CreatedAt, t.UpdatedAt)
return err
}

func (s *SQLiteStore) Get(id string) (*task.Task, error) {
query := `SELECT id, type, status, payload, max_retries, current_retry, last_error, created_at, updated_at FROM tasks WHERE id = ?`
row := s.db.QueryRow(query, id)

var t task.Task
err := row.Scan(&t.ID, &t.Name, &t.Status, &t.Payload, &t.MaxRetries, &t.CurrentRetry, &t.LastError, &t.CreatedAt, &t.UpdatedAt)
if err == sql.ErrNoRows {
return nil, ErrTaskNotFound
}
if err != nil {
return nil, err
}
return &t, nil
}

func (s *SQLiteStore) List(statusFilter ...string) ([]*task.Task, error) {
query := `SELECT id, type, status, payload, max_retries, current_retry, last_error, created_at, updated_at FROM tasks`
var rows *sql.Rows
var err error

if len(statusFilter) > 0 {
query += ` WHERE status = ?`
rows, err = s.db.Query(query, statusFilter[0])
} else {
rows, err = s.db.Query(query)
}

if err != nil {
return nil, err
}
defer rows.Close()

var tasks []*task.Task
for rows.Next() {
var t task.Task
if err := rows.Scan(&t.ID, &t.Name, &t.Status, &t.Payload, &t.MaxRetries, &t.CurrentRetry, &t.LastError, &t.CreatedAt, &t.UpdatedAt); err != nil {
return nil, err
}
tasks = append(tasks, &t)
}
return tasks, nil
}

func (s *SQLiteStore) UpdateStatus(id string, status task.Status, lastErr string) error {
query := `UPDATE tasks SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`
res, err := s.db.Exec(query, status, lastErr, time.Now(), id)
if err != nil {
return err
}
affected, _ := res.RowsAffected()
if affected == 0 {
return ErrTaskNotFound
}
return nil
}

func (s *SQLiteStore) Close() error {
return s.db.Close()
}
