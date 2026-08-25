package store

import (
"database/sql"
"fmt"
"taskforge/pkg/task"
"time"

_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
// Enable WAL mode and a 5000ms busy timeout for concurrent writes
dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL", dbPath)
db, err := sql.Open("sqlite3", dsn)
if err != nil {
return nil, err
}

// Limit open connections to prevent connection pool write-racing
db.SetMaxOpenConns(1)

if err := db.Ping(); err != nil {
return nil, err
}

s := &SQLiteStore{db: db}
if err := s.initSchema(); err != nil {
return nil, err
}

return s, nil
}

func (s *SQLiteStore) initSchema() error {
query := `
CREATE TABLE IF NOT EXISTS tasks (
id TEXT PRIMARY KEY,
name TEXT,
payload BLOB,
status TEXT,
priority INTEGER,
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
INSERT INTO tasks (id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

_, err := s.db.Exec(query,
t.ID, t.Name, t.Payload, t.Status, t.Priority,
t.MaxRetries, t.CurrentRetry, t.LastError,
t.ScheduledAt, t.CreatedAt, t.UpdatedAt,
)
return err
}

func (s *SQLiteStore) Get(id string) (*task.Task, error) {
query := `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE id = ?;`
row := s.db.QueryRow(query, id)

var t task.Task
err := row.Scan(
&t.ID, &t.Name, &t.Payload, &t.Status, &t.Priority,
&t.MaxRetries, &t.CurrentRetry, &t.LastError,
&t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt,
)
if err != nil {
return nil, err
}
return &t, nil
}

func (s *SQLiteStore) UpdateStatus(id string, status task.Status, lastError string) error {
query := `UPDATE tasks SET status = ?, last_error = ?, updated_at = ? WHERE id = ?;`
_, err := s.db.Exec(query, status, lastError, time.Now().UTC(), id)
return err
}

func (s *SQLiteStore) List(status string) ([]*task.Task, error) {
var query string
var args []interface{}

if status != "" {
query = `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks WHERE status = ? ORDER BY created_at DESC;`
args = append(args, status)
} else {
query = `SELECT id, name, payload, status, priority, max_retries, current_retry, last_error, scheduled_at, created_at, updated_at FROM tasks ORDER BY created_at DESC;`
}

rows, err := s.db.Query(query, args...)
if err != nil {
return nil, err
}
defer rows.Close()

var tasks []*task.Task
for rows.Next() {
var t task.Task
if err := rows.Scan(
&t.ID, &t.Name, &t.Payload, &t.Status, &t.Priority,
&t.MaxRetries, &t.CurrentRetry, &t.LastError,
&t.ScheduledAt, &t.CreatedAt, &t.UpdatedAt,
); err != nil {
return nil, err
}
tasks = append(tasks, &t)
}
return tasks, nil
}

func (s *SQLiteStore) Close() error {
return s.db.Close()
}
