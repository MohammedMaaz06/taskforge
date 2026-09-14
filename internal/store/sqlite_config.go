package store

import (
"database/sql"
"fmt"
"time"
)

func ConfigureSQLitePerformance(db *sql.DB) error {
// WAL mode for non-blocking concurrent reads/writes
pragmas := []string{
"PRAGMA journal_mode=WAL;",
"PRAGMA synchronous=NORMAL;",
"PRAGMA cache_size=-64000;", // 64MB cache size
"PRAGMA busy_timeout=5000;", // 5s timeout on lock contention
"PRAGMA foreign_keys=ON;",
}

for _, pragma := range pragmas {
if _, err := db.Exec(pragma); err != nil {
return fmt.Errorf("failed to apply %s: %w", pragma, err)
}
}

// Connection Pool Limits
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(10)
db.SetConnMaxLifetime(5 * time.Minute)
db.SetConnMaxIdleTime(2 * time.Minute)

return nil
}
