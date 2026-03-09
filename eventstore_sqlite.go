package substrate

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // register the sqlite3 driver
)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    stream_id   TEXT NOT NULL,
    sequence    INTEGER NOT NULL,
    timestamp   INTEGER NOT NULL,
    service     TEXT,
    operation   TEXT,
    account_id  TEXT,
    region      TEXT,
    status_code INTEGER,
    cost        REAL,
    body        BLOB,
    UNIQUE(stream_id, sequence)
);
CREATE TABLE IF NOT EXISTS snapshots (
    id         TEXT PRIMARY KEY,
    stream_id  TEXT NOT NULL,
    position   INTEGER NOT NULL,
    state      BLOB,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_stream    ON events(stream_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_service   ON events(service);
CREATE INDEX IF NOT EXISTS idx_events_operation ON events(operation);
`

// sqliteBackend persists events and snapshots to a SQLite database.
type sqliteBackend struct {
	db          *sql.DB
	serializer  Serializer
	flushCursor int64 // count of events already flushed
}

// newSQLiteBackend opens (or creates) the SQLite database at dsn and applies
// the schema.
func newSQLiteBackend(dsn string, s Serializer) (*sqliteBackend, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}

	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return &sqliteBackend{db: db, serializer: s}, nil
}

// flush writes events and snapshots that have not yet been persisted.
// INSERT OR IGNORE ensures idempotency.
func (sb *sqliteBackend) flush(ctx context.Context, events []*Event, snapshots map[string]*Snapshot) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	newEvents := events[sb.flushCursor:]
	for _, ev := range newEvents {
		body, serErr := sb.serializer.Serialize(ev)
		if serErr != nil {
			return fmt.Errorf("serialize event %s: %w", ev.ID, serErr)
		}
		statusCode := 0
		if ev.Response != nil {
			statusCode = ev.Response.StatusCode
		}
		_, execErr := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO events
			    (id, stream_id, sequence, timestamp, service, operation,
			     account_id, region, status_code, cost, body)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			ev.ID, ev.StreamID, ev.Sequence, ev.Timestamp.UnixNano(),
			ev.Service, ev.Operation, ev.AccountID, ev.Region,
			statusCode, ev.Cost, body,
		)
		if execErr != nil {
			return fmt.Errorf("insert event %s: %w", ev.ID, execErr)
		}
	}

	for _, snap := range snapshots {
		_, execErr := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO snapshots
			    (id, stream_id, position, state, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			snap.ID, snap.StreamID, snap.Sequence, snap.State, snap.Timestamp.UnixNano(),
		)
		if execErr != nil {
			return fmt.Errorf("insert snapshot %s: %w", snap.ID, execErr)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	sb.flushCursor = int64(len(events))
	return nil
}

// load reads all events and snapshots from the database, ordered by sequence.
func (sb *sqliteBackend) load(ctx context.Context) ([]*Event, map[string]*Snapshot, error) {
	rows, err := sb.db.QueryContext(ctx,
		`SELECT body FROM events ORDER BY sequence`)
	if err != nil {
		return nil, nil, fmt.Errorf("query events: %w", err)
	}

	var events []*Event
	for rows.Next() {
		var body []byte
		if scanErr := rows.Scan(&body); scanErr != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("scan event: %w", scanErr)
		}
		ev, deserErr := sb.serializer.Deserialize(body)
		if deserErr != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("deserialize event: %w", deserErr)
		}
		events = append(events, ev)
	}
	if iterErr := rows.Err(); iterErr != nil {
		_ = rows.Close()
		return nil, nil, fmt.Errorf("iterate events: %w", iterErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return nil, nil, fmt.Errorf("close event rows: %w", closeErr)
	}

	snapRows, err := sb.db.QueryContext(ctx,
		`SELECT id, stream_id, position, state, created_at FROM snapshots`)
	if err != nil {
		return nil, nil, fmt.Errorf("query snapshots: %w", err)
	}

	snapshots := make(map[string]*Snapshot)
	for snapRows.Next() {
		var (
			id        string
			streamID  string
			position  int64
			state     []byte
			createdAt int64
		)
		if scanErr := snapRows.Scan(&id, &streamID, &position, &state, &createdAt); scanErr != nil {
			_ = snapRows.Close()
			return nil, nil, fmt.Errorf("scan snapshot: %w", scanErr)
		}
		snapshots[id] = &Snapshot{
			ID:        id,
			StreamID:  streamID,
			Sequence:  position,
			State:     state,
			Timestamp: time.Unix(0, createdAt),
		}
	}
	if iterErr := snapRows.Err(); iterErr != nil {
		_ = snapRows.Close()
		return nil, nil, fmt.Errorf("iterate snapshots: %w", iterErr)
	}
	if closeErr := snapRows.Close(); closeErr != nil {
		return nil, nil, fmt.Errorf("close snapshot rows: %w", closeErr)
	}

	sb.flushCursor = int64(len(events))
	return events, snapshots, nil
}

// close releases the database connection.
func (sb *sqliteBackend) close() error {
	return sb.db.Close()
}
