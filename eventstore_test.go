package substrate_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate"
)

func TestEventStore_WithTimeController(t *testing.T) {
	// WithTimeController wires the controlled clock so that event timestamps
	// reflect the simulated time rather than real wall-clock time.
	fixedTime := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(fixedTime)

	store := substrate.NewEventStore(
		substrate.EventStoreConfig{Enabled: true, Backend: "memory"},
		substrate.WithTimeController(tc),
	)
	require.NotNil(t, store)

	ctx := context.Background()
	reqCtx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	resp := &substrate.AWSResponse{StatusCode: 200}

	err := store.RecordRequest(ctx, reqCtx, req, resp, time.Millisecond, 0, nil)
	require.NoError(t, err)

	events, err := store.GetEvents(ctx, substrate.EventFilter{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, fixedTime.Unix(), events[0].Timestamp.Unix())
}

func TestNewEventStore(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	require.NotNil(t, store)

	stats := store.GetStats(context.Background())
	assert.Equal(t, int64(0), stats.TotalEvents)
}

func TestEventStore_RecordEvent(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	event := &substrate.Event{
		StreamID:  "stream-1",
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Now(),
	}

	require.NoError(t, store.RecordEvent(ctx, event))

	// ID and sequence should be assigned.
	assert.NotEmpty(t, event.ID)
	assert.Equal(t, int64(0), event.Sequence)
}

func TestEventStore_RecordEvent_Disabled(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: false, Backend: "memory"})
	ctx := context.Background()

	event := &substrate.Event{Service: "s3", Operation: "PutObject", Timestamp: time.Now()}
	require.NoError(t, store.RecordEvent(ctx, event))

	stats := store.GetStats(ctx)
	assert.Equal(t, int64(0), stats.TotalEvents, "disabled store must not record events")
}

func TestEventStore_GetStream(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	const streamID = "test-stream"

	for i := range 5 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  streamID,
			Service:   "s3",
			Operation: "PutObject",
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
		}))
	}
	// Extra event on a different stream.
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "other-stream",
		Service:   "iam",
		Operation: "CreateUser",
		Timestamp: time.Now(),
	}))

	events, err := store.GetStream(ctx, streamID)
	require.NoError(t, err)
	assert.Len(t, events, 5)

	for _, e := range events {
		assert.Equal(t, streamID, e.StreamID)
	}
}

func TestEventStore_GetStream_Unknown(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	events, err := store.GetStream(context.Background(), "no-such-stream")
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestEventStore_GetEvent(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	event := &substrate.Event{
		StreamID:  "s",
		Service:   "dynamodb",
		Operation: "GetItem",
		Timestamp: time.Now(),
	}
	require.NoError(t, store.RecordEvent(ctx, event))

	got, err := store.GetEvent(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, event.ID, got.ID)
	assert.Equal(t, "GetItem", got.Operation)
}

func TestEventStore_GetEvent_NotFound(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	_, err := store.GetEvent(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestEventStore_GetEvents_Filter(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for _, svc := range []string{"s3", "s3", "iam", "dynamodb"} {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  "s",
			Service:   svc,
			Operation: "Op",
			Timestamp: time.Now(),
		}))
	}

	events, err := store.GetEvents(ctx, substrate.EventFilter{Service: "s3"})
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestEventStore_GetStats(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	hasErr := true
	events := []*substrate.Event{
		{StreamID: "a", Service: "s3", Operation: "Put", Cost: 0.005, Timestamp: time.Now()},
		{StreamID: "a", Service: "s3", Operation: "Get", Cost: 0.001, Error: "boom", Timestamp: time.Now()},
		{StreamID: "b", Service: "iam", Operation: "CreateUser", Timestamp: time.Now()},
	}
	for _, e := range events {
		require.NoError(t, store.RecordEvent(ctx, e))
	}

	stats := store.GetStats(ctx)
	assert.Equal(t, int64(3), stats.TotalEvents)
	assert.Equal(t, int64(2), stats.TotalStreams)
	assert.Equal(t, int64(1), stats.ErrorCount)
	assert.Equal(t, int64(2), stats.SuccessCount)
	assert.InDelta(t, 0.006, stats.TotalCost, 1e-9)
	assert.Equal(t, int64(2), stats.ServiceBreakdown["s3"])
	assert.Equal(t, int64(1), stats.ServiceBreakdown["iam"])

	_ = hasErr
}

func TestEventStore_ReplayIterator(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for i := range 3 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  "s",
			Service:   "s3",
			Operation: "Put",
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
		}))
	}

	iter, err := store.NewReplayIterator(ctx, substrate.EventFilter{StreamID: "s"})
	require.NoError(t, err)

	count := 0
	for iter.HasNext() {
		e, ok := iter.Next()
		require.True(t, ok)
		require.NotNil(t, e)
		count++
	}
	assert.Equal(t, 3, count)

	// After exhaustion, Next returns false.
	_, ok := iter.Next()
	assert.False(t, ok)

	// Reset restarts iteration.
	iter.Reset()
	assert.True(t, iter.HasNext())
}

func TestEventStore_Flush_Memory(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	assert.NoError(t, store.Flush(context.Background()))
}

func TestEventStore_Load_Memory(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	assert.NoError(t, store.Load(context.Background()))
}

func TestJSONSerializer(t *testing.T) {
	s := &substrate.JSONSerializer{}

	original := &substrate.Event{
		ID:        "evt-1",
		Sequence:  42,
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Cost:      0.005,
	}

	data, err := s.Serialize(original)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := s.Deserialize(data)
	require.NoError(t, err)
	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Sequence, decoded.Sequence)
	assert.Equal(t, original.Service, decoded.Service)
	assert.Equal(t, original.Cost, decoded.Cost)
}

func TestJSONSerializer_InvalidJSON(t *testing.T) {
	s := &substrate.JSONSerializer{}
	_, err := s.Deserialize([]byte("not json"))
	assert.Error(t, err)
}

func TestEventStore_Close_NoGoroutine(t *testing.T) {
	// Close must not panic when no async goroutine was started.
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	t.Cleanup(func() {}) // no additional cleanup needed
	assert.NoError(t, store.Close())
}

func TestEventStore_SnapshotInterval_Zero_Disabled(t *testing.T) {
	// SnapshotInterval=0 with a state ref should NOT start a goroutine.
	sm := substrate.NewMemoryStateManager()
	store := substrate.NewEventStore(
		substrate.EventStoreConfig{Enabled: true, Backend: "memory", SnapshotInterval: 0},
		substrate.WithStateManager(sm),
	)
	ctx := context.Background()
	for range 10 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "s", Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}
	// No snapshot should exist because the interval is disabled.
	_, err := store.GetLatestSnapshot(ctx, "s")
	assert.Error(t, err, "no snapshot expected when SnapshotInterval is zero")
	assert.NoError(t, store.Close())
}

func TestEventStore_AsyncSnapshot_CreatesSnapshot(t *testing.T) {
	sm := substrate.NewMemoryStateManager()
	store := substrate.NewEventStore(
		substrate.EventStoreConfig{Enabled: true, Backend: "memory", SnapshotInterval: 5},
		substrate.WithStateManager(sm),
	)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	ctx := context.Background()
	const streamID = "async-snap-stream"
	for range 5 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: streamID, Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}

	// Poll up to 500ms for the async snapshot to appear.
	deadline := time.Now().Add(500 * time.Millisecond)
	var snap interface{}
	for time.Now().Before(deadline) {
		s, err := store.GetLatestSnapshot(ctx, streamID)
		if err == nil && s != nil {
			snap = s
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.NotNil(t, snap, "async snapshot should have been created after 5 events")
}

func TestEventStore_FilePersistence(t *testing.T) {
	dir := t.TempDir()
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "file",
		PersistPath: dir,
	})

	ctx := context.Background()
	for range 100 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "file-stream", Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}

	require.NoError(t, store.Flush(ctx))

	// New store that loads from the same directory.
	store2 := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "file",
		PersistPath: dir,
	})
	require.NoError(t, store2.Load(ctx))

	events, err := store2.GetEvents(ctx, substrate.EventFilter{Service: "s3"})
	require.NoError(t, err)
	assert.Len(t, events, 100, "all 100 events should be loaded from NDJSON files")
}

func TestEventStore_FilePersistence_AppendOnly(t *testing.T) {
	dir := t.TempDir()
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "file",
		PersistPath: dir,
	})
	ctx := context.Background()

	// Record and flush 50 events.
	for range 50 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "append-stream", Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}
	require.NoError(t, store.Flush(ctx))

	// Record 50 more and flush again — should append, not duplicate.
	for range 50 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "append-stream", Service: "s3", Operation: "Get", Timestamp: time.Now(),
		}))
	}
	require.NoError(t, store.Flush(ctx))

	// Load into a fresh store and check exactly 100 events.
	store2 := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "file",
		PersistPath: dir,
	})
	require.NoError(t, store2.Load(ctx))

	events, err := store2.GetEvents(ctx, substrate.EventFilter{})
	require.NoError(t, err)
	assert.Len(t, events, 100, "should have exactly 100 events, not 150 (append-only)")
}

func TestEventStore_SQLitePersistence(t *testing.T) {
	dir := t.TempDir()
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "sqlite",
		PersistPath: dir,
		DSN:         "test.db",
	})
	ctx := context.Background()

	for range 1000 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "sqlite-stream", Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}
	require.NoError(t, store.Flush(ctx))
	require.NoError(t, store.Close())

	// Load into a fresh store.
	store2 := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "sqlite",
		PersistPath: dir,
		DSN:         "test.db",
	})
	require.NoError(t, store2.Load(ctx))

	events, err := store2.GetEvents(ctx, substrate.EventFilter{Service: "s3"})
	require.NoError(t, err)
	assert.Len(t, events, 1000, "all 1000 events should round-trip through SQLite")
	require.NoError(t, store2.Close())
}

func TestEventStore_SQLite_IdempotentFlush(t *testing.T) {
	dir := t.TempDir()
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "sqlite",
		PersistPath: dir,
		DSN:         "idempotent.db",
	})
	ctx := context.Background()

	for range 1000 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "idempotent-stream", Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}

	// Flush twice — INSERT OR IGNORE means the second flush adds no rows.
	require.NoError(t, store.Flush(ctx))
	require.NoError(t, store.Flush(ctx))

	// Load into a fresh store.
	store2 := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "sqlite",
		PersistPath: dir,
		DSN:         "idempotent.db",
	})
	require.NoError(t, store2.Load(ctx))

	events, err := store2.GetEvents(ctx, substrate.EventFilter{})
	require.NoError(t, err)
	assert.Len(t, events, 1000, "double flush must not duplicate rows")
	require.NoError(t, store.Close())
	require.NoError(t, store2.Close())
}

func TestEventStore_SQLite_SnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "sqlite",
		PersistPath: dir,
		DSN:         "snap.db",
	})
	ctx := context.Background()

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID: "snap-sqlite-stream", Service: "s3", Operation: "Put", Timestamp: time.Now(),
	}))

	sm := substrate.NewMemoryStateManager()
	snap, err := store.CreateSnapshot(ctx, "snap-sqlite-stream", sm)
	require.NoError(t, err)
	require.NotNil(t, snap)

	require.NoError(t, store.Flush(ctx))
	require.NoError(t, store.Close())

	store2 := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled:     true,
		Backend:     "sqlite",
		PersistPath: dir,
		DSN:         "snap.db",
	})
	require.NoError(t, store2.Load(ctx))

	latest, err := store2.GetLatestSnapshot(ctx, "snap-sqlite-stream")
	require.NoError(t, err)
	assert.NotNil(t, latest)
	require.NoError(t, store2.Close())
}

func TestExportNDJSON_Empty(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	var buf bytes.Buffer
	n, err := store.ExportNDJSON(ctx, substrate.EventFilter{}, &buf)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Empty(t, strings.TrimSpace(buf.String()))
}

func TestExportNDJSON_RoundTrip(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID: "s1", Service: "s3", Operation: "PutObject", AccountID: "123456789012", Region: "us-east-1", Timestamp: ts,
	}))
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID: "s1", Service: "s3", Operation: "GetObject", AccountID: "123456789012", Region: "us-east-1", Timestamp: ts,
	}))

	var buf bytes.Buffer
	n, err := store.ExportNDJSON(ctx, substrate.EventFilter{}, &buf)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 2)

	var ev substrate.Event
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ev))
	assert.Equal(t, "PutObject", ev.Operation)
	assert.Equal(t, "s3", ev.Service)
}

func TestExportCSV_HeaderRow(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	var buf bytes.Buffer
	n, err := store.ExportCSV(ctx, substrate.EventFilter{}, &buf)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	r := csv.NewReader(&buf)
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1) // header only
	assert.Equal(t, []string{"sequence", "timestamp", "account_id", "region", "service", "operation", "duration_ms", "cost", "error", "error_code", "stream_id"}, records[0])
}

func TestExportCSV_Values(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID: "stream1", Service: "dynamodb", Operation: "GetItem",
		AccountID: "111111111111", Region: "us-west-2", Timestamp: ts,
	}))

	var buf bytes.Buffer
	n, err := store.ExportCSV(ctx, substrate.EventFilter{}, &buf)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	r := csv.NewReader(&buf)
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2) // header + 1 row

	row := records[1]
	assert.Equal(t, "dynamodb", row[4])
	assert.Equal(t, "GetItem", row[5])
	assert.Equal(t, "stream1", row[10])
}
