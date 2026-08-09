package emulator_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #599: MaxEventsInMemory was declared, plumbed through the config and asserted by
// a config test, but read nowhere — and EventStore.Flush had zero callers. Two
// halves of one unfinished feature: a run configured with a cap persisted nothing
// unless the consumer knew to call Flush itself, so a crash or a test binary that
// simply exited lost every event, with the setting reporting otherwise.

// recordN records n events on one stream, so a test reads as "the count crossed the
// threshold" rather than as a loop.
func recordN(t *testing.T, store *emulator.EventStore, streamID string, n int) {
	t.Helper()
	ctx := context.Background()
	for range n {
		require.NoError(t, store.RecordEvent(ctx, &emulator.Event{
			StreamID: streamID, Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}
}

// ndjsonLines counts persisted events across every NDJSON file, rotations included.
// The backend writes one event per line with a trailing newline, so newlines are the
// event count. Counting bytes on disk rather than loading through a second store
// keeps the assertion on what was actually persisted — and is what distinguishes an
// append from a re-write of the whole stream.
func ndjsonLines(t *testing.T, dir string) int {
	t.Helper()
	eventsDir := filepath.Join(dir, "events")
	entries, err := os.ReadDir(eventsDir)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)

	total := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(eventsDir, entry.Name())) //nolint:gosec // test-owned temp dir
		require.NoError(t, readErr)
		total += strings.Count(string(data), "\n")
	}
	return total
}

// TestEventStore_MaxEventsInMemory_FlushesWithoutAnExplicitCall is #599's point.
// Nothing here calls Flush.
func TestEventStore_MaxEventsInMemory_FlushesWithoutAnExplicitCall(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "file",
		PersistPath:       dir,
		MaxEventsInMemory: 5,
	})

	// Below the threshold: nothing is written yet, so the test cannot pass by the
	// store having flushed on every single event.
	recordN(t, store, "auto", 4)
	assert.Equal(t, 0, ndjsonLines(t, dir),
		"a flush before the threshold would make the threshold meaningless")

	// The crossing.
	recordN(t, store, "auto", 1)
	assert.Equal(t, 5, ndjsonLines(t, dir), "crossing the threshold must flush")
}

// TestEventStore_MaxEventsInMemory_FlushesRepeatedly pins that the trigger fires on
// every crossing rather than once. A long-running server is the case that matters:
// flushing only at the first threshold would persist the first N events and then
// silently stop.
func TestEventStore_MaxEventsInMemory_FlushesRepeatedly(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "file",
		PersistPath:       dir,
		MaxEventsInMemory: 10,
	})

	recordN(t, store, "repeat", 10)
	require.Equal(t, 10, ndjsonLines(t, dir))

	recordN(t, store, "repeat", 10)
	assert.Equal(t, 20, ndjsonLines(t, dir), "the second crossing must flush too")

	// And the events between crossings are not lost — they are in memory, and the
	// next crossing writes them.
	recordN(t, store, "repeat", 7)
	assert.Equal(t, 20, ndjsonLines(t, dir), "no crossing yet")
	recordN(t, store, "repeat", 3)
	assert.Equal(t, 30, ndjsonLines(t, dir),
		"the crossing writes everything recorded since the last one")
}

// TestEventStore_MaxEventsInMemory_AppendsRatherThanDuplicating pins the cursor
// contract the repeated trigger now depends on. The backend writes events[cursor:]
// and advances the cursor, so a flush that re-wrote from zero would multiply every
// event by the number of crossings — and the count is the only thing that shows it.
func TestEventStore_MaxEventsInMemory_AppendsRatherThanDuplicating(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "file",
		PersistPath:       dir,
		MaxEventsInMemory: 5,
	})

	recordN(t, store, "dup", 25)
	assert.Equal(t, 25, ndjsonLines(t, dir),
		"five crossings must produce 25 lines, not 75")

	// An explicit Flush after the automatic ones must also add nothing.
	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, 25, ndjsonLines(t, dir), "a redundant flush must be a no-op")
}

// TestEventStore_MaxEventsInMemory_KeepsTheFullHistoryInMemory is the property that
// makes this safe for replay. MaxEventsInMemory is a *flush* threshold, not a cap:
// the events stay in memory, so replay and time-travel debugging see the whole run.
// If it ever became an eviction bound, this is the test that would fail.
func TestEventStore_MaxEventsInMemory_KeepsTheFullHistoryInMemory(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "file",
		PersistPath:       dir,
		MaxEventsInMemory: 5,
	})
	ctx := context.Background()

	recordN(t, store, "history", 23)

	events, err := store.GetEvents(ctx, emulator.EventFilter{})
	require.NoError(t, err)
	assert.Len(t, events, 23, "flushing must not discard events from memory")

	stream, err := store.GetStream(ctx, "history")
	require.NoError(t, err)
	assert.Len(t, stream, 23, "the stream must still replay in full")

	// Sequence numbers are unbroken, which is what a replay walks.
	for i, ev := range stream {
		require.Equal(t, int64(i), ev.Sequence)
	}
}

// TestEventStore_MaxEventsInMemory_MemoryBackendIsANoOp pins that a cap set on the
// memory backend records normally and does not error. There is nowhere to flush to,
// and the default test server runs on this backend — a trigger that failed here
// would break every consumer's test rather than only a persistence one.
func TestEventStore_MaxEventsInMemory_MemoryBackendIsANoOp(t *testing.T) {
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "memory",
		MaxEventsInMemory: 3,
	})

	recordN(t, store, "mem", 10)

	events, err := store.GetEvents(context.Background(), emulator.EventFilter{})
	require.NoError(t, err)
	assert.Len(t, events, 10)
}

// TestEventStore_MaxEventsInMemory_ZeroDisablesTheTrigger is the control for the
// tests above: with no threshold set, nothing is written until Flush is called, so
// the existing explicit-flush behavior is unchanged for every consumer that does
// not set the value.
func TestEventStore_MaxEventsInMemory_ZeroDisablesTheTrigger(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:     true,
		Backend:     "file",
		PersistPath: dir,
	})

	recordN(t, store, "off", 50)
	assert.Equal(t, 0, ndjsonLines(t, dir), "zero must disable the trigger")

	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, 50, ndjsonLines(t, dir))
}

// TestServer_Stop_FlushesTheEventStore covers the threshold's blind spot, which is
// the other half of #599: the trigger only fires on a crossing, so everything
// recorded after the last one lives in memory until something flushes. Nothing did
// — Flush had zero callers — so a server exiting cleanly dropped them, and with the
// default max_in_memory of 1000 a short recorded run persisted nothing at all
// despite a file backend being configured. Server.Stop now flushes.
//
// The threshold is left at zero here deliberately: the events reach disk because of
// the shutdown flush and nothing else.
func TestServer_Stop_FlushesTheEventStore(t *testing.T) {
	dir := t.TempDir()
	cfg := emulator.DefaultConfig()
	cfg.EventStore.Enabled = true
	cfg.EventStore.Backend = "file"
	cfg.EventStore.PersistPath = dir
	cfg.EventStore.MaxInMemory = 0

	registry := emulator.NewPluginRegistry()
	registry.Register(&serverPlugin{
		serviceName: "dynamodb",
		resp: &emulator.AWSResponse{
			StatusCode: 200, Headers: map[string]string{}, Body: []byte(`{}`),
		},
	})
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	srv := emulator.NewServer(*cfg, registry, store,
		emulator.NewMemoryStateManager(), emulator.NewTimeController(time.Now()),
		emulator.NewDefaultLogger(slog.LevelInfo, false))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	baseURL := fmt.Sprintf("http://%s", ln.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx, ln)
	}()

	waitForHealth(t, baseURL)

	const requests = 3
	for range requests {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/",
			strings.NewReader("{}"))
		require.NoError(t, reqErr)
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
		resp, doErr := http.DefaultClient.Do(req)
		require.NoError(t, doErr)
		require.NoError(t, resp.Body.Close())
	}

	require.Equal(t, 0, ndjsonLines(t, dir),
		"with no threshold set nothing should be persisted before shutdown")

	require.NoError(t, srv.Stop(context.Background()))
	<-done

	assert.Equal(t, requests, ndjsonLines(t, dir),
		"shutdown must persist the events recorded since the last flush")
}

// TestServer_Serve_WaitsForTheShutdownFlush is the regression test for what the
// test above could not see. Stop's flush is correct, but on the path a real server
// takes — SIGTERM cancels the context, a goroutine calls Stop — http.Server.Shutdown
// makes Serve return the moment the listener closes, while the flush is still
// running. main then returned and the process exited, so a SIGTERM'd server still
// dropped everything since the last crossing. A live SIGTERM found it; calling Stop
// directly never could.
//
// The assertion is therefore about ordering, not about flushing: by the time Serve
// has returned, the events must already be on disk.
func TestServer_Serve_WaitsForTheShutdownFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := emulator.DefaultConfig()
	cfg.EventStore.Enabled = true
	cfg.EventStore.Backend = "file"
	cfg.EventStore.PersistPath = dir
	cfg.EventStore.MaxInMemory = 0

	registry := emulator.NewPluginRegistry()
	registry.Register(&serverPlugin{
		serviceName: "dynamodb",
		resp: &emulator.AWSResponse{
			StatusCode: 200, Headers: map[string]string{}, Body: []byte(`{}`),
		},
	})
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	srv := emulator.NewServer(*cfg, registry, store,
		emulator.NewMemoryStateManager(), emulator.NewTimeController(time.Now()),
		emulator.NewDefaultLogger(slog.LevelInfo, false))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	baseURL := fmt.Sprintf("http://%s", ln.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()

	waitForHealth(t, baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/",
		strings.NewReader("{}"))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// What a SIGTERM does.
	cancel()
	require.NoError(t, <-served)

	// Read immediately, with no sleep and no wait for the Stop goroutine: Serve
	// returning must already imply the flush completed. Before the awaitStop fix
	// this read won the race and saw an empty directory.
	assert.Positive(t, ndjsonLines(t, dir),
		"Serve must not return until the shutdown flush has persisted its events")
}

// waitForHealth blocks until the server answers, so a test does not race the bind.
func waitForHealth(t *testing.T, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/health") //nolint:noctx // liveness probe, no ctx to thread
		if err == nil {
			require.NoError(t, resp.Body.Close())
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("server at %s did not become healthy", baseURL)
}

// TestEventStore_MaxEventsInMemory_SQLiteFlushesToo covers the other persisting
// backend. It matters on its own because sqlite tracks a single flushCursor rather
// than a per-stream map, and because its flush runs inside a transaction — a
// threshold trigger that worked for NDJSON files but not here would leave the
// documented setting half-true.
func TestEventStore_MaxEventsInMemory_SQLiteFlushesToo(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "sqlite",
		PersistPath:       dir,
		DSN:               "auto.db",
		MaxEventsInMemory: 5,
	})
	ctx := context.Background()

	// No explicit Flush anywhere in this test — only Close, which does not flush.
	recordN(t, store, "sqlite-auto", 20)
	require.NoError(t, store.Close())

	store2 := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true, Backend: "sqlite", PersistPath: dir, DSN: "auto.db",
	})
	require.NoError(t, store2.Load(ctx))
	t.Cleanup(func() { require.NoError(t, store2.Close()) })

	events, err := store2.GetEvents(ctx, emulator.EventFilter{Service: "s3"})
	require.NoError(t, err)
	assert.Len(t, events, 20,
		"the automatic flush must reach sqlite, and four crossings must not duplicate")
}

// TestEventStore_MaxEventsInMemory_ConcurrentRecordersDoNotRace is why the file and
// sqlite backends gained their own mutexes. Their flush methods advance a cursor
// while the EventStore holds only its *read* lock, which permits concurrent
// holders — so the struct comment claiming that lock serialized them was wrong. It
// was harmless while Flush had no callers; the automatic trigger made it live.
//
// Worth running under -race specifically, which is what make test does. Without the
// backend mutex the detector reports a write/write on flushCursors here.
func TestEventStore_MaxEventsInMemory_ConcurrentRecordersDoNotRace(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "file",
		PersistPath:       dir,
		MaxEventsInMemory: 2,
	})
	ctx := context.Background()

	const writers, each = 8, 25

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// A stream per writer, so the flushes contend over several cursor
			// entries rather than one — a single key would still race, but the map
			// itself is what the detector reports on.
			streamID := fmt.Sprintf("racer-%d", w)
			for range each {
				require.NoError(t, store.RecordEvent(ctx, &emulator.Event{
					StreamID: streamID, Service: "s3", Operation: "Put", Timestamp: time.Now(),
				}))
			}
		}(w)
	}
	wg.Wait()

	// Every event is in memory regardless of how the flushes interleaved.
	events, err := store.GetEvents(ctx, emulator.EventFilter{})
	require.NoError(t, err)
	assert.Len(t, events, writers*each)

	// And a final flush leaves exactly one line per event: no crossing wrote a
	// range another had already written.
	require.NoError(t, store.Flush(ctx))
	assert.Equal(t, writers*each, ndjsonLines(t, dir),
		"interleaved flushes must not duplicate events")
}

// TestEventStore_ExplicitFlushConcurrentWithRecording is the test that actually
// reaches the backend cursor mutex, and it exists because the concurrency test above
// does not: the automatic path is serialized by the store's own flushMu, so removing
// the backend lock leaves that test passing. Flush is exported, though, and a
// consumer calling it while requests are still being recorded takes only the store's
// *read* lock — which permits concurrent holders. That is the live race (#599), and
// under -race this reproduces it in a few hundred iterations.
func TestEventStore_ExplicitFlushConcurrentWithRecording(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "file",
		PersistPath:       dir,
		MaxEventsInMemory: 2,
	})
	ctx := context.Background()

	const events = 200

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range events {
			require.NoError(t, store.RecordEvent(ctx, &emulator.Event{
				StreamID: "concurrent", Service: "s3", Operation: "Put", Timestamp: time.Now(),
			}))
		}
	}()
	go func() {
		defer wg.Done()
		for range events {
			require.NoError(t, store.Flush(ctx))
		}
	}()
	wg.Wait()

	require.NoError(t, store.Flush(ctx))
	assert.Equal(t, events, ndjsonLines(t, dir),
		"a flush racing a recorder must still write each event exactly once")
}

// TestEventStore_SQLite_ExplicitFlushConcurrentWithRecording is the sqlite half of
// the test above. It needs its own case because the backends guard different things:
// the file backend a per-stream map, sqlite a single int64 cursor. Both were written
// under the store's read lock alone, and a test covering one leaves the other's lock
// removable without failing anything.
func TestEventStore_SQLite_ExplicitFlushConcurrentWithRecording(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:           true,
		Backend:           "sqlite",
		PersistPath:       dir,
		DSN:               "concurrent.db",
		MaxEventsInMemory: 2,
	})
	ctx := context.Background()
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	const events = 200

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range events {
			require.NoError(t, store.RecordEvent(ctx, &emulator.Event{
				StreamID: "concurrent", Service: "s3", Operation: "Put", Timestamp: time.Now(),
			}))
		}
	}()
	go func() {
		defer wg.Done()
		for range events {
			require.NoError(t, store.Flush(ctx))
		}
	}()
	wg.Wait()

	require.NoError(t, store.Flush(ctx))

	store2 := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true, Backend: "sqlite", PersistPath: dir, DSN: "concurrent.db",
	})
	require.NoError(t, store2.Load(ctx))
	t.Cleanup(func() { require.NoError(t, store2.Close()) })

	persisted, err := store2.GetEvents(ctx, emulator.EventFilter{Service: "s3"})
	require.NoError(t, err)
	assert.Len(t, persisted, events,
		"a flush racing a recorder must persist each event exactly once")
}
