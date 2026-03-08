// Package substrate_test contains additional tests for coverage of helper
// functions, snapshot operations, and replay navigation.
package substrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate"
)

// memStateManager is a simple in-memory StateManager used in tests.
type memStateManager struct {
	data map[string]map[string][]byte
}

func newMemStateManager() *memStateManager {
	return &memStateManager{data: make(map[string]map[string][]byte)}
}

func (m *memStateManager) Get(_ context.Context, namespace, key string) ([]byte, error) {
	if ns, ok := m.data[namespace]; ok {
		return ns[key], nil
	}
	return nil, nil
}

func (m *memStateManager) Put(_ context.Context, namespace, key string, value []byte) error {
	if _, ok := m.data[namespace]; !ok {
		m.data[namespace] = make(map[string][]byte)
	}
	m.data[namespace][key] = value
	return nil
}

func (m *memStateManager) Delete(_ context.Context, namespace, key string) error {
	if ns, ok := m.data[namespace]; ok {
		delete(ns, key)
	}
	return nil
}

func (m *memStateManager) List(_ context.Context, namespace, prefix string) ([]string, error) {
	ns, ok := m.data[namespace]
	if !ok {
		return nil, nil
	}
	keys := make([]string, 0, len(ns))
	for k := range ns {
		if prefix == "" || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// mockPlugin is a minimal Plugin for registry tests.
type mockPlugin struct{ name string }

func (p *mockPlugin) Name() string                                                 { return p.name }
func (p *mockPlugin) Initialize(_ context.Context, _ substrate.PluginConfig) error { return nil }
func (p *mockPlugin) HandleRequest(_ *substrate.RequestContext, _ *substrate.AWSRequest) (*substrate.AWSResponse, error) {
	return &substrate.AWSResponse{StatusCode: 200}, nil
}
func (p *mockPlugin) Shutdown(_ context.Context) error { return nil }

// ---------------------------------------------------------------------------
// EventStore helper and snapshot tests
// ---------------------------------------------------------------------------

func TestEventStore_RecordRequest(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory", IncludeBodies: true})
	ctx := context.Background()

	reqCtx := &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		Principal: &substrate.Principal{ARN: "arn:aws:iam::123:user/alice"},
		Metadata:  map[string]interface{}{"stream_id": "stream-rr"},
	}
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	resp := &substrate.AWSResponse{StatusCode: 200}

	err := store.RecordRequest(ctx, reqCtx, req, resp, 5*time.Millisecond, 0.005, nil)
	require.NoError(t, err)

	events, err := store.GetStream(ctx, "stream-rr")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "s3", events[0].Service)
	assert.Equal(t, "arn:aws:iam::123:user/alice", events[0].Principal)
	assert.NotNil(t, events[0].Request)
}

func TestEventStore_RecordRequest_WithError(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	reqCtx := &substrate.RequestContext{}
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}

	// Plain error.
	plainErr := &substrate.AWSError{Code: "AccessDenied", Message: "denied"}
	err := store.RecordRequest(ctx, reqCtx, req, nil, 0, 0, plainErr)
	require.NoError(t, err)

	events, err := store.GetEvents(ctx, substrate.EventFilter{Service: "s3"})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "AccessDenied: denied", events[0].Error)
	assert.Equal(t, "AccessDenied", events[0].ErrorCode)
}

func TestEventStore_RecordRequest_NoBodies(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory", IncludeBodies: false})
	ctx := context.Background()

	req := &substrate.AWSRequest{Service: "s3", Operation: "GetObject"}
	resp := &substrate.AWSResponse{StatusCode: 200}
	err := store.RecordRequest(ctx, &substrate.RequestContext{}, req, resp, 0, 0, nil)
	require.NoError(t, err)

	events, err := store.GetEvents(ctx, substrate.EventFilter{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Nil(t, events[0].Request, "bodies should not be stored when IncludeBodies is false")
}

func TestEventStore_Snapshot_Roundtrip(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for range 3 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID: "snap-stream", Service: "s3", Operation: "Put", Timestamp: time.Now(),
		}))
	}

	sm := newMemStateManager()
	snap, err := store.CreateSnapshot(ctx, "snap-stream", sm)
	require.NoError(t, err)
	require.NotEmpty(t, snap.ID)
	assert.Equal(t, "snap-stream", snap.StreamID)

	// GetSnapshot by ID.
	got, err := store.GetSnapshot(ctx, snap.ID)
	require.NoError(t, err)
	assert.Equal(t, snap.ID, got.ID)

	// GetLatestSnapshot.
	latest, err := store.GetLatestSnapshot(ctx, "snap-stream")
	require.NoError(t, err)
	assert.Equal(t, snap.ID, latest.ID)
}

func TestEventStore_GetSnapshot_NotFound(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	_, err := store.GetSnapshot(context.Background(), "ghost")
	assert.Error(t, err)
}

func TestEventStore_GetLatestSnapshot_NoSnapshot(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	_, err := store.GetLatestSnapshot(context.Background(), "no-snaps")
	assert.Error(t, err)
}

func TestEventStore_Flush_NonMemory(t *testing.T) {
	// Non-memory backend — flush is a no-error stub.
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "sqlite"})
	assert.NoError(t, store.Flush(context.Background()))
}

func TestEventStore_Load_NonMemory(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "sqlite"})
	assert.NoError(t, store.Load(context.Background()))
}

func TestEventFilter_HasError(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	hasErr := true
	noErr := false

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{Service: "s3", Timestamp: time.Now()}))
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{Service: "s3", Error: "boom", Timestamp: time.Now()}))

	errOnly, err := store.GetEvents(ctx, substrate.EventFilter{HasError: &hasErr})
	require.NoError(t, err)
	assert.Len(t, errOnly, 1)

	noErrOnly, err := store.GetEvents(ctx, substrate.EventFilter{HasError: &noErr})
	require.NoError(t, err)
	assert.Len(t, noErrOnly, 1)
}

func TestEventFilter_TimeRange(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			Service:   "s3",
			Timestamp: base.Add(time.Duration(i) * time.Hour),
		}))
	}

	events, err := store.GetEvents(ctx, substrate.EventFilter{
		StartTime: base.Add(1 * time.Hour),
		EndTime:   base.Add(3 * time.Hour),
	})
	require.NoError(t, err)
	assert.Len(t, events, 3) // hours 1, 2, 3
}

func TestEventFilter_SequenceRange(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for range 5 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			Service: "s3", Timestamp: time.Now(),
		}))
	}

	events, err := store.GetEvents(ctx, substrate.EventFilter{MinSequence: 1, MaxSequence: 3})
	require.NoError(t, err)
	assert.Len(t, events, 3) // sequences 1, 2, 3
}

// ---------------------------------------------------------------------------
// PluginRegistry tests
// ---------------------------------------------------------------------------

func TestPluginRegistry_Register_And_Route(t *testing.T) {
	registry := substrate.NewPluginRegistry()
	registry.Register(&mockPlugin{name: "s3"})

	resp, err := registry.RouteRequest(
		&substrate.RequestContext{},
		&substrate.AWSRequest{Service: "s3", Operation: "ListBuckets"},
	)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// ReplayEngine navigation tests
// ---------------------------------------------------------------------------

func newEngineWithEvents(t *testing.T, n int) (*substrate.ReplayEngine, string) {
	t.Helper()

	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	const streamID = "nav-stream"
	for i := range n {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  streamID,
			Service:   "s3",
			Operation: "Put",
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			// No Request — events will be skipped during replay.
		}))
	}

	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()
	engine := substrate.NewReplayEngine(store, nil, tc, registry, substrate.ReplayConfig{}, &testLogger{})
	return engine, streamID
}

func TestReplayEngine_StepForward_AfterReplay(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 3)
	ctx := context.Background()

	results, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)
	assert.Equal(t, 3, results.TotalEvents)

	// Replay ran to completion; jump back to position 0 and step forward.
	require.NoError(t, engine.JumpToEvent(ctx, 0))

	event, err := engine.StepForward(ctx)
	require.NoError(t, err)
	require.NotNil(t, event)
}

func TestReplayEngine_StepForward_AtEnd(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 1)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	// Position is at the end after Replay.
	_, err = engine.StepForward(ctx)
	assert.Error(t, err)
}

func TestReplayEngine_JumpToEvent_Valid(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 5)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	require.NoError(t, engine.JumpToEvent(ctx, 2))

	event, err := engine.GetCurrentEvent()
	require.NoError(t, err)
	assert.Equal(t, int64(2), event.Sequence)
}

func TestReplayEngine_JumpToEvent_OutOfRange(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 3)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	err = engine.JumpToEvent(ctx, 99)
	assert.Error(t, err)

	err = engine.JumpToEvent(ctx, -1)
	assert.Error(t, err)
}

func TestReplayEngine_StepBackward_NoSnapshot(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 3)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	require.NoError(t, engine.JumpToEvent(ctx, 2))

	// No state snapshots populated yet — must return an error.
	_, err = engine.StepBackward(ctx)
	assert.Error(t, err)
}

func TestReplayEngine_StepBackward_AtBeginning(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 3)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	require.NoError(t, engine.JumpToEvent(ctx, 0))

	_, err = engine.StepBackward(ctx)
	assert.Error(t, err)
}

func TestReplayEngine_SetClearBreakpoint_ActiveReplay(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 5)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	// With an active replay, these should not panic.
	engine.SetBreakpoint(2)
	engine.ClearBreakpoint(2)
}

func TestReplayEngine_PauseResume_ActiveReplay(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 3)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	engine.Pause()
	engine.Resume()
}

func TestReplayEngine_GetCurrentEvent_AtEnd(t *testing.T) {
	engine, streamID := newEngineWithEvents(t, 2)
	ctx := context.Background()

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	// Position is past end after completion.
	_, err = engine.GetCurrentEvent()
	assert.Error(t, err)
}

func TestReplayEngine_InspectState(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	sm := newMemStateManager()

	require.NoError(t, sm.Put(context.Background(), "iam", "user:alice", []byte(`{"name":"alice"}`)))

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, sm, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{}, &testLogger{})

	state, err := engine.InspectState(context.Background(), "iam")
	require.NoError(t, err)
	assert.Contains(t, state, "user:alice")
}

func TestReplayEngine_StopRecording_WithEvents(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, nil, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{}, &testLogger{})

	session, err := engine.StartRecording(ctx, "with-events")
	require.NoError(t, err)

	// Record events under the session's stream ID.
	for range 4 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  session.StreamID,
			Service:   "s3",
			Operation: "Put",
			Timestamp: time.Now(),
		}))
	}

	count, err := engine.StopRecording(ctx, session)
	require.NoError(t, err)
	assert.Equal(t, 4, count)
}

func TestReplayEngine_Replay_WithRequestAndUnknownService(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "s",
		Service:   "dynamodb",
		Operation: "GetItem",
		Timestamp: time.Now(),
		Request:   &substrate.AWSRequest{Service: "dynamodb", Operation: "GetItem"},
		Response:  &substrate.AWSResponse{StatusCode: 200},
	}))

	// Original succeeded; replay will fail (no dynamodb plugin).
	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()
	engine := substrate.NewReplayEngine(store, nil, tc, registry,
		substrate.ReplayConfig{StopOnError: true}, &testLogger{})

	results, err := engine.Replay(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, 1, results.FailedEvents)
	assert.NotEmpty(t, results.Differences)
}

func TestReplayEngine_Replay_StopOnError(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	// Two events that will both fail — StopOnError should halt after first.
	for range 2 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  "stop-stream",
			Service:   "sqs",
			Operation: "SendMessage",
			Timestamp: time.Now(),
			Request:   &substrate.AWSRequest{Service: "sqs", Operation: "SendMessage"},
		}))
	}

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, nil, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{StopOnError: true}, &testLogger{})

	results, err := engine.Replay(ctx, "stop-stream")
	require.NoError(t, err)
	assert.Equal(t, 1, results.FailedEvents)
}

func TestReplayEngine_Replay_DifferentStatusCode(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	registry := substrate.NewPluginRegistry()
	registry.Register(&mockPlugin{name: "s3"}) // returns 200

	// Record an event that originally returned 404.
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "diff-stream",
		Service:   "s3",
		Operation: "GetObject",
		Timestamp: time.Now(),
		Request:   &substrate.AWSRequest{Service: "s3", Operation: "GetObject"},
		Response:  &substrate.AWSResponse{StatusCode: 404}, // different from what plugin returns
	}))

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, nil, tc, registry,
		substrate.ReplayConfig{}, &testLogger{})

	results, err := engine.Replay(ctx, "diff-stream")
	require.NoError(t, err)
	// Status code difference should be recorded.
	assert.NotEmpty(t, results.Differences)
	assert.Equal(t, "status_code", results.Differences[0].Field)
}

func TestReplayEngine_Replay_MatchingError(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	// Original event also failed — same error code means no difference recorded.
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "err-match",
		Service:   "lambda",
		Operation: "Invoke",
		Timestamp: time.Now(),
		Request:   &substrate.AWSRequest{Service: "lambda", Operation: "Invoke"},
		Error:     "ServiceNotAvailable: service not emulated: lambda",
	}))

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, nil, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{}, &testLogger{})

	results, err := engine.Replay(ctx, "err-match")
	require.NoError(t, err)
	// Same error = expected, no difference added.
	assert.Empty(t, results.Differences)
}
