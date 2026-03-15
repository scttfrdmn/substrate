package substrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate"
)

// testLogger is a no-op Logger for use in tests.
type testLogger struct{}

func (l *testLogger) Debug(_ string, _ ...any) {}
func (l *testLogger) Info(_ string, _ ...any)  {}
func (l *testLogger) Warn(_ string, _ ...any)  {}
func (l *testLogger) Error(_ string, _ ...any) {}

func newTestEngine(t *testing.T) *substrate.ReplayEngine {
	t.Helper()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()
	return substrate.NewReplayEngine(store, nil, tc, registry, substrate.ReplayConfig{}, &testLogger{})
}

func TestReplayEngine_ReplayWithStateManager(t *testing.T) {
	// Use a SnapshotableStateManager so resetState exercises the Reset() path.
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	const streamID = "state-stream"
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  streamID,
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Now(),
	}))

	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, state, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{}, &testLogger{})

	results, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)
	assert.Equal(t, 1, results.TotalEvents)
}

func TestReplayEngine_RandFloat64(t *testing.T) {
	engine := newTestEngine(t)
	for range 10 {
		v := engine.RandFloat64()
		assert.GreaterOrEqual(t, v, 0.0)
		assert.Less(t, v, 1.0)
	}
}

func TestReplayEngine_RandInt64(t *testing.T) {
	engine := newTestEngine(t)
	for range 10 {
		v := engine.RandInt64()
		assert.GreaterOrEqual(t, v, int64(0))
	}
}

func TestReplayEngine_StartStopRecording(t *testing.T) {
	engine := newTestEngine(t)
	ctx := context.Background()

	session, err := engine.StartRecording(ctx, "my-test")
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.NotEmpty(t, session.StreamID)
	assert.False(t, session.StartTime.IsZero())

	count, err := engine.StopRecording(ctx, session)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestReplayEngine_Replay_EmptyStream(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.Replay(context.Background(), "no-such-stream")
	assert.Error(t, err)
}

func TestReplayEngine_Replay_NoRequest(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	// Event without a request body should be skipped, not crash.
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "s",
		Service:   "s3",
		Operation: "List",
		Timestamp: time.Now(),
		// Request is nil intentionally.
	}))

	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()
	engine := substrate.NewReplayEngine(store, nil, tc, registry, substrate.ReplayConfig{}, &testLogger{})

	results, err := engine.Replay(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, 1, results.TotalEvents)
	assert.Equal(t, 1, results.SkippedEvents)
	assert.Equal(t, 0, results.FailedEvents)
}

func TestReplayEngine_Replay_UnknownService(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "s",
		Service:   "lambda",
		Operation: "Invoke",
		Timestamp: time.Now(),
		Request:   &substrate.AWSRequest{Service: "lambda", Operation: "Invoke"},
	}))

	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry() // empty — no lambda plugin
	engine := substrate.NewReplayEngine(store, nil, tc, registry,
		substrate.ReplayConfig{StopOnError: false}, &testLogger{})

	results, err := engine.Replay(ctx, "s")
	require.NoError(t, err)
	// Should record the failure in Differences, not panic.
	assert.Equal(t, 1, results.FailedEvents)
}

func TestReplayEngine_SetClearBreakpoint(t *testing.T) {
	engine := newTestEngine(t)
	ctx := context.Background()

	session, err := engine.StartRecording(ctx, "bp-test")
	require.NoError(t, err)

	// SetBreakpoint and ClearBreakpoint should not panic even with no active replay.
	engine.SetBreakpoint(5)
	engine.ClearBreakpoint(5)

	_, err = engine.StopRecording(ctx, session)
	require.NoError(t, err)
}

func TestReplayEngine_PauseResume(t *testing.T) {
	engine := newTestEngine(t)

	// Should not panic when no replay is active.
	engine.Pause()
	engine.Resume()
}

func TestReplayEngine_StepForward_NoReplay(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.StepForward(context.Background())
	assert.Error(t, err)
}

func TestReplayEngine_StepBackward_NoReplay(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.StepBackward(context.Background())
	assert.Error(t, err)
}

func TestReplayEngine_JumpToEvent_NoReplay(t *testing.T) {
	engine := newTestEngine(t)
	err := engine.JumpToEvent(context.Background(), 0)
	assert.Error(t, err)
}

func TestReplayEngine_GetCurrentEvent_NoReplay(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.GetCurrentEvent()
	assert.Error(t, err)
}

func TestTimeController(t *testing.T) {
	base := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(base)

	assert.Equal(t, base, tc.Now())

	later := base.Add(24 * time.Hour)
	tc.SetTime(later)
	assert.Equal(t, later, tc.Now())

	tc.SetScale(86400.0)
	// Scale is stored; precise time-advancement is implemented later.
}

func TestPluginRegistry_RouteRequest_NoPlugin(t *testing.T) {
	registry := substrate.NewPluginRegistry()
	_, err := registry.RouteRequest(&substrate.RequestContext{}, &substrate.AWSRequest{Service: "s3"})
	require.Error(t, err)

	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "ServiceNotAvailable", awsErr.Code)
	assert.Equal(t, 501, awsErr.HTTPStatus)
}

func TestAWSError_Error(t *testing.T) {
	err := &substrate.AWSError{Code: "NoSuchBucket", Message: "the bucket does not exist"}
	assert.Equal(t, "NoSuchBucket: the bucket does not exist", err.Error())
}

func TestReplayEngine_UseSnapshots_WithSnapshot(t *testing.T) {
	// UseSnapshots=true with an existing snapshot exercises loadFromSnapshot → restoreState.
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	const streamID = "snap-stream"

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  streamID,
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Now(),
	}))

	state := substrate.NewMemoryStateManager()
	_, err := store.CreateSnapshot(ctx, streamID, state)
	require.NoError(t, err)

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, state, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{UseSnapshots: true}, &testLogger{})

	results, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)
	assert.Equal(t, 1, results.TotalEvents)
}

func TestReplayEngine_UseSnapshots_MissingSnapshot(t *testing.T) {
	// UseSnapshots=true but no snapshot: should warn and continue (not fail).
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	const streamID = "no-snap-stream"

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  streamID,
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Now(),
	}))

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, nil, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{UseSnapshots: true}, &testLogger{})

	results, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)
	assert.Equal(t, 1, results.TotalEvents)
}

func TestReplayEngine_ValidateState_HashMismatch(t *testing.T) {
	// ValidateState=true with non-empty StateHashBefore exercises computeStateHash.
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	const streamID = "validate-stream"

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:        streamID,
		Service:         "s3",
		Operation:       "PutObject",
		Timestamp:       time.Now(),
		StateHashBefore: "expected-before",
		StateHashAfter:  "expected-after",
		Request:         &substrate.AWSRequest{Service: "s3", Operation: "PutObject"},
	}))

	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, state, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{ValidateState: true, StopOnError: false}, &testLogger{})

	results, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)
	// Hash mismatch differences are recorded; StateValid set to false.
	assert.False(t, results.StateValid)
}

func TestReplayEngine_RandWithSeed(t *testing.T) {
	// After Replay() with RandomSeed set, r.rng is initialized;
	// RandFloat64/RandInt64 use the deterministic seeded path.
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	const streamID = "seed-stream"

	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  streamID,
		Service:   "s3",
		Operation: "Get",
		Timestamp: time.Now(),
	}))

	tc := substrate.NewTimeController(time.Now())
	engine := substrate.NewReplayEngine(store, nil, tc, substrate.NewPluginRegistry(),
		substrate.ReplayConfig{RandomSeed: 42}, &testLogger{})

	_, err := engine.Replay(ctx, streamID)
	require.NoError(t, err)

	v := engine.RandFloat64()
	assert.GreaterOrEqual(t, v, 0.0)
	assert.Less(t, v, 1.0)

	n := engine.RandInt64()
	assert.GreaterOrEqual(t, n, int64(0))
}
