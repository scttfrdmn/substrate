package emulator_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// gatedServer is an in-process server with a body cap and a quota controller.
type gatedServer struct {
	srv   *httptest.Server
	store *emulator.EventStore
	state *emulator.MemoryStateManager
	cfg   *emulator.Config
}

// newGatedServer starts a server whose event store caps bodies at capBytes and whose
// quota allows burst S3 requests and refills none.
func newGatedServer(t *testing.T, capBytes int, burst float64) gatedServer {
	t.Helper()
	cfg := emulator.DefaultConfig()
	cfg.EventStore.MaxBodyBytes = capBytes
	cfg.Quotas = emulator.QuotaCfg{Enabled: true, Rules: map[string]emulator.RateRuleCfg{"s3": {Rate: 0, Burst: burst}}}
	cfg.Log.Level = "error"
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(replayClockStart)
	tc.Freeze()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig(), emulator.WithTimeController(tc))
	require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc, logger, store, cfg))
	quota := emulator.NewQuotaController(cfg.Quotas.ToQuotaConfig(), tc)
	srv := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger,
		emulator.ServerOptions{Quota: quota}))
	t.Cleanup(srv.Close)
	return gatedServer{srv: srv, store: store, state: state, cfg: cfg}
}

// do sends one S3 request and returns its status.
func (g gatedServer) do(t *testing.T, method, path string, body []byte) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, g.srv.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

// TestReplay_ASkippedEventStillSpendsItsQuotaToken pins that an event a replay skips for
// its over-cap body is charged to the quota it spent live. With a burst of three, the
// CreateBucket and the over-cap PutObject take two tokens, a small PutObject the third,
// and the next PutObject was throttled; without the charge the replay would allow it.
func TestReplay_ASkippedEventStillSpendsItsQuotaToken(t *testing.T) {
	g := newGatedServer(t, 64, 3)
	require.Equal(t, http.StatusOK, g.do(t, http.MethodPut, "/charged", nil))
	require.Equal(t, http.StatusOK, g.do(t, http.MethodPut, "/charged/big", bytes.Repeat([]byte("x"), 200)))
	require.Equal(t, http.StatusOK, g.do(t, http.MethodPut, "/charged/small", []byte("tiny")))
	require.Equal(t, http.StatusServiceUnavailable, g.do(t, http.MethodPut, "/charged/throttled", []byte("tiny")))

	sb, err := emulator.NewReplaySandbox(t.Context(), g.cfg, g.store, emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	results, err := sb.Engine().Replay(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	assert.Equal(t, map[emulator.ReplaySkipReason]int{emulator.SkipBodyOverCap: 1}, results.SkipReasons)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	assert.Equal(t, 1, results.FailedEvents, "the throttle replayed as a throttle")
}

// TestTimeTravel_UseSnapshotsJumpsBackToTheStoredSnapshot pins that a backward jump under
// use_snapshots resets before it restores the stored snapshot, so what plugins keep on
// themselves is rewound too: the S3 payload at a position is the one written by then.
func TestTimeTravel_UseSnapshotsJumpsBackToTheStoredSnapshot(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, ts, "stored")
	replayPutObject(t, ts, "stored", "k", "v1")
	replayPutObject(t, ts, "stored", "k", "v2")
	_, err := ts.Store().CreateSnapshot(t.Context(), emulator.DefaultStreamID, emulator.NewMemoryStateManager())
	require.NoError(t, err)

	// A quota of exactly the stream's three requests: a jump that did not reset would
	// re-run them against spent buckets and be throttled.
	cfg := emulator.DefaultConfig()
	cfg.Quotas = emulator.QuotaCfg{Enabled: true, Rules: map[string]emulator.RateRuleCfg{"s3": {Rate: 0, Burst: 3}}}
	newSandbox := func() *emulator.ReplaySandbox {
		sb, err := emulator.NewReplaySandbox(t.Context(), cfg, ts.Store(), emulator.NewDefaultLogger(slog.LevelError, false),
			emulator.WithSandboxReplayConfig(emulator.ReplayConfig{UseSnapshots: true}))
		require.NoError(t, err)
		return sb
	}
	last := lastDefaultSequence(t, ts)

	moved := newSandbox()
	_, err = moved.StateAt(t.Context(), emulator.DefaultStreamID, last+1)
	require.NoError(t, err)
	back, err := moved.StateAt(t.Context(), emulator.DefaultStreamID, last)
	require.NoError(t, err)
	fresh, err := newSandbox().StateAt(t.Context(), emulator.DefaultStreamID, last)
	require.NoError(t, err)

	assert.Empty(t, fresh.Differences)
	assert.Empty(t, back.Differences)
	assert.JSONEq(t, string(fresh.State), string(back.State))
	assert.Equal(t, "v1", s3Object(t, moved, "stored", "k"), "the payload was rewound with the state")
}

// brokenSnapshotState is a state manager whose snapshot is not a state document.
type brokenSnapshotState struct{ *emulator.MemoryStateManager }

// Snapshot returns bytes no state manager can restore.
func (brokenSnapshotState) Snapshot(context.Context) ([]byte, error) { return []byte("not json"), nil }

// TestTimeTravel_AStoredSnapshotThatCannotBeRestoredIsAnError pins that a replay under
// use_snapshots refuses to start from a stored snapshot it cannot restore, and that a
// failed start leaves no active replay.
func TestTimeTravel_AStoredSnapshotThatCannotBeRestoredIsAnError(t *testing.T) {
	ctx := t.Context()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true})
	require.NoError(t, store.RecordEvent(ctx, &emulator.Event{StreamID: "broken", Service: "s3", Operation: "Put"}))
	_, err := store.CreateSnapshot(ctx, "broken", brokenSnapshotState{emulator.NewMemoryStateManager()})
	require.NoError(t, err)

	engine := emulator.NewReplayEngine(store, emulator.NewMemoryStateManager(), emulator.NewTimeController(replayClockStart),
		emulator.NewPluginRegistry(), emulator.ReplayConfig{UseSnapshots: true}, &testLogger{})
	_, err = engine.Replay(ctx, "broken")
	require.ErrorContains(t, err, "load stored snapshot")
	_, err = engine.GetCurrentEvent()
	assert.ErrorContains(t, err, "no active replay", "a failed start publishes no replay")
}

// TestTimeTravel_AStackDispatchIsNotFaultGated pins that a replayed deployer dispatch
// passes no fault gate, because the live deployer consults none.
func TestTimeTravel_AStackDispatchIsNotFaultGated(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	cfnCreateStack(t, ts, "unfaulted", cfnTemplateOneBucket)

	fault := emulator.NewFaultController(emulator.FaultConfig{Enabled: true, Rules: []emulator.FaultRule{
		{Service: "s3", FaultType: "error", ErrorCode: "InternalError", HTTPStatus: 500, Probability: 1, Times: -1},
	}}, 1)
	sb, err := emulator.NewReplaySandbox(t.Context(), nil, ts.Store(), emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.WithSandboxFaults(fault))
	require.NoError(t, err)
	at, err := sb.StateAt(t.Context(), "unfaulted", 1<<40)
	require.NoError(t, err)
	assert.Empty(t, at.Differences)
	assert.Zero(t, at.FailedEvents)
	assert.Contains(t, string(at.State), "from-stack")
}

// TestTimeTravel_ASandboxSeesTrafficAfterItLoaded pins that a sandbox held across new
// requests answers a later sequence from them rather than from the list it loaded.
func TestTimeTravel_ASandboxSeesTrafficAfterItLoaded(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	for i := range 5 {
		replayPutBucket(t, ts, "early-"+string(rune('a'+i)))
	}
	sb := ts.ReplaySandbox(t)
	at, err := sb.StateAt(t.Context(), emulator.DefaultStreamID, 5)
	require.NoError(t, err)
	require.Equal(t, 5, at.EventsApplied)

	for i := range 10 {
		replayPutBucket(t, ts, "late-"+string(rune('a'+i)))
	}
	at, err = sb.StateAt(t.Context(), emulator.DefaultStreamID, 15)
	require.NoError(t, err)
	assert.Equal(t, 15, at.EventsApplied)
	assert.JSONEq(t, liveSnapshot(t, ts), string(at.State))
}
