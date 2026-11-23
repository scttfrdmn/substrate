package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestTimeTravel_StateAtCountsFailuresAndSkips pins the counts a partial replay reports:
// a recorded error that replays as the same error is a failure and not a difference,
// and an event with no request is a skip with its reason.
func TestTimeTravel_StateAtCountsFailuresAndSkips(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)

	status, body := replayDo(t, ts, http.MethodPost, "/", "dynamodb.us-east-1.amazonaws.com",
		[]byte(`{"TableName":"missing"}`), map[string]string{
			"X-Amz-Target": "DynamoDB_20120810.DescribeTable",
			"Content-Type": "application/x-amz-json-1.0",
		})
	require.Equal(t, http.StatusBadRequest, status, string(body))
	require.NoError(t, ts.Store().RecordEvent(t.Context(), &emulator.Event{
		StreamID: emulator.DefaultStreamID, Service: "s3", Operation: "ListBuckets",
	}))
	replayPutBucket(t, ts, "after")

	status, at, raw := getStateAt(t, ts, 1<<40, "")
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, 3, at.EventsApplied)
	assert.Equal(t, 1, at.FailedEvents, "the recorded ResourceNotFoundException replayed as itself")
	assert.Equal(t, map[emulator.ReplaySkipReason]int{emulator.SkipNoRequest: 1}, at.Skipped)
	assert.Empty(t, at.Differences)
}

// TestTimeTravel_StateDiffRefusals pins the diff endpoint's errors.
func TestTimeTravel_StateDiffRefusals(t *testing.T) {
	ts := emulator.StartTestServer(t)
	replayPutBucket(t, ts, "diff-refusals")
	for query, want := range map[string]int{
		"from=x&to=1":              http.StatusBadRequest,
		"from=0&to=x":              http.StatusBadRequest,
		"from=0&to=1&stream=nope":  http.StatusNotFound,
		"from=0&to=1&stream=nope2": http.StatusNotFound,
	} {
		resp, err := http.Get(ts.URL + "/v1/debug/state/diff?" + query) //nolint:noctx
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, want, resp.StatusCode, query)
	}
	resp, err := http.Get(ts.URL + "/v1/debug/events/x/state") //nolint:noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestTimeTravel_NoSnapshotWhereStateCannotBeCaptured pins the two cases in which a
// snapshot cannot stand for the whole emulator, and that a jump is still correct there:
// an S3 filesystem the caller injected, and armed fault rules.
func TestTimeTravel_NoSnapshotWhereStateCannotBeCaptured(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, ts, "uncaptured")
	replayPutObject(t, ts, "uncaptured", "k", "v1")
	replayPutObject(t, ts, "uncaptured", "k", "v2")
	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)

	t.Run("injected filesystem", func(t *testing.T) {
		registry, _, state, tc, logger := newClientTestDeps(t)
		store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, SnapshotInterval: 1})
		for _, ev := range events {
			cp := *ev
			require.NoError(t, store.RecordEvent(t.Context(), &cp))
		}
		engine := emulator.NewReplayEngine(store, state, tc, registry, emulator.ReplayConfig{}, logger)
		_, err := engine.Replay(t.Context(), emulator.DefaultStreamID)
		require.NoError(t, err)
		require.NoError(t, engine.JumpToEvent(t.Context(), 2))
		require.NoError(t, engine.JumpToEvent(t.Context(), 1))
		assert.Empty(t, emulator.ReplaySnapshotPositionsForTest(engine))
	})

	t.Run("armed fault rules", func(t *testing.T) {
		fault := emulator.NewFaultController(emulator.FaultConfig{Enabled: true, Rules: []emulator.FaultRule{
			{Service: "dynamodb", Operation: "NeverCalled"},
		}}, 1)
		store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, SnapshotInterval: 1})
		for _, ev := range events {
			cp := *ev
			require.NoError(t, store.RecordEvent(t.Context(), &cp))
		}
		sb, err := emulator.NewReplaySandbox(t.Context(), nil, store, emulator.NewDefaultLogger(slog.LevelError, false),
			emulator.WithSandboxFaults(fault))
		require.NoError(t, err)
		_, err = sb.Engine().Replay(t.Context(), emulator.DefaultStreamID)
		require.NoError(t, err)
		require.NoError(t, sb.Engine().JumpToEvent(t.Context(), 2))
		require.NoError(t, sb.Engine().JumpToEvent(t.Context(), 1))
		require.NoError(t, sb.Engine().JumpToEvent(t.Context(), 3))
		assert.Empty(t, emulator.ReplaySnapshotPositionsForTest(sb.Engine()))
		assert.Equal(t, "v2", s3Object(t, sb, "uncaptured", "k"))
	})
}

// TestTimeTravel_ASnapshotKeepsAnEventSourceMappingPolling pins that a snapshot restores
// the set of mappings Lambda polls. A mapping created before the snapshot must still
// consume the messages sent after it once the replay moves forward again.
func TestTimeTravel_ASnapshotKeepsAnEventSourceMappingPolling(t *testing.T) {
	f := newESMClockFixture(t)
	esmSendSQSMessage(t, f.ts, f.queueURL, "three")
	esmSendSQSMessage(t, f.ts, f.queueURL, "four")
	f.ts.AdvanceTime(time.Second)
	require.Zero(t, esmPeekCount(t, f.ts, f.queueURL), "the live mapping consumed every message")

	events, err := f.ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	mapping := -1
	for i, ev := range events {
		if ev.Operation == "CreateEventSourceMapping" {
			mapping = i
		}
	}
	require.GreaterOrEqual(t, mapping, 0)
	snapAt := mapping + 1 + (mapping+1)%2 // the first even position after the mapping exists

	snapped := sandboxOver(t, events, 2)
	plain := sandboxOver(t, events, 0)
	ctx := t.Context()
	require.NoError(t, snapped.Engine().JumpToEvent(ctx, len(events)-1)) // turns snapshots on
	require.Contains(t, emulator.ReplaySnapshotPositionsForTest(snapped.Engine()), snapAt)
	require.NoError(t, snapped.Engine().JumpToEvent(ctx, snapAt)) // restores that snapshot
	require.NoError(t, snapped.Engine().JumpToEvent(ctx, len(events)))

	want, err := plain.StateManager().(emulator.SnapshotableStateManager).Snapshot(ctx)
	require.NoError(t, err)
	got, err := snapped.StateManager().(emulator.SnapshotableStateManager).Snapshot(ctx)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got), "the restored mapping polled as the from-start replay did")
}

// TestTimeTravel_AThrottledCallReplaysThrottled pins that the state at a point re-decides
// quota as the recording server did. The second CreateBucket was throttled live; if the
// replay exempted it, the state would hold a bucket the live server never created.
func TestTimeTravel_AThrottledCallReplaysThrottled(t *testing.T) {
	cfg := emulator.DefaultConfig()
	cfg.Log.Level = "error"
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(replayClockStart)
	tc.Freeze()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig(), emulator.WithTimeController(tc))
	require.NoError(t, emulator.RegisterDefaultPlugins(t.Context(), registry, state, tc, logger, store, cfg))
	quota := emulator.NewQuotaController(emulator.QuotaConfig{
		Enabled: true, Rules: map[string]emulator.RateRule{"s3": {Rate: 0, Burst: 1}},
	}, tc)
	srv := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger,
		emulator.ServerOptions{Quota: quota}))
	t.Cleanup(srv.Close)

	put := func(bucket string) int {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, srv.URL+"/"+bucket, nil)
		require.NoError(t, err)
		req.Host = "s3.us-east-1.amazonaws.com"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}
	require.Equal(t, http.StatusOK, put("allowed"))
	require.Equal(t, http.StatusServiceUnavailable, put("throttled"), "S3 throttles with 503 SlowDown")
	live, err := state.Snapshot(t.Context())
	require.NoError(t, err)

	resp, err := http.Get(srv.URL + "/v1/debug/events/100/state") //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var at emulator.StateAt
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&at))
	assert.JSONEq(t, string(live), string(at.State), "the throttled bucket does not exist in the replayed state")
	assert.Empty(t, at.Differences, "the throttle replayed as the same SlowDown")
	assert.Equal(t, 1, at.FailedEvents)

	// The configuration path, as `substrate debug state-at` builds it, decides the same.
	cfg.Quotas = emulator.QuotaCfg{Enabled: true, Rules: map[string]emulator.RateRuleCfg{"s3": {Rate: 0, Burst: 1}}}
	sb, err := emulator.NewReplaySandbox(t.Context(), cfg, store, logger)
	require.NoError(t, err)
	fromCfg, err := sb.StateAt(t.Context(), emulator.DefaultStreamID, 100)
	require.NoError(t, err)
	assert.JSONEq(t, string(live), string(fromCfg.State))
	assert.Empty(t, fromCfg.Differences)
}

// TestTimeTravel_ABackwardJumpRestartsTheRunAsReplayDid pins that a jump with no
// snapshot to restore starts the run the way Replay started it: from the stream's stored
// snapshot under UseSnapshots, and with the random source re-seeded.
func TestTimeTravel_ABackwardJumpRestartsTheRunAsReplayDid(t *testing.T) {
	ctx := t.Context()
	setup := func(t *testing.T, cfg emulator.ReplayConfig) (*emulator.ReplayEngine, *emulator.MemoryStateManager) {
		t.Helper()
		store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true})
		for range 3 {
			require.NoError(t, store.RecordEvent(ctx, &emulator.Event{StreamID: "restart", Service: "s3", Operation: "Put"}))
		}
		base := emulator.NewMemoryStateManager()
		require.NoError(t, base.Put(ctx, "marker", "from-snapshot", []byte("1")))
		_, err := store.CreateSnapshot(ctx, "restart", base)
		require.NoError(t, err)
		state := emulator.NewMemoryStateManager()
		return emulator.NewReplayEngine(store, state, emulator.NewTimeController(replayClockStart),
			emulator.NewPluginRegistry(), cfg, &testLogger{}), state
	}

	t.Run("UseSnapshots", func(t *testing.T) {
		engine, state := setup(t, emulator.ReplayConfig{UseSnapshots: true})
		_, err := engine.Replay(ctx, "restart")
		require.NoError(t, err)
		require.NoError(t, state.Delete(ctx, "marker", "from-snapshot"))
		require.NoError(t, engine.JumpToEvent(ctx, 1))
		got, err := state.Get(ctx, "marker", "from-snapshot")
		require.NoError(t, err)
		assert.Equal(t, "1", string(got), "the jump restarted from the stored snapshot, not from empty state")
	})

	t.Run("RandomSeed", func(t *testing.T) {
		engine, _ := setup(t, emulator.ReplayConfig{RandomSeed: 42})
		_, err := engine.Replay(ctx, "restart")
		require.NoError(t, err)
		require.NoError(t, engine.JumpToEvent(ctx, 0))
		first := []float64{engine.RandFloat64(), engine.RandFloat64()}
		require.NoError(t, engine.JumpToEvent(ctx, 3))
		require.NoError(t, engine.JumpToEvent(ctx, 0))
		assert.Equal(t, first, []float64{engine.RandFloat64(), engine.RandFloat64()},
			"the random source restarts with the run")
	})
}

// TestTimeTravel_AStackDispatchIsSkippedOnlyWithItsParent pins that a stack's resource
// dispatch is skipped when the CreateStack that made it is replayed too, and executed when
// it is not. A stack named "default" puts both in one stream.
func TestTimeTravel_AStackDispatchIsSkippedOnlyWithItsParent(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	cfnCreateStack(t, ts, emulator.DefaultStreamID, cfnTemplateOneBucket)
	live := liveSnapshot(t, ts)

	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	// The dispatches are recorded as they run, before the CreateStack that made them.
	var parentEv *emulator.Event
	var childEvs []*emulator.Event
	for _, ev := range events {
		if ev.Operation == "CreateStack" {
			parentEv = ev
		} else {
			childEvs = append(childEvs, ev)
		}
	}
	require.NotNil(t, parentEv)
	children := len(childEvs)
	require.Positive(t, children)
	for _, ev := range childEvs {
		assert.Equal(t, parentEv.RequestID, ev.ParentRequestID, "a dispatch names its CreateStack")
	}

	results := ts.AssertReplay(t)
	assert.Equal(t, children, results.SkipReasons[emulator.SkipInternalDispatch])
	status, at, raw := getStateAt(t, ts, 1<<40, "")
	require.Equal(t, http.StatusOK, status, raw)
	assert.JSONEq(t, live, string(at.State), "applied once, by the CreateStack's re-run")

	// Without the parent, the dispatches are executed.
	sb, err := emulator.NewReplaySandbox(t.Context(), nil, ts.Store(), emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	alone, err := sb.Engine().ReplayEvents(t.Context(), childEvs)
	require.NoError(t, err)
	assert.Zero(t, alone.SkippedEvents)
	assert.Equal(t, children, alone.SuccessEvents)

	// A dispatch recorded before parents were, beside a top-level request, is skipped
	// as unverified.
	legacy := *childEvs[0]
	legacy.ParentRequestID = ""
	mixed, err := sb.Engine().ReplayEvents(t.Context(), []*emulator.Event{&legacy, parentEv})
	require.NoError(t, err)
	assert.Equal(t, 1, mixed.SkipReasons[emulator.SkipUnattributedDispatch])
	assert.True(t, emulator.SkipUnattributedDispatch.Unverified())
}

// TestTimeTravel_ConcurrentStateAtCalls pins that two StateAt calls on one sandbox, as two
// concurrent debug requests make, each return the state at their own point. Run under
// -race.
func TestTimeTravel_ConcurrentStateAtCalls(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, ts, "concurrent-a")
	afterA := liveSnapshot(t, ts)
	replayPutBucket(t, ts, "concurrent-b")
	afterB := liveSnapshot(t, ts)
	last := lastDefaultSequence(t, ts)
	sb := ts.ReplaySandbox(t)

	want := map[int64]string{last: afterA, last + 1: afterB}
	results := make(chan error, 20)
	for i := range 20 {
		seq := last + int64(i%2)
		go func() {
			at, err := sb.StateAt(context.Background(), emulator.DefaultStreamID, seq)
			if err == nil && !jsonEqual(string(at.State), want[seq]) {
				err = fmt.Errorf("seq %d returned another point's state", seq)
			}
			results <- err
		}()
	}
	for range 20 {
		assert.NoError(t, <-results)
	}
}

// jsonEqual reports whether two JSON documents are equal as values.
func jsonEqual(a, b string) bool {
	var x, y any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// TestTimeTravel_EngineRefusals pins the errors of the navigation methods.
func TestTimeTravel_EngineRefusals(t *testing.T) {
	engine := emulator.NewReplayEngine(emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true}),
		emulator.NewMemoryStateManager(), emulator.NewTimeController(replayClockStart),
		emulator.NewPluginRegistry(), emulator.ReplayConfig{}, &testLogger{})
	ctx := t.Context()
	assert.Error(t, engine.JumpToEvent(ctx, 0))
	assert.Error(t, engine.JumpToSequence(ctx, 0))
	_, err := engine.StepBackward(ctx)
	assert.Error(t, err)
	_, err = engine.ReplayEvents(ctx, nil)
	assert.Error(t, err)
}

// TestTimeTravel_DebugSessionEdges pins a debug session over a stream that does not
// exist, and its accessors before navigation.
func TestTimeTravel_DebugSessionEdges(t *testing.T) {
	registry, store, state, tc, logger := newClientTestDeps(t)
	client := emulator.NewClient(registry, store, state, tc, logger)
	dbg := client.NewDebugSession("no-such-stream")
	assert.Nil(t, dbg.Results())
	assert.Error(t, dbg.JumpToEvent(t.Context(), 0))
	assert.Error(t, dbg.StepBackward(t.Context()))
	snap, err := dbg.InspectState(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, "{}", string(snap), "before navigation it reads the client's own, empty, state")
}

// TestTimeTravel_TestServerReplayEventsRunsIntoTheServer pins that ReplayEvents leaves
// the server holding the replayed state, and resets what was there before.
func TestTimeTravel_TestServerReplayEventsRunsIntoTheServer(t *testing.T) {
	source := emulator.StartTestServer(t)
	source.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, source, "carried")
	events, err := source.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)

	target := emulator.StartTestServer(t)
	replayPutBucket(t, target, "wiped")
	results := target.ReplayEvents(t, roundTrip(t, events))
	emulator.AssertReplayMatches(t, results)

	assert.Equal(t, http.StatusOK, replayHeadBucket(t, target, "carried"))
	assert.Equal(t, http.StatusNotFound, replayHeadBucket(t, target, "wiped"))
}

// replayHeadBucket returns the status of a HeadBucket.
func replayHeadBucket(t *testing.T, ts *emulator.TestServer, bucket string) int {
	t.Helper()
	status, _ := replayDo(t, ts, http.MethodHead, "/"+bucket, "s3.us-east-1.amazonaws.com", nil, nil)
	return status
}
