package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// liveSnapshot returns the test server's state as the state-at endpoint renders it.
func liveSnapshot(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	raw, err := ts.StateManager().(emulator.SnapshotableStateManager).Snapshot(t.Context())
	require.NoError(t, err)
	return string(raw)
}

// lastDefaultSequence returns the sequence of the newest event in the default stream.
func lastDefaultSequence(t *testing.T, ts *emulator.TestServer) int64 {
	t.Helper()
	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	return events[len(events)-1].Sequence
}

// getStateAt calls GET /v1/debug/events/{seq}/state and decodes the answer.
func getStateAt(t *testing.T, ts *emulator.TestServer, seq int64, query string) (int, *emulator.StateAt, string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/v1/debug/events/%d/state%s", ts.URL, seq, query)) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil, string(raw)
	}
	var at emulator.StateAt
	require.NoError(t, json.Unmarshal(raw, &at), string(raw))
	return resp.StatusCode, &at, string(raw)
}

// cfnTemplateOneBucket is a stack whose deployer dispatches land in their own stream,
// interleaving sequences with the default stream.
const cfnTemplateOneBucket = `{"Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"from-stack"}}}}`

// cfnCreateStack creates a stack over the wire.
func cfnCreateStack(t *testing.T, ts *emulator.TestServer, name, template string) {
	t.Helper()
	form := url.Values{"Action": {"CreateStack"}, "Version": {"2010-05-15"},
		"StackName": {name}, "TemplateBody": {template}}
	status, body := replayDo(t, ts, http.MethodPost, "/", "cloudformation.us-east-1.amazonaws.com",
		[]byte(form.Encode()), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	require.Equal(t, http.StatusOK, status, string(body))
}

// TestTimeTravel_StateAtMatchesTheLiveStateAtEveryPoint is #1601's acceptance: for a
// session whose later requests name identifiers earlier ones minted, the state at N is
// the live state at N, at every N, and no difference is reported.
//
// The session includes a CreateStack. Its resource dispatches are recorded in the stack's
// own stream with sequences between the default stream's, so it also pins that a
// sequence is not used as a stream position and that a stack's dispatches are not applied
// on top of the CreateStack that re-runs them.
func TestTimeTravel_StateAtMatchesTheLiveStateAtEveryPoint(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)

	type point struct {
		seq   int64
		state string
	}
	empty := liveSnapshot(t, ts)
	var points []point
	record := func() { points = append(points, point{lastDefaultSequence(t, ts), liveSnapshot(t, ts)}) }

	vpc := xmlElement(t, ec2ReplayCall(t, ts, "CreateVpc", url.Values{"CidrBlock": {"10.20.0.0/16"}}), "vpcId")
	record()
	subnet := xmlElement(t, ec2ReplayCall(t, ts, "CreateSubnet", url.Values{
		"VpcId": {vpc}, "CidrBlock": {"10.20.1.0/24"},
	}), "subnetId")
	record()
	cfnCreateStack(t, ts, "tt-stack", cfnTemplateOneBucket)
	record()
	ec2ReplayCall(t, ts, "CreateTags", url.Values{
		"ResourceId.1": {subnet}, "Tag.1.Key": {"Name"}, "Tag.1.Value": {"web"},
	})
	record()
	replayPutBucket(t, ts, "after-stack")
	record()

	stack, err := ts.Store().GetStream(t.Context(), "tt-stack")
	require.NoError(t, err)
	require.NotEmpty(t, stack, "the stack's dispatches are recorded in their own stream")

	status, at, raw := getStateAt(t, ts, 0, "")
	require.Equal(t, http.StatusOK, status, raw)
	assert.JSONEq(t, empty, string(at.State), "sequence 0 is the state before any event")
	assert.Zero(t, at.EventsApplied)

	for i, p := range points {
		status, at, raw := getStateAt(t, ts, p.seq+1, "")
		require.Equal(t, http.StatusOK, status, raw)
		assert.Equal(t, i+1, at.EventsApplied, "after request %d", i)
		assert.JSONEq(t, p.state, string(at.State), "state after request %d (seq %d)", i, p.seq)
		assert.Empty(t, at.Differences, "after request %d", i)
		assert.Zero(t, at.FailedEvents)
		assert.Empty(t, at.Skipped)
	}

	// The stack's own stream replays its dispatches, since its parent is not in it.
	status, at, raw = getStateAt(t, ts, 1<<40, "?stream=tt-stack")
	require.Equal(t, http.StatusOK, status, raw)
	assert.Contains(t, string(at.State), "from-stack")
	assert.NotContains(t, string(at.State), vpc, "another stream's events are not applied")
	assert.Empty(t, at.Differences)
}

// TestTimeTravel_StateAtReportsAnUnknownStream pins the 404.
func TestTimeTravel_StateAtReportsAnUnknownStream(t *testing.T) {
	ts := emulator.StartTestServer(t)
	replayPutBucket(t, ts, "only-default")
	status, _, raw := getStateAt(t, ts, 1, "?stream=nope")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, raw, "no events in stream")
}

// TestTimeTravel_StateAtReportsADivergence pins that a replay that does not reproduce
// the recording says so in the state-at answer, rather than returning a state that
// looks authoritative.
func TestTimeTravel_StateAtReportsADivergence(t *testing.T) {
	ts := emulator.StartTestServer(t)
	replayPutBucket(t, ts, "diverges")
	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	events[0].Response.StatusCode = http.StatusTeapot

	status, at, raw := getStateAt(t, ts, 1, "")
	require.Equal(t, http.StatusOK, status, raw)
	require.Len(t, at.Differences, 1)
	assert.Equal(t, "status_code", at.Differences[0].Field)
	assert.Contains(t, raw, `"differences":[{"event_id"`)
}

// TestTimeTravel_StateDiffIsPerKeyAndPerStream pins the diff endpoint on the same path.
func TestTimeTravel_StateDiffIsPerKeyAndPerStream(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, ts, "diff-a")
	first := lastDefaultSequence(t, ts)
	replayPutBucket(t, ts, "diff-b")

	resp, err := http.Get(fmt.Sprintf("%s/v1/debug/state/diff?from=%d&to=%d", ts.URL, first+1, first+100)) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var diff struct {
		Stream      string                       `json:"stream"`
		Added       map[string]string            `json:"added"`
		Removed     map[string]string            `json:"removed"`
		Changed     map[string]map[string]string `json:"changed"`
		Differences []*emulator.EventDifference  `json:"differences"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&diff))
	assert.Equal(t, emulator.DefaultStreamID, diff.Stream)
	assert.Empty(t, diff.Removed)
	assert.Empty(t, diff.Differences)
	var added []string
	for k := range diff.Added {
		added = append(added, k)
	}
	assert.True(t, len(added) > 0 && strings.Contains(strings.Join(added, " "), "diff-b"), "added: %v", added)
	assert.NotContains(t, strings.Join(added, " "), "diff-a", "diff-a existed at from")
}

// s3Object reads an object's bytes through the sandbox's own S3 plugin.
func s3Object(t *testing.T, sb *emulator.ReplaySandbox, bucket, key string) string {
	t.Helper()
	resp, err := sb.Registry().RouteRequest(
		&emulator.RequestContext{RequestID: "req-read", AccountID: "123456789012", Region: "us-east-1"},
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject", HTTPMethod: http.MethodGet,
			Path: "/" + bucket + "/" + key, Headers: map[string]string{}, Params: map[string]string{}})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.Code
	}
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		return xmlElement(t, resp.Body, "Code")
	}
	return string(resp.Body)
}

// sandboxOver copies events into a fresh store with the given snapshot interval and
// returns a sandbox over it with the stream loaded and replayed to its end.
func sandboxOver(t *testing.T, events []*emulator.Event, interval int) *emulator.ReplaySandbox {
	t.Helper()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, SnapshotInterval: interval})
	for _, ev := range events {
		cp := *ev
		require.NoError(t, store.RecordEvent(t.Context(), &cp))
	}
	// The recording test server had no quota controller, so neither does the replay;
	// an enabled one is run state a snapshot cannot hold.
	cfg := emulator.DefaultConfig()
	cfg.Quotas.Enabled = false
	sb, err := emulator.NewReplaySandbox(t.Context(), cfg, store, emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	results, err := sb.Engine().Replay(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	return sb
}

// TestTimeTravel_StepBackwardAndSnapshots pins StepBackward and JumpToEvent with and
// without snapshots: every position reaches the same state, including the S3 payload
// bytes a StateManager snapshot alone does not hold, and snapshots are taken.
func TestTimeTravel_StepBackwardAndSnapshots(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, ts, "steps")
	replayPutObject(t, ts, "steps", "k", "v1")
	replayPutObject(t, ts, "steps", "k", "v2")
	replayDo(t, ts, http.MethodDelete, "/steps/k", "s3.us-east-1.amazonaws.com", nil, nil)
	replayPutObject(t, ts, "steps", "k", "v3")

	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, events, 5)
	want := []string{"NoSuchBucket", "NoSuchKey", "v1", "v2", "NoSuchKey", "v3"}

	snapped := sandboxOver(t, events, 2)
	plain := sandboxOver(t, events, 0)
	assert.Empty(t, emulator.ReplaySnapshotPositionsForTest(snapped.Engine()),
		"a replay that only moved forward takes no snapshot")

	ctx := context.Background()
	for pos := len(events); pos > 0; pos-- {
		require.Equal(t, pos, emulator.ReplayPositionForTest(snapped.Engine()))
		ev, err := snapped.Engine().StepBackward(ctx)
		require.NoError(t, err)
		assert.Equal(t, events[pos-1].Sequence, ev.Sequence, "StepBackward returns the event it stepped back over")

		require.NoError(t, plain.Engine().JumpToEvent(ctx, pos-1))
		snappedState, err := snapped.StateManager().(emulator.SnapshotableStateManager).Snapshot(ctx)
		require.NoError(t, err)
		plainState, err := plain.StateManager().(emulator.SnapshotableStateManager).Snapshot(ctx)
		require.NoError(t, err)
		assert.JSONEq(t, string(plainState), string(snappedState), "position %d", pos-1)
		assert.Equal(t, want[pos-1], s3Object(t, snapped, "steps", "k"), "payload at position %d", pos-1)
		assert.Equal(t, want[pos-1], s3Object(t, plain, "steps", "k"), "payload at position %d", pos-1)
	}
	_, err = snapped.Engine().StepBackward(ctx)
	assert.ErrorContains(t, err, "beginning")
	assert.Equal(t, []int{2}, emulator.ReplaySnapshotPositionsForTest(snapped.Engine()),
		"the first step back replayed from the start and took the snapshot the later ones used")
	assert.Empty(t, emulator.ReplaySnapshotPositionsForTest(plain.Engine()), "snapshot_interval 0 takes none")

	// Forward again over the snapshots, and to the end.
	require.NoError(t, snapped.Engine().JumpToEvent(ctx, 3))
	assert.Equal(t, "v2", s3Object(t, snapped, "steps", "k"))
	require.NoError(t, snapped.Engine().JumpToEvent(ctx, len(events)))
	assert.Equal(t, "v3", s3Object(t, snapped, "steps", "k"))
	assert.Equal(t, []int{2, 4}, emulator.ReplaySnapshotPositionsForTest(snapped.Engine()))
	assert.Error(t, snapped.Engine().JumpToEvent(ctx, len(events)+1))
}

// TestTimeTravel_JumpsDoNotRepeatDifferences pins that re-running an event on the way to
// a jump target does not report its differences again.
func TestTimeTravel_JumpsDoNotRepeatDifferences(t *testing.T) {
	ts := emulator.StartTestServer(t)
	replayPutBucket(t, ts, "once")
	replayPutBucket(t, ts, "twice")
	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	events[0].Response.StatusCode = http.StatusTeapot

	sb, err := emulator.NewReplaySandbox(t.Context(), nil, ts.Store(), emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	engine := sb.Engine()
	results, err := engine.Replay(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, results.Differences, 1)

	for _, pos := range []int{0, 2, 1, 2, 0, 2} {
		require.NoError(t, engine.JumpToEvent(t.Context(), pos))
	}
	_, err = engine.StepBackward(t.Context())
	require.NoError(t, err)
	_, err = engine.StepForward(t.Context())
	require.NoError(t, err)
	assert.Len(t, results.Differences, 1, "%s", replayDifferenceSummary(results))
}

// TestTimeTravel_JumpToSequenceMapsAcrossStreams pins JumpToSequence over a stream whose
// sequences are not its positions.
func TestTimeTravel_JumpToSequenceMapsAcrossStreams(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	replayPutBucket(t, ts, "seq-a")
	cfnCreateStack(t, ts, "seq-stack", cfnTemplateOneBucket)
	afterStack := liveSnapshot(t, ts)
	replayPutBucket(t, ts, "seq-b")
	last := lastDefaultSequence(t, ts)
	require.Greater(t, last, int64(2), "the stack's dispatches took sequences in between")

	sb, err := emulator.NewReplaySandbox(t.Context(), nil, ts.Store(), emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	_, err = sb.Engine().Replay(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)

	require.NoError(t, sb.Engine().JumpToSequence(t.Context(), last))
	assert.Equal(t, 2, emulator.ReplayPositionForTest(sb.Engine()))
	got, err := sb.StateManager().(emulator.SnapshotableStateManager).Snapshot(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, afterStack, string(got))

	require.NoError(t, sb.Engine().JumpToSequence(t.Context(), last+1000))
	assert.Equal(t, 3, emulator.ReplayPositionForTest(sb.Engine()))
}

// failingGetState is a state manager whose reads fail, for InspectState.
type failingGetState struct{ *emulator.MemoryStateManager }

// Get always fails.
func (f failingGetState) Get(context.Context, string, string) ([]byte, error) {
	return nil, errors.New("disk on fire")
}

// TestTimeTravel_InspectStateReturnsReadErrors pins that a failed read is returned, not
// silently dropped from the map.
func TestTimeTravel_InspectStateReturnsReadErrors(t *testing.T) {
	state := failingGetState{emulator.NewMemoryStateManager()}
	require.NoError(t, state.Put(t.Context(), "s3", "bucket:x", []byte("{}")))
	engine := emulator.NewReplayEngine(emulator.NewEventStore(emulator.EventStoreConfig{}), state,
		emulator.NewTimeController(replayClockStart), emulator.NewPluginRegistry(), emulator.ReplayConfig{}, &testLogger{})
	_, err := engine.InspectState(t.Context(), "s3")
	require.ErrorContains(t, err, "disk on fire")

	_, err = emulator.NewReplayEngine(emulator.NewEventStore(emulator.EventStoreConfig{}), nil,
		nil, nil, emulator.ReplayConfig{}, &testLogger{}).InspectState(t.Context(), "s3")
	assert.Error(t, err)
}

// TestTimeTravel_DebugSessionLeavesTheClientStateAlone pins that navigating a debug
// session never resets the client's live state.
func TestTimeTravel_DebugSessionLeavesTheClientStateAlone(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newClientTestDeps(t)
	client := emulator.NewClient(registry, store, state, tc, logger)
	result, err := client.Deploy(ctx, `{"Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"live-bucket"}}}}`,
		emulator.Intent{})
	require.NoError(t, err)

	before, err := state.Snapshot(ctx)
	require.NoError(t, err)
	require.Contains(t, string(before), "live-bucket")

	dbg := client.NewDebugSession(result.StreamID)
	require.NoError(t, dbg.JumpToEvent(ctx, 0))
	require.NotNil(t, dbg.Results())

	after, err := state.Snapshot(ctx)
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after), "the client's state is untouched")

	snap, err := dbg.InspectState(ctx)
	require.NoError(t, err)
	assert.NotContains(t, string(snap), "live-bucket", "the session is at position 0")
}
