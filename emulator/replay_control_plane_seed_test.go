package emulator_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1140: a replay of a seeded stream answered the unseeded sequence, because a
// control-plane seed was not an event.
//
// Every seedable outcome in substrate is written through a control-plane endpoint rather
// than an AWS request, and only the AWS path recorded anything — so a seed never entered
// the stream. A replay opens by resetting the whole state manager, and a seed lives in the
// state manager. So a recording in which a conditional PUT was refused twice and then
// accepted replayed as three acceptances, with nothing failing: every request was
// re-executed and every one succeeded. CLAUDE.md's "same inputs (including seeds) → same
// outputs" was false in the parenthesis.
//
// The fix records the write as an event of its own and replays it in position. Every
// assertion below is on what the *replayed observations report*, rather than on where the
// seed is stored: the storage is the thing that changed, and the observation is the thing a
// consumer sees.
//
// # Why the S3 conditional-conflict seed is the subject
//
// Two reasons, and the first is that it is the hardest case for the fix that was not
// chosen. Its budget is spent **in place** — [S3Plugin.consumeConditionalConflict]
// decrements the stored record and writes it back — so by the end of a recording the state
// manager holds a budget of zero. Exempting the seed namespaces from the reset, #1140's
// smaller option, would therefore hand the replay a spent seed and reproduce neither the
// recording's sequence nor the unseeded one. Recording the write reproduces it exactly,
// because the reset still wipes the counter and the recorded POST re-arms it.
//
// The second is that every identifier in the stream is one the caller chose — a bucket
// name, a key, a body — so a replay of these streams mints nothing and can be asserted to
// produce **no** differences at all. A stream containing an EC2 create could not: substrate
// mints its ids from crypto/rand, so the replay's volume id differs from the recording's
// and every later request naming it diverges (#856). That debt would force these tests to
// assert something weaker than "identical", and the point of #1140 is that the two runs are
// identical.

// controlPlaneEventService is the [emulator.Event.Service] a recorded control-plane write
// carries.
//
// Spelled out rather than exported from the package under test, because it is the
// wire-visible name a consumer reading an exported event stream sees. A test is the right
// place to pin that independently of the constant that produces it.
const controlPlaneEventService = "substrate-control"

// TestReplay_ASeededConflictBudgetReplaysUnderTheSameSeed is #1140's regression test.
//
// The recording spends a two-conflict budget and then succeeds, so the recorded sequence is
// 409, 409, 200. Two distinct failures are ruled out by the same assertion:
//
//   - if the seed is not re-applied, all three replayed writes answer 200;
//   - if the seed were re-applied from preserved state rather than from the event, the
//     replay would inherit the budget the recording spent down to zero — #1140's fourth
//     criterion, that a consumed count must reset rather than carry over — and would
//     answer 200 three times as well.
func TestReplay_ASeededConflictBudgetReplaysUnderTheSameSeed(t *testing.T) {
	const bucket, key = "replay-seeded-budget", "obj"
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	cpSeedConflict(t, ts, `{"bucket":"`+bucket+`","key":"`+key+`","putConflicts":2}`)
	cpCreateBucket(t, ts, bucket)

	recorded := []int{
		cpConditionalPut(t, ts, bucket, key, "payload"),
		cpConditionalPut(t, ts, bucket, key, "payload"),
		cpConditionalPut(t, ts, bucket, key, "payload"),
	}
	require.Equal(t, []int{http.StatusConflict, http.StatusConflict, http.StatusOK}, recorded,
		"the recording has to spend the budget, or the replay has nothing to reproduce")

	seedEvents := cpControlPlaneEvents(t, ts)
	require.Len(t, seedEvents, 1, "the seed is one event in the stream")
	assert.Equal(t, "POST /v1/s3/conditional-conflict", seedEvents[0].Operation,
		"named by the endpoint's route pattern, so the name is a bounded set even for a path carrying an id")
	assert.Equal(t, "/v1/s3/conditional-conflict", seedEvents[0].Request.Path)
	assert.Equal(t, http.StatusOK, seedEvents[0].Response.StatusCode)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Zero(t, results.SkippedEvents, "the seed is re-executed, not skipped")
	assert.Empty(t, results.Differences,
		"the replayed writes report the recorded sequence, not the unseeded one: %s",
		replayDifferenceSummary(results))
}

// TestReplay_ASeedWrittenBetweenTwoWritesReplaysBetweenThem is the case that decided #1140
// between its options.
//
// Preserving the seed namespaces across the reset would make the seed present for the
// *whole* replay, so the first write below — which the recording accepted, because nothing
// was armed yet — would be refused. Only an event carries the position, and position is the
// whole of what this stream says.
func TestReplay_ASeedWrittenBetweenTwoWritesReplaysBetweenThem(t *testing.T) {
	const bucket = "replay-seed-position"
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	cpCreateBucket(t, ts, bucket)
	require.Equal(t, http.StatusOK, cpConditionalPut(t, ts, bucket, "before", "a"),
		"unarmed, a conditional write to an absent key succeeds")

	// The wildcard, so that the only thing separating the two writes is when the seed was
	// written — not which key it names.
	cpSeedConflict(t, ts, `{"putConflicts":1}`)

	require.Equal(t, http.StatusConflict, cpConditionalPut(t, ts, bucket, "after", "b"),
		"and armed, the next one is refused")

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences,
		"accepted-then-refused replays in that order: %s", replayDifferenceSummary(results))
}

// TestReplay_AClearedSeedReplaysAsCleared covers the other half of every seed endpoint —
// the DELETE — and with it the reason a recorded event carries the query string.
//
// Most clear endpoints key on one (`?bucket=&key=`, `?snapshotId=`, `?roleName=`), so an
// event recording the path alone would replay as a clear-everything. That is why the
// recorded target is asserted here and not only the replayed outcome: with a single seed
// armed, clearing one and clearing all produce the same answer, and the target is what
// distinguishes them.
func TestReplay_AClearedSeedReplaysAsCleared(t *testing.T) {
	const bucket, key = "replay-cleared-seed", "obj"
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	cpSeedConflict(t, ts, `{"bucket":"`+bucket+`","key":"`+key+`","putConflicts":5}`)
	cpCreateBucket(t, ts, bucket)
	require.Equal(t, http.StatusConflict, cpConditionalPut(t, ts, bucket, key, "a"))

	cpClearConflict(t, ts, bucket, key)
	require.Equal(t, http.StatusOK, cpConditionalPut(t, ts, bucket, key, "a"),
		"clearing the seed leaves the write to be decided by the key's own state")

	events := cpControlPlaneEvents(t, ts)
	require.Len(t, events, 2)
	assert.Equal(t, "DELETE /v1/s3/conditional-conflict", events[1].Operation)
	assert.Equal(t, "/v1/s3/conditional-conflict?bucket="+bucket+"&key="+key, events[1].Request.Path,
		"the target carries the query, because that is what says which seed was cleared")

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences,
		"refused-then-accepted replays in that order: %s", replayDifferenceSummary(results))
}

// TestReplay_WithoutAControlPlaneHandlerASeedIsSkipped pins the documented fallback on
// [emulator.WithControlPlaneHandler], and is what makes the three tests above non-vacuous.
//
// An engine given no handler cannot re-issue the write, and what it does instead is report a
// skip — the behavior every replay had before #1140, now visible in the counters rather than
// silent. The same stream replayed that way diverges on exactly the observation the tests
// above assert is identical, which is the defect itself.
func TestReplay_WithoutAControlPlaneHandlerASeedIsSkipped(t *testing.T) {
	const bucket, key = "replay-no-handler", "obj"
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	cpSeedConflict(t, ts, `{"bucket":"`+bucket+`","key":"`+key+`","putConflicts":1}`)
	cpCreateBucket(t, ts, bucket)
	require.Equal(t, http.StatusConflict, cpConditionalPut(t, ts, bucket, key, "a"))

	engine := emulator.NewReplayEngine(
		ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
	)
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Equal(t, 1, results.SkippedEvents, "the one control-plane event in the stream")
	assert.NotEmpty(t, results.Differences,
		"and the write the recording refused is accepted on replay, which is the whole defect")
}

// TestReplay_AReplayAppendsNoControlPlaneEvent guards the marker the engine puts on the
// request it synthesizes.
//
// A replayed seed goes back through the very middleware that recorded it, and a replay
// writes no events. Without the marker each replay would append a copy of every seed in the
// stream, so replaying twice would find a longer stream the second time — and with a
// file-backed store the copies would land in the recording.
func TestReplay_AReplayAppendsNoControlPlaneEvent(t *testing.T) {
	const bucket, key = "replay-no-append", "obj"
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	cpSeedConflict(t, ts, `{"bucket":"`+bucket+`","key":"`+key+`","putConflicts":1}`)
	cpCreateBucket(t, ts, bucket)
	require.Equal(t, http.StatusConflict, cpConditionalPut(t, ts, bucket, key, "a"))

	before, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	for range 2 {
		_, err = engine.Replay(t.Context(), replayStreamID)
		require.NoError(t, err)
	}

	after, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "two replays leave the stream exactly as long as it was")
}

// TestControlPlane_ARefusedSeedIsNotRecorded pins the status filter.
//
// A seed the endpoint refuses changed nothing, so there is nothing for a replay to
// re-apply. Recording it would put an event in the stream for a state transition that never
// happened, and make the replay re-issue a request whose only effect is an error body.
func TestControlPlane_ARefusedSeedIsNotRecorded(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	// A bucket without a key: the endpoint refuses it rather than storing a seed under a
	// state key no write could match (#540).
	status, body := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/s3/conditional-conflict",
		[]byte(`{"bucket":"b"}`))
	require.Equal(t, http.StatusBadRequest, status, "body = %s", body)

	assert.Empty(t, cpControlPlaneEvents(t, ts),
		"a refusal is not a write, so it is not an event")
}

// TestReplay_AControlPlaneEventThatCannotBeReappliedIsReported covers the two answers a
// replay can give to a control-plane event it cannot reproduce.
//
// Both events are hand-built, because neither is reachable through the server: the
// middleware only ever records a target the router matched and a 2xx it answered. They are
// worth pinning anyway, because an event stream is a file — an exported regression fixture
// is exactly that — so a replay must report a stream it cannot re-apply rather than
// panicking on it or passing it in silence.
func TestReplay_AControlPlaneEventThatCannotBeReappliedIsReported(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	now := ts.TimeController().Now()

	// A target that is not a URL at all: net/url rejects an ASCII control character
	// anywhere in one, so the request cannot even be built.
	require.NoError(t, ts.Store().RecordEvent(t.Context(), &emulator.Event{
		Timestamp: now,
		StreamID:  replayStreamID,
		Service:   controlPlaneEventService,
		Operation: "POST /v1/\x7f",
		Request: &emulator.AWSRequest{
			Service: controlPlaneEventService, HTTPMethod: http.MethodPost, Path: "/v1/\x7f",
		},
		Response: &emulator.AWSResponse{StatusCode: http.StatusOK},
	}))

	// A seed the event records as accepted that the endpoint refuses — a bucket without a
	// key. This is the case the status comparison exists for: without it, every later
	// observation would diverge with nothing in the report pointing at the reason.
	require.NoError(t, ts.Store().RecordEvent(t.Context(), &emulator.Event{
		Timestamp: now,
		StreamID:  replayStreamID,
		Service:   controlPlaneEventService,
		Operation: "POST /v1/s3/conditional-conflict",
		Request: &emulator.AWSRequest{
			Service: controlPlaneEventService, HTTPMethod: http.MethodPost,
			Path: "/v1/s3/conditional-conflict",
			Body: []byte(`{"bucket":"b"}`),
		},
		Response: &emulator.AWSResponse{StatusCode: http.StatusOK},
	}))

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Equal(t, 1, results.FailedEvents, "the unbuildable request is an error, not a divergence")
	assert.Equal(t, 1, results.SuccessEvents, "the refused seed was re-issued, and disagreed")
	require.Len(t, results.Differences, 2, "%s", replayDifferenceSummary(results))
	assert.Equal(t, "error", results.Differences[0].Field)
	assert.Equal(t, "status_code", results.Differences[1].Field,
		"the refusal is a divergence, because the request was issued and answered")
	assert.Equal(t, http.StatusBadRequest, results.Differences[1].Actual)
}

// cpControlPlaneEvents returns the recorded control-plane writes in the replayed stream, in
// order.
func cpControlPlaneEvents(t *testing.T, ts *emulator.TestServer) []*emulator.Event {
	t.Helper()
	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)

	var out []*emulator.Event
	for _, event := range events {
		if event.Service == controlPlaneEventService {
			out = append(out, event)
		}
	}
	return out
}

// cpControlPlaneRequest issues one control-plane request and returns its status and body.
//
// The target is the test server's own address rather than an S3 endpoint, because the
// control plane is substrate's own API and is not served under any AWS host.
func cpControlPlaneRequest(
	t *testing.T,
	ts *emulator.TestServer,
	method, target string,
	body []byte,
) (int, string) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+target, reader)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(got)
}

// cpSeedConflict arms a conditional-conflict seed and requires it accepted.
func cpSeedConflict(t *testing.T, ts *emulator.TestServer, body string) {
	t.Helper()
	status, got := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/s3/conditional-conflict",
		[]byte(body))
	require.Equal(t, http.StatusOK, status, "seed = %s", got)
}

// cpClearConflict clears the seed armed for one bucket and key.
func cpClearConflict(t *testing.T, ts *emulator.TestServer, bucket, key string) {
	t.Helper()
	query := url.Values{"bucket": {bucket}, "key": {key}}
	status, got := cpControlPlaneRequest(t, ts, http.MethodDelete,
		"/v1/s3/conditional-conflict?"+query.Encode(), nil)
	require.Equal(t, http.StatusOK, status, "clear = %s", got)
}

// cpS3 issues one path-style S3 request against the test server and returns its status.
func cpS3(
	t *testing.T,
	ts *emulator.TestServer,
	method, path string,
	body []byte,
	headers map[string]string,
) int {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = "s3.amazonaws.com"
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotContains(t, string(got), "InternalError", "%s %s", method, path)
	return resp.StatusCode
}

// cpCreateBucket creates a bucket and requires it created.
func cpCreateBucket(t *testing.T, ts *emulator.TestServer, bucket string) {
	t.Helper()
	require.Equal(t, http.StatusOK, cpS3(t, ts, http.MethodPut, "/"+bucket, nil, nil),
		"create bucket %s", bucket)
}

// cpConditionalPut writes an object under If-None-Match: * and returns the status.
//
// The precondition is what makes the write eligible for a seeded conflict at all: AWS
// documents ConditionalRequestConflict only on the two conditional headers, so an
// unconditional PUT would ignore the seed and the test would pass vacuously.
func cpConditionalPut(t *testing.T, ts *emulator.TestServer, bucket, key, body string) int {
	t.Helper()
	return cpS3(t, ts, http.MethodPut, "/"+bucket+"/"+key, []byte(body),
		map[string]string{"If-None-Match": "*"})
}
