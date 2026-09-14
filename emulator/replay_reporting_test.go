package emulator_test

// Replay reporting: what a replay counts, and what it compares.
//
// Three defects, all of the same kind — a number or a comparison that stood for
// work that never happened (#833, #817):
//
//   - A skipped event was counted as a *success*. replayEvent incremented
//     SkippedEvents and returned a nil error; the driver read the nil error as a
//     success and incremented SuccessEvents too. Because IncludeBodies defaults to
//     false, no event on a default-configured stream carries a request, so *every*
//     event was skipped — and a replay of such a stream reported total=N success=N
//     while executing nothing.
//   - A recorded refusal that replayed as a success reported no difference. The
//     error comparison ran only when the replay itself errored, and the status
//     comparison is guarded on event.Response, which is nil for exactly the events
//     a pre-plugin refusal produced.
//   - ValidateState was inert. IncludeStateHashes was declared, plumbed and
//     defaulted, and never read, so StateHashBefore/StateHashAfter were never
//     written and both comparisons were guarded on a field that was always "".
//
// Every test here records over the wire through a [emulator.TestServer] and
// replays that stream, because the defects are all in the relationship between
// what the recorder writes and what the replayer counts. A hand-built event can
// carry a state hash nothing would have recorded, which is how the existing
// replay_test.go coverage passed while the feature did nothing.

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// replayStreamID is the stream an HTTP request with no stream metadata records to
// (see streamIDFromContext).
const replayStreamID = "default"

// replayEngineFor builds a ReplayEngine over the same store, state, clock and
// registry the test server itself uses — the wiring test/e2e/journey_replay_test.go
// uses, and the wiring `substrate replay` does not (#855).
func replayEngineFor(ts *emulator.TestServer, cfg emulator.ReplayConfig) *emulator.ReplayEngine {
	return emulator.NewReplayEngine(
		ts.Store(),
		ts.StateManager(),
		ts.TimeController(),
		ts.Registry(),
		cfg,
		emulator.NewDefaultLogger(slog.LevelError, false),
	)
}

// replayPutBucket creates a bucket through the S3 plugin over the wire.
func replayPutBucket(t *testing.T, ts *emulator.TestServer, bucket string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+"/"+bucket, nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateBucket %s", bucket)
}

// replayPutObject writes an object through the S3 plugin over the wire.
func replayPutObject(t *testing.T, ts *emulator.TestServer, bucket, key, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		ts.URL+"/"+bucket+"/"+key, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "PutObject %s/%s", bucket, key)
}

// replayGetObjectStatus reads an object and returns the HTTP status, so a test can
// assert a fault refused it and, later, that the same request succeeds.
func replayGetObjectStatus(t *testing.T, ts *emulator.TestServer, bucket, key string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

// replayHeadBucketStatus reports whether a bucket exists, used to prove a replay
// actually rebuilt state rather than merely reporting that it had.
func replayHeadBucketStatus(t *testing.T, ts *emulator.TestServer, bucket string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, ts.URL+"/"+bucket, nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

// replaySetFault installs a single always-firing error rule, or clears every rule
// when rule is nil. Faults are evaluated in the server pipeline, which a replay
// does not enter — which is what makes a fault-refused event replay as a success.
func replaySetFault(t *testing.T, ts *emulator.TestServer, rule *emulator.FaultRule) {
	t.Helper()
	cfg := emulator.FaultConfig{Enabled: rule != nil}
	if rule != nil {
		cfg.Rules = []emulator.FaultRule{*rule}
	}
	body, err := json.Marshal(cfg)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/v1/fault/rules", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "POST /v1/fault/rules")
}

// replayDifferencesOn returns the recorded differences whose Field is field.
func replayDifferencesOn(results *emulator.ReplayResults, field string) []*emulator.EventDifference {
	var out []*emulator.EventDifference
	for _, d := range results.Differences {
		if d.Field == field {
			out = append(out, d)
		}
	}
	return out
}

// TestReplayReporting_ASkippedEventIsNotCountedAsASuccess is the headline defect:
// a stream recorded on a default test server carries no request on any event, so
// every event is skipped, and every skip used to increment SuccessEvents as well.
//
// The assertion that matters is SuccessEvents == 0 with SkippedEvents == total.
// Before #833 this read success == skipped == total, which is what let
// test/e2e/journey_replay_test.go assert FailedEvents == 0 and pass having
// executed nothing.
func TestReplayReporting_ASkippedEventIsNotCountedAsASuccess(t *testing.T) {
	ts := emulator.StartTestServer(t)

	replayPutBucket(t, ts, "skip-accounting")
	replayPutObject(t, ts, "skip-accounting", "a.txt", "one")

	results, err := replayEngineFor(ts, emulator.ReplayConfig{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	require.GreaterOrEqual(t, results.TotalEvents, 2, "both calls should be recorded")
	assert.Equal(t, results.TotalEvents, results.SkippedEvents,
		"no event carries a request without WithRecordedBodies, so all of them are skipped")
	assert.Zero(t, results.SuccessEvents,
		"a skipped event was never executed and must not be reported as a successful replay")
	assert.Zero(t, results.FailedEvents)
	assert.Equal(t, results.TotalEvents,
		results.SuccessEvents+results.FailedEvents+results.SkippedEvents,
		"the three counters must partition the stream")
}

// TestReplayReporting_RecordedBodiesAreActuallyReExecuted is the other half: with
// bodies recorded the events replay, and the proof is state rather than a counter.
//
// Replay resets state before re-executing (ReplayEngine.resetState), so a bucket
// that is readable *after* the replay can only have been created by the replay
// itself. A counter cannot make that claim — which is the whole reason the flagship
// e2e test passed against a replay that executed nothing.
func TestReplayReporting_RecordedBodiesAreActuallyReExecuted(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	const bucket = "executed-for-real"
	replayPutBucket(t, ts, bucket)
	replayPutObject(t, ts, bucket, "a.txt", "one")

	results, err := replayEngineFor(ts, emulator.ReplayConfig{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	require.GreaterOrEqual(t, results.TotalEvents, 2)
	assert.Zero(t, results.SkippedEvents, "every recorded event carries a request")
	assert.Zero(t, results.FailedEvents)
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)

	assert.Equal(t, http.StatusOK, replayHeadBucketStatus(t, ts, bucket),
		"the replay wiped state and re-created the bucket; a skipped replay would leave it absent")
}

// recordRefusedGetObject records a stream whose GetObject was refused by a fault
// and whose fault is then cleared, so replaying it re-executes a request that now
// succeeds. It returns the stream's server.
//
// Fault injection is the cheapest way to record a *pre-plugin* refusal: the fault
// check is step 4.5 of the server pipeline and a replay enters at step 5, so the
// refusal cannot happen again. Every such refusal records with a nil response
// (server.go:849), which is exactly the shape the old status comparison could not
// see.
func recordRefusedGetObject(t *testing.T, opts ...emulator.TestServerOption) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, opts...)

	const bucket = "refused-then-allowed"
	replayPutBucket(t, ts, bucket)
	replayPutObject(t, ts, bucket, "a.txt", "one")

	replaySetFault(t, ts, &emulator.FaultRule{
		Service:     "s3",
		Operation:   "GetObject",
		FaultType:   "error",
		ErrorCode:   "ServiceUnavailable",
		HTTPStatus:  http.StatusServiceUnavailable,
		ErrorMsg:    "Reduce your request rate.",
		Probability: 1.0,
	})
	require.Equal(t, http.StatusServiceUnavailable, replayGetObjectStatus(t, ts, bucket, "a.txt"),
		"the seeded fault must refuse the read, or the recording is not of a refusal")

	// Clear the rules so the *live* server would allow the read too. The replay
	// bypasses fault injection either way; clearing keeps the test honest about why
	// the replay succeeds.
	replaySetFault(t, ts, nil)
	require.Equal(t, http.StatusOK, replayGetObjectStatus(t, ts, bucket, "a.txt"))

	return ts
}

// TestReplayReporting_ARecordedRefusalReplayingAsSuccessIsCritical asserts the
// second defect: a recorded error whose replay succeeds is reported.
//
// Before #833 this produced zero differences. The error comparison sat inside the
// `if err != nil` arm, so a successful replay never consulted event.Error; and the
// status comparison could not stand in for it, because a refusal records
// Response = nil and the comparison is guarded on Response being non-nil.
func TestReplayReporting_ARecordedRefusalReplayingAsSuccessIsCritical(t *testing.T) {
	ts := recordRefusedGetObject(t, emulator.WithRecordedBodies())

	results, err := replayEngineFor(ts, emulator.ReplayConfig{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	errDiffs := replayDifferencesOn(results, "error")
	require.Len(t, errDiffs, 1,
		"exactly one recorded event errored, and its replay succeeded, so exactly one difference")
	assert.Equal(t, "critical", errDiffs[0].Significance,
		"a replay that allows what the recording refused is the divergence that makes a test meaningless")
	assert.Contains(t, errDiffs[0].Expected, "ServiceUnavailable",
		"the recorded error is what was expected")
	assert.Nil(t, errDiffs[0].Actual, "the replay produced no error at all")

	// It executed, so it is a success by the counter's own definition — a success
	// carrying a critical difference, which is why SuccessEvents alone is not a
	// verdict on whether the replay reproduced the run.
	assert.Zero(t, results.FailedEvents)
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)
}

// TestReplayReporting_ASuccessfulReplayReportsNoErrorDifference is the negative
// twin of the test above, and it is what makes that one non-vacuous: the same
// workload with nothing refused must report no `error` difference at all.
//
// Without this, a renderer that appended an error difference unconditionally would
// pass the refusal test.
func TestReplayReporting_ASuccessfulReplayReportsNoErrorDifference(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	const bucket = "nothing-refused"
	replayPutBucket(t, ts, bucket)
	replayPutObject(t, ts, bucket, "a.txt", "one")
	require.Equal(t, http.StatusOK, replayGetObjectStatus(t, ts, bucket, "a.txt"))

	results, err := replayEngineFor(ts, emulator.ReplayConfig{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Empty(t, replayDifferencesOn(results, "error"),
		"no recorded event errored, so nothing diverged on the error field")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)
}

// TestReplayReporting_StateHashesAreRecorded asserts the third defect at the
// recorder: nothing ever wrote StateHashBefore or StateHashAfter, so both replay
// comparisons were guarded on a field that was always "".
//
// Asserted on the stored events rather than through a replay, because that is where
// the value was missing. The before/after inequality on a mutating call is what
// distinguishes a real hash from a constant.
func TestReplayReporting_StateHashesAreRecorded(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes())

	replayPutBucket(t, ts, "hashed")

	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	var mutated int
	for _, ev := range events {
		assert.NotEmpty(t, ev.StateHashBefore, "%s carries no before-hash", ev.Operation)
		assert.NotEmpty(t, ev.StateHashAfter, "%s carries no after-hash", ev.Operation)
		if ev.StateHashBefore != ev.StateHashAfter {
			mutated++
		}
	}
	assert.Positive(t, mutated,
		"CreateBucket changed state, so its two hashes must differ — equal hashes on every "+
			"event would mean a constant, not a hash")
}

// TestReplayReporting_StateHashesAreAbsentWhenNotRequested is the negative twin:
// the store's config is the authority, and a server that did not ask for hashes
// records none. It is what keeps the default cheap — a hash is a full state
// snapshot, taken twice per request.
func TestReplayReporting_StateHashesAreAbsentWhenNotRequested(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	assert.False(t, ts.Store().RecordsStateHashes())

	replayPutBucket(t, ts, "unhashed")

	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	for _, ev := range events {
		assert.Empty(t, ev.StateHashBefore, "%s", ev.Operation)
		assert.Empty(t, ev.StateHashAfter, "%s", ev.Operation)
	}
}

// TestReplayReporting_ValidateStateReportsADivergence asserts the third defect at
// the comparison: with hashes recorded, ValidateState now actually runs and reports
// a state divergence.
//
// The divergence is produced deliberately and is the same scenario as the refusal
// test: the recorded GetObject was refused and changed nothing, while the replayed
// GetObject succeeds. Any state the replay reaches from there differs from what was
// recorded, so the after-hash cannot match.
//
// This is the assertion the feature never had. Before #833 ValidateState was inert
// on every recorded stream, and the only coverage of the comparison used a
// hand-built event carrying a hash the recorder would never have written.
func TestReplayReporting_ValidateStateReportsADivergence(t *testing.T) {
	ts := recordRefusedGetObject(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.False(t, results.StateValid, "the replay reached a state the recording never had")
	assert.NotEmpty(t, append(
		replayDifferencesOn(results, "state_hash_before"),
		replayDifferencesOn(results, "state_hash_after")...,
	), "a state hash comparison must have run and reported the divergence")
}

// TestReplayReporting_ValidateStateIsSilentWithoutRecordedHashes is why the
// previous test proves something: with no hashes on the events, ValidateState has
// nothing to compare and reports nothing — which was the *only* behavior available
// before the recorder started writing them, on every stream, regardless of config.
func TestReplayReporting_ValidateStateIsSilentWithoutRecordedHashes(t *testing.T) {
	ts := recordRefusedGetObject(t, emulator.WithRecordedBodies())

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.True(t, results.StateValid, "nothing was compared, so nothing can be reported invalid")
	assert.Empty(t, replayDifferencesOn(results, "state_hash_before"))
	assert.Empty(t, replayDifferencesOn(results, "state_hash_after"))
}
