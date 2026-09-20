package emulator_test

// What a replay re-executes before it reaches a plugin (#833).
//
// The server runs four controllers between parsing a request and dispatching it —
// authorization, quota, consistency and fault injection — and a replay ran none of
// them: ReplayEngine.replayEvent called PluginRegistry.RouteRequest directly. So a
// request the recording refused with a 403 or an injected 500 was re-executed on
// replay and *succeeded*, applying a state change the recorded run never made.
//
// The half of the contract that already existed makes the gap unmistakable:
// QuotaController.CheckQuota and ConsistencyController.CheckRead each open with an
// isReplaying guard, and replayEvent has always set the flag those guards read — the
// two ends were written and never joined.
//
// Every test here records over the wire through a [emulator.TestServer] and replays
// that stream, because the defect is in the relationship between what the server
// refuses and what the replay re-decides. Two of them assert on *state* after the
// replay rather than on a counter: a replay resets state before re-executing, so a
// bucket readable afterwards can only have been created by the replay, and a bucket
// the recording's refusal prevented must still be absent.
//
// The gates the replay path is *expected to exempt* are asserted through
// [emulator.CheckReplayGatesForTest] instead. A replay that reproduced a stream
// without a quota refusal could be exempting the request or never reaching the
// controller at all, and those are the two states this issue found indistinguishable.

import (
	"bytes"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// pipelineReplayEngine builds a replay engine over the test server's own store,
// state, clock and registry, handed the pipeline it should re-decide with.
//
// The controllers are the server's own, which is the arrangement the CLI produces
// (newReplayEngineWiring builds all four from the same config sections `substrate
// server` reads): AuthController reads policies out of the StateManager, so a replay
// pointed at a different one would disagree with its recording for a reason that is
// not a divergence.
func pipelineReplayEngine(ts *emulator.TestServer, pipeline emulator.ReplayPipeline) *emulator.ReplayEngine {
	return emulator.NewReplayEngine(
		ts.Store(),
		ts.StateManager(),
		ts.TimeController(),
		ts.Registry(),
		emulator.ReplayConfig{},
		emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.WithReplayPipeline(pipeline),
		// Given unconditionally: a seed in the recording is re-applied in position
		// (#1140), and a stream with no seed in it is unaffected — so every replay test
		// gets the wiring a consumer replaying a live server's stream would have.
		emulator.WithControlPlaneHandler(ts.ControlPlaneHandler()),
	)
}

// pipelineIAMCall issues an unsigned IAM query-protocol call and returns the body.
//
// Unsigned on purpose: the setup calls must not themselves be authorized, or a user
// with no policies could not create its own access key. Enforcement is opt-in by
// resolving to an IAM entity, and an unsigned request resolves to no principal.
func pipelineIAMCall(t *testing.T, ts *emulator.TestServer, operation string, params map[string]string) []byte {
	t.Helper()

	form := url.Values{}
	form.Set("Action", operation)
	form.Set("Version", "2010-05-08")
	for k, v := range params {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		bytes.NewReader([]byte(form.Encode())))
	require.NoError(t, err)
	req.Host = "iam.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", operation, body)
	return body
}

// pipelineCreateUserWithKey creates an IAM user holding no policies and returns the
// access key ID minted for it. A user with no policies is denied every action by
// implicit deny, which is the cheapest recording of an authorization refusal.
func pipelineCreateUserWithKey(t *testing.T, ts *emulator.TestServer, userName string) string {
	t.Helper()

	pipelineIAMCall(t, ts, "CreateUser", map[string]string{"UserName": userName})
	body := pipelineIAMCall(t, ts, "CreateAccessKey", map[string]string{"UserName": userName})

	var parsed struct {
		AccessKeyID string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed))
	require.NotEmpty(t, parsed.AccessKeyID, "CreateAccessKey minted no key: %s", body)
	return parsed.AccessKeyID
}

// pipelineSignedPutBucket issues CreateBucket as the holder of accessKeyID and
// returns the status and body.
//
// The signature is the literal string "unverified": a test server does not verify
// signatures unless asked to, so what the access key does here is resolve the
// principal — which is the input authorization runs on (server.go step 1.5).
func pipelineSignedPutBucket(t *testing.T, ts *emulator.TestServer, bucket, accessKeyID string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+"/"+bucket, nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKeyID+
		"/20250101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=unverified")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, body
}

// TestReplayPipeline_ReproducesAnAuthorizationRefusal is the headline case: a
// recorded 403 replays as a 403, and the bucket the refusal prevented is still
// absent afterwards.
//
// It is also the test that settles what #833 was thought to be blocked on. The
// recorded Authorization header names an access key that a replay re-mints
// *differently* (#856), so re-resolving the key would find nothing and leave the
// request unenforced. Nothing in the authorization path reads the key: CheckAccess
// resolves the caller with resolveIAMEntity(…, Principal.ARN), and an IAM user's ARN
// is derived from the name CreateUser was given — so it survives a replay where the
// key does not. Event.Principal records exactly that ARN.
//
// Before the fix a replay carried no principal at all, so this stream replayed with
// every authorization door open — the pipeline's and each plugin's own — and the
// bucket existed afterwards.
func TestReplayPipeline_ReproducesAnAuthorizationRefusal(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	const (
		userName = "replay-denied"
		bucket   = "denied-by-policy"
	)
	keyID := pipelineCreateUserWithKey(t, ts, userName)

	status, body := pipelineSignedPutBucket(t, ts, bucket, keyID)
	require.Equal(t, http.StatusForbidden, status,
		"a user holding no policies must be denied, or the recording is not of a refusal: %s", body)
	require.Contains(t, string(body), "AccessDenied")
	require.Equal(t, http.StatusNotFound, replayHeadBucketStatus(t, ts, bucket),
		"the refused CreateBucket must not have created anything")

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{Auth: ts.AuthController()})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Zero(t, results.SkippedEvents, "every recorded event carries a request")
	assert.Equal(t, 1, results.FailedEvents,
		"the refused CreateBucket is the one event that returned an error; FailedEvents means that, not diverged")
	// The refusal itself diverged in nothing: same step, same message, same status.
	assert.Empty(t, replayDifferencesOn(results, "error"),
		"the replay refused the same request the recording refused")
	assert.Empty(t, replayDifferencesOn(results, "error_message"),
		"the replay refused it with the same message, which is what makes the ARN-based principal load-bearing")
	assert.Empty(t, replayDifferencesOn(results, "status_code"),
		"no event answered a different status")

	// What does diverge is the two earlier events' bodies, and it is the honest
	// report of #856 rather than a failure of this replay: CreateUser's UserId and
	// CreateAccessKey's AccessKeyId and SecretAccessKey are minted from crypto/rand,
	// so a replay produces three values the recording cannot predict. #817's body
	// comparison reports them by path instead of normalising them away, precisely
	// because masking a minted identifier would report a reproduced run that was not
	// one. Every difference in this stream is one of those.
	for _, diff := range results.Differences {
		assert.True(t, strings.HasPrefix(diff.Field, "response_body/"),
			"unexpected difference outside a response body: %s = %v vs %v",
			diff.Field, diff.Expected, diff.Actual)
	}

	assert.Equal(t, http.StatusNotFound, replayHeadBucketStatus(t, ts, bucket),
		"a replay that authorized nothing would have created the bucket the recording refused")
}

// TestReplayPipeline_APrincipalIsRestoredWithoutItsAccessKey asserts the mechanism
// the test above depends on, at the recorder: the refusal names the *user*, not the
// key it was made with.
//
// It is worth its own assertion because the message is what the replay compares
// against. A principal restored from anything else — the synthesized
// user/<access-key-id> ARN of a registry hit, say — would produce a different message
// and report a divergence, and #745 is explicit that a credential ID must never be
// published as aws:username.
func TestReplayPipeline_APrincipalIsRestoredWithoutItsAccessKey(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	const userName = "replay-named"
	keyID := pipelineCreateUserWithKey(t, ts, userName)
	status, _ := pipelineSignedPutBucket(t, ts, "named-in-the-denial", keyID)
	require.Equal(t, http.StatusForbidden, status)

	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)

	var refused *emulator.Event
	for _, ev := range events {
		if ev.Error != "" {
			refused = ev
		}
	}
	require.NotNil(t, refused, "the stream records no refusal")

	assert.Contains(t, refused.Principal, ":user/"+userName,
		"the event must record the caller ARN a replay restores from")
	assert.NotContains(t, refused.Principal, keyID,
		"the recorded principal must not be the access key: a replay re-mints that one")
	assert.Contains(t, refused.Error, ":user/"+userName,
		"the denial names the user, so a replay carrying any other principal reports a divergence")
}

// TestReplayPipeline_ReproducesAnInjectedFault asserts the second re-decided gate,
// and the reason the controller has to be rewound.
//
// Two buckets are recorded: one allowed, one refused by a rule bounded to a single
// firing. After the replay the allowed bucket must exist and the refused one must
// not, which is the assertion a counter cannot make — a replay resets state, so the
// absent bucket is the replay's own refusal rather than the recording's.
//
// The stream is replayed twice, and that is what pins the rewind. A rule's Times
// bound and its per-rule PRNG position are mutable state a run advances, so without
// returning the controller to its armed state at the start of each replay the rule is
// spent, the replay takes the unfaulted path, and the bucket the fault refused gets
// created — the exact divergence #833 exists to remove, arriving through the fix for
// it. It is the same class as #886/#902/#903, and the fired count is what shows the
// rewind rather than a one-off allowance: it reads 1 after each replay, never 2,
// because each replay starts from the count the rule was armed with.
func TestReplayPipeline_ReproducesAnInjectedFault(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	const (
		allowed = "fault-allowed"
		refused = "fault-refused"
	)

	ts.FaultController().UpdateConfig(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "CreateBucket",
			PathSuffix: "/" + refused,
			FaultType:  "error",
			ErrorCode:  "InternalError",
			HTTPStatus: http.StatusInternalServerError,
			ErrorMsg:   "We encountered an internal error. Please try again.",
			// One firing, which is what makes the rewind load-bearing: without it the
			// bound is spent by the recording and no replay can reproduce the refusal.
			Times: 1,
		}},
	})

	replayPutBucket(t, ts, allowed)
	require.Equal(t, http.StatusInternalServerError, pipelinePutBucketStatus(t, ts, refused),
		"the armed rule must refuse this bucket, or the recording is not of a refusal")
	require.Equal(t, 1, ts.FaultController().FaultsFired())

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{Fault: ts.FaultController()})

	// Twice, because one replay cannot distinguish a rewound controller from one that
	// happened to have a firing left. Nothing is read back between the two: a live
	// request would be recorded onto the same stream, and the second replay would then
	// be replaying a longer one.
	for _, label := range []string{"first replay", "second replay"} {
		results, err := engine.Replay(t.Context(), replayStreamID)
		require.NoError(t, err, "%s", label)

		assert.Zero(t, results.SkippedEvents, "%s", label)
		assert.Equal(t, 1, results.FailedEvents,
			"%s: one recorded event was refused and must be refused again", label)
		assert.Empty(t, results.Differences,
			"%s: the same rule refused the same request, so nothing diverged", label)

		assert.Equal(t, 1, ts.FaultController().FaultsFired(),
			"%s: the rewind restores the armed count, so each replay's own firing is the only one on it", label)
	}

	assert.Equal(t, http.StatusOK, replayHeadBucketStatus(t, ts, allowed),
		"the replay re-created the bucket the recording created")
	assert.Equal(t, http.StatusNotFound, replayHeadBucketStatus(t, ts, refused),
		"a replay that skipped fault injection would create the bucket the recording never had")
}

// pipelinePutBucketStatus issues CreateBucket unsigned and returns the status, for
// the recording of a refusal that is not authorization's.
func pipelinePutBucketStatus(t *testing.T, ts *emulator.TestServer, bucket string) int {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+"/"+bucket, nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

// TestReplayPipeline_RewindRestoresTheArmedFiredCounts asserts the rewind directly,
// including the part a replay cannot show: an armed count is *restored*, not zeroed.
//
// A rule may be armed carrying a non-zero Fired — the field is documented as
// round-tripping through GET /v1/fault/rules — so zeroing would arm a different
// config than the one handed in.
func TestReplayPipeline_RewindRestoresTheArmedFiredCounts(t *testing.T) {
	fault := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{Service: "s3", FaultType: "error", Times: -1},
			{Service: "sqs", FaultType: "error", Times: -1, Fired: 3},
		},
	}, 1)

	reqCtx := &emulator.RequestContext{}
	for range 2 {
		_, _, err := emulator.CheckReplayGatesForTest(emulator.ReplayPipeline{Fault: fault}, reqCtx,
			&emulator.AWSRequest{Service: "s3", Operation: "CreateBucket"})
		require.Error(t, err)
	}
	require.Equal(t, 5, fault.FaultsFired(), "two firings on top of the armed count of 3")

	emulator.RewindFaultsForTest(fault)

	assert.Equal(t, 3, fault.FaultsFired(),
		"the rewind returns each rule to the count it was armed with, not to zero")
}

// TestReplayPipeline_QuotaAndConsistencyAreConsultedAndExempt asserts the two gates
// whose correct answer on replay is "allowed", which is why they need asserting
// through the gate rather than through a replay.
//
// Both controllers have carried an isReplaying guard since they were written, and
// replayEvent has always set the flag — but a replay never called either one, so the
// guards were unreachable from the only path that sets the flag. A replay that
// reproduces such a stream cannot tell the two apart: an exempted request and a
// request that never reached the controller both look like success. The gate can.
//
// Consistency is exempt for a stronger reason than quota's: RecordWrite is guarded on
// the same flag, so no propagation window ever opens during a replay, and a CheckRead
// that enforced would be refusing against a window that does not exist.
func TestReplayPipeline_QuotaAndConsistencyAreConsultedAndExempt(t *testing.T) {
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	quota := emulator.NewQuotaController(emulator.QuotaConfig{
		Enabled: true,
		// An empty bucket that never refills, so the refusal does not depend on how
		// fast the test runs.
		Rules: map[string]emulator.RateRule{"s3": {Rate: 0, Burst: 0}},
	}, tc)

	consistency, err := emulator.NewConsistencyController(emulator.ConsistencyConfig{
		Enabled:          true,
		PropagationDelay: time.Minute,
		AffectedServices: []string{"s3"},
	}, tc)
	require.NoError(t, err)

	read := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "GetObject",
		Params:    map[string]string{"BucketName": "gated"},
	}
	write := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Params:    map[string]string{"BucketName": "gated"},
	}

	live := &emulator.RequestContext{}
	replaying := &emulator.RequestContext{Metadata: map[string]any{"replaying": true}}

	// The write is what opens the consistency window a live read then falls into.
	consistency.RecordWrite(live, write)

	tests := []struct {
		name     string
		pipeline emulator.ReplayPipeline
		wantStep string
		wantCode string
	}{
		// S3 answers SlowDown rather than ThrottlingException, whichever rule
		// refused — it has no ThrottlingException (#818). The rule here is the
		// service-level one, so the refusal is not accounted per prefix, which is
		// beside the point of this test: what it asserts is that the gate is
		// consulted live and exempt on replay.
		{"quota", emulator.ReplayPipeline{Quota: quota}, "quota", "SlowDown"},
		{"consistency", emulator.ReplayPipeline{Consistency: consistency}, "consistency", "InconsistentStateException"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Live first, which is what makes the exemption below non-vacuous: a
			// controller that allowed everything would satisfy the replay assertion.
			step, _, err := emulator.CheckReplayGatesForTest(tt.pipeline, live, read)
			require.Error(t, err, "the controller must refuse a live request, or it is not configured to refuse")
			assert.Equal(t, tt.wantStep, step)
			assert.Contains(t, err.Error(), tt.wantCode)

			step, _, err = emulator.CheckReplayGatesForTest(tt.pipeline, replaying, read)
			assert.NoError(t, err, "a replayed request is exempt from %s", tt.name)
			assert.Empty(t, step, "no gate refused")
		})
	}
}

// TestReplayPipeline_ALatencyFaultIsReportedButNotSlept records the one gate answer
// that is neither a refusal nor an allow: a latency rule.
//
// The gate reports the delay alongside a nil error and leaves the waiting to the
// caller, because the two callers must answer differently — the live server sleeps,
// and a replay does not, since the recorded Duration is what the live run took and no
// replay may consume wall-clock time.
func TestReplayPipeline_ALatencyFaultIsReportedButNotSlept(t *testing.T) {
	fault := emulator.NewFaultController(emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:   "s3",
			FaultType: "latency",
			LatencyMs: 250,
			Times:     -1,
		}},
	}, 1)

	step, latency, err := emulator.CheckReplayGatesForTest(
		emulator.ReplayPipeline{Fault: fault},
		&emulator.RequestContext{},
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject"},
	)

	require.NoError(t, err, "a latency rule is not a refusal: the request proceeds")
	assert.Empty(t, step, "no gate refused")
	assert.Equal(t, 250*time.Millisecond, latency)
	assert.Equal(t, 1, fault.FaultsFired(),
		"the rule's bound is spent either way, exactly as it is on the live path")
}

// TestReplayPipeline_NoPipelineLeavesAReplayUngated is the negative control for the
// whole file: NewReplayEngine's pipeline is optional, and a caller that supplies none
// gets the pre-#833 behavior rather than a nil-pointer panic.
//
// That matters because the option is variadic precisely so the four existing
// non-test call sites keep compiling, and an emulator embedded in someone's test
// harness may legitimately want to re-execute a stream without re-deciding
// authorization — the recorded refusal is then reported as a critical difference,
// which is what #857 established.
func TestReplayPipeline_NoPipelineLeavesAReplayUngated(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	const bucket = "ungated"
	keyID := pipelineCreateUserWithKey(t, ts, "replay-ungated")
	status, _ := pipelineSignedPutBucket(t, ts, bucket, keyID)
	require.Equal(t, http.StatusForbidden, status)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Zero(t, results.FailedEvents, "with no pipeline nothing re-decides the refusal")
	errDiffs := replayDifferencesOn(results, "error")
	require.Len(t, errDiffs, 1, "the recorded refusal replayed as a success, which is a divergence")
	assert.Equal(t, "critical", errDiffs[0].Significance)
	assert.Equal(t, http.StatusOK, replayHeadBucketStatus(t, ts, bucket),
		"an ungated replay creates the bucket the recording refused, which is the defect stated as behavior")
}
