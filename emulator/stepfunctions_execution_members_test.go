package emulator_test

// The response members StartSyncExecution and DescribeExecution publish and neither answered
// (#1071).
//
// StartSyncExecution answered five of the fourteen members its Response Syntax publishes. The pair
// that mattered most is error/cause, because AWS publishes this operation's failure contract as
// "StartSyncExecution will return a 200 OK response, even if your execution fails, because the
// status code in the API response doesn't reflect function errors" — so the body is the only place
// a failure can be reported, and a consumer testing a failure path saw "status":"FAILED" and
// nothing about why. DescribeExecution had the same blind spot on the operation a consumer polls.
//
// Every assertion here reads the raw body, because an absent member and an empty one are different
// answers and a decoded struct cannot tell them apart (#765) — which is the whole point of the
// criterion that error and cause stay *absent* on a succeeded execution.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// sfnMembersInput is the execution input, chosen non-empty so `input` and — through a Pass
	// state — `output` are both present and their details members can be asserted alongside them.
	sfnMembersInput = `{"who":"alejandro"}`

	// sfnMembersFailDefinition fails with a published error code and cause, which is the fixture
	// nothing in the tree had: every existing execution test succeeds.
	sfnMembersFailDefinition = `{"StartAt":"Boom","States":{"Boom":{"Type":"Fail",` +
		`"Error":"MyError","Cause":"the reason it failed"}}}`

	// sfnMembersColonDefinition carries a ": " inside the *error code*, which is legal — a Fail
	// state's Error is caller-supplied — and is exactly the input the old joined ErrorDetails
	// string could not be split back apart.
	sfnMembersColonDefinition = `{"StartAt":"Boom","States":{"Boom":{"Type":"Fail",` +
		`"Error":"Outer: Inner","Cause":"a: cause: with: colons"}}}`
)

// sfnMembersDecode decodes a response body into a member map.
func sfnMembersDecode(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &out), "decode %s", raw)
	return out
}

// sfnMembersRequireAbsent fails unless every named member is missing from the body entirely.
func sfnMembersRequireAbsent(t *testing.T, raw string, members ...string) {
	t.Helper()
	out := sfnMembersDecode(t, raw)
	for _, member := range members {
		_, present := out[member]
		assert.Falsef(t, present, "%q must be absent, not empty, in %s", member, raw)
	}
}

// sfnMembersDataDetails asserts one details member is the published
// CloudWatchEventsExecutionDataDetails shape.
//
// `included` is the page's own value rather than a derivation: "Indicates whether input or output
// was included in the response. Always true for API calls." The type publishes no other member,
// so the whole object is asserted.
func sfnMembersDataDetails(t *testing.T, raw, member string) {
	t.Helper()
	out := sfnMembersDecode(t, raw)
	details, ok := out[member].(map[string]any)
	require.Truef(t, ok, "no %q object in %s", member, raw)
	assert.Equalf(t, map[string]any{"included": true}, details, "%q in %s", member, raw)
}

// sfnMembersSync creates an EXPRESS state machine with one definition and returns the raw
// StartSyncExecution body.
func sfnMembersSync(t *testing.T, name, definition string) string {
	t.Helper()
	ts := sfnArnServer(t)
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, name, "EXPRESS", definition)
	return sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartSyncExecution", map[string]any{
		"stateMachineArn": smARN,
		"name":            name + "-run",
		"input":           sfnMembersInput,
	})
}

// sfnMembersDescribe creates a STANDARD state machine, starts an execution and returns the raw
// DescribeExecution body.
func sfnMembersDescribe(t *testing.T, name, definition string) string {
	t.Helper()
	ts := sfnArnServer(t)
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, name, "STANDARD", definition)
	started := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartExecution", map[string]any{
		"stateMachineArn": smARN,
		"name":            name + "-run",
		"input":           sfnMembersInput,
	})
	return sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution", map[string]any{
		"executionArn": sfnArnMember(t, started, "executionArn"),
	})
}

// TestSFNSyncExecution_ASucceededExecutionAnswersTwelveOfFourteen asserts every member substrate
// now reports, and the four it must not.
//
// The two absences that are decisions rather than gaps are `billingDetails` and `traceHeader`:
// billing reports the metering of a workload substrate does not run — `billedMemoryUsedInMB` is
// memory consumed inside the execution, and a duration measured on the simulated clock would be 0
// for a sync execution that completes inside one handler — and `traceHeader` echoes a request
// member no path decodes, with a published header-wins-over-body rule that makes answering it a
// matter of modeling X-Ray rather than adding a field.
func TestSFNSyncExecution_ASucceededExecutionAnswersTwelveOfFourteen(t *testing.T) {
	raw := sfnMembersSync(t, "sfn-members-ok", sfnArnPassDefinition)

	out := sfnMembersDecode(t, raw)
	for _, member := range []string{"executionArn", "stateMachineArn", "name", "input", "output",
		"inputDetails", "outputDetails", "startDate", "stopDate", "status"} {
		assert.Containsf(t, out, member, "%q is published and must be answered: %s", member, raw)
	}
	assert.Equal(t, "SUCCEEDED", sfnArnMember(t, raw, "status"), raw)
	assert.Equal(t, "sfn-members-ok-run", sfnArnMember(t, raw, "name"), raw)
	assert.Contains(t, sfnArnMember(t, raw, "stateMachineArn"), ":stateMachine:sfn-members-ok", raw)
	assert.JSONEq(t, sfnMembersInput, sfnArnMember(t, raw, "input"), raw)
	sfnMembersDataDetails(t, raw, "inputDetails")
	sfnMembersDataDetails(t, raw, "outputDetails")

	// error and cause carry no meaning on a succeeded execution, so they are absent rather than
	// empty strings; billingDetails and traceHeader are the two deliberate divergences.
	sfnMembersRequireAbsent(t, raw, "error", "cause", "billingDetails", "traceHeader")
}

// TestSFNSyncExecution_AFailedExecutionReportsErrorAndCause covers the pair a consumer could not
// observe at all.
//
// `output` goes absent with it, which is the published rule for that member: "This field is set
// only if the execution succeeds. If the execution fails, this field is null." So `outputDetails`
// goes with it — details about a payload the body does not carry would describe nothing.
func TestSFNSyncExecution_AFailedExecutionReportsErrorAndCause(t *testing.T) {
	raw := sfnMembersSync(t, "sfn-members-fail", sfnMembersFailDefinition)

	assert.Equal(t, "FAILED", sfnArnMember(t, raw, "status"), raw)
	assert.Equal(t, "MyError", sfnArnMember(t, raw, "error"), raw)
	assert.Equal(t, "the reason it failed", sfnArnMember(t, raw, "cause"), raw)
	sfnMembersDataDetails(t, raw, "inputDetails")
	sfnMembersRequireAbsent(t, raw, "output", "outputDetails")
}

// TestSFNSyncExecution_AnErrorCodeCarryingAColonSurvivesWhole is why the two members are recorded
// separately instead of derived from one string.
//
// A Fail state's `Error` is caller-supplied, so it may contain ": ". The old
// `ErrorCode + ": " + Cause` join was therefore already lossy: no split of it can tell the tail of
// the caller's error code from the head of the cause. Two fields can.
func TestSFNSyncExecution_AnErrorCodeCarryingAColonSurvivesWhole(t *testing.T) {
	raw := sfnMembersSync(t, "sfn-members-colon", sfnMembersColonDefinition)

	assert.Equal(t, "Outer: Inner", sfnArnMember(t, raw, "error"), raw)
	assert.Equal(t, "a: cause: with: colons", sfnArnMember(t, raw, "cause"), raw)
}

// TestSFNSyncExecution_StatusIsOneOfTheThreePublishedValues pins the narrower Valid Values.
//
// API_StartSyncExecution publishes `SUCCEEDED | FAILED | TIMED_OUT`, where
// `ExecutionState.Status` holds one of four and `RUNNING` is what a new execution is initialized
// to — so a path that neither succeeded nor called aslFail would answer a value this operation
// does not publish.
func TestSFNSyncExecution_StatusIsOneOfTheThreePublishedValues(t *testing.T) {
	for label, definition := range map[string]string{
		"a definition that succeeds": sfnArnPassDefinition,
		"a definition that fails":    sfnMembersFailDefinition,
	} {
		t.Run(label, func(t *testing.T) {
			raw := sfnMembersSync(t, "sfn-members-status", definition)
			assert.Contains(t, []string{"SUCCEEDED", "FAILED", "TIMED_OUT"},
				sfnArnMember(t, raw, "status"), raw)
		})
	}
}

// TestSFNDescribeExecution_AFailedStandardExecutionReportsErrorAndCause covers the operation the
// issue scopes out and one split serves anyway.
//
// A standard execution recorded its failure reason and then reported it nowhere, on the operation a
// consumer's wait loop actually polls. The four members are published identically on both pages, at
// the same length bounds.
func TestSFNDescribeExecution_AFailedStandardExecutionReportsErrorAndCause(t *testing.T) {
	raw := sfnMembersDescribe(t, "sfn-members-std-fail", sfnMembersFailDefinition)

	assert.Equal(t, "FAILED", sfnArnMember(t, raw, "status"), raw)
	assert.Equal(t, "MyError", sfnArnMember(t, raw, "error"), raw)
	assert.Equal(t, "the reason it failed", sfnArnMember(t, raw, "cause"), raw)
	sfnMembersDataDetails(t, raw, "inputDetails")
	sfnMembersRequireAbsent(t, raw, "output", "outputDetails")
}

// TestSFNDescribeExecution_ASucceededStandardExecutionOmitsTheFailurePair is the mirror, so the
// two members are known to be conditional rather than always present.
func TestSFNDescribeExecution_ASucceededStandardExecutionOmitsTheFailurePair(t *testing.T) {
	raw := sfnMembersDescribe(t, "sfn-members-std-ok", sfnArnPassDefinition)

	assert.Equal(t, "SUCCEEDED", sfnArnMember(t, raw, "status"), raw)
	sfnMembersDataDetails(t, raw, "inputDetails")
	sfnMembersDataDetails(t, raw, "outputDetails")
	sfnMembersRequireAbsent(t, raw, "error", "cause")
}

// TestSFNExecution_APreSplitRecordStillReportsItsFailure covers the state-shape fallback.
//
// ErrorDetails was a single joined string, and a record persisted by a substrate older than #1071 —
// or replayed from an event log written by one — still carries it with the two new fields empty.
// Splitting on the first ": " recovers the pair for every failure substrate itself wrote, which is
// the whole reason the fallback is worth having rather than dropping those records' reasons.
func TestSFNExecution_APreSplitRecordStillReportsItsFailure(t *testing.T) {
	ts := sfnArnServer(t)
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "sfn-members-legacy",
		"STANDARD", sfnMembersFailDefinition)
	started := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartExecution", map[string]any{
		"stateMachineArn": smARN,
		"name":            "legacy-run",
	})
	execARN := sfnArnMember(t, started, "executionArn")

	// Rewrite the stored record into the shape an older substrate wrote: the joined string, and
	// neither of the fields that replaced it.
	state := ts.StateManager()
	key := "execution:" + taggingTestAccount + "/" + sfnArnEastRegion + "/sfn-members-legacy/legacy-run"
	rawState, err := state.Get(t.Context(), "states", key)
	require.NoError(t, err)
	require.NotNilf(t, rawState, "no execution record at %s", key)

	var exec map[string]any
	require.NoError(t, json.Unmarshal(rawState, &exec))
	delete(exec, "ErrorCode")
	delete(exec, "ErrorCause")
	exec["ErrorDetails"] = "MyError: the reason it failed"
	patched, err := json.Marshal(exec)
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "states", key, patched))

	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution", map[string]any{
		"executionArn": execARN,
	})
	assert.Equal(t, "MyError", sfnArnMember(t, raw, "error"), raw)
	assert.Equal(t, "the reason it failed", sfnArnMember(t, raw, "cause"), raw)
}
