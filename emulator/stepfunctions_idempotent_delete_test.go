package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DeleteStateMachine and DeleteActivity refused an absent resource with a code neither operation
// publishes (#995).
//
// API_DeleteStateMachine lists exactly two errors, InvalidArn/400 and ValidationException/400, and
// API_DeleteActivity lists exactly one, InvalidArn/400. StateMachineDoesNotExist and
// ActivityDoesNotExist are real Step Functions codes — published at DescribeStateMachine,
// UpdateStateMachine, StartExecution, StartSyncExecution, ListExecutions and DescribeActivity — just not
// at these two. Substrate answered them because both deletes went through the same require* helper as
// their five and one siblings, so the code came along with the lookup.
//
// Reading the omission as idempotence is substrate's reading, and weaker evidence than SNS's #992 where
// API_DeleteTopic states the property outright. What carries it: the error list is the only thing either
// page says on the matter, and the description makes an idempotent delete the behavior a caller needs —
// "This is an asynchronous operation. It sets the state machine's status to DELETING and begins the
// deletion process. A state machine is deleted only when all its executions are completed." A caller
// that has issued a delete and retries cannot distinguish "already gone" from "still DELETING".
//
// The tests below are the three shapes a retried or speculative delete takes, plus the boundary: a 200
// for an ARN that names nothing is not a 200 for a string that is not the right kind of ARN.

// TestSFNDelete_ASecondDeleteOfTheSameResourceSucceeds is the retry a caller actually writes.
//
// The state after the second call matters as much as its status: a no-op delete must leave the name
// index alone, because the index and the record come off together on the first call and a second pass
// over an index that no longer holds the name must not corrupt it.
func TestSFNDelete_ASecondDeleteOfTheSameResourceSucceeds(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)
	keptARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "invoices", "STANDARD",
		sfnArnPassDefinition)

	for attempt := 1; attempt <= 3; attempt++ {
		status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "DeleteStateMachine",
			map[string]any{"stateMachineArn": smARN})
		assert.Emptyf(t, errCode, "DeleteStateMachine attempt %d: %s", attempt, raw)
		assert.Equalf(t, http.StatusOK, status, "DeleteStateMachine attempt %d: %s", attempt, raw)
		assert.Equalf(t, "{}", raw, "attempt %d: AWS documents an empty body", attempt)
	}

	// The record is gone and the sibling is untouched, by ListStateMachines rather than by Describe:
	// the index is the half a repeated delete could have damaged.
	listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListStateMachines", map[string]any{})
	assert.NotContains(t, listed, smARN, "a deleted state machine is still in the index")
	assert.Contains(t, listed, keptARN, "a repeated delete removed a name it was not asked about")

	status, errCode, _ := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	assert.Equal(t, "StateMachineDoesNotExist", errCode,
		"Describe still refuses: only the delete is idempotent")
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestSFNDelete_ASecondDeleteOfTheSameActivitySucceeds is the same for an activity, whose page publishes
// one error rather than two.
func TestSFNDelete_ASecondDeleteOfTheSameActivitySucceeds(t *testing.T) {
	ts := sfnArnServer(t)

	actARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")
	keptARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "approver")

	for attempt := 1; attempt <= 3; attempt++ {
		status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "DeleteActivity",
			map[string]any{"activityArn": actARN})
		assert.Emptyf(t, errCode, "DeleteActivity attempt %d: %s", attempt, raw)
		assert.Equalf(t, http.StatusOK, status, "DeleteActivity attempt %d: %s", attempt, raw)
		assert.Equalf(t, "{}", raw, "attempt %d: AWS documents an empty body", attempt)
	}

	listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListActivities", map[string]any{})
	assert.NotContains(t, listed, actARN, "a deleted activity is still in the index")
	assert.Contains(t, listed, keptARN, "a repeated delete removed a name it was not asked about")

	status, errCode, _ := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
		map[string]any{"activityArn": actARN})
	assert.Equal(t, "ActivityDoesNotExist", errCode,
		"Describe still refuses: only the delete is idempotent")
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestSFNDelete_ANeverCreatedNameSucceeds covers the delete of something that never existed, which is
// what a teardown running before its create ever ran looks like.
//
// It also covers the cross-account and cross-Region shapes in the same table, because those are the
// same fact from the emulator's side: after #912 an ARN in another account or Region builds a state key
// nothing is stored at, so all three are "names nothing" and all three must answer alike.
func TestSFNDelete_ANeverCreatedNameSucceeds(t *testing.T) {
	ts := sfnArnServer(t)

	ownSM := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)
	ownActivity := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")

	arnFor := func(account, region, kind, name string) string {
		return "arn:aws:states:" + region + ":" + account + ":" + kind + ":" + name
	}

	cases := map[string]struct {
		op     string
		member string
		arn    string
	}{
		"state machine never created": {
			"DeleteStateMachine", "stateMachineArn",
			arnFor(taggingTestAccount, sfnArnEastRegion, "stateMachine", "never-existed"),
		},
		"state machine in another Region": {
			"DeleteStateMachine", "stateMachineArn",
			arnFor(taggingTestAccount, sfnArnWestRegion, "stateMachine", "orders"),
		},
		"state machine in another account": {
			"DeleteStateMachine", "stateMachineArn",
			arnFor(sfnArnOtherAccount, sfnArnEastRegion, "stateMachine", "orders"),
		},
		"activity never created": {
			"DeleteActivity", "activityArn",
			arnFor(taggingTestAccount, sfnArnEastRegion, "activity", "never-existed"),
		},
		"activity in another Region": {
			"DeleteActivity", "activityArn",
			arnFor(taggingTestAccount, sfnArnWestRegion, "activity", "reviewer"),
		},
		"activity in another account": {
			"DeleteActivity", "activityArn",
			arnFor(sfnArnOtherAccount, sfnArnEastRegion, "activity", "reviewer"),
		},
	}

	for label, tc := range cases {
		t.Run(label, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, tc.op,
				map[string]any{tc.member: tc.arn})
			assert.Emptyf(t, errCode, "%s %s: %s", tc.op, tc.arn, raw)
			assert.Equalf(t, http.StatusOK, status, "%s %s: %s", tc.op, tc.arn, raw)
		})
	}

	// Both of the caller's own same-named resources survive every one of those calls. This is the
	// assertion the 200 costs: before #995 the refusal itself proved the delete had not acted, and now
	// nothing about the status does.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": ownSM})
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
		map[string]any{"activityArn": ownActivity})
}

// TestSFNDelete_IdempotenceDoesNotSwallowAShapeRefusal is the boundary.
//
// Idempotence licenses an ARN that names nothing, not a string that is not the right kind of ARN. The
// parse therefore stays ahead of the load in both handlers, and each case below would answer 200 if it
// did not — which is the specific way this change could have gone wrong, since the whole edit was to
// stop returning an error from a lookup.
//
// The shared tables in stepfunctions_arn_test.go already iterate both deletes over ten malformed shapes,
// every wrong resource type and the version and alias forms. These four cases are here so the reason is
// stated where the idempotence is.
func TestSFNDelete_IdempotenceDoesNotSwallowAShapeRefusal(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)
	actARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")

	cases := map[string]struct {
		op     string
		member string
		arn    string
	}{
		"malformed at the state-machine delete": {"DeleteStateMachine", "stateMachineArn", "orders"},
		"an activity ARN at the state-machine delete": {
			"DeleteStateMachine", "stateMachineArn", actARN,
		},
		"a version-qualified ARN at the state-machine delete": {
			"DeleteStateMachine", "stateMachineArn", smARN + ":1",
		},
		"a state-machine ARN at the activity delete": {"DeleteActivity", "activityArn", smARN},
	}

	for label, tc := range cases {
		t.Run(label, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, tc.op,
				map[string]any{tc.member: tc.arn})
			assert.Equalf(t, "InvalidArn", errCode, "%s %s: %s", tc.op, tc.arn, raw)
			assert.Equalf(t, http.StatusBadRequest, status, "%s %s", tc.op, tc.arn)
		})
	}

	// Neither refusal was a write.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
		map[string]any{"activityArn": actARN})
}

// TestSFNDelete_TheDeletingStatusIsNotModeled records the other half of #995's decision.
//
// API_DescribeStateMachine publishes two status values, ACTIVE and DELETING, and StateMachineState's own
// doc comment claimed both — while no writer anywhere set DELETING. Nothing can, because the delete
// removes the record synchronously: there is no observation between ACTIVE and gone for a DELETING to
// occupy. So a caller cannot test a poll loop that waits for a delete to finish here, and
// StateMachineDeleting/400 — published at UpdateStateMachine, StartExecution and StartSyncExecution — is
// unreachable for the same reason.
//
// The test asserts the emulator's actual answer rather than the documented enum, so that whoever models
// the transition finds a failing assertion naming this decision rather than a silent widening.
func TestSFNDelete_TheDeletingStatusIsNotModeled(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)

	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	require.Equal(t, "ACTIVE", sfnArnMember(t, raw, "status"),
		"the only status substrate reports")

	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DeleteStateMachine",
		map[string]any{"stateMachineArn": smARN})

	// Immediately after the delete — no clock advance, no second observation. The record is gone rather
	// than DELETING, which is what makes the transition unobservable.
	status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	assert.Equal(t, "StateMachineDoesNotExist", errCode, raw)
	assert.Equal(t, http.StatusBadRequest, status)

	// And StartExecution against it answers the same, not StateMachineDeleting.
	status, errCode, raw = sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "StartExecution",
		map[string]any{"stateMachineArn": smARN, "name": "after-delete"})
	assert.Equal(t, "StateMachineDoesNotExist", errCode,
		"StateMachineDeleting is unreachable while the delete is synchronous")
	assert.Equal(t, http.StatusBadRequest, status, raw)
}
