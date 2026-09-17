package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Every operation that refuses a secret scheduled for deletion — #956.
//
// #953 made the scheduled state observable and refused it at GetSecretValue and the unforced
// DeleteSecret; #952 refused it at RotateSecret. Four operations still accepted a secret inside its
// recovery window, although each of their pages publishes "The secret is scheduled for deletion." as
// the first of InvalidRequestException's three possible causes: PutSecretValue attached a new version
// and moved CurrentVersionID onto it, UpdateSecret rewrote the description, the KMS key and the value,
// and TagResource and UntagResource edited the tag list. So a consumer's "is this secret usable?"
// logic got a 200 here and a 400 from AWS at four operations, and a RestoreSecret afterwards handed
// back a secret carrying writes AWS would have refused.
//
// Three assertion styles follow from that, and none substitutes for the others:
//
//  1. **The table below runs over every operation that refuses**, not a sample. The fix is one shared
//     constructor — [smSecretScheduledForDeletion] — and a table over every caller is the only thing
//     that proves no handler builds its own refusal, the arrangement #930 used for smSecretNotFound's
//     nine callers. It carries seven rows rather than the six #956 names, because #952 added
//     RotateSecret to the set between the issue being filed and this test being written.
//
//  2. **Each refusal is asserted against the state it did not write.** A status-only assertion would
//     pass against a handler that refused *after* storing, which is the half of the defect that
//     matters: the writes were already reachable, so what has to be shown is that they stopped.
//     Description, Tags and the secret value are therefore read back after every refusal.
//
//  3. **Each refusal is compared with the answer an absent secret gets.** The point of the whole
//     scheduled state is that it is neither live nor gone, so a test that only checked for "some 400"
//     would pass against a handler that answered ResourceNotFoundException — which is precisely the
//     collapse #953 exists to undo.
//
// Every call goes over the wire through [smRawCall] and every secret is created with CreateSecret, per
// #765: a helper that wrote a DeletionDate into state directly could not show that the stamp
// DeleteSecret itself writes is the one these seven read.

// smScheduledRefusers is every operation that must refuse a secret scheduled for deletion, with a body
// that is otherwise valid.
//
// Each body carries the parameters its own shape requires and nothing more, so a refusal cannot be
// mistaken for a rejected parameter. RotateSecret's includes a ClientRequestToken and a rotation
// function ARN because it validates both before it loads the secret (#952) — without them it would
// answer InvalidParameterException and this table would assert nothing about the scheduled state.
//
// DeleteSecret appears unforced only. A forced DeleteSecret against a scheduled secret is not a
// refusal: "if you forcibly delete an already deleted or nonexistent secret, the operation does not
// return ResourceNotFoundException", and #953 pinned that path separately.
var smScheduledRefusers = []struct {
	name string
	body func(secretID string) map[string]any
}{
	{"GetSecretValue", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
	{"PutSecretValue", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "SecretString": "rewritten"}
	}},
	{"UpdateSecret", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "Description": "rewritten", "SecretString": "rewritten"}
	}},
	{"DeleteSecret", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
	{"TagResource", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "Tags": []map[string]string{{"Key": "added", "Value": "later"}}}
	}},
	{"UntagResource", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "TagKeys": []string{"env"}}
	}},
	{"RotateSecret", func(id string) map[string]any {
		return map[string]any{
			"SecretId":           id,
			"ClientRequestToken": smRotationToken,
			"RotationLambdaARN":  smRotationLambda,
		}
	}},
}

// smScheduleSecret creates a secret carrying a description and one tag, schedules its deletion, and
// returns its ARN.
//
// The description and the tag are there so a refusal can be checked against something a successful call
// would have changed; the delete is the plain one, so the secret is inside a 30-day recovery window
// rather than gone.
func smScheduleSecret(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	arn := smCreateSecretIn(t, ts, smTarget, name, "value-of-"+name, map[string]string{"env": "prod"})
	status, _, code := smRawCall(t, ts, smTarget, "UpdateSecret",
		map[string]any{"SecretId": arn, "Description": "original"})
	require.Empty(t, code, "UpdateSecret sets the description while the secret is live")
	require.Equal(t, http.StatusOK, status)

	status, out, code := smDeleteSecret(t, ts, map[string]any{"SecretId": arn})
	require.Empty(t, code, "a plain DeleteSecret schedules rather than removes")
	require.Equal(t, http.StatusOK, status)
	require.NotNil(t, out.DeletionDate, "the secret is scheduled, not gone")
	return arn
}

func TestSMScheduled_EverySchedulingRefusalIsTheSameAnswer(t *testing.T) {
	ts := smTagServer(t)

	for _, op := range smScheduledRefusers {
		t.Run(op.name, func(t *testing.T) {
			// A secret per case, because a refusal that wrote something would otherwise be visible
			// only to whichever case ran after it.
			arn := smScheduleSecret(t, ts, "scheduled-"+op.name)

			status, _, code := smRawCall(t, ts, smTarget, op.name, op.body(arn))
			assert.Equal(t, "InvalidRequestException", code,
				"%s refuses a scheduled secret under the code API_%s publishes", op.name, op.name)
			assert.Equal(t, http.StatusBadRequest, status,
				"%s answers 400, the status published for InvalidRequestException", op.name)

			// The message names which of the three causes applies, which is #953's choice rather than
			// AWS's: one code covers three unrelated conditions, so a caller reading only the code
			// cannot tell "restore it" from "configure a rotation function".
			_, raw, _ := smRawCall(t, ts, smTarget, op.name, op.body(arn))
			assert.Contains(t, raw, "scheduled for deletion",
				"%s names the cause rather than answering a bare InvalidRequestException", op.name)
			assert.Contains(t, raw, "RestoreSecret",
				"%s says how to make the secret usable again", op.name)
		})
	}
}

func TestSMScheduled_AScheduledSecretIsRefusedDifferentlyFromAnAbsentOne(t *testing.T) {
	ts := smTagServer(t)

	// A well-formed ARN in the caller's own account naming nothing, so the identifier itself is fine
	// and the only difference between the two calls is whether the secret exists.
	absent := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:no-such-secret"

	for _, op := range smScheduledRefusers {
		t.Run(op.name, func(t *testing.T) {
			arn := smScheduleSecret(t, ts, "distinct-"+op.name)

			_, _, scheduledCode := smRawCall(t, ts, smTarget, op.name, op.body(arn))
			_, _, absentCode := smRawCall(t, ts, smTarget, op.name, op.body(absent))

			assert.Equal(t, "InvalidRequestException", scheduledCode)
			assert.Equal(t, "ResourceNotFoundException", absentCode)
			assert.NotEqual(t, absentCode, scheduledCode,
				"%s tells a scheduled secret from one that never existed — the distinction #953 exists "+
					"to make observable, which four operations collapsed until #956", op.name)
		})
	}
}

func TestSMScheduled_ARefusedWriteLeavesTheSecretExactlyAsItWas(t *testing.T) {
	ts := smTagServer(t)
	arn := smScheduleSecret(t, ts, "untouched")

	// DescribeSecret is the read path for all three of these, and it answers while the window is open —
	// which is what makes "wrote nothing" assertable at all.
	before := smDescribeMembers(t, ts, arn)

	for _, op := range smScheduledRefusers {
		_, _, code := smRawCall(t, ts, smTarget, op.name, op.body(arn))
		require.Equal(t, "InvalidRequestException", code, "%s is refused", op.name)
	}

	after := smDescribeMembers(t, ts, arn)
	assert.Equal(t, smMemberNames(before), smMemberNames(after),
		"seven refusals add and remove no DescribeSecret member")
	for _, member := range []string{"Description", "Tags", "DeletedDate", "ARN", "Name"} {
		assert.JSONEq(t, string(before[member]), string(after[member]),
			"%s is unchanged by seven refused calls", member)
	}

	// RotationEnabled would be the eighth: RotateSecret's refusal is the one that used to write it as
	// true on the way to a 200, so its absence here is the assertion #952 and #956 share.
	assert.NotContains(t, after, "RotationLambdaARN",
		"a refused RotateSecret configures no rotation function")

	// The value is the one thing DescribeSecret never reports, so it is read back through the operation
	// that does — after clearing the stamp, since GetSecretValue is itself one of the seven.
	status, _, code := smRawCall(t, ts, smTarget, "RestoreSecret", map[string]any{"SecretId": arn})
	require.Empty(t, code, "RestoreSecret cancels the deletion")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "value-of-untouched", smSecretValue(t, ts, smTarget, arn),
		"a restored secret reports the value it had before the delete, not one a refused "+
			"PutSecretValue or UpdateSecret wrote")
}

func TestSMScheduled_ARestoredSecretAcceptsAllFourAgain(t *testing.T) {
	ts := smTagServer(t)
	arn := smScheduleSecret(t, ts, "restored")

	status, _, code := smRawCall(t, ts, smTarget, "RestoreSecret", map[string]any{"SecretId": arn})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	// The refusal turns on the stamp and nothing else, so clearing the stamp has to restore all four
	// operations at once. Without this the guards could have been keyed on something a restore does not
	// clear and every test above would still pass.
	for _, op := range []string{"PutSecretValue", "UpdateSecret", "TagResource", "UntagResource"} {
		var body map[string]any
		for _, candidate := range smScheduledRefusers {
			if candidate.name == op {
				body = candidate.body(arn)
			}
		}
		require.NotNil(t, body, "%s is in the refusal table", op)
		status, _, code := smRawCall(t, ts, smTarget, op, body)
		assert.Empty(t, code, "%s succeeds once the deletion is canceled", op)
		assert.Equal(t, http.StatusOK, status, "%s answers 200 on a restored secret", op)
	}

	assert.Equal(t, "rewritten", smSecretValue(t, ts, smTarget, arn),
		"the writes those four were refused land once the secret is live again")
	tags := smTagMap(t, ts, arn)
	assert.Equal(t, map[string]string{"added": "later"}, tags,
		"TagResource added a tag and UntagResource removed the original, both on a restored secret")
}

func TestSMScheduled_AnUntagNamingNoMatchingKeyIsStillRefused(t *testing.T) {
	ts := smTagServer(t)
	arn := smScheduleSecret(t, ts, "idempotent-untag")

	// "This operation is idempotent. If a requested tag is not attached to the secret, no error is
	// returned and the secret metadata is unchanged." That sentence is about which *keys* are present,
	// not about whether the secret may be modified — and InvalidRequestException reports "a parameter
	// value is not valid for the current state of the resource", which the stamp decides. So the
	// scheduled refusal outranks the idempotency, which is substrate's reading of the two sentences
	// together and the one place in #956 where they could be read the other way.
	status, _, code := smRawCall(t, ts, smTarget, "UntagResource",
		map[string]any{"SecretId": arn, "TagKeys": []string{"never-attached"}})
	assert.Equal(t, "InvalidRequestException", code,
		"the stamp is checked before the tag keys are")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, map[string]string{"env": "prod"}, smTagMap(t, ts, arn),
		"nothing was removed")
}

func TestSMScheduled_AForcedDeleteStillSucceedsOnAScheduledSecret(t *testing.T) {
	ts := smTagServer(t)
	arn := smScheduleSecret(t, ts, "forced-after-schedule")

	// The counterweight to the table: the four new refusals must not have made the scheduled state a
	// dead end. AWS's own sentence is explicit — "if you forcibly delete an already deleted or
	// nonexistent secret, the operation does not return ResourceNotFoundException" — so this path stays
	// open, and it is asserted here rather than only in the #953 file because that is what stops a later
	// guard from being added to deleteSecret unconditionally.
	status, out, code := smDeleteSecret(t, ts,
		map[string]any{"SecretId": arn, "ForceDeleteWithoutRecovery": true})
	require.Empty(t, code, "a forced delete of a scheduled secret succeeds")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, arn, out.ARN)

	_, _, gone := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": arn})
	assert.Equal(t, "ResourceNotFoundException", gone,
		"the record is removed, so the secret now reads as absent rather than as scheduled")
}
