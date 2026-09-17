package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file covers #963: ScheduleKeyDeletion and CancelKeyDeletion checked no key state, validated no
// waiting period, answered bodies AWS does not publish, and CancelKeyDeletion left the key Enabled
// where API_CancelKeyDeletion says it leaves it Disabled.
//
// Four things are asserted that a smaller test could not:
//
//   - The resulting state after a cancel is Disabled, shown by the operation that proves it matters —
//     Encrypt still refuses afterwards. Reading KeyState back says the state changed; Encrypt refusing
//     says the state means what it says.
//   - Each refusal wrote nothing, read back over the wire. For ScheduleKeyDeletion that needs the
//     deletion date to be observable, which is why #963 stores it and DescribeKey reports it: a
//     re-stamped deadline is invisible if the date is computed and discarded.
//   - Both response bodies are asserted on raw JSON, because a decoded map cannot tell an absent
//     member from a zero one — and the two defects here are an absent KeyId and an absent
//     PendingWindowInDays.
//   - The waiting-period range is asserted on both sides of both bounds, so the guard cannot be off by
//     one, and the default is asserted through the member the response now echoes rather than through
//     the date arithmetic.
//
// Every call goes over the wire, and every state is reached through the operation a consumer would use,
// per #765.

// kmsRawBody posts an operation and returns its parsed success body, requiring 200 first.
//
// It decodes into map[string]json.RawMessage rather than map[string]any so that a caller can ask
// whether a member is present at all — the distinction #963's two body defects turn on, and the one
// iam_shape_members_test.go:88 records having missed by decoding into a struct.
func kmsRawBody(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) map[string]json.RawMessage {
	t.Helper()
	var out map[string]json.RawMessage
	status, code := decodeAWSResponse(
		t, signedRequest(t, ts, kmsTarget, taggingTestAccount, op, body), &out)
	require.Empty(t, code, "%s should have succeeded", op)
	require.Equal(t, http.StatusOK, status, "%s should have succeeded", op)
	return out
}

// kmsKeyMetadata posts DescribeKey and returns the KeyMetadata member, undecoded.
func kmsKeyMetadata(t *testing.T, ts *emulator.TestServer, keyID string) map[string]json.RawMessage {
	t.Helper()
	body := kmsRawBody(t, ts, "DescribeKey", map[string]any{"KeyId": keyID})
	raw, ok := body["KeyMetadata"]
	require.True(t, ok, "DescribeKey should report KeyMetadata")
	var meta map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &meta))
	return meta
}

// kmsScheduleDeletion schedules a deletion and returns the response body, requiring success.
func kmsScheduleDeletion(t *testing.T, ts *emulator.TestServer, keyID string, days int) map[string]json.RawMessage {
	t.Helper()
	body := map[string]any{"KeyId": keyID}
	if days != 0 {
		body["PendingWindowInDays"] = days
	}
	return kmsRawBody(t, ts, "ScheduleKeyDeletion", body)
}

// TestKMSCancelKeyDeletion_LeavesTheKeyDisabled pins the first sentence of API_CancelKeyDeletion:
// "when this operation succeeds, the key state of the KMS key is Disabled. To enable the KMS key, use
// EnableKey."
//
// Recovery from a scheduled deletion is therefore two calls, and the assertion that it is two rather
// than one is Encrypt: it must still refuse after the cancel, and succeed only once EnableKey has run.
// Asserting KeyState alone would pass against a handler that wrote the string and left the key usable.
func TestKMSCancelKeyDeletion_LeavesTheKeyDisabled(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	kmsScheduleDeletion(t, ts, keyID, 7)
	kmsRawBody(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})

	meta := kmsKeyMetadata(t, ts, keyID)
	assert.JSONEq(t, `"Disabled"`, string(meta["KeyState"]), "the key state after a cancel")
	assert.JSONEq(t, `false`, string(meta["Enabled"]),
		"API_KeyMetadata: when KeyState is Enabled this value is true, otherwise it is false")

	// The state means what it says: a disabled key cannot encrypt.
	status, code := kmsCall(t, ts, "Encrypt", map[string]any{
		"KeyId": keyID, "Plaintext": "aGVsbG8=",
	})
	assert.Equal(t, "DisabledException", code, "Encrypt after a cancel, before EnableKey")
	assert.Equal(t, http.StatusBadRequest, status, "Encrypt after a cancel, before EnableKey")

	// And the second step of the documented recovery does make it usable.
	kmsRawBody(t, ts, "EnableKey", map[string]any{"KeyId": keyID})
	status, code = kmsCall(t, ts, "Encrypt", map[string]any{
		"KeyId": keyID, "Plaintext": "aGVsbG8=",
	})
	assert.Empty(t, code, "Encrypt after EnableKey")
	assert.Equal(t, http.StatusOK, status, "Encrypt after EnableKey")
}

// TestKMSCancelKeyDeletion_AKeyNotPendingDeletionIsRefused covers the row that makes
// CancelKeyDeletion unlike every other key-state-sensitive operation: its permitted set is a single
// state, and every other row in the developer guide's table is footnote [4], "KMSInvalidStateException:
// <key ARN> is not pending deletion".
//
// Before #963 this answered 200 and enabled the key, so the operation was EnableKey under another name.
// The assertion that it no longer is has to be that the key is untouched, not merely that the call
// failed — which is why an enabled key and a disabled key are both tried and both read back.
func TestKMSCancelKeyDeletion_AKeyNotPendingDeletionIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		reach     string
		wantState string
		wantOn    bool
	}{
		{"an enabled key", "", "Enabled", true},
		{"a disabled key", "DisableKey", "Disabled", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)
			if tc.reach != "" {
				kmsRawBody(t, ts, tc.reach, map[string]any{"KeyId": keyID})
			}

			status, code := kmsCall(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})
			assert.Equal(t, "KMSInvalidStateException", code, "CancelKeyDeletion on %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "CancelKeyDeletion on %s", tc.name)

			// The refusal changed nothing. Without this a handler that enabled the key and *then*
			// returned the error would satisfy the two assertions above.
			meta := kmsKeyMetadata(t, ts, keyID)
			assert.JSONEq(t, `"`+tc.wantState+`"`, string(meta["KeyState"]),
				"the key state after a refused CancelKeyDeletion")
			assert.Equal(t, tc.wantOn, string(meta["Enabled"]) == "true",
				"Enabled after a refused CancelKeyDeletion")
		})
	}
}

// TestKMSCancelKeyDeletion_AnswersTheKeyARN pins the single response element the page publishes:
// KeyId, "the Amazon Resource Name (key ARN) of the KMS key whose deletion is canceled", with a sample
// body of {"KeyId":"arn:aws:kms:us-east-2:111122223333:key/1234abcd-..."}.
//
// Substrate answered {} before #963, so a consumer reading response["KeyId"] got nothing at all. The
// member is checked for presence on the raw body first, because an absent member and an empty string
// are the same value once decoded into a struct.
func TestKMSCancelKeyDeletion_AnswersTheKeyARN(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	kmsScheduleDeletion(t, ts, keyID, 7)
	body := kmsRawBody(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})

	raw, ok := body["KeyId"]
	require.True(t, ok, "CancelKeyDeletion should answer a KeyId member")
	assert.JSONEq(t, `"`+arn+`"`, string(raw), "KeyId is the key ARN, not the key ID")
}

// TestKMSScheduleKeyDeletion_AnAlreadyPendingKeyIsRefused covers footnote [3] on the operation's own
// row: a key already in PendingDeletion is refused rather than re-scheduled.
//
// The value of the assertion is the deletion date, not the code. Before #963 a second call recomputed
// the date and saved it, so a caller that had scheduled a 7-day deletion and then called again with 30
// silently moved a deadline it believed was fixed. Proving that no longer happens needs the date to be
// readable back, which is why #963 stores it and DescribeKey reports it.
func TestKMSScheduleKeyDeletion_AnAlreadyPendingKeyIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	first := kmsScheduleDeletion(t, ts, keyID, 7)
	scheduled := string(first["DeletionDate"])
	require.NotEmpty(t, scheduled, "ScheduleKeyDeletion should report a DeletionDate")

	// A second call, asking for the longest window AWS allows — the case that would move the deadline
	// furthest if it were honored.
	status, code := kmsCall(t, ts, "ScheduleKeyDeletion", map[string]any{
		"KeyId": keyID, "PendingWindowInDays": 30,
	})
	assert.Equal(t, "KMSInvalidStateException", code, "ScheduleKeyDeletion on a pending key")
	assert.Equal(t, http.StatusBadRequest, status, "ScheduleKeyDeletion on a pending key")

	meta := kmsKeyMetadata(t, ts, keyID)
	assert.Equal(t, scheduled, string(meta["DeletionDate"]),
		"the refused call must not have moved the deletion date")
	assert.JSONEq(t, `"PendingDeletion"`, string(meta["KeyState"]))
}

// TestKMSScheduleKeyDeletion_AnswersEveryPublishedMember covers the response shape.
//
// API_ScheduleKeyDeletion publishes four elements — DeletionDate, KeyId, KeyState and
// PendingWindowInDays — and its sample response carries all four. Substrate answered three, omitting
// PendingWindowInDays, and its KeyId was the bare key ID where the page says "the Amazon Resource Name
// (key ARN)" and the sample shows one.
func TestKMSScheduleKeyDeletion_AnswersEveryPublishedMember(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	body := kmsScheduleDeletion(t, ts, keyID, 7)

	for _, member := range []string{"DeletionDate", "KeyId", "KeyState", "PendingWindowInDays"} {
		_, ok := body[member]
		assert.True(t, ok, "ScheduleKeyDeletion should answer %s", member)
	}
	assert.JSONEq(t, `"`+arn+`"`, string(body["KeyId"]), "KeyId is the key ARN, not the key ID")
	assert.JSONEq(t, `"PendingDeletion"`, string(body["KeyState"]))
	assert.JSONEq(t, `7`, string(body["PendingWindowInDays"]), "the window the caller asked for")
}

// TestKMSScheduleKeyDeletion_TheWaitingPeriodIsRangeChecked pins the published 7-30 range on both
// sides of both bounds, so a guard written with the wrong comparison cannot pass.
//
// AWS publishes no error code for the violation — the page's list is DependencyTimeoutException,
// InvalidArnException, KMSInternalException, KMSInvalidStateException and NotFoundException — so
// ValidationError/400 is substrate's reading, taken from CommonErrors for the same reason an
// unparseable body is.
func TestKMSScheduleKeyDeletion_TheWaitingPeriodIsRangeChecked(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		days    int
		refused bool
	}{
		{"a negative window", -5, true},
		{"one day below the minimum", 6, true},
		{"the minimum", 7, false},
		{"the maximum", 30, false},
		{"one day above the maximum", 31, true},
		{"a year", 365, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			status, code := kmsCall(t, ts, "ScheduleKeyDeletion", map[string]any{
				"KeyId": keyID, "PendingWindowInDays": tc.days,
			})
			if !tc.refused {
				assert.Empty(t, code, "PendingWindowInDays=%d is within 7-30", tc.days)
				assert.Equal(t, http.StatusOK, status, "PendingWindowInDays=%d", tc.days)
				return
			}
			assert.Equal(t, "ValidationError", code, "PendingWindowInDays=%d is outside 7-30", tc.days)
			assert.Equal(t, http.StatusBadRequest, status, "PendingWindowInDays=%d", tc.days)

			// A refused window scheduled nothing: the key is still usable. Read back through
			// DescribeKey rather than by inspecting state, per #765.
			meta := kmsKeyMetadata(t, ts, keyID)
			assert.JSONEq(t, `"Enabled"`, string(meta["KeyState"]),
				"a refused window must not have scheduled the deletion")
			_, hasDate := meta["DeletionDate"]
			assert.False(t, hasDate,
				"API_KeyMetadata confines DeletionDate to KeyState PendingDeletion")
		})
	}
}

// TestKMSScheduleKeyDeletion_AnAbsentWaitingPeriodDefaultsTo30 pins "if you do not include a value, it
// defaults to 30".
//
// Asserted through the echoed PendingWindowInDays rather than by arithmetic on DeletionDate, because
// the member is what a caller can read and the arithmetic would be re-deriving the implementation.
func TestKMSScheduleKeyDeletion_AnAbsentWaitingPeriodDefaultsTo30(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	body := kmsScheduleDeletion(t, ts, keyID, 0) // 0 omits the member entirely.
	assert.JSONEq(t, `30`, string(body["PendingWindowInDays"]),
		"an absent PendingWindowInDays defaults to 30")
}

// TestKMSDescribeKey_ReportsTheDeletionDateOnlyWhilePending pins the condition API_KeyMetadata
// publishes for the member: "this value is present only when the KMS key is scheduled for deletion,
// that is, when its KeyState is PendingDeletion".
//
// Both halves matter. Reporting it while pending is what makes a scheduled deletion observable more
// than once. Omitting it otherwise is what keeps a canceled deletion from leaving a date behind — and
// asserting absence needs the raw body, since a zero time decodes to a perfectly plausible epoch
// timestamp.
func TestKMSDescribeKey_ReportsTheDeletionDateOnlyWhilePending(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	_, before := kmsKeyMetadata(t, ts, keyID)["DeletionDate"]
	assert.False(t, before, "an enabled key has no DeletionDate")

	scheduled := kmsScheduleDeletion(t, ts, keyID, 7)
	meta := kmsKeyMetadata(t, ts, keyID)
	assert.Equal(t, string(scheduled["DeletionDate"]), string(meta["DeletionDate"]),
		"DescribeKey reports the date ScheduleKeyDeletion answered")

	kmsRawBody(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})
	_, after := kmsKeyMetadata(t, ts, keyID)["DeletionDate"]
	assert.False(t, after, "a canceled deletion leaves no DeletionDate behind")
}

// TestKMSKeyDeletion_TheRoundTripIsRepeatable is the guard against every refusal above being satisfied
// by a handler that simply refuses more than it should.
//
// A key is scheduled, canceled, enabled and scheduled again — with a different window the second time,
// asserted through the echoed member, so the second call is shown to be a fresh schedule rather than a
// remembered one. Without this, a CancelKeyDeletion that left the key permanently unschedulable, or a
// ScheduleKeyDeletion that refused any key it had ever seen, would pass the rest of the file.
func TestKMSKeyDeletion_TheRoundTripIsRepeatable(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	first := kmsScheduleDeletion(t, ts, keyID, 7)
	assert.JSONEq(t, `7`, string(first["PendingWindowInDays"]))

	kmsRawBody(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})
	kmsRawBody(t, ts, "EnableKey", map[string]any{"KeyId": keyID})

	second := kmsScheduleDeletion(t, ts, keyID, 30)
	assert.JSONEq(t, `30`, string(second["PendingWindowInDays"]),
		"the second schedule takes its own window")
	assert.NotEqual(t, string(first["DeletionDate"]), string(second["DeletionDate"]),
		"a longer window gives a later date, so the second call scheduled afresh")

	// And the cancel still works the second time round.
	kmsRawBody(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})
	meta := kmsKeyMetadata(t, ts, keyID)
	assert.JSONEq(t, `"Disabled"`, string(meta["KeyState"]))
}
