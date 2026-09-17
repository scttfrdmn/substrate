package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// DeleteSecret's recovery window and RestoreSecret — #953.
//
// Two things are asserted here that no existing test could assert, and neither substitutes for the
// other:
//
//  1. **DeletedDate's presence and absence, on the raw bytes**, following #930's precedent in
//     secretsmanager_describe_test.go. AWS documents DeletedDate as one of the four DescribeSecret
//     members that are *omitted* rather than nulled when they have no value, and the whole of what
//     makes a secret "scheduled for deletion" observable is that this member appears. A decoded struct
//     turns an absent member and a zero-valued one into the same thing, which is exactly the
//     distinction under test.
//
//  2. **That a scheduled secret is refused differently from an absent one.** Before this issue every
//     DeleteSecret removed the record, so both answered ResourceNotFoundException and a consumer could
//     not tell "restore this" from "this never existed". GetSecretValue now answers
//     InvalidRequestException for the first and ResourceNotFoundException for the second, and the pair
//     is asserted together rather than each alone.
//
// Every call goes over the wire through [smRawCall], and every secret is created through CreateSecret —
// nothing here writes state directly, per #765's standing rule.
//
// The simulated clock is pinned with [emulator.TestServer.SetTime] immediately before a delete, so a
// DeletionDate assertion compares two known quantities. The small tolerance is not a wall-clock
// dependence: [emulator.TimeController.Now] advances from its baseline by wall elapsed at scale 1.0, so
// the microseconds between SetTime and the handler's own Now() land in the stamp. Nothing below depends
// on what the wall clock *reads*, only on the fact that two calls in one test are close together.

// smDeleteSecret posts DeleteSecret with whatever parameters a case names, and returns the status, the
// decoded {ARN, Name, DeletionDate} and the bare error code a refusal carries.
//
// DeletionDate is *int64 so that a case can tell a member AWS omits from one reported as zero — the
// same reason smDescribeMembers decodes into json.RawMessage.
func smDeleteSecret(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, struct {
	ARN          string `json:"ARN"`
	Name         string `json:"Name"`
	DeletionDate *int64 `json:"DeletionDate"`
}, string) {
	t.Helper()
	var out struct {
		ARN          string `json:"ARN"`
		Name         string `json:"Name"`
		DeletionDate *int64 `json:"DeletionDate"`
	}
	status, raw, code := smRawCall(t, ts, smTarget, "DeleteSecret", body)
	require.NoError(t, json.Unmarshal([]byte(raw), &out), "decode DeleteSecret %v", body)
	return status, out, code
}

// smRestoreMembers posts RestoreSecret and returns its response as raw members, so a case can assert
// the member *set* AWS publishes for it rather than only the two values.
func smRestoreMembers(t *testing.T, ts *emulator.TestServer, secretID string) (int, map[string]json.RawMessage, string) {
	t.Helper()
	status, raw, code := smRawCall(t, ts, smTarget, "RestoreSecret", map[string]any{"SecretId": secretID})
	out := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal([]byte(raw), &out), "decode RestoreSecret %s", secretID)
	return status, out, code
}

// smGetSecretValueCode reads a secret's value and returns the status and the error code, so the two
// refusals a scheduled secret and an absent secret get can be compared side by side.
func smGetSecretValueCode(t *testing.T, ts *emulator.TestServer, secretID string) (int, string) {
	t.Helper()
	status, _, code := smRawCall(t, ts, smTarget, "GetSecretValue", map[string]any{"SecretId": secretID})
	return status, code
}

// smPinnedClock sets the simulated clock to a fixed instant and returns it, so a DeletionDate can be
// compared against a known baseline rather than against whatever the clock had drifted to.
func smPinnedClock(ts *emulator.TestServer) time.Time {
	pinned := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	ts.SetTime(pinned)
	return pinned
}

// smWindowTolerance is how far a stamp may sit from its expected value: the wall-clock microseconds
// TimeController.Now() adds between the SetTime call and the handler's own read. Five seconds is orders
// of magnitude above that and orders of magnitude below one day, so it cannot mask a wrong window.
const smWindowTolerance = 5 * time.Second

func TestSMDeletion_ADefaultDeleteSchedulesRatherThanRemoves(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "scheduled", nil)

	pinned := smPinnedClock(ts)
	status, out, code := smDeleteSecret(t, ts, map[string]any{"SecretId": arn})
	require.Empty(t, code, "a plain DeleteSecret succeeds")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, arn, out.ARN, "the response reports the secret's own ARN")
	assert.Equal(t, "scheduled", out.Name)

	// "If you don't use either, then by default Secrets Manager uses a 30 day recovery window", and
	// "this value is the date and time of the delete request plus the number of days in
	// RecoveryWindowInDays".
	require.NotNil(t, out.DeletionDate, "DeleteSecret reports a DeletionDate")
	wantDefault := pinned.AddDate(0, 0, 30)
	assert.WithinDuration(t, wantDefault, time.Unix(*out.DeletionDate, 0), smWindowTolerance,
		"the default window is 30 days from the request")

	// "At the end of the recovery window, Secrets Manager deletes the secret permanently" — so until
	// then the secret exists, and both of these say so.
	assert.Contains(t, smListSecretNames(t, ts, smTarget), "scheduled",
		"ListSecrets still reports a secret inside its recovery window")
	members := smDescribeMembers(t, ts, arn)
	require.Contains(t, members, "DeletedDate",
		"DescribeSecret reports DeletedDate while the window is open")
	var deletedDate int64
	require.NoError(t, json.Unmarshal(members["DeletedDate"], &deletedDate))
	assert.Equal(t, *out.DeletionDate, deletedDate,
		"DescribeSecret's DeletedDate is the same stamp DeleteSecret answered, under AWS's other name for it")
}

func TestSMDeletion_AScheduledSecretIsRefusedDifferentlyFromAnAbsentOne(t *testing.T) {
	ts := smTagServer(t)
	scheduledARN := smCreateSecret(t, ts, "value-withheld", nil)
	absentARN := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:never-existed"

	_, _, code := smDeleteSecret(t, ts, map[string]any{"SecretId": scheduledARN})
	require.Empty(t, code)

	// "When a secret is scheduled for deletion, you cannot retrieve the secret value." The refusal is
	// InvalidRequestException, whose first published cause is "The secret is scheduled for deletion."
	status, gotCode := smGetSecretValueCode(t, ts, scheduledARN)
	assert.Equal(t, "InvalidRequestException", gotCode,
		"a scheduled secret's value is withheld under the code AWS publishes for it")
	assert.Equal(t, http.StatusBadRequest, status)

	// The pair is the point: before #953 both of these read ResourceNotFoundException, because a plain
	// DeleteSecret removed the record, so a consumer could not tell a restorable secret from one that
	// never existed.
	absentStatus, absentCode := smGetSecretValueCode(t, ts, absentARN)
	assert.Equal(t, "ResourceNotFoundException", absentCode,
		"a secret that does not exist still reports absent, not scheduled")
	assert.Equal(t, http.StatusBadRequest, absentStatus)
}

func TestSMDeletion_DeletedDateIsAbsentUntilScheduledAndAbsentAgainAfterRestore(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "round-trip", nil)

	// Absent, not null and not zero: DeletedDate is one of the four members API_DescribeSecret
	// documents as "this field is omitted".
	assert.NotContains(t, smDescribeMembers(t, ts, arn), "DeletedDate",
		"a live secret reports no DeletedDate at all")

	_, _, code := smDeleteSecret(t, ts, map[string]any{"SecretId": arn})
	require.Empty(t, code)
	assert.Contains(t, smDescribeMembers(t, ts, arn), "DeletedDate", "scheduling adds the member")

	// "Cancels the scheduled deletion of a secret by removing the DeletedDate time stamp."
	status, members, restoreCode := smRestoreMembers(t, ts, arn)
	require.Empty(t, restoreCode, "RestoreSecret succeeds")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"ARN", "Name"}, smMemberNames(members),
		"RestoreSecret's response is exactly the two members its Response Syntax publishes")

	assert.NotContains(t, smDescribeMembers(t, ts, arn), "DeletedDate",
		"restoring removes the member rather than zeroing it")

	// "You can access a secret again after it has been restored" — and it is the same value, so the
	// restore did not quietly re-create an empty secret.
	assert.Equal(t, "value-of-round-trip", smSecretValue(t, ts, smTarget, arn),
		"GetSecretValue succeeds again and reports the original value")
}

func TestSMDeletion_TheRecoveryWindowIsTheOneRequested(t *testing.T) {
	ts := smTagServer(t)

	// "The number of days from 7 to 30 that Secrets Manager waits before permanently deleting the
	// secret." Both bounds are included, because both are AWS's own numbers rather than a range's
	// interior.
	for _, days := range []int{7, 14, 30} {
		t.Run(fmt.Sprintf("%dd", days), func(t *testing.T) {
			name := fmt.Sprintf("window-%dd", days)
			arn := smCreateSecret(t, ts, name, nil)

			pinned := smPinnedClock(ts)
			status, out, code := smDeleteSecret(t, ts, map[string]any{
				"SecretId":             arn,
				"RecoveryWindowInDays": days,
			})
			require.Empty(t, code, "RecoveryWindowInDays %d is accepted", days)
			require.Equal(t, http.StatusOK, status)
			require.NotNil(t, out.DeletionDate)
			assert.WithinDuration(t, pinned.AddDate(0, 0, days), time.Unix(*out.DeletionDate, 0),
				smWindowTolerance, "the window is the %d days requested", days)
		})
	}
}

func TestSMDeletion_AWindowOutsideSevenToThirtyIsRefusedAndWritesNothing(t *testing.T) {
	ts := smTagServer(t)

	for _, days := range []int{0, 1, 6, 31, 365, -1} {
		t.Run(fmt.Sprintf("%dd", days), func(t *testing.T) {
			name := fmt.Sprintf("bad-window-%dd", days)
			arn := smCreateSecret(t, ts, name, nil)

			status, _, code := smDeleteSecret(t, ts, map[string]any{
				"SecretId":             arn,
				"RecoveryWindowInDays": days,
			})
			assert.Equal(t, "InvalidParameterException", code,
				"RecoveryWindowInDays %d is outside AWS's 7-to-30 range", days)
			assert.Equal(t, http.StatusBadRequest, status)

			// The refusal must not have half-applied: a rejected window that stamped a DeletionDate
			// anyway would withhold the value of a secret the caller was told nothing happened to.
			assert.NotContains(t, smDescribeMembers(t, ts, arn), "DeletedDate",
				"a refused window leaves the secret unscheduled")
			assert.Equal(t, "value-of-"+name, smSecretValue(t, ts, smTarget, arn),
				"and its value still readable")
		})
	}
}

func TestSMDeletion_BothParametersInOneCallAreRefused(t *testing.T) {
	ts := smTagServer(t)

	// "You can't use both this parameter and ForceDeleteWithoutRecovery in the same call", stated twice
	// — once under each parameter. Substrate refuses on *presence*, which is its reading of "use both":
	// the false case below is the one that reading decides, and refusing it is what keeps an explicit
	// false from meaning the same as an omission and silently scheduling a deletion the caller asked to
	// be immediate.
	for name, force := range map[string]bool{"force-true": true, "force-false": false} {
		t.Run(name, func(t *testing.T) {
			arn := smCreateSecret(t, ts, name, nil)
			status, _, code := smDeleteSecret(t, ts, map[string]any{
				"SecretId":                   arn,
				"RecoveryWindowInDays":       7,
				"ForceDeleteWithoutRecovery": force,
			})
			assert.Equal(t, "InvalidParameterException", code)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "value-of-"+name, smSecretValue(t, ts, smTarget, arn),
				"the secret is untouched by the refusal")
		})
	}
}

func TestSMDeletion_AForcedDeleteRemovesTheRecord(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "forced", nil)

	pinned := smPinnedClock(ts)
	status, out, code := smDeleteSecret(t, ts, map[string]any{
		"SecretId":                   arn,
		"ForceDeleteWithoutRecovery": true,
	})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, arn, out.ARN)

	// A forced delete has no window, so the same "request plus RecoveryWindowInDays" derivation puts
	// the stamp at the request itself. Reporting one at all is substrate's reading — AWS publishes the
	// member unconditionally and publishes no separate forced-delete response.
	require.NotNil(t, out.DeletionDate)
	assert.WithinDuration(t, pinned, time.Unix(*out.DeletionDate, 0), smWindowTolerance)

	describeStatus, _, describeCode := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": arn})
	assert.Equal(t, "ResourceNotFoundException", describeCode, "the record is gone, not scheduled")
	assert.Equal(t, http.StatusBadRequest, describeStatus)
	assert.NotContains(t, smListSecretNames(t, ts, smTarget), "forced",
		"and the index entry with it")
}

func TestSMDeletion_AForcedDeleteOfAnAbsentSecretSucceeds(t *testing.T) {
	ts := smTagServer(t)
	absent := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:no-such-secret"

	// "If you forcibly delete an already deleted or nonexistent secret, the operation does not return
	// ResourceNotFoundException." AWS publishes the suspension of the code without publishing the body
	// answered instead, so reporting the ARN the identifier names is substrate's reading.
	t.Run("nonexistent", func(t *testing.T) {
		status, out, code := smDeleteSecret(t, ts, map[string]any{
			"SecretId":                   absent,
			"ForceDeleteWithoutRecovery": true,
		})
		require.Empty(t, code, "a forced delete of a secret that never existed is not refused")
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, absent, out.ARN)
		assert.Equal(t, "no-such-secret", out.Name)
		require.NotNil(t, out.DeletionDate)
	})

	// "Already deleted" is the other half of AWS's sentence, and it is the half a stack teardown hits:
	// a forced delete run twice.
	t.Run("already deleted", func(t *testing.T) {
		arn := smCreateSecret(t, ts, "twice", nil)
		body := map[string]any{"SecretId": arn, "ForceDeleteWithoutRecovery": true}
		_, _, first := smDeleteSecret(t, ts, body)
		require.Empty(t, first)
		status, _, second := smDeleteSecret(t, ts, body)
		assert.Empty(t, second, "the second forced delete is not refused either")
		assert.Equal(t, http.StatusOK, status)
	})

	// Without force the suspension does not apply, which is the sentence's own condition.
	t.Run("without force", func(t *testing.T) {
		status, _, code := smDeleteSecret(t, ts, map[string]any{"SecretId": absent})
		assert.Equal(t, "ResourceNotFoundException", code,
			"an unforced delete of an absent secret is still refused")
		assert.Equal(t, http.StatusBadRequest, status)
	})
}

func TestSMDeletion_ASecondUnforcedDeleteIsRefusedButAForcedOneSucceeds(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "double", nil)

	_, _, code := smDeleteSecret(t, ts, map[string]any{"SecretId": arn})
	require.Empty(t, code)

	status, _, again := smDeleteSecret(t, ts, map[string]any{"SecretId": arn})
	assert.Equal(t, "InvalidRequestException", again,
		"scheduling an already-scheduled secret names the scheduled-for-deletion cause")
	assert.Equal(t, http.StatusBadRequest, status)

	// A forced delete does apply to a scheduled secret — that is how a caller shortens a window they
	// have already opened, and AWS suspends no code for it.
	forcedStatus, _, forcedCode := smDeleteSecret(t, ts, map[string]any{
		"SecretId":                   arn,
		"ForceDeleteWithoutRecovery": true,
	})
	require.Empty(t, forcedCode)
	assert.Equal(t, http.StatusOK, forcedStatus)
	assert.NotContains(t, smListSecretNames(t, ts, smTarget), "double")
}

func TestSMDeletion_RestoreSecretRefusesAnAbsentSecretAndAcceptsAnUnscheduledOne(t *testing.T) {
	ts := smTagServer(t)

	t.Run("absent", func(t *testing.T) {
		absent := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:nothing-to-restore"
		status, _, code := smRawCall(t, ts, smTarget, "RestoreSecret", map[string]any{"SecretId": absent})
		assert.Equal(t, "ResourceNotFoundException", code)
		assert.Equal(t, http.StatusBadRequest, status,
			"400, the status API_RestoreSecret publishes for it — the #930 rule")
	})

	// Substrate's reading, recorded rather than invented: API_RestoreSecret publishes
	// InvalidRequestException, but its cause list names only the three conditions shared across the
	// service and none of them is "not scheduled", so there is no published code to refuse with.
	// Refusing anyway would make an idempotent restore fail here and succeed against AWS.
	t.Run("not scheduled", func(t *testing.T) {
		arn := smCreateSecret(t, ts, "never-scheduled", nil)
		status, _, code := smRestoreMembers(t, ts, arn)
		require.Empty(t, code)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "value-of-never-scheduled", smSecretValue(t, ts, smTarget, arn))
	})
}

func TestSMDeletion_ASecretIsStillReportedAfterItsWindowHasPassed(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "expired-window", nil)

	_, out, code := smDeleteSecret(t, ts, map[string]any{
		"SecretId":             arn,
		"RecoveryWindowInDays": 7,
	})
	require.Empty(t, code)
	require.NotNil(t, out.DeletionDate)

	// The simulated clock is moved well past the end of the window, which is the only way this
	// condition is reachable without a wall-clock wait.
	ts.AdvanceTime(30 * 24 * time.Hour)
	require.True(t, ts.TimeController().Now().After(time.Unix(*out.DeletionDate, 0)),
		"the simulated clock is now past the DeletionDate")

	// **This is a recorded decision, not an oversight.** AWS publishes no guarantee to model here —
	// "There is no guarantee of a specific time after the recovery window for the permanent delete to
	// occur" — so substrate does not make the secret vanish at any simulated instant. A test asserting
	// it had would assert something AWS explicitly declines to promise. What is modeled is the stamp
	// and the refusals it causes, both of which still hold below.
	assert.Contains(t, smListSecretNames(t, ts, smTarget), "expired-window",
		"a secret whose window has passed is still reported")
	assert.Contains(t, smDescribeMembers(t, ts, arn), "DeletedDate",
		"and still carries the stamp")
	status, getCode := smGetSecretValueCode(t, ts, arn)
	assert.Equal(t, "InvalidRequestException", getCode,
		"its value is still withheld, rather than the secret reading absent")
	assert.Equal(t, http.StatusBadRequest, status)

	// And it is still restorable, which is the observable consequence of not modeling the permanent
	// delete: the recovery window's end is a stamp, not a transition.
	_, _, restoreCode := smRestoreMembers(t, ts, arn)
	assert.Empty(t, restoreCode)
}
