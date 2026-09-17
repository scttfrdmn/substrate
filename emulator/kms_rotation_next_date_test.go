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

// #973: GetKeyRotationStatus answered two of its five published members.
//
// KeyId and KeyRotationEnabled were there; RotationPeriodInDays arrived with #964. NextRotationDate was
// missing entirely, and it is the member a consumer's rotation-monitoring code reads — the whole point of
// asking for a rotation status is usually to find out when the next one is. AWS defines it against the
// enable date, on API_EnableKeyRotation: "the rotation period defines the number of days after you enable
// automatic key rotation that AWS KMS will rotate your key material, and the number of days between each
// automatic rotation thereafter." So it is derivable from two things substrate can hold, and until #973
// it held only one of them — [emulator.KMSKey] gained the enable date for this and nothing else.
//
// The second half of the issue is the answer for a key pending deletion, which substrate got wrong rather
// than omitted: it reported the stored flag, where AWS publishes "while a KMS key is pending deletion, its
// key rotation status is false and AWS KMS does not rotate the key material. If you cancel the deletion,
// the original key rotation status returns to true." A caller polling a key it had scheduled for deletion
// was told rotation was still on.
//
// Five things are asserted:
//
//  1. **The date is the enable date plus the period**, for the default period and both ends of the
//     published range. Against a pinned simulated clock, so the arithmetic is checked rather than merely
//     the member's presence.
//  2. **The schedule is absent when rotation is off**, before any enable and after a disable — which is
//     the honest-empty reading #827 established, since neither a period nor a next date is a fact about a
//     key with no rotation schedule.
//  3. **A second enable moves the date**, both when the clock has advanced and when only the period
//     changed. This is the reading recorded on the field: keeping the first enable date and shortening
//     the period would report a rotation already in the past.
//  4. **A key pending deletion reports false and no schedule, and canceling restores both** — the walk
//     AWS's two sentences describe, asserted as one sequence because the restoration is the half that
//     cannot pass by accident. It also pins the derivation: the stored flag has to survive the deletion
//     for the cancel to restore anything, so an implementation that wrote false on ScheduleKeyDeletion
//     would satisfy the first half and fail here.
//  5. **OnDemandRotationStartDate is never reported.** Asserted on every read in this file rather than
//     once, by [kmsRotationStatusBody]. It is the one published member still absent, and it is blocked
//     rather than deferred: it reports an in-progress RotateKeyOnDemand, which substrate does not
//     implement, so no request can set it and a zero timestamp would report a rotation nobody started.
//
// Every read is raw JSON, because "the member is absent" and "the member is zero" are different answers
// here and a decoded struct cannot tell them apart — the distinction #963's two body defects turned on.
// Every call goes over the wire and every state is reached through the operation a consumer would use,
// per #765.

// kmsRotationDateTolerance is how far a reported date may sit from its expected value.
//
// TimeController.Now() advances by wall-clock elapsed from the last SetTime, so the enable date a handler
// records is the pinned instant plus the microseconds between the pin and the request. Five seconds is
// orders of magnitude above that and orders of magnitude below one day, so it cannot mask a wrong period
// — the same bound, for the same reason, as secretsmanager_deletion_test.go's smWindowTolerance.
const kmsRotationDateTolerance = 5 * time.Second

// kmsPinnedClock sets the simulated clock to a fixed instant and returns it, so a NextRotationDate can be
// compared against a known baseline rather than against whatever the clock had drifted to.
//
// The instant is a parameter because assertion 3 needs two of them on one server: a re-enable has to be
// shown to move the date, and that is only visible if the clock moved between the two calls.
func kmsPinnedClock(ts *emulator.TestServer, at time.Time) time.Time {
	ts.SetTime(at)
	return at
}

// kmsRotationEpoch is the instant assertion 1 and assertion 4 pin the clock to.
//
// A fixed date rather than time.Now(), so the arithmetic in a failure message is readable, and one well
// clear of a month boundary so that AddDate's month-end clamping cannot come into it.
var kmsRotationEpoch = time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)

// kmsRotationStatusBody reads a GetKeyRotationStatus body as raw JSON, and asserts on every call that
// OnDemandRotationStartDate is absent.
//
// The blanket assertion is deliberate. That member is the one published response element substrate does
// not answer, and its absence is a decision rather than an oversight (assertion 5), so it is checked
// wherever a status is read rather than in one test a later change could pass while regressing every
// other path.
func kmsRotationStatusBody(t *testing.T, ts *emulator.TestServer, keyID string) map[string]json.RawMessage {
	t.Helper()
	body := kmsRawBody(t, ts, "GetKeyRotationStatus", map[string]any{"KeyId": keyID})
	_, ok := body["OnDemandRotationStartDate"]
	require.False(t, ok,
		"OnDemandRotationStartDate reports an in-progress RotateKeyOnDemand, which substrate does not "+
			"implement, so no request can have set it: %v", body)
	return body
}

// kmsNextRotationDate reads NextRotationDate out of a status body, requiring it to be present.
//
// Seconds since the epoch, matching what DescribeKey does with DeletionDate. AWS's own page types the
// member number in its Response Syntax and renders it as an ISO-8601 string in its sample, so the two
// disagree there; substrate keeps one KMS timestamp convention, which is the choice this decode pins.
func kmsNextRotationDate(t *testing.T, body map[string]json.RawMessage) time.Time {
	t.Helper()
	raw, ok := body["NextRotationDate"]
	require.True(t, ok, "the status carries NextRotationDate: %v", body)
	var seconds int64
	require.NoError(t, json.Unmarshal(raw, &seconds), "NextRotationDate is a number of seconds")
	return time.Unix(seconds, 0)
}

// kmsRotationMember reads an integer member out of a status body, requiring it to be present.
func kmsRotationMember(t *testing.T, body map[string]json.RawMessage, member string) int {
	t.Helper()
	raw, ok := body[member]
	require.True(t, ok, "the status carries %s: %v", member, body)
	var value int
	require.NoError(t, json.Unmarshal(raw, &value), "decode %s", member)
	return value
}

// kmsRequireBool reads a boolean member out of a status body, requiring it to be present.
//
// Present, not merely true or false: KeyRotationEnabled has no "omitempty" reading available to it —
// API_GetKeyRotationStatus's sample renders it for a key whose rotation is off — so an absent member
// would be a defect that a decode into a bool would silently report as false.
func kmsRequireBool(t *testing.T, body map[string]json.RawMessage, member string) bool {
	t.Helper()
	raw, ok := body[member]
	require.True(t, ok, "the status carries %s: %v", member, body)
	var value bool
	require.NoError(t, json.Unmarshal(raw, &value), "decode %s", member)
	return value
}

// kmsRequireNoSchedule asserts that neither schedule member is reported.
//
// Both together, because they are one decision: the period and the date follow the *reported* rotation
// status, so answering one without the other would describe half a schedule. Answering a period beside a
// false status is the specific failure this rules out — it would describe a rotation AWS has just said
// will not happen.
func kmsRequireNoSchedule(t *testing.T, body map[string]json.RawMessage, when string) {
	t.Helper()
	for _, member := range []string{"RotationPeriodInDays", "NextRotationDate"} {
		_, ok := body[member]
		assert.False(t, ok, "%s is not reported %s: %v", member, when, body)
	}
}

// TestKMSGetKeyRotationStatus_ReportsTheNextRotationDate is assertion 1.
//
// The default and both ends of the published 90-2560 range, because the date is the period's only
// observable consequence and a handler that ignored the period would pass a single-row test. The absent
// row also pins the two halves agreeing: RotationPeriodInDays reports 365 and NextRotationDate is 365
// days out, so the member a caller reads and the arithmetic behind it cannot diverge.
func TestKMSGetKeyRotationStatus_ReportsTheNextRotationDate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// send is the period the request carries; 0 sends no member at all, which is
		// [kmsEnableRotation]'s own convention and the distinction the handler's pointer field exists
		// for.
		send   int
		period int
	}{
		{"the default period", 0, 365},
		{"the published minimum", 90, 90},
		{"the published maximum", 2560, 2560},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			pinned := kmsPinnedClock(ts, kmsRotationEpoch)
			kmsEnableRotation(t, ts, keyID, tc.send)

			status := kmsRotationStatusBody(t, ts, keyID)
			assert.True(t, kmsRequireBool(t, status, "KeyRotationEnabled"), "rotation is on")
			assert.Equal(t, tc.period, kmsRotationMember(t, status, "RotationPeriodInDays"),
				"the period the enable applied")
			assert.WithinDuration(t, pinned.AddDate(0, 0, tc.period), kmsNextRotationDate(t, status),
				kmsRotationDateTolerance,
				"NextRotationDate is the enable date plus %d days, per API_EnableKeyRotation's "+
					"definition of a rotation period", tc.period)
		})
	}
}

// TestKMSGetKeyRotationStatus_OmitsTheScheduleWhenRotationIsOff is assertion 2.
//
// Both directions, because they are different code paths: a key that has never had rotation enabled has
// no stored period or date to report, while a key whose rotation was turned off has both and must report
// neither. The second is the one that could regress silently — the values are deliberately kept across a
// DisableKeyRotation, so what suppresses them is the reported status rather than their absence.
func TestKMSGetKeyRotationStatus_OmitsTheScheduleWhenRotationIsOff(t *testing.T) {
	t.Parallel()

	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	fresh := kmsRotationStatusBody(t, ts, keyID)
	assert.False(t, kmsRequireBool(t, fresh, "KeyRotationEnabled"), "a new key does not rotate")
	kmsRequireNoSchedule(t, fresh, "for a key that has never had rotation enabled")

	kmsPinnedClock(ts, kmsRotationEpoch)
	kmsEnableRotation(t, ts, keyID, 90)
	enabled := kmsRotationStatusBody(t, ts, keyID)
	require.True(t, kmsRequireBool(t, enabled, "KeyRotationEnabled"), "rotation is on")
	require.Equal(t, 90, kmsRotationMember(t, enabled, "RotationPeriodInDays"))

	status, code := kmsCall(t, ts, "DisableKeyRotation", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "DisableKeyRotation")
	require.Equal(t, http.StatusOK, status, "DisableKeyRotation")

	disabled := kmsRotationStatusBody(t, ts, keyID)
	assert.False(t, kmsRequireBool(t, disabled, "KeyRotationEnabled"), "rotation is off again")
	kmsRequireNoSchedule(t, disabled, "once rotation has been turned off")
}

// TestKMSEnableKeyRotation_MovesTheNextRotationDate is assertion 3.
//
// AWS documents no answer for what a second enable does to an existing schedule — the parameter is
// described as able to "modify the rotation period of a key that you previously enabled automatic key
// rotation on", with nothing said about the date it counts from — so overwriting the enable date is
// substrate's reading. The second row is why it is the reading taken: a key enabled with 2560 days and
// re-enabled with 90 would, under the alternative, report a next rotation date roughly six years in the
// past.
func TestKMSEnableKeyRotation_MovesTheNextRotationDate(t *testing.T) {
	t.Parallel()

	t.Run("the clock advanced", func(t *testing.T) {
		t.Parallel()

		ts := arnGuardServer(t)
		_, keyID := createKMSKey(t, ts)

		first := kmsPinnedClock(ts, kmsRotationEpoch)
		kmsEnableRotation(t, ts, keyID, 90)
		assert.WithinDuration(t, first.AddDate(0, 0, 90),
			kmsNextRotationDate(t, kmsRotationStatusBody(t, ts, keyID)), kmsRotationDateTolerance)

		later := kmsPinnedClock(ts, kmsRotationEpoch.AddDate(0, 0, 30))
		kmsEnableRotation(t, ts, keyID, 90)
		assert.WithinDuration(t, later.AddDate(0, 0, 90),
			kmsNextRotationDate(t, kmsRotationStatusBody(t, ts, keyID)), kmsRotationDateTolerance,
			"the second enable counts from when it ran, not from the first one")
	})

	t.Run("only the period changed", func(t *testing.T) {
		t.Parallel()

		ts := arnGuardServer(t)
		_, keyID := createKMSKey(t, ts)

		pinned := kmsPinnedClock(ts, kmsRotationEpoch)
		kmsEnableRotation(t, ts, keyID, 2560)
		require.WithinDuration(t, pinned.AddDate(0, 0, 2560),
			kmsNextRotationDate(t, kmsRotationStatusBody(t, ts, keyID)), kmsRotationDateTolerance)

		kmsEnableRotation(t, ts, keyID, 90)
		shortened := kmsRotationStatusBody(t, ts, keyID)
		assert.Equal(t, 90, kmsRotationMember(t, shortened, "RotationPeriodInDays"))
		assert.WithinDuration(t, pinned.AddDate(0, 0, 90), kmsNextRotationDate(t, shortened),
			kmsRotationDateTolerance,
			"shortening the period moves the date forward from the new enable, never into the past")
	})
}

// TestKMSGetKeyRotationStatus_APendingDeletionKeyReportsFalseUntilTheDeletionIsCancelled is assertion 4.
//
// The whole walk in one test, because each step is only meaningful after the one before it: a false that
// is never restored would be indistinguishable from ScheduleKeyDeletion clearing the flag, and that is
// the implementation AWS's second sentence rules out. It is also what makes the derivation in the handler
// necessary rather than stylistic — there has to be something left to restore *from*.
//
// The date is captured before the deletion and compared after the cancel, so the restoration is shown to
// return the original schedule rather than a fresh one: nothing in the walk re-enables rotation, so a
// handler that recomputed the enable date on the way back would be reporting a rotation it invented.
func TestKMSGetKeyRotationStatus_APendingDeletionKeyReportsFalseUntilTheDeletionIsCancelled(t *testing.T) {
	t.Parallel()

	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	pinned := kmsPinnedClock(ts, kmsRotationEpoch)
	kmsEnableRotation(t, ts, keyID, 90)

	before := kmsRotationStatusBody(t, ts, keyID)
	require.True(t, kmsRequireBool(t, before, "KeyRotationEnabled"), "rotation is on")
	scheduled := kmsNextRotationDate(t, before)
	require.WithinDuration(t, pinned.AddDate(0, 0, 90), scheduled, kmsRotationDateTolerance)

	kmsScheduleDeletion(t, ts, keyID, 7)

	pending := kmsRotationStatusBody(t, ts, keyID)
	assert.False(t, kmsRequireBool(t, pending, "KeyRotationEnabled"),
		"while a KMS key is pending deletion, its key rotation status is false")
	kmsRequireNoSchedule(t, pending, "while the key is pending deletion")

	kmsLeaveState(t, ts, keyID, "CancelKeyDeletion")

	restored := kmsRotationStatusBody(t, ts, keyID)
	assert.True(t, kmsRequireBool(t, restored, "KeyRotationEnabled"),
		"if you cancel the deletion, the original key rotation status returns to true")
	assert.Equal(t, 90, kmsRotationMember(t, restored, "RotationPeriodInDays"),
		"the period comes back with the status")
	assert.Equal(t, scheduled.Unix(), kmsNextRotationDate(t, restored).Unix(),
		fmt.Sprintf("the restored schedule is the original one, not a new one counted from the cancel "+
			"(%s)", scheduled.UTC()))
}
