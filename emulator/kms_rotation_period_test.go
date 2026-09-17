package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #964: EnableKeyRotation's RotationPeriodInDays, and #971: the RotationEnabled member DescribeKey
// invented.
//
// The two are one defect seen from opposite ends, which is why they are tested together: substrate did
// not report the rotation period on the operation AWS publishes it on, and did report rotation state on
// the shape AWS omits it from. Both are about where rotation state is observable.
//
// What is asserted, and why each needs its own case:
//
//  1. The period is stored and reported. #964's fourth criterion allowed for the value being
//     unobservable, in which case the honest fix was to validate and discard it. It is observable —
//     API_GetKeyRotationStatus publishes RotationPeriodInDays with the same 90-2560 range
//     EnableKeyRotation accepts — so the full round trip is the assertion.
//  2. Both bounds are accepted, so the range cannot be off by one, which #964 asks for explicitly.
//  3. An out-of-range value is refused with the range named. AWS publishes no code for this, so the code
//     is substrate's reading; see [kmsInvalidRotationPeriod].
//  4. An absent member means 365, and — the part that is substrate's reading rather than AWS's plain
//     text — a *second* call that omits it resets the period rather than preserving it.
//  5. DescribeKey does not report RotationEnabled, asserted against raw JSON, because a decoded struct
//     cannot tell an absent member from a false one and that is the whole distinction.
//
// Every call goes over the wire, per #765, and every refusal pairs the code with the status, per #923.

// kmsRotationBody reads a key's full GetKeyRotationStatus body undecoded.
//
// Raw members rather than a struct, for the reason #971 exists: whether RotationPeriodInDays is *present*
// is under test, and unmarshalling into an int would report an omitted member and a zero one alike.
func kmsRotationBody(t *testing.T, ts *emulator.TestServer, keyID string) map[string]json.RawMessage {
	t.Helper()
	return kmsRawBody(t, ts, "GetKeyRotationStatus", map[string]any{"KeyId": keyID})
}

// kmsRotationPeriod requires GetKeyRotationStatus to report a period and returns it.
func kmsRotationPeriod(t *testing.T, ts *emulator.TestServer, keyID string) int {
	t.Helper()
	body := kmsRotationBody(t, ts, keyID)
	raw, ok := body["RotationPeriodInDays"]
	require.True(t, ok, "GetKeyRotationStatus should report RotationPeriodInDays: %v", body)
	var period int
	require.NoError(t, json.Unmarshal(raw, &period), "decode RotationPeriodInDays")
	return period
}

// kmsEnableRotation posts EnableKeyRotation, omitting the period entirely when days is 0, and requires
// success.
//
// The zero-means-omit convention matches [kmsScheduleDeletion], and it is safe for the same reason: 0 is
// outside the published range, so no test needs to send it as a value. The one case that does — the
// explicit-zero refusal — builds its own body.
func kmsEnableRotation(t *testing.T, ts *emulator.TestServer, keyID string, days int) {
	t.Helper()
	body := map[string]any{"KeyId": keyID}
	if days != 0 {
		body["RotationPeriodInDays"] = days
	}
	status, code := kmsCall(t, ts, "EnableKeyRotation", body)
	require.Empty(t, code, "EnableKeyRotation with period %d", days)
	require.Equal(t, http.StatusOK, status, "EnableKeyRotation with period %d", days)
}

// TestKMSEnableKeyRotation_RecordsThePeriodAtBothBounds covers the round trip and #964's off-by-one
// criterion in one table.
//
// 90 and 2560 are the published bounds and both must be accepted; 180 is the value
// API_EnableKeyRotation's own example request sends. Before #964 every one of these answered 200 and
// reported nothing, because the handler decoded only KeyId.
func TestKMSEnableKeyRotation_RecordsThePeriodAtBothBounds(t *testing.T) {
	t.Parallel()

	for _, period := range []int{90, 180, 365, 2560} {
		t.Run(kmsPeriodName(period), func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			kmsEnableRotation(t, ts, keyID, period)

			assert.True(t, kmsRotationStatus(t, ts, keyID), "rotation is on")
			assert.Equal(t, period, kmsRotationPeriod(t, ts, keyID),
				"GetKeyRotationStatus reports the period EnableKeyRotation was given")
		})
	}
}

// kmsPeriodName labels a subtest with its period, so a failure names the value rather than an index.
func kmsPeriodName(period int) string {
	switch period {
	case kmsPublishedMinPeriod:
		return "the published minimum"
	case kmsPublishedMaxPeriod:
		return "the published maximum"
	case 365:
		return "the default, sent explicitly"
	default:
		return "AWS's own example value"
	}
}

// The published bounds, restated in the test package rather than exported from the plugin.
//
// A test that imported the constants it is checking would pass against a wrong pair of numbers, which is
// the one thing this table exists to catch.
const (
	kmsPublishedMinPeriod = 90
	kmsPublishedMaxPeriod = 2560
)

// TestKMSEnableKeyRotation_DefaultsToTheDocumentedThreeSixtyFive covers the absent member.
//
// API_EnableKeyRotation: "If no value is specified, the default value is 365 days", and
// API_GetKeyRotationStatus repeats it on the response element. So an omitted member is not an omitted
// answer.
func TestKMSEnableKeyRotation_DefaultsToTheDocumentedThreeSixtyFive(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	kmsEnableRotation(t, ts, keyID, 0) // no RotationPeriodInDays member at all

	assert.Equal(t, 365, kmsRotationPeriod(t, ts, keyID),
		"an absent RotationPeriodInDays reports as the documented default")
}

// TestKMSEnableKeyRotation_RefusesAPeriodOutsideTheRange covers the range, including the two values a
// pointer-typed member exists to tell apart.
//
// An explicit 0 is the case that would break under a plain int: it would decode identically to an absent
// member and be silently accepted as 365, which is #964's defect one level down rather than fixed. A
// negative value is the same trap from the other side.
//
// The refusal must also write nothing, so each case re-reads the status afterwards. A handler that
// validated after the write would satisfy the code assertion and still have turned rotation on — the
// property #949 established for this same pair of handlers.
func TestKMSEnableKeyRotation_RefusesAPeriodOutsideTheRange(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		period int
	}{
		{"an explicit zero, which an absent member must not be confused with", 0},
		{"a negative period", -1},
		{"one below the published minimum", kmsPublishedMinPeriod - 1},
		{"one above the published maximum", kmsPublishedMaxPeriod + 1},
		{"the deletion window's maximum, a plausible wrong analogy", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			status, code, message := kmsRefusal(t, ts, "EnableKeyRotation", map[string]any{
				"KeyId":                keyID,
				"RotationPeriodInDays": tc.period,
			})
			assert.Equal(t, "ValidationError", code, "EnableKeyRotation with %d", tc.period)
			assert.Equal(t, http.StatusBadRequest, status, "EnableKeyRotation with %d", tc.period)
			assert.Contains(t, message, "90", "the message names the lower bound")
			assert.Contains(t, message, "2560", "the message names the upper bound")

			assert.False(t, kmsRotationStatus(t, ts, keyID),
				"a refused EnableKeyRotation must not have turned rotation on")
		})
	}
}

// TestKMSEnableKeyRotation_RefusesABadPeriodBeforeResolvingTheKey pins an ordering substrate decides.
//
// A range violation does not depend on the key, so answering NotFoundException would point the caller at
// the wrong problem — they would go looking for a missing key when the number is what is wrong. AWS
// publishes no precedence, so this is recorded rather than matched, and the assertion is written so the
// two candidate answers are visibly different.
func TestKMSEnableKeyRotation_RefusesABadPeriodBeforeResolvingTheKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	status, code, _ := kmsRefusal(t, ts, "EnableKeyRotation", map[string]any{
		"KeyId":                kmsAbsentKeyID,
		"RotationPeriodInDays": 1,
	})
	assert.Equal(t, "ValidationError", code,
		"the out-of-range period is reported, not the absent key")
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestKMSEnableKeyRotation_ModifiesThePeriodOfAnAlreadyRotatingKey covers the sentence that makes the
// value worth storing rather than merely validating.
//
// API_EnableKeyRotation: "or you can use RotationPeriodInDays to modify the rotation period of a key that
// you previously enabled automatic key rotation on". So a second call is a legitimate change, not a
// conflict to refuse, and the new value must replace the old one rather than being ignored as redundant.
func TestKMSEnableKeyRotation_ModifiesThePeriodOfAnAlreadyRotatingKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	kmsEnableRotation(t, ts, keyID, 180)
	require.Equal(t, 180, kmsRotationPeriod(t, ts, keyID))

	kmsEnableRotation(t, ts, keyID, kmsPublishedMinPeriod)

	assert.Equal(t, kmsPublishedMinPeriod, kmsRotationPeriod(t, ts, keyID),
		"a second EnableKeyRotation modifies the period")
	assert.True(t, kmsRotationStatus(t, ts, keyID), "and rotation stays on")
}

// TestKMSEnableKeyRotation_OmittingThePeriodOnASecondCallResetsIt pins substrate's reading, and it is the
// case most likely to be "fixed" the other way by someone who has not read the page.
//
// The intuitive behavior is that omitting the member leaves the previous period alone. AWS's text does
// not say that: "if no value is specified, the default value is 365 days" is stated unconditionally, with
// no carve-out for a key that already rotates. So an omitted member means 365 every time, and a caller
// who wants to keep 180 has to send 180 again.
//
// Recorded as substrate's reading because AWS does not address the interaction directly. It is pinned
// here so a change of mind is a deliberate one against a failing test.
func TestKMSEnableKeyRotation_OmittingThePeriodOnASecondCallResetsIt(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	kmsEnableRotation(t, ts, keyID, 180)
	require.Equal(t, 180, kmsRotationPeriod(t, ts, keyID))

	kmsEnableRotation(t, ts, keyID, 0) // omitted, not 180 again

	assert.Equal(t, 365, kmsRotationPeriod(t, ts, keyID),
		"an omitted period resets to the default rather than preserving the previous value")
}

// TestKMSGetKeyRotationStatus_ReportsTheBareKeyIDForEitherInputForm covers a member #964 adds, and the
// distinction AWS draws precisely enough that echoing the input would be wrong.
//
// The response element is glossed only "Identifies the specified symmetric encryption KMS key", with none
// of the "Amazon Resource Name (key ARN)" wording API_ScheduleKeyDeletion and API_ReEncrypt use for their
// KeyId elements, and the page's sample renders a bare "1234abcd-...". So the answer is the key ID
// whichever form the caller addressed the key by — which is what makes echoing the request wrong rather
// than merely lazy.
func TestKMSGetKeyRotationStatus_ReportsTheBareKeyIDForEitherInputForm(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	for _, tc := range []struct {
		name  string
		input string
	}{
		{"addressed by key ID", keyID},
		{"addressed by key ARN", arn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := kmsRotationBody(t, ts, tc.input)
			var got string
			require.NoError(t, json.Unmarshal(body["KeyId"], &got), "decode KeyId")
			assert.Equal(t, keyID, got, "KeyId is the bare key ID, not the ARN and not the input")
			assert.NotEqual(t, arn, got, "KeyId is not the ARN")
		})
	}
}

// TestKMSGetKeyRotationStatus_OmitsThePeriodWhenRotationIsOff walks the member's presence across the
// operations that change it.
//
// AWS documents no period for a key that has never rotated, so emitting one would be inventing a fact —
// the honest-empty behavior #827 established. The disable step then re-checks it, because the stored
// value is deliberately *kept* across a DisableKeyRotation: what changes is whether it is reported.
//
// Presence is read from raw JSON throughout. A struct with an int would make "absent" and "0"
// indistinguishable, which is the only thing this test is about.
func TestKMSGetKeyRotationStatus_OmitsThePeriodWhenRotationIsOff(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	fresh := kmsRotationBody(t, ts, keyID)
	require.Contains(t, fresh, "KeyRotationEnabled", "the boolean is always reported")
	assert.NotContains(t, fresh, "RotationPeriodInDays",
		"a key that has never rotated has no period to report")

	kmsEnableRotation(t, ts, keyID, kmsPublishedMinPeriod)
	assert.Equal(t, kmsPublishedMinPeriod, kmsRotationPeriod(t, ts, keyID))

	status, code := kmsCall(t, ts, "DisableKeyRotation", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "DisableKeyRotation")
	require.Equal(t, http.StatusOK, status, "DisableKeyRotation")

	off := kmsRotationBody(t, ts, keyID)
	assert.NotContains(t, off, "RotationPeriodInDays",
		"the period is not reported once rotation is off")

	// Re-enabling without a period reports the default, which also shows the disable did not leave a
	// stale 90 behind where a caller would read it.
	kmsEnableRotation(t, ts, keyID, 0)
	assert.Equal(t, 365, kmsRotationPeriod(t, ts, keyID))
}

// TestKMSDescribeKey_DoesNotReportRotationEnabled is #971, and the assertion has to be on raw JSON.
//
// API_KeyMetadata publishes 26 members and RotationEnabled is not one of them; the string does not occur
// on the page. Substrate rendered it until #971, so a caller could branch on
// DescribeKey().KeyMetadata.RotationEnabled against the emulator and get an absent field from AWS.
//
// The second half is what stops this being a test that merely deletes coverage: rotation state must still
// be readable through GetKeyRotationStatus for the same key, so the removal is shown to have removed a
// duplicate route rather than the only one.
func TestKMSDescribeKey_DoesNotReportRotationEnabled(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	before := kmsKeyMetadata(t, ts, keyID)
	require.Contains(t, before, "KeyState", "the metadata is being read at all")
	assert.NotContains(t, before, "RotationEnabled",
		"API_KeyMetadata publishes no RotationEnabled member")

	// After enabling rotation is the case that matters: a handler that rendered the member from the
	// stored flag would look correct on a fresh key, where the flag is false and easy to overlook.
	kmsEnableRotation(t, ts, keyID, kmsPublishedMinPeriod)

	after := kmsKeyMetadata(t, ts, keyID)
	assert.NotContains(t, after, "RotationEnabled",
		"enabling rotation must not make DescribeKey report an unpublished member")

	assert.True(t, kmsRotationStatus(t, ts, keyID),
		"rotation state is still observable, through the operation AWS publishes it on")
}
