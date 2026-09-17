package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #968: EnableKey and DisableKey checked no key state, so EnableKey against a key pending deletion
// answered 200 and made it usable in one call.
//
// This is the last member of the key-state class in this package — #949 fixed the rotation pair, #961
// the five cryptographic operations, #963 the deletion pair — and it is the one where the missing
// refusal defeated a guarantee another operation had just established rather than merely permitting a
// call AWS refuses. API_CancelKeyDeletion makes recovery two calls: "when this operation succeeds, the
// key state of the KMS key is Disabled. To enable the KMS key, use EnableKey." An unguarded EnableKey
// skips the first one.
//
// Three things are asserted that a single refusal test would not:
//
//   - The refusal writes nothing, including the deletion date. A guard placed after the assignment
//     would answer the right code while having already abandoned the deletion, and #963 stores the date
//     precisely so that "unchanged" is observable over the wire.
//   - The documented recovery is the *only* path, asserted as one sequence rather than as two
//     independent tests: the short cut is refused, then the two calls succeed, with Encrypt as the
//     witness at each step. Reading KeyState back says the state changed; Encrypt says it means what it
//     says.
//   - The nominal transitions are untouched in both directions and repeatedly, so the guard is a
//     function of the current state and not of anything the key remembers.
//
// Every state is reached through the operation a consumer would use, per #765, and every refusal pairs
// the code with the status, per #923.

// kmsDeletionDate reads a key's DeletionDate through DescribeKey, requiring it to be present.
//
// Raw JSON, because "the date is unchanged" and "the date is gone" are the two outcomes under test and
// DescribeKey renders the member only while the key is pending deletion — so an absent member is a
// failure worth naming rather than a zero value to compare against.
func kmsDeletionDate(t *testing.T, ts *emulator.TestServer, keyID string) string {
	t.Helper()
	meta := kmsKeyMetadata(t, ts, keyID)
	raw, ok := meta["DeletionDate"]
	require.True(t, ok, "DescribeKey should report DeletionDate for a key pending deletion: %v", meta)
	return string(raw)
}

// TestKMSSetKeyState_RefusesAKeyPendingDeletion covers footnote [3] for both operations, and asserts the
// refusal wrote nothing.
//
// EnableKey and DisableKey have identical rows in the developer guide's key-state table, and
// PendingDeletion is the only refused row substrate can reach: ScheduleKeyDeletion is its sole writer of
// a state other than Enabled or Disabled, so PendingImport [5], Creating [14] and Updating [15] are
// recorded as unreachable rather than tested. Both operations are run against one table so a later
// change cannot guard one and leave the other.
//
// DisableKey is the less obvious half and is the reason the table has two rows rather than one. Its
// effect on a pending key looks harmless — the key is already unusable — but AWS refuses it, and
// allowing it would clear the deletion date and report a state the key is not in.
func TestKMSSetKeyState_RefusesAKeyPendingDeletion(t *testing.T) {
	t.Parallel()

	for _, op := range []string{"EnableKey", "DisableKey"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			kmsScheduleDeletion(t, ts, keyID, 7)
			scheduled := kmsDeletionDate(t, ts, keyID)

			status, code, message := kmsRefusal(t, ts, op, map[string]any{"KeyId": keyID})
			assert.Equal(t, "KMSInvalidStateException", code, "%s against a key pending deletion", op)
			assert.Equal(t, http.StatusBadRequest, status, "%s against a key pending deletion", op)
			assert.Contains(t, message, "PendingDeletion",
				"the message names the offending state, as footnote [3] does")

			meta := kmsKeyMetadata(t, ts, keyID)
			assert.JSONEq(t, `"PendingDeletion"`, string(meta["KeyState"]),
				"a refused %s leaves the key state alone", op)
			assert.JSONEq(t, `false`, string(meta["Enabled"]),
				"and leaves the enabled flag alone")
			assert.Equal(t, scheduled, kmsDeletionDate(t, ts, keyID),
				"and leaves the deletion date alone rather than abandoning the deletion")
		})
	}
}

// TestKMSEnableKey_CannotShortCircuitTheTwoStepRecovery is the assertion #968 exists for, and it has to
// be one sequence.
//
// Two separate tests — "EnableKey is refused" and "cancel then enable works" — would both pass against a
// version that refused every EnableKey, or against one that let CancelKeyDeletion leave the key Enabled.
// What matters is that the short cut is closed *and* the documented path is open, in that order, on one
// key.
//
// Encrypt is the witness at each step rather than KeyState, following #963: a state that reads correctly
// but does not gate the operations it is supposed to gate is the defect one level down.
func TestKMSEnableKey_CannotShortCircuitTheTwoStepRecovery(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	kmsScheduleDeletion(t, ts, keyID, 7)

	// Step one skipped: refused, and the key is no more usable than before.
	_, code, _ := kmsRefusal(t, ts, "EnableKey", map[string]any{"KeyId": keyID})
	require.Equal(t, "KMSInvalidStateException", code, "EnableKey on a key pending deletion")

	status, code := kmsCall(t, ts, "Encrypt", map[string]any{"KeyId": keyID, "Plaintext": "aGVsbG8="})
	assert.Equal(t, "KMSInvalidStateException", code, "Encrypt on a key still pending deletion")
	assert.Equal(t, http.StatusBadRequest, status, "Encrypt on a key still pending deletion")

	// Step one taken. The key is Disabled now, not Enabled, so Encrypt still refuses — with the other
	// code, which is what tells a caller the remedy has changed from CancelKeyDeletion to EnableKey.
	kmsRawBody(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})
	status, code = kmsCall(t, ts, "Encrypt", map[string]any{"KeyId": keyID, "Plaintext": "aGVsbG8="})
	assert.Equal(t, "DisabledException", code, "Encrypt after the cancel, before EnableKey")
	assert.Equal(t, http.StatusBadRequest, status, "Encrypt after the cancel, before EnableKey")

	// Step two, and only now is the key usable.
	kmsRawBody(t, ts, "EnableKey", map[string]any{"KeyId": keyID})
	status, code = kmsCall(t, ts, "Encrypt", map[string]any{"KeyId": keyID, "Plaintext": "aGVsbG8="})
	assert.Empty(t, code, "Encrypt after the documented two-step recovery")
	assert.Equal(t, http.StatusOK, status, "Encrypt after the documented two-step recovery")
}

// TestKMSSetKeyState_PermittedTransitionsAreUnaffected pins the two rows the table permits, which is the
// half a guard is most likely to get wrong.
//
// Both operations' rows permit Enabled *and* Disabled, so neither is idempotence-refusing: DisableKey on
// an already-disabled key succeeds, and EnableKey on an already-enabled one does too. That is why the
// rotation rule cannot be reused here — an enabled-key check would refuse a call AWS accepts — and it is
// the distinction #949's own record names when it explains why these two were left out of that pass.
//
// The cycle is run twice so that the guard cannot depend on how the key arrived at its current state.
func TestKMSSetKeyState_PermittedTransitionsAreUnaffected(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	for _, step := range []struct {
		op    string
		state string
		flag  string
	}{
		{"EnableKey", "Enabled", `true`}, // already enabled: permitted, not a conflict
		{"DisableKey", "Disabled", `false`},
		{"DisableKey", "Disabled", `false`}, // already disabled: permitted too
		{"EnableKey", "Enabled", `true`},
		{"DisableKey", "Disabled", `false`},
		{"EnableKey", "Enabled", `true`},
	} {
		status, code := kmsCall(t, ts, step.op, map[string]any{"KeyId": keyID})
		require.Empty(t, code, "%s from %s", step.op, step.state)
		require.Equal(t, http.StatusOK, status, "%s from %s", step.op, step.state)

		meta := kmsKeyMetadata(t, ts, keyID)
		assert.JSONEq(t, `"`+step.state+`"`, string(meta["KeyState"]), "%s writes its state", step.op)
		assert.JSONEq(t, step.flag, string(meta["Enabled"]),
			"API_KeyMetadata: when KeyState is Enabled this value is true, otherwise it is false")
		assert.NotContains(t, meta, "DeletionDate",
			"a key that is not pending deletion reports no deletion date")
	}
}
