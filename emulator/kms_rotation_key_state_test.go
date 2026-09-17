package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// EnableKeyRotation and DisableKeyRotation against a key whose state forbids them — #949.
//
// Both operations carry the sentence "the KMS key that you use for this operation must be in a
// compatible key state" and substrate checked no state at all: it wrote RotationEnabled and answered
// 200 whatever the key was. #923 recorded the gap while fixing DisabledException's status, and named
// it as a missing refusal rather than folding it into a status change.
//
// Three things are asserted here that a narrower test would miss.
//
//  1. **Both published codes, not one.** The developer guide's "Key states of AWS KMS keys" table
//     refuses a Disabled key and a key pending deletion with *different* codes — footnote [1]
//     DisabledException and footnote [3] KMSInvalidStateException. Substrate can reach both states,
//     because ScheduleKeyDeletion clears Enabled at the same time as it writes the state, so a lone
//     !Enabled check would answer the wrong one of the two. Each state is asserted against its own
//     code.
//
//  2. **That the refusal wrote nothing**, read back through GetKeyRotationStatus over the wire rather
//     than by inspecting state. A guard placed after the assignment would still answer the right code
//     while leaving RotationEnabled changed, and only the read-back distinguishes the two.
//
//  3. **That GetKeyRotationStatus itself is not guarded.** Its row in the same table permits Enabled,
//     Disabled and pending deletion alike, and its page publishes neither code — so this is the one
//     of the three rotation operations that must keep answering 200, and a sweep that guarded "every
//     key-state-sensitive operation" would break it silently.
//
// Every call goes over the wire and every state is reached through the operation a consumer would use
// — DisableKey and ScheduleKeyDeletion — rather than by writing a key record, per #765.

// kmsRotationStatus reads a key's rotation flag through GetKeyRotationStatus and requires the call to
// have succeeded.
//
// It is the read-back the issue asks for: asserting that a refused call wrote nothing has to go
// through the operation that reports the value, because a state inspection could agree with a handler
// that never persisted anything.
func kmsRotationStatus(t *testing.T, ts *emulator.TestServer, keyID string) bool {
	t.Helper()
	var out struct {
		KeyRotationEnabled bool `json:"KeyRotationEnabled"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "GetKeyRotationStatus",
			map[string]any{"KeyId": keyID}), &out)
	require.Empty(t, code, "GetKeyRotationStatus")
	require.Equal(t, http.StatusOK, status, "GetKeyRotationStatus")
	return out.KeyRotationEnabled
}

// kmsRotationStates is the two key states substrate can put a key into that both rotation operations
// must refuse, each with the code the key-state table gives it.
//
// PendingImport, Unavailable, Creating and Updating are refused by the same table rows and are absent
// because substrate never writes them: ScheduleKeyDeletion is the only writer of a state other than
// Enabled or Disabled. That is recorded rather than tested — a test would have to fabricate a key
// record no operation can produce.
var kmsRotationStates = []struct {
	name string
	// reach puts an existing, enabled key into the state, through the operation a consumer would use.
	reach string
	code  string
}{
	{"a disabled key", "DisableKey", "DisabledException"},
	{"a key pending deletion", "ScheduleKeyDeletion", "KMSInvalidStateException"},
}

// TestKMSKeyRotation_AStateThatForbidsRotationIsRefused is the issue's first two criteria, plus the
// distinction between the two codes that a single-state test cannot make.
func TestKMSKeyRotation_AStateThatForbidsRotationIsRefused(t *testing.T) {
	t.Parallel()

	for _, state := range kmsRotationStates {
		for _, op := range []string{"EnableKeyRotation", "DisableKeyRotation"} {
			t.Run(op+"/"+state.name, func(t *testing.T) {
				t.Parallel()

				ts := arnGuardServer(t)
				_, keyID := createKMSKey(t, ts)

				status, code := kmsCall(t, ts, state.reach, map[string]any{"KeyId": keyID})
				require.Empty(t, code, "%s to reach %s", state.reach, state.name)
				require.Equal(t, http.StatusOK, status, "%s to reach %s", state.reach, state.name)

				gotStatus, gotCode := kmsCall(t, ts, op, map[string]any{"KeyId": keyID})
				assert.Equal(t, state.code, gotCode, "%s against %s", op, state.name)
				assert.Equal(t, http.StatusBadRequest, gotStatus,
					"%s against %s — KMS publishes no status but 400 and 500, and neither of these "+
						"two codes is a 500", op, state.name)
			})
		}
	}
}

// TestKMSKeyRotation_ARefusalLeavesTheRotationFlagAlone is the issue's third criterion: the guard runs
// before the write, not beside it.
//
// Both directions are covered, and they are not symmetric. Refusing EnableKeyRotation on a key whose
// rotation is already off could be satisfied by a handler that wrote true and then refused, so the
// flag is turned *on* first in the second half — a handler that assigned before guarding would report
// false there, which no ordering of a correct implementation produces.
func TestKMSKeyRotation_ARefusalLeavesTheRotationFlagAlone(t *testing.T) {
	t.Parallel()

	for _, state := range kmsRotationStates {
		t.Run(state.name, func(t *testing.T) {
			t.Parallel()

			t.Run("EnableKeyRotation does not turn rotation on", func(t *testing.T) {
				ts := arnGuardServer(t)
				_, keyID := createKMSKey(t, ts)
				require.False(t, kmsRotationStatus(t, ts, keyID), "a new key does not rotate")

				status, code := kmsCall(t, ts, state.reach, map[string]any{"KeyId": keyID})
				require.Empty(t, code, state.reach)
				require.Equal(t, http.StatusOK, status, state.reach)

				_, code = kmsCall(t, ts, "EnableKeyRotation", map[string]any{"KeyId": keyID})
				require.Equal(t, state.code, code, "EnableKeyRotation against %s", state.name)

				assert.False(t, kmsRotationStatus(t, ts, keyID),
					"the refused EnableKeyRotation must not have written RotationEnabled")
			})

			t.Run("DisableKeyRotation does not turn rotation off", func(t *testing.T) {
				ts := arnGuardServer(t)
				_, keyID := createKMSKey(t, ts)

				// Turned on while the key still permits it, so the refusal below has something to undo.
				status, code := kmsCall(t, ts, "EnableKeyRotation", map[string]any{"KeyId": keyID})
				require.Empty(t, code, "EnableKeyRotation on an enabled key")
				require.Equal(t, http.StatusOK, status, "EnableKeyRotation on an enabled key")
				require.True(t, kmsRotationStatus(t, ts, keyID), "rotation is on")

				status, code = kmsCall(t, ts, state.reach, map[string]any{"KeyId": keyID})
				require.Empty(t, code, state.reach)
				require.Equal(t, http.StatusOK, status, state.reach)

				_, code = kmsCall(t, ts, "DisableKeyRotation", map[string]any{"KeyId": keyID})
				require.Equal(t, state.code, code, "DisableKeyRotation against %s", state.name)

				assert.True(t, kmsRotationStatus(t, ts, keyID),
					"the refused DisableKeyRotation must not have cleared RotationEnabled")
			})
		})
	}
}

// TestKMSGetKeyRotationStatus_AnswersForADisabledAndAPendingDeletionKey pins the operation that must
// *not* gain a guard, which is the criterion #949 adds for the benefit of a later change rather than
// for this one.
//
// API_GetKeyRotationStatus publishes DependencyTimeoutException, InvalidArnException,
// KMSInternalException, KMSInvalidStateException, NotFoundException and UnsupportedOperationException
// — and its row in the key-state table permits Enabled, Disabled and pending deletion alike, marking
// only PendingImport, Unavailable, Creating and Updating as refused. None of those four is reachable
// in substrate, so every state substrate can produce answers 200 here, and a sweep that guarded this
// operation "for consistency" with its two siblings would introduce a refusal AWS does not have.
func TestKMSGetKeyRotationStatus_AnswersForADisabledAndAPendingDeletionKey(t *testing.T) {
	t.Parallel()

	for _, state := range kmsRotationStates {
		t.Run(state.name, func(t *testing.T) {
			t.Parallel()

			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			// Set while the key still permits it, so the read below has a value to report rather than
			// the zero one it would report anyway.
			status, code := kmsCall(t, ts, "EnableKeyRotation", map[string]any{"KeyId": keyID})
			require.Empty(t, code, "EnableKeyRotation on an enabled key")
			require.Equal(t, http.StatusOK, status, "EnableKeyRotation on an enabled key")

			status, code = kmsCall(t, ts, state.reach, map[string]any{"KeyId": keyID})
			require.Empty(t, code, state.reach)
			require.Equal(t, http.StatusOK, status, state.reach)

			var out struct {
				KeyRotationEnabled bool `json:"KeyRotationEnabled"`
			}
			status, code = decodeAWSResponse(t,
				signedRequest(t, ts, kmsTarget, taggingTestAccount, "GetKeyRotationStatus",
					map[string]any{"KeyId": keyID}), &out)
			assert.Empty(t, code, "GetKeyRotationStatus against %s", state.name)
			assert.Equal(t, http.StatusOK, status, "GetKeyRotationStatus against %s", state.name)
			assert.True(t, out.KeyRotationEnabled,
				"the value set before the key changed state is still reported")
		})
	}
}

// TestKMSKeyRotation_AnEnabledKeyStillRotates is the guard against the two refusals above being
// reached by something other than the key state.
//
// Without it, a handler that refused every key would satisfy every assertion in this file. Both
// operations are exercised in both directions on one key, and the flag is read back after each so the
// nominal path is shown to write as well as to answer 200.
func TestKMSKeyRotation_AnEnabledKeyStillRotates(t *testing.T) {
	t.Parallel()

	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	for _, step := range []struct {
		op   string
		want bool
	}{
		{"EnableKeyRotation", true},
		{"DisableKeyRotation", false},
		// Twice through, because an idempotent repeat is the case a guard reading RotationEnabled
		// instead of the key state would refuse.
		{"EnableKeyRotation", true},
		{"EnableKeyRotation", true},
	} {
		status, code := kmsCall(t, ts, step.op, map[string]any{"KeyId": keyID})
		require.Empty(t, code, "%s on an enabled key", step.op)
		require.Equal(t, http.StatusOK, status, "%s on an enabled key", step.op)
		assert.Equal(t, step.want, kmsRotationStatus(t, ts, keyID), "after %s", step.op)
	}

	// A key re-enabled after being disabled rotates again, so the refusal is a function of the current
	// state rather than of anything the key remembers.
	status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")

	_, code = kmsCall(t, ts, "EnableKeyRotation", map[string]any{"KeyId": keyID})
	require.Equal(t, "DisabledException", code, "EnableKeyRotation while disabled")

	status, code = kmsCall(t, ts, "EnableKey", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "EnableKey")
	require.Equal(t, http.StatusOK, status, "EnableKey")

	status, code = kmsCall(t, ts, "DisableKeyRotation", map[string]any{"KeyId": keyID})
	assert.Empty(t, code, "DisableKeyRotation once the key is enabled again")
	assert.Equal(t, http.StatusOK, status, "DisableKeyRotation once the key is enabled again")
	assert.False(t, kmsRotationStatus(t, ts, keyID), "rotation is off again")
}
