package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #972: EnableKeyRotation and DisableKeyRotation against a key whose *type* cannot rotate.
//
// AWS states the restriction twice on API_EnableKeyRotation — once in the prose and once on the KeyId
// parameter — and repeats it verbatim on API_DisableKeyRotation and API_GetKeyRotationStatus:
// "automatic key rotation is supported only on symmetric encryption KMS keys. You cannot enable
// automatic rotation of asymmetric KMS keys, HMAC KMS keys, KMS keys with imported key material, or KMS
// keys in a custom key store." Substrate checked none of it: EnableKeyRotation on an RSA key answered
// 200 and wrote RotationEnabled, so GetKeyRotationStatus then reported a rotation schedule for a key
// AWS will never rotate.
//
// It is the third condition the rotation pair owes and the second one it did not check. #964 added the
// range on RotationPeriodInDays, #949 added the key state, and this is the key type — a permanent
// property of the key rather than a transient one, which is why it is ordered first (see
// [TestKMSKeyRotation_TheKeyTypeIsRefusedBeforeTheKeyState]).
//
// Four things are asserted:
//
//  1. **Every unsupported family is refused, on both operations.** The discriminator is KeySpec, so one
//     equality test in the plugin covers the asymmetric, HMAC and ML-DSA families at once — which means
//     a test of RSA alone would not distinguish a real check from `strings.HasPrefix(spec, "RSA")`. Each
//     family AWS names, plus the two it implies, gets a row.
//  2. **The refusal precedes the key-state refusal**, so a disabled asymmetric key hears about its
//     type. This is the assertion the ordering exists for: a caller told "enable the key and retry"
//     would retry forever.
//  3. **The refusal writes nothing**, read back through GetKeyRotationStatus over the wire per #949's
//     third criterion.
//  4. **A symmetric key still rotates**, with the spec sent explicitly. Without it a handler that
//     refused every key would satisfy the three assertions above, and the explicit spec additionally
//     shows the guard reads the *stored* value rather than passing an empty string.
//
// GetKeyRotationStatus is deliberately not refused for these keys, and that is asserted too. The
// sentence forbids *enabling* rotation on such a key, not asking whether it rotates, and the operation's
// own key-state row permits the read; answering false for a key that will never rotate is a true answer.
//
// Every non-symmetric key below is created through the gap #977 records — CreateKey validates KeySpec
// against nothing — which is the same route kms_encryption_algorithm_test.go takes, and the same reason
// #977 must re-verify this file when it closes it. Three of AWS's five restrictions are unreachable
// rather than untested: imported key material, a custom key store and an AWS managed key each need a
// member substrate does not model, and [emulator.KMSKey] is where that is recorded.
//
// Every call goes over the wire per #765, and every refusal pairs the code with the status per #923.

// kmsRotationOperations is the two operations the key-type guard applies to.
//
// Both, because their pages carry the identical sentence and the identical seven-error list. Turning
// rotation *off* on an asymmetric key looks harmless — it is already off and always will be — and AWS
// refuses it anyway, which is the same argument #949 rejected for the key state.
var kmsRotationOperations = []string{"EnableKeyRotation", "DisableKeyRotation"}

// kmsUnrotatableKeySpecs is one key spec per family that supports no automatic rotation.
//
// Verified against the developer guide's key spec reference. AWS's sentence names two families
// explicitly — asymmetric and HMAC — and the reference supplies the rest: the ECC specs sign or derive
// and the ML-DSA specs sign, so neither can rotate either, and SM2 is asymmetric under a
// China-Regions-only name that a prefix test on "RSA" or "ECC" would miss.
//
// One row per family rather than all fifteen specs: the plugin tests KeySpec for equality with
// SYMMETRIC_DEFAULT, so a second RSA size would exercise no new code. What the five rows buy is a
// guard against the check being written as a family test — which is the shape the sentence's own wording
// ("asymmetric KMS keys, HMAC KMS keys") invites.
var kmsUnrotatableKeySpecs = []struct {
	spec   string
	family string
}{
	{"RSA_2048", "asymmetric encryption"},
	{"ECC_NIST_P256", "asymmetric signing"},
	{"SM2", "asymmetric, China Regions only"},
	{"HMAC_256", "MAC generation"},
	{"ML_DSA_44", "post-quantum signing"},
}

// TestKMSKeyRotation_AKeyTypeThatCannotRotateIsRefused is assertion 1.
func TestKMSKeyRotation_AKeyTypeThatCannotRotateIsRefused(t *testing.T) {
	t.Parallel()

	for _, key := range kmsUnrotatableKeySpecs {
		for _, op := range kmsRotationOperations {
			t.Run(op+"/"+key.spec, func(t *testing.T) {
				t.Parallel()

				ts := arnGuardServer(t)
				_, keyID := createKMSKeySpec(t, ts, key.spec)

				status, code, message := kmsRefusal(t, ts, op, map[string]any{"KeyId": keyID})
				assert.Equal(t, "UnsupportedOperationException", code,
					"%s against a %s key (%s)", op, key.spec, key.family)
				assert.Equal(t, http.StatusBadRequest, status,
					"%s against a %s key — KMS publishes no status but 400 and 500, and this code is "+
						"not a 500", op, key.spec)
				assert.Contains(t, message, key.spec,
					"the message names the key spec, which is what tells this refusal from a "+
						"key-state one at the same code's status")
			})
		}
	}
}

// TestKMSKeyRotation_TheKeyTypeIsRefusedBeforeTheKeyState is assertion 2.
//
// A disabled asymmetric key and an asymmetric key pending deletion each have two applicable refusals,
// and AWS publishes no precedence between them, so the order is substrate's reading — argued where the
// guard lives. What makes it more than a preference is that the two remedies differ in kind: a key state
// has one (EnableKey, or CancelKeyDeletion and then EnableKey) and a key type has none, so answering the
// state first would send a caller round a loop that cannot terminate.
//
// Both states are here because they come from different arms of the key-state check, and either arm
// running first would be the defect.
func TestKMSKeyRotation_TheKeyTypeIsRefusedBeforeTheKeyState(t *testing.T) {
	t.Parallel()

	for _, state := range kmsRotationStates {
		for _, op := range kmsRotationOperations {
			t.Run(op+"/"+state.name, func(t *testing.T) {
				t.Parallel()

				ts := arnGuardServer(t)
				_, keyID := createKMSKeySpec(t, ts, "RSA_2048")

				status, code := kmsCall(t, ts, state.reach, map[string]any{"KeyId": keyID})
				require.Empty(t, code, "%s to reach %s", state.reach, state.name)
				require.Equal(t, http.StatusOK, status, "%s to reach %s", state.reach, state.name)

				gotStatus, gotCode := kmsCall(t, ts, op, map[string]any{"KeyId": keyID})
				assert.Equal(t, "UnsupportedOperationException", gotCode,
					"%s against an RSA key that is also %s answers the key type, not %s",
					op, state.name, state.code)
				assert.Equal(t, http.StatusBadRequest, gotStatus, "%s against %s", op, state.name)
			})
		}
	}
}

// TestKMSKeyRotation_ARefusedKeyTypeWritesNothing is assertion 3, and the assertion that
// GetKeyRotationStatus is not guarded on the key type.
//
// The two travel together because they are one call: reading the flag back is how the write is shown not
// to have happened, and the read succeeding on an asymmetric key is the other half of #972's decision.
// A guard copied from the rotation pair into this operation would fail here rather than silently
// introducing a refusal AWS does not publish — which is the failure mode #949's own third criterion
// exists to catch, one operation over.
func TestKMSKeyRotation_ARefusedKeyTypeWritesNothing(t *testing.T) {
	t.Parallel()

	ts := arnGuardServer(t)
	_, keyID := createKMSKeySpec(t, ts, "RSA_2048")

	assert.False(t, kmsRotationStatus(t, ts, keyID),
		"GetKeyRotationStatus answers for an asymmetric key rather than refusing it, and the answer "+
			"is false: it will never rotate")

	for _, op := range kmsRotationOperations {
		_, code := kmsCall(t, ts, op, map[string]any{"KeyId": keyID})
		require.Equal(t, "UnsupportedOperationException", code, "%s against an RSA key", op)
	}

	assert.False(t, kmsRotationStatus(t, ts, keyID),
		"neither refused call wrote RotationEnabled")
}

// TestKMSKeyRotation_ASymmetricKeyRotatesWithTheSpecSentExplicitly is assertion 4.
//
// [TestKMSKeyRotation_AnEnabledKeyStillRotates] already covers the nominal path for a key created with
// an empty body, which is how every other KMS test makes one. This covers the same path for a key whose
// SYMMETRIC_DEFAULT spec arrived in the request, so a guard that compared against "" — the value a
// pre-#972 key record would carry if CreateKey had not defaulted it — is caught rather than passing on
// the default's back.
func TestKMSKeyRotation_ASymmetricKeyRotatesWithTheSpecSentExplicitly(t *testing.T) {
	t.Parallel()

	ts := arnGuardServer(t)
	_, keyID := createKMSKeySpec(t, ts, "SYMMETRIC_DEFAULT")

	for _, step := range []struct {
		op   string
		want bool
	}{
		{"EnableKeyRotation", true},
		{"DisableKeyRotation", false},
	} {
		status, code := kmsCall(t, ts, step.op, map[string]any{"KeyId": keyID})
		require.Empty(t, code, "%s on a symmetric key", step.op)
		require.Equal(t, http.StatusOK, status, "%s on a symmetric key", step.op)
		assert.Equal(t, step.want, kmsRotationStatus(t, ts, keyID), "after %s", step.op)
	}
}
