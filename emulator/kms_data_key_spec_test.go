package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #988: GenerateDataKey and GenerateDataKeyWithoutPlaintext require a symmetric encryption KMS key, and
// substrate accepted an asymmetric one.
//
// The only key-type check either operation made was [kmsKeyUsageError], which tests the **usage**: an
// RSA_2048 key created with KeyUsage ENCRYPT_DECRYPT — a pair AWS publishes, and CreateKey must accept —
// passed it and was handed a wrapped data key and a 200. AWS states the restriction four times over
// between the two pages, twice in a description and twice on the KeyId parameter itself: "you cannot use an
// asymmetric KMS key to encrypt data keys", and "you cannot specify an asymmetric KMS key or a KMS key in a
// custom key store".
//
// Five assertions:
//
//  1. **A symmetric encryption key still wraps a data key**, at both operations, and the response is
//     unchanged. This is what the other four are measured against: refusing every key would satisfy them
//     and break the operations outright.
//  2. **Every key spec that reaches this check and is not SYMMETRIC_DEFAULT is refused**, at both
//     operations, with InvalidKeyUsageException/400 and a message naming the spec. All four are walked
//     rather than one RSA spec standing in, because the refusal is a property of the spec table and SM2 is
//     the spec a check written around the string "RSA" would miss.
//  3. **The refusal mints nothing.** The body carries no CiphertextBlob and no Plaintext, which is the
//     observable form of the check running before the data key is generated rather than after.
//  4. **The refusal is ordered against its two neighbors**, and each side is asserted through the message
//     rather than the code, because [kmsKeyUsageError] answers the *same* code and a key-state refusal is
//     the same 400. An RSA signing key reads the usage refusal; a *disabled* RSA encryption key reads this
//     one rather than DisabledException.
//  5. **The two operations answer identically** — same code, same message, for the same key. #961 is the
//     precedent for asserting it rather than assuming it: it found GenerateDataKeyWithoutPlaintext
//     refusing nothing while its four siblings each refused something.
//
// Every call goes over the wire (#765) and every refusal pairs the code with the status (#923).

// kmsDataKeyOperations is the two operations that wrap a data key under a KMS key.
//
// Both are walked at every assertion below rather than one standing in for the other. The pages publish the
// identical restriction and the identical nine errors, and GenerateDataKeyWithoutPlaintext is the operation
// #961 caught diverging, so "the sibling agrees" is a claim about this service's history and not a
// formality.
var kmsDataKeyOperations = []string{"GenerateDataKey", "GenerateDataKeyWithoutPlaintext"}

// kmsEncryptDecryptKeySpecs is every key spec that can carry KeyUsage ENCRYPT_DECRYPT, paired with whether
// it can wrap a data key.
//
// Transcribed from the developer guide's key spec reference — the specs with an encryption algorithm are
// SYMMETRIC_DEFAULT, the three RSA sizes and SM2 — rather than read from
// [kmsEncryptionAlgorithmsByKeySpec], for the reason [kmsSpecAdmissibleUsages] records: a test that shares
// the structure under test agrees with a wrong structure.
//
// It is the set that *reaches* this refusal, which is narrower than the set the predicate rejects. The four
// HMAC specs are also not symmetric encryption keys, and are unreachable here because their usage is
// GENERATE_VERIFY_MAC and [kmsKeyUsageError] runs first; the signing and key-agreement specs the same.
// Walking only the reachable set is what keeps assertion 4's ordering claim meaningful.
var kmsEncryptDecryptKeySpecs = []struct {
	keySpec  string
	wrapsKey bool
}{
	{"SYMMETRIC_DEFAULT", true},
	{"RSA_2048", false},
	{"RSA_3072", false},
	{"RSA_4096", false},
	{"SM2", false},
}

// kmsDataKeyErrorBody posts one of the two operations and returns the status and the whole decoded body.
//
// The body rather than the code and message alone, because assertion 3 is about what the response does
// *not* carry: a check that ran after the mint would answer the same code with a CiphertextBlob beside it,
// and [kmsRefusal] would report that as a clean refusal.
func kmsDataKeyErrorBody(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, map[string]json.RawMessage) {
	t.Helper()

	resp := signedRequest(t, ts, kmsTarget, taggingTestAccount, op, body)
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read the %s response body", op)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &out), "decode the %s response %s", op, raw)
	return resp.StatusCode, out
}

// TestKMSDataKeySpec_ASymmetricEncryptionKeyStillWrapsADataKey is assertion 1.
//
// SYMMETRIC_DEFAULT is the spec every existing caller uses and the one an empty CreateKey body produces, so
// this is the assertion a refusal written one condition too wide would break. Both members AWS publishes on
// the success path are checked at the operation that carries them, since a guard placed before the mint is
// exactly where a response could lose them.
func TestKMSDataKeySpec_ASymmetricEncryptionKeyStillWrapsADataKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	out := kmsRawBody(t, ts, "GenerateDataKey", map[string]any{"KeyId": keyID, "KeySpec": "AES_256"})
	assert.Contains(t, out, "CiphertextBlob", "GenerateDataKey wraps the data key it minted")
	assert.Contains(t, out, "Plaintext", "and hands back the plaintext copy, which is what it is for")

	out = kmsRawBody(t, ts, "GenerateDataKeyWithoutPlaintext",
		map[string]any{"KeyId": keyID, "KeySpec": "AES_256"})
	assert.Contains(t, out, "CiphertextBlob", "the sibling wraps one too")
	assert.NotContains(t, out, "Plaintext",
		"and publishes no Plaintext, which is the whole difference between the two")
}

// TestKMSDataKeySpec_AnAsymmetricKeyIsRefused is assertions 2 and 3, and it is the test that replaced the
// omission #978 recorded at these two sites (kms_key_material_id_test.go).
//
// The code is substrate's reading of an unsplit bullet rather than a published rule — neither of
// InvalidKeyUsageException's two bullets describes this condition exactly, since the KeyUsage is the one
// thing about such a key that is correct — so [kmsInvalidKeySpecForDataKey] carries the argument and this
// pins the answer. The message is asserted because the code cannot distinguish this refusal from the two
// others that share it, and because a caller holding an asymmetric key needs to be told which operation
// AWS sends it to.
func TestKMSDataKeySpec_AnAsymmetricKeyIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, spec := range kmsEncryptDecryptKeySpecs {
		if spec.wrapsKey {
			continue
		}
		for _, op := range kmsDataKeyOperations {
			t.Run(spec.keySpec+"/"+op, func(t *testing.T) {
				_, keyID := createKMSKeySpecUsage(t, ts, spec.keySpec, "ENCRYPT_DECRYPT")

				status, code, message := kmsRefusal(t, ts, op, map[string]any{
					"KeyId":   keyID,
					"KeySpec": "AES_256",
				})
				assert.Equal(t, "InvalidKeyUsageException", code,
					"the only code either page publishes about a key being the wrong kind")
				assert.Equal(t, http.StatusBadRequest, status, "published at 400")
				assert.Contains(t, message, spec.keySpec, "the refusal names the spec the key has")
				assert.Contains(t, message, "SYMMETRIC_DEFAULT", "and the one the operation requires")
				assert.Contains(t, message, "GenerateDataKeyPair",
					"and the operation AWS directs an asymmetric caller to, which substrate does not implement")

				status, body := kmsDataKeyErrorBody(t, ts, op, map[string]any{
					"KeyId":   keyID,
					"KeySpec": "AES_256",
				})
				require.Equal(t, http.StatusBadRequest, status, "the same request refused twice")
				assert.NotContains(t, body, "CiphertextBlob",
					"the check runs before the data key is wrapped, so there is nothing to report")
				assert.NotContains(t, body, "Plaintext",
					"and before it is minted, so no data key was generated and discarded")
			})
		}
	}
}

// TestKMSDataKeySpec_TheRefusalIsOrderedAgainstItsNeighbors is assertion 4.
//
// Both neighbors are indistinguishable from this refusal by code alone — [kmsKeyUsageError] answers the
// same InvalidKeyUsageException, and a disabled key answers a different code at the same 400 — so each side
// is asserted through the message. Nothing here is published: AWS states all three conditions and no
// precedence between them, and [kmsDataKeyKeySpecError] records the argument, which is that a key spec and
// a key usage are permanent where a key state has a remedy.
func TestKMSDataKeySpec_TheRefusalIsOrderedAgainstItsNeighbors(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, op := range kmsDataKeyOperations {
		t.Run(op+" answers the usage before the spec", func(t *testing.T) {
			_, signingKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "SIGN_VERIFY")

			status, code, message := kmsRefusal(t, ts, op, map[string]any{"KeyId": signingKeyID})
			assert.Equal(t, "InvalidKeyUsageException", code, "one code carries both refusals")
			assert.Equal(t, http.StatusBadRequest, status, "at the published 400")
			assert.Contains(t, message, "has key usage",
				"a signing key is wrong about the operation, which is the uniform refusal across all five")
			assert.NotContains(t, message, "has key spec",
				"and is not told about its spec, which would explain an RSA signing key differently from an ECC one")
		})

		t.Run(op+" answers the spec before the key state", func(t *testing.T) {
			_, rsaKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "ENCRYPT_DECRYPT")
			status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": rsaKeyID})
			require.Empty(t, code, "DisableKey")
			require.Equal(t, http.StatusOK, status, "DisableKey")

			status, code, message := kmsRefusal(t, ts, op, map[string]any{"KeyId": rsaKeyID})
			assert.Equal(t, "InvalidKeyUsageException", code,
				"the permanent property is answered first, so this is not DisabledException")
			assert.Equal(t, http.StatusBadRequest, status, "both candidates are 400, so only the code differs")
			assert.Contains(t, message, "has key spec",
				"enabling this key would not make the call succeed, and the refusal must not imply it would")
		})
	}
}

// TestKMSDataKeySpec_BothOperationsAnswerIdentically is assertion 5.
//
// The messages are compared to each other rather than each to a literal, which is the assertion that
// actually holds the shared helper in place: two sites could both answer InvalidKeyUsageException/400 with
// two different explanations and satisfy every assertion above.
func TestKMSDataKeySpec_BothOperationsAnswerIdentically(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, rsaKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "ENCRYPT_DECRYPT")

	first, firstCode, firstMessage := kmsRefusal(t, ts, "GenerateDataKey", map[string]any{"KeyId": rsaKeyID})
	second, secondCode, secondMessage := kmsRefusal(t, ts, "GenerateDataKeyWithoutPlaintext",
		map[string]any{"KeyId": rsaKeyID})

	assert.Equal(t, first, second, "one restriction, one status")
	assert.Equal(t, firstCode, secondCode, "one restriction, one code")
	assert.Equal(t, firstMessage, secondMessage,
		"and one message, because both refusals come from one helper rather than from two checks")
}
