package emulator_test

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #991: Encrypt's Plaintext member is refused for the reasons API_Encrypt publishes, with a code
// API_Encrypt publishes, at the point in the handler each reason belongs to.
//
// Substrate enforced no length at all and answered InvalidCiphertextException for an undecodable
// value — a code that page does not list, and which belongs to the two operations that *consume* a
// ciphertext. The page carries two separate constraints: the member's own "Length Constraints:
// Minimum length of 1. Maximum length of 4096", and a per-key table an order of magnitude smaller
// under "the maximum size of the data that you can encrypt varies with the type of KMS key and the
// encryption algorithm that you choose".
//
// Five assertions:
//
//  1. **A well-formed Plaintext still encrypts**, at the boundary as well as in the middle: exactly
//     the maximum admitted by each key spec and algorithm answers 200 with all three published
//     response members. This is what the four refusals are measured against — refusing everything
//     satisfies them and breaks the operation.
//  2. **The published 1-4096 range is enforced**, at both ends, with ValidationError/400.
//  3. **An undecodable Plaintext is ValidationError, not InvalidCiphertextException.** The old code is
//     asserted absent by name, because that is the correction rather than a side effect of it.
//  4. **Both key-independent refusals happen before the key is resolved.** A request whose Plaintext
//     is unusable and whose KeyId names no key hears about the Plaintext, and the same request against
//     a *disabled* key and against a signing key does too. This is the ordering half of #991 and the
//     half no code assertion alone can show.
//  5. **The per-spec maximum is enforced for every admissible pair**, one byte over answering
//     ValidationError/400 with a message naming the spec, the algorithm, the size sent and the
//     maximum — and it runs *after* the algorithm-for-key check, so an RSA key sent the default
//     SYMMETRIC_DEFAULT algorithm hears about the algorithm rather than about the length.
//
// Assertions 1 and 5 together are also what stands in for a unit test that the size table and
// [kmsEncryptionAlgorithmsByKeySpec] agree: every pair the latter admits is exercised at its maximum
// and one byte beyond it over the wire, so a pair missing from the size table shows up as a request
// that should have been refused and was not. Every call goes over the wire (#765) and every refusal
// pairs the code with the status (#923).

// kmsPlaintextMaxima is the maximum plaintext AWS publishes for each key spec and encryption
// algorithm, in bytes.
//
// Transcribed from API_Encrypt's own list — the one under "the maximum size of the data that you can
// encrypt varies with the type of KMS key and the encryption algorithm that you choose" — rather than
// read from the map under test, for the reason [kmsSpecAdmissibleUsages] records: a test that shares
// the structure under test agrees with a wrong structure. AWS heads its RSA entries with a key spec
// and its SM2 entry with an algorithm; both coordinates are named here, since the RSA specs need
// both and naming them uniformly is what makes the pairs enumerable.
//
// SYMMETRIC_DEFAULT's 4096 coincides with the member's own maximum, which means the per-spec refusal
// is unobservable for it: a symmetric request one byte over is caught by the range check before any
// key is loaded. That is not a gap — it is why assertion 5 walks the asymmetric specs for the
// refusal while assertion 1 walks all five for the success.
var kmsPlaintextMaxima = []struct {
	keySpec   string
	algorithm string
	maximum   int
}{
	{"SYMMETRIC_DEFAULT", "SYMMETRIC_DEFAULT", 4096},
	{"RSA_2048", "RSAES_OAEP_SHA_1", 214},
	{"RSA_2048", "RSAES_OAEP_SHA_256", 190},
	{"RSA_3072", "RSAES_OAEP_SHA_1", 342},
	{"RSA_3072", "RSAES_OAEP_SHA_256", 318},
	{"RSA_4096", "RSAES_OAEP_SHA_1", 470},
	{"RSA_4096", "RSAES_OAEP_SHA_256", 446},
	{"SM2", "SM2PKE", 1024},
}

// kmsPlaintextOf renders n bytes of data as the base64 the Plaintext member carries.
//
// The bytes are arbitrary because nothing about them is under test — only how many there are — but
// they are non-zero so that a length computed from a trimmed or truncated value cannot accidentally
// agree with the length sent.
func kmsPlaintextOf(n int) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat("p", n)))
}

// kmsEncryptRequest posts Encrypt with the given key, algorithm and already-encoded Plaintext.
//
// The algorithm is omitted from the body when empty, rather than sent as "", because an absent member
// and an empty one are different requests: absent defaults to SYMMETRIC_DEFAULT per
// [kmsResolveEncryptionAlgorithm], and assertion 5's ordering case depends on that default reaching
// the key.
func kmsEncryptRequest(keyID, algorithm, plaintext string) map[string]any {
	body := map[string]any{"KeyId": keyID, "Plaintext": plaintext}
	if algorithm != "" {
		body["EncryptionAlgorithm"] = algorithm
	}
	return body
}

// TestKMSEncryptPlaintext_EveryPublishedMaximumIsAccepted is assertion 1.
//
// Exactly the maximum, at every one of the eight published pairs, because a boundary implemented with
// the wrong comparison fails here and nowhere else: `<` where `<=` belongs refuses a request AWS
// accepts, and no test of an oversize value can see it. The three published response members are
// checked at the same time, since a guard inserted before the ciphertext is minted is exactly where a
// response could lose them.
func TestKMSEncryptPlaintext_EveryPublishedMaximumIsAccepted(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, pair := range kmsPlaintextMaxima {
		t.Run(pair.keySpec+"/"+pair.algorithm, func(t *testing.T) {
			_, keyID := createKMSKeySpecUsage(t, ts, pair.keySpec, "ENCRYPT_DECRYPT")

			out := kmsRawBody(t, ts, "Encrypt",
				kmsEncryptRequest(keyID, pair.algorithm, kmsPlaintextOf(pair.maximum)))
			assert.Contains(t, out, "CiphertextBlob", "%d bytes is the published maximum, not one over it", pair.maximum)
			assert.Contains(t, out, "KeyId", "the published response names the key")
			assert.Contains(t, out, "EncryptionAlgorithm", "and the algorithm it used")
		})
	}
}

// TestKMSEncryptPlaintext_ThePublishedRangeIsEnforced is assertion 2.
//
// Both ends, and the minimum is the one worth having: an empty Plaintext satisfies Required: Yes,
// since the member is present, so nothing but the length range refuses it. The message names the
// range rather than only the offending size, because a caller told "4097" still has to discover 4096
// from somewhere.
func TestKMSEncryptPlaintext_ThePublishedRangeIsEnforced(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	for _, tc := range []struct {
		name      string
		plaintext string
	}{
		{"empty, which Required: Yes does not catch", ""},
		{"one byte over the published maximum", kmsPlaintextOf(4097)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "Encrypt", kmsEncryptRequest(keyID, "", tc.plaintext))
			assert.Equal(t, http.StatusBadRequest, status, "ValidationError is published at 400")
			assert.Equal(t, "ValidationError", code,
				"API_Encrypt publishes no code for a member out of range, so CommonErrors is where it lands")
			assert.Contains(t, message, "Plaintext", "the message names the member at fault")
			assert.Contains(t, message, "4096", "and the range, which is the only actionable part of it")
		})
	}
}

// TestKMSEncryptPlaintext_AnUndecodableValueIsNotACiphertextError is assertion 3.
//
// The old code is asserted absent by name. InvalidCiphertextException is not among API_Encrypt's nine
// errors and is published on Decrypt and ReEncrypt, whose CiphertextBlob decode is what it is for —
// and those two sites are deliberately untouched by #991, so a sweep that "unified" the three would
// break them.
func TestKMSEncryptPlaintext_AnUndecodableValueIsNotACiphertextError(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	status, code, message := kmsRefusal(t, ts, "Encrypt", kmsEncryptRequest(keyID, "", "not base64 at all!!"))
	assert.Equal(t, http.StatusBadRequest, status, "ValidationError is published at 400")
	assert.Equal(t, "ValidationError", code, "not InvalidCiphertextException, which this page does not publish")
	assert.NotEqual(t, "InvalidCiphertextException", code,
		"Encrypt produces a ciphertext; it does not consume one")
	assert.Contains(t, message, "base64", "the message says what is wrong with the value")
}

// TestKMSEncryptPlaintext_TheMemberIsCheckedBeforeTheKey is assertion 4, the ordering half of #991.
//
// Three keys the request could name, all answering the Plaintext refusal: one that does not exist, a
// disabled one, and a signing key. The first is the case AWS's own protocol layer decides — a
// malformed member is refused before any key is looked up — and the other two show the same for the
// two conditions substrate checks *at* the key, [kmsKeyStateError] and [kmsKeyUsageError]. A caller
// that can only fix one thing per round trip should be told the thing it can fix without an AWS
// account.
func TestKMSEncryptPlaintext_TheMemberIsCheckedBeforeTheKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, disabled := createKMSKey(t, ts)
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "DisableKey", map[string]any{"KeyId": disabled}), nil)
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")

	_, signing := createKMSKeySpecUsage(t, ts, "RSA_2048", "SIGN_VERIFY")

	for _, tc := range []struct {
		name  string
		keyID string
	}{
		{"a key that does not exist", "00000000-0000-0000-0000-000000000000"},
		{"a disabled key, which owes DisabledException", disabled},
		{"a signing key, which owes InvalidKeyUsageException", signing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := kmsRefusal(t, ts, "Encrypt", kmsEncryptRequest(tc.keyID, "", "not base64 at all!!"))
			assert.Equal(t, http.StatusBadRequest, status, "every one of these is a 400")
			assert.Equal(t, "ValidationError", code,
				"the member is malformed whatever the key is, so the key's own refusal waits")
		})
	}
}

// TestKMSEncryptPlaintext_ThePerSpecMaximumIsEnforced is assertion 5.
//
// One byte over each published asymmetric maximum. The symmetric pair is excluded because its
// maximum *is* the member's, so a symmetric request one byte over is already refused by assertion 2's
// range check — there is no request that reaches the per-spec branch under SYMMETRIC_DEFAULT.
//
// All four values the caller needs are asserted in the message, because no two of them determine the
// answer: 300 bytes is fine under RSA_3072 and 110 bytes too long under RSA_2048 with
// RSAES_OAEP_SHA_256, and the same key spec gives two different maxima across the two RSAES_OAEP
// algorithms.
func TestKMSEncryptPlaintext_ThePerSpecMaximumIsEnforced(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, pair := range kmsPlaintextMaxima {
		if pair.maximum >= 4096 {
			continue
		}
		t.Run(pair.keySpec+"/"+pair.algorithm, func(t *testing.T) {
			_, keyID := createKMSKeySpecUsage(t, ts, pair.keySpec, "ENCRYPT_DECRYPT")

			status, code, message := kmsRefusal(t, ts, "Encrypt",
				kmsEncryptRequest(keyID, pair.algorithm, kmsPlaintextOf(pair.maximum+1)))
			assert.Equal(t, http.StatusBadRequest, status, "ValidationError is published at 400")
			assert.Equal(t, "ValidationError", code,
				"not InvalidKeyUsageException: the key admits this algorithm, and a shorter plaintext would succeed")
			assert.Contains(t, message, pair.keySpec, "the message names the key spec")
			assert.Contains(t, message, pair.algorithm, "and the algorithm, since the maximum depends on it")
			assert.Contains(t, message, "Plaintext", "and the member at fault")
		})
	}
}

// TestKMSEncryptPlaintext_TheAlgorithmForTheKeyIsDecidedFirst is the ordering claim inside assertion 5.
//
// An RSA key with no EncryptionAlgorithm member defaults to SYMMETRIC_DEFAULT, which RSA does not
// admit, and the plaintext is far past every RSA maximum. Both conditions hold; the algorithm's is
// the one answered, because the per-spec maximum is only meaningful once the pair is admissible —
// there is no maximum to quote for a pair that cannot encrypt anything at all.
func TestKMSEncryptPlaintext_TheAlgorithmForTheKeyIsDecidedFirst(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "ENCRYPT_DECRYPT")

	status, code, _ := kmsRefusal(t, ts, "Encrypt", kmsEncryptRequest(keyID, "", kmsPlaintextOf(1000)))
	assert.Equal(t, http.StatusBadRequest, status, "InvalidKeyUsageException is published at 400")
	assert.Equal(t, "InvalidKeyUsageException", code,
		"the defaulted SYMMETRIC_DEFAULT is incompatible with RSA_2048, which is a published refusal")
}
