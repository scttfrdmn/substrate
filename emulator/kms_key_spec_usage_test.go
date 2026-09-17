package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #977: CreateKey validates its KeySpec and KeyUsage, and the five cryptographic operations refuse a key
// whose usage is not ENCRYPT_DECRYPT.
//
// Before this, CreateKey stored whatever strings it was handed. `KeySpec: "MADEUP"` answered 200 and
// DescribeKey read it back; `{"KeySpec": "HMAC_256"}` with no KeyUsage answered 200 with an *encryption*
// key, because the ENCRYPT_DECRYPT default fired unconditionally; and an RSA signing key encrypted data
// happily, because nothing compared a key's usage against the operation. Six assertions:
//
//  1. **Every one of the 68 spec/usage pairs is created or refused according to AWS's seven pairing
//     bullets**, transcribed here as [kmsSpecAdmissibleUsages]. This is the assertion that makes the rest
//     more than spot checks, and it is the reason the table is written out in the test rather than derived:
//     the implementation derives the pairing from four algorithm tables, so a test that derived it the same
//     way would agree with a wrong derivation. Here the two disagree unless both match the page.
//  2. **A value outside a published enum is refused with a different code from an inadmissible pair** —
//     ValidationError against UnsupportedOperationException. Both are 400, so the code is the only thing
//     that tells a caller a typo from a misunderstanding, which is why the distinction is asserted rather
//     than assumed.
//  3. **KeyUsage is required for every spec but SYMMETRIC_DEFAULT**, including HMAC, where AWS says so in
//     as many words. The symmetric case is asserted from both directions — omitted and explicit — because
//     the default is what makes an empty CreateKey body still work.
//  4. **A refusal writes nothing.** The validation runs before the key is minted, so ListKeys does not
//     grow. A check that ran after the write would satisfy assertions 1 to 3 and leave a key behind.
//  5. **All five cryptographic operations refuse a non-encryption key** with InvalidKeyUsageException,
//     ReEncrypt at both of its ends — the first bullet of that code's gloss, which #969 left open when it
//     implemented the second.
//  6. **The usage refusal precedes the key-state and algorithm ones.** A *disabled* signing key answers
//     InvalidKeyUsageException, not DisabledException: the usage is permanent and the state is not, so
//     answering the state first would promise a caller that enabling the key helps. And an ECC key
//     addressed with an RSA algorithm hears about its usage rather than about its key spec, which is what
//     keeps one condition to one message.
//
// Every call goes over the wire and every assertion pairs the code with the status, per #765 and #923.
// Several assert the message too, because assertions 2 and 6 are about which of two refusals a caller
// reads and [decodeAWSResponse] drops the prose.

// kmsSpecAdmissibleUsages is the key usages each key spec admits, transcribed from API_CreateKey's seven
// bullets under KeyUsage.
//
// Verbatim, in the page's order: "for symmetric encryption KMS keys, omit the parameter or specify
// ENCRYPT_DECRYPT. For HMAC KMS keys (symmetric), specify GENERATE_VERIFY_MAC. For asymmetric KMS keys with
// RSA key pairs, specify ENCRYPT_DECRYPT or SIGN_VERIFY. For asymmetric KMS keys with NIST-standard elliptic
// curve key pairs, specify SIGN_VERIFY or KEY_AGREEMENT. For asymmetric KMS keys with ECC_SECG_P256K1 key
// pairs, specify SIGN_VERIFY. For asymmetric KMS keys with ML-DSA key pairs, specify SIGN_VERIFY. For
// asymmetric KMS keys with SM2 key pairs (China Regions only), specify ENCRYPT_DECRYPT, SIGN_VERIFY, or
// KEY_AGREEMENT."
//
// ECC_NIST_EDWARDS25519 is not covered by those bullets — it is not among the "NIST-standard elliptic curve"
// specs for the key-agreement bullet's purposes — and its own annotation in the KeySpec list settles it:
// "signing and verification only". #974 found the same thing from the other side, that it publishes no key
// agreement algorithm.
//
// This is a transcription and not a derivation, deliberately: see this file's preamble, assertion 1.
var kmsSpecAdmissibleUsages = map[string][]string{
	"RSA_2048": {"ENCRYPT_DECRYPT", "SIGN_VERIFY"},
	"RSA_3072": {"ENCRYPT_DECRYPT", "SIGN_VERIFY"},
	"RSA_4096": {"ENCRYPT_DECRYPT", "SIGN_VERIFY"},

	"ECC_NIST_P256":         {"SIGN_VERIFY", "KEY_AGREEMENT"},
	"ECC_NIST_P384":         {"SIGN_VERIFY", "KEY_AGREEMENT"},
	"ECC_NIST_P521":         {"SIGN_VERIFY", "KEY_AGREEMENT"},
	"ECC_SECG_P256K1":       {"SIGN_VERIFY"},
	"ECC_NIST_EDWARDS25519": {"SIGN_VERIFY"},

	"SYMMETRIC_DEFAULT": {"ENCRYPT_DECRYPT"},

	"HMAC_224": {"GENERATE_VERIFY_MAC"},
	"HMAC_256": {"GENERATE_VERIFY_MAC"},
	"HMAC_384": {"GENERATE_VERIFY_MAC"},
	"HMAC_512": {"GENERATE_VERIFY_MAC"},

	"SM2": {"ENCRYPT_DECRYPT", "SIGN_VERIFY", "KEY_AGREEMENT"},

	"ML_DSA_44": {"SIGN_VERIFY"},
	"ML_DSA_65": {"SIGN_VERIFY"},
	"ML_DSA_87": {"SIGN_VERIFY"},
}

// kmsPublishedKeyUsages is the four key usages API_CreateKey publishes, in its order.
var kmsPublishedKeyUsages = []string{
	"SIGN_VERIFY", "ENCRYPT_DECRYPT", "GENERATE_VERIFY_MAC", "KEY_AGREEMENT",
}

// kmsDefaultKeyUsageFor is the usage a test asks for when it cares about the key spec and not the usage.
//
// ENCRYPT_DECRYPT where the spec admits it, otherwise the first usage it does admit. The rule exists
// because #977 made a key spec unusable without a usage, so the eight [createKMSKeySpec] callers — which
// are about encryption algorithms and about rotation — each needed one chosen for them, and choosing
// ENCRYPT_DECRYPT wherever possible is what preserves what those tests were already asserting: an RSA key
// that admits encryption algorithms still gets them.
//
// It fails the test rather than returning "" for an unknown spec, because a caller that reaches that has a
// typo in a spec name and would otherwise see a refusal it would read as the behavior under test.
func kmsDefaultKeyUsageFor(t *testing.T, keySpec string) string {
	t.Helper()
	usages, ok := kmsSpecAdmissibleUsages[keySpec]
	require.True(t, ok, "%s is a key spec kmsSpecAdmissibleUsages carries", keySpec)
	require.NotEmpty(t, usages, "%s admits at least one key usage", keySpec)
	for _, usage := range usages {
		if usage == "ENCRYPT_DECRYPT" {
			return usage
		}
	}
	return usages[0]
}

// createKMSKeySpec creates a key with an explicit KeySpec and an admissible KeyUsage.
//
// [createKMSKey] sends an empty body and gets SYMMETRIC_DEFAULT, which is the right default for almost
// every KMS test and useless for the two files that call this one: a symmetric key admits exactly one
// encryption algorithm and is the only spec that rotates, so neither the per-key-spec algorithm tables
// (#969) nor the rotation refusal (#972) is observable through one.
//
// It sent the KeySpec alone until #977, which is what made a usage necessary: CreateKey now requires one
// for every spec but SYMMETRIC_DEFAULT. [kmsDefaultKeyUsageFor] records which one it picks and why.
func createKMSKeySpec(t *testing.T, ts *emulator.TestServer, keySpec string) (arn, keyID string) {
	t.Helper()
	return createKMSKeySpecUsage(t, ts, keySpec, kmsDefaultKeyUsageFor(t, keySpec))
}

// createKMSKeySpecUsage creates a key with an explicit KeySpec and KeyUsage, requiring that AWS would
// accept the pair.
//
// The two-member form is separate from [createKMSKeySpec] because the usage is what a #977 test is about
// and what its callers elsewhere do not care to name — and because a test that wants an *inadmissible*
// pair wants the refusal, not this.
func createKMSKeySpecUsage(t *testing.T, ts *emulator.TestServer, keySpec, keyUsage string) (arn, keyID string) {
	t.Helper()
	var out struct {
		KeyMetadata struct {
			KeyID    string `json:"KeyId"`
			Arn      string `json:"Arn"`
			KeySpec  string `json:"KeySpec"`
			KeyUsage string `json:"KeyUsage"`
		} `json:"KeyMetadata"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "CreateKey",
			map[string]any{"KeySpec": keySpec, "KeyUsage": keyUsage}), &out)
	require.Empty(t, code, "CreateKey with KeySpec %s and KeyUsage %s", keySpec, keyUsage)
	require.Equal(t, http.StatusOK, status, "CreateKey with KeySpec %s and KeyUsage %s", keySpec, keyUsage)
	require.Equal(t, keySpec, out.KeyMetadata.KeySpec, "CreateKey records the KeySpec it was given")
	require.Equal(t, keyUsage, out.KeyMetadata.KeyUsage, "CreateKey records the KeyUsage it was given")
	return out.KeyMetadata.Arn, out.KeyMetadata.KeyID
}

// TestKMSCreateKey_EveryPublishedPairIsAcceptedOrRefusedAsAWSPairsThem is assertion 1: all 17 key specs
// against all 4 key usages, 68 cases, each one decided by AWS's bullets rather than by a sample of them.
//
// The 25 admissible pairs must be created and read back with the values sent, and the 43 inadmissible ones
// must answer UnsupportedOperationException. Neither half alone would do: accepting everything passes the
// first, refusing everything passes the second, and only the matrix rules out both.
func TestKMSCreateKey_EveryPublishedPairIsAcceptedOrRefusedAsAWSPairsThem(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for keySpec, admissible := range kmsSpecAdmissibleUsages {
		for _, keyUsage := range kmsPublishedKeyUsages {
			admits := false
			for _, usage := range admissible {
				if usage == keyUsage {
					admits = true
					break
				}
			}
			t.Run(keySpec+"/"+keyUsage, func(t *testing.T) {
				if admits {
					createKMSKeySpecUsage(t, ts, keySpec, keyUsage)
					return
				}
				status, code, message := kmsRefusal(t, ts, "CreateKey",
					map[string]any{"KeySpec": keySpec, "KeyUsage": keyUsage})
				assert.Equal(t, "UnsupportedOperationException", code,
					"%s cannot be created with %s", keySpec, keyUsage)
				assert.Equal(t, http.StatusBadRequest, status,
					"%s cannot be created with %s", keySpec, keyUsage)
				assert.Contains(t, message, keySpec, "the refusal names the key spec")
				assert.Contains(t, message, keyUsage, "the refusal names the key usage the caller sent")
			})
		}
	}
}

// TestKMSCreateKey_AnUnpublishedValueIsRefusedWithADifferentCode is assertion 2.
//
// The two codes are the whole point. Both refusals are 400 — KMS publishes no other status for a rejected
// request — so a caller that cannot distinguish them cannot tell "fix the spelling" from "that key type
// does not do that". The pair case is included here as the contrast rather than left to the matrix above,
// because the assertion is about the difference.
func TestKMSCreateKey_AnUnpublishedValueIsRefusedWithADifferentCode(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name    string
		body    map[string]any
		code    string
		message string
	}{
		{
			"a key spec outside the published seventeen",
			map[string]any{"KeySpec": "RSA_2049", "KeyUsage": "ENCRYPT_DECRYPT"},
			"ValidationError", "RSA_2049",
		},
		{
			"a key spec that is a plausible misreading of a real one",
			map[string]any{"KeySpec": "AES_256", "KeyUsage": "ENCRYPT_DECRYPT"},
			"ValidationError", "AES_256",
		},
		{
			"a key usage outside the published four",
			map[string]any{"KeySpec": "SYMMETRIC_DEFAULT", "KeyUsage": "TRANSMOGRIFY"},
			"ValidationError", "TRANSMOGRIFY",
		},
		{
			"a published pair AWS does not admit",
			map[string]any{"KeySpec": "HMAC_256", "KeyUsage": "KEY_AGREEMENT"},
			"UnsupportedOperationException", "KEY_AGREEMENT",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "CreateKey", tc.body)
			assert.Equal(t, tc.code, code, "the code distinguishes this refusal")
			assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
			assert.Contains(t, message, tc.message, "the refusal names the value the caller sent")
		})
	}
}

// TestKMSCreateKey_AnUnknownKeySpecIsReportedBeforeTheKeyUsage pins the order of the two enum checks.
//
// A request wrong in both members hears about the key spec, because the spec is what decides whether the
// usage may be omitted and which usages are admissible — so telling the caller about the usage first would
// name a set that the corrected spec may not have.
func TestKMSCreateKey_AnUnknownKeySpecIsReportedBeforeTheKeyUsage(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	status, code, message := kmsRefusal(t, ts, "CreateKey",
		map[string]any{"KeySpec": "MADEUP", "KeyUsage": "TRANSMOGRIFY"})
	assert.Equal(t, "ValidationError", code, "both members are outside their enums")
	assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
	assert.Contains(t, message, "KeySpec", "the key spec is the member reported")
	assert.NotContains(t, message, "TRANSMOGRIFY", "the key usage is not reported yet")
}

// TestKMSCreateKey_KeyUsageIsRequiredExceptForTheSymmetricDefault is assertion 3.
//
// AWS's rule is "this parameter is optional when you are creating a symmetric encryption KMS key;
// otherwise, it is required", and the HMAC case is the one that shows what "symmetric encryption" excludes —
// an HMAC key is symmetric, and AWS still requires the usage: "you must set the key usage even though
// GENERATE_VERIFY_MAC is the only valid key usage value for HMAC KMS keys". Substrate defaulted
// ENCRYPT_DECRYPT for all sixteen non-symmetric specs before #977, so each answered 200 with an encryption
// key.
//
// The refusal names the admissible set, which for fourteen of the sixteen is a single value — so the
// message is the whole answer rather than a report that something is missing.
func TestKMSCreateKey_KeyUsageIsRequiredExceptForTheSymmetricDefault(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for keySpec, admissible := range kmsSpecAdmissibleUsages {
		if keySpec == "SYMMETRIC_DEFAULT" {
			continue
		}
		t.Run(keySpec, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "CreateKey", map[string]any{"KeySpec": keySpec})
			assert.Equal(t, "ValidationError", code, "%s requires a KeyUsage", keySpec)
			assert.Equal(t, http.StatusBadRequest, status, "%s requires a KeyUsage", keySpec)
			assert.Contains(t, message, keySpec, "the refusal names the key spec that requires it")
			for _, usage := range admissible {
				assert.Contains(t, message, usage, "the refusal names %s as admissible", usage)
			}
		})
	}
}

// TestKMSCreateKey_TheSymmetricDefaultNeedsNoKeyUsage is the other half of assertion 3.
//
// Three requests that must all produce one key: an empty body, a KeySpec with no KeyUsage, and both
// members explicit. The empty body is the case the default exists for — CreateKey has no required
// parameter, and the operation's own first guidance section is about creating a symmetric encryption key —
// so a rule that required KeyUsage unconditionally would break every caller that never named a key type.
func TestKMSCreateKey_TheSymmetricDefaultNeedsNoKeyUsage(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"an empty body", map[string]any{}},
		{"a key spec with no key usage", map[string]any{"KeySpec": "SYMMETRIC_DEFAULT"}},
		{"both members explicit", map[string]any{"KeySpec": "SYMMETRIC_DEFAULT", "KeyUsage": "ENCRYPT_DECRYPT"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out struct {
				KeyMetadata struct {
					KeySpec  string `json:"KeySpec"`
					KeyUsage string `json:"KeyUsage"`
				} `json:"KeyMetadata"`
			}
			status, code := decodeAWSResponse(t,
				signedRequest(t, ts, kmsTarget, taggingTestAccount, "CreateKey", tc.body), &out)
			require.Empty(t, code, "CreateKey with %s", tc.name)
			require.Equal(t, http.StatusOK, status, "CreateKey with %s", tc.name)
			assert.Equal(t, "SYMMETRIC_DEFAULT", out.KeyMetadata.KeySpec, "the key spec defaults")
			assert.Equal(t, "ENCRYPT_DECRYPT", out.KeyMetadata.KeyUsage, "the key usage defaults")
		})
	}
}

// TestKMSCreateKey_ARefusedRequestCreatesNoKey is assertion 4.
//
// The validation runs before the key is minted and before either state write, so a refused CreateKey leaves
// the account exactly as it was. An implementation that validated after saving would pass every other
// assertion in this file and leave an unusable key in ListKeys — the failure #949 found for the rotation
// pair, which wrote state and then refused.
func TestKMSCreateKey_ARefusedRequestCreatesNoKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	before := kmsListKeyCount(t, ts)

	for _, body := range []map[string]any{
		{"KeySpec": "MADEUP", "KeyUsage": "ENCRYPT_DECRYPT"},
		{"KeySpec": "SYMMETRIC_DEFAULT", "KeyUsage": "TRANSMOGRIFY"},
		{"KeySpec": "HMAC_256", "KeyUsage": "KEY_AGREEMENT"},
		{"KeySpec": "ML_DSA_44"},
	} {
		status, code, _ := kmsRefusal(t, ts, "CreateKey", body)
		require.NotEmpty(t, code, "CreateKey %v is refused", body)
		require.Equal(t, http.StatusBadRequest, status, "CreateKey %v is refused", body)
	}

	assert.Equal(t, before, kmsListKeyCount(t, ts), "four refusals create no key")
}

// kmsListKeyCount reports how many keys the account holds, over the wire.
//
// ListKeys rather than a state read, per #765: a refusal that wrote a key would be invisible to a helper
// that inspected the same state the write went to, and the point is that no caller can observe one.
func kmsListKeyCount(t *testing.T, ts *emulator.TestServer) int {
	t.Helper()
	var out struct {
		Keys []struct {
			KeyID string `json:"KeyId"`
		} `json:"Keys"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "ListKeys", map[string]any{}), &out)
	require.Empty(t, code, "ListKeys")
	require.Equal(t, http.StatusOK, status, "ListKeys")
	return len(out.Keys)
}

// TestKMSCreateKey_AnUnparseableBodyIsRefused covers the parse guard the validation made necessary.
//
// CreateKey discarded the JSON error until #977 — the body is optional, so an unparseable one was treated
// as absent. With the members validated that is no longer tenable: `{"KeySpec": 4096}` would zero both
// members and answer 200 with a symmetric key, which is the silent substitution this issue is about,
// reached by a different route. The code is the one the plugin's other twenty-one parse guards answer.
func TestKMSCreateKey_AnUnparseableBodyIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	status, code := kmsRawCall(t, ts, "CreateKey", []byte("{not json"))
	assert.Equal(t, "ValidationError", code, "an unparseable body is refused")
	assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")

	status, code = kmsRawCall(t, ts, "CreateKey", []byte(`{"KeySpec": 4096}`))
	assert.Equal(t, "ValidationError", code, "a KeySpec of the wrong JSON type is refused")
	assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
}

// TestKMSCryptographicOperations_ANonEncryptionKeyIsRefused is assertion 5.
//
// The five operations are [kmsCryptoOperations], the same table #961 used for the key-state refusals,
// because AWS gives all five the same requirement — "for encrypting, decrypting, re-encrypting, and
// generating data keys, the KeyUsage must be ENCRYPT_DECRYPT" — and publishes InvalidKeyUsageException on
// each. Reusing the table is what makes it impossible to add a sixth operation to one list and forget the
// other.
//
// The ciphertext is a hand-built stub. Decrypt and ReEncrypt take their key from the ciphertext rather
// than from a member, and nothing that mints a real ciphertext will do so under a signing key any more —
// so the only way to reach those two guards is to construct the blob, which is what [kmsStubCiphertext]
// exists for.
func TestKMSCryptographicOperations_ANonEncryptionKeyIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, keySpec := range []string{"RSA_2048", "ECC_NIST_P256", "HMAC_256", "ML_DSA_44"} {
		t.Run(keySpec, func(t *testing.T) {
			keyUsage := kmsSpecAdmissibleUsages[keySpec][0]
			if keySpec == "RSA_2048" {
				keyUsage = "SIGN_VERIFY" // The one spec here that also admits encryption; take the other.
			}
			_, keyID := createKMSKeySpecUsage(t, ts, keySpec, keyUsage)
			ciphertext := kmsStubCiphertext(keyID, []byte("secret"))

			for _, op := range kmsCryptoOperations {
				t.Run(op.name, func(t *testing.T) {
					status, code, message := kmsRefusal(t, ts, op.name, op.body(keyID, ciphertext))
					assert.Equal(t, "InvalidKeyUsageException", code,
						"%s against a %s key", op.name, keyUsage)
					assert.Equal(t, http.StatusBadRequest, status,
						"%s against a %s key", op.name, keyUsage)
					assert.Contains(t, message, keyUsage, "the refusal names the key's own usage")
					assert.Contains(t, message, "ENCRYPT_DECRYPT", "and the usage the operation requires")
				})
			}
		})
	}
}

// TestKMSReEncrypt_BothEndsRefuseANonEncryptionKey is the other half of assertion 5.
//
// ReEncrypt reads one key and writes under another, and an implementation that checked one of them would
// pass the table above — which drives the *source* end, because the stub ciphertext names the signing key.
// The destination end is the one a caller can actually reach: a real ciphertext under a working key,
// re-encrypted into a signing key.
func TestKMSReEncrypt_BothEndsRefuseANonEncryptionKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, symmetricKeyID, ciphertext := kmsCryptoFixture(t, ts)
	_, signingKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "SIGN_VERIFY")

	t.Run("the destination key", func(t *testing.T) {
		status, code, message := kmsRefusal(t, ts, "ReEncrypt", map[string]any{
			"CiphertextBlob":   ciphertext,
			"DestinationKeyId": signingKeyID,
		})
		assert.Equal(t, "InvalidKeyUsageException", code, "a signing key cannot be re-encrypted into")
		assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
		assert.Contains(t, message, signingKeyID, "the refusal names the destination key")
	})

	t.Run("the source key", func(t *testing.T) {
		status, code, message := kmsRefusal(t, ts, "ReEncrypt", map[string]any{
			"CiphertextBlob":   kmsStubCiphertext(signingKeyID, []byte("secret")),
			"DestinationKeyId": symmetricKeyID,
		})
		assert.Equal(t, "InvalidKeyUsageException", code, "a signing key cannot be re-encrypted from")
		assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
		assert.Contains(t, message, signingKeyID, "the refusal names the source key")
	})
}

// TestKMSCryptographicOperations_TheKeyUsageIsRefusedBeforeTheKeyState is the first half of assertion 6.
//
// A disabled signing key earns two refusals and gets the one that is true of it permanently. This is the
// ordering [kmsRotationKeySpecError] established for the rotation pair, and the reason is a caller's
// remedy: DisabledException invites an EnableKey and a retry, which for a signing key succeeds at the
// EnableKey and fails at the retry, however many times it is tried.
func TestKMSCryptographicOperations_TheKeyUsageIsRefusedBeforeTheKeyState(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "SIGN_VERIFY")
	status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")

	status, code, message := kmsRefusal(t, ts, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
	})
	assert.Equal(t, "InvalidKeyUsageException", code, "the permanent property is reported")
	assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
	assert.NotContains(t, message, "not enabled", "the transient one is not")
}

// TestKMSCryptographicOperations_TheKeyUsageIsRefusedBeforeTheAlgorithm is the second half of assertion 6.
//
// Here the ordering decides which message a caller reads for one condition rather than which code, since
// both refusals answer InvalidKeyUsageException — AWS gives the code two bullets and this is the seam
// between them. Usage first is what makes the answer uniform: an ECC signing key and an RSA signing key
// are both simply not encryption keys, where algorithm-first would explain the ECC one in terms of its key
// spec and the RSA one in terms of its usage.
//
// The consequence is that [kmsIncompatibleEncryptionAlgorithm]'s "no encryption algorithm" wording is no
// longer reachable, which is asserted rather than left implicit: it is the wording this test would read if
// the two checks were the other way round.
func TestKMSCryptographicOperations_TheKeyUsageIsRefusedBeforeTheAlgorithm(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKeySpecUsage(t, ts, "ECC_NIST_P256", "SIGN_VERIFY")

	status, code, message := kmsRefusal(t, ts, "Encrypt", map[string]any{
		"KeyId":               keyID,
		"Plaintext":           base64.StdEncoding.EncodeToString([]byte("secret")),
		"EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
	})
	assert.Equal(t, "InvalidKeyUsageException", code, "one code, and this is its first bullet")
	assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
	assert.Contains(t, message, "SIGN_VERIFY", "the refusal is about the key usage")
	assert.NotContains(t, message, "no encryption algorithm", "not about the key spec's empty algorithm list")
}

// TestKMSCreateKey_TheReportedMetadataFollowsTheResolvedPair ties the validation to what #974 renders.
//
// The pair CreateKey resolves is the pair the key carries for the rest of its life, so the algorithm list
// in its own CreateKey response is the one DescribeKey will report — which is what makes the four algorithm
// tables total over the keys substrate can hold, the property #974's builder could not rely on before this.
// An RSA spec is the case worth pinning because it is the only family that admits two usages, so it is the
// only one where the reported member is a function of the usage rather than of the spec alone.
func TestKMSCreateKey_TheReportedMetadataFollowsTheResolvedPair(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		keyUsage string
		member   string
		absent   string
	}{
		{"ENCRYPT_DECRYPT", "EncryptionAlgorithms", "SigningAlgorithms"},
		{"SIGN_VERIFY", "SigningAlgorithms", "EncryptionAlgorithms"},
	} {
		t.Run(tc.keyUsage, func(t *testing.T) {
			body := kmsRawBody(t, ts, "CreateKey",
				map[string]any{"KeySpec": "RSA_2048", "KeyUsage": tc.keyUsage})
			var metadata map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body["KeyMetadata"], &metadata), "decode KeyMetadata")

			assert.Contains(t, metadata, tc.member, "the usage's own algorithm member is reported")
			assert.NotContains(t, metadata, tc.absent, "and no other")
		})
	}
}
