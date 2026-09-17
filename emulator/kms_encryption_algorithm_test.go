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

// #969: the encryption algorithm members on Encrypt, Decrypt and both ends of ReEncrypt.
//
// Four members were decoded by nobody and reported by nobody, so a caller could not say which algorithm
// it wanted and could not read which one was used. Adding them is only defensible because AWS does not
// let a key choose — "you cannot configure a KMS key to use a particular encryption algorithm" — and
// instead fixes the admissible set per key spec, so the reported value is a function of the request and
// the key rather than of nothing. See kms_encryption_algorithms.go's preamble.
//
// Five things are asserted:
//
//  1. **The value is echoed**, defaulting to SYMMETRIC_DEFAULT when absent, on all three operations —
//     and ReEncrypt reports *both* of its ends.
//  2. **A value outside the published four is refused** with ValidationError, which is substrate's
//     reading: no operation page gives a code for a malformed enum member, so it comes from
//     CommonErrors.html.
//  3. **That refusal precedes the key lookup**, because a misspelled enum is a malformed request rather
//     than a statement about a key. A caller that misspells the algorithm *and* names a missing key hears
//     about the misspelling.
//  4. **A published value the key spec does not admit is refused** with InvalidKeyUsageException, which
//     is published — it is the second bullet of that code's own gloss. This is the assertion that
//     distinguishes a real check from a spelling test, and it covers the specs that admit **no**
//     encryption algorithm at all.
//  5. **ReEncrypt's two ends are independent**, each checked against its own key. An implementation that
//     checked one algorithm against one key would pass every assertion above.
//
// The first bullet of InvalidKeyUsageException's gloss — a KeyUsage incompatible with the operation — is
// deliberately **not** covered, because it is not implemented: that is #977, which also fixes CreateKey
// accepting any string as a KeySpec. Every non-symmetric key below is created through that gap, which is
// why #977 must re-verify these tests when it closes it.
//
// Every call goes over the wire and every assertion pairs the code with the status, per #765 and #923.

// createKMSKeySpec creates a key with an explicit KeySpec.
//
// [createKMSKey] sends an empty body and gets SYMMETRIC_DEFAULT, which is the right default for almost
// every KMS test and useless for this file: a symmetric key admits exactly one algorithm, so nothing
// about the per-key-spec table is observable through one.
func createKMSKeySpec(t *testing.T, ts *emulator.TestServer, keySpec string) (arn, keyID string) {
	t.Helper()
	var out struct {
		KeyMetadata struct {
			KeyID   string `json:"KeyId"`
			Arn     string `json:"Arn"`
			KeySpec string `json:"KeySpec"`
		} `json:"KeyMetadata"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "CreateKey",
			map[string]any{"KeySpec": keySpec}), &out)
	require.Empty(t, code, "CreateKey with KeySpec %s", keySpec)
	require.Equal(t, http.StatusOK, status, "CreateKey with KeySpec %s", keySpec)
	require.Equal(t, keySpec, out.KeyMetadata.KeySpec, "CreateKey records the KeySpec it was given")
	return out.KeyMetadata.Arn, out.KeyMetadata.KeyID
}

// kmsAlgorithmMember reads one algorithm member out of a successful response.
func kmsAlgorithmMember(t *testing.T, body map[string]json.RawMessage, member string) string {
	t.Helper()
	raw, ok := body[member]
	require.True(t, ok, "the response carries %s: %v", member, body)
	var value string
	require.NoError(t, json.Unmarshal(raw, &value), "decode %s", member)
	return value
}

// TestKMSCryptographicOperations_ReportTheEncryptionAlgorithm is assertion 1.
//
// The absent and the explicit case are both here because they are different code paths that must agree:
// AWS states the default unconditionally — "the default value, SYMMETRIC_DEFAULT, is the algorithm used
// for symmetric encryption KMS keys" — so a caller that sends nothing and one that sends
// SYMMETRIC_DEFAULT must read the same value back.
func TestKMSCryptographicOperations_ReportTheEncryptionAlgorithm(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		explicit bool
	}{
		{"absent", false},
		{"explicit", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)
			_, destKeyID := createKMSKey(t, ts)

			explicit := tc.explicit

			encryptBody := map[string]any{
				"KeyId":     keyID,
				"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
			}
			if explicit {
				encryptBody["EncryptionAlgorithm"] = "SYMMETRIC_DEFAULT"
			}
			encrypted := kmsRawBody(t, ts, "Encrypt", encryptBody)
			assert.Equal(t, "SYMMETRIC_DEFAULT", kmsAlgorithmMember(t, encrypted, "EncryptionAlgorithm"),
				"Encrypt reports the algorithm it used")

			var ciphertext string
			require.NoError(t, json.Unmarshal(encrypted["CiphertextBlob"], &ciphertext), "decode CiphertextBlob")

			decryptBody := map[string]any{"CiphertextBlob": ciphertext}
			if explicit {
				decryptBody["EncryptionAlgorithm"] = "SYMMETRIC_DEFAULT"
			}
			decrypted := kmsRawBody(t, ts, "Decrypt", decryptBody)
			assert.Equal(t, "SYMMETRIC_DEFAULT", kmsAlgorithmMember(t, decrypted, "EncryptionAlgorithm"),
				"Decrypt reports the algorithm it used")

			reEncryptBody := map[string]any{"CiphertextBlob": ciphertext, "DestinationKeyId": destKeyID}
			if explicit {
				reEncryptBody["SourceEncryptionAlgorithm"] = "SYMMETRIC_DEFAULT"
				reEncryptBody["DestinationEncryptionAlgorithm"] = "SYMMETRIC_DEFAULT"
			}
			reEncrypted := kmsRawBody(t, ts, "ReEncrypt", reEncryptBody)
			assert.Equal(t, "SYMMETRIC_DEFAULT",
				kmsAlgorithmMember(t, reEncrypted, "SourceEncryptionAlgorithm"),
				"ReEncrypt reports the algorithm the data was encrypted under")
			assert.Equal(t, "SYMMETRIC_DEFAULT",
				kmsAlgorithmMember(t, reEncrypted, "DestinationEncryptionAlgorithm"),
				"ReEncrypt reports the algorithm the data was re-encrypted under")
		})
	}
}

// TestKMSEncryptionAlgorithm_AnUnpublishedValueIsRefused is assertion 2.
//
// All four members are covered, because each is a separate decode and a separate call to the resolver,
// and the message must name the member so a caller with a bad value on a ReEncrypt learns which end of
// the operation it belongs to. It must also list the published set: a caller that sent
// "RSAES_OAEP_SHA256" — the real value with one underscore missing — cannot discover the right spelling
// from a bare refusal.
func TestKMSEncryptionAlgorithm_AnUnpublishedValueIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)
	ciphertext := kmsCiphertext(t, ts, keyID)
	_, destKeyID := createKMSKey(t, ts)

	const bad = "RSAES_OAEP_SHA256"

	for _, tc := range []struct {
		name   string
		op     string
		member string
		body   map[string]any
	}{
		{"Encrypt", "Encrypt", "EncryptionAlgorithm", map[string]any{
			"KeyId":               keyID,
			"Plaintext":           base64.StdEncoding.EncodeToString([]byte("secret")),
			"EncryptionAlgorithm": bad,
		}},
		{"Decrypt", "Decrypt", "EncryptionAlgorithm", map[string]any{
			"CiphertextBlob":      ciphertext,
			"EncryptionAlgorithm": bad,
		}},
		{"ReEncrypt source", "ReEncrypt", "SourceEncryptionAlgorithm", map[string]any{
			"CiphertextBlob":            ciphertext,
			"DestinationKeyId":          destKeyID,
			"SourceEncryptionAlgorithm": bad,
		}},
		{"ReEncrypt destination", "ReEncrypt", "DestinationEncryptionAlgorithm", map[string]any{
			"CiphertextBlob":                 ciphertext,
			"DestinationKeyId":               destKeyID,
			"DestinationEncryptionAlgorithm": bad,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, tc.op, tc.body)
			assert.Equal(t, "ValidationError", code, "%s with an unpublished %s", tc.op, tc.member)
			assert.Equal(t, http.StatusBadRequest, status, "%s with an unpublished %s", tc.op, tc.member)
			assert.Contains(t, message, tc.member, "the message names the member that was wrong")
			assert.Contains(t, message, "RSAES_OAEP_SHA_256", "the message lists the published set")
			assert.Contains(t, message, "SM2PKE", "the message lists the published set")
		})
	}
}

// TestKMSEncryptionAlgorithm_TheEnumIsCheckedBeforeTheKey is assertion 3.
//
// The request is wrong twice over — an unpublished algorithm and a KeyId naming no key — so there are
// two candidate answers and only one can be reported. AWS's protocol layer validates an enum before any
// key is looked up, and substrate follows it: an algorithm outside the published set says nothing about
// any key, so reporting NotFoundException here would hide the defect the caller can actually fix.
func TestKMSEncryptionAlgorithm_TheEnumIsCheckedBeforeTheKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	status, code, _ := kmsRefusal(t, ts, "Encrypt", map[string]any{
		"KeyId":               kmsAbsentKeyID,
		"Plaintext":           base64.StdEncoding.EncodeToString([]byte("secret")),
		"EncryptionAlgorithm": "NOT_AN_ALGORITHM",
	})
	assert.Equal(t, "ValidationError", code,
		"the malformed algorithm is reported, not the absence of the key")
	assert.Equal(t, http.StatusBadRequest, status, "Encrypt with a bad algorithm and an absent key")
}

// TestKMSEncryptionAlgorithm_AKeySpecThatDoesNotAdmitItIsRefused is assertion 4, and it is what makes
// the members mean something rather than being echoed back unexamined.
//
// The three rows are the three shapes the per-key-spec table has. A symmetric key admits only
// SYMMETRIC_DEFAULT, so an RSA algorithm against one is refused. An RSA key does **not** admit
// SYMMETRIC_DEFAULT, which is how AWS's separate rule that the member "is required only for asymmetric
// KMS keys" falls out for free: the default is a value RSA cannot use, so omitting the member on an RSA
// key is refused with no required-ness branch anywhere. And an ECC key admits **nothing** — the message
// has to say so, because listing an empty set would leave a caller retrying algorithms forever.
func TestKMSEncryptionAlgorithm_AKeySpecThatDoesNotAdmitItIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		keySpec   string
		algorithm string
		permitted string
	}{
		{"an RSA algorithm against a symmetric key", "SYMMETRIC_DEFAULT", "RSAES_OAEP_SHA_256", "SYMMETRIC_DEFAULT"},
		{"the default against an RSA key", "RSA_2048", "", "RSAES_OAEP_SHA_1"},
		{"any algorithm against an ECC key", "ECC_NIST_P256", "RSAES_OAEP_SHA_256", "no encryption algorithm"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKeySpec(t, ts, tc.keySpec)

			body := map[string]any{
				"KeyId":     keyID,
				"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
			}
			if tc.algorithm != "" {
				body["EncryptionAlgorithm"] = tc.algorithm
			}

			status, code, message := kmsRefusal(t, ts, "Encrypt", body)
			assert.Equal(t, "InvalidKeyUsageException", code, "Encrypt with %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "Encrypt with %s", tc.name)
			assert.Contains(t, message, tc.keySpec, "the message names the key spec that refused")
			assert.Contains(t, message, tc.permitted, "the message names what the key spec does admit")
		})
	}
}

// TestKMSEncryptionAlgorithm_AnRSAKeyAcceptsItsOwnTwoAlgorithms is the other half of assertion 4, and it
// exists so the test above cannot pass by refusing everything.
//
// Both RSAES_OAEP algorithms are admitted by all three RSA key specs, and they differ only in the hash
// used internally, which is a per-request rather than a per-key property — so both must work against one
// key and both must be echoed. A Decrypt of the resulting ciphertext with the *default* algorithm is
// refused, which is the same rule seen from the other side.
func TestKMSEncryptionAlgorithm_AnRSAKeyAcceptsItsOwnTwoAlgorithms(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKeySpec(t, ts, "RSA_2048")

	for _, algorithm := range []string{"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"} {
		t.Run(algorithm, func(t *testing.T) {
			body := kmsRawBody(t, ts, "Encrypt", map[string]any{
				"KeyId":               keyID,
				"Plaintext":           base64.StdEncoding.EncodeToString([]byte("secret")),
				"EncryptionAlgorithm": algorithm,
			})
			assert.Equal(t, algorithm, kmsAlgorithmMember(t, body, "EncryptionAlgorithm"),
				"Encrypt reports the algorithm the caller asked for")

			var ciphertext string
			require.NoError(t, json.Unmarshal(body["CiphertextBlob"], &ciphertext), "decode CiphertextBlob")

			decrypted := kmsRawBody(t, ts, "Decrypt", map[string]any{
				"CiphertextBlob":      ciphertext,
				"EncryptionAlgorithm": algorithm,
			})
			assert.Equal(t, algorithm, kmsAlgorithmMember(t, decrypted, "EncryptionAlgorithm"),
				"Decrypt reports the algorithm the caller asked for")

			// The default is not admissible for an RSA key, which is how AWS's "required only for
			// asymmetric KMS keys" rule is enforced without a separate branch for it.
			status, code, _ := kmsRefusal(t, ts, "Decrypt", map[string]any{"CiphertextBlob": ciphertext})
			assert.Equal(t, "InvalidKeyUsageException", code, "Decrypt against an RSA key with no algorithm")
			assert.Equal(t, http.StatusBadRequest, status, "Decrypt against an RSA key with no algorithm")
		})
	}
}

// TestKMSReEncrypt_TheTwoAlgorithmEndsAreIndependent is assertion 5, and it is the strongest test in the
// file: it is the only one that fails against an implementation checking one algorithm against one key.
//
// A ReEncrypt exists to move data between keys, so the two ends need not agree — here a symmetric source
// re-encrypts to an RSA destination, and each algorithm is admissible only for its own key. Swapping
// either end for the other's value must be refused, which the two rows after the success case assert;
// otherwise a resolver that validated both members against the destination key, or against the source,
// would pass the success case by luck.
func TestKMSReEncrypt_TheTwoAlgorithmEndsAreIndependent(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, sourceKeyID := createKMSKey(t, ts)
	_, destKeyID := createKMSKeySpec(t, ts, "RSA_2048")
	ciphertext := kmsCiphertext(t, ts, sourceKeyID)

	body := kmsRawBody(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":                 ciphertext,
		"DestinationKeyId":               destKeyID,
		"SourceEncryptionAlgorithm":      "SYMMETRIC_DEFAULT",
		"DestinationEncryptionAlgorithm": "RSAES_OAEP_SHA_256",
	})
	assert.Equal(t, "SYMMETRIC_DEFAULT", kmsAlgorithmMember(t, body, "SourceEncryptionAlgorithm"),
		"the source algorithm is the source key's")
	assert.Equal(t, "RSAES_OAEP_SHA_256", kmsAlgorithmMember(t, body, "DestinationEncryptionAlgorithm"),
		"the destination algorithm is the destination key's")

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"the destination's algorithm at the source", map[string]any{
			"CiphertextBlob":                 ciphertext,
			"DestinationKeyId":               destKeyID,
			"SourceEncryptionAlgorithm":      "RSAES_OAEP_SHA_256",
			"DestinationEncryptionAlgorithm": "RSAES_OAEP_SHA_256",
		}},
		{"the source's algorithm at the destination", map[string]any{
			"CiphertextBlob":                 ciphertext,
			"DestinationKeyId":               destKeyID,
			"SourceEncryptionAlgorithm":      "SYMMETRIC_DEFAULT",
			"DestinationEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := kmsRefusal(t, ts, "ReEncrypt", tc.body)
			assert.Equal(t, "InvalidKeyUsageException", code, "ReEncrypt with %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "ReEncrypt with %s", tc.name)
		})
	}
}

// TestKMSReEncrypt_TheSourceAlgorithmIsCheckedBeforeTheDestinationKey pins the order between the two
// ends, which follows the order #961 already established for their key states.
//
// The source algorithm is inadmissible and the destination key does not exist, so either could be
// reported. AWS's own description — "Decrypts ciphertext and then reencrypts it" — puts the source
// first, and reporting the destination's absence would tell a caller to go fix the half of the request
// that is not yet reachable.
func TestKMSReEncrypt_TheSourceAlgorithmIsCheckedBeforeTheDestinationKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, sourceKeyID := createKMSKey(t, ts)
	ciphertext := kmsCiphertext(t, ts, sourceKeyID)

	status, code, _ := kmsRefusal(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":            ciphertext,
		"DestinationKeyId":          kmsAbsentKeyID,
		"SourceEncryptionAlgorithm": "RSAES_OAEP_SHA_256",
	})
	assert.Equal(t, "InvalidKeyUsageException", code,
		"the source algorithm is reported, not the destination key's absence")
	assert.Equal(t, http.StatusBadRequest, status, "ReEncrypt with a bad source algorithm and an absent destination")
}
