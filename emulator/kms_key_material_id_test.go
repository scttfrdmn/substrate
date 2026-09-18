package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
	"slices"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #978: substrate modeled no key material identity, so six response members across five operations were
// absent.
//
// The members are KeyMetadata's CurrentKeyMaterialId (reported by CreateKey and DescribeKey), Decrypt's
// KeyMaterialId, GenerateDataKey's and GenerateDataKeyWithoutPlaintext's KeyMaterialId, and ReEncrypt's
// SourceKeyMaterialId and DestinationKeyMaterialId. Encrypt publishes none, which is asserted here too,
// because it is the kind of asymmetry a later sweep over "the operations that report key material" would
// tidy away.
//
// What makes the value worth reporting is that it is comparable *across* operations. AWS glosses Decrypt's
// as "the identifier of the key material used to decrypt the ciphertext" — so a caller compares it against
// DescribeKey's CurrentKeyMaterialId and learns that the material which decrypted its data is the material
// the key currently holds. That comparison is the whole point, and it is only meaningful because every site
// reads one stored value rather than computing its own: a per-site derivation would satisfy every
// assertion below except the ones that cross an operation boundary, and would satisfy those by
// construction.
//
// Six things are asserted:
//
//  1. **The published shape**, 64 lowercase hex characters, and one value per key: two keys differ, and one
//     key answers the same value on every observation.
//  2. **The condition**, across all seventeen key specs. Only a symmetric encryption key reports one —
//     which is narrower than "symmetric", since an HMAC key is symmetric and encrypts nothing.
//  3. **Every operation agrees with the key**, asserted against DescribeKey rather than against a literal.
//  4. **ReEncrypt reports one per side**, the two differ, and each is its own key's — the only response in
//     the service carrying two material identities, and the only one where reporting both from one key
//     would still look well-formed.
//  5. **Each ReEncrypt member is conditioned on its own key**, so an asymmetric source and a symmetric
//     destination report the destination member alone.
//  6. **Encrypt reports none**, the one recorded decision left here. This assertion also covered the two
//     GenerateDataKey* operations under an asymmetric key until #988; that request is now refused outright,
//     so the case moved to kms_data_key_spec_test.go and what remains at those two sites is that a
//     *symmetric* key does report one, which assertion 3 already walks.
//
// Every call goes over the wire (#765).

// kmsKeyMaterialIDShape is the constraint AWS publishes for all six members: "Length Constraints: Fixed
// length of 64" and "Pattern: ^[a-f0-9]+$".
//
// Written as one anchored expression rather than a length check plus a character-class check, because the
// fixed length is what rules out the obvious near-miss — a truncated or reshaped digest, which
// [cfnDeterministicUUID] produces from the same hash and which would satisfy the pattern alone.
var kmsKeyMaterialIDShape = regexp.MustCompile(`^[a-f0-9]{64}$`)

// kmsMaterialIDOf reads a key's CurrentKeyMaterialId through DescribeKey, requiring it to be present.
//
// Every cross-operation assertion below compares against this rather than against a value captured from
// CreateKey, because DescribeKey is the call a consumer actually has: a test that compared Decrypt's
// member against CreateKey's response would pass against an emulator that reported the value once and
// then lost it.
func kmsMaterialIDOf(t *testing.T, ts *emulator.TestServer, keyID string) string {
	t.Helper()
	return kmsMetadataString(t, kmsKeyMetadata(t, ts, keyID), "CurrentKeyMaterialId")
}

// kmsCiphertextUnder encrypts a plaintext under a key and returns the ciphertext, requiring success.
func kmsCiphertextUnder(t *testing.T, ts *emulator.TestServer, keyID, plaintext string) string {
	t.Helper()
	out := kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte(plaintext)),
	})
	return kmsResponseString(t, out, "CiphertextBlob")
}

// kmsResponseString reads a top-level string member from a KMS response body, requiring it to be present.
func kmsResponseString(t *testing.T, body map[string]json.RawMessage, member string) string {
	t.Helper()
	raw, ok := body[member]
	require.True(t, ok, "the response should carry %s: %v", member, kmsMetadataMembers(body))
	var value string
	require.NoError(t, json.Unmarshal(raw, &value), "decode %s", member)
	return value
}

// TestKMSKeyMaterialID_ThePublishedShapeAndOneValuePerKey is assertion 1.
//
// The shape is AWS's, stated identically on all six members, and the fixed length of 64 is the half that
// pins the derivation: a SHA-256 digest hex-encodes to exactly 64 characters, so any reshaping of it —
// hyphenating into a UUID, truncating to fit a shorter field — fails here rather than at whichever consumer
// eventually validates the pattern.
//
// Distinctness across keys and stability across observations are the two properties an identifier has to
// have, and each fails a different plausible implementation: a constant satisfies stability, and minting
// per response satisfies distinctness.
func TestKMSKeyMaterialID_ThePublishedShapeAndOneValuePerKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	created := kmsCreateKeyMetadata(t, ts, map[string]any{"Description": "the first key"})
	first := kmsMetadataString(t, created, "CurrentKeyMaterialId")
	assert.Regexp(t, kmsKeyMaterialIDShape, first,
		"the published constraint is a fixed length of 64 and ^[a-f0-9]+$")

	keyID := kmsMetadataString(t, created, "KeyId")
	assert.Equal(t, first, kmsMaterialIDOf(t, ts, keyID),
		"CreateKey and DescribeKey report one key's material, not two")
	assert.Equal(t, first, kmsMaterialIDOf(t, ts, keyID),
		"a second observation reports the same value; nothing here is minted per response")

	_, secondKeyID := createKMSKey(t, ts)
	assert.NotEqual(t, first, kmsMaterialIDOf(t, ts, secondKeyID),
		"two keys hold two different materials")
}

// TestKMSKeyMaterialID_OnlyASymmetricEncryptionKeyReportsOne is assertion 2, and it is the reason
// CurrentKeyMaterialId left kmsUnreachableMetadataMembers rather than simply becoming unconditional.
//
// API_KeyMetadata confines the member to "symmetric encryption keys with AWS_KMS or EXTERNAL origin".
// Substrate's only origin is AWS_KMS, so what remains is the key spec — and the phrase is "symmetric
// encryption key", not "symmetric key". The four HMAC specs are the rows that distinguish the two
// readings: an HMAC key is symmetric, holds key material, and encrypts nothing, so an implementation that
// read the condition as "not asymmetric" passes every other row here and fails those four.
//
// Every spec substrate accepts is walked, so a spec added later is covered without anyone remembering to
// add a row.
func TestKMSKeyMaterialID_OnlyASymmetricEncryptionKeyReportsOne(t *testing.T) {
	t.Parallel()

	specs := make([]string, 0, len(kmsSpecAdmissibleUsages))
	for spec := range kmsSpecAdmissibleUsages {
		specs = append(specs, spec)
	}
	slices.Sort(specs)
	require.Len(t, specs, 17, "API_CreateKey publishes seventeen key specs")

	for _, keySpec := range specs {
		t.Run(keySpec, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			_, keyID := createKMSKeySpec(t, ts, keySpec)
			meta := kmsKeyMetadata(t, ts, keyID)

			if keySpec == "SYMMETRIC_DEFAULT" {
				assert.Regexp(t, kmsKeyMaterialIDShape,
					kmsMetadataString(t, meta, "CurrentKeyMaterialId"),
					"the one spec the published condition admits")
				return
			}
			assert.NotContains(t, meta, "CurrentKeyMaterialId",
				"%s is not a symmetric encryption key, so the member is absent rather than empty", keySpec)
		})
	}
}

// TestKMSKeyMaterialID_EveryOperationThatReportsItAgreesWithTheKey is assertion 3.
//
// Four operations, each compared against DescribeKey's value for the same key. The comparison is the
// content of the assertion: each member is separately capable of being a well-formed 64-hex string that
// belongs to no key, and nothing about a response body distinguishes the two.
//
// Decrypt is the row that matters most, because its value is the one AWS ties to the ciphertext — "the
// identifier of the key material used to decrypt the ciphertext" — while substrate reads it from the key.
// Those agree for as long as one key has one material, which is the reading [KMSKey.KeyMaterialID]
// records; the day rotation mints a second material, this row is what fails.
func TestKMSKeyMaterialID_EveryOperationThatReportsItAgreesWithTheKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	arn, keyID := createKMSKey(t, ts)
	want := kmsMaterialIDOf(t, ts, keyID)

	t.Run("Decrypt", func(t *testing.T) {
		ciphertext := kmsCiphertextUnder(t, ts, keyID, "the plaintext")
		out := kmsRawBody(t, ts, "Decrypt", map[string]any{"CiphertextBlob": ciphertext})
		assert.Equal(t, want, kmsResponseString(t, out, "KeyMaterialId"),
			"the material that decrypted the data is the material the key reports holding")
		assert.Equal(t, arn, kmsResponseString(t, out, "KeyId"),
			"and it is that key's response, not another's")
	})

	t.Run("GenerateDataKey", func(t *testing.T) {
		out := kmsRawBody(t, ts, "GenerateDataKey", map[string]any{"KeyId": keyID, "KeySpec": "AES_256"})
		assert.Equal(t, want, kmsResponseString(t, out, "KeyMaterialId"),
			"the material that wrapped the data key")
	})

	t.Run("GenerateDataKeyWithoutPlaintext", func(t *testing.T) {
		out := kmsRawBody(t, ts, "GenerateDataKeyWithoutPlaintext",
			map[string]any{"KeyId": keyID, "KeySpec": "AES_256"})
		assert.Equal(t, want, kmsResponseString(t, out, "KeyMaterialId"),
			"the sibling operation agrees, which #961 is the precedent for asserting")
	})

	t.Run("ReEncrypt under one key", func(t *testing.T) {
		ciphertext := kmsCiphertextUnder(t, ts, keyID, "the plaintext")
		out := kmsRawBody(t, ts, "ReEncrypt", map[string]any{
			"CiphertextBlob": ciphertext, "DestinationKeyId": keyID,
		})
		assert.Equal(t, want, kmsResponseString(t, out, "SourceKeyMaterialId"),
			"re-encrypting under the same key reports one material twice")
		assert.Equal(t, want, kmsResponseString(t, out, "DestinationKeyMaterialId"),
			"re-encrypting under the same key reports one material twice")
	})
}

// TestKMSKeyMaterialID_ReEncryptReportsOneMaterialPerSide is assertion 4.
//
// ReEncrypt is the only operation in the service whose response carries two key material identities, which
// makes it the only one where the values can be crossed or collapsed without the body ceasing to look
// correct. AWS's sample response renders two different values, and its two glosses name different things —
// "the key material used to originally encrypt the data" against "the key material used to reencrypt the
// data" — so both halves are asserted: that they differ, and that each is the material of the key on its
// own side.
//
// The same-key case is covered above, where the two values are deliberately equal. Together they show the
// members follow their keys rather than a rule about being equal or unequal.
func TestKMSKeyMaterialID_ReEncryptReportsOneMaterialPerSide(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, sourceKeyID := createKMSKey(t, ts)
	_, destKeyID := createKMSKey(t, ts)
	sourceWant := kmsMaterialIDOf(t, ts, sourceKeyID)
	destWant := kmsMaterialIDOf(t, ts, destKeyID)
	require.NotEqual(t, sourceWant, destWant, "two keys, two materials")

	out := kmsRawBody(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":   kmsCiphertextUnder(t, ts, sourceKeyID, "the plaintext"),
		"DestinationKeyId": destKeyID,
	})
	assert.Equal(t, sourceWant, kmsResponseString(t, out, "SourceKeyMaterialId"),
		"the source member is the source key's material")
	assert.Equal(t, destWant, kmsResponseString(t, out, "DestinationKeyMaterialId"),
		"the destination member is the destination key's material")
	assert.NotEqual(t,
		kmsResponseString(t, out, "SourceKeyMaterialId"),
		kmsResponseString(t, out, "DestinationKeyMaterialId"),
		"the data moved between two materials, as AWS's own sample response shows")
}

// TestKMSKeyMaterialID_EachReEncryptMemberFollowsItsOwnKey is assertion 5.
//
// AWS conditions the two members separately — each names the symmetric encryption key on its own side — so
// a ReEncrypt out of an asymmetric key and into a symmetric one reports the destination member and not the
// source. An implementation that applied one condition to both, on either key, reports both members or
// neither and fails here while passing every row above.
//
// The source key is asymmetric with KeyUsage ENCRYPT_DECRYPT, a pair AWS publishes and CreateKey must
// accept, and the source algorithm is one the spec admits. Its ciphertext is built by hand for the reason
// [kmsStubCiphertext] records: nothing but Encrypt mints one, and Encrypt would need the same algorithm
// threaded through to no additional effect. It uses [kmsStubCiphertextWith] because #979 made the blob
// record the algorithm that wrote it — a blob claiming SYMMETRIC_DEFAULT against a request naming
// RSAES_OAEP_SHA_256 is now refused as a mismatch before this assertion is reached.
func TestKMSKeyMaterialID_EachReEncryptMemberFollowsItsOwnKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, rsaKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "ENCRYPT_DECRYPT")
	_, destKeyID := createKMSKey(t, ts)

	out := kmsRawBody(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":            kmsStubCiphertextWith(rsaKeyID, "RSAES_OAEP_SHA_256", nil, []byte("the plaintext")),
		"SourceEncryptionAlgorithm": "RSAES_OAEP_SHA_256",
		"DestinationKeyId":          destKeyID,
	})
	assert.NotContains(t, out, "SourceKeyMaterialId",
		"an asymmetric source reports no material identity, per the member's own gloss")
	assert.Equal(t, kmsMaterialIDOf(t, ts, destKeyID),
		kmsResponseString(t, out, "DestinationKeyMaterialId"),
		"the destination member is unaffected by what the source was")
}

// TestKMSKeyMaterialID_TheOneOperationThatReportsNone is assertion 6, and it is a decision rather than
// behavior anyone would infer from the code.
//
// **Encrypt** publishes exactly CiphertextBlob, EncryptionAlgorithm and KeyId — no KeyMaterialId — where
// Decrypt, ReEncrypt and both GenerateDataKey* operations all publish one. The asymmetry is AWS's and is
// not obviously deliberate: Encrypt names the material it used no more than Decrypt names the material
// that produced the ciphertext it was handed. Asserted so that a sweep over "the operations that report
// key material" cannot quietly add a fifth site AWS does not have.
//
// It had a second half until #988: both GenerateDataKey* operations under an asymmetric key, which reported
// no material ID because reporting one would have put a value on a response AWS cannot produce. That was
// #978's stated workaround for substrate accepting a request AWS refuses, and the request is now refused —
// see TestKMSDataKeySpec_AnAsymmetricKeyIsRefused, which asserts the refusal in place of the omission. The
// omission itself is now unreachable at those two sites and is a guard; [kmsReportsKeyMaterialID] records
// that.
func TestKMSKeyMaterialID_TheOneOperationThatReportsNone(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)
	out := kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte("the plaintext")),
	})
	assert.NotContains(t, out, "KeyMaterialId",
		"API_Encrypt's Response Syntax is CiphertextBlob, EncryptionAlgorithm and KeyId")
	assert.Equal(t, []string{"CiphertextBlob", "EncryptionAlgorithm", "KeyId"},
		kmsMetadataMembers(out), "and those three exactly")
}
