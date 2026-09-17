package emulator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// KMS's stub ciphertext: what a substrate ciphertext carries, and the two refusals that carrying it
// makes reachable.
//
// Substrate performs no cryptography. Its ciphertext is a reversible envelope, and until #979 that
// envelope held a key ID and a plaintext — base64("kms:{keyID}:" + base64(plaintext)) — which was
// enough for Decrypt to find its key and nothing else. Two refusals AWS publishes were therefore
// unreachable, both of them about a decrypt request disagreeing with the encrypt request that produced
// the ciphertext:
//
//   - An encryption context that is not an exact case-sensitive match. API_Encrypt states it as a
//     consequence rather than a footnote: "if you specify an EncryptionContext when encrypting data, you
//     must specify the same encryption context (a case-sensitive exact match) when decrypting the data.
//     Otherwise, the request to decrypt fails with an InvalidCiphertextException."
//   - An encryption algorithm other than the one that encrypted the data. API_Decrypt: "specify the same
//     algorithm that was used to encrypt the data. If you specify a different algorithm, the Decrypt
//     operation fails."
//
// Both are worth having for the reason substrate exists: an application that encrypts with
// EncryptionContext {"tenant": "acme"} and decrypts without it is broken in production, and before #979
// it passed here. The context was decoded by nobody at any of the five operations that take one.
//
// # What the envelope holds, and why each field is in it
//
// The envelope is JSON rather than a delimited string, and that is a correctness requirement rather
// than a preference: an encryption context is caller-supplied text, so a key or a value may contain the
// delimiter. A colon-separated format would need escaping, and an escaping bug is a ciphertext that
// decrypts to the wrong context — the exact failure this file exists to detect. json.Marshal sorts map
// keys, so the blob is deterministic without a canonicalization step of substrate's own, which is the
// same property computeStateHash relies on.
//
// The key ID and the algorithm are recorded for every key. The context is recorded only for a symmetric
// encryption key, and that split is AWS's:
//
//   - The context is authenticated data that lives *in* the ciphertext, and AWS says its asymmetric
//     format has nowhere to put it — "a destination encryption context is valid only when the
//     destination KMS key is a symmetric encryption KMS key. The standard ciphertext format for
//     asymmetric KMS keys does not include fields for metadata." Recording one for an asymmetric key
//     would invent a refusal AWS cannot produce.
//   - The algorithm is recorded regardless, although AWS's asymmetric format holds no metadata either,
//     because AWS reaches the same *observable* answer cryptographically: decrypting RSA ciphertext
//     with the wrong OAEP hash fails. Substrate models the observation, not the mechanism, so it
//     records the value it needs to produce the refusal AWS publishes. That is the scope rule applied
//     rather than bent — the divergence is in the blob's bytes, which no caller is entitled to read,
//     and the agreement is in the answer, which every caller is.
//
// One consequence of AWS's own asymmetry is recorded and not fixed here: substrate's blob names a key
// ID even for an asymmetric key, which AWS's format cannot, so ReEncrypt can find an asymmetric source
// key that AWS would need SourceKeyId for. That predates #979 and is unrelated to the context.

// kmsStubFormat marks a blob as substrate's stub ciphertext, and marks *which* stub format it is.
//
// It is a version string rather than a bare tag because #979 is the second format and there may be a
// third: a blob from a substrate that predates this release fails to decode, which is the honest
// answer, and a blob from a later format will fail against this one rather than being read with the
// wrong field meanings.
const kmsStubFormat = "substrate-kms-v2"

// kmsStubEnvelope is everything substrate's stub cipher carries from an encrypt to the decrypt that
// reverses it.
//
// Field order is the marshaled order, and the plaintext is last so a human reading a decoded blob
// sees what the ciphertext is *about* before the payload. Context is omitempty so that a blob with no
// context and a blob with an empty one are byte-identical, which is the same equivalence
// [kmsEncryptionContextEqual] applies on the way back in.
type kmsStubEnvelope struct {
	Format    string            `json:"format"`
	KeyID     string            `json:"keyId"`
	Algorithm string            `json:"algorithm"`
	Context   map[string]string `json:"encryptionContext,omitempty"`
	Plaintext []byte            `json:"plaintext"`
}

// kmsIsSymmetricEncryptionKey reports whether a key is a symmetric *encryption* key, which is the
// condition four different AWS glosses reduce to here.
//
// "Symmetric encryption key" is narrower than "symmetric key", and the four HMAC specs are the
// difference: an HMAC key is symmetric, holds key material and encrypts nothing. An implementation that
// read the condition as "not asymmetric" would be wrong on exactly those four specs, which is why this
// is a named predicate rather than an inline comparison at each site.
//
// Origin does not appear in it although two of the four glosses mention one — CurrentKeyMaterialId is
// "present for symmetric encryption keys with AWS_KMS or EXTERNAL origin" — because substrate mints no
// other origin (#984), so the origin half of the condition is always satisfied.
//
// The rotation guard's identical-looking test is deliberately not routed through this. Rotation is
// restricted to SYMMETRIC_DEFAULT because it is "the only spec that rotates", a different published
// rule that happens to name the same spec; collapsing the two would tie an unrelated pair of
// behaviors together.
func kmsIsSymmetricEncryptionKey(key *KMSKey) bool {
	return key.KeySpec == kmsSymmetricDefaultKeySpec
}

// kmsEncryptStub produces the deterministic stub ciphertext substrate hands back from Encrypt,
// GenerateDataKey, GenerateDataKeyWithoutPlaintext and ReEncrypt's destination end.
//
// It takes the key rather than a key ID because whether the encryption context is recorded is a
// property of the key, per the file comment above.
//
// The algorithm is whatever the caller passes and is not re-checked here. At the three sites that take
// an EncryptionAlgorithm member it has already been through [kmsResolveEncryptionAlgorithm] and
// [kmsCheckEncryptionAlgorithmForKey], so it is a value the key admits. The two GenerateDataKey*
// operations take no such member and pass SYMMETRIC_DEFAULT, because the data key they wrap is wrapped
// under a symmetric encryption key — which is true of every such call AWS accepts, and false only on
// the RSA path #988 exists to close.
func kmsEncryptStub(key *KMSKey, algorithm string, encryptionContext map[string]string, plaintext []byte) []byte {
	envelope := kmsStubEnvelope{
		Format:    kmsStubFormat,
		KeyID:     key.KeyID,
		Algorithm: algorithm,
		Plaintext: plaintext,
	}
	if len(encryptionContext) > 0 && kmsIsSymmetricEncryptionKey(key) {
		envelope.Context = maps.Clone(encryptionContext)
	}
	// The error is unreachable: every field is a string, a string map or a byte slice, none of which
	// json.Marshal can fail on. Panicking rather than returning it keeps the four call sites from each
	// growing an error path for a condition none of them can provoke, which is the disposition
	// [randomHex] already takes for an equally unreachable crypto/rand failure.
	raw, err := json.Marshal(envelope)
	if err != nil {
		panic(fmt.Sprintf("kms encrypt stub: marshal envelope: %v", err))
	}
	return []byte(base64.StdEncoding.EncodeToString(raw))
}

// kmsDecryptStub reverses [kmsEncryptStub], returning the whole envelope rather than the two fields a
// caller strictly needs.
//
// Decrypt and ReEncrypt each need the key ID and the plaintext to proceed and the algorithm and the
// context to *refuse*, and returning one value keeps the two operations reading the same thing. The
// format check is what makes a blob from another source — a real AWS ciphertext, a truncated one, a
// blob from the pre-#979 format — a refusal rather than a misread.
func kmsDecryptStub(ciphertext []byte) (*kmsStubEnvelope, error) {
	raw, err := base64.StdEncoding.DecodeString(string(ciphertext))
	if err != nil {
		return nil, fmt.Errorf("kms decrypt stub: decode outer: %w", err)
	}
	var envelope kmsStubEnvelope
	if unmarshalErr := json.Unmarshal(raw, &envelope); unmarshalErr != nil {
		return nil, fmt.Errorf("kms decrypt stub: decode envelope: %w", unmarshalErr)
	}
	if envelope.Format != kmsStubFormat {
		return nil, fmt.Errorf("kms decrypt stub: format is %q, not %q", envelope.Format, kmsStubFormat)
	}
	if envelope.KeyID == "" {
		return nil, fmt.Errorf("kms decrypt stub: the envelope names no key")
	}
	return &envelope, nil
}

// kmsCheckCiphertextMatchesRequest refuses a decrypt request that disagrees with the encrypt request
// that produced the ciphertext.
//
// Shared by Decrypt and by ReEncrypt's source end, which is the point of it being a function: AWS
// glosses the two with the same sentences and publishes the same code on both, and #969 found what a
// per-site check produces when it discovered one operation refusing nothing that its four siblings
// refused. The member names differ between the two (EncryptionAlgorithm against
// SourceEncryptionAlgorithm) so they are passed in, following [kmsResolveEncryptionAlgorithm].
//
// The algorithm is checked before the context. AWS orders the two nowhere, so this is recorded rather
// than matched, and the reason is that the algorithm is what a real implementation needs in order to
// attempt a decryption at all, where the context is authenticated once the decryption has happened. A
// caller wrong about both hears about the one that would have stopped it first.
//
// Both refusals are InvalidCiphertextException at 400, which is published on both operations and whose
// gloss names one of the two conditions outright: "the specified ciphertext, or additional
// authenticated data incorporated into the ciphertext, such as the encryption context, is corrupted,
// missing, or otherwise invalid". The algorithm case rests on "or otherwise invalid" plus the two
// operations' own statements that the decrypt fails — neither page names a code for it, so that half is
// substrate's reading of a published behavior. IncorrectKeyException is the near miss and is wrong for
// both: it is about the key the caller *named*, which [KMSPlugin.checkNamedKeyMatches] answers.
func kmsCheckCiphertextMatchesRequest(
	key *KMSKey,
	envelope *kmsStubEnvelope,
	algorithmMember, contextMember, algorithm string,
	encryptionContext map[string]string,
) *AWSError {
	if algorithm != envelope.Algorithm {
		return kmsCiphertextAlgorithmMismatch(algorithmMember, envelope.Algorithm, algorithm)
	}
	// The context is compared only where it was recorded, for the reason the file comment gives: an
	// asymmetric key's ciphertext holds no context on AWS, so nothing about a context sent with one is a
	// statement AWS makes. Such a member is accepted and ignored, which is what AWS does with an
	// argument it publishes no code for.
	if !kmsIsSymmetricEncryptionKey(key) {
		return nil
	}
	if !kmsEncryptionContextEqual(envelope.Context, encryptionContext) {
		return kmsCiphertextContextMismatch(contextMember, envelope.Context, encryptionContext)
	}
	return nil
}

// kmsEncryptionContextEqual reports whether two encryption contexts are the exact case-sensitive match
// AWS requires.
//
// maps.Equal is the whole of it, and it gives the two asymmetric cases for free: a context recorded and
// none supplied, and none recorded and one supplied, are both unequal. An absent map and an empty one
// compare equal, which is the one equivalence substrate asserts of its own accord — a caller sending
// EncryptionContext {} and a caller sending the member not at all have expressed the same thing, and
// AWS documents no way to tell them apart on the wire.
//
// Case sensitivity is Go's string comparison and needs no code, but it is the property most likely to
// be "helpfully" relaxed later, so it is named here and pinned by a test.
func kmsEncryptionContextEqual(recorded, supplied map[string]string) bool {
	return maps.Equal(recorded, supplied)
}

// kmsFormatEncryptionContext renders an encryption context for a refusal message, in sorted key order.
//
// Sorted so that one mismatch produces one message however the caller's JSON was ordered, which is the
// determinism rule applied to an error string rather than to a response body.
func kmsFormatEncryptionContext(encryptionContext map[string]string) string {
	if len(encryptionContext) == 0 {
		return "none"
	}
	pairs := make([]string, 0, len(encryptionContext))
	for _, k := range slices.Sorted(maps.Keys(encryptionContext)) {
		pairs = append(pairs, fmt.Sprintf("%q=%q", k, encryptionContext[k]))
	}
	return strings.Join(pairs, ", ")
}
