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

// #979: KMS decoded EncryptionContext at none of the five operations that publish it, so the refusal AWS
// publishes for a mismatch was unreachable and an application that encrypted under one context and
// decrypted under another passed here and failed in production.
//
// Everything below goes over the wire through a real signed request (#765) and asserts the **status
// alongside the code** (#923), because InvalidCiphertextException at 400 is the whole of what a caller's
// error path matches on and a decoded error struct carries only the code.
//
// The published rule these assertions rest on is stated on API_Encrypt as a consequence rather than a
// footnote: "if you specify an EncryptionContext when encrypting data, you must specify the same
// encryption context (a case-sensitive exact match) when decrypting the data. Otherwise, the request to
// decrypt fails with an InvalidCiphertextException." API_GenerateDataKey repeats it verbatim for the data
// key it wraps, and InvalidCiphertextException's own gloss names the condition on both operations that can
// answer it: "the specified ciphertext, or additional authenticated data incorporated into the ciphertext,
// such as the encryption context, is corrupted, missing, or otherwise invalid."

// kmsTenantContext is the encryption context these tests encrypt under.
//
// Two entries rather than one, and one of them differing from the other only in case, because a
// single-entry context cannot distinguish a comparison that is exact from one that happens to agree on
// the first key it looks at.
var kmsTenantContext = map[string]string{"tenant": "acme", "Tenant": "Acme"}

// kmsEncryptWithContext encrypts a plaintext under a key and an encryption context, requiring success.
func kmsEncryptWithContext(
	t *testing.T, ts *emulator.TestServer, keyID, plaintext string, encryptionContext map[string]string,
) string {
	t.Helper()
	body := map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte(plaintext)),
	}
	if encryptionContext != nil {
		body["EncryptionContext"] = encryptionContext
	}
	return kmsResponseString(t, kmsRawBody(t, ts, "Encrypt", body), "CiphertextBlob")
}

// kmsDecryptedPlaintext decrypts a ciphertext under an encryption context, requiring success, and returns
// the plaintext it recovered.
//
// The plaintext is returned rather than discarded so that every success assertion here proves the round
// trip rather than merely a 200: a decrypt that answered success while handing back the wrong bytes would
// satisfy a status check.
func kmsDecryptedPlaintext(
	t *testing.T, ts *emulator.TestServer, ciphertext string, encryptionContext map[string]string,
) string {
	t.Helper()
	body := map[string]any{"CiphertextBlob": ciphertext}
	if encryptionContext != nil {
		body["EncryptionContext"] = encryptionContext
	}
	out := kmsRawBody(t, ts, "Decrypt", body)
	decoded, err := base64.StdEncoding.DecodeString(kmsResponseString(t, out, "Plaintext"))
	require.NoError(t, err, "decode the recovered plaintext")
	return string(decoded)
}

// TestKMSEncryptionContext_RoundTripsAndRefusesItsAbsence is the pair the issue exists for.
//
// One key, one plaintext, one context: repeating the context recovers the plaintext, and omitting it is
// refused. The second half is the case that passed before #979 — the exact case AWS says fails — and it is
// the one an application hits, because a caller that forgets a context forgets it at the decrypt end.
func TestKMSEncryptionContext_RoundTripsAndRefusesItsAbsence(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	ciphertext := kmsEncryptWithContext(t, ts, keyID, "the plaintext", kmsTenantContext)

	assert.Equal(t, "the plaintext", kmsDecryptedPlaintext(t, ts, ciphertext, kmsTenantContext),
		"repeating the encryption context recovers the plaintext")

	status, code, message := kmsRefusal(t, ts, "Decrypt", map[string]any{"CiphertextBlob": ciphertext})
	assert.Equal(t, http.StatusBadRequest, status, "InvalidCiphertextException is published at 400")
	assert.Equal(t, "InvalidCiphertextException", code,
		"API_Encrypt names this code for a context that is not repeated")
	assert.Contains(t, message, "EncryptionContext",
		"the refusal names the member, so a caller learns which half of the request is wrong")
}

// TestKMSEncryptionContext_EveryMismatchShape walks the ways two contexts can differ.
//
// AWS states one rule — "an exact case-sensitive match" — and every row is a way to fail it. The two
// asymmetric shapes are the pair the issue names: a context recorded and none supplied, and none recorded
// and one supplied. Neither is a special case in the code (maps.Equal answers both), which is exactly why
// both are asserted: an implementation that compared only the keys it was given would pass the value rows
// and fail these two.
//
// Case is split into a differing key and a differing value, because an implementation folding case would
// have to fold it in both places and might fold it in one.
func TestKMSEncryptionContext_EveryMismatchShape(t *testing.T) {
	t.Parallel()

	recorded := map[string]string{"tenant": "acme", "stage": "prod"}

	tests := []struct {
		name     string
		recorded map[string]string
		supplied map[string]string
	}{
		{"recorded and none supplied", recorded, nil},
		{"none recorded and one supplied", nil, recorded},
		{"a value differs", recorded, map[string]string{"tenant": "acme", "stage": "dev"}},
		{"a key differs", recorded, map[string]string{"tenant": "acme", "phase": "prod"}},
		{"a value differs in case only", recorded, map[string]string{"tenant": "Acme", "stage": "prod"}},
		{"a key differs in case only", recorded, map[string]string{"Tenant": "acme", "stage": "prod"}},
		{"an entry is missing", recorded, map[string]string{"tenant": "acme"}},
		{"an entry is added", recorded, map[string]string{"tenant": "acme", "stage": "prod", "extra": "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			ciphertext := kmsEncryptWithContext(t, ts, keyID, "the plaintext", tt.recorded)

			body := map[string]any{"CiphertextBlob": ciphertext}
			if tt.supplied != nil {
				body["EncryptionContext"] = tt.supplied
			}
			status, code, _ := kmsRefusal(t, ts, "Decrypt", body)
			assert.Equal(t, http.StatusBadRequest, status, "the published status")
			assert.Equal(t, "InvalidCiphertextException", code, "the published code")
		})
	}
}

// TestKMSEncryptionContext_AnEmptyContextEqualsAnAbsentOne is the one equivalence substrate asserts of its
// own accord.
//
// A caller sending EncryptionContext {} and a caller sending the member not at all have expressed the same
// thing, and AWS documents no way to tell them apart — the member is published Required: No with no
// statement that an empty map differs from an omitted one. Both directions are asserted, because the
// equivalence has to hold on the encrypt end and the decrypt end or a round trip through one and out the
// other is refused.
func TestKMSEncryptionContext_AnEmptyContextEqualsAnAbsentOne(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	empty := kmsEncryptWithContext(t, ts, keyID, "empty on encrypt", map[string]string{})
	assert.Equal(t, "empty on encrypt", kmsDecryptedPlaintext(t, ts, empty, nil),
		"an empty context recorded is no context recorded")

	absent := kmsEncryptWithContext(t, ts, keyID, "absent on encrypt", nil)
	assert.Equal(t, "absent on encrypt", kmsDecryptedPlaintext(t, ts, absent, map[string]string{}),
		"an empty context supplied is no context supplied")
}

// TestKMSEncryptionContext_TheBlobIsDeterministic is the replay assertion, and it is why the envelope is
// JSON rather than a delimited string.
//
// Two Encrypt calls with the same key, plaintext and context must produce byte-identical ciphertext, and
// the context's JSON key order must not reach the blob — json.Marshal sorts map keys, so the ordering is
// established by the encoding rather than by a canonicalization pass of substrate's own. A blob that
// varied with the order in which a caller happened to write its context would make a recorded run
// unreplayable, which is the property the whole emulator rests on.
//
// The two contexts are sent as raw JSON with the same entries in opposite order, because a Go map cannot
// express the distinction: decoding either into map[string]string loses it, so only the wire bytes can
// carry the case this asserts.
func TestKMSEncryptionContext_TheBlobIsDeterministic(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	encrypt := func(rawContext string) string {
		out := kmsRawBody(t, ts, "Encrypt", map[string]any{
			"KeyId":             keyID,
			"Plaintext":         base64.StdEncoding.EncodeToString([]byte("the plaintext")),
			"EncryptionContext": json.RawMessage(rawContext),
		})
		return kmsResponseString(t, out, "CiphertextBlob")
	}

	first := encrypt(`{"alpha":"1","beta":"2"}`)
	second := encrypt(`{"beta":"2","alpha":"1"}`)

	assert.Equal(t, first, second,
		"one input produces one ciphertext, whatever order the context arrived in")
	assert.Equal(t, first, encrypt(`{"alpha":"1","beta":"2"}`),
		"and the same call twice produces the same blob, so a recorded run replays")
}

// TestKMSEncryptionContext_DecryptRefusesAMismatchedAlgorithm is the second refusal the recorded envelope
// makes reachable.
//
// API_Decrypt: "specify the same algorithm that was used to encrypt the data. If you specify a different
// algorithm, the Decrypt operation fails." Both algorithms here are ones the key spec admits, so
// [kmsIncompatibleEncryptionAlgorithm]'s check passes and what fails is the comparison against the
// ciphertext — which is the distinction the two refusals exist to keep apart. AWS names no code for this
// one; InvalidCiphertextException is substrate's reading, recorded in kms_errors.go.
//
// An RSA key is the only way to reach it: SYMMETRIC_DEFAULT admits exactly one algorithm, so a symmetric
// key has no second value to disagree about.
func TestKMSEncryptionContext_DecryptRefusesAMismatchedAlgorithm(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, rsaKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "ENCRYPT_DECRYPT")

	ciphertext := kmsResponseString(t, kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":               rsaKeyID,
		"Plaintext":           base64.StdEncoding.EncodeToString([]byte("the plaintext")),
		"EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
	}), "CiphertextBlob")

	out := kmsRawBody(t, ts, "Decrypt", map[string]any{
		"CiphertextBlob":      ciphertext,
		"EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
	})
	recovered, err := base64.StdEncoding.DecodeString(kmsResponseString(t, out, "Plaintext"))
	require.NoError(t, err, "decode the recovered plaintext")
	assert.Equal(t, "the plaintext", string(recovered), "the same algorithm decrypts")

	status, code, message := kmsRefusal(t, ts, "Decrypt", map[string]any{
		"CiphertextBlob":      ciphertext,
		"EncryptionAlgorithm": "RSAES_OAEP_SHA_1",
	})
	assert.Equal(t, http.StatusBadRequest, status, "InvalidCiphertextException is published at 400")
	assert.Equal(t, "InvalidCiphertextException", code,
		"an algorithm the key admits but which did not write these bytes is a ciphertext refusal")
	assert.Contains(t, message, "RSAES_OAEP_SHA_256",
		"the refusal names the recorded algorithm, which API_ReEncrypt tells a caller to keep anyway")
}

// TestKMSEncryptionContext_AnAsymmetricKeyRecordsNoContext is the decision AWS's own text forces, and it is
// the one place substrate accepts a member and ignores it.
//
// API_ReEncrypt, verbatim and decisive: "a destination encryption context is valid only when the
// destination KMS key is a symmetric encryption KMS key. The standard ciphertext format for asymmetric KMS
// keys does not include fields for metadata." So an asymmetric ciphertext has nowhere to hold a context,
// and AWS publishes no code for sending one — recording it would invent a refusal AWS cannot produce.
// Accepting and ignoring is #827's honest reading of an argument with no published outcome.
//
// The assertion is deliberately the *opposite* shape from every other one here: two different contexts
// across the encrypt and the decrypt must **succeed**. A later change that recorded the context
// unconditionally would fail exactly this and nothing else.
func TestKMSEncryptionContext_AnAsymmetricKeyRecordsNoContext(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, rsaKeyID := createKMSKeySpecUsage(t, ts, "RSA_2048", "ENCRYPT_DECRYPT")

	ciphertext := kmsResponseString(t, kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":               rsaKeyID,
		"Plaintext":           base64.StdEncoding.EncodeToString([]byte("the plaintext")),
		"EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
		"EncryptionContext":   map[string]string{"tenant": "acme"},
	}), "CiphertextBlob")

	out := kmsRawBody(t, ts, "Decrypt", map[string]any{
		"CiphertextBlob":      ciphertext,
		"EncryptionAlgorithm": "RSAES_OAEP_SHA_256",
		"EncryptionContext":   map[string]string{"tenant": "different"},
	})
	recovered, err := base64.StdEncoding.DecodeString(kmsResponseString(t, out, "Plaintext"))
	require.NoError(t, err, "decode the recovered plaintext")
	assert.Equal(t, "the plaintext", string(recovered),
		"an asymmetric ciphertext holds no context, so there is nothing for a decrypt to disagree with")
}

// TestKMSEncryptionContext_BothGenerateDataKeyOperationsRecordIt covers the two operations whose blob a
// caller cannot inspect.
//
// API_GenerateDataKey states the round trip in its own words — "if you specify an EncryptionContext, you
// must specify the same encryption context (a case-sensitive exact match) when decrypting the data.
// Otherwise, the request to decrypt fails with an InvalidCiphertextException" — and neither page publishes
// InvalidCiphertextException itself, which is consistent: the recording happens here and the refusal
// happens at Decrypt. Both operations are walked because #969 found what a per-site behavior produces, and
// GenerateDataKeyWithoutPlaintext is the one that had been forgotten then.
func TestKMSEncryptionContext_BothGenerateDataKeyOperationsRecordIt(t *testing.T) {
	t.Parallel()

	for _, op := range []string{"GenerateDataKey", "GenerateDataKeyWithoutPlaintext"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, keyID := createKMSKey(t, ts)

			body := map[string]any{"KeyId": keyID, "EncryptionContext": kmsTenantContext}
			if op == "GenerateDataKey" {
				body["NumberOfBytes"] = 32
			}
			ciphertext := kmsResponseString(t, kmsRawBody(t, ts, op, body), "CiphertextBlob")

			// The data key itself is opaque, so the assertion is that the *decrypt* succeeds under the
			// recorded context and is refused without it. That is the whole of what a caller can observe.
			kmsDecryptedPlaintext(t, ts, ciphertext, kmsTenantContext)

			status, code, _ := kmsRefusal(t, ts, "Decrypt", map[string]any{"CiphertextBlob": ciphertext})
			assert.Equal(t, http.StatusBadRequest, status, "the published status")
			assert.Equal(t, "InvalidCiphertextException", code,
				"a wrapped data key is refused for a mismatched context like any other ciphertext")
		})
	}
}

// TestKMSEncryptionContext_ReEncryptMatchesTheSourceAndWritesTheDestination is the operation where the two
// context members are visibly different things.
//
// SourceEncryptionContext is matched against the incoming blob — "enter the same encryption context that
// was used to encrypt the ciphertext" — while DestinationEncryptionContext is written into the outgoing
// one. So a ReEncrypt is the call that *changes* a ciphertext's context, and the assertions follow that:
// the wrong source context is refused, the right one succeeds, and the resulting blob answers to the
// destination context and not to the source's.
//
// The last row is the one worth having: re-encrypting with no destination context **strips** the context,
// which follows from the destination member being a write rather than a merge. Nothing in AWS's text
// suggests a context is inherited across a ReEncrypt, and a caller relying on inheritance would find its
// data readable without the context it thought it had set.
func TestKMSEncryptionContext_ReEncryptMatchesTheSourceAndWritesTheDestination(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, sourceKeyID := createKMSKey(t, ts)
	_, destKeyID := createKMSKey(t, ts)

	sourceContext := map[string]string{"tenant": "acme"}
	destContext := map[string]string{"tenant": "acme", "stage": "prod"}
	ciphertext := kmsEncryptWithContext(t, ts, sourceKeyID, "the plaintext", sourceContext)

	status, code, message := kmsRefusal(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":          ciphertext,
		"DestinationKeyId":        destKeyID,
		"SourceEncryptionContext": map[string]string{"tenant": "other"},
	})
	assert.Equal(t, http.StatusBadRequest, status, "the published status")
	assert.Equal(t, "InvalidCiphertextException", code,
		"the source end refuses exactly what Decrypt refuses, through the same helper")
	assert.Contains(t, message, "SourceEncryptionContext",
		"and names its own member rather than Decrypt's")

	reEncrypted := kmsResponseString(t, kmsRawBody(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":               ciphertext,
		"DestinationKeyId":             destKeyID,
		"SourceEncryptionContext":      sourceContext,
		"DestinationEncryptionContext": destContext,
	}), "CiphertextBlob")

	assert.Equal(t, "the plaintext", kmsDecryptedPlaintext(t, ts, reEncrypted, destContext),
		"the new blob answers to the destination context")

	status, code, _ = kmsRefusal(t, ts, "Decrypt", map[string]any{
		"CiphertextBlob":    reEncrypted,
		"EncryptionContext": sourceContext,
	})
	assert.Equal(t, http.StatusBadRequest, status, "the published status")
	assert.Equal(t, "InvalidCiphertextException", code,
		"and not to the source's, which the ReEncrypt replaced")

	stripped := kmsResponseString(t, kmsRawBody(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":          ciphertext,
		"DestinationKeyId":        destKeyID,
		"SourceEncryptionContext": sourceContext,
	}), "CiphertextBlob")
	assert.Equal(t, "the plaintext", kmsDecryptedPlaintext(t, ts, stripped, nil),
		"a ReEncrypt with no destination context strips the context rather than inheriting it")
}

// TestKMSEncryptionContext_TheRefusalNamesBothContexts pins what the message says, which needs a test
// because it is caller data being rendered into an error.
//
// AWS settles that it may be: the member's gloss is "an encryption context is a collection of non-secret
// key-value pairs", and API_GenerateDataKey adds "do not include confidential or sensitive information in
// this field. This field may be displayed in plaintext in CloudTrail logs and other output." A refusal
// message is that other output. Naming both sides is the only thing that makes the refusal actionable — a
// caller whose context differs in one character cannot act on a message that will not say which.
//
// The absent side renders as "none" rather than as an empty string, so the sentence reads as a sentence.
func TestKMSEncryptionContext_TheRefusalNamesBothContexts(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	ciphertext := kmsEncryptWithContext(t, ts, keyID, "the plaintext", map[string]string{"tenant": "acme"})

	_, _, message := kmsRefusal(t, ts, "Decrypt", map[string]any{"CiphertextBlob": ciphertext})
	assert.Contains(t, message, `"tenant"="acme"`, "the recorded context is named")
	assert.Contains(t, message, "none", "and the absent one reads as a word rather than as nothing")

	_, _, message = kmsRefusal(t, ts, "Decrypt", map[string]any{
		"CiphertextBlob":    ciphertext,
		"EncryptionContext": map[string]string{"tenant": "other"},
	})
	assert.Contains(t, message, `"tenant"="other"`, "the supplied context is named")
	assert.Contains(t, message, `"tenant"="acme"`, "alongside the recorded one it failed to match")
}

// TestKMSEncryptionContext_ABlobFromAnotherSourceIsRefused covers what the envelope's format marker is for.
//
// A real AWS ciphertext, a truncated blob, and a blob in the format substrate shipped before #979 are all
// refused rather than misread. The last is the one worth stating: the previous format was
// base64("kms:{keyID}:" + base64(plaintext)), and reading its fields under the new meanings would produce
// a key ID from arbitrary bytes rather than an error. This is the compatibility break the CHANGELOG names.
func TestKMSEncryptionContext_ABlobFromAnotherSourceIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	tests := []struct {
		name       string
		ciphertext string
	}{
		{"not base64", "!!! not base64 !!!"},
		{"base64 but not JSON", base64.StdEncoding.EncodeToString([]byte("not json at all"))},
		{"JSON without the format marker", base64.StdEncoding.EncodeToString(
			[]byte(`{"keyId":"` + keyID + `","algorithm":"SYMMETRIC_DEFAULT","plaintext":"eHg="}`))},
		{"the pre-#979 format", base64.StdEncoding.EncodeToString(
			[]byte("kms:" + keyID + ":" + base64.StdEncoding.EncodeToString([]byte("the plaintext"))))},
		{"an envelope naming no key", base64.StdEncoding.EncodeToString(
			[]byte(`{"format":"substrate-kms-v2","algorithm":"SYMMETRIC_DEFAULT","plaintext":"eHg="}`))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, code, _ := kmsRefusal(t, ts, "Decrypt", map[string]any{"CiphertextBlob": tt.ciphertext})
			assert.Equal(t, http.StatusBadRequest, status, "the published status")
			assert.Equal(t, "InvalidCiphertextException", code,
				"a blob substrate did not write is refused rather than read under the wrong field meanings")
		})
	}
}
