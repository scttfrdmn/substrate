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

// #969: Decrypt's KeyId and ReEncrypt's SourceKeyId are constraints, and a caller that gets one wrong
// is refused.
//
// Both members had been decoded and then discarded, so naming the wrong key was indistinguishable from
// naming the right one: substrate found the key inside the ciphertext, used it, and answered 200. AWS
// glosses the two members with the same sentence and gives them the same code — "enter a key ID of the
// KMS key that was used to encrypt the ciphertext. If you identify a different KMS key, the operation
// throws an IncorrectKeyException" — which is why one helper answers both and one table tests both.
//
// Four things are asserted, each of which the one before it would otherwise hide:
//
//  1. **A wrong key is refused at all**, with IncorrectKeyException at 400, and the message names both
//     ARNs — the whole content of the refusal is that two identifiers differ, and a caller holding a
//     ciphertext it did not create has no other way to learn which key it needs.
//  2. **Every accepted form of the member is accepted.** The check resolves the member to a key and
//     compares ARNs rather than comparing strings, so a key ARN and an alias must both still work. A
//     string comparison would pass assertion 1 and fail here, and that is the likeliest wrong
//     implementation.
//  3. **A member naming nothing answers NotFoundException, not IncorrectKeyException**, because that is
//     the truth about it. Collapsing the two would tell a caller with a typo to go find a different key.
//  4. **The wrong-key refusal precedes the key-state check.** A request defect comes before a resource
//     condition, which is the order #964 took for a range check — and this is the case that shows the
//     two are ordered at all, since only one of the two candidate answers can be reported.
//
// Every call goes over the wire and every assertion pairs the code with the status, per #765 and #923.

// kmsCiphertext mints a ciphertext under a named key, so a test can hold two keys and a blob belonging
// to exactly one of them.
//
// [kmsCryptoFixture] creates the key it encrypts under, which is right for a test about one key's state
// and wrong here: the point of every test below is that two keys exist and the member names the wrong
// one.
func kmsCiphertext(t *testing.T, ts *emulator.TestServer, keyID string) string {
	t.Helper()
	body := kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
	})
	var ciphertext string
	require.NoError(t, json.Unmarshal(body["CiphertextBlob"], &ciphertext), "decode CiphertextBlob")
	require.NotEmpty(t, ciphertext, "Encrypt reports a ciphertext")
	return ciphertext
}

// kmsNamedKeyOperations are the two operations that take a key identifier constraining which key may
// decrypt a ciphertext.
//
// The body builder takes the identifier separately from the ciphertext because that is the whole
// variable under test: every row below sends one ciphertext and varies only what the member says about
// it. ReEncrypt additionally needs a destination, which is passed in already enabled so that nothing
// but the source constraint can be refusing.
var kmsNamedKeyOperations = []struct {
	name   string
	member string
	body   func(ciphertext, named, destKeyID string) map[string]any
}{
	{"Decrypt", "KeyId", func(ciphertext, named, _ string) map[string]any {
		body := map[string]any{"CiphertextBlob": ciphertext}
		if named != "" {
			body["KeyId"] = named
		}
		return body
	}},
	{"ReEncrypt", "SourceKeyId", func(ciphertext, named, destKeyID string) map[string]any {
		body := map[string]any{"CiphertextBlob": ciphertext, "DestinationKeyId": destKeyID}
		if named != "" {
			body["SourceKeyId"] = named
		}
		return body
	}},
}

// TestKMSNamedKey_NamingADifferentKeyIsRefused is assertion 1, and it is the refusal #969 exists for.
//
// Both keys are enabled and both are real, so IncorrectKeyException is the only code that can be
// correct: there is no state to complain about and nothing is missing. Before the fix both operations
// answered 200 and quietly used the ciphertext's own key.
func TestKMSNamedKey_NamingADifferentKeyIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	encryptARN, encryptKeyID := createKMSKey(t, ts)
	otherARN, otherKeyID := createKMSKey(t, ts)
	ciphertext := kmsCiphertext(t, ts, encryptKeyID)
	_, destKeyID := createKMSKey(t, ts)

	for _, op := range kmsNamedKeyOperations {
		t.Run(op.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, op.name, op.body(ciphertext, otherKeyID, destKeyID))
			assert.Equal(t, "IncorrectKeyException", code, "%s with a %s naming another key", op.name, op.member)
			assert.Equal(t, http.StatusBadRequest, status, "%s with a wrong %s", op.name, op.member)
			assert.Contains(t, message, otherARN, "the message names the key the caller asked for")
			assert.Contains(t, message, encryptARN, "the message names the key the ciphertext needs")
		})
	}
}

// TestKMSNamedKey_EveryAcceptedFormOfTheMemberIsAccepted is assertion 2, and it is the test a string
// comparison fails.
//
// AWS accepts a key ID, a key ARN, an alias name and an alias ARN wherever it takes a key identifier,
// and substrate resolves all four through resolveKeyTarget. Since the ciphertext carries a bare key ID,
// an implementation that compared the member against that string would refuse three of these four
// correct requests. The absent row is here too: both members are published Required: No, so omitting
// one is not a refusal but the operation proceeding on the ciphertext's own key.
func TestKMSNamedKey_EveryAcceptedFormOfTheMemberIsAccepted(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	encryptARN, encryptKeyID := createKMSKey(t, ts)
	ciphertext := kmsCiphertext(t, ts, encryptKeyID)
	_, destKeyID := createKMSKey(t, ts)

	const aliasName = "alias/incorrect-key-test"
	status, code := kmsCreateAlias(t, ts, kmsTarget, aliasName, encryptKeyID)
	require.Empty(t, code, "CreateAlias")
	require.Equal(t, http.StatusOK, status, "CreateAlias")

	for _, form := range []struct {
		name  string
		named string
	}{
		{"absent", ""},
		{"a bare key ID", encryptKeyID},
		{"a key ARN", encryptARN},
		{"an alias name", aliasName},
		{"an alias ARN", "arn:aws:kms:us-east-1:" + taggingTestAccount + ":" + aliasName},
	} {
		for _, op := range kmsNamedKeyOperations {
			t.Run(op.name+" with "+form.name, func(t *testing.T) {
				gotStatus, gotCode := kmsCall(t, ts, op.name, op.body(ciphertext, form.named, destKeyID))
				assert.Empty(t, gotCode, "%s with %s as %s", op.name, form.name, op.member)
				assert.Equal(t, http.StatusOK, gotStatus, "%s with %s as %s", op.name, form.name, op.member)
			})
		}
	}
}

// TestKMSNamedKey_AMemberNamingNothingIsNotFound is assertion 3.
//
// [kmsAbsentKeyID] is well formed and names no key, so resolution succeeds and the *lookup* is what
// fails. NotFoundException is what every KMS operation answers for that, and answering
// IncorrectKeyException instead would be the wrong remedy: there is no correct key for the caller to go
// and find, only a typo to fix.
func TestKMSNamedKey_AMemberNamingNothingIsNotFound(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, encryptKeyID := createKMSKey(t, ts)
	ciphertext := kmsCiphertext(t, ts, encryptKeyID)
	_, destKeyID := createKMSKey(t, ts)

	for _, op := range kmsNamedKeyOperations {
		t.Run(op.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, op.name, op.body(ciphertext, kmsAbsentKeyID, destKeyID))
			assert.Equal(t, "NotFoundException", code, "%s with a %s naming no key", op.name, op.member)
			assert.Equal(t, http.StatusBadRequest, status, "%s with an absent %s", op.name, op.member)
			assert.Contains(t, message, op.member, "the message names the member that was wrong")
		})
	}
}

// TestKMSNamedKey_TheWrongKeyIsReportedBeforeTheKeyState is assertion 4, and it pins an order AWS does
// not publish.
//
// The ciphertext's key is disabled and the member names a different, enabled key, so there are two
// candidate answers and only one can be reported. Substrate reports the wrong-key refusal, because a
// caller that named the wrong key has a request to fix and telling it about the state of a key it did
// not ask for sends it after the wrong remedy. That is the same direction #964 took, and the assertion
// is written so the other order is visible rather than silent.
func TestKMSNamedKey_TheWrongKeyIsReportedBeforeTheKeyState(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, encryptKeyID := createKMSKey(t, ts)
	ciphertext := kmsCiphertext(t, ts, encryptKeyID)
	_, otherKeyID := createKMSKey(t, ts)
	_, destKeyID := createKMSKey(t, ts)

	status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": encryptKeyID})
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")

	for _, op := range kmsNamedKeyOperations {
		t.Run(op.name, func(t *testing.T) {
			gotStatus, gotCode, _ := kmsRefusal(t, ts, op.name, op.body(ciphertext, otherKeyID, destKeyID))
			assert.Equal(t, "IncorrectKeyException", gotCode,
				"the wrong %s is reported, not the disabled state of the key the ciphertext names", op.member)
			assert.Equal(t, http.StatusBadRequest, gotStatus, "%s with a wrong %s and a disabled key", op.name, op.member)
		})
	}
}
