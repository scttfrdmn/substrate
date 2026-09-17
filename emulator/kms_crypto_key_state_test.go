package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #961: the five cryptographic KMS operations and the key states they must refuse.
//
// Four things are asserted here, and the order matters because each is a defect the one before it
// would hide:
//
//  1. All five refuse a **disabled** key. Two of them — GenerateDataKeyWithoutPlaintext and ReEncrypt —
//     refused nothing whatever before #961, so a disabled key minted a data key and re-encrypted and
//     answered 200. That is a missing refusal, not a wrong code, and it is the worst of what #961
//     covers.
//  2. All five refuse a key **pending deletion**, with KMSInvalidStateException rather than
//     DisabledException. AWS's key-state cell for these rows reads "[2] or [3]" and admits either code,
//     so this one is substrate's reading; [kmsKeyStateError] carries the argument.
//  3. The **message names the state**. Whichever code is chosen, a caller must be able to tell "enable
//     the key" from "cancel the deletion, then enable the key" — and before #961 neither the code nor
//     the message distinguished them, because both states answered "is not enabled".
//  4. ReEncrypt guards **both** of its keys, and reports the source key's ARN. It loaded only the
//     destination, and its SourceKeyId was the ciphertext blob.
//
// Every call goes over the wire and every assertion pairs the code with the status, per #765 and #923.
// The recovery test at the end is the guard against a refusal that is really a broken key: it walks a
// key back out of pending deletion and shows all five working again.

// kmsCryptoRefusal posts one operation and returns the status, the error code **and the message**.
//
// [decodeAWSResponse] deliberately drops the message — "which is prose", as its own comment says — and
// for every other KMS test that is right. It is not right here: assertion 3 above is *about* the
// message, because the code alone cannot carry two remedies when AWS admits one code for both states.
func kmsCryptoRefusal(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, string, string) {
	t.Helper()

	resp := signedRequest(t, ts, kmsTarget, taggingTestAccount, op, body)
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read the %s response body", op)

	var shape struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(raw, &shape), "decode the %s response %s", op, raw)
	return resp.StatusCode, shape.Type, shape.Message
}

// kmsCryptoOperations are the five operations whose key-state row in the developer guide's table is
// identical: permitted for Enabled, footnote [1] for Disabled, "[2] or [3]" for pending deletion.
//
// The ciphertext is a parameter because two of the five take no KeyId at all — Decrypt's key travels
// inside the ciphertext, and ReEncrypt's source does. Those two are exactly the operations that a table
// keyed only on a key ID would have to leave out, and one of them is the operation #961 found
// unguarded.
var kmsCryptoOperations = []struct {
	name string
	body func(keyID, ciphertext string) map[string]any
}{
	{"Encrypt", func(keyID, _ string) map[string]any {
		return map[string]any{
			"KeyId":     keyID,
			"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
		}
	}},
	{"Decrypt", func(_, ciphertext string) map[string]any {
		return map[string]any{"CiphertextBlob": ciphertext}
	}},
	{"GenerateDataKey", func(keyID, _ string) map[string]any {
		return map[string]any{"KeyId": keyID, "NumberOfBytes": 32}
	}},
	{"GenerateDataKeyWithoutPlaintext", func(keyID, _ string) map[string]any {
		return map[string]any{"KeyId": keyID, "KeySpec": "AES_256"}
	}},
	{"ReEncrypt", func(keyID, ciphertext string) map[string]any {
		return map[string]any{"CiphertextBlob": ciphertext, "DestinationKeyId": keyID}
	}},
}

// kmsCryptoFixture creates a key and mints a ciphertext under it while it still works.
//
// Every test below needs both, and the ciphertext has to be taken first: Decrypt and ReEncrypt cannot
// be driven to a refusal without one, and the key is about to be put into a state that refuses Encrypt.
func kmsCryptoFixture(t *testing.T, ts *emulator.TestServer) (arn, keyID, ciphertext string) {
	t.Helper()

	arn, keyID = createKMSKey(t, ts)
	body := kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
	})
	require.NoError(t, json.Unmarshal(body["CiphertextBlob"], &ciphertext), "decode CiphertextBlob")
	require.NotEmpty(t, ciphertext, "Encrypt reports a ciphertext while the key is enabled")
	return arn, keyID, ciphertext
}

// TestKMSCryptographicOperations_ADisabledKeyIsRefused covers the missing refusal, which is the half of
// #961 that is not a matter of interpretation.
//
// GenerateDataKeyWithoutPlaintext and ReEncrypt had no key check of any kind, so this fails against
// both of them before the fix and passes for the other three, which already answered DisabledException.
func TestKMSCryptographicOperations_ADisabledKeyIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID, ciphertext := kmsCryptoFixture(t, ts)

	status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")

	for _, op := range kmsCryptoOperations {
		t.Run(op.name, func(t *testing.T) {
			gotStatus, gotCode, message := kmsCryptoRefusal(t, ts, op.name, op.body(keyID, ciphertext))
			assert.Equal(t, "DisabledException", gotCode, "%s against a disabled key", op.name)
			assert.Equal(t, http.StatusBadRequest, gotStatus, "%s against a disabled key", op.name)
			assert.Contains(t, message, "not enabled", "%s names why it refused", op.name)
		})
	}
}

// TestKMSCryptographicOperations_AKeyPendingDeletionIsRefused covers the code choice.
//
// All five answered DisabledException here before #961 — three by reaching an enabled-check that cannot
// tell the two states apart, and two by refusing nothing at all. The state is reached through
// ScheduleKeyDeletion rather than by writing it, so the refusal is reached the way a consumer reaches
// it.
func TestKMSCryptographicOperations_AKeyPendingDeletionIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID, ciphertext := kmsCryptoFixture(t, ts)

	kmsScheduleDeletion(t, ts, keyID, 0)

	for _, op := range kmsCryptoOperations {
		t.Run(op.name, func(t *testing.T) {
			gotStatus, gotCode, message := kmsCryptoRefusal(t, ts, op.name, op.body(keyID, ciphertext))
			assert.Equal(t, "KMSInvalidStateException", gotCode, "%s against a key pending deletion", op.name)
			assert.Equal(t, http.StatusBadRequest, gotStatus, "%s against a key pending deletion", op.name)
			assert.Contains(t, message, "PendingDeletion", "%s names the state it refused", op.name)
		})
	}
}

// TestKMSCryptographicOperations_TheTwoStatesAreDistinguishable is the assertion the issue was really
// about, and it is deliberately not folded into either test above.
//
// A caller's two remedies differ — EnableKey for a disabled key, CancelKeyDeletion *then* EnableKey for
// a key pending deletion, which #963 made a genuine two-step recovery. Before #961 the two states
// produced one code and one message, so a consumer's error handler had nothing to branch on. This
// compares the two answers from one operation directly, so it fails if a later change collapses them
// again by either route: the same code, or two codes with one message.
func TestKMSCryptographicOperations_TheTwoStatesAreDistinguishable(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	// Two keys rather than two servers: one key cannot be disabled and pending deletion at once, and
	// comparing across servers would leave the two answers free to differ for a reason other than state.
	_, disabledKeyID := createKMSKey(t, ts)
	_, pendingKeyID := createKMSKey(t, ts)

	status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": disabledKeyID})
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")
	kmsScheduleDeletion(t, ts, pendingKeyID, 0)

	plaintext := base64.StdEncoding.EncodeToString([]byte("secret"))
	_, disabledCode, disabledMessage := kmsCryptoRefusal(t, ts, "Encrypt",
		map[string]any{"KeyId": disabledKeyID, "Plaintext": plaintext})
	_, pendingCode, pendingMessage := kmsCryptoRefusal(t, ts, "Encrypt",
		map[string]any{"KeyId": pendingKeyID, "Plaintext": plaintext})

	assert.NotEqual(t, disabledCode, pendingCode,
		"the two key states answer different codes, so an error handler can branch")
	assert.NotEqual(t, disabledMessage, pendingMessage,
		"the two key states answer different messages, so a caller reading a log can tell them apart")
	assert.NotContains(t, disabledMessage, "PendingDeletion",
		"a disabled key is not reported as pending deletion")
}

// TestKMSReEncrypt_TheSourceKeyIsGuardedToo covers the key ReEncrypt never loaded.
//
// The destination is left enabled throughout, so nothing but the source can be refusing — which is what
// makes this mean the thing #961 is about rather than catching the destination guard on the way past.
// Both bad states are exercised, because they come from different arms of [kmsKeyStateError].
func TestKMSReEncrypt_TheSourceKeyIsGuardedToo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		disable func(t *testing.T, ts *emulator.TestServer, keyID string)
		code    string
	}{
		{"a disabled source key", func(t *testing.T, ts *emulator.TestServer, keyID string) {
			status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": keyID})
			require.Empty(t, code, "DisableKey")
			require.Equal(t, http.StatusOK, status, "DisableKey")
		}, "DisabledException"},
		{"a source key pending deletion", func(t *testing.T, ts *emulator.TestServer, keyID string) {
			kmsScheduleDeletion(t, ts, keyID, 0)
		}, "KMSInvalidStateException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)
			_, sourceKeyID, ciphertext := kmsCryptoFixture(t, ts)
			_, destKeyID := createKMSKey(t, ts)

			tc.disable(t, ts, sourceKeyID)

			status, code, _ := kmsCryptoRefusal(t, ts, "ReEncrypt", map[string]any{
				"CiphertextBlob":   ciphertext,
				"DestinationKeyId": destKeyID,
			})
			assert.Equal(t, tc.code, code, "ReEncrypt from %s to an enabled key", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "ReEncrypt from %s", tc.name)
		})
	}
}

// TestKMSReEncrypt_TheDestinationKeyIsStillGuarded is the other half, and it exists so the test above
// cannot pass by guarding the source and dropping the destination.
//
// The source is enabled and the destination is not, which is the case AWS's split permissions
// (kms:ReEncryptFrom, kms:ReEncryptTo) make two separate refusals.
func TestKMSReEncrypt_TheDestinationKeyIsStillGuarded(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, sourceKeyID, ciphertext := kmsCryptoFixture(t, ts)
	_, destKeyID := createKMSKey(t, ts)

	kmsScheduleDeletion(t, ts, destKeyID, 0)

	status, code, _ := kmsCryptoRefusal(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":   ciphertext,
		"DestinationKeyId": destKeyID,
	})
	assert.Equal(t, "KMSInvalidStateException", code, "ReEncrypt to a key pending deletion")
	assert.Equal(t, http.StatusBadRequest, status, "ReEncrypt to a key pending deletion")

	// The source is untouched by the refusal, which is what makes the two guards independent rather
	// than one guard reached twice.
	body := kmsRawBody(t, ts, "Encrypt", map[string]any{
		"KeyId":     sourceKeyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
	})
	assert.NotEmpty(t, body["CiphertextBlob"], "the source key still encrypts")
}

// TestKMSReEncrypt_TheSourceIsCheckedBeforeTheDestination pins the order, which AWS does not publish
// and substrate therefore decides.
//
// The choice follows the operation's own description — "Decrypts ciphertext and then reencrypts it" —
// and the assertion is written so that either order is *visible* rather than silently changing: with a
// bad source and an absent destination there are two candidate answers, and only one of them can be
// right.
func TestKMSReEncrypt_TheSourceIsCheckedBeforeTheDestination(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, sourceKeyID, ciphertext := kmsCryptoFixture(t, ts)

	kmsScheduleDeletion(t, ts, sourceKeyID, 0)

	status, code, _ := kmsCryptoRefusal(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":   ciphertext,
		"DestinationKeyId": kmsAbsentKeyID,
	})
	assert.Equal(t, "KMSInvalidStateException", code,
		"the source key's state is reported, not the destination's absence")
	assert.Equal(t, http.StatusBadRequest, status, "ReEncrypt with a bad source and an absent destination")
}

// TestKMSReEncrypt_ReportsTheSourceKeyARN covers the value that was not an identifier of anything.
//
// SourceKeyId held the **ciphertext blob** before #961, where API_ReEncrypt's sample renders a key ARN
// and the element is glossed "unique identifier of the KMS key used to originally encrypt the data".
// The assertion is not that the field equals a string this test built: it is that the value substrate
// reports is one the owning service will accept back, which is #765's rule applied to a response member
// rather than to a tag. A blob fails that, and so would a bare key ID where the sample shows an ARN.
func TestKMSReEncrypt_ReportsTheSourceKeyARN(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	sourceARN, _, ciphertext := kmsCryptoFixture(t, ts)
	destARN, destKeyID := createKMSKey(t, ts)

	body := kmsRawBody(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":   ciphertext,
		"DestinationKeyId": destKeyID,
	})

	var gotSource, gotKeyID string
	require.NoError(t, json.Unmarshal(body["SourceKeyId"], &gotSource), "decode SourceKeyId")
	require.NoError(t, json.Unmarshal(body["KeyId"], &gotKeyID), "decode KeyId")

	assert.Equal(t, sourceARN, gotSource, "SourceKeyId is the source key's ARN")
	assert.Equal(t, destARN, gotKeyID, "KeyId is the destination key's ARN")
	assert.NotEqual(t, ciphertext, gotSource, "SourceKeyId is not the ciphertext blob")
	require.True(t, strings.HasPrefix(gotSource, "arn:aws:kms:"), "SourceKeyId is an ARN: %s", gotSource)

	// The round trip is the point: a caller can take the reported SourceKeyId straight back to
	// DescribeKey. The blob could not, and a bare key ID would not match the published sample.
	metadata := kmsKeyMetadata(t, ts, gotSource)
	var describedARN string
	require.NoError(t, json.Unmarshal(metadata["Arn"], &describedARN), "decode Arn")
	assert.Equal(t, sourceARN, describedARN, "DescribeKey resolves the reported SourceKeyId")
}

// TestKMSCryptographicOperations_TheRecoveryPathRestoresAllFive is the guard against a refusal that is
// really a broken key.
//
// A table of refusals cannot see the difference between "this state is refused" and "this key stopped
// working", and two of the five operations here gained their first guard in #961 — so a guard written
// with an inverted condition would satisfy every assertion above. The walk back is the two-step recovery
// #963 established, and all five must work at the end of it.
func TestKMSCryptographicOperations_TheRecoveryPathRestoresAllFive(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)
	_, keyID, ciphertext := kmsCryptoFixture(t, ts)

	kmsScheduleDeletion(t, ts, keyID, 0)

	// CancelKeyDeletion alone leaves the key Disabled, so the five are still refused here — a single
	// step is not a recovery.
	status, code := kmsCall(t, ts, "CancelKeyDeletion", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "CancelKeyDeletion")
	require.Equal(t, http.StatusOK, status, "CancelKeyDeletion")

	_, midCode, _ := kmsCryptoRefusal(t, ts, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
	})
	require.Equal(t, "DisabledException", midCode,
		"canceling a deletion leaves the key disabled, per API_CancelKeyDeletion")

	status, code = kmsCall(t, ts, "EnableKey", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "EnableKey")
	require.Equal(t, http.StatusOK, status, "EnableKey")

	for _, op := range kmsCryptoOperations {
		t.Run(op.name, func(t *testing.T) {
			gotStatus, gotCode := kmsCall(t, ts, op.name, op.body(keyID, ciphertext))
			assert.Empty(t, gotCode, "%s after the key is recovered", op.name)
			assert.Equal(t, http.StatusOK, gotStatus, "%s after the key is recovered", op.name)
		})
	}
}
