package emulator_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #923: three KMS error codes carried a status KMS does not publish, and one of the three was not a
// KMS error code at all.
//
// Everything here goes over the wire and asserts the **status alongside the code**, because that is
// the whole of the issue and a decoded error struct carries only the code. That is why a green suite
// held a 404 for as long as it did: kms_plugin_test.go asserts codes, and the two assertions in
// tagging_kms_test.go that did check a status were written against the new resolver's own refusals,
// which were already 400.
//
// The finding underneath all of it, from the reference pages themselves: KMS publishes exactly two
// statuses — 500 for DependencyTimeoutException, KMSInternalException and KeyUnavailableException,
// and 400 for everything else. There is no 404 for any resource on any operation. So these tests
// assert http.StatusBadRequest throughout, and a future site answering anything else is the defect.

// kmsRawCall posts a byte-for-byte request body to a KMS operation, for the one case a marshaled
// map cannot express: a body that is not JSON.
//
// [signedRequest] takes a Go value and marshals it, so it can only ever produce valid JSON — which
// is exactly the input the twenty-one guards under test do *not* refuse. The signature has to cover
// the raw bytes, so the header is computed here rather than reused.
func kmsRawCall(t *testing.T, ts *emulator.TestServer, op string, body []byte) (int, string) {
	t.Helper()

	creds, ok := ts.CredentialsFor(taggingTestAccount)
	require.True(t, ok, "a credential for %s", taggingTestAccount)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(body))
	require.NoError(t, err, "build the %s request", op)
	req.Host = kmsTarget.host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", kmsTarget.target+"."+op)
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(
		http.MethodPost, "/", kmsTarget.host, kmsTarget.signingName, "us-east-1", sigV4TestDateTime,
		body, creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "post %s", op)
	return decodeAWSResponse(t, resp, nil)
}

// kmsCall posts one KMS operation and returns the status and error code.
func kmsCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, string) {
	t.Helper()
	return decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, op, body), nil)
}

// kmsStubCiphertext builds the ciphertext substrate's stub cipher produces for a key, so a Decrypt
// or ReEncrypt test can name a key that does not exist.
//
// Those two operations are the only ones whose key does not come from a KeyId parameter — it comes
// out of the ciphertext — so they cannot be driven to the not-found path through a request member.
// The format is the stub's own (kms_ciphertext.go); a test that could not build it would have to skip the
// two operations, which are two of the fifteen sites the issue is about.
//
// SYMMETRIC_DEFAULT and no encryption context, which is what a Decrypt sending neither member matches.
// A test that needs either recorded reaches for [kmsStubCiphertextWith].
func kmsStubCiphertext(keyID string, plaintext []byte) string {
	return kmsStubCiphertextWith(keyID, "SYMMETRIC_DEFAULT", nil, plaintext)
}

// kmsStubCiphertextWith builds a stub ciphertext recording a chosen algorithm and encryption context.
//
// It exists because #979 made the blob's contents load-bearing: the algorithm and the context are what
// Decrypt and ReEncrypt now *compare* a request against, so a test for either refusal has to be able to
// write a blob that disagrees with the request it then sends. The envelope is duplicated from
// kmsEncryptStub rather than shared, which is the point — a test asserting a wire behavior must not be
// able to pass because it and the code agree on a mistake.
//
// The context is omitted when empty so that the blob is byte-identical to one written with no context at
// all, matching what kmsEncryptStub's omitempty tag produces.
func kmsStubCiphertextWith(keyID, algorithm string, encryptionContext map[string]string, plaintext []byte) string {
	envelope := map[string]any{
		"format":    "substrate-kms-v2",
		"keyId":     keyID,
		"algorithm": algorithm,
		"plaintext": plaintext,
	}
	if len(encryptionContext) > 0 {
		envelope["encryptionContext"] = encryptionContext
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		panic("kms stub ciphertext: " + err.Error())
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// kmsAbsentKeyID is a well-formed key identifier no CreateKey minted.
//
// It is a bare identifier rather than an ARN so that resolution succeeds and the *lookup* is what
// fails: an ARN would risk the refusal coming from the parser instead, which would leave the
// not-found sites untested. It is shaped like a real key ID for the same reason.
const kmsAbsentKeyID = "00000000-0000-0000-0000-000000000000"

// kmsKeyIDOperations are the operations that take a KeyId and answer NotFoundException when it names
// no key. Every one publishes NotFoundException at HTTP 400 on its own reference page.
//
// Decrypt and ReEncrypt are absent because their key comes from the ciphertext rather than from a
// member; they have their own test. GetKeyPolicy and PutKeyPolicy are absent because substrate does
// not check the key exists on either — a separate gap, not a status.
var kmsKeyIDOperations = []struct {
	name string
	body func(keyID string) map[string]any
}{
	{"DescribeKey", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"EnableKey", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"DisableKey", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"ScheduleKeyDeletion", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"CancelKeyDeletion", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"GetKeyRotationStatus", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"EnableKeyRotation", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"DisableKeyRotation", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"TagResource", func(k string) map[string]any {
		return map[string]any{"KeyId": k, "Tags": []map[string]string{{"TagKey": "env", "TagValue": "prod"}}}
	}},
	{"UntagResource", func(k string) map[string]any {
		return map[string]any{"KeyId": k, "TagKeys": []string{"env"}}
	}},
	{"ListResourceTags", func(k string) map[string]any { return map[string]any{"KeyId": k} }},
	{"Encrypt", func(k string) map[string]any {
		return map[string]any{"KeyId": k, "Plaintext": base64.StdEncoding.EncodeToString([]byte("secret"))}
	}},
	{"GenerateDataKey", func(k string) map[string]any {
		return map[string]any{"KeyId": k, "NumberOfBytes": 32}
	}},
	{"GenerateDataKeyWithoutPlaintext", func(k string) map[string]any {
		return map[string]any{"KeyId": k, "KeySpec": "AES_256"}
	}},
}

// TestKMSErrorStatus_AnAbsentKeyIsReportedAt400 pins the status of the not-found answer on every
// operation that takes a KeyId and can give it.
//
// The identifier is well formed and in the caller's own account and Region, so nothing but the
// missing record can be refusing it — which is what makes the assertion mean the thing the issue is
// about rather than catching a resolver refusal on the way past.
func TestKMSErrorStatus_AnAbsentKeyIsReportedAt400(t *testing.T) {
	ts := arnGuardServer(t)

	for _, op := range kmsKeyIDOperations {
		t.Run(op.name, func(t *testing.T) {
			status, code := kmsCall(t, ts, op.name, op.body(kmsAbsentKeyID))
			assert.Equal(t, "NotFoundException", code, "%s on an absent key", op.name)
			assert.Equal(t, http.StatusBadRequest, status, "%s on an absent key", op.name)
		})
	}
}

// TestKMSErrorStatus_TheTwoCiphertextOperationsReportAnAbsentKeyAt400 covers the two of the fifteen
// sites a KeyId member cannot reach.
//
// Decrypt takes no key selector at all — the key travels in the ciphertext — and ReEncrypt's
// destination is only consulted after the ciphertext decodes, so both need a ciphertext built for a
// key that was never created. ReEncrypt's site carries its own message ("Destination key not
// found"), which is why it is asserted separately rather than folded into the table above.
func TestKMSErrorStatus_TheTwoCiphertextOperationsReportAnAbsentKeyAt400(t *testing.T) {
	ts := arnGuardServer(t)
	orphan := kmsStubCiphertext(kmsAbsentKeyID, []byte("secret"))

	status, code := kmsCall(t, ts, "Decrypt", map[string]any{"CiphertextBlob": orphan})
	assert.Equal(t, "NotFoundException", code, "Decrypt of a ciphertext naming no key")
	assert.Equal(t, http.StatusBadRequest, status, "Decrypt of a ciphertext naming no key")

	// ReEncrypt's ciphertext must decode, so it names a real key; the destination is the absent one.
	_, keyID := createKMSKey(t, ts)
	status, code = kmsCall(t, ts, "ReEncrypt", map[string]any{
		"CiphertextBlob":   kmsStubCiphertext(keyID, []byte("secret")),
		"DestinationKeyId": kmsAbsentKeyID,
	})
	assert.Equal(t, "NotFoundException", code, "ReEncrypt to an absent destination key")
	assert.Equal(t, http.StatusBadRequest, status, "ReEncrypt to an absent destination key")
}

// TestKMSErrorStatus_AnAbsentAliasIsReportedAt400 covers the sixteenth not-found site, which is
// reached through a KeyId rather than through an alias operation.
//
// Neither DeleteAlias nor UpdateAlias consults the alias pointer — the first deletes whatever is
// there and the second overwrites it — so the only way to the followAlias refusal is a KeyId given
// as an alias. Both accepted forms are exercised, because an alias ARN and an alias name reach it
// down different arms of the resolver.
func TestKMSErrorStatus_AnAbsentAliasIsReportedAt400(t *testing.T) {
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name  string
		keyID string
	}{
		{"an alias name", "alias/no-such-alias"},
		{"an alias ARN", "arn:aws:kms:us-east-1:" + taggingTestAccount + ":alias/no-such-alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := kmsCall(t, ts, "DescribeKey", map[string]any{"KeyId": tc.keyID})
			assert.Equal(t, "NotFoundException", code, "DescribeKey through %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "DescribeKey through %s", tc.name)
		})
	}
}

// TestKMSErrorStatus_ADisabledKeyIsRefusedAt400 pins DisabledException, which answered 409.
//
// The three operations here are the three that check `Enabled`, and each publishes DisabledException
// at 400 on its own page — "The request was rejected because the specified KMS key is not enabled".
// The key is disabled through DisableKey rather than by writing state, so the refusal is reached the
// way a consumer reaches it.
func TestKMSErrorStatus_ADisabledKeyIsRefusedAt400(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	// A ciphertext for Decrypt has to be minted while the key still works.
	var encrypted struct {
		CiphertextBlob string `json:"CiphertextBlob"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "Encrypt", map[string]any{
			"KeyId":     keyID,
			"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
		}), &encrypted)
	require.Empty(t, code, "Encrypt while the key is enabled")
	require.Equal(t, http.StatusOK, status, "Encrypt while the key is enabled")
	require.NotEmpty(t, encrypted.CiphertextBlob, "Encrypt reports a ciphertext")

	status, code = kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "DisableKey")
	require.Equal(t, http.StatusOK, status, "DisableKey")

	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"Encrypt", map[string]any{
			"KeyId":     keyID,
			"Plaintext": base64.StdEncoding.EncodeToString([]byte("secret")),
		}},
		{"Decrypt", map[string]any{"CiphertextBlob": encrypted.CiphertextBlob}},
		{"GenerateDataKey", map[string]any{"KeyId": keyID, "NumberOfBytes": 32}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			gotStatus, gotCode := kmsCall(t, ts, tc.op, tc.body)
			assert.Equal(t, "DisabledException", gotCode, "%s against a disabled key", tc.op)
			assert.Equal(t, http.StatusBadRequest, gotStatus, "%s against a disabled key", tc.op)
		})
	}
}

// TestKMSErrorStatus_AnUnparseableBodyIsAValidationError covers the twenty-one guards that answered
// InvalidRequest — a code that appears nowhere in KMS's documentation, so a caller matching on it
// matched something no SDK models.
//
// ValidationError is a common error rather than an operation error, which is what these sites need:
// a body that will not parse belongs to no single operation, and TagException — the alternative — is
// glossed "one or more tags are not valid" and would be wrong on the eighteen of these that take no
// tags.
//
// The body is sent raw, since a marshaled map cannot be invalid JSON. CreateKey, ListKeys and
// ListAliases are absent from the list because all three treat the body as optional and ignore a
// parse failure, which is a different decision and not one this issue disturbs.
func TestKMSErrorStatus_AnUnparseableBodyIsAValidationError(t *testing.T) {
	ts := arnGuardServer(t)

	for _, op := range []string{
		"DescribeKey", "EnableKey", "DisableKey", "ScheduleKeyDeletion", "CancelKeyDeletion",
		"GetKeyPolicy", "PutKeyPolicy", "GetKeyRotationStatus", "EnableKeyRotation",
		"DisableKeyRotation", "TagResource", "UntagResource", "ListResourceTags", "CreateAlias",
		"DeleteAlias", "UpdateAlias", "Encrypt", "Decrypt", "GenerateDataKey",
		"GenerateDataKeyWithoutPlaintext", "ReEncrypt",
	} {
		t.Run(op, func(t *testing.T) {
			status, code := kmsRawCall(t, ts, op, []byte(`{"KeyId": `))
			assert.Equal(t, "ValidationError", code, "%s with a truncated body", op)
			assert.Equal(t, http.StatusBadRequest, status, "%s with a truncated body", op)
		})
	}
}

// TestKMSErrorStatus_TheNominalPathsStillSucceed is the guard against the refusals above being
// reached by something other than what they name, which a table of refusals cannot see.
//
// Every operation whose status changed is exercised against a key CreateKey minted, in an order that
// leaves the deletion last. Without this, a resolver that refused every identifier would satisfy
// each assertion above.
//
// The tail sequence is the lifecycle, not an arbitrary order: DisableKey, then ScheduleKeyDeletion,
// then CancelKeyDeletion, each of which needs the state the one before it left. #963 moved
// CancelKeyDeletion out of the loop and into that tail, because it now requires a key pending deletion
// and no longer succeeds against an enabled one — the whole point of that change being that it is not
// EnableKey.
func TestKMSErrorStatus_TheNominalPathsStillSucceed(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	for _, op := range kmsKeyIDOperations {
		switch op.name {
		case "DisableKey", "ScheduleKeyDeletion", "CancelKeyDeletion":
			continue // each needs the state the one before it leaves; run below, in order.
		}
		status, code := kmsCall(t, ts, op.name, op.body(keyID))
		assert.Empty(t, code, "%s on a key that exists", op.name)
		assert.Equal(t, http.StatusOK, status, "%s on a key that exists", op.name)
	}

	// Last, because they take the key out of service — and in this order, because each is the
	// precondition of the next.
	for _, op := range []string{"DisableKey", "ScheduleKeyDeletion", "CancelKeyDeletion"} {
		status, code := kmsCall(t, ts, op, map[string]any{"KeyId": keyID})
		assert.Empty(t, code, "%s on a key that exists", op)
		assert.Equal(t, http.StatusOK, status, "%s on a key that exists", op)
	}
}
