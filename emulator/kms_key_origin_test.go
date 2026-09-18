package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #984: CreateKey decodes Origin, CustomKeyStoreId and XksKeyId, and refuses what substrate does not
// model instead of discarding it.
//
// Before this, all three members were accepted and thrown away. `CreateKey` with `Origin: "EXTERNAL"` —
// the first call of every key-import workflow — answered 200 with `Origin: "AWS_KMS"`,
// `KeyState: "Enabled"` and `Enabled: true`, so a consumer testing an import got a fully usable key and
// discovered nothing until GetParametersForImport turned out not to exist either. Five assertions:
//
//  1. **The one origin substrate creates is accepted**, both explicitly and by default, and the key it
//     produces reports that origin. This is the assertion the other four are measured against: refusing
//     everything would satisfy them and break every existing caller.
//  2. **Every other published origin is refused with UnsupportedOperationException**, the code
//     API_CreateKey publishes for a parameter it will not accept. All three are asserted rather than one,
//     because the three fail for two different underlying reasons — imported material and a custom key
//     store — and a check written for one of them could miss the other.
//  3. **An origin outside the published four is refused with a *different* code** — ValidationError.
//     Both are 400, so the code is the only thing that tells a caller a misspelling from a boundary of
//     the emulator, which is the same distinction #977 drew between an unknown key spec and an
//     inadmissible pair. Asserted from both sides in one table so the difference is the subject.
//  4. **The store parameters are refused, and XksKeyId's refusal is AWS's own rather than substrate's.**
//     An XksKeyId with an AWS_KMS origin answers ValidationError, because AWS itself publishes that the
//     member "is not valid for KMS keys with any other Origin value"; an XksKeyId with the one origin
//     AWS accepts it for answers the UnsupportedOperationException about the store. That ordering is the
//     whole content of [kmsResolveKeyOrigin]'s step 2 and is invisible without both cases.
//  5. **A refusal writes nothing.** ListKeys does not grow, so the check runs before the key is minted.
//     Assertions 2 to 4 would all pass with a validation that ran after the write and left a key behind
//     — which is precisely the state #984's own failure mode produced.
//
// Every call goes over the wire and every refusal pairs the code with the status, per #765 and #923.
// The messages are asserted where the assertion is about which of two refusals a caller reads.

// kmsPublishedKeyOrigins is the four origins API_CreateKey publishes, in its order, paired with whether
// substrate creates a key for one.
//
// Transcribed from the page's Valid Values — `AWS_KMS | EXTERNAL | AWS_CLOUDHSM | EXTERNAL_KEY_STORE` —
// rather than read from the implementation's own slice, for the reason
// [kmsSpecAdmissibleUsages] records: a test that shares the structure under test agrees with a wrong
// structure. The `supported` column is substrate's boundary and not AWS's, which is why it is a separate
// column rather than a shorter list.
var kmsPublishedKeyOrigins = []struct {
	origin    string
	supported bool
}{
	{"AWS_KMS", true},
	{"EXTERNAL", false},
	{"AWS_CLOUDHSM", false},
	{"EXTERNAL_KEY_STORE", false},
}

// kmsKeyCount answers how many keys ListKeys reports, for the assertion that a refusal wrote nothing.
func kmsKeyCount(t *testing.T, ts *emulator.TestServer) int {
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

// kmsCreateKeyOrigin creates a key with the given body and returns the Origin its metadata reports.
func kmsCreateKeyOrigin(t *testing.T, ts *emulator.TestServer, body map[string]any) string {
	t.Helper()
	var out struct {
		KeyMetadata struct {
			Origin   string `json:"Origin"`
			KeyState string `json:"KeyState"`
			Enabled  bool   `json:"Enabled"`
		} `json:"KeyMetadata"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "CreateKey", body), &out)
	require.Empty(t, code, "CreateKey %v", body)
	require.Equal(t, http.StatusOK, status, "CreateKey %v", body)
	// Asserted here rather than in one caller because it is what made #984 a defect rather than an
	// omission: the key the caller got back was usable, which is the one thing an import workflow relies
	// on being false.
	assert.Equal(t, "Enabled", out.KeyMetadata.KeyState, "a key substrate creates is usable")
	assert.True(t, out.KeyMetadata.Enabled, "a key substrate creates is enabled")
	return out.KeyMetadata.Origin
}

// TestKMSCreateKey_TheSupportedOriginIsAcceptedExplicitlyAndByDefault is assertion 1.
//
// The default is AWS's — "the default is AWS_KMS, which means that AWS KMS creates the key material" — so
// an absent Origin has to keep working: every other KMS test sends no Origin at all, and a refusal that
// caught the default would have broken all of them rather than being noticed here.
func TestKMSCreateKey_TheSupportedOriginIsAcceptedExplicitlyAndByDefault(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	assert.Equal(t, "AWS_KMS", kmsCreateKeyOrigin(t, ts, map[string]any{}),
		"an absent Origin defaults to AWS_KMS, which is what an empty CreateKey body has always created")
	assert.Equal(t, "AWS_KMS", kmsCreateKeyOrigin(t, ts, map[string]any{"Origin": "AWS_KMS"}),
		"an explicit AWS_KMS is the origin substrate creates and reports")
}

// TestKMSCreateKey_AnUnmodeledOriginIsRefusedWithThePublishedCode is assertion 2.
//
// Each refusal has to name the origin the caller sent and the one substrate supports: a consumer whose
// import workflow stops here needs to learn that this is the emulator's boundary, not that its request is
// malformed, and the code alone cannot say that when both refusals are 400.
func TestKMSCreateKey_AnUnmodeledOriginIsRefusedWithThePublishedCode(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, tc := range kmsPublishedKeyOrigins {
		if tc.supported {
			continue
		}
		t.Run(tc.origin, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "CreateKey", map[string]any{"Origin": tc.origin})
			assert.Equal(t, "UnsupportedOperationException", code,
				"Origin %s is published and substrate does not create it", tc.origin)
			assert.Equal(t, http.StatusBadRequest, status,
				"Origin %s is refused at the status API_CreateKey publishes", tc.origin)
			assert.Contains(t, message, tc.origin, "the refusal names the origin the caller asked for")
			assert.Contains(t, message, "AWS_KMS", "the refusal names the origin substrate does create")
		})
	}
}

// TestKMSCreateKey_AnUnpublishedOriginIsRefusedWithADifferentCode is assertion 3.
//
// The pair in one table is the point: the same operation, the same status, and two codes that answer two
// different questions about whether the caller did anything wrong.
func TestKMSCreateKey_AnUnpublishedOriginIsRefusedWithADifferentCode(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name   string
		origin string
		code   string
	}{
		{"a misspelling of the default", "AWS_KMS_", "ValidationError"},
		{"a plausible misreading of EXTERNAL", "IMPORTED", "ValidationError"},
		{"the right value in the wrong case", "aws_kms", "ValidationError"},
		{"a published origin substrate does not model", "EXTERNAL", "UnsupportedOperationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "CreateKey", map[string]any{"Origin": tc.origin})
			assert.Equal(t, tc.code, code, "Origin %q", tc.origin)
			assert.Equal(t, http.StatusBadRequest, status, "Origin %q", tc.origin)
			assert.Contains(t, message, tc.origin, "the refusal quotes the value the caller sent")
		})
	}
}

// TestKMSCreateKey_TheKeyStoreParametersAreRefused is assertion 4.
//
// XksKeyId appears twice on purpose. With an AWS_KMS origin the refusal is one real KMS would also make,
// so the code is ValidationError; with EXTERNAL_KEY_STORE the request is one real KMS honors, so the
// code is the boundary one. Ordering the XksKeyId check ahead of the origin's own support is what
// produces that pair, and dropping either case would let the two collapse into one code unnoticed.
func TestKMSCreateKey_TheKeyStoreParametersAreRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name    string
		body    map[string]any
		code    string
		message string
	}{
		{
			"a custom key store ID with the default origin",
			map[string]any{"CustomKeyStoreId": "cks-1234567890abcdef0"},
			"UnsupportedOperationException", "cks-1234567890abcdef0",
		},
		{
			"an external key ID with the default origin, which AWS itself refuses",
			map[string]any{"XksKeyId": "bb8562717f809024"},
			"ValidationError", "EXTERNAL_KEY_STORE",
		},
		{
			"an external key ID with the one origin AWS accepts it for",
			map[string]any{"Origin": "EXTERNAL_KEY_STORE", "XksKeyId": "bb8562717f809024"},
			"UnsupportedOperationException", "EXTERNAL_KEY_STORE",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "CreateKey", tc.body)
			assert.Equal(t, tc.code, code, "CreateKey %v", tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "CreateKey %v", tc.body)
			assert.Contains(t, message, tc.message, "the refusal says which condition failed")
		})
	}
}

// TestKMSCreateKey_ARefusedOriginWritesNoKey is assertion 5.
//
// The count is taken before and after so the assertion is about this request rather than about the
// server's initial state, and it goes through ListKeys rather than through the state manager because a
// key a caller can list is the only kind that matters.
func TestKMSCreateKey_ARefusedOriginWritesNoKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	before := kmsKeyCount(t, ts)
	for _, body := range []map[string]any{
		{"Origin": "EXTERNAL"},
		{"Origin": "NONSENSE"},
		{"CustomKeyStoreId": "cks-1234567890abcdef0"},
		{"XksKeyId": "bb8562717f809024"},
	} {
		status, _, _ := kmsRefusal(t, ts, "CreateKey", body)
		require.Equal(t, http.StatusBadRequest, status, "CreateKey %v is refused", body)
	}
	assert.Equal(t, before, kmsKeyCount(t, ts),
		"four refused CreateKey requests mint no key, so the check runs before the write")
}
