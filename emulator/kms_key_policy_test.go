package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #983: CreateKey's Policy is recorded rather than dropped, GetKeyPolicy answers AWS's default document
// rather than one that grants nothing, and both policy operations refuse what their pages publish.
//
// Two defects with one cause. `createKey` decoded no Policy member, so a caller that attached a policy at
// creation time read back a document it had never sent; and `getKeyPolicy` fell back to
// `{"Version":"2012-10-17","Statement":[]}` for any key that had never had PutKeyPolicy called on it. The
// second is the one that matters beyond fidelity: a key policy is the only place a KMS key's own
// permissions live, so an empty-statement default is not a placeholder — it says the opposite of AWS's
// default, which grants the key's own account full control.
//
// Eight assertions:
//
//  1. **A key created with no Policy reports AWS's default document**, decoded member by member against
//     API_GetKeyPolicy's own Example Response — including the `Id` member the developer-guide prose omits —
//     with the root principal naming the key's own account rather than AWS's documentation placeholder.
//  2. **A Policy supplied to CreateKey is what GetKeyPolicy answers**, byte for byte, and the default is
//     not substituted for it. This is the round trip the issue is named for.
//  3. **The published 1-32768 range is enforced on both operations** with LimitExceededException/400 — the
//     code CreateKey's own Policy note names for this condition, rather than the malformed-document one an
//     oversize document might otherwise attract.
//  4. **A document that is not a JSON object is MalformedPolicyDocumentException/400 on both**, for the two
//     conditions substrate checks and no more: AWS's own page says a statement missing Action or Resource
//     still succeeds, so there is no statement-level refusal to assert.
//  5. **A PolicyName other than `default` is NotFoundException/400 on both**, and an absent or empty one is
//     the default rather than a refusal.
//  6. **Both operations now answer NotFoundException for a key that does not exist**, and the document is
//     checked first: a request that is wrong about both hears about the member it can fix on its own.
//     (The status half of this is in `kms_error_status_test.go`'s table, which #983 extended.)
//  7. **Neither operation refuses a key for its state**, including PendingDeletion. That is not an
//     omission: the developer guide's key-state table gives both a checkmark in all seven columns, so
//     KMSInvalidStateException is published-but-unreachable here, and this pins it against a later sweep
//     that "completes" the key-state checks.
//  8. **A refused PutKeyPolicy leaves the stored policy alone**, which is the observable form of the
//     document being validated before anything is written.
//
// Every call goes over the wire (#765) and every refusal pairs the code with the status (#923).

// kmsMinimalPolicy is the shortest document that satisfies every check substrate makes: in range, valid
// JSON, and an object.
//
// It is deliberately *not* a useful key policy — no statement, no principal — because the checks under test
// are the ones AWS publishes, and AWS publishes none about a statement's contents. Using a realistic
// document here would suggest the realism was load-bearing.
const kmsMinimalPolicy = `{"Version":"2012-10-17","Statement":[]}`

// kmsAccountPolicy is a document a caller would plausibly attach, used for the round trip.
//
// Distinct from the default in more than one member — a different Id, a different Sid and an extra Action
// — so that assertion 2 cannot pass against a response that quietly substituted the default.
const kmsAccountPolicy = `{"Version":"2012-10-17","Id":"caller-supplied-1","Statement":` +
	`[{"Sid":"Callers own statement","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},` +
	`"Action":["kms:Encrypt","kms:Decrypt"],"Resource":"*"}]}`

// kmsPolicyOf renders a valid JSON object of exactly n bytes, for the length assertions.
//
// Valid rather than arbitrary padding, so that a refusal proves the *size* was what refused it: a
// deliberately malformed 32769-byte string would be refused by either check and could not tell the two
// apart. The padding lands in an Id member, which is a real member of a policy document.
//
// n must exceed the nine bytes of scaffolding; every caller passes a value near 32768, so no guard is
// added for a case a test would have to be rewritten to reach.
func kmsPolicyOf(n int) string {
	const prefix = `{"Id":"`
	const suffix = `"}`
	return prefix + strings.Repeat("p", n-len(prefix)-len(suffix)) + suffix
}

// kmsGetPolicy reads a key's policy through GetKeyPolicy, requiring success, and returns the Policy and
// PolicyName members.
func kmsGetPolicy(t *testing.T, ts *emulator.TestServer, keyID string) (policy, policyName string) {
	t.Helper()
	body := kmsRawBody(t, ts, "GetKeyPolicy", map[string]any{"KeyId": keyID})
	require.Contains(t, body, "Policy", "GetKeyPolicy publishes Policy")
	require.Contains(t, body, "PolicyName", "and PolicyName")
	require.NoError(t, json.Unmarshal(body["Policy"], &policy))
	require.NoError(t, json.Unmarshal(body["PolicyName"], &policyName))
	return policy, policyName
}

// createKMSKeyWithPolicy creates a key with a Policy member and returns its key ID.
func createKMSKeyWithPolicy(t *testing.T, ts *emulator.TestServer, policy string) string {
	t.Helper()
	body := kmsRawBody(t, ts, "CreateKey", map[string]any{"Policy": policy})
	var meta struct {
		KeyID string `json:"KeyId"`
	}
	require.NoError(t, json.Unmarshal(body["KeyMetadata"], &meta))
	require.NotEmpty(t, meta.KeyID, "CreateKey reports a key ID")
	return meta.KeyID
}

// TestKMSKeyPolicy_ANewKeyReportsTheDefaultDocument is assertion 1.
//
// Every member is decoded and compared rather than the whole string, for two reasons: AWS formats the
// document with spaces around its colons and substrate does not, so a string comparison would assert
// transport; and the members are what a consumer reads, so naming them individually is what makes a
// missing one fail here rather than in whatever reads the policy next.
func TestKMSKeyPolicy_ANewKeyReportsTheDefaultDocument(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	policy, policyName := kmsGetPolicy(t, ts, keyID)
	assert.Equal(t, "default", policyName, "the only policy name AWS publishes")

	var document struct {
		Version   string `json:"Version"`
		ID        string `json:"Id"`
		Statement []struct {
			Sid       string `json:"Sid"`
			Effect    string `json:"Effect"`
			Principal struct {
				AWS string `json:"AWS"`
			} `json:"Principal"`
			Action   string `json:"Action"`
			Resource string `json:"Resource"`
		} `json:"Statement"`
	}
	require.NoError(t, json.Unmarshal([]byte(policy), &document), "the default is a JSON object: %s", policy)

	assert.Equal(t, "2012-10-17", document.Version, "the policy language version the example carries")
	assert.Equal(t, "key-default-1", document.ID,
		"the Id member API_GetKeyPolicy's example carries and the developer-guide prose does not mention")
	require.Len(t, document.Statement, 1, "one statement, where the old stand-in had none: %s", policy)
	assert.Equal(t, "Enable IAM User Permissions", document.Statement[0].Sid)
	assert.Equal(t, "Allow", document.Statement[0].Effect)
	assert.Equal(t, "arn:aws:iam::"+taggingTestAccount+":root", document.Statement[0].Principal.AWS,
		"the root of the key's own account, not AWS's documentation placeholder 111122223333")
	assert.Equal(t, "kms:*", document.Statement[0].Action, "full control, which is what delegates to IAM")
	assert.Equal(t, "*", document.Statement[0].Resource)
}

// TestKMSKeyPolicy_ASuppliedPolicyIsWhatIsReadBack is assertion 2.
//
// Byte for byte, because the member is a document a caller composed and substrate is not entitled to
// reformat it — the same reading that keeps the stored value out of [kmsDefaultKeyPolicy]'s hands. The
// default's own Id is asserted absent, which is what would fail if the fallback fired for a key that has a
// policy.
func TestKMSKeyPolicy_ASuppliedPolicyIsWhatIsReadBack(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	keyID := createKMSKeyWithPolicy(t, ts, kmsAccountPolicy)

	policy, _ := kmsGetPolicy(t, ts, keyID)
	assert.Equal(t, kmsAccountPolicy, policy, "the document the caller attached, unchanged")
	assert.NotContains(t, policy, "key-default-1", "the default was not substituted for it")

	// And PutKeyPolicy replaces it, which is the operation the stored value has always come from.
	kmsRawBody(t, ts, "PutKeyPolicy", map[string]any{"KeyId": keyID, "Policy": kmsMinimalPolicy})
	policy, _ = kmsGetPolicy(t, ts, keyID)
	assert.Equal(t, kmsMinimalPolicy, policy, "the later document wins, and neither is merged with the other")
}

// TestKMSKeyPolicy_ThePublishedLengthRangeIsEnforced is assertion 3.
//
// LimitExceededException rather than MalformedPolicyDocumentException, which is the whole point of the
// assertion: an oversize document is a plausible candidate for either, and CreateKey's Policy member settles
// it in one sentence — "if the key policy exceeds the length constraint, AWS KMS returns a
// LimitExceededException". Both ends are walked on both operations, and the minimum is the load-bearing one
// on PutKeyPolicy, where Policy is Required: Yes and a present-but-empty member satisfies presence.
func TestKMSKeyPolicy_ThePublishedLengthRangeIsEnforced(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	for _, tc := range []struct {
		name   string
		policy string
	}{
		{"empty, which Required: Yes does not catch", ""},
		{"one byte over the published maximum", kmsPolicyOf(32769)},
	} {
		t.Run("PutKeyPolicy/"+tc.name, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, "PutKeyPolicy",
				map[string]any{"KeyId": keyID, "Policy": tc.policy})
			assert.Equal(t, "LimitExceededException", code,
				"the code CreateKey's Policy member names for a length violation")
			assert.Equal(t, http.StatusBadRequest, status, "published at 400 on both pages")
			assert.Contains(t, message, "32768", "the message names the limit, not only the size sent")
		})

		t.Run("CreateKey/"+tc.name, func(t *testing.T) {
			status, code, _ := kmsRefusal(t, ts, "CreateKey", map[string]any{"Policy": tc.policy})
			assert.Equal(t, "LimitExceededException", code, "the same reading at the other site")
			assert.Equal(t, http.StatusBadRequest, status, "published at 400 on both pages")
		})
	}

	// Exactly the maximum is accepted, which is what a `<` where `<=` belongs would fail and no oversize
	// case can see.
	kmsRawBody(t, ts, "PutKeyPolicy", map[string]any{"KeyId": keyID, "Policy": kmsPolicyOf(32768)})
}

// TestKMSKeyPolicy_AMalformedDocumentIsRefused is assertion 4.
//
// Three shapes, covering both conditions substrate checks: a value that is not JSON at all, and two that are
// valid JSON whose top-level value is not an object. The second pair is the interesting one — `[]` is what a
// caller sends when it confuses the document with its Statement array, and `json.Unmarshal` into a map is
// the only thing standing between that and a stored non-document.
func TestKMSKeyPolicy_AMalformedDocumentIsRefused(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	for _, tc := range []struct {
		name   string
		policy string
	}{
		{"not JSON at all", "{not a policy"},
		{"a JSON array, which is the Statement member rather than the document", `[{"Effect":"Allow"}]`},
		{"a JSON string", `"AdministratorAccess"`},
	} {
		t.Run("PutKeyPolicy/"+tc.name, func(t *testing.T) {
			status, code, _ := kmsRefusal(t, ts, "PutKeyPolicy",
				map[string]any{"KeyId": keyID, "Policy": tc.policy})
			assert.Equal(t, "MalformedPolicyDocumentException", code, "published on both pages at 400")
			assert.Equal(t, http.StatusBadRequest, status, "published on both pages at 400")
		})

		t.Run("CreateKey/"+tc.name, func(t *testing.T) {
			status, code, _ := kmsRefusal(t, ts, "CreateKey", map[string]any{"Policy": tc.policy})
			assert.Equal(t, "MalformedPolicyDocumentException", code, "the same reading at the other site")
			assert.Equal(t, http.StatusBadRequest, status, "published on both pages at 400")
		})
	}

	// A statement missing Action and Resource is accepted, which is not laxity: AWS's own Policy description
	// says such a statement "has no effect" while "the CreateKey and PutKeyPolicy API requests succeed".
	kmsRawBody(t, ts, "PutKeyPolicy", map[string]any{
		"KeyId":  keyID,
		"Policy": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"}}]}`,
	})
}

// TestKMSKeyPolicy_OnlyTheDefaultPolicyNameExists is assertion 5.
//
// NotFoundException is a one-step reading rather than a transcription — neither page publishes a code *for*
// PolicyName — so it is pinned here and argued at [kmsUnknownPolicyName]. The accepted half matters as much:
// PolicyName is Required: No with a documented default, so omitting it and sending "" both name the one
// policy that exists, which is a different reading from Plaintext's published minimum length.
func TestKMSKeyPolicy_OnlyTheDefaultPolicyNameExists(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)

	for _, op := range []string{"GetKeyPolicy", "PutKeyPolicy"} {
		t.Run(op, func(t *testing.T) {
			status, code, message := kmsRefusal(t, ts, op, map[string]any{
				"KeyId":      keyID,
				"PolicyName": "administrator",
				"Policy":     kmsMinimalPolicy,
			})
			assert.Equal(t, "NotFoundException", code,
				"a name that names no policy is the entity this code is glossed for")
			assert.Equal(t, http.StatusBadRequest, status, "every KMS refusal is a 400")
			assert.Contains(t, message, "default",
				"the message names the one valid value, which the refusal alone would not reveal")
		})
	}

	// Absent and empty are both the default, on both operations.
	kmsRawBody(t, ts, "PutKeyPolicy",
		map[string]any{"KeyId": keyID, "PolicyName": "", "Policy": kmsMinimalPolicy})
	_, policyName := kmsGetPolicy(t, ts, keyID)
	assert.Equal(t, "default", policyName, "an omitted PolicyName resolves to the documented default")
}

// TestKMSKeyPolicy_TheDocumentIsCheckedBeforeTheKey is assertion 6's ordering half.
//
// The status half lives in `kms_error_status_test.go`'s table, which #983 extended with both operations.
// What that table cannot show is the order, and the order is the half a caller feels: a request whose
// document is unusable and whose KeyId names no key is told about the document, which it can fix without an
// AWS account. This follows #991's reading for Encrypt's Plaintext.
func TestKMSKeyPolicy_TheDocumentIsCheckedBeforeTheKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	status, code, _ := kmsRefusal(t, ts, "PutKeyPolicy", map[string]any{
		"KeyId":  kmsAbsentKeyID,
		"Policy": "{not a policy",
	})
	assert.Equal(t, "MalformedPolicyDocumentException", code,
		"the document is malformed whatever key the request names, so the key's own refusal waits")
	assert.Equal(t, http.StatusBadRequest, status, "both candidates are 400, so only the code differs")

	status, code, _ = kmsRefusal(t, ts, "GetKeyPolicy", map[string]any{
		"KeyId":      kmsAbsentKeyID,
		"PolicyName": "administrator",
	})
	assert.Equal(t, "NotFoundException", code,
		"both refusals share a code here, which is why the message is what distinguishes them")
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestKMSKeyPolicy_NeitherOperationRefusesAKeyForItsState is assertion 7.
//
// A key pending deletion still answers both operations at 200. Nothing about that is an accident and nothing
// about it is a gap: the developer guide's key-state table gives GetKeyPolicy and PutKeyPolicy a checkmark in
// all seven state columns, with no footnote on any of them, where TagResource two rows below is refused at
// PendingDeletion under footnote [3]. Both pages publish KMSInvalidStateException, so without this test a
// later sweep completing the key-state checks would read the published code as a missing refusal and add a
// divergence.
func TestKMSKeyPolicy_NeitherOperationRefusesAKeyForItsState(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)
	status, code := kmsCall(t, ts, "ScheduleKeyDeletion", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "ScheduleKeyDeletion")
	require.Equal(t, http.StatusOK, status, "ScheduleKeyDeletion")

	kmsRawBody(t, ts, "PutKeyPolicy", map[string]any{"KeyId": keyID, "Policy": kmsAccountPolicy})
	policy, _ := kmsGetPolicy(t, ts, keyID)
	assert.Equal(t, kmsAccountPolicy, policy,
		"a key pending deletion still takes a policy and still reports it")
}

// TestKMSKeyPolicy_ARefusedPutLeavesTheStoredPolicyAlone is assertion 8.
//
// The observable form of the validation running before the write. A check placed after `state.Put` would
// satisfy every code assertion above and still have replaced a caller's working policy with an unusable one,
// which is the failure mode a refusal is supposed to prevent.
func TestKMSKeyPolicy_ARefusedPutLeavesTheStoredPolicyAlone(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	keyID := createKMSKeyWithPolicy(t, ts, kmsAccountPolicy)

	for _, policy := range []string{"{not a policy", "", kmsPolicyOf(32769)} {
		status, _, _ := kmsRefusal(t, ts, "PutKeyPolicy", map[string]any{"KeyId": keyID, "Policy": policy})
		require.Equal(t, http.StatusBadRequest, status, "the put is refused")
	}

	stored, _ := kmsGetPolicy(t, ts, keyID)
	assert.Equal(t, kmsAccountPolicy, stored, "three refusals later, the key still has the policy it had")
}
