package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Secrets Manager resource policies: stored, validated and read back, not enforced (#1623).

// smFixedPrincipalPolicy grants one named role GetSecretValue — the API reference's own example
// shape, which no broad-access check refuses.
const smFixedPrincipalPolicy = `{"Version":"2012-10-17","Statement":{"Effect":"Allow",` +
	`"Principal":{"AWS":"arn:aws:iam::123456789012:role/MyRole"},` +
	`"Action":"secretsmanager:GetSecretValue","Resource":"*"}}`

// smWildcardPolicy grants every principal GetSecretValue, which BlockPublicPolicy refuses.
const smWildcardPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
	`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`

// smDenyAllButOnePolicy is the policy the consumer that filed #1623 attaches: deny GetSecretValue to
// every principal but one role. A wildcard principal on a Deny narrows rather than grants.
const smDenyAllButOnePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",` +
	`"Action":"secretsmanager:GetSecretValue","Resource":"*",` +
	`"Condition":{"StringNotEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/endpoint"}}}]}`

// smPinnedWildcardPolicy names a wildcard principal pinned by a fixed aws:PrincipalArn condition.
const smPinnedWildcardPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
	`"Action":"secretsmanager:GetSecretValue","Resource":"*",` +
	`"Condition":{"StringEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/endpoint"}}}]}`

// smWildcardWith is a wildcard-principal Allow of GetSecretValue carrying condition.
func smWildcardWith(condition string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
		`"Action":"secretsmanager:GetSecretValue","Resource":"*","Condition":` + condition + `}]}`
}

// smPolicyCall posts a resource-policy operation and decodes the members it answers.
func smPolicyCall(t *testing.T, call func() (int, string, string)) (int, map[string]json.RawMessage, string) {
	t.Helper()
	status, raw, code := call()
	var members map[string]json.RawMessage
	if code == "" {
		require.NoError(t, json.Unmarshal([]byte(raw), &members), raw)
	}
	return status, members, code
}

func TestSMPolicy_APutPolicyIsReadBackVerbatim(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "policied", nil)

	status, put, code := smPolicyCall(t, func() (int, string, string) {
		return smRawCall(t, ts, smTarget, "PutResourcePolicy", map[string]any{"SecretId": "policied", "ResourcePolicy": smFixedPrincipalPolicy})
	})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"ARN":"`+arn+`","Name":"policied","PolicyName":"default"}`, smMembersJSON(t, put),
		"the response is ARN, Name and the default PolicyName, as the page's sample shows")

	_, got, code := smPolicyCall(t, func() (int, string, string) {
		return smRawCall(t, ts, smTarget, "GetResourcePolicy", map[string]any{"SecretId": arn})
	})
	require.Empty(t, code)
	var text string
	require.NoError(t, json.Unmarshal(got["ResourcePolicy"], &text))
	assert.Equal(t, smFixedPrincipalPolicy, text, "the text the caller submitted, not a re-marshaled form")
	assert.JSONEq(t, `"default"`, string(got["PolicyName"]))
	assert.JSONEq(t, `"`+arn+`"`, string(got["ARN"]))

	// A second put replaces the first.
	status, _, code = smRawCall(t, ts, smTarget, "PutResourcePolicy", map[string]any{"SecretId": arn, "ResourcePolicy": smDenyAllButOnePolicy})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	_, got, _ = smPolicyCall(t, func() (int, string, string) {
		return smRawCall(t, ts, smTarget, "GetResourcePolicy", map[string]any{"SecretId": arn})
	})
	require.NoError(t, json.Unmarshal(got["ResourcePolicy"], &text))
	assert.Equal(t, smDenyAllButOnePolicy, text)
}

func TestSMPolicy_ASecretWithNoPolicyAnswersWithoutOne(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "bare", nil)

	status, got, code := smPolicyCall(t, func() (int, string, string) {
		return smRawCall(t, ts, smTarget, "GetResourcePolicy", map[string]any{"SecretId": arn})
	})
	require.Empty(t, code, "Terraform reads this on every refresh of a secret, policy or not (#1607)")
	require.Equal(t, http.StatusOK, status)
	assert.NotContains(t, got, "ResourcePolicy")
	assert.JSONEq(t, `"bare"`, string(got["Name"]))

	// Deleting a policy that is not there succeeds; so does deleting one that is.
	for range 2 {
		status, del, code := smPolicyCall(t, func() (int, string, string) {
			return smRawCall(t, ts, smTarget, "DeleteResourcePolicy", map[string]any{"SecretId": arn})
		})
		require.Empty(t, code)
		require.Equal(t, http.StatusOK, status)
		assert.JSONEq(t, `"default"`, string(del["PolicyName"]))
		_, _, code = smRawCall(t, ts, smTarget, "PutResourcePolicy", map[string]any{"SecretId": arn, "ResourcePolicy": smFixedPrincipalPolicy})
		require.Empty(t, code)
	}
	status, _, code = smRawCall(t, ts, smTarget, "DeleteResourcePolicy", map[string]any{"SecretId": arn})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	_, got, _ = smPolicyCall(t, func() (int, string, string) {
		return smRawCall(t, ts, smTarget, "GetResourcePolicy", map[string]any{"SecretId": arn})
	})
	assert.NotContains(t, got, "ResourcePolicy", "DeleteResourcePolicy removed it")
}

func TestSMPolicy_RefusalsUseThePublishedCodes(t *testing.T) {
	ts := smTagServer(t)
	smCreateSecret(t, ts, "target", nil)

	for _, tc := range []struct {
		name, op string
		body     map[string]any
		code     string
	}{
		{"Put names another policy", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": smFixedPrincipalPolicy, "PolicyName": "other"}, "InvalidParameterException"},
		{"Delete names another policy", "DeleteResourcePolicy", map[string]any{"SecretId": "target", "PolicyName": "other"}, "InvalidParameterException"},
		{"Get names a policy that does not exist", "GetResourcePolicy", map[string]any{"SecretId": "target", "PolicyName": "other"}, "ResourceNotFoundException"},
		{"a PolicyName outside the pattern", "GetResourcePolicy", map[string]any{"SecretId": "target", "PolicyName": "not valid!"}, "InvalidParameterException"},
		{"no ResourcePolicy", "PutResourcePolicy", map[string]any{"SecretId": "target"}, "InvalidParameterException"},
		{"a ResourcePolicy over the maximum", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": `{"a":"` + strings.Repeat("x", 20480) + `"}`}, "InvalidParameterException"},
		{"a policy that is not JSON", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": "allow everyone"}, "MalformedPolicyDocumentException"},
		{"a policy that is not an object", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": "[]"}, "MalformedPolicyDocumentException"},
		{"a Statement that is not a statement", "ValidateResourcePolicy", map[string]any{"ResourcePolicy": `{"Statement":5}`}, "MalformedPolicyDocumentException"},
		{"a policy that is JSON null", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": "null"}, "MalformedPolicyDocumentException"},
		{"a policy with no Statement", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": `{"Version":"2012-10-17"}`}, "MalformedPolicyDocumentException"},
		{"a policy with an empty Statement", "ValidateResourcePolicy", map[string]any{"ResourcePolicy": `{"Version":"2012-10-17","Statement":[]}`}, "MalformedPolicyDocumentException"},
		{"BlockPublicPolicy on an absent secret is about the secret", "PutResourcePolicy", map[string]any{"SecretId": "absent", "ResourcePolicy": smWildcardPolicy, "BlockPublicPolicy": true}, "ResourceNotFoundException"},
		{"Put on an absent secret", "PutResourcePolicy", map[string]any{"SecretId": "absent", "ResourcePolicy": smFixedPrincipalPolicy}, "ResourceNotFoundException"},
		{"Get on an absent secret", "GetResourcePolicy", map[string]any{"SecretId": "absent"}, "ResourceNotFoundException"},
		{"Delete on an absent secret", "DeleteResourcePolicy", map[string]any{"SecretId": "absent"}, "ResourceNotFoundException"},
		{"Validate against an absent secret", "ValidateResourcePolicy", map[string]any{"SecretId": "absent", "ResourcePolicy": smFixedPrincipalPolicy}, "ResourceNotFoundException"},
		{"BlockPublicPolicy and a wildcard grant", "PutResourcePolicy", map[string]any{"SecretId": "target", "ResourcePolicy": smWildcardPolicy, "BlockPublicPolicy": true}, "PublicPolicyException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw, code := smRawCall(t, ts, smTarget, tc.op, tc.body)
			assert.Equal(t, tc.code, code, raw)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}

	_, got, _ := smPolicyCall(t, func() (int, string, string) {
		return smRawCall(t, ts, smTarget, "GetResourcePolicy", map[string]any{"SecretId": "target"})
	})
	assert.NotContains(t, got, "ResourcePolicy", "no refused put stored anything")
}

func TestSMPolicy_BlockPublicPolicyRefusesOnlyBroadGrants(t *testing.T) {
	ts := smTagServer(t)
	smCreateSecret(t, ts, "guarded", nil)

	for _, tc := range []struct {
		name, policy string
		block        bool
		stored       bool
	}{
		{"a wildcard grant is stored when not blocked", smWildcardPolicy, false, true},
		{"a named principal passes the block", smFixedPrincipalPolicy, true, true},
		{"a wildcard Deny passes the block", smDenyAllButOnePolicy, true, true},
		{"a wildcard pinned by a fixed aws:PrincipalArn passes the block", smPinnedWildcardPolicy, true, true},
		{"a wildcard with StringNotEquals is refused", smWildcardWith(`{"StringNotEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/endpoint"}}`), true, false},
		{"a wildcard with StringEqualsIfExists is refused", smWildcardWith(`{"StringEqualsIfExists":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/endpoint"}}`), true, false},
		{"a wildcard with Null is refused", smWildcardWith(`{"Null":{"aws:PrincipalArn":"false"}}`), true, false},
		{"S3's access-point keys do not qualify a secret's policy", smWildcardWith(`{"StringEquals":{"s3:DataAccessPointAccount":"123456789012"}}`), true, false},
		{"a wildcard pinned by ArnEquals passes the block", smWildcardWith(`{"ArnEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/endpoint"}}`), true, true},
		{"a wildcard grant is refused when blocked", smWildcardPolicy, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, code := smRawCall(t, ts, smTarget, "DeleteResourcePolicy", map[string]any{"SecretId": "guarded"})
			require.Empty(t, code)
			_, _, code = smRawCall(t, ts, smTarget, "PutResourcePolicy",
				map[string]any{"SecretId": "guarded", "ResourcePolicy": tc.policy, "BlockPublicPolicy": tc.block})
			_, got, _ := smPolicyCall(t, func() (int, string, string) {
				return smRawCall(t, ts, smTarget, "GetResourcePolicy", map[string]any{"SecretId": "guarded"})
			})
			if tc.stored {
				assert.Empty(t, code)
				assert.Contains(t, got, "ResourcePolicy")
				return
			}
			assert.Equal(t, "PublicPolicyException", code)
			assert.NotContains(t, got, "ResourcePolicy")
		})
	}
}

func TestSMPolicy_ValidateReportsBroadAccessAsAFailedCheck(t *testing.T) {
	ts := smTagServer(t)
	smCreateSecret(t, ts, "validated", nil)

	for _, tc := range []struct {
		name, policy, secretID string
		passed                 bool
	}{
		{"a named principal passes", smFixedPrincipalPolicy, "", true},
		{"against a named secret", smFixedPrincipalPolicy, "validated", true},
		{"a wildcard Deny passes", smDenyAllButOnePolicy, "", true},
		{"a wildcard grant fails", smWildcardPolicy, "", false},
		{"a negated condition narrows nothing", smWildcardWith(`{"StringNotEquals":{"aws:SourceAccount":"123456789012"}}`), "", false},
		{"a fixed aws:SourceAccount passes", smWildcardWith(`{"StringEquals":{"aws:SourceAccount":"123456789012"}}`), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"ResourcePolicy": tc.policy}
			if tc.secretID != "" {
				body["SecretId"] = tc.secretID
			}
			status, got, code := smPolicyCall(t, func() (int, string, string) {
				return smRawCall(t, ts, smTarget, "ValidateResourcePolicy", body)
			})
			require.Empty(t, code)
			require.Equal(t, http.StatusOK, status)
			var out struct {
				Passed bool `json:"PolicyValidationPassed"`
				Errors []struct {
					CheckName    string `json:"CheckName"`
					ErrorMessage string `json:"ErrorMessage"`
				} `json:"ValidationErrors"`
			}
			require.NoError(t, json.Unmarshal([]byte(smMembersJSON(t, got)), &out))
			assert.Equal(t, tc.passed, out.Passed)
			require.Contains(t, got, "ValidationErrors", "the member is present even when empty, as the CLI example shows")
			if tc.passed {
				assert.Empty(t, out.Errors)
				return
			}
			require.Len(t, out.Errors, 1)
			assert.NotEmpty(t, out.Errors[0].CheckName)
			assert.NotEmpty(t, out.Errors[0].ErrorMessage)
		})
	}
}

// The secret is checked before the policy's breadth, so a scheduled secret is refused for being
// scheduled even with BlockPublicPolicy set and a broad grant (#1641).
func TestSMPolicy_AScheduledSecretIsRefusedBeforeTheBroadAccessCheck(t *testing.T) {
	ts := smTagServer(t)
	arn := smScheduleSecret(t, ts, "scheduled-public")
	status, raw, code := smRawCall(t, ts, smTarget, "PutResourcePolicy",
		map[string]any{"SecretId": arn, "ResourcePolicy": smWildcardPolicy, "BlockPublicPolicy": true})
	assert.Equal(t, "InvalidRequestException", code, raw)
	assert.Equal(t, http.StatusBadRequest, status)
}

// The policy is stored, not enforced (#1636): a policy denying everyone GetSecretValue leaves the
// call to the caller's identity policies, as it was before #1623.
func TestSMPolicy_AStoredPolicyIsNotEnforced(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "unenforced", nil)
	denyAll := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`
	_, _, code := smRawCall(t, ts, smTarget, "PutResourcePolicy", map[string]any{"SecretId": arn, "ResourcePolicy": denyAll})
	require.Empty(t, code)

	assert.Equal(t, "value-of-unenforced", smSecretValue(t, ts, smTarget, arn))
}

// smMembersJSON re-encodes decoded members, for a JSONEq over a whole response.
func smMembersJSON(t *testing.T, members map[string]json.RawMessage) string {
	t.Helper()
	data, err := json.Marshal(members)
	require.NoError(t, err)
	return string(data)
}
