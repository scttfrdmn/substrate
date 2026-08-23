package emulator_test

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file is #737: IAM state was keyed by name alone, so every account shared one
// IAM. Two accounts could not both hold a role called "deploy" — the second create
// answered EntityAlreadyExists — and a listing made by one account returned the
// other's entities. Nothing on the wire says which account an IAM entity belongs
// to except its ARN, which is exactly why a single-account suite could not see it.
//
// Each test signs as a specific account, because that is the only way to be a
// second caller (see signed_request_test.go).

// The two accounts these tests are. Neither is the default account, so a test that
// accidentally resolved to it would fail rather than pass by coincidence.
const (
	iamScopeAccountA = "111122223333"
	iamScopeAccountB = "444455556666"
)

// iamSignedForm posts an IAM query-protocol request signed as the given account.
//
// IAM is a query-protocol service, so the operation travels in the Action
// parameter of a form body, which is what a real client sends — the same shape
// iam_query_wire_test.go uses, plus a signature.
func iamSignedForm(t *testing.T, ts *emulator.TestServer, account, operation string,
	params map[string]string,
) *http.Response {
	t.Helper()

	creds, ok := ts.CredentialsFor(account)
	if !ok {
		t.Fatalf("no credential registered for account %s", account)
	}

	form := url.Values{}
	form.Set("Action", operation)
	form.Set("Version", "2010-05-08")
	for k, v := range params {
		form.Set(k, v)
	}
	body := []byte(form.Encode())

	const host = "iam.amazonaws.com"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(http.MethodPost, "/", host, "iam", "us-east-1",
		sigV4TestDateTime, body, creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "%s as %s", operation, account)
	return resp
}

// iamScopeResult runs an IAM operation as an account and returns the status, the
// error code a refusal carries, and the decoded response.
func iamScopeResult(t *testing.T, ts *emulator.TestServer, account, operation string,
	params map[string]string,
) (int, string, map[string]any) {
	t.Helper()

	resp := iamSignedForm(t, ts, account, operation, params)
	status := resp.StatusCode
	var decoded map[string]any
	decodeIAMXML(t, resp, &decoded)
	require.NoError(t, resp.Body.Close())

	code, _ := decoded["__type"].(string)
	return status, code, decoded
}

// iamScopeServer starts a server both accounts can sign as.
func iamScopeServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServerWithAccounts(t, iamScopeAccountA, iamScopeAccountB)
}

// TestIAM_TwoAccountsHoldTheSameNames is #737's repro. Before the fix the second
// account's create answered EntityAlreadyExists 409 for every entity kind, because
// the state key was the bare name.
func TestIAM_TwoAccountsHoldTheSameNames(t *testing.T) {
	tests := []struct {
		operation string
		params    map[string]string
		arnPath   string // the response member holding the created entity's ARN
	}{
		{
			operation: "CreateUser",
			params:    map[string]string{"UserName": "deploy"},
			arnPath:   "User",
		},
		{
			operation: "CreateRole",
			params: map[string]string{
				"RoleName":                 "deploy",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			arnPath: "Role",
		},
		{
			operation: "CreateGroup",
			params:    map[string]string{"GroupName": "deploy"},
			arnPath:   "Group",
		},
		{
			operation: "CreateInstanceProfile",
			params:    map[string]string{"InstanceProfileName": "deploy"},
			arnPath:   "InstanceProfile",
		},
		{
			operation: "CreatePolicy",
			params: map[string]string{
				"PolicyName":     "deploy",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
			},
			arnPath: "Policy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			ts := iamScopeServer(t)

			for _, account := range []string{iamScopeAccountA, iamScopeAccountB} {
				status, code, decoded := iamScopeResult(t, ts, account, tt.operation, tt.params)
				require.Equalf(t, http.StatusOK, status,
					"%s in account %s: %s — the name belongs to the account, not to the emulator",
					tt.operation, account, code)

				entity, ok := decoded[tt.arnPath].(map[string]any)
				require.Truef(t, ok, "%s response has no %s member: %v", tt.operation, tt.arnPath, decoded)
				arn, _ := entity["Arn"].(string)
				assert.Truef(t, strings.Contains(arn, ":"+account+":"),
					"created in account %s but its ARN is %q", account, arn)
			}
		})
	}
}

// TestIAM_ListsSeeOnlyTheCallersAccount covers the prefix scans. A scan over a
// name-keyed prefix returned every account's entities, so an account that had
// created nothing still listed another account's.
func TestIAM_ListsSeeOnlyTheCallersAccount(t *testing.T) {
	ts := iamScopeServer(t)

	// Account A creates one of everything the listings walk.
	for _, seed := range []struct {
		operation string
		params    map[string]string
	}{
		{"CreateUser", map[string]string{"UserName": "a-user"}},
		{"CreateRole", map[string]string{
			"RoleName":                 "a-role",
			"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}},
		{"CreateGroup", map[string]string{"GroupName": "a-group"}},
		{"CreateInstanceProfile", map[string]string{"InstanceProfileName": "a-profile"}},
		{"CreatePolicy", map[string]string{
			"PolicyName":     "a-policy",
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		}},
	} {
		status, code, _ := iamScopeResult(t, ts, iamScopeAccountA, seed.operation, seed.params)
		require.Equalf(t, http.StatusOK, status, "seeding %s: %s", seed.operation, code)
	}

	tests := []struct {
		operation string
		params    map[string]string
		member    string
	}{
		{"ListUsers", nil, "Users"},
		{"ListRoles", nil, "Roles"},
		{"ListGroups", nil, "Groups"},
		{"ListInstanceProfiles", nil, "InstanceProfiles"},
		// Local scope excludes the bundled AWS-managed catalog, so what comes back
		// is exactly what an account created.
		{"ListPolicies", map[string]string{"Scope": "Local"}, "Policies"},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			status, code, decoded := iamScopeResult(t, ts, iamScopeAccountA, tt.operation, tt.params)
			require.Equalf(t, http.StatusOK, status, "%s as A: %s", tt.operation, code)
			assert.Lenf(t, iamScopeList(decoded, tt.member), 1,
				"account A created one; %s reported %v", tt.operation, decoded[tt.member])

			status, code, decoded = iamScopeResult(t, ts, iamScopeAccountB, tt.operation, tt.params)
			require.Equalf(t, http.StatusOK, status, "%s as B: %s", tt.operation, code)
			assert.Emptyf(t, iamScopeList(decoded, tt.member),
				"account B created nothing; %s reported %v — the scan crossed accounts",
				tt.operation, decoded[tt.member])
		})
	}
}

// iamScopeList returns the members of a list-shaped response member, tolerating the
// XML decoder's single-element and absent forms.
func iamScopeList(decoded map[string]any, member string) []any {
	switch v := decoded[member].(type) {
	case []any:
		return v
	case map[string]any:
		return []any{v}
	default:
		return nil
	}
}

// TestIAM_ReadsDoNotCrossAccounts is the other half of the isolation: an entity
// account A created must be absent for account B, rather than readable.
func TestIAM_ReadsDoNotCrossAccounts(t *testing.T) {
	ts := iamScopeServer(t)

	status, code, _ := iamScopeResult(t, ts, iamScopeAccountA, "CreateRole", map[string]string{
		"RoleName":                 "shared-name",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.Equalf(t, http.StatusOK, status, "CreateRole as A: %s", code)

	status, code, _ = iamScopeResult(t, ts, iamScopeAccountA, "GetRole",
		map[string]string{"RoleName": "shared-name"})
	require.Equalf(t, http.StatusOK, status, "GetRole as its own account: %s", code)

	status, code, _ = iamScopeResult(t, ts, iamScopeAccountB, "GetRole",
		map[string]string{"RoleName": "shared-name"})
	assert.Equal(t, http.StatusNotFound, status,
		"account B read account A's role")
	assert.Equal(t, "NoSuchEntity", code)

	// A delete must not reach across either, or one account could remove another's
	// role by name.
	status, code, _ = iamScopeResult(t, ts, iamScopeAccountB, "DeleteRole",
		map[string]string{"RoleName": "shared-name"})
	assert.Equal(t, http.StatusNotFound, status, "account B deleted account A's role")
	assert.Equal(t, "NoSuchEntity", code)

	status, code, _ = iamScopeResult(t, ts, iamScopeAccountA, "GetRole",
		map[string]string{"RoleName": "shared-name"})
	assert.Equalf(t, http.StatusOK, status, "account A's role survived B's delete: %s", code)
}

// TestIAM_InlineAndAttachedPoliciesAreScoped covers the per-entity index keys —
// "<kind>_inline:", "<kind>_inline_names:" and "<kind>_policies:" — which are keyed
// by entity name and so were also shared.
func TestIAM_InlineAndAttachedPoliciesAreScoped(t *testing.T) {
	ts := iamScopeServer(t)

	for _, account := range []string{iamScopeAccountA, iamScopeAccountB} {
		status, code, _ := iamScopeResult(t, ts, account, "CreateUser",
			map[string]string{"UserName": "worker"})
		require.Equalf(t, http.StatusOK, status, "CreateUser in %s: %s", account, code)
	}

	// Only account A gives its user an inline policy and an attachment.
	status, code, _ := iamScopeResult(t, ts, iamScopeAccountA, "PutUserPolicy", map[string]string{
		"UserName":       "worker",
		"PolicyName":     "inline-read",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
	})
	require.Equalf(t, http.StatusOK, status, "PutUserPolicy: %s", code)

	status, code, _ = iamScopeResult(t, ts, iamScopeAccountA, "AttachUserPolicy", map[string]string{
		"UserName":  "worker",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	require.Equalf(t, http.StatusOK, status, "AttachUserPolicy: %s", code)

	status, code, decoded := iamScopeResult(t, ts, iamScopeAccountA, "ListUserPolicies",
		map[string]string{"UserName": "worker"})
	require.Equalf(t, http.StatusOK, status, "ListUserPolicies as A: %s", code)
	assert.Len(t, iamScopeList(decoded, "PolicyNames"), 1)

	status, code, decoded = iamScopeResult(t, ts, iamScopeAccountB, "ListUserPolicies",
		map[string]string{"UserName": "worker"})
	require.Equalf(t, http.StatusOK, status, "ListUserPolicies as B: %s", code)
	assert.Emptyf(t, iamScopeList(decoded, "PolicyNames"),
		"account B's user reports account A's inline policy: %v", decoded["PolicyNames"])

	status, code, decoded = iamScopeResult(t, ts, iamScopeAccountB, "ListAttachedUserPolicies",
		map[string]string{"UserName": "worker"})
	require.Equalf(t, http.StatusOK, status, "ListAttachedUserPolicies as B: %s", code)
	assert.Emptyf(t, iamScopeList(decoded, "AttachedPolicies"),
		"account B's user reports account A's attachment: %v", decoded["AttachedPolicies"])

	// And the inline document itself must not be readable across the accounts.
	status, code, _ = iamScopeResult(t, ts, iamScopeAccountB, "GetUserPolicy", map[string]string{
		"UserName":   "worker",
		"PolicyName": "inline-read",
	})
	assert.Equal(t, http.StatusNotFound, status, "account B read account A's inline policy")
	assert.Equal(t, "NoSuchEntity", code)
}
