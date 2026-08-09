package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #594: UpdateAssumeRolePolicy was absent from the IAM operation switch, so a
// role's trust policy could only ever be set by CreateRole. That was invisible
// until #593 made the document load-bearing; now a consumer that creates a role
// and narrows its trust policy in a second pass — the update-in-place shape CDK
// and Terraform both emit — silently kept the original policy, so the tightening
// the test existed to verify was unobservable.
//
// The decisive test is TestIAMPlugin_UpdateAssumeRolePolicy_TakesEffectOnNextAssume:
// the others pin the operation's own contract, but only that one proves the
// replacement reaches the gate that reads it.

func TestIAMPlugin_UpdateAssumeRolePolicy_ReplacesTheDocument(t *testing.T) {
	srv := newIAMTestServer(t)

	const original = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	const replacement = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateRole", map[string]any{
		"RoleName":                 "worker",
		"AssumeRolePolicyDocument": original,
	}).StatusCode)

	resp := iamRequest(t, srv, "UpdateAssumeRolePolicy", map[string]any{
		"RoleName":       "worker",
		"PolicyDocument": replacement,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The response shape carries only ResponseMetadata — the model declares no
	// output shape at all — so there is no Result element to decode.
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Contains(t, string(body), "<UpdateAssumeRolePolicyResponse")
	assert.Contains(t, string(body), "<ResponseMetadata>")

	// Read back through GetRole. Substrate stores the document parsed, so the
	// round trip is by principal rather than by byte: the replacement's Service
	// principal must be there and the original's must be gone.
	var result map[string]any
	get := iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "worker"})
	require.Equal(t, http.StatusOK, get.StatusCode)
	decodeIAMXML(t, get, &result)
	role, ok := result["Role"].(map[string]any)
	require.True(t, ok, "GetRole must report a Role element: %v", result)

	doc, ok := role["AssumeRolePolicyDocument"].(string)
	require.True(t, ok, "GetRole must report the trust policy: %v", role)
	assert.Contains(t, doc, "ecs-tasks.amazonaws.com")
	assert.NotContains(t, doc, "lambda.amazonaws.com",
		"the replacement must displace the original, not merge with it")

	// And it must still parse as a policy document, so a consumer reading it back
	// can act on it. botocore url-decodes then json.loads every policyDocumentType
	// member, and unquoting plain JSON is a no-op, so this is what a real SDK sees.
	var parsed emulator.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(doc), &parsed))
	require.Len(t, parsed.Statement, 1)
	assert.Equal(t, "Allow", parsed.Statement[0].Effect)
}

// TestIAMPlugin_UpdateAssumeRolePolicy_SetsAPolicyOnARoleCreatedWithout pins the
// other direction of the same call: substrate permits creating a role with no
// trust policy (real IAM requires one), and such a role is unenforced. An update
// is how a test opts that role into enforcement without recreating it.
func TestIAMPlugin_UpdateAssumeRolePolicy_SetsAPolicyOnARoleCreatedWithout(t *testing.T) {
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK,
		iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "bare"}).StatusCode)

	// GetRole reports no document at all before the update — not an empty one,
	// which would be a document AWS never sends.
	var before map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "bare"}), &before)
	role, ok := before["Role"].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, role, "AssumeRolePolicyDocument")

	require.Equal(t, http.StatusOK, iamRequest(t, srv, "UpdateAssumeRolePolicy", map[string]any{
		"RoleName": "bare",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}`,
	}).StatusCode)

	var after map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "bare"}), &after)
	role, ok = after["Role"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, role["AssumeRolePolicyDocument"], "123456789012")
}

// TestIAMPlugin_UpdateAssumeRolePolicy_ReadBackIsNormalized pins the shape of the
// read-back, so the normalization is a documented property rather than a surprise.
//
// Substrate stores the parsed document and re-marshals it, which is what
// GetRolePolicy and GetUserPolicy already do for inline policies. AWS stores the
// text and returns the submitted bytes, so a consumer comparing the read-back
// byte-for-byte against what it sent sees a difference here. Both forms are legal
// AWS input for the same meaning and unmarshal identically, so a consumer that
// parses the document is unaffected — which is the case worth pinning, because a
// change that broke it would look like a passing test.
func TestIAMPlugin_UpdateAssumeRolePolicy_ReadBackIsNormalized(t *testing.T) {
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK,
		iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "worker"}).StatusCode)

	// A lone AWS principal in map form. PolicyPrincipal marshals this back as the
	// bare string AWS also accepts.
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "UpdateAssumeRolePolicy", map[string]any{
		"RoleName": "worker",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}`,
	}).StatusCode)

	var result map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "worker"}), &result)
	role, ok := result["Role"].(map[string]any)
	require.True(t, ok)
	doc, ok := role["AssumeRolePolicyDocument"].(string)
	require.True(t, ok)

	assert.Contains(t, doc, `"Principal":"123456789012"`,
		"a lone AWS principal round-trips through the bare-string form")

	// The meaning survives, which is what the STS gate reads.
	var parsed emulator.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(doc), &parsed))
	require.Len(t, parsed.Statement, 1)
	require.NotNil(t, parsed.Statement[0].Principal)
	assert.Equal(t, []string{"123456789012"}, parsed.Statement[0].Principal.AWS)
	assert.False(t, parsed.Statement[0].Principal.All,
		"a named account must not normalize into the wildcard form")
}

// TestIAMPlugin_UpdateAssumeRolePolicy_Errors covers the operation's documented
// error shapes. The codes and statuses are the botocore IAM model's:
// NoSuchEntity/404 and MalformedPolicyDocument/400.
func TestIAMPlugin_UpdateAssumeRolePolicy_Errors(t *testing.T) {
	const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":"*","Action":"sts:AssumeRole"}]}`

	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
		wantCode   string
	}{
		{
			name:       "an unknown role is NoSuchEntity",
			body:       map[string]any{"RoleName": "nobody", "PolicyDocument": validPolicy},
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchEntity",
		},
		{
			name:       "a document that does not parse is MalformedPolicyDocument",
			body:       map[string]any{"RoleName": "worker", "PolicyDocument": "not json"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MalformedPolicyDocument",
		},
		{
			// PolicyDocument is required by the model, unlike CreateRole's optional
			// AssumeRolePolicyDocument — there is no "clear the trust policy" form of
			// this call, so an absent document is a validation error rather than a
			// silent unenforcing of the role.
			name:       "an absent PolicyDocument is a validation error",
			body:       map[string]any{"RoleName": "worker"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
		},
		{
			name:       "an absent RoleName is a validation error",
			body:       map[string]any{"PolicyDocument": validPolicy},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newIAMTestServer(t)
			require.Equal(t, http.StatusOK,
				iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "worker"}).StatusCode)

			resp := iamRequest(t, srv, "UpdateAssumeRolePolicy", tt.body)
			require.Equal(t, tt.wantStatus, resp.StatusCode)

			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, tt.wantCode, result["__type"])
		})
	}
}

// TestIAMPlugin_UpdateAssumeRolePolicy_LeavesTheRoleOtherwiseIntact pins that the
// update is a replacement of one field, not of the role. The handler re-marshals
// the whole record, so a dropped field would be silent — GetRole would simply
// stop reporting it.
func TestIAMPlugin_UpdateAssumeRolePolicy_LeavesTheRoleOtherwiseIntact(t *testing.T) {
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateRole", map[string]any{
		"RoleName":           "worker",
		"Path":               "/service-role/",
		"Description":        "A role whose trust policy gets tightened.",
		"MaxSessionDuration": 7200,
	}).StatusCode)

	var before map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "worker"}), &before)
	original, ok := before["Role"].(map[string]any)
	require.True(t, ok)

	require.Equal(t, http.StatusOK, iamRequest(t, srv, "UpdateAssumeRolePolicy", map[string]any{
		"RoleName": "worker",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":"*","Action":"sts:AssumeRole"}]}`,
	}).StatusCode)

	var after map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "worker"}), &after)
	updated, ok := after["Role"].(map[string]any)
	require.True(t, ok)

	for _, field := range []string{"RoleId", "RoleName", "Arn", "Path", "CreateDate", "Description", "MaxSessionDuration"} {
		assert.Equal(t, original[field], updated[field], "%s must survive the update", field)
	}
}

// TestIAMPlugin_UpdateAssumeRolePolicy_TakesEffectOnNextAssume is #594's point:
// the same role, assumed successfully, tightened to another account, then refused.
// Before this operation existed a test had to create two roles to express that,
// which is not what the code under test does.
func TestIAMPlugin_UpdateAssumeRolePolicy_TakesEffectOnNextAssume(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	accessKey := trustSetupCaller(t, srv, "alice")
	trustCreateRole(t, srv, "broker", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",`+
		`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}`)

	const roleARN = "arn:aws:iam::123456789012:role/broker"

	allowed := trustAssumeRole(t, srv, accessKey, roleARN, "session1", "")
	require.Equal(t, http.StatusOK, allowed.StatusCode,
		"the caller's own account is trusted, so this must mint a session")
	require.NoError(t, allowed.Body.Close())

	// Tighten to a partner account the caller is not in.
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "UpdateAssumeRolePolicy", map[string]any{
		"RoleName": "broker",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"999999999999"},"Action":"sts:AssumeRole"}]}`,
	}).StatusCode)

	denied := trustAssumeRole(t, srv, accessKey, roleARN, "session2", "")
	require.Equal(t, http.StatusForbidden, denied.StatusCode,
		"the tightened policy must be the one the gate reads")
	code, message := trustErrorFrom(t, denied)
	assert.Equal(t, "AccessDenied", code)
	assert.Equal(t, trustMsgImplicit, message)
}

// TestIAMPlugin_UpdateAssumeRolePolicy_DoesNotRevokeExistingSessions pins the
// other half of "takes effect on the next assume". AWS does not revoke sessions
// minted under a policy it later replaces, and substrate should not either: the
// gate runs at AssumeRole time, so a session already in state stays valid.
//
// Asserted through GetCallerIdentity rather than by reading state, because the
// question is whether the *credential still works*, and only a request answers
// that.
func TestIAMPlugin_UpdateAssumeRolePolicy_DoesNotRevokeExistingSessions(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	accessKey := trustSetupCaller(t, srv, "alice")
	trustCreateRole(t, srv, "broker", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",`+
		`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}`)

	resp := trustAssumeRole(t, srv, accessKey,
		"arn:aws:iam::123456789012:role/broker", "session1", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	sessionKey := trustSessionKeyFrom(t, resp)

	// Replace the policy with one that would refuse a fresh assumption outright.
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "UpdateAssumeRolePolicy", map[string]any{
		"RoleName": "broker",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",` +
			`"Principal":"*","Action":"sts:AssumeRole"}]}`,
	}).StatusCode)

	identity := trustGetCallerIdentity(t, srv, sessionKey)
	require.Equal(t, http.StatusOK, identity.StatusCode,
		"a session minted under the old policy must keep working")
	body, err := io.ReadAll(identity.Body)
	require.NoError(t, err)
	require.NoError(t, identity.Body.Close())
	assert.Contains(t, string(body), "assumed-role/broker/session1",
		"and must still identify as the role it assumed")

	// The control: a *new* assumption is now refused, so the replacement did land.
	fresh := trustAssumeRole(t, srv, accessKey,
		"arn:aws:iam::123456789012:role/broker", "session2", "")
	assert.Equal(t, http.StatusForbidden, fresh.StatusCode)
	require.NoError(t, fresh.Body.Close())
}
