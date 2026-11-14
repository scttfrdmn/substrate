package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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

	// Read back through GetRole, which serves the stored text RFC 3986-encoded
	// (#1452): decoded, it is the replacement byte for byte.
	var result map[string]any
	get := iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "worker"})
	require.Equal(t, http.StatusOK, get.StatusCode)
	decodeIAMXML(t, get, &result)
	role, ok := result["Role"].(map[string]any)
	require.True(t, ok, "GetRole must report a Role element: %v", result)

	encoded, ok := role["AssumeRolePolicyDocument"].(string)
	require.True(t, ok, "GetRole must report the trust policy: %v", role)
	doc, err := url.PathUnescape(encoded)
	require.NoError(t, err)
	assert.Equal(t, replacement, doc)
	assert.Contains(t, doc, "ecs-tasks.amazonaws.com")
	assert.NotContains(t, doc, "lambda.amazonaws.com",
		"the replacement must displace the original, not merge with it")

	// And it must still parse as a policy document, so a consumer reading it back
	// can act on it. botocore url-decodes then json.loads every policyDocumentType
	// member, so this is what a real SDK sees.
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

// TestIAMPlugin_TrustPolicyReadBackIsVerbatim is #1452: the trust policy is served
// as the text that was submitted, not re-marshaled from its parsed form.
//
// Re-marshaling flattened a lone AWS principal written as {"AWS":"<arn>"} to the
// bare string "<arn>", a Principal IAM accepts only as "*", so the document read back
// was not one UpdateAssumeRolePolicy would take, and code converging a trust policy
// by comparison rewrote one real IAM reports unchanged. The cases cover both writers
// and a document whose whitespace and list-form members a re-marshal would also
// have changed.
//
// GetRole serves the text RFC 3986-encoded, as its page states; CreateRole and
// ListRoles, whose pages carry no such note, serve it plain.
func TestIAMPlugin_TrustPolicyReadBackIsVerbatim(t *testing.T) {
	const awsPrincipal = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::444455556666:role/X"},"Action":"sts:AssumeRole"}]}`
	const formatted = "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [{\"Effect\": \"Allow\", " +
		"\"Principal\": {\"Service\": [\"ec2.amazonaws.com\"]}, \"Action\": [\"sts:AssumeRole\"]}]\n}"

	cases := []struct {
		name   string
		doc    string
		update bool
	}{
		{name: "CreateRole, lone AWS principal", doc: awsPrincipal},
		{name: "UpdateAssumeRolePolicy, lone AWS principal", doc: awsPrincipal, update: true},
		{name: "CreateRole, whitespace and list-form members", doc: formatted},
		{name: "UpdateAssumeRolePolicy, whitespace and list-form members", doc: formatted, update: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newIAMTestServer(t)
			create := map[string]any{"RoleName": "worker"}
			if !tc.update {
				create["AssumeRolePolicyDocument"] = tc.doc
			}
			created := iamRequest(t, srv, "CreateRole", create)
			require.Equal(t, http.StatusOK, created.StatusCode)
			if tc.update {
				require.Equal(t, http.StatusOK, iamRequest(t, srv, "UpdateAssumeRolePolicy", map[string]any{
					"RoleName": "worker", "PolicyDocument": tc.doc,
				}).StatusCode)
			} else {
				var out map[string]any
				decodeIAMXML(t, created, &out)
				role, ok := out["Role"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tc.doc, role["AssumeRolePolicyDocument"],
					"CreateRole answers the same trust policy in JSON format, unencoded")
			}

			var got map[string]any
			decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "worker"}), &got)
			role, ok := got["Role"].(map[string]any)
			require.True(t, ok)
			encoded, ok := role["AssumeRolePolicyDocument"].(string)
			require.True(t, ok)
			assert.NotContains(t, encoded, "{", "GetRole URL-encodes the document")
			decoded, err := url.PathUnescape(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.doc, decoded, "GetRole answers the submitted text")

			// The meaning survives, which is what the STS gate reads.
			var parsed emulator.PolicyDocument
			require.NoError(t, json.Unmarshal([]byte(decoded), &parsed))
			require.Len(t, parsed.Statement, 1)
		})
	}
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
