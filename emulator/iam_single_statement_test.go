package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A policy whose Statement is one object rather than an array — #1477.
//
// The IAM grammar's conventions make the brackets optional when an array element carries one value,
// and AWS's own sample responses (GetGroupPolicy) use the bare form. Substrate decoded only the
// array, so every policy writer answered MalformedPolicyDocument for a document IAM takes.

const (
	iamSingleStatementIdentity = `{"Version":"2012-10-17","Statement":` +
		`{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}}`
	iamSingleStatementIdentityList = `{"Version":"2012-10-17","Statement":` +
		`[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	iamSingleStatementTrust = `{"Version":"2012-10-17","Statement":` +
		`{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}}`
)

// TestPolicyDocument_SingleObjectStatementIsAOneElementList asserts both forms decode to the same
// list, so evaluation and policyDocumentsEqual cannot tell them apart.
func TestPolicyDocument_SingleObjectStatementIsAOneElementList(t *testing.T) {
	t.Parallel()
	var single, list emulator.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(iamSingleStatementIdentity), &single))
	require.NoError(t, json.Unmarshal([]byte(iamSingleStatementIdentityList), &list))

	require.Len(t, single.Statement, 1)
	assert.Equal(t, list, single, "the two forms decode identically")
	assert.True(t, emulator.PolicyDocumentsEqualForTest(single, list))

	for _, tc := range []struct {
		action string
		want   string
	}{
		{"s3:GetObject", emulator.DecisionAllow},
		{"s3:PutObject", emulator.DecisionImplicitDeny},
	} {
		req := emulator.EvaluationRequest{Action: tc.action, Resource: "arn:aws:s3:::b/k"}
		assert.Equal(t, tc.want, emulator.Evaluate([]emulator.PolicyDocument{single}, req).Decision, tc.action)
		assert.Equal(t, tc.want, emulator.Evaluate([]emulator.PolicyDocument{list}, req).Decision, tc.action)
	}

	for _, bad := range []string{
		`{"Version":"2012-10-17","Statement":"Allow"}`,
		`{"Version":"2012-10-17","Statement":{"Effect":["Allow"]}}`,
	} {
		var doc emulator.PolicyDocument
		assert.Error(t, json.Unmarshal([]byte(bad), &doc), bad)
	}
}

// TestIAMPlugin_SingleObjectStatementOnEveryWriter sends the bare form to each policy writer and
// asserts the write is accepted and the document is read back as submitted (#1452).
func TestIAMPlugin_SingleObjectStatementOnEveryWriter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		setup     []func(*testing.T, *emulator.Server)
		write     string
		writeBody map[string]any
		read      string
		readBody  map[string]any
		member    string
		doc       string
	}{
		{
			name: "CreateRole", write: "CreateRole",
			writeBody: map[string]any{"RoleName": "r", "AssumeRolePolicyDocument": iamSingleStatementTrust},
			read:      "GetRole", readBody: map[string]any{"RoleName": "r"},
			member: "Role.AssumeRolePolicyDocument", doc: iamSingleStatementTrust,
		},
		{
			name: "UpdateAssumeRolePolicy", write: "UpdateAssumeRolePolicy",
			setup: []func(*testing.T, *emulator.Server){iamSingleStatementCreate("CreateRole",
				map[string]any{"RoleName": "r"})},
			writeBody: map[string]any{"RoleName": "r", "PolicyDocument": iamSingleStatementTrust},
			read:      "GetRole", readBody: map[string]any{"RoleName": "r"},
			member: "Role.AssumeRolePolicyDocument", doc: iamSingleStatementTrust,
		},
		{
			name: "CreatePolicy", write: "CreatePolicy",
			writeBody: map[string]any{"PolicyName": "p", "PolicyDocument": iamSingleStatementIdentity},
			read:      "GetPolicyVersion",
			readBody:  map[string]any{"PolicyArn": "arn:aws:iam::123456789012:policy/p", "VersionId": "v1"},
			member:    "PolicyVersion.Document", doc: iamSingleStatementIdentity,
		},
		{
			name: "PutUserPolicy", write: "PutUserPolicy",
			setup: []func(*testing.T, *emulator.Server){iamSingleStatementCreate("CreateUser",
				map[string]any{"UserName": "u"})},
			writeBody: map[string]any{"UserName": "u", "PolicyName": "p", "PolicyDocument": iamSingleStatementIdentity},
			read:      "GetUserPolicy", readBody: map[string]any{"UserName": "u", "PolicyName": "p"},
			member: "PolicyDocument", doc: iamSingleStatementIdentity,
		},
		{
			name: "PutGroupPolicy", write: "PutGroupPolicy",
			setup: []func(*testing.T, *emulator.Server){iamSingleStatementCreate("CreateGroup",
				map[string]any{"GroupName": "g"})},
			writeBody: map[string]any{"GroupName": "g", "PolicyName": "p", "PolicyDocument": iamSingleStatementIdentity},
			read:      "GetGroupPolicy", readBody: map[string]any{"GroupName": "g", "PolicyName": "p"},
			member: "PolicyDocument", doc: iamSingleStatementIdentity,
		},
		{
			name: "PutRolePolicy", write: "PutRolePolicy",
			setup: []func(*testing.T, *emulator.Server){iamSingleStatementCreate("CreateRole",
				map[string]any{"RoleName": "r"})},
			writeBody: map[string]any{"RoleName": "r", "PolicyName": "p", "PolicyDocument": iamSingleStatementIdentity},
			read:      "GetRolePolicy", readBody: map[string]any{"RoleName": "r", "PolicyName": "p"},
			member: "PolicyDocument", doc: iamSingleStatementIdentity,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			for _, s := range tc.setup {
				s(t, srv)
			}
			var wrote map[string]any
			resp := iamRequest(t, srv, tc.write, tc.writeBody)
			status := resp.StatusCode
			decodeIAMXML(t, resp, &wrote)
			require.Equal(t, http.StatusOK, status, "%s refused a single-object Statement: %v", tc.write, wrote)

			var got map[string]any
			decodeIAMXML(t, iamRequest(t, srv, tc.read, tc.readBody), &got)
			encoded := iamSingleStatementMember(t, got, tc.member)
			decoded, err := url.PathUnescape(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.doc, decoded, "%s serves the submitted form", tc.read)
		})
	}
}

// iamSingleStatementCreate returns a setup step that issues one IAM call and requires a 200.
func iamSingleStatementCreate(op string, body map[string]any) func(*testing.T, *emulator.Server) {
	return func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		require.Equal(t, http.StatusOK, iamRequest(t, srv, op, body).StatusCode, op)
	}
}

// iamSingleStatementMember reads a dotted path out of a decoded IAM result.
func iamSingleStatementMember(t *testing.T, result map[string]any, path string) string {
	t.Helper()
	var cur any = result
	for _, name := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		require.True(t, ok, "%s: %v", path, result)
		cur = m[name]
	}
	s, ok := cur.(string)
	require.True(t, ok, "%s is not a string: %v", path, result)
	return s
}

// TestSTSAssumeRole_SingleObjectTrustPolicyIsEnforced asserts the STS trust gate reads the bare form
// as it reads the list: the named account is admitted, and a policy naming another is refused.
func TestSTSAssumeRole_SingleObjectTrustPolicyIsEnforced(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy string
		want   int
	}{
		{"the named account is admitted", iamSingleStatementTrust, http.StatusOK},
		{"another account is refused", `{"Version":"2012-10-17","Statement":` +
			`{"Effect":"Allow","Principal":{"AWS":"999999999999"},"Action":"sts:AssumeRole"}}`,
			http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTrustPolicyTestServer(t)
			accessKey := trustSetupCaller(t, srv, "alice")
			trustCreateRole(t, srv, "broker", tc.policy)
			resp := trustAssumeRole(t, srv, accessKey, "arn:aws:iam::123456789012:role/broker", "s", "")
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tc.want, resp.StatusCode)
		})
	}
}
