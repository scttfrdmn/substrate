package emulator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Which members each IAM entity shape reports (#807).
//
// Substrate both over- and under-reported what the IAM API reference documents, independently
// of tags (#796, which covered `Tags` only):
//
//   - `ListUsers` and `ListRoles` rendered `PermissionsBoundary`, which AWS's own note on those
//     operations excludes by name — a divergence in the direction where the emulator is *more*
//     generous than the service, so a consumer could write an assertion AWS never satisfies.
//   - `IsAttachable` and `Description` were stored from `CreatePolicy` and never rendered, and
//     `PasswordLastUsed` was on the user record and never rendered.
//
// Every assertion here is against **raw XML**, not a decoded map, because the question is
// whether a member is present at all and a map cannot tell absent from empty: both decode to
// the zero value, which is exactly what an SDK reports for a member the service omitted.
//
// The two members this release leaves unmodeled have no test: `PermissionsBoundaryUsageCount`
// (#815) and `RoleLastUsed` (#816). Their absence is asserted here only in the negative sense
// that the shapes below list every member substrate renders.

// iamRawBody returns a response's body as a string, for an assertion about an element being
// present or absent rather than about its decoded value.
func iamRawBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	return string(body)
}

// iamFormRaw runs an IAM query-protocol operation and returns its raw XML body.
func iamFormRaw(t *testing.T, srv *emulator.Server, operation string, params map[string]string) string {
	t.Helper()
	return iamRawBody(t, iamFormRequest(t, srv, operation, params))
}

// TestIAM_PermissionsBoundaryIsSingleEntityOnly pins AWS's exclusion note: a boundary is
// readable through GetUser and GetRole and absent from ListUsers and ListRoles.
func TestIAM_PermissionsBoundaryIsSingleEntityOnly(t *testing.T) {
	srv := newIAMTestServer(t)

	const boundaryARN = "arn:aws:iam::aws:policy/PowerUserAccess"

	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": boundaryARN,
	})
	iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})
	iamFormRaw(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": boundaryARN,
	})

	tests := []struct {
		name      string
		operation string
		params    map[string]string
		want      bool
	}{
		{"GetUser reports it", "GetUser", map[string]string{"UserName": "alice"}, true},
		{"ListUsers does not", "ListUsers", nil, false},
		{"GetRole reports it", "GetRole", map[string]string{"RoleName": "deploy"}, true},
		{"ListRoles does not", "ListRoles", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := iamFormRaw(t, srv, tt.operation, tt.params)
			// The entity itself must be in the response, or an absence assertion would
			// pass against a listing that returned nothing at all.
			assert.Contains(t, body, boundaryPresenceAnchor(tt.operation), "entity missing from response")
			if tt.want {
				assert.Contains(t, body, "<PermissionsBoundary>", "boundary should be reported")
				assert.Contains(t, body, "<PolicyArn>"+boundaryARN+"</PolicyArn>")
				assert.Contains(t, body, "<PolicyName>PowerUserAccess</PolicyName>")
				return
			}
			assert.NotContains(t, body, "PermissionsBoundary",
				"AWS's listing note excludes the member by name")
		})
	}
}

// boundaryPresenceAnchor returns an element that must appear in the named operation's
// response, so an absence assertion cannot pass vacuously.
func boundaryPresenceAnchor(operation string) string {
	if strings.Contains(operation, "Role") {
		return "<RoleName>deploy</RoleName>"
	}
	return "<UserName>alice</UserName>"
}

// TestIAM_PolicyShapeMembers pins the two members AWS documents per policy shape:
// IsAttachable on both, Description on the single-entity shape only.
func TestIAM_PolicyShapeMembers(t *testing.T) {
	srv := newIAMTestServer(t)

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	created := iamFormRaw(t, srv, "CreatePolicy", map[string]string{
		"PolicyName":     "readonly",
		"PolicyDocument": doc,
		"Description":    "read objects only",
	})
	assert.Contains(t, created, "<IsAttachable>true</IsAttachable>",
		"CreatePolicy returns the Policy shape, which documents IsAttachable")

	arn := iamPolicyARNFrom(t, created)

	single := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
	assert.Contains(t, single, "<IsAttachable>true</IsAttachable>")
	assert.Contains(t, single, "<Description>read objects only</Description>",
		`the Policy type: "included in the response to the GetPolicy operation"`)

	listing := iamFormRaw(t, srv, "ListPolicies", map[string]string{"Scope": "Local"})
	assert.Contains(t, listing, "<PolicyName>readonly</PolicyName>", "policy missing from listing")
	assert.Contains(t, listing, "<IsAttachable>true</IsAttachable>",
		"ListPolicies' own sample response renders IsAttachable on every member")
	assert.NotContains(t, listing, "Description",
		`the Policy type: "not included in the response to the ListPolicies operation"`)
}

// iamPolicyARNFrom extracts the ARN a CreatePolicy response reported.
func iamPolicyARNFrom(t *testing.T, body string) string {
	t.Helper()
	const open, close = "<Arn>", "</Arn>"
	start := strings.Index(body, open)
	require.GreaterOrEqual(t, start, 0, "no <Arn> in CreatePolicy response")
	rest := body[start+len(open):]
	end := strings.Index(rest, close)
	require.GreaterOrEqual(t, end, 0, "unterminated <Arn>")
	return rest[:end]
}

// TestIAM_PolicyDescriptionIsOmittedWhenUnset keeps the Description member off a policy that
// has none, rather than reporting an empty one where AWS reports nothing.
func TestIAM_PolicyDescriptionIsOmittedWhenUnset(t *testing.T) {
	srv := newIAMTestServer(t)

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	created := iamFormRaw(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "plain", "PolicyDocument": doc,
	})
	arn := iamPolicyARNFrom(t, created)

	body := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
	assert.Contains(t, body, "<PolicyName>plain</PolicyName>")
	assert.NotContains(t, body, "Description", "Description is Required: No on the Policy type")
}

// TestIAM_PasswordLastUsedIsRenderedWhenSet renders the member from the field the user record
// has always carried, and omits it when nil.
//
// Nothing in substrate assigns it — no password operation is modeled — so the value is seeded
// into state directly, which is also the only way a consumer can observe it. That makes the
// omission case the one every ordinary run takes.
func TestIAM_PasswordLastUsedIsRenderedWhenSet(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	srv := newIAMTestServerWithState(t, state)
	ctx := context.Background()

	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})

	fresh := iamFormRaw(t, srv, "GetUser", map[string]string{"UserName": "alice"})
	assert.NotContains(t, fresh, "PasswordLastUsed",
		"a user who never signed in with a password has no PasswordLastUsed")

	keys, err := state.List(ctx, "iam", "user:")
	require.NoError(t, err)
	require.Len(t, keys, 1, "exactly one user was created")

	raw, err := state.Get(ctx, "iam", keys[0])
	require.NoError(t, err)
	var user map[string]any
	require.NoError(t, json.Unmarshal(raw, &user))
	user["PasswordLastUsed"] = "2026-03-04T05:06:07Z"
	updated, err := json.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", keys[0], updated))

	single := iamFormRaw(t, srv, "GetUser", map[string]string{"UserName": "alice"})
	assert.Contains(t, single, "<PasswordLastUsed>2026-03-04T05:06:07Z</PasswordLastUsed>")

	listing := iamFormRaw(t, srv, "ListUsers", nil)
	assert.Contains(t, listing, "<PasswordLastUsed>2026-03-04T05:06:07Z</PasswordLastUsed>",
		`the User type: "returned only in the GetUser and ListUsers operations"`)
}
