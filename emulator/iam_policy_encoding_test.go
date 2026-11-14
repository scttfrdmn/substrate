package emulator_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMPlugin_InstanceProfileRoleTrustPolicyIsEncoded asserts a role nested in an instance
// profile renders its trust policy as every other Role shape does: the submitted text,
// URL-encoded per RFC 3986 (#1452).
func TestIAMPlugin_InstanceProfileRoleTrustPolicyIsEncoded(t *testing.T) {
	srv := newIAMTestServer(t)
	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateRole", map[string]any{
		"RoleName": "nested", "AssumeRolePolicyDocument": doc,
	}).StatusCode)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateInstanceProfile", map[string]any{
		"InstanceProfileName": "profile",
	}).StatusCode)
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "AddRoleToInstanceProfile", map[string]any{
		"InstanceProfileName": "profile", "RoleName": "nested",
	}).StatusCode)

	for _, op := range []string{"GetInstanceProfile", "ListInstanceProfiles"} {
		resp := iamRequest(t, srv, op, map[string]any{"InstanceProfileName": "profile"})
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, body)

		const open, closing = "<AssumeRolePolicyDocument>", "</AssumeRolePolicyDocument>"
		start := strings.Index(string(body), open)
		end := strings.Index(string(body), closing)
		require.True(t, start >= 0 && end > start, "%s must carry the trust policy: %s", op, body)
		got := string(body)[start+len(open) : end]
		assert.NotContains(t, got, "{", "%s must URL-encode the nested role's trust policy", op)
		decoded, err := url.PathUnescape(got)
		require.NoError(t, err)
		assert.Equal(t, doc, decoded, op)
	}
}
