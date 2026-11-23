package emulator_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListInstanceProfilesForRole covers the call the Terraform AWS provider makes before
// deleting a role (#1519): the profiles that hold the role, paged, and NoSuchEntity for an
// absent role.
func TestListInstanceProfilesForRole(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	for _, role := range []string{"app", "other"} {
		resp := iamFormRequest(t, srv, "CreateRole", map[string]string{
			"RoleName":                 role,
			"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}
	iamSeedProfiles(t, srv, []iamProfileSpec{{name: "p-a"}, {name: "p-b"}, {name: "p-c"}})
	for profile, role := range map[string]string{"p-a": "app", "p-b": "other", "p-c": "app"} {
		resp := iamFormRequest(t, srv, "AddRoleToInstanceProfile", map[string]string{
			"InstanceProfileName": profile, "RoleName": role,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}

	list := func(params map[string]string) map[string]any {
		t.Helper()
		resp := iamFormRequest(t, srv, "ListInstanceProfilesForRole", params)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var result map[string]any
		decodeIAMXML(t, resp, &result)
		return result
	}

	tests := []struct {
		name      string
		params    map[string]string
		want      []string
		truncated bool
	}{
		{name: "every profile holding the role", params: map[string]string{"RoleName": "app"}, want: []string{"p-a", "p-c"}},
		{name: "a role in one profile", params: map[string]string{"RoleName": "other"}, want: []string{"p-b"}},
		{name: "MaxItems pages the result", params: map[string]string{"RoleName": "app", "MaxItems": "1"},
			want: []string{"p-a"}, truncated: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := list(tc.params)
			assert.Equal(t, tc.want, iamProfileNames(t, result))
			assert.Equal(t, tc.truncated, iamIsTruncated(t, result))
		})
	}

	t.Run("the marker resumes the walk", func(t *testing.T) {
		first := list(map[string]string{"RoleName": "app", "MaxItems": "1"})
		marker, ok := first["Marker"].(string)
		require.True(t, ok)
		second := list(map[string]string{"RoleName": "app", "MaxItems": "1", "Marker": marker})
		assert.Equal(t, []string{"p-c"}, iamProfileNames(t, second))
		assert.False(t, iamIsTruncated(t, second))
	})

	refusals := []struct {
		name   string
		params map[string]string
		status int
		code   string
	}{
		{name: "an absent role", params: map[string]string{"RoleName": "missing"}, status: http.StatusNotFound, code: "NoSuchEntity"},
		{name: "no RoleName", params: map[string]string{}, status: http.StatusBadRequest, code: "ValidationError"},
		{name: "MaxItems out of range", params: map[string]string{"RoleName": "app", "MaxItems": "0"},
			status: http.StatusBadRequest, code: "ValidationError"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			resp := iamFormRequest(t, srv, "ListInstanceProfilesForRole", tc.params)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tc.status, resp.StatusCode, "%s", body)
			assert.Contains(t, string(body), "<Code>"+tc.code+"</Code>")
		})
	}
}
