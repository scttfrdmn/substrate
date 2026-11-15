package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// UpdateRole (#1545): the operation CloudFormation's in-place AWS::IAM::Role update sends for a
// changed Description or MaxSessionDuration.
func TestUpdateRole(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamRequireOK(t, srv, "CreateRole", map[string]any{
		"RoleName": "updatable", "Description": "first",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	role := func() map[string]any {
		var out map[string]any
		decodeIAMXML(t, iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "updatable"}), &out)
		r, ok := out["Role"].(map[string]any)
		require.True(t, ok, "GetRole: %v", out)
		return r
	}

	iamRequireOK(t, srv, "UpdateRole", map[string]any{"RoleName": "updatable", "MaxSessionDuration": 7200})
	got := role()
	assert.Equal(t, "7200", got["MaxSessionDuration"])
	assert.Equal(t, "first", got["Description"], "a member the request omits is left as it was")

	iamRequireOK(t, srv, "UpdateRole", map[string]any{"RoleName": "updatable", "Description": "second"})
	got = role()
	assert.Equal(t, "second", got["Description"])
	assert.Equal(t, "7200", got["MaxSessionDuration"])

	tests := []struct {
		name   string
		body   map[string]any
		status int
		code   string
	}{
		{"an absent role", map[string]any{"RoleName": "absent", "Description": "x"}, http.StatusNotFound, "NoSuchEntity"},
		{"a session under an hour", map[string]any{"RoleName": "updatable", "MaxSessionDuration": 60},
			http.StatusBadRequest, "ValidationError"},
		{"a session over twelve hours", map[string]any{"RoleName": "updatable", "MaxSessionDuration": 43201},
			http.StatusBadRequest, "ValidationError"},
		{"no role name", map[string]any{"Description": "x"}, http.StatusBadRequest, "ValidationError"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.code, iamExpectError(t, srv, "UpdateRole", tt.body, tt.status))
		})
	}
}
