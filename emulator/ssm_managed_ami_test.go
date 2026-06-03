package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSSMPlugin_ManagedAMIParameter verifies that GetParameter resolves the
// AWS-managed public AMI parameters under /aws/service/* (which are not
// user-created) to a deterministic synthetic AMI id, rather than 404. Tools
// like spawn auto-detect their AMI via these parameters.
func TestSSMPlugin_ManagedAMIParameter(t *testing.T) {
	srv := newSSMTestServer(t)

	const name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"

	resp := ssmRequest(t, srv, "GetParameter", map[string]interface{}{"Name": name})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSSMBody(t, resp)
	param, ok := body["Parameter"].(map[string]interface{})
	require.True(t, ok)

	val, _ := param["Value"].(string)
	assert.True(t, strings.HasPrefix(val, "ami-"), "managed AMI param should resolve to an ami- value, got %q", val)
	assert.Equal(t, name, param["Name"])

	// Deterministic: a second lookup returns the same value.
	resp2 := ssmRequest(t, srv, "GetParameter", map[string]interface{}{"Name": name})
	body2 := readSSMBody(t, resp2)
	param2, _ := body2["Parameter"].(map[string]interface{})
	assert.Equal(t, val, param2["Value"], "managed AMI resolution should be stable across calls")

	// A user-created param still wins over the synthetic resolver.
	ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name": "/aws/service/custom-override", "Value": "explicit", "Type": "String",
	})
	resp3 := ssmRequest(t, srv, "GetParameter", map[string]interface{}{"Name": "/aws/service/custom-override"})
	body3 := readSSMBody(t, resp3)
	param3, _ := body3["Parameter"].(map[string]interface{})
	assert.Equal(t, "explicit", param3["Value"])

	// A non-managed, non-existent param still 404s.
	resp4 := ssmRequest(t, srv, "GetParameter", map[string]interface{}{"Name": "/not/aws/service/missing"})
	assert.Equal(t, http.StatusNotFound, resp4.StatusCode)
}
