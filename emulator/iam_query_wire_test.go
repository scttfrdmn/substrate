package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file drives the IAM plugin the way a real client does: an
// application/x-www-form-urlencoded body in the AWS query protocol, with lists
// encoded as `.member.N`. Every other IAM unit test uses iamRequest, which
// hand-marshals a JSON body containing real arrays — a shape no AWS client
// produces. That is why #639 shipped: TagUser answered 200 and stored nothing, and
// the whole unit suite agreed it worked.
//
// The SDK-level regression lives in test/e2e/journey_iam_tags_test.go. These are
// the same assertions one layer down, so a decode regression is reported by the
// package that owns the decoder rather than only by the separate e2e module.

// iamFormRequest executes an IAM query-protocol request against srv, encoding
// params as a form body exactly as an AWS client would. The operation travels in
// the Action parameter, which is the query protocol's own mechanism, rather than in
// an X-Amz-Target header.
func iamFormRequest(t *testing.T, srv *emulator.Server, operation string, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	form.Set("Action", operation)
	form.Set("Version", "2010-05-08")
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// iamFormTagNames returns the tag keys and values a Tags-bearing IAM XML response
// reports, so an assertion can name what it expected rather than only a count.
func iamFormTagNames(t *testing.T, resp *http.Response) map[string]string {
	t.Helper()
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	raw, ok := result["Tags"].([]any)
	if !ok {
		return map[string]string{}
	}
	tags := make(map[string]string, len(raw))
	for _, entry := range raw {
		tag, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		key, _ := tag["Key"].(string)
		value, _ := tag["Value"].(string)
		tags[key] = value
	}
	return tags
}

// TestIAMWire_UserTagsFromMemberList is #639 for the user pair. Two tags, because a
// decoder that reads only index 1 would pass a single-tag assertion.
func TestIAMWire_UserTagsFromMemberList(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamFormRequest(t, srv, "CreateUser", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "TagUser", map[string]string{
		"UserName":            "jill",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
		"Tags.member.2.Key":   "team",
		"Tags.member.2.Value": "infra",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "ListUserTags", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, iamFormTagNames(t, resp),
		"the tags a client sent over the wire never reached tagUser")

	// TagKeys is a flat member list rather than a struct one, so it exercises the
	// other decoder. Removing one of two proves the keys are read rather than the
	// whole set being dropped.
	resp = iamFormRequest(t, srv, "UntagUser", map[string]string{
		"UserName":         "jill",
		"TagKeys.member.1": "env",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "ListUserTags", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"team": "infra"}, iamFormTagNames(t, resp))
}

// TestIAMWire_RoleTagsFromMemberList is the role pair, which was broken
// independently of the user pair: a fix applied to only one would leave these nil.
func TestIAMWire_RoleTagsFromMemberList(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	resp := iamFormRequest(t, srv, "CreateRole", map[string]string{
		"RoleName":                 "tagged-role",
		"AssumeRolePolicyDocument": trustDoc,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "TagRole", map[string]string{
		"RoleName":            "tagged-role",
		"Tags.member.1.Key":   "owner",
		"Tags.member.1.Value": "platform",
		"Tags.member.2.Key":   "tier",
		"Tags.member.2.Value": "prod",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "ListRoleTags", map[string]string{"RoleName": "tagged-role"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"owner": "platform", "tier": "prod"}, iamFormTagNames(t, resp))

	resp = iamFormRequest(t, srv, "UntagRole", map[string]string{
		"RoleName":         "tagged-role",
		"TagKeys.member.1": "owner",
		"TagKeys.member.2": "tier",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "ListRoleTags", map[string]string{"RoleName": "tagged-role"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, iamFormTagNames(t, resp), "both keys were named, so nothing should remain")
}

// TestIAMWire_CreateWithTags covers the create-time Tags parameter, which is the
// same encoding on a different operation: CDK and Terraform tag at creation rather
// than in a second call, so a user or role created with tags kept none of them.
func TestIAMWire_CreateWithTags(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamFormRequest(t, srv, "CreateUser", map[string]string{
		"UserName":            "born-tagged",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "ListUserTags", map[string]string{"UserName": "born-tagged"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "prod"}, iamFormTagNames(t, resp))

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	resp = iamFormRequest(t, srv, "CreateRole", map[string]string{
		"RoleName":                 "born-tagged-role",
		"AssumeRolePolicyDocument": trustDoc,
		"Tags.member.1.Key":        "tier",
		"Tags.member.1.Value":      "prod",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "ListRoleTags", map[string]string{"RoleName": "born-tagged-role"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"tier": "prod"}, iamFormTagNames(t, resp))
}

// TestIAMWire_NoTagsLeavesJSONPathIntact guards the fallback the fix relies on: a
// request carrying no Tags member list must not have its JSON-body tags cleared.
// That dual read is what keeps every existing iamRequest-driven test meaningful.
func TestIAMWire_NoTagsLeavesJSONPathIntact(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "json-tagged"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "TagUser", map[string]any{
		"UserName": "json-tagged",
		"Tags":     []map[string]string{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "ListUserTags", map[string]any{"UserName": "json-tagged"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "test"}, iamFormTagNames(t, resp))
}
