package emulator_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestIAMListEntitiesForPolicy_ListsAnEntityOnceAndSkipsAVanishedOne(t *testing.T) {
	d := newIDPDirect(t)
	const policy = "arn:aws:iam::aws:policy/ReadOnlyAccess"
	for _, c := range []struct {
		op   string
		body map[string]string
	}{
		{"CreateUser", map[string]string{"UserName": "both"}},
		{"AttachUserPolicy", map[string]string{"UserName": "both", "PolicyArn": policy}},
		{"PutUserPermissionsBoundary", map[string]string{"UserName": "both", "PermissionsBoundary": policy}},
		{"CreateUser", map[string]string{"UserName": "gone"}},
		{"AttachUserPolicy", map[string]string{"UserName": "gone", "PolicyArn": policy}},
	} {
		resp, err := d.call(t, d.iam, c.op, c.body, nil)
		require.NoError(t, err, c.op)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", c.op, resp.Body)
	}
	// An attachment list left naming a user whose record is gone.
	require.NoError(t, d.state.inner.Delete(context.Background(), "iam", "user:123456789012/gone"))

	resp, err := d.call(t, d.iam, "ListEntitiesForPolicy", map[string]string{"PolicyArn": policy}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	assert.Equal(t, []string{"both"}, iamMembers(t, string(resp.Body), "PolicyUsers>member>UserName"),
		"attached and a boundary is one entity, and a vanished one is skipped")
}

func TestTagging_IAMARNEdges(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	for _, c := range []url.Values{
		{"Action": {"CreateUser"}, "UserName": {"u"}},
		{"Action": {"CreateInstanceProfile"}, "InstanceProfileName": {"ip"}},
	} {
		c.Set("Version", "2010-05-08")
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, c, nil)
		require.Equal(t, http.StatusOK, status, body)
	}
	tagResources := func(arn, tags string) string {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
			strings.NewReader(`{"ResourceARNList":["`+arn+`"],"Tags":`+tags+`}`))
		require.NoError(t, err)
		r.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126.TagResources")
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(setup, "tagging"))
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		out, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(out))
		return string(out)
	}

	assert.NotContains(t, tagResources("arn:aws:iam::123456789012:instance-profile/ip", `{"k":"v"}`), "FailedResourcesMap\":{\"arn",
		"an instance profile is tagged through IAM")
	status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup,
		url.Values{"Action": {"ListInstanceProfileTags"}, "Version": {"2010-05-08"}, "InstanceProfileName": {"ip"}}, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, []string{"k"}, iamMembers(t, body, "Tags>member>Key"))

	for _, c := range []struct{ name, arn, tags string }{
		{"reserved key", "arn:aws:iam::123456789012:user/u", `{"aws:reserved":"v"}`},
		{"no name", "arn:aws:iam::123456789012:user/", `{"k":"v"}`},
		{"untaggable type", "arn:aws:iam::123456789012:server-certificate/c", `{"k":"v"}`},
	} {
		assert.Contains(t, tagResources(c.arn, c.tags), c.arn, "%s: the ARN is reported failed", c.name)
	}
}

func TestCFN_IAMPathAndLoginProfileOutOfBand(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	user := func(path, profile string) string {
		return `{"Resources":{"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"oob","Path":"` + path + `"` + profile + `}}}}`
	}
	_, err := d.Deploy(ctx, user("/a/", `,"LoginProfile":{"Password":"Initial-Passw0rd!"}`), "oob", nil)
	require.NoError(t, err)

	// Moved and stripped of its profile outside the stack: the update finds both already as declared.
	cfnIAMOK(t, d, "UpdateUser", map[string]string{"UserName": "oob", "NewPath": "/b/"})
	cfnIAMOK(t, d, "DeleteLoginProfile", map[string]string{"UserName": "oob"})
	updated, err := d.UpdateStack(ctx, user("/b/", ""), "oob", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, "arn:aws:iam::123456789012:user/b/oob", resourceByLogicalID(t, updated, "U").ARN)

	// Deleted outside the stack: a Path change cannot read the user and fails the update.
	cfnIAMOK(t, d, "DeleteUser", map[string]string{"UserName": "oob"})
	updated, err = d.UpdateStack(ctx, user("/c/", ""), "oob", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.NotEqual(t, "UPDATE_COMPLETE", updated.Status)

	group := func(path string) string {
		return `{"Resources":{"G":{"Type":"AWS::IAM::Group","Properties":{"GroupName":"oob-group","Path":"` + path + `"}}}}`
	}
	_, err = d.Deploy(ctx, group("/a/"), "oob-group", nil)
	require.NoError(t, err)
	cfnIAMOK(t, d, "DeleteGroup", map[string]string{"GroupName": "oob-group"})
	updated, err = d.UpdateStack(ctx, group("/b/"), "oob-group", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.NotEqual(t, "UPDATE_COMPLETE", updated.Status, "a group Path change fails when the group is gone")
}
