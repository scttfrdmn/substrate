package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Cognito User Pools' three tagging operations (#1135), over the wire: they read and write the tag
// set DescribeUserPool reports as UserPoolTags and GetResources scans.

const (
	cognitoTagsHost = "cognito-idp.us-east-1.amazonaws.com"
	cognitoTagsTgt  = "AWSCognitoIdentityProviderService"
)

func TestCognitoIDPTagging_RoundTripsThroughEveryReader(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cognitoTagsHost, cognitoTagsTgt, op, body)
	}
	status, body := call("CreateUserPool", map[string]any{"PoolName": "tag-pool", "UserPoolTags": map[string]string{"env": "test"}})
	require.Equal(t, http.StatusOK, status, "CreateUserPool: %s", body)
	var created struct {
		UserPool struct {
			ID  string `json:"Id"`
			Arn string `json:"Arn"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(body, &created), "%s", body)
	arn := created.UserPool.Arn
	require.NotEmpty(t, arn)

	listTags := func() map[string]string {
		t.Helper()
		status, body := call("ListTagsForResource", map[string]any{"ResourceArn": arn})
		require.Equal(t, http.StatusOK, status, "ListTagsForResource: %s", body)
		var out struct {
			Tags map[string]string `json:"Tags"`
		}
		require.NoError(t, json.Unmarshal(body, &out), "%s", body)
		require.NotNil(t, out.Tags, "Tags is always present: %s", body)
		return out.Tags
	}
	describeTags := func() map[string]string {
		t.Helper()
		status, body := call("DescribeUserPool", map[string]any{"UserPoolId": created.UserPool.ID})
		require.Equal(t, http.StatusOK, status, "DescribeUserPool: %s", body)
		var out struct {
			UserPool struct {
				Tags map[string]string `json:"UserPoolTags"`
			} `json:"UserPool"`
		}
		require.NoError(t, json.Unmarshal(body, &out), "%s", body)
		return out.UserPool.Tags
	}
	scanned := func() (bool, int) {
		t.Helper()
		status, body := cicdCall(t, ts, "tagging.us-east-1.amazonaws.com", "ResourceGroupsTaggingAPI_20170126", "GetResources",
			map[string]any{"ResourceTypeFilters": []string{"cognito-idp:userpool"}})
		require.Equal(t, http.StatusOK, status, "GetResources: %s", body)
		var out struct {
			List []struct {
				ARN  string           `json:"ResourceARN"`
				Tags []map[string]any `json:"Tags"`
			} `json:"ResourceTagMappingList"`
		}
		require.NoError(t, json.Unmarshal(body, &out), "%s", body)
		for _, m := range out.List {
			if m.ARN == arn {
				return true, len(m.Tags)
			}
		}
		return false, 0
	}

	require.Equal(t, map[string]string{"env": "test"}, listTags(), "the create-time tag set")

	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
		want map[string]string
	}{
		{"TagResource merges", "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"team": "a", "env": "prod"}},
			map[string]string{"env": "prod", "team": "a"}},
		{"UntagResource removes the keys named", "UntagResource", map[string]any{"ResourceArn": arn, "TagKeys": []string{"env", "absent"}},
			map[string]string{"team": "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := call(tc.op, tc.body)
			require.Equal(t, http.StatusOK, status, "%s: %s", tc.op, body)
			require.Empty(t, body, "%s publishes an empty HTTP body", tc.op)
			require.Equal(t, tc.want, listTags())
			require.Equal(t, tc.want, describeTags())
			found, n := scanned()
			require.True(t, found, "GetResources reports the pool")
			require.Equal(t, len(tc.want), n)
		})
	}

	// Emptied, the pool is still reported with an empty set: it has been tagged.
	status, body = call("UntagResource", map[string]any{"ResourceArn": arn, "TagKeys": []string{"team"}})
	require.Equal(t, http.StatusOK, status, "%s", body)
	require.Empty(t, listTags())
	found, n := scanned()
	require.True(t, found, "a previously tagged pool stays visible")
	require.Zero(t, n)
}

func TestCognitoIDPTagging_RefusesWhatThePagesRefuse(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cognitoTagsHost, cognitoTagsTgt, op, body)
	}
	status, body := call("CreateUserPool", map[string]any{"PoolName": "refuse-pool"})
	require.Equal(t, http.StatusOK, status, "CreateUserPool: %s", body)
	var created struct {
		UserPool struct {
			ID  string `json:"Id"`
			Arn string `json:"Arn"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(body, &created), "%s", body)
	arn := created.UserPool.Arn

	const acct = "123456789012"
	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
		code string
	}{
		{"no ResourceArn", "ListTagsForResource", map[string]any{}, "InvalidParameterException"},
		{"no Tags", "TagResource", map[string]any{"ResourceArn": arn}, "InvalidParameterException"},
		{"no TagKeys", "UntagResource", map[string]any{"ResourceArn": arn}, "InvalidParameterException"},
		{"absent pool", "ListTagsForResource", map[string]any{"ResourceArn": "arn:aws:cognito-idp:us-east-1:" + acct + ":userpool/us-east-1_absent0000"}, "ResourceNotFoundException"},
		{"another Region", "ListTagsForResource", map[string]any{"ResourceArn": "arn:aws:cognito-idp:us-west-2:" + acct + ":userpool/" + created.UserPool.ID}, "ResourceNotFoundException"},
		{"another account", "TagResource", map[string]any{"ResourceArn": "arn:aws:cognito-idp:us-east-1:210987654321:userpool/" + created.UserPool.ID, "Tags": map[string]string{"k": "v"}}, "ResourceNotFoundException"},
		{"an app-client path", "UntagResource", map[string]any{"ResourceArn": arn + "/client/abc", "TagKeys": []string{"k"}}, "ResourceNotFoundException"},
		{"an identity pool", "ListTagsForResource", map[string]any{"ResourceArn": "arn:aws:cognito-identity:us-east-1:" + acct + ":identitypool/us-east-1:abc"}, "ResourceNotFoundException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := call(tc.op, tc.body)
			require.Equal(t, http.StatusBadRequest, status, "%s: %s", tc.op, body)
			require.Equal(t, tc.code, cicdCode(t, body), "%s: %s", tc.op, body)
		})
	}
}
