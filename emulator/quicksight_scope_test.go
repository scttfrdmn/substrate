package emulator_test

// QuickSight reads the path's AwsAccountId and keys resources by account, Region and ID (#1167).

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// quicksightScopeCall issues one request and returns its status and, for a refusal, its code.
func quicksightScopeCall(t *testing.T, p *emulator.QuickSightPlugin, ctx *emulator.RequestContext, method, path string, body map[string]any) (int, string, []byte) {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "quicksight", HTTPMethod: method, Path: path, Body: raw,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code, nil
	}
	require.NoError(t, err)
	return resp.StatusCode, "", resp.Body
}

func TestQuickSightScope_TheSameIDInTwoRegionsIsTwoResources(t *testing.T) {
	p, east, _ := setupQuickSightWirePlugin(t)
	west := &emulator.RequestContext{
		AccountID: quicksightWireAccount, Region: "eu-west-1",
		RequestID: "req-quicksight-west", IDs: emulator.NewIDMint("req-quicksight-west"),
	}
	base := "/accounts/" + quicksightWireAccount

	status, _, _ := quicksightScopeCall(t, p, east, "POST", base+"/data-sources",
		map[string]any{"DataSourceId": "shared-id", "Name": "east", "Type": "S3"})
	require.Equal(t, http.StatusCreated, status)

	// Not visible from the other Region.
	status, code, _ := quicksightScopeCall(t, p, west, "GET", base+"/data-sources/shared-id", nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFoundException", code)

	// And creatable there under the same ID, as a separate resource.
	status, _, _ = quicksightScopeCall(t, p, west, "POST", base+"/data-sources",
		map[string]any{"DataSourceId": "shared-id", "Name": "west", "Type": "S3"})
	require.Equal(t, http.StatusCreated, status)
	for region, ctx := range map[string]*emulator.RequestContext{"east": east, "west": west} {
		status, _, raw := quicksightScopeCall(t, p, ctx, "GET", base+"/data-sources/shared-id", nil)
		require.Equal(t, http.StatusOK, status)
		var out struct {
			DataSource struct {
				Name string `json:"Name"`
				Arn  string `json:"Arn"`
			} `json:"DataSource"`
		}
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.Equal(t, region, out.DataSource.Name, "each Region answers its own data source")
		assert.Contains(t, out.DataSource.Arn, ":"+ctx.Region+":")
	}

	// Datasets are scoped the same way.
	status, _, raw := quicksightScopeCall(t, p, east, "POST", base+"/data-sets",
		map[string]any{"DataSetId": "shared-set", "Name": "east"})
	require.Equal(t, http.StatusCreated, status)
	var created struct {
		IngestionID string `json:"IngestionId"`
	}
	require.NoError(t, json.Unmarshal(raw, &created))
	path := base + "/data-sets/shared-set/ingestions/" + created.IngestionID
	status, _, _ = quicksightScopeCall(t, p, east, "GET", path, nil)
	assert.Equal(t, http.StatusOK, status)
	status, code, _ = quicksightScopeCall(t, p, west, "GET", path, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFoundException", code)
}

func TestQuickSightScope_ThePathAccountMustBeTheCallers(t *testing.T) {
	p, ctx, state := setupQuickSightWirePlugin(t)
	for _, tc := range []struct {
		name, account string
		status        int
		code          string
	}{
		// AccessDeniedException is published at 401 on every QuickSight page substrate routes.
		{"another account", "999999999999", http.StatusUnauthorized, "AccessDeniedException"},
		{"not twelve digits", "12345", http.StatusBadRequest, "InvalidParameterValueException"},
		{"not digits", "abcdefghijkl", http.StatusBadRequest, "InvalidParameterValueException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := "/accounts/" + tc.account
			for _, req := range []struct {
				method, path string
				body         map[string]any
			}{
				{"POST", base + "/data-sources", map[string]any{"DataSourceId": "ds", "Name": "n", "Type": "S3"}},
				{"GET", base + "/data-sources/ds", nil},
				{"POST", base + "/data-sets", map[string]any{"DataSetId": "set", "Name": "n"}},
				{"GET", base + "/data-sets/set/ingestions/ing", nil},
			} {
				status, code, _ := quicksightScopeCall(t, p, ctx, req.method, req.path, req.body)
				assert.Equal(t, tc.status, status, "%s %s", req.method, req.path)
				assert.Equal(t, tc.code, code, "%s %s", req.method, req.path)
			}
		})
	}
	// A refused create wrote nothing, in either account.
	keys, err := state.List(t.Context(), "quicksight", "")
	require.NoError(t, err)
	assert.Empty(t, keys)
}
