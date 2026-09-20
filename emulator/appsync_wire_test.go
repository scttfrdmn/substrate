package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #1121: AppSync answered four records straight out of state, and the persisted shape is not the
// published one — the ARN was spelled `apiArn` where API_GraphqlApi publishes `arn`, the API carried
// substrate's own `region` and `accountId`, and a data source, resolver and function each carried an
// `apiId` their published types do not list.
//
// Every assertion below reads the raw bytes as well as the decoded map, for the reason
// agwNoInternalFields gives: a bookkeeping member nested inside a map would satisfy a top-level key
// check and still reach the caller.

// appSyncWire sends a request and returns the status, the raw body and the decoded body.
func appSyncWire(t *testing.T, ts *httptest.Server, method, path string, body any,
) (int, []byte, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = "appsync.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	decoded := map[string]any{}
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &decoded), string(raw))
	}
	return resp.StatusCode, raw, decoded
}

// appSyncWireNoBookkeeping fails if a response carries a member AppSync publishes nowhere.
//
// `apiId` is checked only on the child shapes, because API_GraphqlApi does publish it — hence the
// separate argument rather than one fixed list.
func appSyncWireNoBookkeeping(t *testing.T, what string, raw []byte, alsoAPIID bool) {
	t.Helper()
	bad := []string{`"region"`, `"accountId"`, `"apiArn"`, `"Region"`, `"AccountID"`}
	if alsoAPIID {
		bad = append(bad, `"apiId"`)
	}
	for _, member := range bad {
		if bytes.Contains(raw, []byte(member)) {
			t.Errorf("%s: response carries %s, which AppSync publishes nowhere: %s", what, member, raw)
		}
	}
}

// appSyncWireAPI creates an API and returns the server, the API ID and the ARN it reported.
func appSyncWireAPI(t *testing.T) (*httptest.Server, string, string) {
	t.Helper()
	ts := newAppSyncTestServer(t)
	status, raw, body := appSyncWire(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "wire_api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusOK, status, string(raw))
	api, ok := body["graphqlApi"].(map[string]any)
	require.True(t, ok, "CreateGraphqlApi answered no graphqlApi: %s", raw)
	apiID, _ := api["apiId"].(string)
	arn, _ := api["arn"].(string)
	require.NotEmpty(t, apiID)
	return ts, apiID, arn
}

// TestAppSyncWire_GraphqlAPIAnswersArn is #1121's first criterion, on all four operations that carry
// a graphqlApi object.
//
// `arn` is what API_GraphqlApi publishes, so `aws.ToString(out.GraphqlApi.Arn)` used to be "" with no
// error — a silent wrong answer, not a refusal.
func TestAppSyncWire_GraphqlAPIAnswersArn(t *testing.T) {
	ts, apiID, arn := appSyncWireAPI(t)
	require.Equal(t, "arn:aws:appsync:us-east-1:123456789012:apis/"+apiID, arn,
		"CreateGraphqlApi: the ARN substrate computes is unchanged; only the member name moved")

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		body    any
		wrapper string
	}{
		{"GetGraphqlApi", http.MethodGet, "/v1/apis/" + apiID, nil, "graphqlApi"},
		{"UpdateGraphqlApi", http.MethodPost, "/v1/apis/" + apiID, map[string]any{"name": "renamed"}, "graphqlApi"},
		{"ListGraphqlApis", http.MethodGet, "/v1/apis", nil, "graphqlApis"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw, body := appSyncWire(t, ts, tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusOK, status, string(raw))

			api := body[tc.wrapper]
			if list, ok := api.([]any); ok {
				require.Len(t, list, 1)
				api = list[0]
			}
			obj, ok := api.(map[string]any)
			require.True(t, ok, "%s: no %s object: %s", tc.name, tc.wrapper, raw)

			assert.Equal(t, arn, obj["arn"], "%s answers the same ARN under the published member", tc.name)
			assert.NotContains(t, obj, "apiArn", "apiArn is a member no SDK reads")
			appSyncWireNoBookkeeping(t, tc.name, raw, false)
			assert.Equal(t, apiID, obj["apiId"], "apiId is published on this shape and stays")
		})
	}
}

// TestAppSyncWire_ChildShapesPublishNoAPIID is #1121's fifth criterion: API_DataSource, API_Resolver
// and API_FunctionConfiguration list no `apiId`, because the API is the path segment the request was
// addressed to rather than data the shape carries — the reading API Gateway v2's Route shape already
// records.
func TestAppSyncWire_ChildShapesPublishNoAPIID(t *testing.T) {
	ts, apiID, _ := appSyncWireAPI(t)
	base := "/v1/apis/" + apiID

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		body    any
		wrapper string
		want    []string
	}{
		{
			name: "CreateDataSource", method: http.MethodPost, path: base + "/datasources",
			body:    map[string]any{"name": "orders", "type": "NONE", "description": "d"},
			wrapper: "dataSource",
			want:    []string{"name", "type", "dataSourceArn", "description"},
		},
		{
			name: "ListDataSources", method: http.MethodGet, path: base + "/datasources",
			wrapper: "dataSources", want: []string{"name", "type", "dataSourceArn"},
		},
		{
			name: "CreateResolver", method: http.MethodPost, path: base + "/types/Query/resolvers",
			body:    map[string]any{"fieldName": "listOrders", "dataSourceName": "orders", "kind": "UNIT"},
			wrapper: "resolver",
			want:    []string{"typeName", "fieldName", "kind", "resolverArn", "dataSourceName"},
		},
		{
			name: "ListResolvers", method: http.MethodGet, path: base + "/types/Query/resolvers",
			wrapper: "resolvers", want: []string{"typeName", "fieldName", "resolverArn"},
		},
		{
			name: "CreateFunction", method: http.MethodPost, path: base + "/functions",
			body:    map[string]any{"name": "enrich", "dataSourceName": "orders"},
			wrapper: "functionConfiguration",
			want:    []string{"functionId", "name", "dataSourceName", "functionArn"},
		},
		{
			name: "ListFunctions", method: http.MethodGet, path: base + "/functions",
			wrapper: "functions", want: []string{"functionId", "name", "functionArn"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw, body := appSyncWire(t, ts, tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusOK, status, string(raw))

			element := body[tc.wrapper]
			if list, ok := element.([]any); ok {
				require.Len(t, list, 1, "%s: %s", tc.name, raw)
				element = list[0]
			}
			obj, ok := element.(map[string]any)
			require.True(t, ok, "%s: no %s object: %s", tc.name, tc.wrapper, raw)

			for _, member := range tc.want {
				assert.Contains(t, obj, member, "%s: %s is published and must be answered", tc.name, member)
			}
			appSyncWireNoBookkeeping(t, tc.name, raw, true)
		})
	}
}

// TestAppSyncWire_CloudFormationStillReportsTheAPIARN is #1121's fourth criterion.
//
// cfn_resources_v31.go reads the ARN out of the plugin's own response rather than rebuilding it, so
// the member rename had to move that reader in the same commit. If it had not, Ref and
// Fn::GetAtt Arn would both answer "" — and a CFN test that asserts only "deploy returned no error"
// would not see it, which is #1123's finding.
func TestAppSyncWire_CloudFormationStillReportsTheAPIARN(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	tmpl := `{"Resources":{
		"API":{"Type":"AWS::AppSync::GraphQLApi","Properties":{
			"Name":"cfn_wire_api","AuthenticationType":"API_KEY"}}},
	 "Outputs":{
		"ApiRef":{"Value":{"Ref":"API"}},
		"ApiArn":{"Value":{"Fn::GetAtt":["API","Arn"]}},
		"ApiID":{"Value":{"Fn::GetAtt":["API","ApiId"]}}}}`

	result, err := d.Deploy(context.Background(), tmpl, "appsync-wire", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)

	apiID := result.Resources[0].PhysicalID
	require.NotEmpty(t, apiID)
	arn := result.Resources[0].ARN
	require.Equal(t, "arn:aws:appsync:us-east-1:123456789012:apis/"+apiID, arn,
		"the deployed resource's ARN comes from the graphqlApi response's arn member")

	assert.Equal(t, arn, result.Outputs["ApiRef"], "Ref on an AWS::AppSync::GraphQLApi is the ARN")
	assert.Equal(t, arn, result.Outputs["ApiArn"])
	assert.Equal(t, apiID, result.Outputs["ApiID"])
	assert.True(t, strings.HasPrefix(arn, "arn:aws:appsync:"), "arn = %q", arn)
}
