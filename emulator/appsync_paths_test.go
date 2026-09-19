package emulator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Path matching is case-sensitive, so a segment spelled in the wrong case is not a cosmetic
// difference — it is a different route. Two of AppSync's were (#1065), which made seven implemented
// operations unreachable through the URIs their own Request Syntax publishes. A test that exercises
// only the spelling substrate happens to match cannot see that, which is why every assertion below
// pairs the published spelling with the one substrate used to match: the first must reach the
// operation and the second must not.
//
// The gates on the api-key tail and the two schema segments are here for the same reason in reverse.
// Lowercasing `apikeys` without gating the tail would have turned an `UpdateApiKey` call from a
// visible 404 into a 200 that mints a second credential, so the refusal is the property under test,
// not an incidental status.

// appSyncPathsAPI creates a GraphQL API on a fresh server and returns both.
func appSyncPathsAPI(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	ts := newAppSyncTestServer(t)
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "paths-api",
		"authenticationType": "API_KEY",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateGraphqlApi: got %d", resp.StatusCode)
	}
	api, _ := appSyncBody(t, resp)["graphqlApi"].(map[string]interface{})
	apiID, _ := api["apiId"].(string)
	if apiID == "" {
		t.Fatal("CreateGraphqlApi answered no apiId")
	}
	return ts, apiID
}

// appSyncPathsCall sends a request and returns the status, the error code and the parsed body.
//
// The code is read from "Code", which is the member substrate's REST-JSON error writer fills, and is
// empty on a success — so one helper serves both halves of every pair below.
func appSyncPathsCall(t *testing.T, ts *httptest.Server, method, path string,
	body any,
) (status int, code string, decoded map[string]interface{}) {
	t.Helper()
	resp := appSyncRequest(t, ts, method, path, body)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	decoded = map[string]interface{}{}
	if len(raw) > 0 {
		if unmarshalErr := json.Unmarshal(raw, &decoded); unmarshalErr != nil {
			t.Fatalf("decode %s %s, %s: %v", method, path, raw, unmarshalErr)
		}
	}
	errCode, _ := decoded["Code"].(string)
	return resp.StatusCode, errCode, decoded
}

// appSyncPathsUnrouted asserts that a method and path resolve to no operation at all.
func appSyncPathsUnrouted(t *testing.T, ts *httptest.Server, method, path string, body any) {
	t.Helper()
	status, code, _ := appSyncPathsCall(t, ts, method, path, body)
	if status != http.StatusNotFound {
		t.Errorf("%s %s: want 404, got %d", method, path, status)
	}
	if code != "UnknownOperationException" {
		t.Errorf("%s %s: want UnknownOperationException, got %q", method, path, code)
	}
}

// TestAppSyncPaths_TheDataSourceSegmentIsSpelledAsItIsPublished walks the five data-source
// operations through the published URI and asserts the old spelling reaches none of them.
func TestAppSyncPaths_TheDataSourceSegmentIsSpelledAsItIsPublished(t *testing.T) {
	ts, apiID := appSyncPathsAPI(t)
	base := "/v1/apis/" + apiID
	published := base + "/datasources"
	// The spelling substrate matched until #1065. AppSync's real endpoint would not accept it either,
	// so it must now resolve to nothing rather than to the operation.
	wrongCase := base + "/DataSources"

	create := map[string]any{"name": "orders", "type": "NONE"}

	status, code, body := appSyncPathsCall(t, ts, http.MethodPost, published, create)
	if status != http.StatusOK {
		t.Fatalf("CreateDataSource: want 200, got %d (%s)", status, code)
	}
	ds, _ := body["dataSource"].(map[string]interface{})
	if ds["name"] != "orders" {
		t.Errorf("CreateDataSource: want name=orders, got %v", ds["name"])
	}

	status, code, body = appSyncPathsCall(t, ts, http.MethodGet, published, nil)
	if status != http.StatusOK {
		t.Fatalf("ListDataSources: want 200, got %d (%s)", status, code)
	}
	if sources, _ := body["dataSources"].([]interface{}); len(sources) != 1 {
		t.Errorf("ListDataSources: want 1 data source, got %d", len(sources))
	}

	status, code, _ = appSyncPathsCall(t, ts, http.MethodGet, published+"/orders", nil)
	if status != http.StatusOK {
		t.Fatalf("GetDataSource: want 200, got %d (%s)", status, code)
	}

	status, code, body = appSyncPathsCall(t, ts, http.MethodPost, published+"/orders",
		map[string]any{"type": "NONE", "description": "updated"})
	if status != http.StatusOK {
		t.Fatalf("UpdateDataSource: want 200, got %d (%s)", status, code)
	}
	if ds, _ = body["dataSource"].(map[string]interface{}); ds["description"] != "updated" {
		t.Errorf("UpdateDataSource: want description=updated, got %v", ds["description"])
	}

	// The wrong case is checked before the delete, so the data source still exists and a route that
	// did resolve would answer 200 rather than a lookup failure — which is what makes the 404 a
	// statement about the route.
	for _, tc := range []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodPost, wrongCase, create},
		{http.MethodGet, wrongCase, nil},
		{http.MethodGet, wrongCase + "/orders", nil},
		{http.MethodPost, wrongCase + "/orders", create},
		{http.MethodDelete, wrongCase + "/orders", nil},
	} {
		appSyncPathsUnrouted(t, ts, tc.method, tc.path, tc.body)
	}

	status, code, _ = appSyncPathsCall(t, ts, http.MethodDelete, published+"/orders", nil)
	if status != http.StatusOK {
		t.Fatalf("DeleteDataSource: want 200, got %d (%s)", status, code)
	}
}

// TestAppSyncPaths_TheAPIKeySegmentIsSpelledAsItIsPublished is the same pair for the two api-key
// operations.
func TestAppSyncPaths_TheAPIKeySegmentIsSpelledAsItIsPublished(t *testing.T) {
	ts, apiID := appSyncPathsAPI(t)
	published := "/v1/apis/" + apiID + "/apikeys"
	wrongCase := "/v1/apis/" + apiID + "/ApiKeys"

	status, code, body := appSyncPathsCall(t, ts, http.MethodPost, published,
		map[string]any{"description": "reader"})
	if status != http.StatusOK {
		t.Fatalf("CreateApiKey: want 200, got %d (%s)", status, code)
	}
	key, _ := body["apiKey"].(map[string]interface{})
	if key["description"] != "reader" {
		t.Errorf("CreateApiKey: want description=reader, got %v", key["description"])
	}

	status, code, body = appSyncPathsCall(t, ts, http.MethodGet, published, nil)
	if status != http.StatusOK {
		t.Fatalf("ListApiKeys: want 200, got %d (%s)", status, code)
	}
	if keys, _ := body["apiKeys"].([]interface{}); len(keys) != 1 {
		t.Errorf("ListApiKeys: want 1 key, got %d", len(keys))
	}

	appSyncPathsUnrouted(t, ts, http.MethodPost, wrongCase, map[string]any{"description": "reader"})
	appSyncPathsUnrouted(t, ts, http.MethodGet, wrongCase, nil)
}

// TestAppSyncPaths_AnUpdateUnderTheAPIKeySegmentDoesNotMintAKey pins the gate that makes the
// lowercasing safe.
//
// UpdateApiKey is POST /v1/apis/{apiId}/apikeys/{id} and DeleteApiKey is DELETE on the same path.
// Neither is implemented, so both must be refused — and the assertion that matters is not the status
// but the key count either side of the call: an ungated arm answered CreateApiKey for any POST under
// the segment, so a caller extending a key's expiry would have been handed a second credential and
// told it was a success.
func TestAppSyncPaths_AnUpdateUnderTheAPIKeySegmentDoesNotMintAKey(t *testing.T) {
	ts, apiID := appSyncPathsAPI(t)
	published := "/v1/apis/" + apiID + "/apikeys"

	_, _, body := appSyncPathsCall(t, ts, http.MethodPost, published, map[string]any{"description": "only"})
	key, _ := body["apiKey"].(map[string]interface{})
	keyID, _ := key["id"].(string)
	if keyID == "" {
		t.Fatal("CreateApiKey answered no key id")
	}

	appSyncPathsUnrouted(t, ts, http.MethodPost, published+"/"+keyID, map[string]any{"expires": 1})
	appSyncPathsUnrouted(t, ts, http.MethodDelete, published+"/"+keyID, nil)

	_, _, body = appSyncPathsCall(t, ts, http.MethodGet, published, nil)
	keys, _ := body["apiKeys"].([]interface{})
	if len(keys) != 1 {
		t.Fatalf("want the one key the create made, got %d — a refused update minted one", len(keys))
	}
	if first, _ := keys[0].(map[string]interface{}); first["id"] != keyID {
		t.Errorf("want key %q, got %v", keyID, first["id"])
	}
}

// TestAppSyncPaths_TheSchemaSegmentsAreGatedOnTheirVerb covers the sharper half of the same class:
// `schemacreation` answered StartSchemaCreation for any method, so a GET performed a write.
func TestAppSyncPaths_TheSchemaSegmentsAreGatedOnTheirVerb(t *testing.T) {
	ts, apiID := appSyncPathsAPI(t)
	base := "/v1/apis/" + apiID

	// A GET on schemacreation is refused, and the refusal is a refusal to *act*: no schema exists
	// afterwards, which GetIntrospectionSchema is the way to observe.
	appSyncPathsUnrouted(t, ts, http.MethodGet, base+"/schemacreation", nil)
	appSyncPathsUnrouted(t, ts, http.MethodDelete, base+"/schemacreation", nil)
	appSyncPathsUnrouted(t, ts, http.MethodPost, base+"/schema", map[string]any{"definition": "x"})
	appSyncPathsUnrouted(t, ts, http.MethodDelete, base+"/schema", nil)

	status, code, body := appSyncPathsCall(t, ts, http.MethodPost, base+"/schemacreation",
		map[string]any{"definition": "type Query { hello: String }"})
	if status != http.StatusOK {
		t.Fatalf("StartSchemaCreation: want 200, got %d (%s)", status, code)
	}
	if body["status"] != "PROCESSING" {
		t.Errorf("StartSchemaCreation: want status=PROCESSING, got %v", body["status"])
	}

	status, code, _ = appSyncPathsCall(t, ts, http.MethodGet, base+"/schema", nil)
	if status != http.StatusOK {
		t.Fatalf("GetIntrospectionSchema: want 200, got %d (%s)", status, code)
	}
}

// TestAppSyncPaths_CloudFormationReachesTheDataSourceBothWays deploys and then deletes an
// `AWS::AppSync::DataSource`, because CloudFormation is the second consumer of the segment and it
// spells the path itself.
//
// Two in-tree sites build the URI rather than going through parseAppSyncOperation —
// cfn_resources_v31.go for the create and cfn_delete.go for the delete — so lowercasing only the
// router would have fixed the SDK path and broken CloudFormation, which is why #1065's fix touches
// all three. The create was already exercised by TestCFN_AppSyncFullStack; the delete was exercised
// nowhere, which is how a wrong-case path could have survived there unnoticed. Asserting the data
// source is gone rather than that DeleteStack returned no error is the point: a delete aimed at an
// unrouted path is a 404 the sweep tolerates, so a clean DeleteStack says nothing.
//
// The template names the API by `Fn::GetAtt ["API", "ApiId"]` and not by `Ref`, because Ref on an
// `AWS::AppSync::GraphQLApi` is documented as the ARN and substrate answers the ARN (#837) — so the
// `Ref` form builds `/v1/apis/arn:aws:appsync:.../datasources` and reaches no operation at all. That
// is what `TestCFN_AppSyncFullStack` does, and it passes because it asserts no per-resource error;
// filed as #1123 rather than fixed here.
func TestAppSyncPaths_CloudFormationReachesTheDataSourceBothWays(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	tmpl := `{"Resources":{
		"API":{"Type":"AWS::AppSync::GraphQLApi","Properties":{
			"Name":"cfn-paths-api","AuthenticationType":"API_KEY"}},
		"DS":{"Type":"AWS::AppSync::DataSource","DependsOn":["API"],"Properties":{
			"ApiId":{"Fn::GetAtt":["API","ApiId"]},"Name":"cfn-orders","Type":"NONE"}}}}`

	result, err := d.Deploy(ctx, tmpl, "appsync-paths", nil)
	require.NoError(t, err)
	byType := make(map[string]emulator.DeployedResource, len(result.Resources))
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "resource %s", r.LogicalID)
		byType[r.Type] = r
	}
	apiID := byType["AWS::AppSync::GraphQLApi"].PhysicalID
	require.NotEmpty(t, apiID, "the API's physical ID is the apiId the data-source path is built from")

	require.Equal(t, http.StatusOK, appSyncPathsGetDataSource(t, d, apiID, "cfn-orders"),
		"the deploy must have created the data source, or the delete proves nothing")

	require.NoError(t, d.DeleteStack(ctx, "appsync-paths"))

	assert.Equal(t, http.StatusNotFound, appSyncPathsGetDataSource(t, d, apiID, "cfn-orders"),
		"the data source dies with its stack")
}

// appSyncPathsGetDataSource reports the status a GetDataSource through the published URI returns.
//
// The status is read off the refusal when there is one, because the AppSync plugin returns its 404 as
// an error rather than as a response — unlike S3's HEAD, which headBucket can read a status from
// directly. Both halves of the assertion above need one number, so the two shapes are collapsed here
// rather than at each call.
func appSyncPathsGetDataSource(t *testing.T, d *emulator.StackDeployer, apiID, name string) int {
	t.Helper()
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "appsync", Operation: "GET",
		Path:    "/v1/apis/" + apiID + "/datasources/" + name,
		Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	if resp != nil {
		return resp.StatusCode
	}
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr, "the probe must reach the plugin and be answered")
	return awsErr.HTTPStatus
}
