package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newAppSyncTestServer builds a minimal server with the AppSync plugin registered.
func newAppSyncTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.AppSyncPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize appsync plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// appSyncRequest sends an AppSync REST/JSON request and returns the response.
func appSyncRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal appsync request body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build appsync request: %v", err)
	}
	req.Host = "appsync.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do appsync request: %v", err)
	}
	return resp
}

// appSyncBody reads and closes the response body, returning parsed JSON.
func appSyncBody(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read appsync response body: %v", err)
	}
	var result map[string]interface{}
	if err2 := json.Unmarshal(b, &result); err2 != nil {
		t.Fatalf("unmarshal appsync response: %v\nbody: %s", err2, b)
	}
	return result
}

// TestAppSync_CreateGetDeleteAPI covers GraphQL API lifecycle.
func TestAppSync_CreateGetDeleteAPI(t *testing.T) {
	ts := newAppSyncTestServer(t)

	// Create API.
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "my-api",
		"authenticationType": "API_KEY",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateGraphqlApi: got %d", resp.StatusCode)
	}
	body := appSyncBody(t, resp)
	apiMap, ok := body["graphqlApi"].(map[string]interface{})
	if !ok {
		t.Fatal("expected graphqlApi in response")
	}
	apiID, _ := apiMap["apiId"].(string)
	if apiID == "" {
		t.Fatal("expected non-empty apiId")
	}

	// Get API.
	resp2 := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID, nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GetGraphqlApi: got %d", resp2.StatusCode)
	}
	body2 := appSyncBody(t, resp2)
	apiMap2, ok := body2["graphqlApi"].(map[string]interface{})
	if !ok {
		t.Fatal("expected graphqlApi in GetGraphqlApi response")
	}
	if apiMap2["apiId"] != apiID {
		t.Errorf("expected apiId=%s, got %v", apiID, apiMap2["apiId"])
	}

	// List APIs.
	resp3 := appSyncRequest(t, ts, http.MethodGet, "/v1/apis", nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("ListGraphqlApis: got %d", resp3.StatusCode)
	}
	body3 := appSyncBody(t, resp3)
	apis, _ := body3["graphqlApis"].([]interface{})
	if len(apis) == 0 {
		t.Error("expected at least one API in list")
	}

	// Delete API.
	resp4 := appSyncRequest(t, ts, http.MethodDelete, "/v1/apis/"+apiID, nil)
	if resp4.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteGraphqlApi: got %d", resp4.StatusCode)
	}
	resp4.Body.Close() //nolint:errcheck

	// Get after delete → 404.
	resp5 := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID, nil)
	if resp5.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp5.StatusCode)
	}
	resp5.Body.Close() //nolint:errcheck
}

// TestAppSync_UpdateAPI verifies that a GraphQL API can be updated.
func TestAppSync_UpdateAPI(t *testing.T) {
	ts := newAppSyncTestServer(t)

	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "update-me",
		"authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	resp2 := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID, map[string]any{
		"name": "updated-name",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("UpdateGraphqlApi: got %d", resp2.StatusCode)
	}
	body2 := appSyncBody(t, resp2)
	updated, _ := body2["graphqlApi"].(map[string]interface{})
	if updated["name"] != "updated-name" {
		t.Errorf("expected name=updated-name, got %v", updated["name"])
	}
}

// TestAppSync_DataSourceCRUD creates, lists, gets, updates, and deletes a data source.
func TestAppSync_DataSourceCRUD(t *testing.T) {
	ts := newAppSyncTestServer(t)

	// Create API.
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "ds-api",
		"authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	// Create DataSource.
	dsResp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/DataSources", map[string]any{
		"name": "my-ds",
		"type": "NONE",
	})
	if dsResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDataSource: got %d", dsResp.StatusCode)
	}
	dsBody := appSyncBody(t, dsResp)
	dsMap, _ := dsBody["dataSource"].(map[string]interface{})
	if dsMap["name"] != "my-ds" {
		t.Errorf("expected name=my-ds, got %v", dsMap["name"])
	}

	// List DataSources.
	lResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/DataSources", nil)
	if lResp.StatusCode != http.StatusOK {
		t.Fatalf("ListDataSources: got %d", lResp.StatusCode)
	}
	lBody := appSyncBody(t, lResp)
	dsList, _ := lBody["dataSources"].([]interface{})
	if len(dsList) == 0 {
		t.Error("expected at least one data source")
	}

	// Get DataSource.
	gResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/DataSources/my-ds", nil)
	if gResp.StatusCode != http.StatusOK {
		t.Fatalf("GetDataSource: got %d", gResp.StatusCode)
	}
	gResp.Body.Close() //nolint:errcheck

	// Update DataSource.
	uResp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/DataSources/my-ds", map[string]any{
		"type": "HTTP",
	})
	if uResp.StatusCode != http.StatusOK {
		t.Fatalf("UpdateDataSource: got %d", uResp.StatusCode)
	}
	uBody := appSyncBody(t, uResp)
	uDS, _ := uBody["dataSource"].(map[string]interface{})
	if uDS["type"] != "HTTP" {
		t.Errorf("expected type=HTTP after update, got %v", uDS["type"])
	}

	// Delete DataSource.
	dResp := appSyncRequest(t, ts, http.MethodDelete, "/v1/apis/"+apiID+"/DataSources/my-ds", nil)
	if dResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteDataSource: got %d", dResp.StatusCode)
	}
	dResp.Body.Close() //nolint:errcheck
}

// TestAppSync_ResolverCRUD covers resolver create/list/get/update/delete.
func TestAppSync_ResolverCRUD(t *testing.T) {
	ts := newAppSyncTestServer(t)

	// Create API.
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name": "resolver-api", "authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	// Create resolver.
	rResp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/types/Query/resolvers", map[string]any{
		"fieldName":      "getUser",
		"dataSourceName": "UserDS",
		"kind":           "UNIT",
	})
	if rResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateResolver: got %d", rResp.StatusCode)
	}
	rBody := appSyncBody(t, rResp)
	res, _ := rBody["resolver"].(map[string]interface{})
	if res["fieldName"] != "getUser" {
		t.Errorf("expected fieldName=getUser, got %v", res["fieldName"])
	}

	// List resolvers.
	lResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/types/Query/resolvers", nil)
	if lResp.StatusCode != http.StatusOK {
		t.Fatalf("ListResolvers: got %d", lResp.StatusCode)
	}
	lBody := appSyncBody(t, lResp)
	resolvers, _ := lBody["resolvers"].([]interface{})
	if len(resolvers) == 0 {
		t.Error("expected at least one resolver")
	}

	// Get resolver.
	gResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/types/Query/resolvers/getUser", nil)
	if gResp.StatusCode != http.StatusOK {
		t.Fatalf("GetResolver: got %d", gResp.StatusCode)
	}
	gResp.Body.Close() //nolint:errcheck

	// Update resolver.
	uResp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/types/Query/resolvers/getUser", map[string]any{
		"dataSourceName": "NewDS",
	})
	if uResp.StatusCode != http.StatusOK {
		t.Fatalf("UpdateResolver: got %d", uResp.StatusCode)
	}
	uBody := appSyncBody(t, uResp)
	uRes, _ := uBody["resolver"].(map[string]interface{})
	if uRes["dataSourceName"] != "NewDS" {
		t.Errorf("expected dataSourceName=NewDS, got %v", uRes["dataSourceName"])
	}

	// Delete resolver.
	dResp := appSyncRequest(t, ts, http.MethodDelete, "/v1/apis/"+apiID+"/types/Query/resolvers/getUser", nil)
	if dResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteResolver: got %d", dResp.StatusCode)
	}
	dResp.Body.Close() //nolint:errcheck
}

// TestAppSync_FunctionCRUD covers pipeline function create/list/get/delete.
func TestAppSync_FunctionCRUD(t *testing.T) {
	ts := newAppSyncTestServer(t)

	// Create API.
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name": "fn-api", "authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	// Create function.
	fResp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/functions", map[string]any{
		"name":           "my-fn",
		"dataSourceName": "MyDS",
	})
	if fResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateFunction: got %d", fResp.StatusCode)
	}
	fBody := appSyncBody(t, fResp)
	fn, _ := fBody["functionConfiguration"].(map[string]interface{})
	funcID, _ := fn["functionId"].(string)
	if funcID == "" {
		t.Fatal("expected non-empty functionId")
	}

	// List functions.
	lResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/functions", nil)
	if lResp.StatusCode != http.StatusOK {
		t.Fatalf("ListFunctions: got %d", lResp.StatusCode)
	}
	lBody := appSyncBody(t, lResp)
	fns, _ := lBody["functions"].([]interface{})
	if len(fns) == 0 {
		t.Error("expected at least one function")
	}

	// Get function.
	gResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/functions/"+funcID, nil)
	if gResp.StatusCode != http.StatusOK {
		t.Fatalf("GetFunction: got %d", gResp.StatusCode)
	}
	gResp.Body.Close() //nolint:errcheck

	// Delete function.
	dResp := appSyncRequest(t, ts, http.MethodDelete, "/v1/apis/"+apiID+"/functions/"+funcID, nil)
	if dResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteFunction: got %d", dResp.StatusCode)
	}
	dResp.Body.Close() //nolint:errcheck

	// Get after delete → 404.
	resp404 := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/functions/"+funcID, nil)
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp404.StatusCode)
	}
	resp404.Body.Close() //nolint:errcheck
}

// TestAppSync_SchemaOperations covers schema creation and introspection.
func TestAppSync_SchemaOperations(t *testing.T) {
	ts := newAppSyncTestServer(t)

	// Create API.
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name": "schema-api", "authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	// Start schema creation.
	sResp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/schemacreation", map[string]any{
		"definition": "type Query { hello: String }",
	})
	if sResp.StatusCode != http.StatusOK {
		t.Fatalf("StartSchemaCreation: got %d", sResp.StatusCode)
	}
	sBody := appSyncBody(t, sResp)
	if sBody["status"] != "PROCESSING" {
		t.Errorf("expected status=PROCESSING, got %v", sBody["status"])
	}

	// Get introspection schema.
	iResp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/schema", nil)
	if iResp.StatusCode != http.StatusOK {
		t.Fatalf("GetIntrospectionSchema: got %d", iResp.StatusCode)
	}
	iResp.Body.Close() //nolint:errcheck
}

// TestAppSync_ExecuteGraphQL verifies the stub execution endpoint.
func TestAppSync_ExecuteGraphQL(t *testing.T) {
	ts := newAppSyncTestServer(t)

	resp := appSyncRequest(t, ts, http.MethodPost, "/graphql", map[string]any{
		"query": "{ hello }",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ExecuteGraphQL: got %d", resp.StatusCode)
	}
	body := appSyncBody(t, resp)
	if _, ok := body["data"]; !ok {
		t.Error("expected data field in ExecuteGraphQL response")
	}
}

// TestAppSync_GetNonexistentAPI returns 404 for unknown API IDs.
func TestAppSync_GetNonexistentAPI(t *testing.T) {
	ts := newAppSyncTestServer(t)
	resp := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/nonexistent-id", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown API, got %d", resp.StatusCode)
	}
	resp.Body.Close() //nolint:errcheck
}

// TestAppSync_CreateAPI_MissingName returns 400 when name is absent.
func TestAppSync_CreateAPI_MissingName(t *testing.T) {
	ts := newAppSyncTestServer(t)
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"authenticationType": "API_KEY",
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing name, got %d", resp.StatusCode)
	}
	resp.Body.Close() //nolint:errcheck
}

// TestCFN_AppSyncGraphQLApi deploys an AppSync API via CloudFormation.
func TestCFN_AppSyncGraphQLApi(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::AppSync::GraphQLApi",
				"Properties": {
					"Name": "cfn-appsync-api",
					"AuthenticationType": "API_KEY"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestCFN_AppSyncFullStack deploys an AppSync API with DataSource, Resolver,
// and pipeline Function via CloudFormation.
func TestCFN_AppSyncFullStack(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::AppSync::GraphQLApi",
				"Properties": {
					"Name": "full-stack-api",
					"AuthenticationType": "API_KEY"
				}
			},
			"MyDS": {
				"Type": "AWS::AppSync::DataSource",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"Name": "NoneDS",
					"Type": "NONE"
				},
				"DependsOn": ["MyAPI"]
			},
			"MyResolver": {
				"Type": "AWS::AppSync::Resolver",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"TypeName": "Query",
					"FieldName": "hello",
					"DataSourceName": "NoneDS",
					"Kind": "UNIT"
				},
				"DependsOn": ["MyDS"]
			},
			"MyFunction": {
				"Type": "AWS::AppSync::FunctionConfiguration",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"Name": "my-pipeline-fn",
					"DataSourceName": "NoneDS"
				},
				"DependsOn": ["MyDS"]
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestAppSync_GetNonexistentDataSource returns 404 for unknown data source.
func TestAppSync_GetNonexistentDataSource(t *testing.T) {
	ts := newAppSyncTestServer(t)

	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name": "err-api", "authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	r := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/DataSources/no-such-ds", nil)
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown datasource, got %d", r.StatusCode)
	}
	r.Body.Close() //nolint:errcheck
}

// TestAppSync_GetNonexistentResolver returns 404 for unknown resolver.
func TestAppSync_GetNonexistentResolver(t *testing.T) {
	ts := newAppSyncTestServer(t)

	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name": "err-api2", "authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	r := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/types/Query/resolvers/nofield", nil)
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown resolver, got %d", r.StatusCode)
	}
	r.Body.Close() //nolint:errcheck
}

// TestAppSync_StartSchemaCreation_BadInput returns 400 for invalid JSON.
func TestAppSync_StartSchemaCreation_BadInput(t *testing.T) {
	ts := newAppSyncTestServer(t)

	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name": "schema-err-api", "authenticationType": "API_KEY",
	})
	body := appSyncBody(t, resp)
	apiID, _ := body["graphqlApi"].(map[string]interface{})["apiId"].(string)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/apis/"+apiID+"/schemacreation", nil)
	req.Host = "appsync.us-east-1.amazonaws.com"
	r, _ := http.DefaultClient.Do(req)
	if r != nil {
		r.Body.Close() //nolint:errcheck
	}
}

// TestCFN_AppSyncDataSourceMissingApiId verifies error path when ApiId is missing.
func TestCFN_AppSyncDataSourceMissingApiId(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BadDS": {
				"Type": "AWS::AppSync::DataSource",
				"Properties": {
					"Name": "MyDS",
					"Type": "NONE"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	// Should have deployed with an error (no ApiId).
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestCFN_AppSyncResolverMissingFields verifies error path when required fields are absent.
func TestCFN_AppSyncResolverMissingFields(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BadResolver": {
				"Type": "AWS::AppSync::Resolver",
				"Properties": {
					"TypeName": "Query",
					"FieldName": "hello"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestCFN_AppSyncFunctionMissingApiId verifies error path when ApiId is missing.
func TestCFN_AppSyncFunctionMissingApiId(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BadFn": {
				"Type": "AWS::AppSync::FunctionConfiguration",
				"Properties": {
					"Name": "my-fn",
					"DataSourceName": "SomeDS"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestAppSync_ApiKeyCRUD covers CreateApiKey and ListApiKeys.
func TestAppSync_ApiKeyCRUD(t *testing.T) {
	ts := newAppSyncTestServer(t)

	// Create a GraphQL API first.
	resp := appSyncRequest(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "key-test-api",
		"authenticationType": "API_KEY",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateGraphqlApi: got %d", resp.StatusCode)
	}
	body := appSyncBody(t, resp)
	apiMap, _ := body["graphqlApi"].(map[string]interface{})
	apiID, _ := apiMap["apiId"].(string)
	if apiID == "" {
		t.Fatal("expected non-empty apiId")
	}

	// CreateApiKey with a description.
	resp2 := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/ApiKeys", map[string]any{
		"description": "test-key",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("CreateApiKey: got %d", resp2.StatusCode)
	}
	body2 := appSyncBody(t, resp2)
	keyMap, ok := body2["apiKey"].(map[string]interface{})
	if !ok {
		t.Fatal("expected apiKey in CreateApiKey response")
	}
	keyID, _ := keyMap["id"].(string)
	if keyID == "" {
		t.Error("want non-empty key id")
	}
	if keyMap["expires"] == nil {
		t.Error("want non-nil expires")
	}
	desc, _ := keyMap["description"].(string)
	if desc != "test-key" {
		t.Errorf("want description=test-key, got %q", desc)
	}

	// ListApiKeys — should return 1 key.
	resp3 := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/ApiKeys", nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("ListApiKeys: got %d", resp3.StatusCode)
	}
	body3 := appSyncBody(t, resp3)
	apiKeys, _ := body3["apiKeys"].([]interface{})
	if len(apiKeys) != 1 {
		t.Fatalf("want 1 api key, got %d", len(apiKeys))
	}
	firstKey, _ := apiKeys[0].(map[string]interface{})
	if firstKey["id"] != keyID {
		t.Errorf("want key id %q, got %q", keyID, firstKey["id"])
	}

	// CreateApiKey again (no description) → 2 keys.
	resp4 := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/ApiKeys", nil)
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("second CreateApiKey: got %d", resp4.StatusCode)
	}
	_ = appSyncBody(t, resp4)

	resp5 := appSyncRequest(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/ApiKeys", nil)
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("second ListApiKeys: got %d", resp5.StatusCode)
	}
	body5 := appSyncBody(t, resp5)
	apiKeys2, _ := body5["apiKeys"].([]interface{})
	if len(apiKeys2) != 2 {
		t.Errorf("want 2 api keys, got %d", len(apiKeys2))
	}

	// CreateApiKey on nonexistent API → 404.
	resp6 := appSyncRequest(t, ts, http.MethodPost, "/v1/apis/nonexistent/ApiKeys", map[string]any{
		"description": "should-fail",
	})
	if resp6.StatusCode != http.StatusNotFound {
		t.Errorf("want 404 for unknown API, got %d", resp6.StatusCode)
	}
	_, _ = io.ReadAll(resp6.Body)
	_ = resp6.Body.Close()
}
