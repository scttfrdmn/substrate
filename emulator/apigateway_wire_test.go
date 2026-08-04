package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// These tests assert on the raw response bytes rather than round-tripping through a
// Go struct, for the reason #529 exists: a struct marshaled and unmarshaled by its
// own definition agrees with itself whatever its tags say, and Go's json.Unmarshal
// matches keys case-insensitively on top of that. So apigateway_plugin_test.go was
// green while `aws apigateway get-rest-apis` returned an empty result and no error.
// Only a literal-key assertion sees the difference.

// agwWire sends one request and decodes the response into a generic map, so every
// assertion is against the key a real SDK would look for.
func agwWire(t *testing.T, p *emulator.APIGatewayPlugin, ctx *emulator.RequestContext,
	method, path string, body map[string]any,
) (map[string]any, []byte) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, apigwRequest(t, method, path, body))
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("%s %s: want 200 or 201, got %d: %s", method, path, resp.StatusCode, resp.Body)
	}
	var m map[string]any
	if err := json.Unmarshal(resp.Body, &m); err != nil {
		t.Fatalf("%s %s: unmarshal body: %v", method, path, err)
	}
	return m, resp.Body
}

// agwAPI creates one REST API and returns its id.
func agwAPI(t *testing.T, p *emulator.APIGatewayPlugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	m, _ := agwWire(t, p, ctx, "POST", "/restapis", map[string]any{
		"name":        name,
		"description": "d-" + name,
		"tags":        map[string]string{"env": "test"},
	})
	id, ok := m["id"].(string)
	if !ok || id == "" {
		t.Fatalf("CreateRestApi: want an id, got %#v", m)
	}
	return id
}

// agwItem pulls the single element out of a v1 collection response. Every v1
// collection nests under "item", singular — "items" parses to nothing.
func agwItem(t *testing.T, what string, m map[string]any) map[string]any {
	t.Helper()
	if _, wrong := m["items"]; wrong {
		t.Fatalf("%s: the envelope is %q, not %q — botocore parses the plural to nothing", what, "item", "items")
	}
	list, ok := m["item"].([]any)
	if !ok {
		t.Fatalf("%s: want an %q list, got %#v", what, "item", m)
	}
	if len(list) != 1 {
		t.Fatalf("%s: want one element, got %d: %#v", what, len(list), list)
	}
	elem, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("%s: want an object element, got %T", what, list[0])
	}
	return elem
}

// agwNoInternalFields checks the raw bytes, not a decoded top-level map: substrate's
// AccountID or Region nested inside a resourceMethods map would pass a top-level key
// check while still reaching the caller. #529 requires no response carry either.
func agwNoInternalFields(t *testing.T, what string, raw []byte) {
	t.Helper()
	for _, bad := range []string{`"AccountID"`, `"Region"`, `"accountId"`, `"region"`, `"APIId"`, `"apiId"`} {
		if bytes.Contains(raw, []byte(bad)) {
			t.Errorf("%s: response carries substrate's internal %s: %s", what, bad, raw)
		}
	}
}

// --- Elements ----------------------------------------------------------------

func TestAPIGatewayWire_RestAPI(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	created, raw := agwWire(t, p, ctx, "POST", "/restapis", map[string]any{
		"name":        "wire-api",
		"description": "a description",
		"tags":        map[string]string{"env": "test"},
	})
	requireValues(t, "CreateRestApi", created, map[string]any{
		"name":        "wire-api",
		"description": "a description",
	})
	requireKeys(t, "CreateRestApi", created, "id", "rootResourceId", "createdDate", "tags")
	agwNoInternalFields(t, "CreateRestApi", raw)
	id := created["id"].(string)

	got, raw := agwWire(t, p, ctx, "GET", "/restapis/"+id, nil)
	requireValues(t, "GetRestApi", got, map[string]any{
		"id":             id,
		"name":           "wire-api",
		"description":    "a description",
		"rootResourceId": created["rootResourceId"],
	})
	if tags, ok := got["tags"].(map[string]any); !ok || tags["env"] != "test" {
		t.Errorf("GetRestApi tags: want env=test, got %#v", got["tags"])
	}
	agwNoInternalFields(t, "GetRestApi", raw)

	patched, raw := agwWire(t, p, ctx, "PATCH", "/restapis/"+id, map[string]any{})
	requireValues(t, "UpdateRestApi", patched, map[string]any{"id": id, "name": "wire-api"})
	agwNoInternalFields(t, "UpdateRestApi", raw)
}

func TestAPIGatewayWire_Resource(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-res")
	root, _ := agwWire(t, p, ctx, "GET", "/restapis/"+apiID, nil)
	rootID := root["rootResourceId"].(string)

	created, raw := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/resources/"+rootID,
		map[string]any{"pathPart": "widgets"})
	requireValues(t, "CreateResource", created, map[string]any{
		"parentId": rootID,
		"pathPart": "widgets",
		"path":     "/widgets",
	})
	requireKeys(t, "CreateResource", created, "id")
	agwNoInternalFields(t, "CreateResource", raw)
	resID := created["id"].(string)

	got, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/resources/"+resID, nil)
	requireValues(t, "GetResource", got, map[string]any{
		"id":       resID,
		"parentId": rootID,
		"pathPart": "widgets",
		"path":     "/widgets",
	})
	agwNoInternalFields(t, "GetResource", raw)
}

// TestAPIGatewayWire_NestedProjection walks the deepest nesting a v1 response has:
// resource → resourceMethods[verb] → methodIntegration. A projection that converts
// only the top level leaves the nested values PascalCase, and a caller parses a
// resource whose method map is populated with empty methods.
func TestAPIGatewayWire_NestedProjection(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-nested")
	root, _ := agwWire(t, p, ctx, "GET", "/restapis/"+apiID, nil)
	rootID := root["rootResourceId"].(string)
	res, _ := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/resources/"+rootID,
		map[string]any{"pathPart": "things"})
	resID := res["id"].(string)

	base := "/restapis/" + apiID + "/resources/" + resID + "/methods/GET"
	method, raw := agwWire(t, p, ctx, "PUT", base, map[string]any{
		"authorizationType": "NONE",
		"apiKeyRequired":    true,
	})
	requireValues(t, "PutMethod", method, map[string]any{
		"httpMethod":        "GET",
		"authorizationType": "NONE",
		"apiKeyRequired":    true,
	})
	agwNoInternalFields(t, "PutMethod", raw)

	integration, raw := agwWire(t, p, ctx, "PUT", base+"/integration", map[string]any{
		"type":       "AWS_PROXY",
		"uri":        "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/f/invocations",
		"httpMethod": "POST",
	})
	requireValues(t, "PutIntegration", integration, map[string]any{
		"type":       "AWS_PROXY",
		"uri":        "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/f/invocations",
		"httpMethod": "POST",
	})
	agwNoInternalFields(t, "PutIntegration", raw)

	// Depth 1: the method, fetched on its own, carries its integration.
	got, raw := agwWire(t, p, ctx, "GET", base, nil)
	mi, ok := got["methodIntegration"].(map[string]any)
	if !ok {
		t.Fatalf("GetMethod: want a methodIntegration object, got %#v", got["methodIntegration"])
	}
	requireValues(t, "GetMethod.methodIntegration", mi, map[string]any{
		"type":       "AWS_PROXY",
		"httpMethod": "POST",
	})
	agwNoInternalFields(t, "GetMethod", raw)

	// Depth 2 and 3: the resource carries the method map, and each method carries
	// its integration.
	gotRes, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/resources/"+resID, nil)
	rm, ok := gotRes["resourceMethods"].(map[string]any)
	if !ok {
		t.Fatalf("GetResource: want a resourceMethods map, got %#v", gotRes["resourceMethods"])
	}
	nested, ok := rm["GET"].(map[string]any)
	if !ok {
		t.Fatalf("resourceMethods[GET]: want an object, got %T", rm["GET"])
	}
	requireValues(t, "resourceMethods[GET]", nested, map[string]any{
		"httpMethod":        "GET",
		"authorizationType": "NONE",
		"apiKeyRequired":    true,
	})
	deep, ok := nested["methodIntegration"].(map[string]any)
	if !ok {
		t.Fatalf("resourceMethods[GET].methodIntegration: want an object, got %#v", nested["methodIntegration"])
	}
	requireValues(t, "resourceMethods[GET].methodIntegration", deep, map[string]any{
		"type": "AWS_PROXY",
		"uri":  "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/f/invocations",
	})
	agwNoInternalFields(t, "GetResource nested", raw)
}

// TestAPIGatewayWire_CallerSuppliedResponsesPassThrough pins that a method
// response's and integration response's contents are the caller's own keys, echoed
// back rather than renamed. Substrate does not model their shape.
func TestAPIGatewayWire_CallerSuppliedResponsesPassThrough(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-resp")
	root, _ := agwWire(t, p, ctx, "GET", "/restapis/"+apiID, nil)
	rootID := root["rootResourceId"].(string)
	base := "/restapis/" + apiID + "/resources/" + rootID + "/methods/GET"
	agwWire(t, p, ctx, "PUT", base, map[string]any{"authorizationType": "NONE"})

	got, _ := agwWire(t, p, ctx, "PUT", base+"/responses/200", map[string]any{
		"statusCode":     "200",
		"responseModels": map[string]any{"application/json": "Empty"},
	})
	requireValues(t, "PutMethodResponse", got, map[string]any{"statusCode": "200"})
	if rm, ok := got["responseModels"].(map[string]any); !ok || rm["application/json"] != "Empty" {
		t.Errorf("PutMethodResponse: caller's responseModels must echo back, got %#v", got["responseModels"])
	}
}

func TestAPIGatewayWire_Deployment(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-dep")

	created, raw := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/deployments",
		map[string]any{"description": "first"})
	requireValues(t, "CreateDeployment", created, map[string]any{"description": "first"})
	requireKeys(t, "CreateDeployment", created, "id", "createdDate")
	agwNoInternalFields(t, "CreateDeployment", raw)
	depID := created["id"].(string)

	got, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/deployments/"+depID, nil)
	requireValues(t, "GetDeployment", got, map[string]any{"id": depID, "description": "first"})
	agwNoInternalFields(t, "GetDeployment", raw)
}

func TestAPIGatewayWire_Stage(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-stage")
	dep, _ := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/deployments", map[string]any{})
	depID := dep["id"].(string)

	created, raw := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/stages", map[string]any{
		"stageName":    "prod",
		"deploymentId": depID,
		"description":  "production",
		"variables":    map[string]string{"k": "v"},
	})
	requireValues(t, "CreateStage", created, map[string]any{
		"stageName":    "prod",
		"deploymentId": depID,
		"description":  "production",
	})
	requireKeys(t, "CreateStage", created, "createdDate", "variables")
	agwNoInternalFields(t, "CreateStage", raw)

	got, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/stages/prod", nil)
	requireValues(t, "GetStage", got, map[string]any{
		"stageName":    "prod",
		"deploymentId": depID,
		"invokeUrl":    "https://" + apiID + ".execute-api.us-east-1.amazonaws.com/prod",
	})
	// The old spelling was InvokeUrl, PascalCase like everything else here.
	if _, ok := got["InvokeUrl"]; ok {
		t.Error("GetStage: the key is invokeUrl, not InvokeUrl")
	}
	agwNoInternalFields(t, "GetStage", raw)

	patched, raw := agwWire(t, p, ctx, "PATCH", "/restapis/"+apiID+"/stages/prod", map[string]any{})
	requireValues(t, "UpdateStage", patched, map[string]any{"stageName": "prod"})
	agwNoInternalFields(t, "UpdateStage", raw)
}

func TestAPIGatewayWire_Authorizer(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-auth")

	created, raw := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/authorizers", map[string]any{
		"name":           "my-auth",
		"type":           "TOKEN",
		"authorizerUri":  "arn:aws:apigateway:us-east-1:lambda:path/f",
		"identitySource": "method.request.header.Authorization",
		"providerARNs":   []string{"arn:aws:cognito-idp:us-east-1:123456789012:userpool/p"},
	})
	requireValues(t, "CreateAuthorizer", created, map[string]any{
		"name":           "my-auth",
		"type":           "TOKEN",
		"authorizerUri":  "arn:aws:apigateway:us-east-1:lambda:path/f",
		"identitySource": "method.request.header.Authorization",
	})
	requireKeys(t, "CreateAuthorizer", created, "id", "providerARNs")
	// providerARNs keeps its capitalised acronym; providerArns is not the member.
	if _, ok := created["providerArns"]; ok {
		t.Error("CreateAuthorizer: the member is providerARNs, not providerArns")
	}
	agwNoInternalFields(t, "CreateAuthorizer", raw)
	authID := created["id"].(string)

	got, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/authorizers/"+authID, nil)
	requireValues(t, "GetAuthorizer", got, map[string]any{"id": authID, "name": "my-auth", "type": "TOKEN"})
	agwNoInternalFields(t, "GetAuthorizer", raw)
}

func TestAPIGatewayWire_APIKey(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	created, raw := agwWire(t, p, ctx, "POST", "/apikeys", map[string]any{
		"name":    "my-key",
		"enabled": true,
		"tags":    map[string]string{"env": "test"},
	})
	requireValues(t, "CreateApiKey", created, map[string]any{"name": "my-key", "enabled": true})
	requireKeys(t, "CreateApiKey", created, "id", "value", "createdDate", "tags")
	// Assert the value is non-empty, not merely equal to itself on both reads: a
	// projection that drops it still emits the key holding "", and comparing the two
	// responses' values would then compare "" with "" and pass.
	value, _ := created["value"].(string)
	if value == "" {
		t.Error("CreateApiKey: value must be a non-empty key material")
	}
	agwNoInternalFields(t, "CreateApiKey", raw)
	keyID := created["id"].(string)

	got, raw := agwWire(t, p, ctx, "GET", "/apikeys/"+keyID, nil)
	requireValues(t, "GetApiKey", got, map[string]any{
		"id":      keyID,
		"name":    "my-key",
		"enabled": true,
		"value":   value,
	})
	agwNoInternalFields(t, "GetApiKey", raw)
}

func TestAPIGatewayWire_UsagePlan(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	created, raw := agwWire(t, p, ctx, "POST", "/usageplans", map[string]any{
		"name":        "my-plan",
		"description": "a plan",
		"tags":        map[string]string{"env": "test"},
	})
	requireValues(t, "CreateUsagePlan", created, map[string]any{
		"name":        "my-plan",
		"description": "a plan",
	})
	requireKeys(t, "CreateUsagePlan", created, "id", "tags")
	// UsagePlan declares no createdDate, so substrate must not report one even
	// though it stores one.
	if _, ok := created["createdDate"]; ok {
		t.Error("CreateUsagePlan: createdDate is not a member of UsagePlan")
	}
	agwNoInternalFields(t, "CreateUsagePlan", raw)
	planID := created["id"].(string)

	got, raw := agwWire(t, p, ctx, "GET", "/usageplans/"+planID, nil)
	requireValues(t, "GetUsagePlan", got, map[string]any{"id": planID, "name": "my-plan"})
	agwNoInternalFields(t, "GetUsagePlan", raw)
}

func TestAPIGatewayWire_DomainNameAndBasePathMapping(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-domain")

	created, raw := agwWire(t, p, ctx, "POST", "/domainnames", map[string]any{
		"domainName":     "api.example.com",
		"certificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/c",
	})
	requireValues(t, "CreateDomainName", created, map[string]any{
		"domainName":         "api.example.com",
		"certificateArn":     "arn:aws:acm:us-east-1:123456789012:certificate/c",
		"regionalDomainName": "api.example.com.regional.execute-api.us-east-1.amazonaws.com",
	})
	agwNoInternalFields(t, "CreateDomainName", raw)

	got, raw := agwWire(t, p, ctx, "GET", "/domainnames/api.example.com", nil)
	requireValues(t, "GetDomainName", got, map[string]any{"domainName": "api.example.com"})
	agwNoInternalFields(t, "GetDomainName", raw)

	mapping, raw := agwWire(t, p, ctx, "POST", "/domainnames/api.example.com/basepathmappings",
		map[string]any{"basePath": "v1", "restApiId": apiID, "stage": "prod"})
	requireValues(t, "CreateBasePathMapping", mapping, map[string]any{
		"basePath":  "v1",
		"restApiId": apiID,
		"stage":     "prod",
	})
	// BasePathMapping declares no domainName: the domain is a path parameter of the
	// request, not a member of the response.
	if _, ok := mapping["domainName"]; ok {
		t.Error("CreateBasePathMapping: domainName is not a member of BasePathMapping")
	}
	agwNoInternalFields(t, "CreateBasePathMapping", raw)
}

// --- Envelopes ---------------------------------------------------------------
//
// One test per list operation. A single test covering all eight would pass while
// seven of them were wrong.

func TestAPIGatewayWire_GetRestAPIsEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	id := agwAPI(t, p, ctx, "wire-list-api")

	m, raw := agwWire(t, p, ctx, "GET", "/restapis", nil)
	elem := agwItem(t, "GetRestApis", m)
	requireValues(t, "GetRestApis element", elem, map[string]any{"id": id, "name": "wire-list-api"})
	agwNoInternalFields(t, "GetRestApis", raw)
}

func TestAPIGatewayWire_GetResourcesEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-list-res")

	m, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/resources", nil)
	elem := agwItem(t, "GetResources", m)
	requireValues(t, "GetResources element", elem, map[string]any{"path": "/"})
	agwNoInternalFields(t, "GetResources", raw)
}

func TestAPIGatewayWire_GetDeploymentsEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-list-dep")
	dep, _ := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/deployments", map[string]any{"description": "d"})

	m, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/deployments", nil)
	elem := agwItem(t, "GetDeployments", m)
	requireValues(t, "GetDeployments element", elem, map[string]any{"id": dep["id"], "description": "d"})
	agwNoInternalFields(t, "GetDeployments", raw)
}

// TestAPIGatewayWire_GetStagesEnvelope is the regression gate for the one handler
// that was already right. GetStages' model member is named "item" where the other
// nineteen collections are named "items" with locationName "item" — so a blanket
// rename to match the others breaks the only v1 list an SDK could already parse.
func TestAPIGatewayWire_GetStagesEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-list-stage")
	agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/stages", map[string]any{"stageName": "prod"})

	m, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/stages", nil)
	elem := agwItem(t, "GetStages", m)
	requireValues(t, "GetStages element", elem, map[string]any{"stageName": "prod"})
	agwNoInternalFields(t, "GetStages", raw)
}

func TestAPIGatewayWire_GetAuthorizersEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-list-auth")
	agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/authorizers",
		map[string]any{"name": "a", "type": "TOKEN"})

	m, raw := agwWire(t, p, ctx, "GET", "/restapis/"+apiID+"/authorizers", nil)
	elem := agwItem(t, "GetAuthorizers", m)
	requireValues(t, "GetAuthorizers element", elem, map[string]any{"name": "a", "type": "TOKEN"})
	agwNoInternalFields(t, "GetAuthorizers", raw)
}

func TestAPIGatewayWire_GetAPIKeysEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	agwWire(t, p, ctx, "POST", "/apikeys", map[string]any{"name": "k", "enabled": true})

	m, raw := agwWire(t, p, ctx, "GET", "/apikeys", nil)
	elem := agwItem(t, "GetApiKeys", m)
	requireValues(t, "GetApiKeys element", elem, map[string]any{"name": "k", "enabled": true})
	agwNoInternalFields(t, "GetApiKeys", raw)
}

func TestAPIGatewayWire_GetUsagePlansEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	agwWire(t, p, ctx, "POST", "/usageplans", map[string]any{"name": "p"})

	m, raw := agwWire(t, p, ctx, "GET", "/usageplans", nil)
	elem := agwItem(t, "GetUsagePlans", m)
	requireValues(t, "GetUsagePlans element", elem, map[string]any{"name": "p"})
	agwNoInternalFields(t, "GetUsagePlans", raw)
}

func TestAPIGatewayWire_GetBasePathMappingsEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-list-bpm")
	agwWire(t, p, ctx, "POST", "/domainnames", map[string]any{"domainName": "api.example.com"})
	agwWire(t, p, ctx, "POST", "/domainnames/api.example.com/basepathmappings",
		map[string]any{"basePath": "v1", "restApiId": apiID, "stage": "prod"})

	m, raw := agwWire(t, p, ctx, "GET", "/domainnames/api.example.com/basepathmappings", nil)
	elem := agwItem(t, "GetBasePathMappings", m)
	requireValues(t, "GetBasePathMappings element", elem, map[string]any{"basePath": "v1", "restApiId": apiID})
	agwNoInternalFields(t, "GetBasePathMappings", raw)
}

// TestAPIGatewayWire_EmptyListsAreLists pins the empty case across every collection
// as a list rather than null: a caller iterating the result must not have to handle
// a JSON null. It also asserts no "position" token is invented — substrate returns
// every element in one page and honors no token, and an empty one would invite a
// caller to page on nothing.
func TestAPIGatewayWire_EmptyListsAreLists(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	apiID := agwAPI(t, p, ctx, "wire-empty")
	agwWire(t, p, ctx, "POST", "/domainnames", map[string]any{"domainName": "api.example.com"})

	for _, tc := range []struct{ name, path string }{
		{"GetDeployments", "/restapis/" + apiID + "/deployments"},
		{"GetStages", "/restapis/" + apiID + "/stages"},
		{"GetAuthorizers", "/restapis/" + apiID + "/authorizers"},
		{"GetApiKeys", "/apikeys"},
		{"GetUsagePlans", "/usageplans"},
		{"GetBasePathMappings", "/domainnames/api.example.com/basepathmappings"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, raw := agwWire(t, p, ctx, "GET", tc.path, nil)
			list, ok := m["item"].([]any)
			if !ok {
				t.Fatalf("%s: want an empty %q list, got %#v", tc.name, "item", m)
			}
			if len(list) != 0 {
				t.Errorf("%s: want 0 elements, got %d", tc.name, len(list))
			}
			if _, ok := m["position"]; ok {
				t.Errorf("%s: substrate returns one page, so no position token must be sent", tc.name)
			}
			if bytes.Contains(raw, []byte("null")) {
				t.Errorf("%s: response contains a JSON null: %s", tc.name, raw)
			}
		})
	}
}

// TestAPIGatewayWire_UnsetOptionalsAreOmitted pins absent-versus-null. Real API
// Gateway omits an unset member; substrate used to send "description": "" and
// "tags": null because it marshaled a state struct whose optional members lacked
// omitempty, and a caller distinguishing the two reads a real observable.
func TestAPIGatewayWire_UnsetOptionalsAreOmitted(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	// No description, no tags.
	created, raw := agwWire(t, p, ctx, "POST", "/restapis", map[string]any{"name": "sparse"})
	if _, ok := created["description"]; ok {
		t.Error("description: an unset optional must be omitted, not sent empty")
	}
	if _, ok := created["tags"]; ok {
		t.Error("tags: an unset map must be omitted, not sent as null")
	}
	if bytes.Contains(raw, []byte("null")) {
		t.Errorf("response contains a JSON null: %s", raw)
	}

	apiID := created["id"].(string)
	root, _ := agwWire(t, p, ctx, "GET", "/restapis/"+apiID, nil)
	rootID := root["rootResourceId"].(string)

	// A method with no integration must omit methodIntegration rather than nulling it.
	base := "/restapis/" + apiID + "/resources/" + rootID + "/methods/GET"
	agwWire(t, p, ctx, "PUT", base, map[string]any{"authorizationType": "NONE"})
	method, raw := agwWire(t, p, ctx, "GET", base, nil)
	if _, ok := method["methodIntegration"]; ok {
		t.Error("methodIntegration: a method with no integration must omit it, not send null")
	}
	if bytes.Contains(raw, []byte("null")) {
		t.Errorf("GetMethod response contains a JSON null: %s", raw)
	}

	// A resource with no methods must omit resourceMethods rather than sending {}.
	res, _ := agwWire(t, p, ctx, "POST", "/restapis/"+apiID+"/resources/"+rootID,
		map[string]any{"pathPart": "bare"})
	if _, ok := res["resourceMethods"]; ok {
		t.Error("resourceMethods: a resource with no methods must omit the map")
	}
}

// TestAPIGatewayWire_StateEncodingUnchanged makes "state encoding is unchanged" a
// fact rather than a claim. The stored bytes are a persisted format that recorded
// runs replay from, so they keep their PascalCase keys even though the wire is
// lowerCamel — including the AccountID and Region that must never reach a caller.
// This is the test that fails if someone later fixes a casing bug by retagging the
// state struct instead of the wire type.
func TestAPIGatewayWire_StateEncodingUnchanged(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := &emulator.APIGatewayPlugin{}
	if err := p.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: emulator.NewDefaultLogger(0, false),
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}

	resp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/restapis", map[string]any{"name": "state-shape"}))
	if err != nil {
		t.Fatalf("CreateRestApi: %v", err)
	}
	var created map[string]any
	if err := json.Unmarshal(resp.Body, &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	apiID := created["id"].(string)

	data, err := state.Get(context.Background(), "apigateway", "api:123456789012/us-east-1/"+apiID)
	if err != nil || data == nil {
		t.Fatalf("state.Get: %v (data=%v)", err, data)
	}
	var stored map[string]any
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("unmarshal stored api: %v", err)
	}
	for _, k := range []string{"Id", "Name", "RootResourceId", "CreatedDate", "AccountID", "Region"} {
		if _, ok := stored[k]; !ok {
			t.Errorf("stored api: key %q is missing; the persisted format must not change", k)
		}
	}
	// And the wire spelling must NOT appear in state, or the two have been conflated.
	for _, k := range []string{"id", "name", "rootResourceId"} {
		if _, ok := stored[k]; ok {
			t.Errorf("stored api: wire spelling %q found in the persisted format", k)
		}
	}
}
