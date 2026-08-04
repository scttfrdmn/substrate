package emulator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// These tests assert on the literal JSON keys API Gateway v2 puts on the wire,
// which is the only thing an AWS SDK can parse (#529). A Go round-trip through
// the plugin's own state struct agrees with itself whatever its tags say —
// encoding/json matches keys case-insensitively — so every assertion here decodes
// into map[string]any and names the wire key.
//
// requireKeys and requireValues come from msk_wire_test.go. requireValues is what
// most of these use: a projection that forgets a member still emits its key
// holding Go's zero value, so presence proves nothing.

// agwv2Wire issues a request and decodes the response body into a map, accepting
// either 200 or 201.
func agwv2Wire(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext,
	method, path string, body map[string]any) (map[string]any, []byte) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, apigwv2Request(t, method, path, body))
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("%s %s: want 200 or 201, got %d", method, path, resp.StatusCode)
	}
	var m map[string]any
	if err := json.Unmarshal(resp.Body, &m); err != nil {
		t.Fatalf("%s %s: unmarshal: %v", method, path, err)
	}
	return m, resp.Body
}

// agwv2API creates an API and returns its wire apiId.
func agwv2API(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	m, _ := agwv2Wire(t, p, ctx, "POST", "/v2/apis", map[string]any{
		"Name": name, "ProtocolType": "HTTP",
	})
	id, _ := m["apiId"].(string)
	if id == "" {
		t.Fatalf("CreateApi %q: no apiId on the wire; got %v", name, m)
	}
	return id
}

// agwv2Items pulls a collection out of a v2 list response, failing explicitly if
// the envelope is the PascalCase "Items" that parses to nothing.
func agwv2Items(t *testing.T, what string, m map[string]any) []any {
	t.Helper()
	if _, bad := m["Items"]; bad {
		t.Fatalf("%s: envelope is %q, which an SDK parses to nothing; want %q", what, "Items", "items")
	}
	items, ok := m["items"].([]any)
	if !ok {
		t.Fatalf("%s: want a list under %q, got %#v", what, "items", m["items"])
	}
	return items
}

// agwv2NoInternalFields asserts substrate's own state members are nowhere in a
// response, at the byte level so a nested occurrence cannot hide from a top-level
// map check.
func agwv2NoInternalFields(t *testing.T, what string, raw []byte) {
	t.Helper()
	for _, leak := range []string{`"AccountID"`, `"Region"`, `"accountId"`, `"region"`} {
		if bytes.Contains(raw, []byte(leak)) {
			t.Errorf("%s: response leaks %s: %s", what, leak, raw)
		}
	}
}

func TestAPIGatewayV2Wire_Api(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)

	created, raw := agwv2Wire(t, p, ctx, "POST", "/v2/apis", map[string]any{
		"Name": "wire-api", "ProtocolType": "HTTP", "Description": "a description",
	})
	requireKeys(t, "CreateApi", created, "apiId", "name", "protocolType", "apiEndpoint", "createdDate")
	requireValues(t, "CreateApi", created, map[string]any{
		"name":         "wire-api",
		"protocolType": "HTTP",
		"description":  "a description",
	})
	agwv2NoInternalFields(t, "CreateApi", raw)

	apiID, _ := created["apiId"].(string)
	if apiID == "" {
		t.Fatal("CreateApi: apiId is empty")
	}
	endpoint, _ := created["apiEndpoint"].(string)
	if !strings.HasPrefix(endpoint, "https://") || !strings.Contains(endpoint, "execute-api") {
		t.Errorf("CreateApi: apiEndpoint = %q, want an https execute-api host", endpoint)
	}

	got, rawGet := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID, nil)
	requireValues(t, "GetApi", got, map[string]any{
		"apiId": apiID, "name": "wire-api", "protocolType": "HTTP",
	})
	agwv2NoInternalFields(t, "GetApi", rawGet)

	patched, _ := agwv2Wire(t, p, ctx, "PATCH", "/v2/apis/"+apiID, map[string]any{
		"Description": "updated",
	})
	requireValues(t, "UpdateApi", patched, map[string]any{
		"apiId": apiID, "description": "updated",
	})
}

func TestAPIGatewayV2Wire_Route(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "route-wire")

	created, raw := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/routes", map[string]any{
		"RouteKey": "GET /users", "Target": "integrations/abc123",
		"AuthorizationType": "NONE",
	})
	requireValues(t, "CreateRoute", created, map[string]any{
		"routeKey":          "GET /users",
		"target":            "integrations/abc123",
		"authorizationType": "NONE",
	})
	agwv2NoInternalFields(t, "CreateRoute", raw)

	// The Route shape declares no apiId — the API is a path parameter, not a
	// response member. botocore drops the key, so reporting it is at best noise.
	if _, ok := created["apiId"]; ok {
		t.Error("CreateRoute: apiId is not a member of the Route shape")
	}

	routeID, _ := created["routeId"].(string)
	if routeID == "" {
		t.Fatalf("CreateRoute: no routeId on the wire; got %v", created)
	}

	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/routes/"+routeID, nil)
	requireValues(t, "GetRoute", got, map[string]any{
		"routeId": routeID, "routeKey": "GET /users",
	})
}

func TestAPIGatewayV2Wire_Integration(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "int-wire")

	created, raw := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/integrations", map[string]any{
		"IntegrationType":      "AWS_PROXY",
		"IntegrationUri":       "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		"PayloadFormatVersion": "2.0",
	})
	requireValues(t, "CreateIntegration", created, map[string]any{
		"integrationType":      "AWS_PROXY",
		"integrationUri":       "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		"payloadFormatVersion": "2.0",
	})
	agwv2NoInternalFields(t, "CreateIntegration", raw)

	intID, _ := created["integrationId"].(string)
	if intID == "" {
		t.Fatalf("CreateIntegration: no integrationId on the wire; got %v", created)
	}

	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/integrations/"+intID, nil)
	requireValues(t, "GetIntegration", got, map[string]any{
		"integrationId": intID, "integrationType": "AWS_PROXY",
	})
}

func TestAPIGatewayV2Wire_Stage(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "stage-wire")

	created, raw := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/stages", map[string]any{
		"StageName": "dev", "DeploymentId": "dep123", "Description": "development",
		"StageVariables": map[string]any{"lang": "go"},
	})
	requireKeys(t, "CreateStage", created, "stageName", "createdDate", "stageVariables")
	requireValues(t, "CreateStage", created, map[string]any{
		"stageName":    "dev",
		"deploymentId": "dep123",
		"description":  "development",
	})
	agwv2NoInternalFields(t, "CreateStage", raw)

	vars, ok := created["stageVariables"].(map[string]any)
	if !ok || vars["lang"] != "go" {
		t.Errorf("CreateStage: stageVariables = %#v, want {lang: go}", created["stageVariables"])
	}

	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/stages/dev", nil)
	requireValues(t, "GetStage", got, map[string]any{
		"stageName": "dev", "deploymentId": "dep123",
	})
}

func TestAPIGatewayV2Wire_Authorizer(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "auth-wire")

	created, raw := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/authorizers", map[string]any{
		"Name": "my-jwt", "AuthorizerType": "JWT",
		"IdentitySource": []string{"$request.header.Authorization"},
		"JwtConfiguration": map[string]any{
			"Audience": []string{"my-client"}, "Issuer": "https://example.com",
		},
	})
	requireValues(t, "CreateAuthorizer", created, map[string]any{
		"name": "my-jwt", "authorizerType": "JWT",
	})
	agwv2NoInternalFields(t, "CreateAuthorizer", raw)

	// The model spells the member jwtConfiguration, whatever its shape is called.
	if _, ok := created["jwtConfiguration"]; !ok {
		t.Errorf("CreateAuthorizer: missing jwtConfiguration; got %v", created)
	}
	src, ok := created["identitySource"].([]any)
	if !ok || len(src) != 1 || src[0] != "$request.header.Authorization" {
		t.Errorf("CreateAuthorizer: identitySource = %#v", created["identitySource"])
	}

	authID, _ := created["authorizerId"].(string)
	if authID == "" {
		t.Fatalf("CreateAuthorizer: no authorizerId on the wire; got %v", created)
	}
	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/authorizers/"+authID, nil)
	requireValues(t, "GetAuthorizer", got, map[string]any{
		"authorizerId": authID, "name": "my-jwt",
	})
}

func TestAPIGatewayV2Wire_Deployment(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "dep-wire")

	created, raw := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/deployments", map[string]any{
		"Description": "initial",
	})
	requireKeys(t, "CreateDeployment", created, "deploymentId", "deploymentStatus", "createdDate")
	requireValues(t, "CreateDeployment", created, map[string]any{
		"deploymentStatus": "DEPLOYED", "description": "initial",
	})
	agwv2NoInternalFields(t, "CreateDeployment", raw)

	depID, _ := created["deploymentId"].(string)
	if depID == "" {
		t.Fatalf("CreateDeployment: no deploymentId on the wire; got %v", created)
	}
	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/deployments/"+depID, nil)
	requireValues(t, "GetDeployment", got, map[string]any{
		"deploymentId": depID, "deploymentStatus": "DEPLOYED",
	})
}

// TestAPIGatewayV2Wire_DomainNameAndApiMapping pins where v2 reports the regional
// hostname. v1 has a top-level regionalDomainName; v2's DomainName shape has no
// such member and nests apiGatewayDomainName inside domainNameConfigurations, so a
// top-level spelling would be dropped by botocore and invisible to a caller.
func TestAPIGatewayV2Wire_DomainNameAndApiMapping(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "mapping-wire")

	dn, rawDN := agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{
		"DomainName": "v2.example.com",
	})
	requireValues(t, "CreateDomainName", dn, map[string]any{"domainName": "v2.example.com"})
	agwv2NoInternalFields(t, "CreateDomainName", rawDN)

	if _, ok := dn["regionalDomainName"]; ok {
		t.Error("CreateDomainName: regionalDomainName is v1's member, not a v2 one")
	}
	cfgs, ok := dn["domainNameConfigurations"].([]any)
	if !ok || len(cfgs) != 1 {
		t.Fatalf("CreateDomainName: domainNameConfigurations = %#v", dn["domainNameConfigurations"])
	}
	cfg, ok := cfgs[0].(map[string]any)
	if !ok {
		t.Fatalf("CreateDomainName: configuration element = %#v", cfgs[0])
	}
	requireValues(t, "CreateDomainName config", cfg, map[string]any{
		"endpointType":     "REGIONAL",
		"domainNameStatus": "AVAILABLE",
	})
	host, _ := cfg["apiGatewayDomainName"].(string)
	if !strings.Contains(host, "v2.example.com") {
		t.Errorf("CreateDomainName: apiGatewayDomainName = %q, want the domain in it", host)
	}

	got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/v2.example.com", nil)
	requireValues(t, "GetDomainName", got, map[string]any{"domainName": "v2.example.com"})

	mapping, rawMap := agwv2Wire(t, p, ctx, "POST",
		"/v2/domainnames/v2.example.com/apimappings", map[string]any{
			"ApiId": apiID, "Stage": "$default", "ApiMappingKey": "v1",
		})
	requireValues(t, "CreateApiMapping", mapping, map[string]any{
		"apiId": apiID, "stage": "$default", "apiMappingKey": "v1",
	})
	agwv2NoInternalFields(t, "CreateApiMapping", rawMap)
	if id, _ := mapping["apiMappingId"].(string); id == "" {
		t.Errorf("CreateApiMapping: no apiMappingId on the wire; got %v", mapping)
	}
	// The ApiMapping shape declares no domainName, for the same reason a route
	// declares no apiId.
	if _, ok := mapping["domainName"]; ok {
		t.Error("CreateApiMapping: domainName is not a member of the ApiMapping shape")
	}
}

// The five list operations get one test each. A single test covering all five
// would pass while four were wrong.

func TestAPIGatewayV2Wire_GetApisEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	agwv2API(t, p, ctx, "alpha")
	agwv2API(t, p, ctx, "beta")

	m, raw := agwv2Wire(t, p, ctx, "GET", "/v2/apis", nil)
	items := agwv2Items(t, "GetApis", m)
	if len(items) != 2 {
		t.Fatalf("GetApis: want 2 items, got %d", len(items))
	}
	agwv2NoInternalFields(t, "GetApis", raw)
	for i, it := range items {
		el, ok := it.(map[string]any)
		if !ok {
			t.Fatalf("GetApis: item %d = %#v", i, it)
		}
		requireKeys(t, "GetApis element", el, "apiId", "name", "protocolType")
		if el["name"] == "" {
			t.Errorf("GetApis: item %d has an empty name", i)
		}
	}
}

func TestAPIGatewayV2Wire_GetRoutesEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "routes-env")
	agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/routes", map[string]any{"RouteKey": "GET /a"})

	m, raw := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/routes", nil)
	items := agwv2Items(t, "GetRoutes", m)
	if len(items) != 1 {
		t.Fatalf("GetRoutes: want 1 item, got %d", len(items))
	}
	el, _ := items[0].(map[string]any)
	requireValues(t, "GetRoutes element", el, map[string]any{"routeKey": "GET /a"})
	agwv2NoInternalFields(t, "GetRoutes", raw)
}

func TestAPIGatewayV2Wire_GetIntegrationsEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "ints-env")
	agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/integrations", map[string]any{
		"IntegrationType": "MOCK",
	})

	m, raw := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/integrations", nil)
	items := agwv2Items(t, "GetIntegrations", m)
	if len(items) != 1 {
		t.Fatalf("GetIntegrations: want 1 item, got %d", len(items))
	}
	el, _ := items[0].(map[string]any)
	requireValues(t, "GetIntegrations element", el, map[string]any{"integrationType": "MOCK"})
	agwv2NoInternalFields(t, "GetIntegrations", raw)
}

func TestAPIGatewayV2Wire_GetStagesEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "stages-env")
	agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/stages", map[string]any{"StageName": "dev"})

	m, raw := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/stages", nil)
	items := agwv2Items(t, "GetStages", m)
	if len(items) != 1 {
		t.Fatalf("GetStages: want 1 item, got %d", len(items))
	}
	el, _ := items[0].(map[string]any)
	requireValues(t, "GetStages element", el, map[string]any{"stageName": "dev"})
	agwv2NoInternalFields(t, "GetStages", raw)
}

func TestAPIGatewayV2Wire_GetAuthorizersEnvelope(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "auths-env")
	agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/authorizers", map[string]any{
		"Name": "a1", "AuthorizerType": "JWT",
	})

	m, raw := agwv2Wire(t, p, ctx, "GET", "/v2/apis/"+apiID+"/authorizers", nil)
	items := agwv2Items(t, "GetAuthorizers", m)
	if len(items) != 1 {
		t.Fatalf("GetAuthorizers: want 1 item, got %d", len(items))
	}
	el, _ := items[0].(map[string]any)
	requireValues(t, "GetAuthorizers element", el, map[string]any{"name": "a1"})
	agwv2NoInternalFields(t, "GetAuthorizers", raw)
}

// TestAPIGatewayV2Wire_EmptyListsAreLists pins that an empty collection is a JSON
// list rather than null. `--query 'length(items)'` errors on a null, which is how
// the v1 defect surfaced, and it costs nothing to hold v2 to the same standard.
// Also asserts no nextToken is invented: substrate returns one page and honors no
// token.
func TestAPIGatewayV2Wire_EmptyListsAreLists(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "empty-lists")

	for _, tc := range []struct{ name, path string }{
		{"GetRoutes", "/v2/apis/" + apiID + "/routes"},
		{"GetIntegrations", "/v2/apis/" + apiID + "/integrations"},
		{"GetStages", "/v2/apis/" + apiID + "/stages"},
		{"GetAuthorizers", "/v2/apis/" + apiID + "/authorizers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, raw := agwv2Wire(t, p, ctx, "GET", tc.path, nil)
			items := agwv2Items(t, tc.name, m)
			if len(items) != 0 {
				t.Errorf("%s: want an empty list, got %d items", tc.name, len(items))
			}
			if bytes.Contains(raw, []byte("null")) {
				t.Errorf("%s: response contains null: %s", tc.name, raw)
			}
			if _, ok := m["nextToken"]; ok {
				t.Errorf("%s: emits a nextToken it does not honor: %s", tc.name, raw)
			}
		})
	}
}

// TestAPIGatewayV2Wire_UnsetOptionalsAreOmitted pins absent-vs-empty. Real API
// Gateway omits an unset member; sending "" or null is a different observation,
// and a caller distinguishing them is making a real one.
func TestAPIGatewayV2Wire_UnsetOptionalsAreOmitted(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)

	api, _ := agwv2Wire(t, p, ctx, "POST", "/v2/apis", map[string]any{
		"Name": "minimal", "ProtocolType": "HTTP",
	})
	for _, k := range []string{"description", "tags"} {
		if _, ok := api[k]; ok {
			t.Errorf("CreateApi: unset %q should be omitted, got %#v", k, api[k])
		}
	}

	apiID, _ := api["apiId"].(string)
	route, _ := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/routes", map[string]any{
		"RouteKey": "GET /min",
	})
	for _, k := range []string{"target", "authorizationType", "authorizerId"} {
		if _, ok := route[k]; ok {
			t.Errorf("CreateRoute: unset %q should be omitted, got %#v", k, route[k])
		}
	}

	stage, _ := agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/stages", map[string]any{
		"StageName": "bare",
	})
	for _, k := range []string{"deploymentId", "description", "stageVariables", "tags"} {
		if _, ok := stage[k]; ok {
			t.Errorf("CreateStage: unset %q should be omitted, got %#v", k, stage[k])
		}
	}
}

// TestAPIGatewayV2Wire_StateEncodingUnchanged is what makes "state encoding
// unchanged" a fact rather than a claim. The state types are a persisted format
// that recorded runs replay from, so the fix belongs entirely in the wire types.
// This test fails if someone later retags a state struct instead.
func TestAPIGatewayV2Wire_StateEncodingUnchanged(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := &emulator.APIGatewayV2Plugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{
		State:  state,
		Logger: emulator.NewDefaultLogger(0, false),
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "r1"}

	apiID := agwv2API(t, p, ctx, "persisted")

	keys, err := state.List(t.Context(), "apigatewayv2", "apiv2:")
	if err != nil {
		t.Fatalf("state.List: %v", err)
	}
	var stored []byte
	for _, k := range keys {
		if strings.HasSuffix(k, apiID) {
			stored, err = state.Get(t.Context(), "apigatewayv2", k)
			if err != nil {
				t.Fatalf("state.Get %s: %v", k, err)
			}
		}
	}
	if stored == nil {
		t.Fatalf("no stored API under %v", keys)
	}

	for _, want := range []string{`"ApiId"`, `"Name"`, `"ProtocolType"`, `"ApiEndpoint"`,
		`"CreatedDate"`, `"AccountID"`, `"Region"`} {
		if !bytes.Contains(stored, []byte(want)) {
			t.Errorf("state no longer carries %s: %s", want, stored)
		}
	}
	// The wire spellings must not appear in state; if they do, a state struct was
	// retagged and every recorded run's format changed with it.
	for _, bad := range []string{`"apiId"`, `"name"`, `"protocolType"`} {
		if bytes.Contains(stored, []byte(bad)) {
			t.Errorf("state carries the wire spelling %s: %s", bad, stored)
		}
	}
}
