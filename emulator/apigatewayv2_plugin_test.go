package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

func setupAPIGatewayV2Plugin(t *testing.T) (*emulator.APIGatewayV2Plugin, *emulator.RequestContext) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	p := &emulator.APIGatewayV2Plugin{}
	if err := p.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("APIGatewayV2Plugin.Initialize: %v", err)
	}
	reqCtx := &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "test-req-1",
	}
	return p, reqCtx
}

func apigwv2Request(t *testing.T, method, path string, body map[string]any) *emulator.AWSRequest {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
	}
	return &emulator.AWSRequest{
		Service:   "apigatewayv2",
		Operation: method,
		Path:      path,
		Headers:   map[string]string{},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestAPIGatewayV2Plugin_CreateApi(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)

	req := apigwv2Request(t, "POST", "/v2/apis", map[string]any{
		"Name":         "my-http-api",
		"ProtocolType": "HTTP",
	})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("CreateApi: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("want status 201, got %d", resp.StatusCode)
	}

	// Decoded into a map with the wire keys, not into V2ApiState: the state
	// struct's PascalCase tags are what an SDK cannot parse (#529), so an
	// assertion made through it would pass whatever the wire said.
	var out map[string]any
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if id, _ := out["apiId"].(string); id == "" {
		t.Error("apiId is empty")
	}
	endpoint, _ := out["apiEndpoint"].(string)
	if endpoint == "" {
		t.Error("apiEndpoint is empty")
	}
	if !strings.HasPrefix(endpoint, "https://") {
		t.Errorf("apiEndpoint should start with https://, got %q", endpoint)
	}
	if !strings.Contains(endpoint, "execute-api") {
		t.Errorf("apiEndpoint should contain execute-api, got %q", endpoint)
	}
	if out["name"] != "my-http-api" {
		t.Errorf("want name my-http-api, got %v", out["name"])
	}
	if out["protocolType"] != "HTTP" {
		t.Errorf("want protocolType HTTP, got %v", out["protocolType"])
	}
}

func TestAPIGatewayV2Plugin_CreateRoute(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)

	// Create an API first.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST", "/v2/apis", map[string]any{
		"Name":         "route-api",
		"ProtocolType": "HTTP",
	}))
	if err != nil {
		t.Fatalf("CreateApi: %v", err)
	}
	apiID := apigwv2APIID(t, createResp.Body)

	// Create a route.
	routeResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/routes",
		map[string]any{
			"RouteKey": "GET /users",
		},
	))
	if err != nil {
		t.Fatalf("CreateRoute: %v", err)
	}
	if routeResp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", routeResp.StatusCode)
	}

	var route map[string]any
	if err := json.Unmarshal(routeResp.Body, &route); err != nil {
		t.Fatalf("unmarshal route: %v", err)
	}
	if id, _ := route["routeId"].(string); id == "" {
		t.Error("routeId is empty")
	}
	if route["routeKey"] != "GET /users" {
		t.Errorf("want routeKey 'GET /users', got %v", route["routeKey"])
	}
}

func TestAPIGatewayV2Plugin_CreateStage(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)

	// Create an API.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST", "/v2/apis", map[string]any{
		"Name":         "stage-api",
		"ProtocolType": "HTTP",
	}))
	if err != nil {
		t.Fatalf("CreateApi: %v", err)
	}
	apiID := apigwv2APIID(t, createResp.Body)

	// Create a stage.
	stageResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/stages",
		map[string]any{
			"StageName": "$default",
		},
	))
	if err != nil {
		t.Fatalf("CreateStage: %v", err)
	}
	if stageResp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", stageResp.StatusCode)
	}

	var stage map[string]any
	if err := json.Unmarshal(stageResp.Body, &stage); err != nil {
		t.Fatalf("unmarshal stage: %v", err)
	}
	if stage["stageName"] != "$default" {
		t.Errorf("want stageName $default, got %v", stage["stageName"])
	}

	// GetStage should return it.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/stages/$default",
		nil,
	))
	if err != nil {
		t.Fatalf("GetStage: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", getResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_GetApis(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)

	for _, name := range []string{"api-alpha", "api-beta"} {
		req := apigwv2Request(t, "POST", "/v2/apis", map[string]any{
			"Name":         name,
			"ProtocolType": "HTTP",
		})
		if _, err := p.HandleRequest(ctx, req); err != nil {
			t.Fatalf("CreateApi %s: %v", name, err)
		}
	}

	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET", "/v2/apis", nil))
	if err != nil {
		t.Fatalf("GetApis: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", listResp.StatusCode)
	}

	// The envelope is "items", lowercase — "Items" parses to nothing (#529).
	var out struct {
		Items []struct {
			APIID string `json:"apiId"`
			Name  string `json:"name"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("want 2 APIs, got %d", len(out.Items))
	}
	for i, it := range out.Items {
		if it.APIID == "" || it.Name == "" {
			t.Errorf("item %d: empty apiId or name: %+v", i, it)
		}
	}
}

// apigwv2APIID pulls the apiId out of a v2 response body. It reads the wire key
// rather than decoding into V2ApiState, which is the state type #529 keeps off the
// wire.
func apigwv2APIID(t *testing.T, body []byte) string {
	t.Helper()
	var m struct {
		APIID string `json:"apiId"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal apiId: %v", err)
	}
	if m.APIID == "" {
		t.Fatalf("no apiId in response: %s", body)
	}
	return m.APIID
}

// createTestV2API creates a V2 API and returns its wire apiId.
func createTestV2API(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST", "/v2/apis", map[string]any{
		"Name":         name,
		"ProtocolType": "HTTP",
	}))
	if err != nil {
		t.Fatalf("CreateApi %q: %v", name, err)
	}
	return apigwv2APIID(t, resp.Body)
}

func TestAPIGatewayV2Plugin_GetApi(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "get-me")

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET", "/v2/apis/"+apiID, nil))
	if err != nil {
		t.Fatalf("GetApi: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	if got := apigwv2APIID(t, resp.Body); got != apiID {
		t.Errorf("want apiId %q, got %q", apiID, got)
	}
}

func TestAPIGatewayV2Plugin_UpdateApi(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "update-me")

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "PATCH", "/v2/apis/"+apiID,
		map[string]any{"Description": "updated"},
	))
	if err != nil {
		t.Fatalf("UpdateApi: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_DeleteApi(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "delete-me")

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE", "/v2/apis/"+apiID, nil))
	if err != nil {
		t.Fatalf("DeleteApi: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", resp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_RouteGetDelete(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "route-api")

	// Create route.
	routeResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/routes",
		map[string]any{"RouteKey": "POST /items"},
	))
	if err != nil {
		t.Fatalf("CreateRoute: %v", err)
	}
	var route struct {
		RouteID string `json:"routeId"`
	}
	if err := json.Unmarshal(routeResp.Body, &route); err != nil {
		t.Fatalf("unmarshal route: %v", err)
	}
	if route.RouteID == "" {
		t.Fatalf("no routeId in response: %s", routeResp.Body)
	}

	// GetRoute.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/routes/"+route.RouteID, nil))
	if err != nil {
		t.Fatalf("GetRoute: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetRoutes.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/routes", nil))
	if err != nil {
		t.Fatalf("GetRoutes: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteRoute.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+apiID+"/routes/"+route.RouteID, nil))
	if err != nil {
		t.Fatalf("DeleteRoute: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_Integration(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "int-api")

	// CreateIntegration.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/integrations",
		map[string]any{
			"IntegrationType":      "AWS_PROXY",
			"IntegrationUri":       "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
			"PayloadFormatVersion": "2.0",
		},
	))
	if err != nil {
		t.Fatalf("CreateIntegration: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", createResp.StatusCode)
	}
	var intOut map[string]any
	if err := json.Unmarshal(createResp.Body, &intOut); err != nil {
		t.Fatalf("unmarshal integration: %v", err)
	}
	intID, _ := intOut["integrationId"].(string)
	if intID == "" {
		t.Fatal("integrationId is empty")
	}

	// GetIntegration.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/integrations/"+intID, nil))
	if err != nil {
		t.Fatalf("GetIntegration: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetIntegrations.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/integrations", nil))
	if err != nil {
		t.Fatalf("GetIntegrations: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteIntegration.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+apiID+"/integrations/"+intID, nil))
	if err != nil {
		t.Fatalf("DeleteIntegration: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_GetStagesAndDeleteStage(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "stages-api")

	// Create stage.
	_, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/stages",
		map[string]any{"StageName": "dev"},
	))
	if err != nil {
		t.Fatalf("CreateStage: %v", err)
	}

	// GetStages.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/stages", nil))
	if err != nil {
		t.Fatalf("GetStages: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var listOut map[string]any
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal stages: %v", err)
	}
	items, _ := listOut["items"].([]any)
	if len(items) != 1 {
		t.Errorf("want 1 stage, got %d", len(items))
	}

	// DeleteStage.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+apiID+"/stages/dev", nil))
	if err != nil {
		t.Fatalf("DeleteStage: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_AuthorizerCRUD(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "auth-v2-api")

	// CreateAuthorizer.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/authorizers",
		map[string]any{
			"Name":           "my-jwt-auth",
			"AuthorizerType": "JWT",
			"IdentitySource": []string{"$request.header.Authorization"},
			"JwtConfiguration": map[string]any{
				"Audience": []string{"my-client"},
				"Issuer":   "https://example.com",
			},
		},
	))
	if err != nil {
		t.Fatalf("CreateAuthorizer: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", createResp.StatusCode)
	}
	var authOut map[string]any
	if err := json.Unmarshal(createResp.Body, &authOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	authID, _ := authOut["authorizerId"].(string)
	if authID == "" {
		t.Fatal("authorizerId is empty")
	}

	// GetAuthorizer.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/authorizers/"+authID, nil))
	if err != nil {
		t.Fatalf("GetAuthorizer: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetAuthorizers.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/authorizers", nil))
	if err != nil {
		t.Fatalf("GetAuthorizers: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteAuthorizer.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+apiID+"/authorizers/"+authID, nil))
	if err != nil {
		t.Fatalf("DeleteAuthorizer: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_DeploymentCRUD(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "dep-v2-api")

	// CreateDeployment.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/deployments",
		map[string]any{"Description": "initial"},
	))
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", createResp.StatusCode)
	}
	var depOut map[string]any
	if err := json.Unmarshal(createResp.Body, &depOut); err != nil {
		t.Fatalf("unmarshal deployment: %v", err)
	}
	depID, _ := depOut["deploymentId"].(string)
	if depID == "" {
		t.Fatal("deploymentId is empty")
	}

	// GetDeployment.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+apiID+"/deployments/"+depID, nil))
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_DomainNameAndApiMapping(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := createTestV2API(t, p, ctx, "mapping-api")

	// Create stage.
	_, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+apiID+"/stages",
		map[string]any{"StageName": "$default"},
	))
	if err != nil {
		t.Fatalf("CreateStage: %v", err)
	}

	// CreateDomainName.
	dnResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST", "/v2/domainnames",
		map[string]any{
			"DomainName": "v2.example.com",
			"DomainNameConfigurations": []map[string]any{
				{"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc"},
			},
		},
	))
	if err != nil {
		t.Fatalf("CreateDomainName: %v", err)
	}
	if dnResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", dnResp.StatusCode)
	}

	// GetDomainName.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET", "/v2/domainnames/v2.example.com", nil))
	if err != nil {
		t.Fatalf("GetDomainName: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// CreateApiMapping.
	mappingResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/domainnames/v2.example.com/apimappings",
		map[string]any{
			"ApiId":         apiID,
			"Stage":         "$default",
			"ApiMappingKey": "",
		},
	))
	if err != nil {
		t.Fatalf("CreateApiMapping: %v", err)
	}
	if mappingResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", mappingResp.StatusCode)
	}
	var mappingOut map[string]any
	if err := json.Unmarshal(mappingResp.Body, &mappingOut); err != nil {
		t.Fatalf("unmarshal mapping: %v", err)
	}
	if _, ok := mappingOut["apiMappingId"]; !ok {
		t.Error("apiMappingId is missing from response")
	}
}
