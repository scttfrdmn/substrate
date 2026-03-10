package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupAPIGatewayV2Plugin(t *testing.T) (*substrate.APIGatewayV2Plugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.APIGatewayV2Plugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("APIGatewayV2Plugin.Initialize: %v", err)
	}
	reqCtx := &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "test-req-1",
	}
	return p, reqCtx
}

func apigwv2Request(t *testing.T, method, path string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
	}
	return &substrate.AWSRequest{
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

	var out substrate.V2ApiState
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.APIID == "" {
		t.Error("ApiId is empty")
	}
	if out.APIEndpoint == "" {
		t.Error("ApiEndpoint is empty")
	}
	if !strings.HasPrefix(out.APIEndpoint, "https://") {
		t.Errorf("ApiEndpoint should start with https://, got %q", out.APIEndpoint)
	}
	if !strings.Contains(out.APIEndpoint, "execute-api") {
		t.Errorf("ApiEndpoint should contain execute-api, got %q", out.APIEndpoint)
	}
	if out.Name != "my-http-api" {
		t.Errorf("want name my-http-api, got %q", out.Name)
	}
	if out.ProtocolType != "HTTP" {
		t.Errorf("want ProtocolType HTTP, got %q", out.ProtocolType)
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
	var api substrate.V2ApiState
	if err := json.Unmarshal(createResp.Body, &api); err != nil {
		t.Fatalf("unmarshal api: %v", err)
	}

	// Create a route.
	routeResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/routes",
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

	var route substrate.V2RouteState
	if err := json.Unmarshal(routeResp.Body, &route); err != nil {
		t.Fatalf("unmarshal route: %v", err)
	}
	if route.RouteID == "" {
		t.Error("RouteId is empty")
	}
	if route.RouteKey != "GET /users" {
		t.Errorf("want RouteKey 'GET /users', got %q", route.RouteKey)
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
	var api substrate.V2ApiState
	if err := json.Unmarshal(createResp.Body, &api); err != nil {
		t.Fatalf("unmarshal api: %v", err)
	}

	// Create a stage.
	stageResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/stages",
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

	var stage substrate.V2StageState
	if err := json.Unmarshal(stageResp.Body, &stage); err != nil {
		t.Fatalf("unmarshal stage: %v", err)
	}
	if stage.StageName != "$default" {
		t.Errorf("want StageName $default, got %q", stage.StageName)
	}

	// GetStage should return it.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/stages/$default",
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

	var out struct {
		Items []substrate.V2ApiState `json:"Items"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Items) != 2 {
		t.Errorf("want 2 APIs, got %d", len(out.Items))
	}
}

// createTestV2API creates a V2 API and returns its state.
func createTestV2API(t *testing.T, p *substrate.APIGatewayV2Plugin, ctx *substrate.RequestContext, name string) substrate.V2ApiState {
	t.Helper()
	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST", "/v2/apis", map[string]any{
		"Name":         name,
		"ProtocolType": "HTTP",
	}))
	if err != nil {
		t.Fatalf("CreateApi %q: %v", name, err)
	}
	var api substrate.V2ApiState
	if err := json.Unmarshal(resp.Body, &api); err != nil {
		t.Fatalf("unmarshal V2ApiState: %v", err)
	}
	return api
}

func TestAPIGatewayV2Plugin_GetApi(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "get-me")

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET", "/v2/apis/"+api.APIID, nil))
	if err != nil {
		t.Fatalf("GetApi: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var out substrate.V2ApiState
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.APIID != api.APIID {
		t.Errorf("want APIID %q, got %q", api.APIID, out.APIID)
	}
}

func TestAPIGatewayV2Plugin_UpdateApi(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "update-me")

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "PATCH", "/v2/apis/"+api.APIID,
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
	api := createTestV2API(t, p, ctx, "delete-me")

	resp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE", "/v2/apis/"+api.APIID, nil))
	if err != nil {
		t.Fatalf("DeleteApi: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", resp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_RouteGetDelete(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "route-api")

	// Create route.
	routeResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/routes",
		map[string]any{"RouteKey": "POST /items"},
	))
	if err != nil {
		t.Fatalf("CreateRoute: %v", err)
	}
	var route substrate.V2RouteState
	if err := json.Unmarshal(routeResp.Body, &route); err != nil {
		t.Fatalf("unmarshal route: %v", err)
	}

	// GetRoute.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/routes/"+route.RouteID, nil))
	if err != nil {
		t.Fatalf("GetRoute: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetRoutes.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/routes", nil))
	if err != nil {
		t.Fatalf("GetRoutes: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteRoute.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+api.APIID+"/routes/"+route.RouteID, nil))
	if err != nil {
		t.Fatalf("DeleteRoute: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_Integration(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "int-api")

	// CreateIntegration.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/integrations",
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
	intID, _ := intOut["IntegrationId"].(string)
	if intID == "" {
		t.Fatal("IntegrationId is empty")
	}

	// GetIntegration.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/integrations/"+intID, nil))
	if err != nil {
		t.Fatalf("GetIntegration: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetIntegrations.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/integrations", nil))
	if err != nil {
		t.Fatalf("GetIntegrations: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteIntegration.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+api.APIID+"/integrations/"+intID, nil))
	if err != nil {
		t.Fatalf("DeleteIntegration: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_GetStagesAndDeleteStage(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "stages-api")

	// Create stage.
	_, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/stages",
		map[string]any{"StageName": "dev"},
	))
	if err != nil {
		t.Fatalf("CreateStage: %v", err)
	}

	// GetStages.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/stages", nil))
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
	items, _ := listOut["Items"].([]any)
	if len(items) != 1 {
		t.Errorf("want 1 stage, got %d", len(items))
	}

	// DeleteStage.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+api.APIID+"/stages/dev", nil))
	if err != nil {
		t.Fatalf("DeleteStage: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_AuthorizerCRUD(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "auth-v2-api")

	// CreateAuthorizer.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/authorizers",
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
	authID, _ := authOut["AuthorizerId"].(string)
	if authID == "" {
		t.Fatal("AuthorizerId is empty")
	}

	// GetAuthorizer.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/authorizers/"+authID, nil))
	if err != nil {
		t.Fatalf("GetAuthorizer: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetAuthorizers.
	listResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/authorizers", nil))
	if err != nil {
		t.Fatalf("GetAuthorizers: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteAuthorizer.
	delResp, err := p.HandleRequest(ctx, apigwv2Request(t, "DELETE",
		"/v2/apis/"+api.APIID+"/authorizers/"+authID, nil))
	if err != nil {
		t.Fatalf("DeleteAuthorizer: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_DeploymentCRUD(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "dep-v2-api")

	// CreateDeployment.
	createResp, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/deployments",
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
	depID, _ := depOut["DeploymentId"].(string)
	if depID == "" {
		t.Fatal("DeploymentId is empty")
	}

	// GetDeployment.
	getResp, err := p.HandleRequest(ctx, apigwv2Request(t, "GET",
		"/v2/apis/"+api.APIID+"/deployments/"+depID, nil))
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}
}

func TestAPIGatewayV2Plugin_DomainNameAndApiMapping(t *testing.T) {
	p, ctx := setupAPIGatewayV2Plugin(t)
	api := createTestV2API(t, p, ctx, "mapping-api")

	// Create stage.
	_, err := p.HandleRequest(ctx, apigwv2Request(t, "POST",
		"/v2/apis/"+api.APIID+"/stages",
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
			"ApiId":         api.APIID,
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
	if _, ok := mappingOut["ApiMappingId"]; !ok {
		t.Error("ApiMappingId is missing from response")
	}
}
