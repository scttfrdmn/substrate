package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupAPIGatewayPlugin(t *testing.T) (*substrate.APIGatewayPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.APIGatewayPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("APIGatewayPlugin.Initialize: %v", err)
	}
	reqCtx := &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "test-req-1",
	}
	return p, reqCtx
}

func apigwRequest(t *testing.T, method, path string, body map[string]any) *substrate.AWSRequest {
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
		Service:   "apigateway",
		Operation: method,
		Path:      path,
		Headers:   map[string]string{},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestAPIGatewayPlugin_CreateRestApi(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	req := apigwRequest(t, "POST", "/restapis", map[string]any{
		"name":        "my-api",
		"description": "test API",
	})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("CreateRestApi: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("want status 201, got %d", resp.StatusCode)
	}

	var out substrate.RestAPIState
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ID == "" {
		t.Error("Id is empty")
	}
	if out.RootResourceID == "" {
		t.Error("RootResourceId is empty")
	}
	if out.Name != "my-api" {
		t.Errorf("want name my-api, got %q", out.Name)
	}
}

func TestAPIGatewayPlugin_CreateResource(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	// Create a REST API first.
	createResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/restapis", map[string]any{"name": "resource-api"}))
	if err != nil {
		t.Fatalf("CreateRestApi: %v", err)
	}
	var api substrate.RestAPIState
	if err := json.Unmarshal(createResp.Body, &api); err != nil {
		t.Fatalf("unmarshal api: %v", err)
	}

	// Create a resource under the root resource.
	resResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID,
		map[string]any{"pathPart": "users"},
	))
	if err != nil {
		t.Fatalf("CreateResource: %v", err)
	}
	if resResp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", resResp.StatusCode)
	}

	var res substrate.ResourceState
	if err := json.Unmarshal(resResp.Body, &res); err != nil {
		t.Fatalf("unmarshal resource: %v", err)
	}
	if res.ID == "" {
		t.Error("resource ID is empty")
	}
	if res.Path != "/users" {
		t.Errorf("want path /users, got %q", res.Path)
	}
	if res.ParentID != api.RootResourceID {
		t.Errorf("want parentId %q, got %q", api.RootResourceID, res.ParentID)
	}
}

func TestAPIGatewayPlugin_CreateDeploymentAndStage(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	// Create a REST API.
	createResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/restapis", map[string]any{"name": "deploy-api"}))
	if err != nil {
		t.Fatalf("CreateRestApi: %v", err)
	}
	var api substrate.RestAPIState
	if err := json.Unmarshal(createResp.Body, &api); err != nil {
		t.Fatalf("unmarshal api: %v", err)
	}

	// Create a deployment.
	depResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/deployments",
		map[string]any{"description": "initial deployment"},
	))
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	var dep substrate.DeploymentState
	if err := json.Unmarshal(depResp.Body, &dep); err != nil {
		t.Fatalf("unmarshal deployment: %v", err)
	}

	// Create a stage.
	stageResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/stages",
		map[string]any{
			"stageName":    "prod",
			"deploymentId": dep.ID,
		},
	))
	if err != nil {
		t.Fatalf("CreateStage: %v", err)
	}
	if stageResp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", stageResp.StatusCode)
	}

	// GetStage should include an InvokeURL.
	getStageResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/stages/prod",
		nil,
	))
	if err != nil {
		t.Fatalf("GetStage: %v", err)
	}

	var stageOut map[string]interface{}
	if err := json.Unmarshal(getStageResp.Body, &stageOut); err != nil {
		t.Fatalf("unmarshal stage: %v", err)
	}
	invokeURL, _ := stageOut["InvokeUrl"].(string)
	if invokeURL == "" {
		t.Error("InvokeUrl is empty")
	}
	expectedURL := "https://" + api.ID + ".execute-api.us-east-1.amazonaws.com/prod"
	if invokeURL != expectedURL {
		t.Errorf("want InvokeUrl %q, got %q", expectedURL, invokeURL)
	}
}

func TestAPIGatewayPlugin_GetRestApis(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	for _, name := range []string{"api-one", "api-two"} {
		req := apigwRequest(t, "POST", "/restapis", map[string]any{"name": name})
		if _, err := p.HandleRequest(ctx, req); err != nil {
			t.Fatalf("CreateRestApi %s: %v", name, err)
		}
	}

	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/restapis", nil))
	if err != nil {
		t.Fatalf("GetRestApis: %v", err)
	}

	var out struct {
		Items []substrate.RestAPIState `json:"items"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Items) != 2 {
		t.Errorf("want 2 APIs, got %d", len(out.Items))
	}
}

func TestAPIGatewayPlugin_DeleteRestApi(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	// Create an API.
	createResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/restapis", map[string]any{"name": "delete-me"}))
	if err != nil {
		t.Fatalf("CreateRestApi: %v", err)
	}
	var api substrate.RestAPIState
	if err := json.Unmarshal(createResp.Body, &api); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Delete it.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE", "/restapis/"+api.ID, nil))
	if err != nil {
		t.Fatalf("DeleteRestApi: %v", err)
	}
	if delResp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202, got %d", delResp.StatusCode)
	}

	// GetRestApi should now return not-found.
	_, err = p.HandleRequest(ctx, apigwRequest(t, "GET", "/restapis/"+api.ID, nil))
	if err == nil {
		t.Fatal("expected error for deleted API, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("expected *substrate.AWSError, got %T", err)
	}
	if awsErr.Code != "NotFoundException" {
		t.Errorf("want NotFoundException, got %q", awsErr.Code)
	}
}

// createTestRestAPI is a helper that creates a REST API and returns its state.
func createTestRestAPI(t *testing.T, p *substrate.APIGatewayPlugin, ctx *substrate.RequestContext, name string) substrate.RestAPIState {
	t.Helper()
	resp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/restapis", map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("CreateRestApi %q: %v", name, err)
	}
	var api substrate.RestAPIState
	if err := json.Unmarshal(resp.Body, &api); err != nil {
		t.Fatalf("unmarshal RestAPIState: %v", err)
	}
	return api
}

func TestAPIGatewayPlugin_UpdateRestApi(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "update-api")

	resp, err := p.HandleRequest(ctx, apigwRequest(t, "PATCH", "/restapis/"+api.ID, map[string]any{
		"description": "updated description",
	}))
	if err != nil {
		t.Fatalf("UpdateRestApi: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestAPIGatewayPlugin_GetResource(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "res-api")

	// Create a resource.
	resResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID,
		map[string]any{"pathPart": "items"},
	))
	if err != nil {
		t.Fatalf("CreateResource: %v", err)
	}
	var res substrate.ResourceState
	if err := json.Unmarshal(resResp.Body, &res); err != nil {
		t.Fatalf("unmarshal resource: %v", err)
	}

	// GetResource
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/resources/"+res.ID, nil))
	if err != nil {
		t.Fatalf("GetResource: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_GetResources(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "resources-api")

	// Create a child resource.
	_, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID,
		map[string]any{"pathPart": "things"},
	))
	if err != nil {
		t.Fatalf("CreateResource: %v", err)
	}

	// GetResources
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/resources", nil))
	if err != nil {
		t.Fatalf("GetResources: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var out map[string]any
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal GetResources: %v", err)
	}
	items, _ := out["items"].([]any)
	if len(items) < 2 {
		t.Errorf("want at least 2 resources (root + child), got %d", len(items))
	}
}

func TestAPIGatewayPlugin_DeleteResource(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "del-res-api")

	// Create a resource.
	resResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID,
		map[string]any{"pathPart": "widgets"},
	))
	if err != nil {
		t.Fatalf("CreateResource: %v", err)
	}
	var res substrate.ResourceState
	if err := json.Unmarshal(resResp.Body, &res); err != nil {
		t.Fatalf("unmarshal resource: %v", err)
	}

	// Delete resource.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE",
		"/restapis/"+api.ID+"/resources/"+res.ID, nil))
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_PutAndGetMethod(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "method-api")

	// PutMethod on root resource.
	putResp, err := p.HandleRequest(ctx, apigwRequest(t, "PUT",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/GET",
		map[string]any{
			"authorizationType": "NONE",
			"apiKeyRequired":    false,
		},
	))
	if err != nil {
		t.Fatalf("PutMethod: %v", err)
	}
	if putResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", putResp.StatusCode)
	}

	// GetMethod.
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/GET", nil))
	if err != nil {
		t.Fatalf("GetMethod: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// DeleteMethod.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/GET", nil))
	if err != nil {
		t.Fatalf("DeleteMethod: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_PutIntegrationAndResponses(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "int-api")

	// PutMethod first.
	_, err := p.HandleRequest(ctx, apigwRequest(t, "PUT",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/POST",
		map[string]any{"authorizationType": "NONE"},
	))
	if err != nil {
		t.Fatalf("PutMethod: %v", err)
	}

	// PutIntegration.
	intResp, err := p.HandleRequest(ctx, apigwRequest(t, "PUT",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/POST/integration",
		map[string]any{
			"type":                "AWS_PROXY",
			"uri":                 "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
			"httpMethod":          "POST",
			"passthroughBehavior": "WHEN_NO_MATCH",
		},
	))
	if err != nil {
		t.Fatalf("PutIntegration: %v", err)
	}
	if intResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", intResp.StatusCode)
	}

	// PutIntegrationResponse.
	irResp, err := p.HandleRequest(ctx, apigwRequest(t, "PUT",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/POST/integration/responses/200",
		map[string]any{"selectionPattern": ""},
	))
	if err != nil {
		t.Fatalf("PutIntegrationResponse: %v", err)
	}
	if irResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", irResp.StatusCode)
	}

	// PutMethodResponse.
	mrResp, err := p.HandleRequest(ctx, apigwRequest(t, "PUT",
		"/restapis/"+api.ID+"/resources/"+api.RootResourceID+"/methods/POST/responses/200",
		map[string]any{},
	))
	if err != nil {
		t.Fatalf("PutMethodResponse: %v", err)
	}
	if mrResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", mrResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_GetAndDeleteDeployment(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "dep-api")

	// Create deployment.
	depResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/deployments",
		map[string]any{"description": "v1"},
	))
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	var dep substrate.DeploymentState
	if err := json.Unmarshal(depResp.Body, &dep); err != nil {
		t.Fatalf("unmarshal deployment: %v", err)
	}

	// GetDeployment.
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/deployments/"+dep.ID, nil))
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetDeployments.
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/deployments", nil))
	if err != nil {
		t.Fatalf("GetDeployments: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}

	// DeleteDeployment.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE",
		"/restapis/"+api.ID+"/deployments/"+dep.ID, nil))
	if err != nil {
		t.Fatalf("DeleteDeployment: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_GetStagesAndDeleteStage(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "stage-api")

	// Create deployment.
	depResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/deployments",
		map[string]any{"description": "v1"},
	))
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	var dep substrate.DeploymentState
	if err := json.Unmarshal(depResp.Body, &dep); err != nil {
		t.Fatalf("unmarshal deployment: %v", err)
	}

	// Create stage.
	_, err = p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/stages",
		map[string]any{"stageName": "v1", "deploymentId": dep.ID},
	))
	if err != nil {
		t.Fatalf("CreateStage: %v", err)
	}

	// GetStages.
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/stages", nil))
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
	stageItems, _ := listOut["item"].([]any)
	if len(stageItems) != 1 {
		t.Errorf("want 1 stage, got %d", len(stageItems))
	}

	// UpdateStage.
	updateResp, err := p.HandleRequest(ctx, apigwRequest(t, "PATCH",
		"/restapis/"+api.ID+"/stages/v1",
		map[string]any{"description": "updated"},
	))
	if err != nil {
		t.Fatalf("UpdateStage: %v", err)
	}
	if updateResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", updateResp.StatusCode)
	}

	// DeleteStage.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE",
		"/restapis/"+api.ID+"/stages/v1", nil))
	if err != nil {
		t.Fatalf("DeleteStage: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_Authorizer(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "auth-api")

	// CreateAuthorizer.
	createResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/restapis/"+api.ID+"/authorizers",
		map[string]any{
			"name":                         "my-authorizer",
			"type":                         "TOKEN",
			"authorizerUri":                "arn:aws:lambda:us-east-1:123456789012:function:auth-fn",
			"identitySource":               "method.request.header.Authorization",
			"authorizerResultTtlInSeconds": 300,
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
		t.Fatalf("unmarshal authorizer: %v", err)
	}
	authID, _ := authOut["Id"].(string)
	if authID == "" {
		t.Fatal("authorizer id is empty")
	}

	// GetAuthorizer.
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/authorizers/"+authID, nil))
	if err != nil {
		t.Fatalf("GetAuthorizer: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetAuthorizers.
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/restapis/"+api.ID+"/authorizers", nil))
	if err != nil {
		t.Fatalf("GetAuthorizers: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var listOut map[string]any
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal authorizers: %v", err)
	}
	authItems, _ := listOut["items"].([]any)
	if len(authItems) != 1 {
		t.Errorf("want 1 authorizer, got %d", len(authItems))
	}

	// DeleteAuthorizer.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE",
		"/restapis/"+api.ID+"/authorizers/"+authID, nil))
	if err != nil {
		t.Fatalf("DeleteAuthorizer: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_APIKey(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	// CreateApiKey.
	createResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/apikeys",
		map[string]any{"name": "my-key", "enabled": true},
	))
	if err != nil {
		t.Fatalf("CreateApiKey: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", createResp.StatusCode)
	}
	var keyOut map[string]any
	if err := json.Unmarshal(createResp.Body, &keyOut); err != nil {
		t.Fatalf("unmarshal api key: %v", err)
	}
	keyID, _ := keyOut["Id"].(string)
	if keyID == "" {
		t.Fatal("api key id is empty")
	}

	// GetApiKey.
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/apikeys/"+keyID, nil))
	if err != nil {
		t.Fatalf("GetApiKey: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetApiKeys.
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/apikeys", nil))
	if err != nil {
		t.Fatalf("GetApiKeys: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var listOut map[string]any
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal api keys: %v", err)
	}
	keyItems, _ := listOut["items"].([]any)
	if len(keyItems) != 1 {
		t.Errorf("want 1 api key, got %d", len(keyItems))
	}

	// DeleteApiKey.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE", "/apikeys/"+keyID, nil))
	if err != nil {
		t.Fatalf("DeleteApiKey: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_UsagePlan(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)

	// CreateUsagePlan.
	createResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/usageplans",
		map[string]any{
			"name":        "basic-plan",
			"description": "Basic usage plan",
			"throttle":    map[string]any{"rateLimit": 100.0, "burstLimit": 200},
			"quota":       map[string]any{"limit": 1000, "period": "MONTH"},
		},
	))
	if err != nil {
		t.Fatalf("CreateUsagePlan: %v", err)
	}
	if createResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", createResp.StatusCode)
	}
	var planOut map[string]any
	if err := json.Unmarshal(createResp.Body, &planOut); err != nil {
		t.Fatalf("unmarshal usage plan: %v", err)
	}
	planID, _ := planOut["Id"].(string)
	if planID == "" {
		t.Fatal("usage plan id is empty")
	}

	// GetUsagePlan.
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/usageplans/"+planID, nil))
	if err != nil {
		t.Fatalf("GetUsagePlan: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// GetUsagePlans.
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/usageplans", nil))
	if err != nil {
		t.Fatalf("GetUsagePlans: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var listOut map[string]any
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal usage plans: %v", err)
	}
	planItems, _ := listOut["items"].([]any)
	if len(planItems) != 1 {
		t.Errorf("want 1 usage plan, got %d", len(planItems))
	}

	// DeleteUsagePlan.
	delResp, err := p.HandleRequest(ctx, apigwRequest(t, "DELETE", "/usageplans/"+planID, nil))
	if err != nil {
		t.Fatalf("DeleteUsagePlan: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("want 204, got %d", delResp.StatusCode)
	}
}

func TestAPIGatewayPlugin_DomainNameAndBasePathMapping(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "domain-api")

	// CreateDomainName.
	domainResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST", "/domainnames",
		map[string]any{
			"domainName":            "api.example.com",
			"certificateArn":        "arn:aws:acm:us-east-1:123456789012:certificate/abc",
			"endpointConfiguration": map[string]any{"types": []string{"EDGE"}},
		},
	))
	if err != nil {
		t.Fatalf("CreateDomainName: %v", err)
	}
	if domainResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", domainResp.StatusCode)
	}

	// GetDomainName.
	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/domainnames/api.example.com", nil))
	if err != nil {
		t.Fatalf("GetDomainName: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}

	// CreateBasePathMapping.
	bpmResp, err := p.HandleRequest(ctx, apigwRequest(t, "POST",
		"/domainnames/api.example.com/basepathmappings",
		map[string]any{
			"restApiId": api.ID,
			"stage":     "prod",
			"basePath":  "(none)",
		},
	))
	if err != nil {
		t.Fatalf("CreateBasePathMapping: %v", err)
	}
	if bpmResp.StatusCode != http.StatusCreated {
		t.Errorf("want 201, got %d", bpmResp.StatusCode)
	}

	// GetBasePathMappings.
	listResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET",
		"/domainnames/api.example.com/basepathmappings", nil))
	if err != nil {
		t.Fatalf("GetBasePathMappings: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var listOut map[string]any
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal base path mappings: %v", err)
	}
	bpmItems, _ := listOut["items"].([]any)
	if len(bpmItems) != 1 {
		t.Errorf("want 1 base path mapping, got %d", len(bpmItems))
	}
}

func TestAPIGatewayPlugin_GetRestApi(t *testing.T) {
	p, ctx := setupAPIGatewayPlugin(t)
	api := createTestRestAPI(t, p, ctx, "get-api")

	getResp, err := p.HandleRequest(ctx, apigwRequest(t, "GET", "/restapis/"+api.ID, nil))
	if err != nil {
		t.Fatalf("GetRestApi: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}
	var out substrate.RestAPIState
	if err := json.Unmarshal(getResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ID != api.ID {
		t.Errorf("want ID %q, got %q", api.ID, out.ID)
	}
	if out.Name != "get-api" {
		t.Errorf("want name get-api, got %q", out.Name)
	}
}
