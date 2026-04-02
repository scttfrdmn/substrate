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

func setupCodeDeployPlugin(t *testing.T) (*substrate.CodeDeployPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CodeDeployPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("CodeDeployPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-codedeploy-1",
	}
}

func codedeployRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal codedeploy request: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "codedeploy",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "CodeDeploy_20141006." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestCodeDeployPlugin_CreateGetDeleteApplication(t *testing.T) {
	p, ctx := setupCodeDeployPlugin(t)

	// CreateApplication.
	resp, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateApplication", map[string]any{
		"applicationName": "my-app",
		"computePlatform": "Server",
	}))
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		ApplicationID string `json:"applicationId"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.ApplicationID == "" {
		t.Error("want non-empty applicationId")
	}

	// GetApplication.
	resp, err = p.HandleRequest(ctx, codedeployRequest(t, "GetApplication", map[string]any{
		"applicationName": "my-app",
	}))
	if err != nil {
		t.Fatalf("GetApplication: %v", err)
	}
	var getResult struct {
		Application struct {
			ApplicationName string `json:"applicationName"`
			ComputePlatform string `json:"computePlatform"`
		} `json:"application"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if getResult.Application.ApplicationName != "my-app" {
		t.Errorf("want ApplicationName=my-app, got %q", getResult.Application.ApplicationName)
	}
	if getResult.Application.ComputePlatform != "Server" {
		t.Errorf("want ComputePlatform=Server, got %q", getResult.Application.ComputePlatform)
	}

	// DeleteApplication.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "DeleteApplication", map[string]any{
		"applicationName": "my-app",
	}))
	if err != nil {
		t.Fatalf("DeleteApplication: %v", err)
	}

	// GetApplication after delete.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "GetApplication", map[string]any{
		"applicationName": "my-app",
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ApplicationDoesNotExistException" {
		t.Errorf("want ApplicationDoesNotExistException, got %v", err)
	}
}

func TestCodeDeployPlugin_ListApplications(t *testing.T) {
	p, ctx := setupCodeDeployPlugin(t)

	for _, name := range []string{"app-one", "app-two"} {
		_, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateApplication", map[string]any{
			"applicationName": name,
		}))
		if err != nil {
			t.Fatalf("CreateApplication %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, codedeployRequest(t, "ListApplications", map[string]any{}))
	if err != nil {
		t.Fatalf("ListApplications: %v", err)
	}
	var result struct {
		Applications []string `json:"applications"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Applications) != 2 {
		t.Errorf("want 2 applications, got %d", len(result.Applications))
	}
}

func TestCodeDeployPlugin_DeploymentGroup_CRUD(t *testing.T) {
	p, ctx := setupCodeDeployPlugin(t)

	_, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateApplication", map[string]any{
		"applicationName": "group-app",
	}))
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}

	// CreateDeploymentGroup.
	resp, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateDeploymentGroup", map[string]any{
		"applicationName":     "group-app",
		"deploymentGroupName": "my-group",
		"serviceRoleArn":      "arn:aws:iam::123456789012:role/codedeploy-role",
	}))
	if err != nil {
		t.Fatalf("CreateDeploymentGroup: %v", err)
	}
	var createGroupResult struct {
		DeploymentGroupID string `json:"deploymentGroupId"`
	}
	if err := json.Unmarshal(resp.Body, &createGroupResult); err != nil {
		t.Fatalf("unmarshal create group: %v", err)
	}
	if createGroupResult.DeploymentGroupID == "" {
		t.Error("want non-empty deploymentGroupId")
	}

	// GetDeploymentGroup.
	resp, err = p.HandleRequest(ctx, codedeployRequest(t, "GetDeploymentGroup", map[string]any{
		"applicationName":     "group-app",
		"deploymentGroupName": "my-group",
	}))
	if err != nil {
		t.Fatalf("GetDeploymentGroup: %v", err)
	}
	var getGroupResult struct {
		DeploymentGroupInfo struct {
			DeploymentGroupName string `json:"deploymentGroupName"`
			ApplicationName     string `json:"applicationName"`
		} `json:"deploymentGroupInfo"`
	}
	if err := json.Unmarshal(resp.Body, &getGroupResult); err != nil {
		t.Fatalf("unmarshal get group: %v", err)
	}
	if getGroupResult.DeploymentGroupInfo.DeploymentGroupName != "my-group" {
		t.Errorf("want DeploymentGroupName=my-group, got %q", getGroupResult.DeploymentGroupInfo.DeploymentGroupName)
	}

	// DeleteDeploymentGroup.
	resp, err = p.HandleRequest(ctx, codedeployRequest(t, "DeleteDeploymentGroup", map[string]any{
		"applicationName":     "group-app",
		"deploymentGroupName": "my-group",
	}))
	if err != nil {
		t.Fatalf("DeleteDeploymentGroup: %v", err)
	}
	var deleteGroupResult struct {
		HooksNotCleanedUp []any `json:"hooksNotCleanedUp"`
	}
	if err := json.Unmarshal(resp.Body, &deleteGroupResult); err != nil {
		t.Fatalf("unmarshal delete group: %v", err)
	}
	if deleteGroupResult.HooksNotCleanedUp == nil {
		t.Error("want empty hooksNotCleanedUp slice, got nil")
	}

	// GetDeploymentGroup after delete.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "GetDeploymentGroup", map[string]any{
		"applicationName":     "group-app",
		"deploymentGroupName": "my-group",
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "DeploymentGroupDoesNotExistException" {
		t.Errorf("want DeploymentGroupDoesNotExistException, got %v", err)
	}
}

func TestCodeDeployPlugin_CreateGetDeployment(t *testing.T) {
	p, ctx := setupCodeDeployPlugin(t)

	_, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateApplication", map[string]any{
		"applicationName": "deploy-app",
	}))
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "CreateDeploymentGroup", map[string]any{
		"applicationName":     "deploy-app",
		"deploymentGroupName": "deploy-group",
	}))
	if err != nil {
		t.Fatalf("CreateDeploymentGroup: %v", err)
	}

	// CreateDeployment.
	resp, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateDeployment", map[string]any{
		"applicationName":     "deploy-app",
		"deploymentGroupName": "deploy-group",
	}))
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	var deployResult struct {
		DeploymentID string `json:"deploymentId"`
	}
	if err := json.Unmarshal(resp.Body, &deployResult); err != nil {
		t.Fatalf("unmarshal deployment: %v", err)
	}
	if deployResult.DeploymentID == "" {
		t.Error("want non-empty deploymentId")
	}
	// Verify deployment ID format: "d-" + 9 uppercase alphanumeric
	if len(deployResult.DeploymentID) != 11 || deployResult.DeploymentID[:2] != "d-" {
		t.Errorf("want deployment ID in format d-XXXXXXXXX, got %q", deployResult.DeploymentID)
	}

	// GetDeployment.
	resp, err = p.HandleRequest(ctx, codedeployRequest(t, "GetDeployment", map[string]any{
		"deploymentId": deployResult.DeploymentID,
	}))
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	var getDeployResult struct {
		DeploymentInfo struct {
			DeploymentID        string `json:"deploymentId"`
			ApplicationName     string `json:"applicationName"`
			DeploymentGroupName string `json:"deploymentGroupName"`
			Status              string `json:"status"`
		} `json:"deploymentInfo"`
	}
	if err := json.Unmarshal(resp.Body, &getDeployResult); err != nil {
		t.Fatalf("unmarshal get deployment: %v", err)
	}
	if getDeployResult.DeploymentInfo.Status != "Succeeded" {
		t.Errorf("want Status=Succeeded, got %q", getDeployResult.DeploymentInfo.Status)
	}
	if getDeployResult.DeploymentInfo.ApplicationName != "deploy-app" {
		t.Errorf("want ApplicationName=deploy-app, got %q", getDeployResult.DeploymentInfo.ApplicationName)
	}
}

func TestCodeDeployPlugin_Duplicate_Errors(t *testing.T) {
	p, ctx := setupCodeDeployPlugin(t)

	_, err := p.HandleRequest(ctx, codedeployRequest(t, "CreateApplication", map[string]any{
		"applicationName": "dup-app",
	}))
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}

	// Duplicate application.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "CreateApplication", map[string]any{
		"applicationName": "dup-app",
	}))
	if err == nil {
		t.Fatal("want error for duplicate application, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ApplicationAlreadyExistsException" {
		t.Errorf("want ApplicationAlreadyExistsException, got %v", err)
	}

	// Create deployment group.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "CreateDeploymentGroup", map[string]any{
		"applicationName":     "dup-app",
		"deploymentGroupName": "dup-group",
	}))
	if err != nil {
		t.Fatalf("CreateDeploymentGroup: %v", err)
	}

	// Duplicate deployment group.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "CreateDeploymentGroup", map[string]any{
		"applicationName":     "dup-app",
		"deploymentGroupName": "dup-group",
	}))
	if err == nil {
		t.Fatal("want error for duplicate deployment group, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok || awsErr.Code != "DeploymentGroupAlreadyExistsException" {
		t.Errorf("want DeploymentGroupAlreadyExistsException, got %v", err)
	}

	// GetDeployment for nonexistent deployment.
	_, err = p.HandleRequest(ctx, codedeployRequest(t, "GetDeployment", map[string]any{
		"deploymentId": "d-NONEXISTENT",
	}))
	if err == nil {
		t.Fatal("want error for nonexistent deployment, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok || awsErr.Code != "DeploymentDoesNotExistException" {
		t.Errorf("want DeploymentDoesNotExistException, got %v", err)
	}
}
