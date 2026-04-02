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

func setupCodeBuildPlugin(t *testing.T) (*substrate.CodeBuildPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CodeBuildPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("CodeBuildPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-codebuild-1",
	}
}

func codebuildRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal codebuild request: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "codebuild",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "CodeBuild_20161006." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestCodeBuildPlugin_CreateBatchGetDeleteProject(t *testing.T) {
	p, ctx := setupCodeBuildPlugin(t)

	// Create.
	resp, err := p.HandleRequest(ctx, codebuildRequest(t, "CreateProject", map[string]any{
		"name":        "my-project",
		"description": "test project",
		"serviceRole": "arn:aws:iam::123456789012:role/codebuild-role",
		"source": map[string]any{
			"type":     "GITHUB",
			"location": "https://github.com/org/repo",
		},
		"artifacts": map[string]any{
			"type": "NO_ARTIFACTS",
		},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	}))
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		Project substrate.CodeBuildProject `json:"project"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.Project.Name != "my-project" {
		t.Errorf("want Name=my-project, got %q", createResult.Project.Name)
	}
	if createResult.Project.ARN == "" {
		t.Error("want non-empty ARN")
	}

	// Duplicate create.
	_, err = p.HandleRequest(ctx, codebuildRequest(t, "CreateProject", map[string]any{
		"name": "my-project",
	}))
	if err == nil {
		t.Fatal("want error for duplicate project, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceAlreadyExistsException" {
		t.Errorf("want ResourceAlreadyExistsException, got %v", err)
	}

	// BatchGetProjects.
	resp, err = p.HandleRequest(ctx, codebuildRequest(t, "BatchGetProjects", map[string]any{
		"names": []string{"my-project", "nonexistent"},
	}))
	if err != nil {
		t.Fatalf("BatchGetProjects: %v", err)
	}
	var batchResult struct {
		Projects         []substrate.CodeBuildProject `json:"projects"`
		ProjectsNotFound []string                     `json:"projectsNotFound"`
	}
	if err := json.Unmarshal(resp.Body, &batchResult); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if len(batchResult.Projects) != 1 {
		t.Errorf("want 1 project, got %d", len(batchResult.Projects))
	}
	if len(batchResult.ProjectsNotFound) != 1 || batchResult.ProjectsNotFound[0] != "nonexistent" {
		t.Errorf("want [nonexistent] not found, got %v", batchResult.ProjectsNotFound)
	}

	// DeleteProject.
	_, err = p.HandleRequest(ctx, codebuildRequest(t, "DeleteProject", map[string]any{
		"name": "my-project",
	}))
	if err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}

	// BatchGet after delete.
	resp, err = p.HandleRequest(ctx, codebuildRequest(t, "BatchGetProjects", map[string]any{
		"names": []string{"my-project"},
	}))
	if err != nil {
		t.Fatalf("BatchGetProjects after delete: %v", err)
	}
	if err := json.Unmarshal(resp.Body, &batchResult); err != nil {
		t.Fatalf("unmarshal after delete: %v", err)
	}
	if len(batchResult.Projects) != 0 {
		t.Errorf("want 0 projects after delete, got %d", len(batchResult.Projects))
	}
}

func TestCodeBuildPlugin_UpdateProject(t *testing.T) {
	p, ctx := setupCodeBuildPlugin(t)

	_, err := p.HandleRequest(ctx, codebuildRequest(t, "CreateProject", map[string]any{
		"name":        "update-project",
		"description": "original description",
	}))
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	resp, err := p.HandleRequest(ctx, codebuildRequest(t, "UpdateProject", map[string]any{
		"project": map[string]any{
			"name":        "update-project",
			"description": "updated description",
		},
	}))
	if err != nil {
		t.Fatalf("UpdateProject: %v", err)
	}

	var result struct {
		Project substrate.CodeBuildProject `json:"project"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result.Project.Description != "updated description" {
		t.Errorf("want description=updated description, got %q", result.Project.Description)
	}
}

func TestCodeBuildPlugin_ListProjects(t *testing.T) {
	p, ctx := setupCodeBuildPlugin(t)

	for _, name := range []string{"proj-alpha", "proj-beta"} {
		_, err := p.HandleRequest(ctx, codebuildRequest(t, "CreateProject", map[string]any{
			"name": name,
		}))
		if err != nil {
			t.Fatalf("CreateProject %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, codebuildRequest(t, "ListProjects", map[string]any{}))
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	var result struct {
		Projects []string `json:"projects"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Projects) != 2 {
		t.Errorf("want 2 projects, got %d", len(result.Projects))
	}
}

func TestCodeBuildPlugin_StartBuildBatchGetBuilds(t *testing.T) {
	p, ctx := setupCodeBuildPlugin(t)

	_, err := p.HandleRequest(ctx, codebuildRequest(t, "CreateProject", map[string]any{
		"name": "build-project",
	}))
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	resp, err := p.HandleRequest(ctx, codebuildRequest(t, "StartBuild", map[string]any{
		"projectName": "build-project",
	}))
	if err != nil {
		t.Fatalf("StartBuild: %v", err)
	}

	var buildResult struct {
		Build substrate.CodeBuildBuild `json:"build"`
	}
	if err := json.Unmarshal(resp.Body, &buildResult); err != nil {
		t.Fatalf("unmarshal build: %v", err)
	}
	if buildResult.Build.ID == "" {
		t.Error("want non-empty build ID")
	}
	if buildResult.Build.BuildStatus != "SUCCEEDED" {
		t.Errorf("want BuildStatus=SUCCEEDED, got %q", buildResult.Build.BuildStatus)
	}
	if buildResult.Build.ProjectName != "build-project" {
		t.Errorf("want ProjectName=build-project, got %q", buildResult.Build.ProjectName)
	}

	// BatchGetBuilds.
	resp, err = p.HandleRequest(ctx, codebuildRequest(t, "BatchGetBuilds", map[string]any{
		"ids": []string{buildResult.Build.ID, "nonexistent-build"},
	}))
	if err != nil {
		t.Fatalf("BatchGetBuilds: %v", err)
	}
	var batchResult struct {
		Builds         []substrate.CodeBuildBuild `json:"builds"`
		BuildsNotFound []string                   `json:"buildsNotFound"`
	}
	if err := json.Unmarshal(resp.Body, &batchResult); err != nil {
		t.Fatalf("unmarshal batch builds: %v", err)
	}
	if len(batchResult.Builds) != 1 {
		t.Errorf("want 1 build, got %d", len(batchResult.Builds))
	}
	if len(batchResult.BuildsNotFound) != 1 {
		t.Errorf("want 1 not found, got %d", len(batchResult.BuildsNotFound))
	}
}

func TestCodeBuildPlugin_StartBuild_ProjectNotFound(t *testing.T) {
	p, ctx := setupCodeBuildPlugin(t)
	_, err := p.HandleRequest(ctx, codebuildRequest(t, "StartBuild", map[string]any{
		"projectName": "nonexistent-project",
	}))
	if err == nil {
		t.Fatal("want error for missing project, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}
}

func TestCodeBuildPlugin_UnsupportedOperation(t *testing.T) {
	p, ctx := setupCodeBuildPlugin(t)
	_, err := p.HandleRequest(ctx, codebuildRequest(t, "ListBuildsForProject", map[string]any{}))
	if err == nil {
		t.Fatal("want error for unsupported op, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "InvalidAction" {
		t.Errorf("want InvalidAction, got %v", err)
	}
}
