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

func setupCodePipelinePlugin(t *testing.T) (*substrate.CodePipelinePlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CodePipelinePlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("CodePipelinePlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-codepipeline-1",
	}
}

func codepipelineRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal codepipeline request: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "codepipeline",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "CodePipeline_20150709." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestCodePipelinePlugin_CreateGetDeletePipeline(t *testing.T) {
	p, ctx := setupCodePipelinePlugin(t)

	// Create.
	resp, err := p.HandleRequest(ctx, codepipelineRequest(t, "CreatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name":    "my-pipeline",
			"roleArn": "arn:aws:iam::123456789012:role/codepipeline-role",
			"stages": []map[string]any{
				{"name": "Source", "actions": []any{}},
				{"name": "Deploy", "actions": []any{}},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		Pipeline struct {
			Name    string `json:"name"`
			Version int    `json:"version"`
		} `json:"pipeline"`
		Metadata struct {
			PipelineARN string `json:"pipelineArn"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.Pipeline.Name != "my-pipeline" {
		t.Errorf("want Name=my-pipeline, got %q", createResult.Pipeline.Name)
	}
	if createResult.Pipeline.Version != 1 {
		t.Errorf("want Version=1, got %d", createResult.Pipeline.Version)
	}
	if createResult.Metadata.PipelineARN == "" {
		t.Error("want non-empty pipelineArn")
	}

	// Duplicate create.
	_, err = p.HandleRequest(ctx, codepipelineRequest(t, "CreatePipeline", map[string]any{
		"pipeline": map[string]any{"name": "my-pipeline"},
	}))
	if err == nil {
		t.Fatal("want error for duplicate pipeline, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "PipelineNameInUseException" {
		t.Errorf("want PipelineNameInUseException, got %v", err)
	}

	// GetPipeline.
	resp, err = p.HandleRequest(ctx, codepipelineRequest(t, "GetPipeline", map[string]any{
		"name": "my-pipeline",
	}))
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	var getResult struct {
		Pipeline struct {
			Name string `json:"name"`
		} `json:"pipeline"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if getResult.Pipeline.Name != "my-pipeline" {
		t.Errorf("want Name=my-pipeline, got %q", getResult.Pipeline.Name)
	}

	// DeletePipeline.
	_, err = p.HandleRequest(ctx, codepipelineRequest(t, "DeletePipeline", map[string]any{
		"name": "my-pipeline",
	}))
	if err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}

	// GetPipeline after delete.
	_, err = p.HandleRequest(ctx, codepipelineRequest(t, "GetPipeline", map[string]any{
		"name": "my-pipeline",
	}))
	if err == nil {
		t.Fatal("want error for deleted pipeline, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok || awsErr.Code != "PipelineNotFoundException" {
		t.Errorf("want PipelineNotFoundException, got %v", err)
	}
}

func TestCodePipelinePlugin_UpdatePipeline(t *testing.T) {
	p, ctx := setupCodePipelinePlugin(t)

	_, err := p.HandleRequest(ctx, codepipelineRequest(t, "CreatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name":    "update-pipeline",
			"roleArn": "arn:aws:iam::123456789012:role/original-role",
		},
	}))
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	resp, err := p.HandleRequest(ctx, codepipelineRequest(t, "UpdatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name":    "update-pipeline",
			"roleArn": "arn:aws:iam::123456789012:role/updated-role",
		},
	}))
	if err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}

	var result struct {
		Pipeline struct {
			Name    string `json:"name"`
			RoleArn string `json:"roleArn"`
			Version int    `json:"version"`
		} `json:"pipeline"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result.Pipeline.RoleArn != "arn:aws:iam::123456789012:role/updated-role" {
		t.Errorf("want updated roleArn, got %q", result.Pipeline.RoleArn)
	}
	if result.Pipeline.Version != 2 {
		t.Errorf("want Version=2 after update, got %d", result.Pipeline.Version)
	}
}

func TestCodePipelinePlugin_ListPipelines(t *testing.T) {
	p, ctx := setupCodePipelinePlugin(t)

	for _, name := range []string{"pipe-alpha", "pipe-beta"} {
		_, err := p.HandleRequest(ctx, codepipelineRequest(t, "CreatePipeline", map[string]any{
			"pipeline": map[string]any{"name": name},
		}))
		if err != nil {
			t.Fatalf("CreatePipeline %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, codepipelineRequest(t, "ListPipelines", map[string]any{}))
	if err != nil {
		t.Fatalf("ListPipelines: %v", err)
	}
	var result struct {
		Pipelines []map[string]any `json:"pipelines"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Pipelines) != 2 {
		t.Errorf("want 2 pipelines, got %d", len(result.Pipelines))
	}
}

func TestCodePipelinePlugin_StartExecution_GetState(t *testing.T) {
	p, ctx := setupCodePipelinePlugin(t)

	_, err := p.HandleRequest(ctx, codepipelineRequest(t, "CreatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name": "exec-pipeline",
			"stages": []map[string]any{
				{"name": "Source"},
				{"name": "Build"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	resp, err := p.HandleRequest(ctx, codepipelineRequest(t, "StartPipelineExecution", map[string]any{
		"name": "exec-pipeline",
	}))
	if err != nil {
		t.Fatalf("StartPipelineExecution: %v", err)
	}

	var execResult struct {
		PipelineExecutionID string `json:"pipelineExecutionId"`
	}
	if err := json.Unmarshal(resp.Body, &execResult); err != nil {
		t.Fatalf("unmarshal execution: %v", err)
	}
	if execResult.PipelineExecutionID == "" {
		t.Error("want non-empty pipelineExecutionId")
	}

	// GetPipelineState.
	resp, err = p.HandleRequest(ctx, codepipelineRequest(t, "GetPipelineState", map[string]any{
		"name": "exec-pipeline",
	}))
	if err != nil {
		t.Fatalf("GetPipelineState: %v", err)
	}

	var stateResult struct {
		PipelineName string `json:"pipelineName"`
		StageStates  []struct {
			StageName       string `json:"stageName"`
			LatestExecution struct {
				Status string `json:"status"`
			} `json:"latestExecution"`
		} `json:"stageStates"`
	}
	if err := json.Unmarshal(resp.Body, &stateResult); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	if stateResult.PipelineName != "exec-pipeline" {
		t.Errorf("want pipelineName=exec-pipeline, got %q", stateResult.PipelineName)
	}
	if len(stateResult.StageStates) != 2 {
		t.Errorf("want 2 stage states, got %d", len(stateResult.StageStates))
	}
	for _, s := range stateResult.StageStates {
		if s.LatestExecution.Status != "Succeeded" {
			t.Errorf("want stage status=Succeeded, got %q for stage %s", s.LatestExecution.Status, s.StageName)
		}
	}
}

func TestCodePipelinePlugin_GetPipelineExecution(t *testing.T) {
	p, ctx := setupCodePipelinePlugin(t)

	_, err := p.HandleRequest(ctx, codepipelineRequest(t, "CreatePipeline", map[string]any{
		"pipeline": map[string]any{"name": "get-exec-pipeline"},
	}))
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	resp, err := p.HandleRequest(ctx, codepipelineRequest(t, "StartPipelineExecution", map[string]any{
		"name": "get-exec-pipeline",
	}))
	if err != nil {
		t.Fatalf("StartPipelineExecution: %v", err)
	}

	var execResult struct {
		PipelineExecutionID string `json:"pipelineExecutionId"`
	}
	if err := json.Unmarshal(resp.Body, &execResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// GetPipelineExecution.
	resp, err = p.HandleRequest(ctx, codepipelineRequest(t, "GetPipelineExecution", map[string]any{
		"pipelineName":        "get-exec-pipeline",
		"pipelineExecutionId": execResult.PipelineExecutionID,
	}))
	if err != nil {
		t.Fatalf("GetPipelineExecution: %v", err)
	}

	var getExecResult struct {
		PipelineExecution struct {
			PipelineExecutionID string `json:"pipelineExecutionId"`
			PipelineName        string `json:"pipelineName"`
			Status              string `json:"status"`
		} `json:"pipelineExecution"`
	}
	if err := json.Unmarshal(resp.Body, &getExecResult); err != nil {
		t.Fatalf("unmarshal get exec: %v", err)
	}
	if getExecResult.PipelineExecution.PipelineExecutionID != execResult.PipelineExecutionID {
		t.Errorf("want execID=%s, got %s", execResult.PipelineExecutionID, getExecResult.PipelineExecution.PipelineExecutionID)
	}
	if getExecResult.PipelineExecution.Status != "Succeeded" {
		t.Errorf("want Status=Succeeded, got %q", getExecResult.PipelineExecution.Status)
	}

	// GetPipelineExecution for nonexistent ID.
	_, err = p.HandleRequest(ctx, codepipelineRequest(t, "GetPipelineExecution", map[string]any{
		"pipelineName":        "get-exec-pipeline",
		"pipelineExecutionId": "nonexistent-exec-id",
	}))
	if err == nil {
		t.Fatal("want error for missing execution, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "PipelineExecutionNotFoundException" {
		t.Errorf("want PipelineExecutionNotFoundException, got %v", err)
	}
}
