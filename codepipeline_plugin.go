package substrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// CodePipelinePlugin emulates the AWS CodePipeline service.
// It handles pipeline CRUD, execution management, and state queries using the
// CodePipeline JSON-target protocol (X-Amz-Target: CodePipeline_20150709.{Op}).
type CodePipelinePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "codepipeline".
func (p *CodePipelinePlugin) Name() string { return codepipelineNamespace }

// Initialize sets up the CodePipelinePlugin with the provided configuration.
func (p *CodePipelinePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CodePipelinePlugin.
func (p *CodePipelinePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CodePipeline JSON-target request to the appropriate handler.
func (p *CodePipelinePlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreatePipeline":
		return p.createPipeline(reqCtx, req)
	case "GetPipeline":
		return p.getPipeline(reqCtx, req)
	case "UpdatePipeline":
		return p.updatePipeline(reqCtx, req)
	case "DeletePipeline":
		return p.deletePipeline(reqCtx, req)
	case "ListPipelines":
		return p.listPipelines(reqCtx, req)
	case "StartPipelineExecution":
		return p.startPipelineExecution(reqCtx, req)
	case "GetPipelineState":
		return p.getPipelineState(reqCtx, req)
	case "GetPipelineExecution":
		return p.getPipelineExecution(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CodePipelinePlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *CodePipelinePlugin) createPipeline(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Pipeline struct {
			Name    string                   `json:"name"`
			RoleArn string                   `json:"roleArn"`
			Stages  []map[string]interface{} `json:"stages"`
		} `json:"pipeline"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Pipeline.Name == "" {
		return nil, &AWSError{Code: "InvalidStructureException", Message: "pipeline name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := codepipelineKey(reqCtx.AccountID, reqCtx.Region, input.Pipeline.Name)
	existing, err := p.state.Get(goCtx, codepipelineNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codepipeline createPipeline get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "PipelineNameInUseException", Message: "Pipeline " + input.Pipeline.Name + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	now := p.tc.Now()
	pipeline := CodePipelineState{
		Name:      input.Pipeline.Name,
		RoleArn:   input.Pipeline.RoleArn,
		Stages:    input.Pipeline.Stages,
		Version:   1,
		Created:   now,
		Updated:   now,
		AccountID: reqCtx.AccountID,
		Region:    reqCtx.Region,
	}

	data, err := json.Marshal(pipeline)
	if err != nil {
		return nil, fmt.Errorf("codepipeline createPipeline marshal: %w", err)
	}
	if err := p.state.Put(goCtx, codepipelineNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codepipeline createPipeline put: %w", err)
	}
	updateStringIndex(goCtx, p.state, codepipelineNamespace, codepipelineNamesKey(reqCtx.AccountID, reqCtx.Region), input.Pipeline.Name)

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipeline": map[string]interface{}{
			"name":    pipeline.Name,
			"roleArn": pipeline.RoleArn,
			"stages":  pipeline.Stages,
			"version": pipeline.Version,
		},
		"metadata": map[string]interface{}{
			"pipelineArn": fmt.Sprintf("arn:aws:codepipeline:%s:%s:%s", reqCtx.Region, reqCtx.AccountID, pipeline.Name),
			"created":     pipeline.Created,
			"updated":     pipeline.Updated,
		},
	})
}

func (p *CodePipelinePlugin) getPipeline(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name    string `json:"name"`
		Version int    `json:"version"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	pipeline, err := p.loadPipeline(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err != nil {
		return nil, err
	}

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipeline": map[string]interface{}{
			"name":    pipeline.Name,
			"roleArn": pipeline.RoleArn,
			"stages":  pipeline.Stages,
			"version": pipeline.Version,
		},
		"metadata": map[string]interface{}{
			"pipelineArn": fmt.Sprintf("arn:aws:codepipeline:%s:%s:%s", reqCtx.Region, reqCtx.AccountID, pipeline.Name),
			"created":     pipeline.Created,
			"updated":     pipeline.Updated,
		},
	})
}

func (p *CodePipelinePlugin) updatePipeline(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Pipeline struct {
			Name    string                   `json:"name"`
			RoleArn string                   `json:"roleArn"`
			Stages  []map[string]interface{} `json:"stages"`
		} `json:"pipeline"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	pipeline, err := p.loadPipeline(reqCtx.AccountID, reqCtx.Region, input.Pipeline.Name)
	if err != nil {
		return nil, err
	}

	if input.Pipeline.RoleArn != "" {
		pipeline.RoleArn = input.Pipeline.RoleArn
	}
	if input.Pipeline.Stages != nil {
		pipeline.Stages = input.Pipeline.Stages
	}
	pipeline.Version++
	pipeline.Updated = p.tc.Now()

	data, err := json.Marshal(pipeline)
	if err != nil {
		return nil, fmt.Errorf("codepipeline updatePipeline marshal: %w", err)
	}

	goCtx := context.Background()
	key := codepipelineKey(reqCtx.AccountID, reqCtx.Region, pipeline.Name)
	if err := p.state.Put(goCtx, codepipelineNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codepipeline updatePipeline put: %w", err)
	}

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipeline": map[string]interface{}{
			"name":    pipeline.Name,
			"roleArn": pipeline.RoleArn,
			"stages":  pipeline.Stages,
			"version": pipeline.Version,
		},
	})
}

func (p *CodePipelinePlugin) deletePipeline(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if _, err := p.loadPipeline(reqCtx.AccountID, reqCtx.Region, input.Name); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := codepipelineKey(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err := p.state.Delete(goCtx, codepipelineNamespace, key); err != nil {
		return nil, fmt.Errorf("codepipeline deletePipeline delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, codepipelineNamespace, codepipelineNamesKey(reqCtx.AccountID, reqCtx.Region), input.Name)

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *CodePipelinePlugin) listPipelines(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, codepipelineNamespace, codepipelineNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("codepipeline listPipelines load index: %w", err)
	}

	summaries := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		pl, err := p.loadPipeline(reqCtx.AccountID, reqCtx.Region, name)
		if err != nil {
			continue
		}
		summaries = append(summaries, map[string]interface{}{
			"name":    pl.Name,
			"version": pl.Version,
			"created": pl.Created,
			"updated": pl.Updated,
		})
	}

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipelines": summaries,
	})
}

func (p *CodePipelinePlugin) startPipelineExecution(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "InvalidStructureException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}

	pipeline, err := p.loadPipeline(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err != nil {
		return nil, err
	}

	execID := generateCodePipelineExecID()
	exec := CodePipelineExecution{
		PipelineExecutionID: execID,
		PipelineName:        pipeline.Name,
		PipelineVersion:     pipeline.Version,
		Status:              "Succeeded",
		StartTime:           p.tc.Now(),
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
	}

	data, err := json.Marshal(exec)
	if err != nil {
		return nil, fmt.Errorf("codepipeline startPipelineExecution marshal: %w", err)
	}

	goCtx := context.Background()
	key := codepipelineExecKey(reqCtx.AccountID, reqCtx.Region, execID)
	if err := p.state.Put(goCtx, codepipelineNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codepipeline startPipelineExecution put: %w", err)
	}

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipelineExecutionId": execID,
	})
}

func (p *CodePipelinePlugin) getPipelineState(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	pipeline, err := p.loadPipeline(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err != nil {
		return nil, err
	}

	// Build stage states from pipeline stage definitions.
	stageStates := make([]map[string]interface{}, 0, len(pipeline.Stages))
	for _, stage := range pipeline.Stages {
		stageName, _ := stage["name"].(string)
		stageStates = append(stageStates, map[string]interface{}{
			"stageName": stageName,
			"latestExecution": map[string]interface{}{
				"status":              "Succeeded",
				"pipelineExecutionId": "",
			},
		})
	}

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipelineName":    pipeline.Name,
		"pipelineVersion": pipeline.Version,
		"stageStates":     stageStates,
		"created":         pipeline.Created,
		"updated":         pipeline.Updated,
	})
}

func (p *CodePipelinePlugin) getPipelineExecution(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PipelineName        string `json:"pipelineName"`
		PipelineExecutionId string `json:"pipelineExecutionId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidStructureException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	goCtx := context.Background()
	key := codepipelineExecKey(reqCtx.AccountID, reqCtx.Region, input.PipelineExecutionId)
	data, err := p.state.Get(goCtx, codepipelineNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codepipeline getPipelineExecution get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "PipelineExecutionNotFoundException", Message: "Execution " + input.PipelineExecutionId + " not found.", HTTPStatus: http.StatusNotFound}
	}

	var exec CodePipelineExecution
	if err := json.Unmarshal(data, &exec); err != nil {
		return nil, fmt.Errorf("codepipeline getPipelineExecution unmarshal: %w", err)
	}

	return codepipelineJSONResponse(http.StatusOK, map[string]interface{}{
		"pipelineExecution": exec,
	})
}

// loadPipeline loads a CodePipelineState from state by name or returns a not-found error.
func (p *CodePipelinePlugin) loadPipeline(acct, region, name string) (*CodePipelineState, error) {
	if name == "" {
		return nil, &AWSError{Code: "InvalidStructureException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := codepipelineKey(acct, region, name)
	data, err := p.state.Get(goCtx, codepipelineNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codepipeline loadPipeline get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "PipelineNotFoundException", Message: "Pipeline " + name + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var pl CodePipelineState
	if err := json.Unmarshal(data, &pl); err != nil {
		return nil, fmt.Errorf("codepipeline loadPipeline unmarshal: %w", err)
	}
	return &pl, nil
}

// State key helpers.
func codepipelineKey(acct, region, name string) string {
	return "pipeline:" + acct + "/" + region + "/" + name
}

func codepipelineNamesKey(acct, region string) string {
	return "pipeline_names:" + acct + "/" + region
}

func codepipelineExecKey(acct, region, execID string) string {
	return "execution:" + acct + "/" + region + "/" + execID
}

// generateCodePipelineExecID generates a UUID for pipeline execution IDs.
func generateCodePipelineExecID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// codepipelineJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.1.
func codepipelineJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("codepipeline json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
