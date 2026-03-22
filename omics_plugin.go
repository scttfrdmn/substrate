package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

// omicsNamespace is the state namespace for Amazon HealthOmics.
const omicsNamespace = "omics"

// OmicsPlugin emulates the Amazon HealthOmics service.
// It handles workflow run CRUD operations (StartRun, GetRun, CancelRun, ListRuns)
// using the HealthOmics REST/JSON API at /run/... paths.
type OmicsPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	rng    *rand.Rand
}

// Name returns the service name "omics".
func (p *OmicsPlugin) Name() string { return omicsNamespace }

// Initialize sets up the OmicsPlugin with the provided configuration.
func (p *OmicsPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	p.rng = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	return nil
}

// Shutdown is a no-op for OmicsPlugin.
func (p *OmicsPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a HealthOmics REST/JSON request to the appropriate handler.
func (p *OmicsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, runID := parseOmicsOperation(req.Operation, req.Path)
	switch op {
	case "StartRun":
		return p.startRun(ctx, req)
	case "GetRun":
		return p.getRun(ctx, req, runID)
	case "CancelRun":
		return p.cancelRun(ctx, req, runID)
	case "ListRuns":
		return p.listRuns(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "OmicsPlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseOmicsOperation maps an HTTP method and path to a HealthOmics operation name
// and optional run ID.
func parseOmicsOperation(method, path string) (op, runID string) {
	rest := strings.TrimPrefix(path, "/")
	switch {
	case rest == "run" && method == "POST":
		return "StartRun", ""
	case rest == "run" && method == "GET":
		return "ListRuns", ""
	case strings.HasPrefix(rest, "run/") && method == "GET":
		return "GetRun", strings.TrimPrefix(rest, "run/")
	case strings.HasPrefix(rest, "run/") && method == "DELETE":
		return "CancelRun", strings.TrimPrefix(rest, "run/")
	// SDK v2 uses POST /run/{id}/cancel instead of DELETE /run/{id}.
	case strings.HasSuffix(rest, "/cancel") && strings.HasPrefix(rest, "run/") && method == "POST":
		return "CancelRun", strings.TrimSuffix(strings.TrimPrefix(rest, "run/"), "/cancel")
	}
	return "", ""
}

// OmicsRun holds persisted state for a HealthOmics workflow run.
type OmicsRun struct {
	// Id is the 10-digit numeric run identifier.
	Id string `json:"id"`

	// Status is the run status (COMPLETED for deterministic emulation).
	Status string `json:"status"`

	// WorkflowId is the workflow to run.
	WorkflowId string `json:"workflowId,omitempty"`

	// WorkflowType is the workflow type (PRIVATE, READY2RUN).
	WorkflowType string `json:"workflowType,omitempty"`

	// Name is an optional user-supplied run name.
	Name string `json:"name,omitempty"`

	// RoleArn is the IAM role ARN used by the run.
	RoleArn string `json:"roleArn,omitempty"`

	// OutputUri is the S3 URI for run outputs.
	OutputUri string `json:"outputUri,omitempty"`

	// StatusMessage is an optional human-readable status message.
	StatusMessage string `json:"statusMessage,omitempty"`

	// AccountID is the AWS account that owns the run.
	AccountID string `json:"accountID"`

	// Region is the AWS region where the run executes.
	Region string `json:"region"`
}

func (p *OmicsPlugin) startRun(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkflowId   string          `json:"workflowId"`
		WorkflowType string          `json:"workflowType"`
		Name         string          `json:"name"`
		RoleArn      string          `json:"roleArn"`
		OutputUri    string          `json:"outputUri"`
		Parameters   json.RawMessage `json:"parameters"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	runID := p.generateOmicsRunID()
	run := OmicsRun{
		Id:           runID,
		Status:       "COMPLETED",
		WorkflowId:   body.WorkflowId,
		WorkflowType: body.WorkflowType,
		Name:         body.Name,
		RoleArn:      body.RoleArn,
		OutputUri:    body.OutputUri,
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
	}

	goCtx := context.Background()
	runKey := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + runID
	data, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("startRun: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, omicsNamespace, runKey, data); err != nil {
		return nil, fmt.Errorf("startRun: put: %w", err)
	}
	idsKey := "run_ids:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, omicsNamespace, idsKey, runID)
	return omicsJSONResponse(http.StatusCreated, map[string]string{"id": runID})
}

func (p *OmicsPlugin) getRun(ctx *RequestContext, _ *AWSRequest, runID string) (*AWSResponse, error) {
	goCtx := context.Background()
	runKey := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + runID
	data, err := p.state.Get(goCtx, omicsNamespace, runKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "run " + runID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var run OmicsRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("getRun: unmarshal: %w", err)
	}
	return omicsJSONResponse(http.StatusOK, run)
}

func (p *OmicsPlugin) cancelRun(ctx *RequestContext, _ *AWSRequest, runID string) (*AWSResponse, error) {
	goCtx := context.Background()
	runKey := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + runID
	data, err := p.state.Get(goCtx, omicsNamespace, runKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "run " + runID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var run OmicsRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("cancelRun: unmarshal: %w", err)
	}
	run.Status = "CANCELLED"
	updated, _ := json.Marshal(run)
	if err := p.state.Put(goCtx, omicsNamespace, runKey, updated); err != nil {
		return nil, fmt.Errorf("cancelRun: put: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *OmicsPlugin) listRuns(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	idsKey := "run_ids:" + ctx.AccountID + "/" + ctx.Region
	ids, _ := loadStringIndex(goCtx, p.state, omicsNamespace, idsKey)

	type runItem struct {
		Id     string `json:"id"`
		Status string `json:"status"`
		Name   string `json:"name,omitempty"`
	}
	items := make([]runItem, 0, len(ids))
	for _, id := range ids {
		key := "run:" + ctx.AccountID + "/" + ctx.Region + "/" + id
		data, err := p.state.Get(goCtx, omicsNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var run OmicsRun
		if json.Unmarshal(data, &run) == nil {
			items = append(items, runItem{Id: run.Id, Status: run.Status, Name: run.Name})
		}
	}
	return omicsJSONResponse(http.StatusOK, map[string]interface{}{"items": items})
}

// generateOmicsRunID generates a 10-digit numeric string matching real HealthOmics run IDs.
func (p *OmicsPlugin) generateOmicsRunID() string {
	n := p.rng.Int63n(9000000000) + 1000000000 //nolint:gosec
	return fmt.Sprintf("%d", n)
}

// omicsJSONResponse serializes v to JSON and returns an AWSResponse.
func omicsJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("omics json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
