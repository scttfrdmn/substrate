package substrate

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// emrServerlessNamespace is the state namespace for Amazon EMR Serverless.
const emrServerlessNamespace = "emrserverless"

// EMRServerlessPlugin emulates the Amazon EMR Serverless service.
// It handles application and job run CRUD operations using the
// EMR Serverless REST/JSON API at /applications/... paths.
type EMRServerlessPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "emrserverless".
func (p *EMRServerlessPlugin) Name() string { return emrServerlessNamespace }

// Initialize sets up the EMRServerlessPlugin with the provided configuration.
func (p *EMRServerlessPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for EMRServerlessPlugin.
func (p *EMRServerlessPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EMR Serverless REST/JSON request to the appropriate handler.
func (p *EMRServerlessPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, appID, runID := parseEMRServerlessOperation(req.Operation, req.Path)
	switch op {
	case "CreateApplication":
		return p.createApplication(ctx, req)
	case "GetApplication":
		return p.getApplication(ctx, req, appID)
	case "DeleteApplication":
		return p.deleteApplication(ctx, req, appID)
	case "StartJobRun":
		return p.startJobRun(ctx, req, appID)
	case "GetJobRun":
		return p.getJobRun(ctx, req, appID, runID)
	case "CancelJobRun":
		return p.cancelJobRun(ctx, req, appID, runID)
	case "ListJobRuns":
		return p.listJobRuns(ctx, req, appID)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "EMRServerlessPlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseEMRServerlessOperation maps an HTTP method and path to an EMR Serverless
// operation name plus optional appID and runID.
func parseEMRServerlessOperation(method, path string) (op, appID, runID string) {
	rest := strings.TrimPrefix(path, "/")
	// /applications
	if rest == "applications" {
		if method == "POST" {
			return "CreateApplication", "", ""
		}
	}
	// /applications/{appId}
	if strings.HasPrefix(rest, "applications/") {
		after := strings.TrimPrefix(rest, "applications/")
		// /applications/{appId}/jobruns[/{runId}]
		if idx := strings.Index(after, "/jobruns"); idx >= 0 {
			aid := after[:idx]
			jobrunsRest := after[idx+len("/jobruns"):]
			jobrunsRest = strings.TrimPrefix(jobrunsRest, "/")
			if jobrunsRest == "" {
				if method == "POST" {
					return "StartJobRun", aid, ""
				}
				if method == "GET" {
					return "ListJobRuns", aid, ""
				}
			} else {
				// /applications/{appId}/jobruns/{runId}
				if method == "GET" {
					return "GetJobRun", aid, jobrunsRest
				}
				if method == "DELETE" {
					return "CancelJobRun", aid, jobrunsRest
				}
			}
		} else {
			// /applications/{appId}
			if method == "GET" {
				return "GetApplication", after, ""
			}
			if method == "DELETE" {
				return "DeleteApplication", after, ""
			}
		}
	}
	return "", "", ""
}

// EMRServerlessApp holds persisted state for an EMR Serverless application.
type EMRServerlessApp struct {
	// ApplicationId is the unique identifier for the application.
	ApplicationId string `json:"applicationId"`

	// Name is the user-supplied name.
	Name string `json:"name"`

	// Type is the application type (SPARK or HIVE).
	Type string `json:"type"`

	// ReleaseLabel is the EMR release version (e.g. "emr-6.9.0").
	ReleaseLabel string `json:"releaseLabel"`

	// State is the application state (CREATED).
	State string `json:"state"`

	// Arn is the ARN of the application.
	Arn string `json:"arn"`

	// AccountID is the AWS account that owns the application.
	AccountID string `json:"accountID"`

	// Region is the AWS region where the application resides.
	Region string `json:"region"`
}

// EMRServerlessJobRun holds persisted state for an EMR Serverless job run.
type EMRServerlessJobRun struct {
	// ApplicationId is the application this job run belongs to.
	ApplicationId string `json:"applicationId"`

	// JobRunId is the unique identifier for the job run.
	JobRunId string `json:"jobRunId"`

	// Name is an optional user-supplied name.
	Name string `json:"name,omitempty"`

	// State is the job run state (SUCCESS).
	State string `json:"state"`

	// Arn is the ARN of the job run.
	Arn string `json:"arn"`

	// AccountID is the AWS account that owns the job run.
	AccountID string `json:"accountID"`

	// Region is the AWS region where the job run executes.
	Region string `json:"region"`
}

func (p *EMRServerlessPlugin) createApplication(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name         string `json:"name"`
		Type         string `json:"type"`
		ReleaseLabel string `json:"releaseLabel"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	appID := generateEMRServerlessAppID()
	arn := fmt.Sprintf("arn:aws:emr-serverless:%s:%s:/applications/%s", ctx.Region, ctx.AccountID, appID)
	app := EMRServerlessApp{
		ApplicationId: appID,
		Name:          body.Name,
		Type:          body.Type,
		ReleaseLabel:  body.ReleaseLabel,
		State:         "CREATED",
		Arn:           arn,
		AccountID:     ctx.AccountID,
		Region:        ctx.Region,
	}

	goCtx := context.Background()
	appKey := "app:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	data, err := json.Marshal(app)
	if err != nil {
		return nil, fmt.Errorf("createApplication: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, emrServerlessNamespace, appKey, data); err != nil {
		return nil, fmt.Errorf("createApplication: put: %w", err)
	}
	idsKey := "app_ids:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, emrServerlessNamespace, idsKey, appID)
	return emrServerlessJSONResponse(http.StatusOK, map[string]string{
		"applicationId": appID,
		"arn":           arn,
	})
}

func (p *EMRServerlessPlugin) getApplication(ctx *RequestContext, _ *AWSRequest, appID string) (*AWSResponse, error) {
	goCtx := context.Background()
	appKey := "app:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	data, err := p.state.Get(goCtx, emrServerlessNamespace, appKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "application " + appID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var app EMRServerlessApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, fmt.Errorf("getApplication: unmarshal: %w", err)
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]interface{}{"application": app})
}

func (p *EMRServerlessPlugin) deleteApplication(ctx *RequestContext, _ *AWSRequest, appID string) (*AWSResponse, error) {
	goCtx := context.Background()
	appKey := "app:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	if err := p.state.Delete(goCtx, emrServerlessNamespace, appKey); err != nil {
		return nil, fmt.Errorf("deleteApplication: %w", err)
	}
	idsKey := "app_ids:" + ctx.AccountID + "/" + ctx.Region
	removeFromStringIndex(goCtx, p.state, emrServerlessNamespace, idsKey, appID)
	return emrServerlessJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *EMRServerlessPlugin) startJobRun(ctx *RequestContext, req *AWSRequest, appID string) (*AWSResponse, error) {
	var body struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(req.Body, &body)

	runID := generateEMRServerlessRunID()
	arn := fmt.Sprintf("arn:aws:emr-serverless:%s:%s:/applications/%s/jobruns/%s",
		ctx.Region, ctx.AccountID, appID, runID)
	run := EMRServerlessJobRun{
		ApplicationId: appID,
		JobRunId:      runID,
		Name:          body.Name,
		State:         "SUCCESS",
		Arn:           arn,
		AccountID:     ctx.AccountID,
		Region:        ctx.Region,
	}

	goCtx := context.Background()
	runKey := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + runID
	data, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("startJobRun: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, emrServerlessNamespace, runKey, data); err != nil {
		return nil, fmt.Errorf("startJobRun: put: %w", err)
	}
	runIdsKey := "jobrun_ids:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	updateStringIndex(goCtx, p.state, emrServerlessNamespace, runIdsKey, runID)
	return emrServerlessJSONResponse(http.StatusOK, map[string]string{
		"applicationId": appID,
		"jobRunId":      runID,
		"arn":           arn,
	})
}

func (p *EMRServerlessPlugin) getJobRun(ctx *RequestContext, _ *AWSRequest, appID, runID string) (*AWSResponse, error) {
	goCtx := context.Background()
	runKey := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + runID
	data, err := p.state.Get(goCtx, emrServerlessNamespace, runKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "job run " + runID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var run EMRServerlessJobRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("getJobRun: unmarshal: %w", err)
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]interface{}{"jobRun": run})
}

func (p *EMRServerlessPlugin) cancelJobRun(ctx *RequestContext, _ *AWSRequest, appID, runID string) (*AWSResponse, error) {
	goCtx := context.Background()
	runKey := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + runID
	data, err := p.state.Get(goCtx, emrServerlessNamespace, runKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "job run " + runID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var run EMRServerlessJobRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("cancelJobRun: unmarshal: %w", err)
	}
	run.State = "CANCELLED"
	updated, _ := json.Marshal(run)
	if err := p.state.Put(goCtx, emrServerlessNamespace, runKey, updated); err != nil {
		return nil, fmt.Errorf("cancelJobRun: put: %w", err)
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]string{
		"applicationId": appID,
		"jobRunId":      runID,
	})
}

func (p *EMRServerlessPlugin) listJobRuns(ctx *RequestContext, _ *AWSRequest, appID string) (*AWSResponse, error) {
	goCtx := context.Background()
	runIdsKey := "jobrun_ids:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	ids, _ := loadStringIndex(goCtx, p.state, emrServerlessNamespace, runIdsKey)

	type runSummary struct {
		ApplicationId string `json:"applicationId"`
		JobRunId      string `json:"jobRunId"`
		State         string `json:"state"`
	}
	summaries := make([]runSummary, 0, len(ids))
	for _, id := range ids {
		key := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + id
		data, err := p.state.Get(goCtx, emrServerlessNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var run EMRServerlessJobRun
		if json.Unmarshal(data, &run) == nil {
			summaries = append(summaries, runSummary{ApplicationId: run.ApplicationId, JobRunId: run.JobRunId, State: run.State})
		}
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]interface{}{"jobRuns": summaries})
}

// generateEMRServerlessAppID generates a numeric-looking EMR Serverless application ID.
func generateEMRServerlessAppID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	n := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	return fmt.Sprintf("00%08x", n)
}

// generateEMRServerlessRunID generates a UUID-formatted job run ID.
func generateEMRServerlessRunID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// emrServerlessJSONResponse serializes v to JSON and returns an AWSResponse.
func emrServerlessJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("emrserverless json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
