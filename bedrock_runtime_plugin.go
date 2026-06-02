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

// bedrockRuntimeNamespace is the state namespace for Amazon Bedrock Runtime.
const bedrockRuntimeNamespace = "bedrock-runtime"

// bedrockRuntimeCtrlNamespace is the state namespace for Bedrock Runtime control-plane data.
const bedrockRuntimeCtrlNamespace = "bedrock-runtime-ctrl"

// bedrockRuntimeCtrlResponseKey returns the state key for a seeded model response.
func bedrockRuntimeCtrlResponseKey(modelID string) string {
	return "response:" + modelID
}

// bedrockRuntimeCtrlJobStatusKey returns the state key for a seeded model
// invocation job status override.
func bedrockRuntimeCtrlJobStatusKey(jobID string) string {
	return "jobstatus:" + jobID
}

// BedrockRuntimePlugin emulates the Amazon Bedrock Runtime service.
// It handles ApplyGuardrail for the bedrock-runtime host, supporting
// pass-through (action NONE) and blocklist-based intervention
// (action GUARDRAIL_INTERVENED). Guardrails are auto-created on first use.
type BedrockRuntimePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "bedrock-runtime".
func (p *BedrockRuntimePlugin) Name() string { return bedrockRuntimeNamespace }

// Initialize sets up the BedrockRuntimePlugin with the provided configuration.
func (p *BedrockRuntimePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for BedrockRuntimePlugin.
func (p *BedrockRuntimePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Bedrock Runtime request to the appropriate handler.
func (p *BedrockRuntimePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, resourceID, version := parseBedrockRuntimeOperation(req.Operation, req.Path)
	switch op {
	case "ApplyGuardrail":
		return p.applyGuardrail(ctx, req, resourceID, version)
	case "InvokeModel":
		return p.invokeModel(ctx, req, resourceID)
	case "CreateModelInvocationJob":
		return p.createModelInvocationJob(ctx, req)
	case "GetModelInvocationJob":
		return p.getModelInvocationJob(ctx, req, resourceID)
	case "StopModelInvocationJob":
		return p.stopModelInvocationJob(ctx, req, resourceID)
	case "ListModelInvocationJobs":
		return p.listModelInvocationJobs(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "BedrockRuntimePlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseBedrockRuntimeOperation maps an HTTP method and path to a Bedrock Runtime
// operation name plus resource IDs.
func parseBedrockRuntimeOperation(method, path string) (op, id1, id2 string) {
	rest := strings.TrimPrefix(path, "/")

	// Control-plane batch inference (ModelInvocationJob) operations.
	// GET /model-invocation-jobs            → ListModelInvocationJobs
	if rest == "model-invocation-jobs" && method == "GET" {
		return "ListModelInvocationJobs", "", ""
	}
	// /model-invocation-job[/{jobId}[/stop]]
	if rest == "model-invocation-job" {
		if method == "POST" {
			return "CreateModelInvocationJob", "", ""
		}
		return "", "", ""
	}
	if strings.HasPrefix(rest, "model-invocation-job/") {
		jobPart := strings.TrimPrefix(rest, "model-invocation-job/")
		if strings.HasSuffix(jobPart, "/stop") {
			jobID := strings.TrimSuffix(jobPart, "/stop")
			if jobID != "" && method == "POST" {
				return "StopModelInvocationJob", jobID, ""
			}
			return "", "", ""
		}
		if jobPart != "" && method == "GET" {
			return "GetModelInvocationJob", jobPart, ""
		}
		return "", "", ""
	}

	// /model/{modelId}/invoke
	if strings.HasPrefix(rest, "model/") {
		rest = strings.TrimPrefix(rest, "model/")
		slashIdx := strings.IndexByte(rest, '/')
		if slashIdx >= 0 {
			modelID := rest[:slashIdx]
			suffix := rest[slashIdx+1:]
			if suffix == "invoke" && method == "POST" {
				return "InvokeModel", modelID, ""
			}
		}
		return "", "", ""
	}

	// /guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply
	if !strings.HasPrefix(rest, "guardrail/") {
		return "", "", ""
	}
	rest = strings.TrimPrefix(rest, "guardrail/")
	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", "", ""
	}
	gID := rest[:slashIdx]
	rest = rest[slashIdx+1:]
	if !strings.HasPrefix(rest, "version/") {
		return "", "", ""
	}
	rest = strings.TrimPrefix(rest, "version/")
	slashIdx = strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", "", ""
	}
	ver := rest[:slashIdx]
	rest = rest[slashIdx+1:]
	if rest == "apply" && method == "POST" {
		return "ApplyGuardrail", gID, ver
	}
	return "", "", ""
}

func (p *BedrockRuntimePlugin) applyGuardrail(ctx *RequestContext, req *AWSRequest, guardrailID, _ string) (*AWSResponse, error) {
	var body struct {
		Source  string `json:"source"`
		Content []struct {
			Text *struct {
				Text string `json:"text"`
			} `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	// Extract input text from first text content item.
	inputText := ""
	for _, item := range body.Content {
		if item.Text != nil {
			inputText = item.Text.Text
			break
		}
	}

	goCtx := context.Background()
	blocklistKey := "guardrail:" + ctx.AccountID + "/" + guardrailID + "/blocklist"

	// Auto-create the guardrail state entry on first use (empty blocklist).
	data, err := p.state.Get(goCtx, bedrockRuntimeNamespace, blocklistKey)
	if err != nil || data == nil {
		// Auto-register with empty blocklist.
		empty, _ := json.Marshal([]string{})
		if putErr := p.state.Put(goCtx, bedrockRuntimeNamespace, blocklistKey, empty); putErr != nil {
			return nil, fmt.Errorf("applyGuardrail: put blocklist: %w", putErr)
		}
		data = empty
	}

	var blocklist []string
	_ = json.Unmarshal(data, &blocklist)

	usage := map[string]int{
		"topicPolicyUnitsProcessed":                    0,
		"contentPolicyUnitsProcessed":                  1,
		"wordPolicyUnitsProcessed":                     0,
		"sensitiveInformationPolicyUnitsProcessed":     0,
		"sensitiveInformationPolicyFreeUnitsProcessed": 0,
		"contextualGroundingPolicyUnitsProcessed":      0,
	}

	// Check blocklist.
	for _, term := range blocklist {
		if strings.Contains(inputText, term) {
			return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{
				"action": "GUARDRAIL_INTERVENED",
				"outputs": []map[string]string{
					{"text": "Sorry, I can't help with that."},
				},
				"assessments": []map[string]interface{}{
					{
						"topicPolicy": map[string]interface{}{
							"topics": []map[string]string{
								{"name": "blocked-topic", "type": "DENY", "action": "BLOCKED"},
							},
						},
					},
				},
				"usage": usage,
			})
		}
	}

	// Pass-through: echo input text.
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{
		"action": "NONE",
		"outputs": []map[string]string{
			{"text": inputText},
		},
		"assessments": []interface{}{},
		"usage":       usage,
	})
}

// invokeModel handles the InvokeModel operation, returning a seeded or default
// canned response.
func (p *BedrockRuntimePlugin) invokeModel(_ *RequestContext, _ *AWSRequest, modelID string) (*AWSResponse, error) {
	goCtx := context.Background()

	// Check for seeded response: exact modelID match, then "*" wildcard.
	for _, key := range []string{
		bedrockRuntimeCtrlResponseKey(modelID),
		bedrockRuntimeCtrlResponseKey("*"),
	} {
		data, err := p.state.Get(goCtx, bedrockRuntimeCtrlNamespace, key)
		if err == nil && data != nil {
			return &AWSResponse{
				StatusCode: http.StatusOK,
				Headers:    map[string]string{"Content-Type": "application/json"},
				Body:       data,
			}, nil
		}
	}

	// Default canned response (Claude Messages API format).
	defaultBody := map[string]interface{}{
		"id":          "msg-substrate-stub",
		"type":        "message",
		"role":        "assistant",
		"model":       modelID,
		"content":     []map[string]string{{"type": "text", "text": "Hello! This is a stubbed response from Substrate."}},
		"stop_reason": "end_turn",
		"usage":       map[string]int{"input_tokens": 0, "output_tokens": 0},
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, defaultBody)
}

// BedrockModelInvocationJob holds persisted state for an Amazon Bedrock
// control-plane batch inference job (ModelInvocationJob API).
type BedrockModelInvocationJob struct {
	// JobArn is the ARN of the model invocation job.
	JobArn string `json:"jobArn"`

	// JobName is the user-supplied name of the job.
	JobName string `json:"jobName"`

	// ModelID is the foundation model identifier used for the job.
	ModelID string `json:"modelId"`

	// RoleArn is the IAM service role granting Bedrock access to the S3 data.
	RoleArn string `json:"roleArn,omitempty"`

	// Status is the job status (Submitted, InProgress, Completed, Failed, Stopped, ...).
	Status string `json:"status"`

	// Message carries an optional status detail (e.g. a failure reason).
	Message string `json:"message,omitempty"`

	// InputDataConfig holds the S3 input configuration as supplied by the caller.
	InputDataConfig json.RawMessage `json:"inputDataConfig,omitempty"`

	// OutputDataConfig holds the S3 output configuration as supplied by the caller.
	OutputDataConfig json.RawMessage `json:"outputDataConfig,omitempty"`

	// SubmitTime is the epoch-seconds timestamp when the job was submitted.
	SubmitTime float64 `json:"submitTime"`

	// AccountID is the AWS account that owns the job.
	AccountID string `json:"accountID"`

	// Region is the AWS region where the job runs.
	Region string `json:"region"`
}

// bedrockModelInvocationJobKey returns the state key for a model invocation job.
func bedrockModelInvocationJobKey(acct, region, jobID string) string {
	return "job:" + acct + "/" + region + "/" + jobID
}

// bedrockModelInvocationJobIDsKey returns the state key for the per-account/region
// index of model invocation job IDs.
func bedrockModelInvocationJobIDsKey(acct, region string) string {
	return "job_ids:" + acct + "/" + region
}

// generateBedrockJobID generates a UUID-formatted model invocation job ID.
func generateBedrockJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// createModelInvocationJob handles CreateModelInvocationJob, storing a new batch
// inference job in the Submitted state.
func (p *BedrockRuntimePlugin) createModelInvocationJob(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobName          string          `json:"jobName"`
		ModelID          string          `json:"modelId"`
		RoleArn          string          `json:"roleArn"`
		InputDataConfig  json.RawMessage `json:"inputDataConfig"`
		OutputDataConfig json.RawMessage `json:"outputDataConfig"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.JobName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "jobName is required", HTTPStatus: http.StatusBadRequest}
	}

	jobID := generateBedrockJobID()
	jobArn := fmt.Sprintf("arn:aws:bedrock:%s:%s:model-invocation-job/%s", ctx.Region, ctx.AccountID, jobID)
	job := BedrockModelInvocationJob{
		JobArn:           jobArn,
		JobName:          body.JobName,
		ModelID:          body.ModelID,
		RoleArn:          body.RoleArn,
		Status:           "Submitted",
		InputDataConfig:  body.InputDataConfig,
		OutputDataConfig: body.OutputDataConfig,
		SubmitTime:       float64(p.tc.Now().UnixNano()) / 1e9,
		AccountID:        ctx.AccountID,
		Region:           ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("createModelInvocationJob: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, bedrockRuntimeNamespace, bedrockModelInvocationJobKey(ctx.AccountID, ctx.Region, jobID), data); err != nil {
		return nil, fmt.Errorf("createModelInvocationJob: put: %w", err)
	}
	updateStringIndex(goCtx, p.state, bedrockRuntimeNamespace, bedrockModelInvocationJobIDsKey(ctx.AccountID, ctx.Region), jobID)
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]string{"jobArn": jobArn})
}

// loadModelInvocationJob fetches a job by ID and applies any seeded status override.
func (p *BedrockRuntimePlugin) loadModelInvocationJob(ctx *RequestContext, jobID string) (*BedrockModelInvocationJob, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, bedrockRuntimeNamespace, bedrockModelInvocationJobKey(ctx.AccountID, ctx.Region, jobID))
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "model invocation job " + jobID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var job BedrockModelInvocationJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("loadModelInvocationJob: unmarshal: %w", err)
	}
	p.applySeededJobStatus(goCtx, jobID, &job)
	return &job, nil
}

// applySeededJobStatus overrides a job's status from a control-plane seed, if any
// (exact job-ID match first, then the "*" wildcard).
func (p *BedrockRuntimePlugin) applySeededJobStatus(goCtx context.Context, jobID string, job *BedrockModelInvocationJob) {
	for _, key := range []string{
		bedrockRuntimeCtrlJobStatusKey(jobID),
		bedrockRuntimeCtrlJobStatusKey("*"),
	} {
		data, err := p.state.Get(goCtx, bedrockRuntimeCtrlNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var seed struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}
		if json.Unmarshal(data, &seed) == nil && seed.Status != "" {
			job.Status = seed.Status
			job.Message = seed.Message
		}
		return
	}
}

// getModelInvocationJob handles GetModelInvocationJob.
func (p *BedrockRuntimePlugin) getModelInvocationJob(ctx *RequestContext, _ *AWSRequest, jobID string) (*AWSResponse, error) {
	job, err := p.loadModelInvocationJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, job)
}

// stopModelInvocationJob handles StopModelInvocationJob, transitioning the job to Stopped.
func (p *BedrockRuntimePlugin) stopModelInvocationJob(ctx *RequestContext, _ *AWSRequest, jobID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, bedrockRuntimeNamespace, bedrockModelInvocationJobKey(ctx.AccountID, ctx.Region, jobID))
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "model invocation job " + jobID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var job BedrockModelInvocationJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("stopModelInvocationJob: unmarshal: %w", err)
	}
	job.Status = "Stopped"
	updated, _ := json.Marshal(job)
	if err := p.state.Put(goCtx, bedrockRuntimeNamespace, bedrockModelInvocationJobKey(ctx.AccountID, ctx.Region, jobID), updated); err != nil {
		return nil, fmt.Errorf("stopModelInvocationJob: put: %w", err)
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{})
}

// listModelInvocationJobs handles ListModelInvocationJobs, returning summaries
// for all jobs in the account and region.
func (p *BedrockRuntimePlugin) listModelInvocationJobs(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, _ := loadStringIndex(goCtx, p.state, bedrockRuntimeNamespace, bedrockModelInvocationJobIDsKey(ctx.AccountID, ctx.Region))

	type summary struct {
		JobArn     string  `json:"jobArn"`
		JobName    string  `json:"jobName"`
		ModelID    string  `json:"modelId"`
		Status     string  `json:"status"`
		SubmitTime float64 `json:"submitTime"`
	}
	summaries := make([]summary, 0, len(ids))
	for _, id := range ids {
		job, err := p.loadModelInvocationJob(ctx, id)
		if err != nil {
			continue
		}
		summaries = append(summaries, summary{
			JobArn:     job.JobArn,
			JobName:    job.JobName,
			ModelID:    job.ModelID,
			Status:     job.Status,
			SubmitTime: job.SubmitTime,
		})
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{"invocationJobSummaries": summaries})
}

// bedrockRuntimeJSONResponse serializes v to JSON and returns an AWSResponse.
func bedrockRuntimeJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("bedrock-runtime json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
