package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
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

// BedrockRuntimePlugin emulates the Amazon Bedrock Runtime service.
// It handles ApplyGuardrail for the bedrock-runtime host, supporting
// pass-through (action NONE) and blocklist-based intervention
// (action GUARDRAIL_INTERVENED). Guardrails are auto-created on first use.
type BedrockRuntimePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController

	// seedMu serializes a job read's observation of its countdown, so two reads cannot spend
	// the same observation (#1174, the shared rule of emulator/progression.go).
	seedMu sync.Mutex
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
	op, resourceID, version := parseBedrockRuntimeOperation(requestMethod(req), req.Path)
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
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
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
		return nil, bedrockInvalidBody()
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
		empty, err := json.Marshal([]string{})
		if err != nil {
			return nil, fmt.Errorf("bedrock applyGuardrail marshal: %w", err)
		}
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

// bedrockJobIDChars is the alphabet Bedrock publishes a batch-inference job ID in: `[a-z0-9]`, from
// the `jobArn` pattern on API_CreateModelInvocationJob and API_GetModelInvocationJob.
const bedrockJobIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// generateBedrockJobID mints a model-invocation job ID from m, derived from the request id so a
// replayed CreateModelInvocationJob reports the ARN the recording reported (#856).
//
// This is the tier's one *shape* change, and it is a fidelity fix rather than a consequence of
// deriving the value. Bedrock publishes `jobArn` as
// `arn:aws…:model-invocation-job/[a-z0-9]{12}` and `jobIdentifier` as that ARN or a bare
// `[a-z0-9]{12}`, so the published alphabet **excludes the hyphen** and the length is twelve — and
// the UUID substrate minted here satisfied neither. #856's rule is not to change the bytes a caller
// sees, but that rule exists to protect a rendering the API model permits; it cannot preserve one
// the model forbids. Twelve characters over the whole published set is what a caller validating the
// ARN, or handing the bare ID back to `GetModelInvocationJob`, can accept.
//
// The `GetModelInvocationJob` sample request shows `BATCHJOB1234`, which the pattern it sits beside
// would reject. The pattern is the model, so the rendering follows the pattern.
func generateBedrockJobID(m *IDMint) string {
	return m.Chars(12, bedrockJobIDChars)
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
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, bedrockInvalidBody()
	}
	// API_CreateModelInvocationJob marks five members Required: Yes (#1174).
	for _, m := range []struct {
		name    string
		present bool
	}{
		{"jobName", body.JobName != ""},
		{"modelId", body.ModelID != ""},
		{"roleArn", body.RoleArn != ""},
		{"inputDataConfig", bedrockJSONPresent(body.InputDataConfig)},
		{"outputDataConfig", bedrockJSONPresent(body.OutputDataConfig)},
	} {
		if !m.present {
			return nil, bedrockValidationError(m.name + " is required")
		}
	}

	jobID := generateBedrockJobID(ctx.IDs)
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
	if err := updateStringIndex(goCtx, p.state, bedrockRuntimeNamespace, bedrockModelInvocationJobIDsKey(ctx.AccountID, ctx.Region), jobID); err != nil {
		return nil, fmt.Errorf("bedrock createModelInvocationJob index: %w", err)
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]string{"jobArn": jobArn})
}

// loadModelInvocationJob fetches a job's stored record by ID, answering the published
// ResourceNotFoundException when there is none. It applies no seed; see
// [BedrockRuntimePlugin.observeModelInvocationJob].
func (p *BedrockRuntimePlugin) loadModelInvocationJob(ctx *RequestContext, jobID string) (*BedrockModelInvocationJob, error) {
	data, err := p.state.Get(context.Background(), bedrockRuntimeNamespace, bedrockModelInvocationJobKey(ctx.AccountID, ctx.Region, jobID))
	if err != nil {
		return nil, fmt.Errorf("loadModelInvocationJob: get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "model invocation job " + jobID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var job BedrockModelInvocationJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("loadModelInvocationJob: unmarshal: %w", err)
	}
	return &job, nil
}

// observeModelInvocationJob loads a job and reports it as one read observes it: the governing
// countdown is spent once, and the record is not rewritten (#1174).
func (p *BedrockRuntimePlugin) observeModelInvocationJob(ctx *RequestContext, jobID string) (*BedrockModelInvocationJob, error) {
	job, err := p.loadModelInvocationJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	view, err := p.bedrockJobObservedStatus(context.Background(), jobID, job, true)
	if err != nil {
		return nil, err
	}
	job.Status, job.Message = view.status, view.message
	return job, nil
}

// getModelInvocationJob handles GetModelInvocationJob.
func (p *BedrockRuntimePlugin) getModelInvocationJob(ctx *RequestContext, _ *AWSRequest, jobID string) (*AWSResponse, error) {
	job, err := p.observeModelInvocationJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, bedrockInvocationJobToWire(*job))
}

// stopModelInvocationJob handles StopModelInvocationJob.
//
// A job in a terminal status (Completed, Failed, Stopped, PartiallyCompleted, Expired) answers the
// page's ConflictException at 400; before #1174 the stop overwrote it with Stopped. Any other job
// is left `Stopping`, which the next reads report until its stop countdown ends and they report
// `Stopped`; see bedrock_job_progression.go. A job already stopping answers success and is left as
// it is, so a repeated stop neither fails nor restarts the countdown. The precondition peeks: a
// refused stop spends no observation.
func (p *BedrockRuntimePlugin) stopModelInvocationJob(ctx *RequestContext, _ *AWSRequest, jobID string) (*AWSResponse, error) {
	goCtx := context.Background()
	job, err := p.loadModelInvocationJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	view, err := p.bedrockJobObservedStatus(goCtx, jobID, job, false)
	if err != nil {
		return nil, err
	}
	if bedrockJobIsTerminal(view.status) {
		return nil, &AWSError{
			Code:       "ConflictException",
			Message:    fmt.Sprintf("model invocation job %s is %s and cannot be stopped", jobID, view.status),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if view.status == "Stopping" {
		return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{})
	}
	job.Status = "Stopping"
	updated, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("bedrock stopModelInvocationJob marshal: %w", err)
	}
	if err := p.state.Put(goCtx, bedrockRuntimeNamespace, bedrockModelInvocationJobKey(ctx.AccountID, ctx.Region, jobID), updated); err != nil {
		return nil, fmt.Errorf("stopModelInvocationJob: put: %w", err)
	}
	if err := bedrockJobStopProgressions.reset(goCtx, p.state, jobID); err != nil {
		return nil, fmt.Errorf("stopModelInvocationJob: %w", err)
	}
	return bedrockRuntimeJSONResponse(http.StatusOK, map[string]interface{}{})
}

// listModelInvocationJobs handles ListModelInvocationJobs, returning summaries
// for all jobs in the account and region.
func (p *BedrockRuntimePlugin) listModelInvocationJobs(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, bedrockRuntimeNamespace, bedrockModelInvocationJobIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("listModelInvocationJobs: load index: %w", err)
	}

	summaries := make([]bedrockInvocationJobOut, 0, len(ids))
	for _, id := range ids {
		job, err := p.observeModelInvocationJob(ctx, id)
		var awsErr *AWSError
		if errors.As(err, &awsErr) && awsErr.Code == "ResourceNotFoundException" {
			continue // an index entry whose record is gone
		}
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, bedrockInvocationJobToWire(*job))
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

// bedrockJSONPresent reports whether a JSON request member was sent with a value.
func bedrockJSONPresent(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null"
}
