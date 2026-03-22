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

// batchNamespace is the state namespace for AWS Batch.
const batchNamespace = "batch"

// BatchPlugin emulates the AWS Batch service.
// It handles job submission, description, termination, and listing, plus
// the prerequisite resource types (compute environments, job queues, job
// definitions) using the Batch REST/JSON API at /v1/... paths.
type BatchPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "batch".
func (p *BatchPlugin) Name() string { return batchNamespace }

// Initialize sets up the BatchPlugin with the provided configuration.
func (p *BatchPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for BatchPlugin.
func (p *BatchPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Batch REST/JSON request to the appropriate handler.
func (p *BatchPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, jobID := parseBatchOperation(req.Operation, req.Path)
	switch op {
	case "SubmitJob":
		return p.submitJob(ctx, req)
	case "DescribeJobs":
		return p.describeJobs(ctx, req)
	case "TerminateJob":
		return p.terminateJob(ctx, req, jobID)
	case "ListJobs":
		return p.listJobs(ctx, req)
	case "CreateComputeEnvironment":
		return p.createComputeEnvironment(ctx, req)
	case "CreateJobQueue":
		return p.createJobQueue(ctx, req)
	case "RegisterJobDefinition":
		return p.registerJobDefinition(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "BatchPlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseBatchOperation maps an HTTP method and path to a Batch operation name and
// optional resource ID.
// parseBatchOperation maps an HTTP method and path to a Batch operation name and
// optional resource ID. The method parameter is the HTTP verb (GET, POST, DELETE).
func parseBatchOperation(method, path string) (op, jobID string) {
	rest := strings.TrimPrefix(path, "/")
	switch {
	case rest == "v1/submitjob" && method == "POST":
		return "SubmitJob", ""
	case rest == "v1/jobs" && method == "POST":
		return "DescribeJobs", ""
	case rest == "v1/jobs" && method == "GET":
		return "ListJobs", ""
	case strings.HasPrefix(rest, "v1/jobs/") && method == "DELETE":
		return "TerminateJob", strings.TrimPrefix(rest, "v1/jobs/")
	case rest == "v1/computeenvironments" && method == "POST":
		return "CreateComputeEnvironment", ""
	case rest == "v1/jobqueues" && method == "POST":
		return "CreateJobQueue", ""
	case rest == "v1/jobdefinitions" && method == "POST":
		return "RegisterJobDefinition", ""
	}
	return "", ""
}

// BatchJob holds persisted state for an AWS Batch job.
type BatchJob struct {
	// JobID is the unique identifier for the job.
	JobID string `json:"jobId"`

	// JobName is the user-supplied name for the job.
	JobName string `json:"jobName"`

	// JobQueue is the job queue ARN or name the job was submitted to.
	JobQueue string `json:"jobQueue"`

	// JobDefinition is the job definition ARN or name.
	JobDefinition string `json:"jobDefinition"`

	// Status is the current job status.
	Status string `json:"status"`

	// StatusReason is an optional human-readable reason string.
	StatusReason string `json:"statusReason,omitempty"`

	// CreatedAt is the epoch-millisecond timestamp when the job was created.
	CreatedAt int64 `json:"createdAt"`

	// AccountID is the AWS account that owns the job.
	AccountID string `json:"accountID"`

	// Region is the AWS region in which the job runs.
	Region string `json:"region"`
}

func (p *BatchPlugin) submitJob(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobName       string `json:"jobName"`
		JobQueue      string `json:"jobQueue"`
		JobDefinition string `json:"jobDefinition"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.JobName == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "jobName is required", HTTPStatus: http.StatusBadRequest}
	}

	jobID := generateBatchJobID()
	job := BatchJob{
		JobID:         jobID,
		JobName:       body.JobName,
		JobQueue:      body.JobQueue,
		JobDefinition: body.JobDefinition,
		Status:        "SUCCEEDED",
		CreatedAt:     p.tc.Now().UnixNano() / int64(time.Millisecond),
		AccountID:     ctx.AccountID,
		Region:        ctx.Region,
	}

	data, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("submitJob: marshal: %w", err)
	}
	goCtx := context.Background()
	jobKey := "job:" + ctx.AccountID + "/" + ctx.Region + "/" + jobID
	if err := p.state.Put(goCtx, batchNamespace, jobKey, data); err != nil {
		return nil, fmt.Errorf("submitJob: put: %w", err)
	}
	idsKey := "job_ids:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, batchNamespace, idsKey, jobID)

	jobArn := fmt.Sprintf("arn:aws:batch:%s:%s:job/%s", ctx.Region, ctx.AccountID, jobID)
	return batchJSONResponse(http.StatusOK, map[string]string{
		"jobArn":  jobArn,
		"jobId":   jobID,
		"jobName": body.JobName,
	})
}

func (p *BatchPlugin) describeJobs(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Jobs []string `json:"jobs"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	var jobs []BatchJob
	for _, id := range body.Jobs {
		key := "job:" + ctx.AccountID + "/" + ctx.Region + "/" + id
		data, err := p.state.Get(goCtx, batchNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var job BatchJob
		if json.Unmarshal(data, &job) == nil {
			jobs = append(jobs, job)
		}
	}
	if jobs == nil {
		jobs = []BatchJob{}
	}
	return batchJSONResponse(http.StatusOK, map[string]interface{}{"jobs": jobs})
}

func (p *BatchPlugin) terminateJob(ctx *RequestContext, req *AWSRequest, jobID string) (*AWSResponse, error) {
	goCtx := context.Background()
	key := "job:" + ctx.AccountID + "/" + ctx.Region + "/" + jobID
	data, err := p.state.Get(goCtx, batchNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ClientException", Message: "job " + jobID + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var job BatchJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("terminateJob: unmarshal: %w", err)
	}

	var body struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(req.Body, &body)
	job.Status = "FAILED"
	if body.Reason != "" {
		job.StatusReason = body.Reason
	}

	updated, _ := json.Marshal(job)
	if err := p.state.Put(goCtx, batchNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("terminateJob: put: %w", err)
	}
	return batchJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *BatchPlugin) listJobs(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	idsKey := "job_ids:" + ctx.AccountID + "/" + ctx.Region
	ids, _ := loadStringIndex(goCtx, p.state, batchNamespace, idsKey)

	type jobSummary struct {
		JobID   string `json:"jobId"`
		JobName string `json:"jobName"`
		Status  string `json:"status"`
	}
	summaries := make([]jobSummary, 0, len(ids))
	for _, id := range ids {
		key := "job:" + ctx.AccountID + "/" + ctx.Region + "/" + id
		data, err := p.state.Get(goCtx, batchNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var job BatchJob
		if json.Unmarshal(data, &job) == nil {
			summaries = append(summaries, jobSummary{JobID: job.JobID, JobName: job.JobName, Status: job.Status})
		}
	}
	return batchJSONResponse(http.StatusOK, map[string]interface{}{"jobSummaryList": summaries})
}

func (p *BatchPlugin) createComputeEnvironment(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ComputeEnvironmentName string `json:"computeEnvironmentName"`
	}
	_ = json.Unmarshal(req.Body, &body)
	return batchJSONResponse(http.StatusOK, map[string]string{
		"computeEnvironmentArn":  "arn:aws:batch:us-east-1:000000000000:compute-environment/" + body.ComputeEnvironmentName,
		"computeEnvironmentName": body.ComputeEnvironmentName,
	})
}

func (p *BatchPlugin) createJobQueue(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobQueueName string `json:"jobQueueName"`
	}
	_ = json.Unmarshal(req.Body, &body)
	return batchJSONResponse(http.StatusOK, map[string]string{
		"jobQueueArn":  "arn:aws:batch:us-east-1:000000000000:job-queue/" + body.JobQueueName,
		"jobQueueName": body.JobQueueName,
	})
}

func (p *BatchPlugin) registerJobDefinition(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobDefinitionName string `json:"jobDefinitionName"`
	}
	_ = json.Unmarshal(req.Body, &body)
	return batchJSONResponse(http.StatusOK, map[string]interface{}{
		"jobDefinitionArn":  "arn:aws:batch:us-east-1:000000000000:job-definition/" + body.JobDefinitionName + ":1",
		"jobDefinitionName": body.JobDefinitionName,
		"revision":          1,
	})
}

// generateBatchJobID generates a random UUID-formatted job ID.
func generateBatchJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// batchJSONResponse serializes v to JSON and returns an AWSResponse.
func batchJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("batch json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
