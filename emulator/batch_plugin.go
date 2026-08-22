package emulator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
	op, jobID := parseBatchOperation(requestMethod(req), req.Path)
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
	case "DescribeComputeEnvironments":
		return p.describeComputeEnvironments(ctx, req)
	case "DescribeJobQueues":
		return p.describeJobQueues(ctx, req)
	case "DescribeJobDefinitions":
		return p.describeJobDefinitions(ctx, req)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseBatchOperation maps an HTTP method and path to a Batch operation name and
// optional resource ID.
// parseBatchOperation maps an HTTP method and path to a Batch operation name and
// optional resource ID. The method parameter is the HTTP verb (GET, POST, DELETE).
func parseBatchOperation(method, path string) (op, jobID string) {
	rest := strings.TrimPrefix(path, "/")
	switch {
	// SDK v2 uses operation-named paths (e.g. /v1/submitjob, /v1/describejobs).
	case rest == "v1/submitjob" && method == "POST":
		return "SubmitJob", ""
	case rest == "v1/describejobs" && method == "POST":
		return "DescribeJobs", ""
	case rest == "v1/terminatejob" && method == "POST":
		// SDK sends jobId in the request body; extracted by terminateJob handler.
		return "TerminateJob", ""
	case rest == "v1/createcomputeenvironment" && method == "POST":
		return "CreateComputeEnvironment", ""
	case rest == "v1/createjobqueue" && method == "POST":
		return "CreateJobQueue", ""
	case rest == "v1/registerjobdefinition" && method == "POST":
		return "RegisterJobDefinition", ""
	// Each create's describe counterpart. Their absence, alongside routed creates,
	// is what #530 reported: a resource could be created and not read back.
	case rest == "v1/describecomputeenvironments" && method == "POST":
		return "DescribeComputeEnvironments", ""
	case rest == "v1/describejobqueues" && method == "POST":
		return "DescribeJobQueues", ""
	case rest == "v1/describejobdefinitions" && method == "POST":
		return "DescribeJobDefinitions", ""
	// Legacy REST-style paths retained for backwards compatibility.
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

// BatchComputeEnvironment holds persisted state for an AWS Batch compute
// environment.
//
// The json tags are the wire names, which for Batch are the same in both
// directions: the API is camelCase and ComputeEnvironmentDetail's members are the
// ones a create accepts. Fields substrate does not interpret are carried as
// map[string]interface{} so a describe reports back what the caller sent rather
// than a lossy projection of it.
type BatchComputeEnvironment struct {
	// ComputeEnvironmentName is the caller-supplied name.
	ComputeEnvironmentName string `json:"computeEnvironmentName"`

	// ComputeEnvironmentARN is the environment's ARN, in the caller's partition,
	// Region and account.
	ComputeEnvironmentARN string `json:"computeEnvironmentArn"`

	// Type is MANAGED or UNMANAGED.
	Type string `json:"type"`

	// State is ENABLED or DISABLED.
	State string `json:"state"`

	// Status is the environment's status; substrate reports VALID.
	Status string `json:"status"`

	// StatusReason is the human-readable reason accompanying Status.
	StatusReason string `json:"statusReason,omitempty"`

	// ServiceRole is the IAM role Batch assumes, when the caller supplied one.
	ServiceRole string `json:"serviceRole,omitempty"`

	// UnmanagedvCPUs is the vCPU reservation for an unmanaged environment.
	UnmanagedvCPUs int `json:"unmanagedvCpus,omitempty"`

	// ECSClusterARN is the ECS cluster the environment reports, which is what an
	// unmanaged caller reads this operation to discover.
	ECSClusterARN string `json:"ecsClusterArn,omitempty"`

	// ComputeResources is the compute resource specification as sent.
	ComputeResources map[string]interface{} `json:"computeResources,omitempty"`

	// Tags are the environment's tags.
	Tags map[string]string `json:"tags,omitempty"`
}

// BatchJobQueue holds persisted state for an AWS Batch job queue.
type BatchJobQueue struct {
	// JobQueueName is the caller-supplied name.
	JobQueueName string `json:"jobQueueName"`

	// JobQueueARN is the queue's ARN, in the caller's partition, Region and account.
	JobQueueARN string `json:"jobQueueArn"`

	// State is ENABLED or DISABLED.
	State string `json:"state"`

	// Status is the queue's status; substrate reports VALID.
	Status string `json:"status"`

	// StatusReason is the human-readable reason accompanying Status.
	StatusReason string `json:"statusReason,omitempty"`

	// Priority is the queue's scheduling priority.
	Priority int `json:"priority"`

	// JobQueueType is EKS, ECS, ECS_FARGATE or SAGEMAKER_TRAINING.
	JobQueueType string `json:"jobQueueType,omitempty"`

	// SchedulingPolicyARN is the fair-share scheduling policy, when set.
	SchedulingPolicyARN string `json:"schedulingPolicyArn,omitempty"`

	// ComputeEnvironmentOrder is the ordered compute environment list as sent.
	ComputeEnvironmentOrder []map[string]interface{} `json:"computeEnvironmentOrder,omitempty"`

	// Tags are the queue's tags.
	Tags map[string]string `json:"tags,omitempty"`
}

// BatchJobDefinition holds persisted state for one revision of an AWS Batch job
// definition.
//
// Each registration of a name is a separate record: a job definition is versioned,
// and DescribeJobDefinitions addresses a revision as ${name}:${revision}.
type BatchJobDefinition struct {
	// JobDefinitionName is the caller-supplied name, without the revision suffix.
	JobDefinitionName string `json:"jobDefinitionName"`

	// JobDefinitionARN is this revision's ARN, ending in :${revision}.
	JobDefinitionARN string `json:"jobDefinitionArn"`

	// Revision is this registration's revision number, starting at 1.
	Revision int `json:"revision"`

	// Status is ACTIVE or INACTIVE.
	Status string `json:"status"`

	// Type is container or multinode.
	Type string `json:"type"`

	// ContainerProperties is the single-node container specification as sent.
	ContainerProperties map[string]interface{} `json:"containerProperties,omitempty"`

	// NodeProperties is the multi-node parallel specification as sent.
	NodeProperties map[string]interface{} `json:"nodeProperties,omitempty"`

	// ECSProperties is the ECS-specific specification as sent.
	ECSProperties map[string]interface{} `json:"ecsProperties,omitempty"`

	// EKSProperties is the EKS-specific specification as sent.
	EKSProperties map[string]interface{} `json:"eksProperties,omitempty"`

	// Parameters are the default parameter substitutions.
	Parameters map[string]string `json:"parameters,omitempty"`

	// RetryStrategy is the retry strategy as sent.
	RetryStrategy map[string]interface{} `json:"retryStrategy,omitempty"`

	// Timeout is the timeout configuration as sent.
	Timeout map[string]interface{} `json:"timeout,omitempty"`

	// PlatformCapabilities is EC2 or FARGATE.
	PlatformCapabilities []string `json:"platformCapabilities,omitempty"`

	// PropagateTags reports whether tags propagate to the ECS task.
	PropagateTags bool `json:"propagateTags,omitempty"`

	// SchedulingPriority is the fair-share scheduling priority.
	SchedulingPriority int `json:"schedulingPriority,omitempty"`

	// Tags are the job definition's tags.
	Tags map[string]string `json:"tags,omitempty"`
}

// batchARN builds a Batch ARN for a resource in the caller's Region and account.
//
// The ARN is what a caller passes to computeEnvironmentOrder, SubmitJob and the
// describe filters, so building it from the RequestContext rather than a fixed
// us-east-1/000000000000 is what makes those cross-references resolve for any other
// caller (#530). The partition is aws, matching submitJob's jobArn below and every
// other plugin's ARNs.
func batchARN(ctx *RequestContext, resource, name string) string {
	return fmt.Sprintf("arn:aws:batch:%s:%s:%s/%s", ctx.Region, ctx.AccountID, resource, name)
}

// batchDeterministicUUID derives a stable UUID-shaped suffix for a resource, so a
// synthesized ARN is the same on every read of the same resource and differs
// between resources.
func batchDeterministicUUID(ctx *RequestContext, resource, name string) string {
	sum := sha256.Sum256([]byte(ctx.AccountID + "/" + ctx.Region + "/" + resource + "/" + name))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

// batchClientError returns the ClientException Batch reports for a bad request.
// Every Batch operation documents exactly two errors, ClientException (400) and
// ServerException (500), so a parameter complaint is a ClientException.
func batchClientError(message string) *AWSError {
	return &AWSError{Code: "ClientException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// putBatchRecord persists a Batch resource and adds its name to the per-account,
// per-Region index the describe operations enumerate when no filter is given.
func (p *BatchPlugin) putBatchRecord(ctx *RequestContext, resource, name string, record interface{}) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("batch %s marshal: %w", resource, err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, batchNamespace, batchRecordKey(ctx, resource, name), data); err != nil {
		return fmt.Errorf("batch %s put: %w", resource, err)
	}
	updateStringIndex(goCtx, p.state, batchNamespace, batchIndexKey(ctx, resource), name)
	return nil
}

// batchRecordKey is the state key for one Batch resource.
func batchRecordKey(ctx *RequestContext, resource, name string) string {
	return resource + ":" + ctx.AccountID + "/" + ctx.Region + "/" + name
}

// batchIndexKey is the state key for a resource type's name index, scoped to the
// caller's account and Region so one substrate process serving several identities
// does not report another caller's resources.
func batchIndexKey(ctx *RequestContext, resource string) string {
	return resource + "_names:" + ctx.AccountID + "/" + ctx.Region
}

// batchNameFromIdentifier reduces a describe filter entry to the name the records
// are keyed by.
//
// Every describe takes "names or full Amazon Resource Name (ARN) entries", so an
// ARN is reduced to its resource segment. A job definition keeps its :revision
// suffix, because that suffix is part of what identifies the record.
func batchNameFromIdentifier(identifier string) string {
	if slash := strings.LastIndexByte(identifier, '/'); slash >= 0 {
		return identifier[slash+1:]
	}
	return identifier
}

// batchPage applies the maxResults/nextToken pagination every Batch describe
// documents to an already-filtered list of names, returning the page and the token
// to report.
//
// "If this parameter isn't used, then Describe… returns up to 100 results", so an
// absent or out-of-range maxResults is 100 rather than unbounded.
func batchPage(names []string, maxResults int, nextToken string) ([]string, string) {
	if maxResults < 1 || maxResults > 100 {
		maxResults = 100
	}
	offset := 0
	if nextToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(nextToken); err == nil {
			if n, err := strconv.Atoi(string(decoded)); err == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var out string
	if len(page) > maxResults {
		page = page[:maxResults]
		out = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + maxResults)))
	}
	return page, out
}

// batchDescribeRequest is the request body shape shared by the three resource
// describes. jobDefinitionName and status are only meaningful to
// DescribeJobDefinitions; the others ignore them.
type batchDescribeRequest struct {
	ComputeEnvironments []string `json:"computeEnvironments"`
	JobQueues           []string `json:"jobQueues"`
	JobDefinitions      []string `json:"jobDefinitions"`
	JobDefinitionName   string   `json:"jobDefinitionName"`
	Status              string   `json:"status"`
	MaxResults          int      `json:"maxResults"`
	NextToken           string   `json:"nextToken"`
}

// describeBatchResources is the body of all three resource describes: resolve the
// filter to a list of names, page it, and load each record.
//
// filter is the caller's list of names or ARNs; an empty filter means every
// resource of this type in the caller's account and Region, which is what each
// operation's reference specifies for the absent-filter case.
//
// A filter entry naming a resource that does not exist is skipped rather than
// refused: the operations describe "one or more of your compute environments" and
// document no not-found error, so an absent name yields an absent result.
func (p *BatchPlugin) describeBatchResources(
	ctx *RequestContext, resource string, filter []string, maxResults int, nextToken string,
) ([]json.RawMessage, string, error) {
	goCtx := context.Background()

	names := make([]string, 0, len(filter))
	if len(filter) > 0 {
		for _, entry := range filter {
			if name := batchNameFromIdentifier(entry); name != "" {
				names = append(names, name)
			}
		}
	} else {
		indexed, err := loadStringIndex(goCtx, p.state, batchNamespace, batchIndexKey(ctx, resource))
		if err != nil {
			return nil, "", fmt.Errorf("describe %s index: %w", resource, err)
		}
		names = indexed
	}

	page, out := batchPage(names, maxResults, nextToken)
	records := make([]json.RawMessage, 0, len(page))
	for _, name := range page {
		data, err := p.state.Get(goCtx, batchNamespace, batchRecordKey(ctx, resource, name))
		if err != nil {
			return nil, "", fmt.Errorf("describe %s get: %w", resource, err)
		}
		if data == nil {
			continue
		}
		records = append(records, data)
	}
	return records, out, nil
}

// batchDescribeResponse assembles a describe response, omitting nextToken when
// there are no further results. "This value is null when there are no more results
// to return", and a caller's paginator stops on its absence.
func batchDescribeResponse(member string, records []json.RawMessage, nextToken string) (*AWSResponse, error) {
	resp := map[string]interface{}{member: records}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}
	return batchJSONResponse(http.StatusOK, resp)
}

func (p *BatchPlugin) describeComputeEnvironments(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body batchDescribeRequest
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, batchClientError("invalid request body")
		}
	}
	records, nextToken, err := p.describeBatchResources(
		ctx, "compute-environment", body.ComputeEnvironments, body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}
	return batchDescribeResponse("computeEnvironments", records, nextToken)
}

func (p *BatchPlugin) describeJobQueues(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body batchDescribeRequest
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, batchClientError("invalid request body")
		}
	}
	records, nextToken, err := p.describeBatchResources(
		ctx, "job-queue", body.JobQueues, body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}
	return batchDescribeResponse("jobQueues", records, nextToken)
}

// describeJobDefinitions describes job definitions, which are versioned and so
// filter differently from the other two resources.
//
// jobDefinitions entries address a specific revision — "an ARN … :${Revision} or a
// short version using the form ${JobDefinitionName}:${Revision}" — while
// jobDefinitionName names every revision of one definition. status filters the
// result either way. The reference states jobDefinitions "can't be used with other
// parameters", so it wins outright when both are sent rather than being intersected.
func (p *BatchPlugin) describeJobDefinitions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body batchDescribeRequest
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, batchClientError("invalid request body")
		}
	}

	goCtx := context.Background()
	filter := body.JobDefinitions
	if len(filter) == 0 && body.JobDefinitionName != "" {
		// Every revision of the named definition, in registration order.
		indexed, err := loadStringIndex(goCtx, p.state, batchNamespace, batchIndexKey(ctx, "job-definition"))
		if err != nil {
			return nil, fmt.Errorf("describe job-definition index: %w", err)
		}
		for _, name := range indexed {
			if colon := strings.LastIndexByte(name, ':'); colon >= 0 && name[:colon] == body.JobDefinitionName {
				filter = append(filter, name)
			}
		}
		if filter == nil {
			// A name matching nothing must not fall through to the unfiltered branch,
			// which would report every definition in the account.
			return batchDescribeResponse("jobDefinitions", []json.RawMessage{}, "")
		}
	}

	records, nextToken, err := p.describeBatchResources(
		ctx, "job-definition", filter, body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}
	if body.Status != "" {
		records = batchFilterByStatus(records, body.Status)
	}
	return batchDescribeResponse("jobDefinitions", records, nextToken)
}

// batchFilterByStatus keeps the records whose status field equals status, which is
// how DescribeJobDefinitions' documented ACTIVE/INACTIVE filter selects revisions.
//
// It filters after pagination rather than before, so a page's size reflects the
// filter — matching the reference's ordering, in which status narrows the results a
// page describes.
func batchFilterByStatus(records []json.RawMessage, status string) []json.RawMessage {
	kept := make([]json.RawMessage, 0, len(records))
	for _, record := range records {
		var probe struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(record, &probe) != nil {
			continue
		}
		if probe.Status == status {
			kept = append(kept, record)
		}
	}
	return kept
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

	// SDK v2 sends jobId in the request body (POST /v1/terminatejob); legacy
	// REST-style callers pass it as a URL path segment.
	var body struct {
		JobID  string `json:"jobId"`
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(req.Body, &body)
	if jobID == "" {
		jobID = body.JobID
	}

	key := "job:" + ctx.AccountID + "/" + ctx.Region + "/" + jobID
	data, err := p.state.Get(goCtx, batchNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ClientException", Message: "job " + jobID + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var job BatchJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("terminateJob: unmarshal: %w", err)
	}

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

func (p *BatchPlugin) createComputeEnvironment(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ComputeEnvironmentName string                 `json:"computeEnvironmentName"`
		Type                   string                 `json:"type"`
		State                  string                 `json:"state"`
		ServiceRole            string                 `json:"serviceRole"`
		UnmanagedvCPUs         int                    `json:"unmanagedvCpus"`
		ComputeResources       map[string]interface{} `json:"computeResources"`
		Tags                   map[string]string      `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, batchClientError("invalid request body")
	}
	if body.ComputeEnvironmentName == "" {
		return nil, batchClientError("computeEnvironmentName is required")
	}
	// computeEnvironmentName and type are the two required parameters.
	if body.Type == "" {
		return nil, batchClientError("type is required")
	}

	env := BatchComputeEnvironment{
		ComputeEnvironmentName: body.ComputeEnvironmentName,
		ComputeEnvironmentARN:  batchARN(ctx, "compute-environment", body.ComputeEnvironmentName),
		Type:                   body.Type,
		// "A compute environment must be created in the ENABLED state", so an
		// omitted state is ENABLED rather than empty.
		State: body.State,
		// A substrate compute environment has no instances to bring up, so it is
		// immediately VALID — the state a caller's wait loop polls for.
		Status:           "VALID",
		StatusReason:     "ComputeEnvironment Healthy",
		ServiceRole:      body.ServiceRole,
		UnmanagedvCPUs:   body.UnmanagedvCPUs,
		ComputeResources: body.ComputeResources,
		Tags:             body.Tags,
		// ecsClusterArn is what DescribeComputeEnvironments exists to report for an
		// unmanaged environment, per the operation's own preamble.
		ECSClusterARN: fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s_Batch_%s",
			ctx.Region, ctx.AccountID, body.ComputeEnvironmentName,
			batchDeterministicUUID(ctx, "compute-environment", body.ComputeEnvironmentName)),
	}
	if env.State == "" {
		env.State = "ENABLED"
	}

	if err := p.putBatchRecord(ctx, "compute-environment", body.ComputeEnvironmentName, env); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]string{
		"computeEnvironmentArn":  env.ComputeEnvironmentARN,
		"computeEnvironmentName": env.ComputeEnvironmentName,
	})
}

func (p *BatchPlugin) createJobQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobQueueName            string                   `json:"jobQueueName"`
		State                   string                   `json:"state"`
		Priority                int                      `json:"priority"`
		JobQueueType            string                   `json:"jobQueueType"`
		SchedulingPolicyARN     string                   `json:"schedulingPolicyArn"`
		ComputeEnvironmentOrder []map[string]interface{} `json:"computeEnvironmentOrder"`
		Tags                    map[string]string        `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, batchClientError("invalid request body")
	}
	if body.JobQueueName == "" {
		return nil, batchClientError("jobQueueName is required")
	}

	queue := BatchJobQueue{
		JobQueueName:            body.JobQueueName,
		JobQueueARN:             batchARN(ctx, "job-queue", body.JobQueueName),
		State:                   body.State,
		Status:                  "VALID",
		StatusReason:            "JobQueue Healthy",
		Priority:                body.Priority,
		JobQueueType:            body.JobQueueType,
		SchedulingPolicyARN:     body.SchedulingPolicyARN,
		ComputeEnvironmentOrder: body.ComputeEnvironmentOrder,
		Tags:                    body.Tags,
	}
	if queue.State == "" {
		queue.State = "ENABLED"
	}

	if err := p.putBatchRecord(ctx, "job-queue", body.JobQueueName, queue); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]string{
		"jobQueueArn":  queue.JobQueueARN,
		"jobQueueName": queue.JobQueueName,
	})
}

// registerJobDefinition registers a job definition, assigning it the next
// revision for its name.
//
// A job definition is versioned: "each entry in the list can either be an ARN in
// the format arn:aws:batch:${Region}:${Account}:job-definition/${JobDefinitionName}:${Revision}
// or a short version using the form ${JobDefinitionName}:${Revision}". So every
// registration of a name is a distinct record keyed by name and revision, and the
// name's revisions are indexed together — a describe by bare name reports all of
// them, which is what makes the revision observable.
func (p *BatchPlugin) registerJobDefinition(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		JobDefinitionName    string                 `json:"jobDefinitionName"`
		Type                 string                 `json:"type"`
		ContainerProperties  map[string]interface{} `json:"containerProperties"`
		NodeProperties       map[string]interface{} `json:"nodeProperties"`
		ECSProperties        map[string]interface{} `json:"ecsProperties"`
		EKSProperties        map[string]interface{} `json:"eksProperties"`
		Parameters           map[string]string      `json:"parameters"`
		RetryStrategy        map[string]interface{} `json:"retryStrategy"`
		Timeout              map[string]interface{} `json:"timeout"`
		PlatformCapabilities []string               `json:"platformCapabilities"`
		PropagateTags        bool                   `json:"propagateTags"`
		SchedulingPriority   int                    `json:"schedulingPriority"`
		Tags                 map[string]string      `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, batchClientError("invalid request body")
	}
	if body.JobDefinitionName == "" {
		return nil, batchClientError("jobDefinitionName is required")
	}
	// jobDefinitionName and type are the two required parameters.
	if body.Type == "" {
		return nil, batchClientError("type is required")
	}

	goCtx := context.Background()
	revision, err := p.nextJobDefinitionRevision(goCtx, ctx, body.JobDefinitionName)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("%s:%d", body.JobDefinitionName, revision)

	def := BatchJobDefinition{
		JobDefinitionName: body.JobDefinitionName,
		JobDefinitionARN:  batchARN(ctx, "job-definition", name),
		Revision:          revision,
		// A newly registered job definition is ACTIVE. Nothing in substrate makes one
		// INACTIVE yet — DeregisterJobDefinition is not routed (#555) — but the status
		// is recorded rather than synthesized at read time so that it can be.
		Status:               "ACTIVE",
		Type:                 body.Type,
		ContainerProperties:  body.ContainerProperties,
		NodeProperties:       body.NodeProperties,
		ECSProperties:        body.ECSProperties,
		EKSProperties:        body.EKSProperties,
		Parameters:           body.Parameters,
		RetryStrategy:        body.RetryStrategy,
		Timeout:              body.Timeout,
		PlatformCapabilities: body.PlatformCapabilities,
		PropagateTags:        body.PropagateTags,
		SchedulingPriority:   body.SchedulingPriority,
		Tags:                 body.Tags,
	}

	if err := p.putBatchRecord(ctx, "job-definition", name, def); err != nil {
		return nil, err
	}
	return batchJSONResponse(http.StatusOK, map[string]interface{}{
		"jobDefinitionArn":  def.JobDefinitionARN,
		"jobDefinitionName": def.JobDefinitionName,
		"revision":          revision,
	})
}

// nextJobDefinitionRevision reports the revision to assign to a new registration
// of name: one past the highest already recorded, starting at 1.
//
// It reads the name's own revision index rather than counting records, so a
// revision number is never reused even if an intermediate revision is one day
// removed.
func (p *BatchPlugin) nextJobDefinitionRevision(goCtx context.Context, ctx *RequestContext, name string) (int, error) {
	key := "job-definition_revisions:" + ctx.AccountID + "/" + ctx.Region + "/" + name
	data, err := p.state.Get(goCtx, batchNamespace, key)
	if err != nil {
		return 0, fmt.Errorf("nextJobDefinitionRevision get: %w", err)
	}
	highest := 0
	if data != nil {
		if err := json.Unmarshal(data, &highest); err != nil {
			return 0, fmt.Errorf("nextJobDefinitionRevision unmarshal: %w", err)
		}
	}
	highest++
	next, err := json.Marshal(highest)
	if err != nil {
		return 0, fmt.Errorf("nextJobDefinitionRevision marshal: %w", err)
	}
	if err := p.state.Put(goCtx, batchNamespace, key, next); err != nil {
		return 0, fmt.Errorf("nextJobDefinitionRevision put: %w", err)
	}
	return highest, nil
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
