package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	// seedMu serializes the job-run and application progressions' read-modify-write; see
	// [progression.observe].
	seedMu sync.Mutex
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
	op, appID, runID := parseEMRServerlessOperation(requestMethod(req), req.Path)
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
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseEMRServerlessOperation maps an HTTP method and path to an EMR Serverless
// operation name plus optional appID and runID.
//
// The path is matched segment by segment against the published URIs (#1205):
//
//	POST   /applications
//	GET    /applications/{applicationId}
//	DELETE /applications/{applicationId}
//	POST   /applications/{applicationId}/jobruns
//	GET    /applications/{applicationId}/jobruns
//	GET    /applications/{applicationId}/jobruns/{jobRunId}
//	DELETE /applications/{applicationId}/jobruns/{jobRunId}
//
// It used to search for the substring "/jobruns", so /applications/ab/jobrunsbad routed as
// ListJobRuns, /applications/jobruns routed with an empty application ID, and
// /applications/ab/cd/jobruns read the application ID as "ab/cd". Each ID is now one segment and
// each literal is a whole segment at its published position, so any other shape answers the
// unknown-operation refusal. An empty ID segment (/applications/) is not a match either: the
// pages publish a minimum length of 1 for both IDs.
func parseEMRServerlessOperation(method, path string) (op, appID, runID string) {
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if segs[0] != "applications" {
		return "", "", ""
	}
	for _, seg := range segs[1:] {
		if seg == "" {
			return "", "", ""
		}
	}
	switch len(segs) {
	case 1:
		if method == http.MethodPost {
			return "CreateApplication", "", ""
		}
	case 2:
		switch method {
		case http.MethodGet:
			return "GetApplication", segs[1], ""
		case http.MethodDelete:
			return "DeleteApplication", segs[1], ""
		}
	case 3:
		if segs[2] != "jobruns" {
			return "", "", ""
		}
		switch method {
		case http.MethodPost:
			return "StartJobRun", segs[1], ""
		case http.MethodGet:
			return "ListJobRuns", segs[1], ""
		}
	case 4:
		if segs[2] != "jobruns" {
			return "", "", ""
		}
		switch method {
		case http.MethodGet:
			return "GetJobRun", segs[1], segs[3]
		case http.MethodDelete:
			return "CancelJobRun", segs[1], segs[3]
		}
	}
	return "", "", ""
}

// EMRServerlessApp holds persisted state for an EMR Serverless application.
//
// The dates are named Created and Updated rather than CreatedAt and UpdatedAt because they are
// published members (`createdAt`/`updatedAt`, both Required on API_Application), not substrate's
// bookkeeping; scripts/check-wire-bookkeeping.sh keys on the Go identifier. A record written before
// #1199 holds neither, and renders both as null.
type EMRServerlessApp struct {
	// ApplicationID is the unique identifier for the application.
	ApplicationID string `json:"applicationId"`

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

	// Created is when the application was created, published as createdAt.
	Created time.Time `json:"createdAt,omitzero"`

	// Updated is when the application was last updated, published as updatedAt.
	Updated time.Time `json:"updatedAt,omitzero"`

	// Architecture is the CPU architecture the request named, if any.
	Architecture string `json:"architecture,omitempty"`

	// Tags are the tags the request assigned.
	Tags map[string]string `json:"tags,omitempty"`

	// InitialCapacity, MaximumCapacity and NetworkConfiguration are recorded verbatim as sent: they
	// are configuration a caller reads back, not anything substrate acts on.
	InitialCapacity      json.RawMessage `json:"initialCapacity,omitempty"`
	MaximumCapacity      json.RawMessage `json:"maximumCapacity,omitempty"`
	NetworkConfiguration json.RawMessage `json:"networkConfiguration,omitempty"`

	// ClientToken is the idempotency token that created the application.
	ClientToken string `json:"clientToken,omitempty"`

	// RequestDigest fingerprints the create request without its clientToken, so a resubmission
	// with the same token can be told apart from one with different parameters.
	RequestDigest string `json:"requestDigest,omitempty"`
}

// EMRServerlessJobRun holds persisted state for an EMR Serverless job run.
//
// Created and Updated are named as on [EMRServerlessApp], for the same reason.
type EMRServerlessJobRun struct {
	// ApplicationID is the application this job run belongs to.
	ApplicationID string `json:"applicationId"`

	// JobRunID is the unique identifier for the job run.
	JobRunID string `json:"jobRunId"`

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

	// Created is when the job run was created, published as createdAt.
	Created time.Time `json:"createdAt,omitzero"`

	// Updated is when the job run was last updated, published as updatedAt.
	Updated time.Time `json:"updatedAt,omitzero"`

	// CreatedBy is the ARN of the caller that started the run.
	CreatedBy string `json:"createdBy,omitempty"`

	// ExecutionRole is the executionRoleArn the request named.
	ExecutionRole string `json:"executionRole,omitempty"`

	// ReleaseLabel is the release of the application the run was started on.
	ReleaseLabel string `json:"releaseLabel,omitempty"`

	// JobDriver is the jobDriver the request sent, verbatim. It is the record of what was submitted;
	// substrate runs nothing.
	JobDriver json.RawMessage `json:"jobDriver,omitempty"`

	// Mode is the run's mode, BATCH unless the request named STREAMING.
	Mode string `json:"mode,omitempty"`

	// Tags are the tags the request assigned.
	Tags map[string]string `json:"tags,omitempty"`

	// ClientToken is the idempotency token that started the run.
	ClientToken string `json:"clientToken,omitempty"`

	// RequestDigest fingerprints the start request without its clientToken.
	RequestDigest string `json:"requestDigest,omitempty"`
}

// The published constraints the handlers below enforce, each quoted from its operation's page.
var (
	emrClientTokenPattern   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	emrReleaseLabelPattern  = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,64}$`)
	emrAppNamePattern       = regexp.MustCompile(`^[A-Za-z0-9._/#-]{1,64}$`)
	emrExecutionRolePattern = regexp.MustCompile(`^arn:(aws[a-zA-Z0-9-]*):iam::([0-9]{12}):(role((\x{002F})|(\x{002F}[\x{0021}-\x{007F}]+\x{002F}))[\w+=,.@-]+)$`)
	emrNonBlankPattern      = regexp.MustCompile(`\S`)
	emrNextTokenPattern     = regexp.MustCompile(`^[A-Za-z0-9_=-]+$`)
)

// emrJobRunStates are API_JobRun's published state values, in the page's order.
var emrJobRunStates = map[string]bool{
	"SUBMITTED": true, "PENDING": true, "SCHEDULED": true, "RUNNING": true, "SUCCESS": true,
	"FAILED": true, "CANCELLING": true, "CANCELLED": true, "QUEUED": true,
}

// emrValidation is the refusal every EMR Serverless page publishes for input that fails its
// constraints: ValidationException/400.
func emrValidation(msg string) *AWSError {
	return &AWSError{Code: "ValidationException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// emrNotFound is the published ResourceNotFoundException/404.
func emrNotFound(msg string) *AWSError {
	return &AWSError{Code: "ResourceNotFoundException", Message: msg, HTTPStatus: http.StatusNotFound}
}

// emrClientTokenConflict refuses a clientToken reused with different parameters.
//
// Both create pages say only that the token's "value must be unique for each request" and publish
// ConflictException/409, "The request could not be processed because of conflict in the current
// state of the resource". The contract substrate applies is the AWS idempotency-token one: the same
// token with the same parameters answers the resource it created, and the same token with different
// parameters is the conflict. The message is substrate's wording.
func emrClientTokenConflict(kind string) *AWSError {
	return &AWSError{
		Code:       "ConflictException",
		Message:    "the client token was already used to create a different " + kind,
		HTTPStatus: http.StatusConflict,
	}
}

// emrRequestDigest fingerprints a JSON request body with its clientToken removed, so two requests
// that differ only in token, or in nothing, compare equal. A body that is not an object digests
// as-is.
func emrRequestDigest(body []byte) string {
	var obj map[string]json.RawMessage
	canonical := body
	if json.Unmarshal(body, &obj) == nil {
		delete(obj, "clientToken")
		if b, err := json.Marshal(obj); err == nil {
			canonical = b
		}
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// requireEMRClientToken checks the Required clientToken against its published 1–64 characters of
// `[A-Za-z0-9._-]`.
func requireEMRClientToken(token string) error {
	if token == "" {
		return emrValidation("clientToken is required")
	}
	if !emrClientTokenPattern.MatchString(token) {
		return emrValidation("clientToken must be 1-64 characters of [A-Za-z0-9._-]")
	}
	return nil
}

// createApplication handles CreateApplication.
//
// API_CreateApplication marks clientToken, releaseLabel and type Required: Yes, and an absent one is
// refused ValidationException/400 rather than defaulted (#1197). Their published Length Constraints
// and Patterns are enforced, as are name's and architecture's Valid Values. The tags map's key and
// value patterns are not enforced; the members are recorded as sent.
func (p *EMRServerlessPlugin) createApplication(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ClientToken          string            `json:"clientToken"`
		Name                 string            `json:"name"`
		Type                 string            `json:"type"`
		ReleaseLabel         string            `json:"releaseLabel"`
		Architecture         string            `json:"architecture"`
		Tags                 map[string]string `json:"tags"`
		InitialCapacity      json.RawMessage   `json:"initialCapacity"`
		MaximumCapacity      json.RawMessage   `json:"maximumCapacity"`
		NetworkConfiguration json.RawMessage   `json:"networkConfiguration"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, emrInvalidBody()
	}
	if err := requireEMRClientToken(body.ClientToken); err != nil {
		return nil, err
	}
	switch {
	case body.ReleaseLabel == "":
		return nil, emrValidation("releaseLabel is required")
	case !emrReleaseLabelPattern.MatchString(body.ReleaseLabel):
		return nil, emrValidation("releaseLabel must be 1-64 characters of [A-Za-z0-9._/-]")
	case body.Type == "":
		return nil, emrValidation("type is required")
	case len(body.Type) > 64:
		return nil, emrValidation("type must be at most 64 characters")
	case body.Name != "" && !emrAppNamePattern.MatchString(body.Name):
		return nil, emrValidation("name must be 1-64 characters of [A-Za-z0-9._/#-]")
	case body.Architecture != "" && body.Architecture != "ARM64" && body.Architecture != "X86_64":
		return nil, emrValidation("architecture must be one of ARM64, X86_64")
	}

	goCtx := context.Background()
	digest := emrRequestDigest(req.Body)
	tokenKey := "app_token:" + ctx.AccountID + "/" + ctx.Region + "/" + body.ClientToken
	if prior, err := p.state.Get(goCtx, emrServerlessNamespace, tokenKey); err != nil {
		return nil, fmt.Errorf("createApplication: token lookup: %w", err)
	} else if prior != nil {
		app, err := p.loadApplication(goCtx, ctx, string(prior))
		if err != nil {
			return nil, err
		}
		if app != nil {
			if app.RequestDigest != digest {
				return nil, emrClientTokenConflict("application")
			}
			return emrServerlessJSONResponse(http.StatusOK, emrCreateApplicationOut(*app))
		}
	}

	appID := generateEMRServerlessAppID(ctx.IDs)
	arn := fmt.Sprintf("arn:aws:emr-serverless:%s:%s:/applications/%s", ctx.Region, ctx.AccountID, appID)
	now := p.tc.Now().UTC()
	app := EMRServerlessApp{
		ApplicationID:        appID,
		Name:                 body.Name,
		Type:                 body.Type,
		ReleaseLabel:         body.ReleaseLabel,
		State:                "CREATED",
		Arn:                  arn,
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
		Created:              now,
		Updated:              now,
		Architecture:         body.Architecture,
		Tags:                 body.Tags,
		InitialCapacity:      body.InitialCapacity,
		MaximumCapacity:      body.MaximumCapacity,
		NetworkConfiguration: body.NetworkConfiguration,
		ClientToken:          body.ClientToken,
		RequestDigest:        digest,
	}

	appKey := "app:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	data, err := json.Marshal(app)
	if err != nil {
		return nil, fmt.Errorf("createApplication: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, emrServerlessNamespace, appKey, data); err != nil {
		return nil, fmt.Errorf("createApplication: put: %w", err)
	}
	if err := p.state.Put(goCtx, emrServerlessNamespace, tokenKey, []byte(appID)); err != nil {
		return nil, fmt.Errorf("createApplication: put token: %w", err)
	}
	idsKey := "app_ids:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, emrServerlessNamespace, idsKey, appID)
	return emrServerlessJSONResponse(http.StatusOK, emrCreateApplicationOut(app))
}

// emrCreateApplicationOut is CreateApplication's published response: applicationId, arn and name.
func emrCreateApplicationOut(app EMRServerlessApp) map[string]string {
	out := map[string]string{"applicationId": app.ApplicationID, "arn": app.Arn}
	if app.Name != "" {
		out["name"] = app.Name
	}
	return out
}

// loadApplication reads one application, answering nil when it does not exist and an error when
// the store fails.
func (p *EMRServerlessPlugin) loadApplication(goCtx context.Context, ctx *RequestContext, appID string) (*EMRServerlessApp, error) {
	appKey := "app:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	data, err := p.state.Get(goCtx, emrServerlessNamespace, appKey)
	if err != nil {
		return nil, fmt.Errorf("emrserverless load application: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var app EMRServerlessApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, fmt.Errorf("emrserverless load application: unmarshal: %w", err)
	}
	return &app, nil
}

// loadJobRun reads one job run, answering nil when it does not exist and an error when the store
// fails.
func (p *EMRServerlessPlugin) loadJobRun(goCtx context.Context, ctx *RequestContext, appID, runID string) (*EMRServerlessJobRun, error) {
	runKey := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + runID
	data, err := p.state.Get(goCtx, emrServerlessNamespace, runKey)
	if err != nil {
		return nil, fmt.Errorf("emrserverless load job run: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var run EMRServerlessJobRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("emrserverless load job run: unmarshal: %w", err)
	}
	return &run, nil
}

func (p *EMRServerlessPlugin) getApplication(ctx *RequestContext, _ *AWSRequest, appID string) (*AWSResponse, error) {
	app, err := p.loadApplication(context.Background(), ctx, appID)
	if err != nil {
		return nil, err
	}
	if app == nil {
		return nil, emrNotFound("application " + appID + " not found")
	}
	observed, err := p.observeApplication(*app)
	if err != nil {
		return nil, err
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]interface{}{"application": emrServerlessAppToWire(observed)})
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

// startJobRun handles StartJobRun.
//
// API_StartJobRun marks clientToken and executionRoleArn Required: Yes; an absent or malformed one is
// ValidationException/400 (#1197). The application must exist (ResourceNotFoundException/404, which
// the page publishes and which substrate used to have no site for). jobDriver is Required: No on the
// request, though API_JobRun marks it Required on the answer: a run started without one has no
// jobDriver to report, and none is invented.
func (p *EMRServerlessPlugin) startJobRun(ctx *RequestContext, req *AWSRequest, appID string) (*AWSResponse, error) {
	var body struct {
		ClientToken      string            `json:"clientToken"`
		ExecutionRoleArn string            `json:"executionRoleArn"`
		Name             *string           `json:"name"`
		Mode             string            `json:"mode"`
		JobDriver        json.RawMessage   `json:"jobDriver"`
		Tags             map[string]string `json:"tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, emrInvalidBody()
		}
	}
	if err := requireEMRClientToken(body.ClientToken); err != nil {
		return nil, err
	}
	switch {
	case body.ExecutionRoleArn == "":
		return nil, emrValidation("executionRoleArn is required")
	case len(body.ExecutionRoleArn) < 20 || len(body.ExecutionRoleArn) > 2048 || !emrExecutionRolePattern.MatchString(body.ExecutionRoleArn):
		return nil, emrValidation("executionRoleArn must be an IAM role ARN")
	case body.Name != nil && (len(*body.Name) < 1 || len(*body.Name) > 256 || !emrNonBlankPattern.MatchString(*body.Name)):
		return nil, emrValidation("name must be 1-256 characters and not blank")
	case body.Mode != "" && body.Mode != "BATCH" && body.Mode != "STREAMING":
		return nil, emrValidation("mode must be one of BATCH, STREAMING")
	}

	goCtx := context.Background()
	app, err := p.loadApplication(goCtx, ctx, appID)
	if err != nil {
		return nil, err
	}
	if app == nil {
		return nil, emrNotFound("application " + appID + " not found")
	}

	digest := emrRequestDigest(req.Body)
	tokenKey := "jobrun_token:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + body.ClientToken
	if prior, err := p.state.Get(goCtx, emrServerlessNamespace, tokenKey); err != nil {
		return nil, fmt.Errorf("startJobRun: token lookup: %w", err)
	} else if prior != nil {
		run, err := p.loadJobRun(goCtx, ctx, appID, string(prior))
		if err != nil {
			return nil, err
		}
		if run != nil {
			if run.RequestDigest != digest {
				return nil, emrClientTokenConflict("job run")
			}
			return emrServerlessJSONResponse(http.StatusOK, map[string]string{
				"applicationId": appID, "jobRunId": run.JobRunID, "arn": run.Arn,
			})
		}
	}

	runID := generateEMRServerlessRunID(ctx.IDs)
	arn := fmt.Sprintf("arn:aws:emr-serverless:%s:%s:/applications/%s/jobruns/%s",
		ctx.Region, ctx.AccountID, appID, runID)
	mode := body.Mode
	if mode == "" {
		mode = "BATCH"
	}
	name := ""
	if body.Name != nil {
		name = *body.Name
	}
	now := p.tc.Now().UTC()
	run := EMRServerlessJobRun{
		ApplicationID: appID,
		JobRunID:      runID,
		Name:          name,
		State:         "SUCCESS",
		Arn:           arn,
		AccountID:     ctx.AccountID,
		Region:        ctx.Region,
		Created:       now,
		Updated:       now,
		CreatedBy:     emrCreatedBy(ctx),
		ExecutionRole: body.ExecutionRoleArn,
		ReleaseLabel:  app.ReleaseLabel,
		JobDriver:     body.JobDriver,
		Mode:          mode,
		Tags:          body.Tags,
		ClientToken:   body.ClientToken,
		RequestDigest: digest,
	}

	runKey := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + runID
	data, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("startJobRun: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, emrServerlessNamespace, runKey, data); err != nil {
		return nil, fmt.Errorf("startJobRun: put: %w", err)
	}
	if err := p.state.Put(goCtx, emrServerlessNamespace, tokenKey, []byte(runID)); err != nil {
		return nil, fmt.Errorf("startJobRun: put token: %w", err)
	}
	runIDsKey := "jobrun_ids:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	updateStringIndex(goCtx, p.state, emrServerlessNamespace, runIDsKey, runID)
	return emrServerlessJSONResponse(http.StatusOK, map[string]string{
		"applicationId": appID,
		"jobRunId":      runID,
		"arn":           arn,
	})
}

// emrCreatedBy is the ARN API_JobRun's createdBy reports: the caller's principal when the request
// carries one, and the account root otherwise, which satisfies the published
// `arn:(aws…):(iam|sts)::(\d{12})?:[\w/+=,.@-]+` pattern.
func emrCreatedBy(ctx *RequestContext) string {
	if ctx.Principal != nil && ctx.Principal.ARN != "" {
		return ctx.Principal.ARN
	}
	return "arn:aws:iam::" + ctx.AccountID + ":root"
}

func (p *EMRServerlessPlugin) getJobRun(ctx *RequestContext, _ *AWSRequest, appID, runID string) (*AWSResponse, error) {
	run, err := p.loadJobRun(context.Background(), ctx, appID, runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, emrNotFound("job run " + runID + " not found")
	}
	observed, details, err := p.observeJobRun(*run)
	if err != nil {
		return nil, err
	}
	out := emrServerlessJobRunToWire(observed)
	if details != "" {
		out.StateDetails = details
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]interface{}{"jobRun": out})
}

// cancelJobRun handles CancelJobRun.
//
// The run is recorded CANCELLED, the spelling API_JobRun's state enum publishes; it used to be
// CANCELED, which no page lists, so a typed-enum SDK read the state as unknown on the one operation
// whose purpose is producing it (#1198). The cancel restarts the run's progression countdown, so under
// a seed the run reports CANCELLING for the seeded observations before CANCELLED (#1196; see
// emrserverless_progression.go). Unseeded, it reports CANCELLED at once.
func (p *EMRServerlessPlugin) cancelJobRun(ctx *RequestContext, _ *AWSRequest, appID, runID string) (*AWSResponse, error) {
	goCtx := context.Background()
	run, err := p.loadJobRun(goCtx, ctx, appID, runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, emrNotFound("job run " + runID + " not found")
	}
	run.State = "CANCELLED"
	run.Updated = p.tc.Now().UTC()
	updated, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("cancelJobRun: marshal: %w", err)
	}
	runKey := "jobrun:" + ctx.AccountID + "/" + ctx.Region + "/" + appID + "/" + runID
	if err := p.state.Put(goCtx, emrServerlessNamespace, runKey, updated); err != nil {
		return nil, fmt.Errorf("cancelJobRun: put: %w", err)
	}
	if err := emrJobRunProgressions.reset(goCtx, p.state, runID); err != nil {
		return nil, fmt.Errorf("cancelJobRun: %w", err)
	}
	return emrServerlessJSONResponse(http.StatusOK, map[string]string{
		"applicationId": appID,
		"jobRunId":      runID,
	})
}

// emrListJobRunsMaxResults is ListJobRuns' published maximum page size, and the size of a page
// whose request names none. The page publishes the 1–50 range and no default.
const emrListJobRunsMaxResults = 50

// listJobRuns handles ListJobRuns, reading every member API_ListJobRuns publishes (#1195).
//
//   - maxResults outside its published 1–50 is ValidationException/400, and a page holds at most 50.
//   - nextToken resumes where the previous page ended, and is omitted on the last page. A token
//     substrate did not issue is ValidationException/400: the page publishes no
//     InvalidNextTokenException (only ValidationException and InternalServerException), so
//     ValidationException, its own code for input that fails a constraint, is the answer.
//   - states (up to 8 published values) narrows the list, which comes back grouped by state in the
//     order the filter names them, as the page says. It is a list, sent as a repeated query key and
//     read from [AWSRequest.MultiValueParams].
//   - mode (BATCH | STREAMING), createdAtAfter and createdAtBefore narrow the list. The two dates
//     are restJson1 query timestamps, ISO 8601; an unparsable one is ValidationException.
//
// A list for an application that does not exist answers empty: the page publishes no
// ResourceNotFoundException.
func (p *EMRServerlessPlugin) listJobRuns(ctx *RequestContext, req *AWSRequest, appID string) (*AWSResponse, error) {
	q := req.Params
	pageSize := emrListJobRunsMaxResults
	if raw := q["maxResults"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > emrListJobRunsMaxResults {
			return nil, emrValidation("maxResults must be an integer from 1 to 50")
		}
		pageSize = n
	}
	offset := 0
	if raw := q["nextToken"]; raw != "" {
		n, ok := decodeOffsetPaginationToken(raw)
		if !ok || len(raw) > 1024 || !emrNextTokenPattern.MatchString(raw) {
			return nil, emrValidation("the nextToken is not one this operation issued")
		}
		offset = n
	}
	states := req.MultiValueParams["states"]
	if states == nil && q["states"] != "" {
		states = []string{q["states"]}
	}
	if len(states) > 8 {
		return nil, emrValidation("states may name at most 8 values")
	}
	for _, s := range states {
		if !emrJobRunStates[s] {
			return nil, emrValidation("states contains " + s + ", which is not a job run state")
		}
	}
	mode := q["mode"]
	if mode != "" && mode != "BATCH" && mode != "STREAMING" {
		return nil, emrValidation("mode must be one of BATCH, STREAMING")
	}
	after, err := emrQueryTime(q["createdAtAfter"], "createdAtAfter")
	if err != nil {
		return nil, err
	}
	before, err := emrQueryTime(q["createdAtBefore"], "createdAtBefore")
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	runIDsKey := "jobrun_ids:" + ctx.AccountID + "/" + ctx.Region + "/" + appID
	ids, err := loadStringIndex(goCtx, p.state, emrServerlessNamespace, runIDsKey)
	if err != nil {
		return nil, fmt.Errorf("listJobRuns: load index: %w", err)
	}

	// The index is sorted by ID, which says nothing about age, so the runs are ordered by createdAt
	// and then by ID for runs created at one instant. That order is stable, which is what the offset
	// token relies on. The page publishes no order beyond the grouping a states filter imposes.
	var runs []EMRServerlessJobRun
	for _, id := range ids {
		run, err := p.loadJobRun(goCtx, ctx, appID, id)
		if err != nil {
			return nil, err
		}
		if run == nil {
			continue
		}
		if mode != "" && emrJobRunMode(*run) != mode {
			continue
		}
		if !after.IsZero() && run.Created.Before(after) {
			continue
		}
		if !before.IsZero() && run.Created.After(before) {
			continue
		}
		runs = append(runs, *run)
	}
	sort.SliceStable(runs, func(i, j int) bool {
		if !runs[i].Created.Equal(runs[j].Created) {
			return runs[i].Created.Before(runs[j].Created)
		}
		return runs[i].JobRunID < runs[j].JobRunID
	})
	// Each listed run is observed, as GetJobRun observes it, so the two agree on a run's state and a
	// states filter narrows by the state the caller is shown (#1196). The observation is spent before
	// the filter, because the state is what the filter compares.
	var matched []emrJobRunSummaryOut
	for _, run := range runs {
		observed, details, err := p.observeJobRun(run)
		if err != nil {
			return nil, err
		}
		summary := emrServerlessJobRunToSummary(observed)
		if details != "" {
			summary.StateDetails = details
		}
		matched = append(matched, summary)
	}
	if len(states) > 0 {
		var grouped []emrJobRunSummaryOut
		for _, s := range states {
			for _, r := range matched {
				if r.State == s {
					grouped = append(grouped, r)
				}
			}
		}
		matched = grouped
	}
	if matched == nil {
		matched = []emrJobRunSummaryOut{}
	}

	page, token := pageByOffsetToken(matched, offset, pageSize)
	out := map[string]interface{}{"jobRuns": page}
	if token != "" {
		out["nextToken"] = token
	}
	return emrServerlessJSONResponse(http.StatusOK, out)
}

// emrQueryTime parses a restJson1 query-string timestamp, ISO 8601, refusing one that does not
// parse. An empty value is the zero time.
func emrQueryTime(raw, member string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, emrValidation(member + " must be an ISO 8601 timestamp")
	}
	return t, nil
}

// generateEMRServerlessAppID mints an EMR Serverless application ID from m, in the
// "00"-prefixed lowercase-hex form the service publishes one in.
func generateEMRServerlessAppID(m *IDMint) string {
	return "00" + m.Hex(4)
}

// generateEMRServerlessRunID mints a job run ID from m: "00" and fourteen lowercase hex digits,
// sixteen characters, the form the application ID takes.
//
// API_JobRun, API_StartJobRun, API_GetJobRun and API_CancelJobRun all publish `Pattern: [0-9a-z]+`,
// length 1–64, for the job run ID, and API_StartJobRun's ARN pattern ends `/jobruns/[0-9a-zA-Z]+`.
// The dashed UUID this used to mint violated both, so a consumer validating the ID, or a typed SDK
// building a URI from it, rejected every ID substrate minted (#1204). The value is still derived
// from the request id, so a replay mints what its recording minted (#856).
func generateEMRServerlessRunID(m *IDMint) string {
	return "00" + m.Hex(7)
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
