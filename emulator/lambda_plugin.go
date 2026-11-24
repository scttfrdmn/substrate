package emulator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LambdaPlugin emulates the AWS Lambda REST API.
// It handles CreateFunction, GetFunction, DeleteFunction, ListFunctions,
// UpdateFunctionCode, UpdateFunctionConfiguration, Invoke, InvokeAsync,
// AddPermission, RemovePermission, GetPolicy, PutFunctionEventInvokeConfig,
// TagResource, UntagResource, ListTags, and event source mappings.
// Function code never runs: Invoke answers a seeded result or error, or a stub
// payload; see [LambdaPlugin.invoke].
type LambdaPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry // for SQS ESM polls
	// esmMu guards esmActive, the enabled SQS mappings [LambdaPlugin.RunDue] polls.
	esmMu     sync.Mutex
	esmActive map[string]struct{}
	// pollMu serializes RunDue, so two concurrent requests cannot both run one due poll.
	pollMu sync.Mutex
	// seedMu serializes advancing a seeded role-propagation window (#1274); see
	// [progression.observe].
	seedMu sync.Mutex
}

// Name returns the service name "lambda".
func (p *LambdaPlugin) Name() string { return "lambda" }

// Initialize sets up the LambdaPlugin with the provided configuration.
func (p *LambdaPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	if reg, ok := cfg.Options["registry"].(*PluginRegistry); ok {
		p.registry = reg
	}
	p.esmActive = make(map[string]struct{})
	return nil
}

// Shutdown empties the set of event-source mappings [LambdaPlugin.RunDue] polls.
func (p *LambdaPlugin) Shutdown(_ context.Context) error {
	p.stopAllPollers()
	return nil
}

// ResetForRun empties the set of event-source mappings [LambdaPlugin.RunDue] polls, the
// one piece of state Lambda keeps outside the [StateManager]. It implements
// [ResettablePlugin]; see [ReplayEngine.resetState] for why a replay needs it, and #903
// for the decision.
//
// Polling itself runs on the simulated clock: RunDue polls each mapping in the set whose
// cursor (a state key the reset deletes) is due, so nothing runs between requests. The
// set has to be emptied because the reset has just deleted the mapping records it names,
// and DeleteEventSourceMapping, the only call that removes an entry, answers
// ResourceNotFoundException once the record is gone. Emptying it leaves a reset plugin
// polling exactly the mappings its state holds, as a fresh process would.
func (p *LambdaPlugin) ResetForRun(_ context.Context) error {
	p.stopAllPollers()
	return nil
}

// stopAllPollers forgets every mapping [LambdaPlugin.RunDue] polls. It is safe on a plugin
// that has never handled a request.
func (p *LambdaPlugin) stopAllPollers() {
	p.esmMu.Lock()
	defer p.esmMu.Unlock()
	p.esmActive = make(map[string]struct{})
}

// HandleRequest dispatches a Lambda REST API request to the appropriate handler.
func (p *LambdaPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, name, subResource := lambdaRequestOperation(req)
	switch op {
	case "CreateFunction":
		return p.createFunction(ctx, req)
	case "GetFunction":
		return p.getFunction(ctx, name)
	case "UpdateFunctionCode":
		return p.updateFunctionCode(ctx, req, name)
	case "UpdateFunctionConfiguration":
		return p.updateFunctionConfiguration(ctx, req, name)
	case "DeleteFunction":
		return p.deleteFunction(ctx, name)
	case "ListFunctions":
		return p.listFunctions(ctx, req)
	case "Invoke":
		return p.invoke(ctx, req, name)
	case "ListVersionsByFunction":
		return p.listVersionsByFunction(ctx, req, name)
	case "GetFunctionCodeSigningConfig":
		return p.getFunctionCodeSigningConfig(ctx, name)
	case "InvokeAsync":
		return p.invokeAsync(name)
	case "AddPermission":
		return p.addPermission(ctx, req, name)
	case "RemovePermission":
		return p.removePermission(ctx, name, subResource)
	case "GetPolicy":
		return p.getPolicy(ctx, name)
	case "PutFunctionEventInvokeConfig":
		return p.putFunctionEventInvokeConfig(ctx, req, name)
	case "TagResource":
		return p.tagResource(ctx, req, name)
	case "UntagResource":
		return p.untagResource(ctx, req, name)
	case "ListTags":
		return p.listTags(ctx, name)
	case "CreateEventSourceMapping":
		return p.createEventSourceMapping(ctx, req)
	case "ListEventSourceMappings":
		return p.listEventSourceMappings(ctx, req)
	case "GetEventSourceMapping":
		return p.getEventSourceMapping(ctx, name)
	case "UpdateEventSourceMapping":
		return p.updateEventSourceMapping(ctx, req, name)
	case "DeleteEventSourceMapping":
		return p.deleteEventSourceMapping(ctx, name)
	case "ListLayers":
		return p.listLayers(ctx, req)
	case "GetLayerVersionByArn":
		return p.getLayerVersionByArn(ctx, req)
	case "PublishLayerVersion", "ListLayerVersions", "GetLayerVersion", "DeleteLayerVersion",
		"AddLayerVersionPermission", "GetLayerVersionPolicy", "RemoveLayerVersionPermission":
		return p.handleLayer(ctx, req, op, name, subResource)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// --- CRUD operations -------------------------------------------------------

func (p *LambdaPlugin) createFunction(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		FunctionName  string            `json:"FunctionName"`
		Runtime       string            `json:"Runtime"`
		Role          string            `json:"Role"`
		Handler       string            `json:"Handler"`
		Description   string            `json:"Description"`
		Timeout       int               `json:"Timeout"`
		MemorySize    int               `json:"MemorySize"`
		PackageType   string            `json:"PackageType"`
		Architectures []string          `json:"Architectures"`
		Tags          map[string]string `json:"Tags"`
		ImageURI      string            `json:"ImageUri"`
		Code          struct {
			ZipFile         string `json:"ZipFile"`
			S3Bucket        string `json:"S3Bucket"`
			S3Key           string `json:"S3Key"`
			S3ObjectVersion string `json:"S3ObjectVersion"`
			ImageURI        string `json:"ImageUri"`
		} `json:"Code"`
		Environment struct {
			Variables map[string]string `json:"Variables"`
		} `json:"Environment"`
		TracingConfig lambdaTracingConfig `json:"TracingConfig"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	if body.FunctionName == "" {
		return nil, lambdaInvalidParameterValue("FunctionName is required")
	}
	if awsErr := body.TracingConfig.validate(); awsErr != nil {
		return nil, awsErr
	}

	// Check for existing function.
	existing, err := p.state.Get(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(ctx.AccountID, ctx.Region, body.FunctionName))
	if err != nil {
		return nil, fmt.Errorf("lambda createFunction state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceConflictException", Message: "Function already exists", HTTPStatus: http.StatusConflict}
	}
	// A role inside a seeded propagation window cannot be assumed yet (#1274).
	if err := p.checkRolePropagated(context.Background(), body.Role); err != nil {
		return nil, err
	}

	timeout := body.Timeout
	if timeout <= 0 {
		timeout = 3
	}
	memSize := body.MemorySize
	if memSize <= 0 {
		memSize = 128
	}
	pkgType := body.PackageType
	if pkgType == "" {
		pkgType = "Zip"
	}
	// The API declares ImageUri inside Code, not at the top level; the top-level
	// field substrate also accepts predates this and is kept so an existing caller
	// keeps working. Code.ImageUri wins, being the documented spelling.
	imageURI := body.ImageURI
	if body.Code.ImageURI != "" {
		imageURI = body.Code.ImageURI
	}
	if imageURI != "" && body.PackageType == "" {
		// "PackageType: Image" is required alongside an ImageUri, and real Lambda
		// rejects the pair with Zip, so an unstated package type cannot conflict.
		pkgType = "Image"
	}
	archs := body.Architectures
	if len(archs) == 0 {
		archs = []string{"x86_64"}
	}

	now := p.tc.Now()
	fn := LambdaFunction{
		FunctionName:  body.FunctionName,
		FunctionArn:   lambdaFunctionARN(ctx.Region, ctx.AccountID, body.FunctionName),
		Runtime:       body.Runtime,
		Role:          body.Role,
		Handler:       body.Handler,
		Description:   body.Description,
		Timeout:       timeout,
		MemorySize:    memSize,
		Environment:   body.Environment.Variables,
		CodeSize:      0,
		CodeSha256:    "",
		RevisionID:    ctx.IDs.HexUUID(),
		State:         "Active",
		PackageType:   pkgType,
		Architectures: archs,
		Tags:          body.Tags,
		ImageURI:      imageURI,
		TracingMode:   body.TracingConfig.Mode,
		LastModified:  now,
		CreateDate:    now,
	}

	// Store ZIP bytes when provided directly.
	if body.Code.ZipFile != "" {
		decoded, decErr := base64.StdEncoding.DecodeString(body.Code.ZipFile)
		if decErr == nil && len(decoded) > 0 {
			fn.CodeSize = int64(len(decoded))
			fn.CodeSha256 = lambdaCodeSha256(decoded)
			fn.ZipStored = true
			if err := p.state.Put(context.Background(), lambdaNamespace,
				lambdaZipStateKey(ctx.AccountID, ctx.Region, body.FunctionName), decoded); err != nil {
				return nil, fmt.Errorf("lambda createFunction state.Put: %w", err)
			}
		}
	} else if body.Code.S3Bucket != "" {
		// The package is not copied, so ZipStored stays false, but the object is usually
		// in substrate's own S3, so its length is knowable without fetching it.
		fn.CodeSize, fn.CodeSha256 = p.sizeS3Package(body.Code.S3Bucket, body.Code.S3Key, body.Code.S3ObjectVersion)
	} else if imageURI != "" {
		// The same rule UpdateFunctionCode applies: an image function's package is the
		// image, so its digest tracks the URI. Reporting one here and only there would
		// mean a function created from an image had no digest until it was updated,
		// which is a difference between two paths a caller cannot see a reason for.
		fn.CodeSha256 = lambdaCodeSha256([]byte(imageURI))
	}

	data, err := json.Marshal(fn)
	if err != nil {
		return nil, fmt.Errorf("lambda createFunction marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(ctx.AccountID, ctx.Region, body.FunctionName), data); err != nil {
		return nil, fmt.Errorf("lambda createFunction state.Put: %w", err)
	}

	// Auto-create the /aws/lambda/{name} log group to match real AWS behavior.
	// We write directly to state (no registry call) to avoid a circular dependency
	// on the CloudWatchLogsPlugin. See issue #73.
	if err := p.autoCreateLambdaLogGroup(ctx, body.FunctionName); err != nil {
		return nil, fmt.Errorf("lambda createFunction autoCreateLambdaLogGroup: %w", err)
	}

	return lambdaJSONResponse(http.StatusCreated, buildFunctionConfig(fn))
}

func (p *LambdaPlugin) getFunction(ctx *RequestContext, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}

	type codeLocation struct {
		RepositoryType   string `json:"RepositoryType"`
		Location         string `json:"Location,omitempty"`
		ImageURI         string `json:"ImageUri,omitempty"`
		ResolvedImageURI string `json:"ResolvedImageUri,omitempty"`
	}
	// Tags is a member of GetFunctionResponse itself, beside Configuration; Terraform reads a
	// function's tags from there and saw none until #1519.
	type response struct {
		Configuration interface{}       `json:"Configuration"`
		Code          codeLocation      `json:"Code"`
		Tags          map[string]string `json:"Tags,omitempty"`
	}
	// FunctionCodeLocation reports "the service that's hosting the file", so an
	// image-packaged function is hosted by ECR and reports ImageUri rather than a
	// presigned Location — reporting an S3 URL for one is a claim a caller can act
	// on and be wrong about. A function created with a Code.ImageUri had nowhere to
	// surface it before, so the deployer forwarding one would otherwise have been a
	// write with no observable.
	loc := codeLocation{
		RepositoryType: "S3",
		Location:       "https://lambda-stub.localhost/code/" + name,
	}
	if fn.PackageType == "Image" && fn.ImageURI != "" {
		loc = codeLocation{
			RepositoryType:   "ECR",
			ImageURI:         fn.ImageURI,
			ResolvedImageURI: fn.ImageURI,
		}
	}
	return lambdaJSONResponse(http.StatusOK, response{
		Configuration: buildFunctionConfig(fn),
		Code:          loc,
		Tags:          fn.Tags,
	})
}

func (p *LambdaPlugin) updateFunctionCode(ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	// Parsed before the lookup, for the reasons given on addPermission (#1006) and now for one more: with
	// the guard added (#1007), leaving the lookup first would answer 404 here and 400 there for the same
	// unparseable body naming the same absent function — one plugin, two codes, for one class of caller
	// error, which is what #950 removed.
	var body struct {
		ZipFile         string `json:"ZipFile"`
		S3Bucket        string `json:"S3Bucket"`
		S3Key           string `json:"S3Key"`
		S3ObjectVersion string `json:"S3ObjectVersion"`
		ImageURI        string `json:"ImageUri"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}

	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}

	// CodeSha256 is "the SHA256 hash of the function's deployment package", so it is
	// derived from the package below when there are bytes to hash rather than being a
	// fresh random value on every update — a caller comparing it across two updates
	// is asking whether the code changed, and a random value answers "always". A
	// RevisionID is not a digest of anything and stays random.
	fn.RevisionID = ctx.IDs.HexUUID()
	fn.LastModified = p.tc.Now()

	switch {
	case body.ZipFile != "":
		decoded, decErr := base64.StdEncoding.DecodeString(body.ZipFile)
		if decErr == nil && len(decoded) > 0 {
			fn.CodeSize = int64(len(decoded))
			fn.CodeSha256 = lambdaCodeSha256(decoded)
			fn.ZipStored = true
			if err := p.state.Put(context.Background(), lambdaNamespace,
				lambdaZipStateKey(ctx.AccountID, ctx.Region, name), decoded); err != nil {
				return nil, fmt.Errorf("lambda updateFunctionCode state.Put: %w", err)
			}
		}
	case body.S3Bucket != "":
		// The package is not copied, so ZipStored stays false, but the object is usually
		// in substrate's own S3, so its length is knowable without fetching it.
		fn.CodeSize, fn.CodeSha256 = p.sizeS3Package(body.S3Bucket, body.S3Key, body.S3ObjectVersion)
		fn.ZipStored = false
	}

	if body.ImageURI != "" {
		fn.ImageURI = body.ImageURI
		fn.PackageType = "Image"
		// An image function's package is the image, so its digest tracks the URI: it
		// changes when the function is pointed at a different image and not otherwise.
		// Substrate never pulls the image, so this is not the digest real Lambda
		// reports, for the same reason the S3 path reports an ETag — see
		// docs/reference/lambda.md.
		fn.CodeSha256 = lambdaCodeSha256([]byte(body.ImageURI))
	}

	resp, err := p.saveFunctionAndRespond(ctx.AccountID, ctx.Region, fn, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *LambdaPlugin) updateFunctionConfiguration(ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	// Parsed before the lookup; see updateFunctionCode above for why all six of Lambda's guarded sites now
	// answer 400 rather than three of them answering 404.
	var body struct {
		Handler     string `json:"Handler"`
		Runtime     string `json:"Runtime"`
		Role        string `json:"Role"`
		Description string `json:"Description"`
		Timeout     int    `json:"Timeout"`
		MemorySize  int    `json:"MemorySize"`
		Environment struct {
			Variables map[string]string `json:"Variables"`
		} `json:"Environment"`
		TracingConfig lambdaTracingConfig `json:"TracingConfig"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	if awsErr := body.TracingConfig.validate(); awsErr != nil {
		return nil, awsErr
	}

	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if body.TracingConfig.Mode != "" {
		fn.TracingMode = body.TracingConfig.Mode
	}
	// A new Role is assumed as CreateFunction's is, so a seeded propagation window refuses it too.
	if err := p.checkRolePropagated(context.Background(), body.Role); err != nil {
		return nil, err
	}

	if body.Handler != "" {
		fn.Handler = body.Handler
	}
	if body.Runtime != "" {
		fn.Runtime = body.Runtime
	}
	if body.Role != "" {
		fn.Role = body.Role
	}
	if body.Description != "" {
		fn.Description = body.Description
	}
	if body.Timeout > 0 {
		fn.Timeout = body.Timeout
	}
	if body.MemorySize > 0 {
		fn.MemorySize = body.MemorySize
	}
	if body.Environment.Variables != nil {
		fn.Environment = body.Environment.Variables
	}
	fn.RevisionID = ctx.IDs.HexUUID()
	fn.LastModified = p.tc.Now()

	resp, err := p.saveFunctionAndRespond(ctx.AccountID, ctx.Region, fn, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *LambdaPlugin) deleteFunction(ctx *RequestContext, name string) (*AWSResponse, error) {
	existing, err := p.state.Get(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(ctx.AccountID, ctx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("lambda deleteFunction state.Get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Function not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	if err := p.state.Delete(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(ctx.AccountID, ctx.Region, name)); err != nil {
		return nil, fmt.Errorf("lambda deleteFunction state.Delete: %w", err)
	}
	// Also delete policy, invoke config and the uploaded deployment package.
	_ = p.state.Delete(context.Background(), lambdaNamespace,
		lambdaPolicyStateKey(ctx.AccountID, ctx.Region, name))
	_ = p.state.Delete(context.Background(), lambdaNamespace,
		lambdaInvokeConfigStateKey(ctx.AccountID, ctx.Region, name))
	// A deployment package is part of the function AWS says this operation deletes (#1035).
	_ = p.state.Delete(context.Background(), lambdaNamespace,
		lambdaZipStateKey(ctx.AccountID, ctx.Region, name))
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *LambdaPlugin) listFunctions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Scoped to the caller's own account and Region: ListFunctions "returns a list of Lambda
	// functions", and a function belongs to one account in one Region, so a scan over every
	// key would report another account's (#943).
	keys, err := p.state.List(context.Background(), lambdaNamespace,
		lambdaFunctionKeyPrefix(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("lambda listFunctions state.List: %w", err)
	}

	// Sort keys for stable ordering.
	sort.Strings(keys)

	// Pagination.
	marker := req.Params["Marker"]
	maxItems := 50
	if s, ok := req.Params["MaxItems"]; ok && s != "" {
		if n, parseErr := parseInt(s); parseErr == nil && n > 0 {
			maxItems = n
		}
	}

	startIdx := 0
	if marker != "" {
		for i, k := range keys {
			if k == marker {
				startIdx = i + 1
				break
			}
		}
	}

	end := startIdx + maxItems
	var nextMarker string
	if end < len(keys) {
		nextMarker = keys[end]
	} else {
		end = len(keys)
	}

	functions := make([]interface{}, 0, end-startIdx)
	for _, key := range keys[startIdx:end] {
		data, getErr := p.state.Get(context.Background(), lambdaNamespace, key)
		if getErr != nil || data == nil {
			continue
		}
		var fn LambdaFunction
		if unmarshalErr := json.Unmarshal(data, &fn); unmarshalErr != nil {
			continue
		}
		functions = append(functions, buildFunctionConfig(fn))
	}

	type response struct {
		Functions  []interface{} `json:"Functions"`
		NextMarker string        `json:"NextMarker,omitempty"`
	}
	return lambdaJSONResponse(http.StatusOK, response{Functions: functions, NextMarker: nextMarker})
}

// Valid X-Amz-Function-Error header values. "Handled" is an exception the runtime
// caught and reported; "Unhandled" is a process that died without responding
// (timeout, out-of-memory, an early exit).
const (
	lambdaFunctionErrorHandled   = "Handled"
	lambdaFunctionErrorUnhandled = "Unhandled"
)

// invokeResponse assembles an Invoke response. Per the Invoke API reference,
// X-Amz-Function-Error is present *only* when the function errored ("If present,
// indicates that an error occurred"), and X-Amz-Log-Result only when the caller
// asked for it with X-Amz-Log-Type: Tail. Emitting either unconditionally makes a
// successful invoke indistinguishable from a failed one for the natural
// `if "FunctionError" in response` check, which inverts against real AWS (#393).
func invokeResponse(body []byte, functionError, logTail string) *AWSResponse {
	headers := map[string]string{
		"Content-Type":           "application/json",
		"X-Amz-Executed-Version": "$LATEST",
	}
	if functionError != "" {
		headers["X-Amz-Function-Error"] = functionError
	}
	if logTail != "" {
		headers["X-Amz-Log-Result"] = base64.StdEncoding.EncodeToString([]byte(logTail))
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: headers, Body: body}
}

// lambdaLogTail returns the execution log to report, or "" when the caller did
// not request one. LogType is a header (X-Amz-Log-Type) whose only log-producing
// value is "Tail"; "None" and absence both mean no log.
//
// The log is synthesized from the invocation's observable facts — substrate does
// not run the handler, so it has no handler output to report. It carries the
// START/END/REPORT lines the runtime emits, which is what a caller parsing
// LogResult for a request ID or a duration reads.
func lambdaLogTail(req *AWSRequest, fn LambdaFunction, requestID string, errored bool) string {
	if !strings.EqualFold(headerValueFold(req.Headers, "X-Amz-Log-Type"), "Tail") {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "START RequestId: %s Version: $LATEST\n", requestID)
	if errored {
		fmt.Fprintf(&b, "[ERROR] Invocation failed\n")
	}
	fmt.Fprintf(&b, "END RequestId: %s\n", requestID)
	fmt.Fprintf(&b, "REPORT RequestId: %s\tDuration: 0.00 ms\tBilled Duration: 1 ms\tMemory Size: %d MB\tMax Memory Used: %d MB\n",
		requestID, fn.MemorySize, fn.MemorySize)
	return b.String()
}

func (p *LambdaPlugin) invoke(ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}

	// Substrate never runs the handler, so a seed is the only way a caller's error
	// branch, or a specific success payload, becomes reachable; see
	// [LambdaPlugin.resolveInvokeSeeds] for which seed wins. A seeded failure still
	// answers 200 — per the Invoke reference, "the status code in the API response
	// doesn't reflect function errors". With no seed, the stub payload is the nominal
	// success.
	seededErr, seededResult, seedErr := p.resolveInvokeSeeds(fn.FunctionName)
	if seedErr != nil {
		return nil, seedErr
	}
	if seededErr != nil {
		body := seededErr.errorPayload(ctx.RequestID)
		return invokeResponse(body, seededErr.functionErrorHeader(),
			lambdaLogTail(req, fn, ctx.RequestID, true)), nil
	}
	if seededResult != nil {
		return invokeResponse(seededResult.body(), "", lambdaLogTail(req, fn, ctx.RequestID, false)), nil
	}

	stubPayload := []byte(`{"statusCode":200,"body":"null"}`)
	return invokeResponse(stubPayload, "", lambdaLogTail(req, fn, ctx.RequestID, false)), nil
}

func (p *LambdaPlugin) invokeAsync(_ string) (*AWSResponse, error) {
	return &AWSResponse{
		StatusCode: http.StatusAccepted,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       []byte(`{"Status":202}`),
	}, nil
}

// lambdaPermissionPrincipal renders AddPermission's Principal the way its output does: an account
// ID as {"AWS": "arn:aws:iam::{account}:root"} ("Principal":{"AWS":"arn:aws:iam::111122223333:root"}
// is the Lambda developer guide's add-permission output for --principal 111122223333), an ARN as
// {"AWS": arn}, "*" as the string "*" (the guide's function-URL policy), and anything else — a
// service principal such as sns.amazonaws.com — as {"Service": p}.
func lambdaPermissionPrincipal(p string) interface{} {
	switch {
	case p == "*":
		return "*"
	case strings.HasPrefix(p, "arn:"):
		return map[string]string{"AWS": p}
	case len(p) == 12 && strings.Trim(p, "0123456789") == "":
		return map[string]string{"AWS": "arn:aws:iam::" + p + ":root"}
	}
	return map[string]string{"Service": p}
}

// lambdaPermissionConditions renders AddPermission's scoping parameters as the statement's
// Condition, each with the operator and key the Lambda developer guide's resource-based policy
// examples use: SourceArn as ArnLike AWS:SourceArn, SourceAccount as StringEquals
// AWS:SourceAccount, PrincipalOrgID as StringEquals aws:PrincipalOrgID, EventSourceToken as
// StringEquals lambda:EventSourceToken, FunctionUrlAuthType as StringEquals
// lambda:FunctionUrlAuthType, and InvokedViaFunctionUrl as Bool lambda:InvokedViaFunctionUrl. The
// two AWS: keys keep the capitalization add-permission's output has always used here. Nil when
// none is given.
func lambdaPermissionConditions(sourceArn, sourceAccount, orgID, token, urlAuthType string, viaURL bool) map[string]map[string]string {
	out := map[string]map[string]string{}
	set := func(op, key, value string) {
		if value == "" {
			return
		}
		if out[op] == nil {
			out[op] = map[string]string{}
		}
		out[op][key] = value
	}
	set("ArnLike", "AWS:SourceArn", sourceArn)
	set("StringEquals", "AWS:SourceAccount", sourceAccount)
	set("StringEquals", "aws:PrincipalOrgID", orgID)
	set("StringEquals", "lambda:EventSourceToken", token)
	set("StringEquals", "lambda:FunctionUrlAuthType", urlAuthType)
	if viaURL {
		set("Bool", "lambda:InvokedViaFunctionUrl", "true")
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// addPermission adds a statement to a function's resource policy.
//
// The body is parsed and its required member checked **before** the function is looked up (#1006).
// Both orders have a case — a caller naming a function that does not exist arguably wants to hear
// that rather than that its JSON will not parse — and no Lambda page states the precedence: both
// Errors sections list `ResourceNotFoundException` and `InvalidParameterValueException` without
// ordering them, and substrate vendors no Smithy model to read the answer off. Substrate's reading is
// that a request whose shape is wrong is wrong whatever state exists, so it is refused without
// consulting state. That is also the only order answering one code per class of caller error: a body
// that will not parse and a body with no `StatementId` are both mistakes visible in the request
// alone, and answering 404 for one and 400 for the other on the same request would be the
// inconsistency #950 corrected elsewhere. This is Lambda's two sites only — `docs/reference/lambda.md`
// records the convention split across the rest of the tree, which this does not settle.
func (p *LambdaPlugin) addPermission(ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	var body struct {
		StatementID      string `json:"StatementId"`
		Action           string `json:"Action"`
		Principal        string `json:"Principal"`
		SourceArn        string `json:"SourceArn"`
		SourceAccount    string `json:"SourceAccount"`
		PrincipalOrgID   string `json:"PrincipalOrgID"`
		EventSourceToken string `json:"EventSourceToken"`
		// FunctionUrlAuthType and InvokedViaFunctionUrl scope a statement to function-URL
		// calls; each becomes a condition (lambdaPermissionConditions).
		FunctionURLAuthType   string `json:"FunctionUrlAuthType"`
		InvokedViaFunctionURL bool   `json:"InvokedViaFunctionUrl"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	if body.StatementID == "" {
		return nil, lambdaInvalidParameterValue("StatementId is required")
	}

	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}

	// Load existing policy or create new one.
	policy, loadErr := p.loadPolicy(ctx.AccountID, ctx.Region, name)
	if loadErr != nil {
		return nil, loadErr
	}
	if policy == nil {
		policy = &LambdaResourcePolicy{Version: "2012-10-17"}
	}

	// Check for duplicate statement ID.
	for _, stmt := range policy.Statement {
		if stmt.Sid == body.StatementID {
			return nil, &AWSError{Code: "ResourceConflictException", Message: "Statement already exists: " + body.StatementID, HTTPStatus: http.StatusConflict}
		}
	}

	// "Qualifier — Specify a version or alias to add permissions to a published version of the
	// function": the statement's Resource is the qualified ARN. Substrate models no aliases, so the
	// qualifier is not looked up.
	resource := fn.FunctionArn
	if q := req.Params["Qualifier"]; q != "" {
		resource += ":" + q
	}
	stmt := LambdaPermissionStatement{
		Sid:       body.StatementID,
		Effect:    "Allow",
		Principal: lambdaPermissionPrincipal(body.Principal),
		Action:    body.Action,
		Resource:  resource,
		Condition: lambdaPermissionConditions(body.SourceArn, body.SourceAccount, body.PrincipalOrgID,
			body.EventSourceToken, body.FunctionURLAuthType, body.InvokedViaFunctionURL),
	}
	policy.Statement = append(policy.Statement, stmt)

	policyData, marshalErr := json.Marshal(policy)
	if marshalErr != nil {
		return nil, fmt.Errorf("lambda addPermission marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(context.Background(), lambdaNamespace,
		lambdaPolicyStateKey(ctx.AccountID, ctx.Region, name), policyData); putErr != nil {
		return nil, fmt.Errorf("lambda addPermission state.Put: %w", putErr)
	}

	stmtData, err := json.Marshal(stmt)
	if err != nil {
		return nil, fmt.Errorf("lambda addPermission marshal: %w", err)
	}
	// API_AddPermission publishes Statement as a string holding the statement's JSON, not as a
	// nested object, so a typed SDK's string field decodes it (#1382).
	return lambdaJSONResponse(http.StatusCreated, map[string]string{"Statement": string(stmtData)})
}

func (p *LambdaPlugin) removePermission(ctx *RequestContext, name, statementID string) (*AWSResponse, error) {
	if _, err := p.loadFunction(ctx.AccountID, ctx.Region, name); err != nil {
		return nil, err
	}

	policy, err := p.loadPolicy(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "No policy found for function: " + name, HTTPStatus: http.StatusNotFound}
	}

	newStmts := make([]LambdaPermissionStatement, 0, len(policy.Statement))
	found := false
	for _, stmt := range policy.Statement {
		if stmt.Sid == statementID {
			found = true
			continue
		}
		newStmts = append(newStmts, stmt)
	}
	if !found {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Statement not found: " + statementID, HTTPStatus: http.StatusNotFound}
	}

	policy.Statement = newStmts
	policyData, err := json.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("lambda removePermission marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace,
		lambdaPolicyStateKey(ctx.AccountID, ctx.Region, name), policyData); err != nil {
		return nil, fmt.Errorf("lambda removePermission state.Put: %w", err)
	}

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *LambdaPlugin) getPolicy(ctx *RequestContext, name string) (*AWSResponse, error) {
	if _, err := p.loadFunction(ctx.AccountID, ctx.Region, name); err != nil {
		return nil, err
	}

	policy, err := p.loadPolicy(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "No policy found for function: " + name, HTTPStatus: http.StatusNotFound}
	}

	policyJSON, marshalErr := json.Marshal(policy)
	if marshalErr != nil {
		return nil, fmt.Errorf("lambda getPolicy marshal: %w", marshalErr)
	}
	return lambdaJSONResponse(http.StatusOK, map[string]string{"Policy": string(policyJSON)})
}

func (p *LambdaPlugin) putFunctionEventInvokeConfig(ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	// Parsed before the lookup; see updateFunctionCode above.
	var body struct {
		MaximumRetryAttempts     int `json:"MaximumRetryAttempts"`
		MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}

	if _, err := p.loadFunction(ctx.AccountID, ctx.Region, name); err != nil {
		return nil, err
	}

	cfg := LambdaEventInvokeConfig{
		FunctionName:             name,
		MaximumRetryAttempts:     body.MaximumRetryAttempts,
		MaximumEventAgeInSeconds: body.MaximumEventAgeInSeconds,
	}
	data, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		return nil, fmt.Errorf("lambda putFunctionEventInvokeConfig marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(context.Background(), lambdaNamespace,
		lambdaInvokeConfigStateKey(ctx.AccountID, ctx.Region, name), data); putErr != nil {
		return nil, fmt.Errorf("lambda putFunctionEventInvokeConfig state.Put: %w", putErr)
	}

	return lambdaJSONResponse(http.StatusOK, cfg)
}

// --- Tagging operations ----------------------------------------------------

// findFunctionByARN returns the LambdaFunction whose FunctionArn matches arn.
// Returns ResourceNotFoundException when no match is found.
//
// The scan is narrowed to the account and Region the ARN itself names, not the caller's:
// an ARN naming another account's function must resolve that account's function or none,
// which is the rule #826 established for the tagging resolver. A malformed ARN narrows to
// nothing and so answers ResourceNotFoundException, which is what a scan for an ARN no
// record carries already answered (#943).
func (p *LambdaPlugin) findFunctionByARN(arn string) (*LambdaFunction, error) {
	ctx := context.Background()
	acct, region := lambdaARNScope(arn)
	names, err := p.state.List(ctx, lambdaNamespace, lambdaFunctionKeyPrefix(acct, region))
	if err != nil {
		return nil, fmt.Errorf("lambda findFunctionByARN list: %w", err)
	}
	for _, k := range names {
		data, getErr := p.state.Get(ctx, lambdaNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var fn LambdaFunction
		if unmarshalErr := json.Unmarshal(data, &fn); unmarshalErr != nil {
			continue
		}
		if fn.FunctionArn == arn {
			return &fn, nil
		}
	}
	return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Function not found: " + arn, HTTPStatus: http.StatusNotFound}
}

// tagResource merges a tag set onto a function.
//
// The body is parsed before the function is looked up, for the reasons given on addPermission above: a
// body that will not parse names nothing, so there is nothing for a lookup to be about.
func (p *LambdaPlugin) tagResource(ctx *RequestContext, req *AWSRequest, arn string) (*AWSResponse, error) {
	var body struct {
		Tags map[string]string `json:"Tags"`
	}
	if unmarshalErr := json.Unmarshal(req.Body, &body); unmarshalErr != nil {
		return nil, lambdaInvalidBody()
	}

	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	fn.EverTagged = taggingEverTagged(fn.EverTagged, len(fn.Tags), len(body.Tags))
	if fn.Tags == nil {
		fn.Tags = make(map[string]string)
	}
	for k, v := range body.Tags {
		fn.Tags[k] = v
	}
	if _, saveErr := p.saveFunctionAndRespond(ctx.AccountID, ctx.Region, *fn, http.StatusOK); saveErr != nil {
		return nil, saveErr
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}, Body: nil}, nil
}

func (p *LambdaPlugin) untagResource(ctx *RequestContext, req *AWSRequest, arn string) (*AWSResponse, error) {
	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	fn.EverTagged = taggingEverTagged(fn.EverTagged, len(fn.Tags), 0)
	if fn.Tags != nil {
		if keys, ok := req.Params["tagKeys"]; ok {
			for _, k := range strings.Split(keys, ",") {
				delete(fn.Tags, strings.TrimSpace(k))
			}
		}
	}
	if _, saveErr := p.saveFunctionAndRespond(ctx.AccountID, ctx.Region, *fn, http.StatusOK); saveErr != nil {
		return nil, saveErr
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}, Body: nil}, nil
}

func (p *LambdaPlugin) listTags(ctx *RequestContext, arn string) (*AWSResponse, error) {
	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	tags := fn.Tags
	if tags == nil {
		tags = make(map[string]string)
	}
	return lambdaJSONResponse(http.StatusOK, map[string]interface{}{"Tags": tags})
}

// --- Helpers ---------------------------------------------------------------

func (p *LambdaPlugin) loadFunction(accountID, region, name string) (LambdaFunction, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(accountID, region, name))
	if err != nil {
		return LambdaFunction{}, fmt.Errorf("lambda loadFunction state.Get: %w", err)
	}
	if data == nil {
		return LambdaFunction{}, &AWSError{Code: "ResourceNotFoundException", Message: "Function not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	var fn LambdaFunction
	if err := json.Unmarshal(data, &fn); err != nil {
		return LambdaFunction{}, fmt.Errorf("lambda loadFunction unmarshal: %w", err)
	}
	return fn, nil
}

func (p *LambdaPlugin) loadPolicy(accountID, region, name string) (*LambdaResourcePolicy, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace,
		lambdaPolicyStateKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("lambda loadPolicy state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var policy LambdaResourcePolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return nil, fmt.Errorf("lambda loadPolicy unmarshal: %w", err)
	}
	return &policy, nil
}

func (p *LambdaPlugin) saveFunctionAndRespond(accountID, region string, fn LambdaFunction, status int) (*AWSResponse, error) {
	data, err := json.Marshal(fn)
	if err != nil {
		return nil, fmt.Errorf("lambda saveFunctionAndRespond marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(accountID, region, fn.FunctionName), data); err != nil {
		return nil, fmt.Errorf("lambda saveFunctionAndRespond state.Put: %w", err)
	}
	return lambdaJSONResponse(status, buildFunctionConfig(fn))
}

// buildFunctionConfig converts a LambdaFunction to the AWS API FunctionConfiguration shape.
func buildFunctionConfig(fn LambdaFunction) map[string]interface{} {
	cfg := map[string]interface{}{
		"FunctionName":  fn.FunctionName,
		"FunctionArn":   fn.FunctionArn,
		"Runtime":       fn.Runtime,
		"Role":          fn.Role,
		"Handler":       fn.Handler,
		"Description":   fn.Description,
		"Timeout":       fn.Timeout,
		"MemorySize":    fn.MemorySize,
		"CodeSize":      fn.CodeSize,
		"CodeSha256":    fn.CodeSha256,
		"RevisionId":    fn.RevisionID,
		"State":         fn.State,
		"PackageType":   fn.PackageType,
		"Architectures": fn.Architectures,
		"LastModified":  fn.LastModified.Format(time.RFC3339),
		// FunctionConfiguration's Version: substrate publishes no versions, so every
		// configuration it answers is the unpublished one (lambda_versions.go).
		"Version": lambdaLatestVersion,
		// Recorded intent only: substrate sends no trace to X-Ray. PassThrough is the
		// mode a function created without a TracingConfig has.
		"TracingConfig": map[string]string{"Mode": cmp.Or(fn.TracingMode, lambdaTracingPassThrough)},
	}
	if len(fn.Environment) > 0 {
		cfg["Environment"] = map[string]interface{}{
			"Variables": fn.Environment,
		}
	}
	if len(fn.Tags) > 0 {
		cfg["Tags"] = fn.Tags
	}
	return cfg
}

// lambdaJSONResponse encodes v as JSON and returns an AWSResponse with Content-Type application/json.
func lambdaJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("lambdaJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}

// autoCreateLambdaLogGroup creates the /aws/lambda/{name} CloudWatch Logs log
// group in state without going through the plugin registry, avoiding a circular
// dependency on CloudWatchLogsPlugin. It is a no-op when the group already exists.
func (p *LambdaPlugin) autoCreateLambdaLogGroup(ctx *RequestContext, name string) error {
	goCtx := context.Background()
	lgName := "/aws/lambda/" + name
	lgKey := cwLogGroupKey(ctx.AccountID, ctx.Region, lgName)
	if existing, _ := p.state.Get(goCtx, cloudwatchLogsNamespace, lgKey); existing != nil {
		return nil
	}
	lg := CWLogGroup{
		LogGroupName: lgName,
		ARN:          cwLogGroupARN(ctx.Region, ctx.AccountID, lgName),
		CreationTime: p.tc.Now().UnixMilli(),
	}
	b, err := json.Marshal(lg)
	if err != nil {
		return fmt.Errorf("lambda autoCreateLambdaLogGroup %s marshal: %w", lgName, err)
	}
	if err := p.state.Put(goCtx, cloudwatchLogsNamespace, lgKey, b); err != nil {
		return fmt.Errorf("lambda autoCreateLambdaLogGroup %s state.Put: %w", lgName, err)
	}
	idxKey := cwLogGroupNamesKey(ctx.AccountID, ctx.Region)
	if err := updateStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey, lgName); err != nil {
		return fmt.Errorf("lambda autoCreateLambdaLogGroup %s index: %w", lgName, err)
	}
	return nil
}

// lambdaCodeSha256 reports the CodeSha256 for a deployment package: "the SHA256 hash
// of the function's deployment package", base64-encoded as Lambda returns it.
func lambdaCodeSha256(pkg []byte) string {
	sum := sha256.Sum256(pkg)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// sizeS3Package reports the CodeSize and CodeSha256 of an S3-sourced deployment
// package by reading the object's recorded metadata out of substrate's own S3 state.
//
// Lambda reports CodeSize as "the size of the function's deployment package, in
// bytes", which for an S3 source is the length of the object — a fact substrate
// already holds, since a template that references an object a test uploaded is the
// ordinary case. Substrate does not fetch the bytes (nothing executes them unless
// they arrived inline), so the digest comes from the object's own ETag rather than
// from a hash of the package: for a single-part upload that ETag *is* the MD5 of the
// body, which makes it change exactly when the package changes, which is the
// question a caller comparing CodeSha256 across deploys is asking. It is not the
// SHA256 real Lambda would report, and docs/reference/lambda.md says so.
//
// An absent object is not an error. Real Lambda would refuse the create, but
// substrate's S3 and Lambda namespaces are independent and a template may
// legitimately name an object a test never uploaded — failing the create would make
// a stack undeployable for a reason unrelated to what the test is checking. It logs
// and reports size 0, which is what the function did for every S3 source before.
func (p *LambdaPlugin) sizeS3Package(bucket, key, versionID string) (int64, string) {
	stateKey := "object:" + bucket + "/" + key
	if versionID != "" {
		stateKey = "object_version:" + bucket + "/" + key + "/" + versionID
	}
	data, err := p.state.Get(context.Background(), s3Namespace, stateKey)
	if err != nil || data == nil {
		p.logger.Warn("lambda: S3 deployment package not found in substrate's S3 state; "+
			"reporting CodeSize 0 and no execution package",
			"bucket", bucket, "key", key, "versionId", versionID)
		return 0, ""
	}
	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		p.logger.Warn("lambda: S3 deployment package metadata is unreadable; reporting CodeSize 0",
			"bucket", bucket, "key", key, "error", err)
		return 0, ""
	}
	// A delete marker lives at the same key as the object it hides, so reading the
	// record is not the same as the object being there. A deleted package is absent,
	// not a zero-length one.
	if obj.IsDeleteMarker {
		p.logger.Warn("lambda: S3 deployment package is a delete marker; reporting CodeSize 0",
			"bucket", bucket, "key", key)
		return 0, ""
	}
	return obj.Size, strings.Trim(obj.ETag, `"`)
}

// parseInt parses a string into an int.
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// --- Event Source Mapping (ESM) -----------------------------------------------

// ESMConfig represents an event source mapping configuration.
type ESMConfig struct {
	// UUID is the unique identifier for this event source mapping.
	UUID string `json:"UUID"`

	// FunctionARN is the Lambda function that will receive events. Lambda publishes it
	// as FunctionArn on EventSourceMappingConfiguration.
	FunctionARN string `json:"FunctionArn"`

	// EventSourceARN is the ARN of the event source (DynamoDB stream or Kinesis stream).
	// Lambda publishes it as EventSourceArn. The lowercase eventSourceARN used in a
	// stream record's event payload is a different, and correct, spelling.
	EventSourceARN string `json:"EventSourceArn"`

	// BatchSize is the maximum number of records in each batch.
	BatchSize int `json:"BatchSize"`

	// State is the current state: Enabled or Disabled.
	State string `json:"State"`

	// StartingPosition is the position from which to start reading: TRIM_HORIZON or LATEST.
	StartingPosition string `json:"StartingPosition"`

	// MaximumBatchingWindowInSeconds is the maximum time to gather records before
	// invoking the function. Zero means no batching window.
	MaximumBatchingWindowInSeconds int `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

func (p *LambdaPlugin) esmKey(uuid string) string {
	return "esm:" + uuid
}

func (p *LambdaPlugin) esmIDsKey(functionARN string) string {
	return "esm_ids:" + functionARN
}

func (p *LambdaPlugin) loadESM(ctx context.Context, uuid string) (*ESMConfig, error) {
	data, err := p.state.Get(ctx, lambdaNamespace, p.esmKey(uuid))
	if err != nil {
		return nil, fmt.Errorf("lambda loadESM: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var esm ESMConfig
	if err := json.Unmarshal(data, &esm); err != nil {
		return nil, fmt.Errorf("lambda loadESM unmarshal: %w", err)
	}
	return &esm, nil
}

func (p *LambdaPlugin) saveESM(ctx context.Context, esm *ESMConfig) error {
	data, err := json.Marshal(esm)
	if err != nil {
		return fmt.Errorf("lambda saveESM marshal: %w", err)
	}
	return p.state.Put(ctx, lambdaNamespace, p.esmKey(esm.UUID), data)
}

func (p *LambdaPlugin) loadESMIDs(ctx context.Context, functionARN string) ([]string, error) {
	data, err := p.state.Get(ctx, lambdaNamespace, p.esmIDsKey(functionARN))
	if err != nil {
		return nil, fmt.Errorf("lambda loadESMIDs: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("lambda loadESMIDs unmarshal: %w", err)
	}
	return ids, nil
}

func (p *LambdaPlugin) saveESMIDs(ctx context.Context, functionARN string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("lambda saveESMIDs marshal: %w", err)
	}
	return p.state.Put(ctx, lambdaNamespace, p.esmIDsKey(functionARN), data)
}

func (p *LambdaPlugin) createEventSourceMapping(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FunctionName     string `json:"FunctionName"`
		EventSourceArn   string `json:"EventSourceArn"`
		BatchSize        int    `json:"BatchSize"`
		StartingPosition string `json:"StartingPosition"`
		Enabled          *bool  `json:"Enabled"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, lambdaInvalidBody()
	}
	if input.FunctionName == "" || input.EventSourceArn == "" {
		return nil, lambdaInvalidParameterValue("FunctionName and EventSourceArn are required")
	}

	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	state := "Enabled"
	if input.Enabled != nil && !*input.Enabled {
		state = "Disabled"
	}

	startingPosition := input.StartingPosition
	if startingPosition == "" {
		startingPosition = "TRIM_HORIZON"
	}

	// Resolve function ARN.
	functionARN := input.FunctionName
	if !strings.HasPrefix(functionARN, "arn:") {
		functionARN = "arn:aws:lambda:" + ctx.Region + ":" + ctx.AccountID + ":function:" + input.FunctionName
	}

	uuid := ctx.IDs.HexUUID()
	esm := &ESMConfig{
		UUID:             uuid,
		FunctionARN:      functionARN,
		EventSourceARN:   input.EventSourceArn,
		BatchSize:        batchSize,
		State:            state,
		StartingPosition: startingPosition,
	}

	bgCtx := context.Background()
	if err := p.saveESM(bgCtx, esm); err != nil {
		return nil, fmt.Errorf("lambda createEventSourceMapping saveESM: %w", err)
	}

	ids, err := p.loadESMIDs(bgCtx, functionARN)
	if err != nil {
		return nil, err
	}
	ids = append(ids, uuid)
	if err := p.saveESMIDs(bgCtx, functionARN, ids); err != nil {
		return nil, fmt.Errorf("lambda createEventSourceMapping saveESMIDs: %w", err)
	}

	// An enabled SQS mapping is polled by RunDue on the simulated clock (#1292).
	if esm.State == "Enabled" && p.registry != nil && strings.Contains(input.EventSourceArn, ":sqs:") {
		if err := p.activatePolling(bgCtx, uuid); err != nil {
			return nil, err
		}
	}

	return lambdaJSONResponse(http.StatusCreated, esm)
}

// The three bounds ListEventSourceMappings' MaxItems publishes, which are three facts rather
// than one range.
//
// API_ListEventSourceMappings publishes "Valid Range: Minimum value of 1. Maximum value of
// 10000." for the parameter, and separately, in the parameter's own description, a cap on what a
// response may carry whatever the parameter says: "Note that ListEventSourceMappings returns a
// maximum of 100 items in each response, even if you set the number higher." A request for 5000
// is therefore valid and answers 100 with a NextMarker, while a request for 10001 is refused —
// the two rules do different things and are kept as separate constants so neither reads as the
// other's ceiling.
const (
	lambdaESMMinMaxItems = 1
	lambdaESMMaxMaxItems = 10000
	lambdaESMMaxItemsCap = 100
)

// listEventSourceMappings reports the account's event source mappings, optionally narrowed to
// one function or one event source, one page at a time.
//
// The two pagination parameters are read here rather than ignored (#917). Both are published
// on the URI — "GET /2015-03-31/event-source-mappings?EventSourceArn={EventSourceArn}&
// FunctionName={FunctionName}&Marker={Marker}&MaxItems={MaxItems}" — and substrate answered the
// whole listing with no NextMarker whatever a caller sent, so a paging loop terminated on the
// first response here and first ran for real against an account holding more mappings than one
// page.
//
// MaxItems publishes "Valid Range: Minimum value of 1. Maximum value of 10000." **and** a cap
// the range does not imply: "Note that ListEventSourceMappings returns a maximum of 100 items
// in each response, even if you set the number higher." So 5000 is a valid request that answers
// at most 100 items with a NextMarker — the cap is what AWS's own page states, not substrate's
// page size — and a value outside 1..10000 is refused rather than clamped, with the
// InvalidParameterValueException the page publishes for "one of the parameters in the request is
// not valid". An absent MaxItems is the same 100, which is substrate's reading: no default is
// published, and the cap applies to "each response" regardless of what was asked for.
//
// Marker is "a pagination token returned by a previous call", and NextMarker is "returned when
// the response doesn't contain all event source mappings" — so the final page carries none, or a
// caller looping until it comes back empty never stops. A Marker substrate could not have issued
// is refused ([decodeOffsetPaginationToken]) rather than silently answering page one, which is
// the defect #915 named at three other services, and it is refused **before** any state is read,
// per #887: the answer to a malformed token must not depend on how many mappings exist.
//
// Unlike EC2's describes, nothing on this page makes the filters mutually exclusive with the
// pagination parameters, so no InvalidParameterCombination is invented here; FunctionName and
// EventSourceArn narrow a page rather than forbidding one.
func (p *LambdaPlugin) listEventSourceMappings(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	functionName := req.Params["FunctionName"]
	eventSourceArn := req.Params["EventSourceArn"]

	pageSize, awsErr := lambdaESMPageSize(req.Params["MaxItems"])
	if awsErr != nil {
		return nil, awsErr
	}
	offset, tokenOK := decodeOffsetPaginationToken(req.Params["Marker"])
	if !tokenOK {
		return nil, lambdaInvalidParameterValue("the Marker '" + req.Params["Marker"] + "' is invalid")
	}

	bgCtx := context.Background()

	var functionARN string
	if functionName != "" {
		if strings.HasPrefix(functionName, "arn:") {
			functionARN = functionName
		} else {
			functionARN = "arn:aws:lambda:" + ctx.Region + ":" + ctx.AccountID + ":function:" + functionName
		}
	}

	var esms []ESMConfig

	if functionARN != "" {
		ids, err := p.loadESMIDs(bgCtx, functionARN)
		if err != nil {
			return nil, err
		}
		// Sorted, because an offset cursor names a position in a sequence and this one comes
		// from a per-function index whose order is the order mappings were created, not a key
		// scan. The other branch inherits [StateManager.List]'s lexicographic guarantee (#865)
		// over "esm:<uuid>" keys, so sorting by UUID here makes the two branches agree about
		// which mapping a given offset names.
		sort.Strings(ids)
		for _, id := range ids {
			esm, err := p.loadESM(bgCtx, id)
			if err != nil || esm == nil {
				continue
			}
			if eventSourceArn != "" && esm.EventSourceARN != eventSourceArn {
				continue
			}
			esms = append(esms, *esm)
		}
	} else {
		// Scan all ESMs — list by scanning state keys.
		keys, err := p.state.List(bgCtx, lambdaNamespace, "esm:")
		if err != nil {
			return nil, fmt.Errorf("lambda listEventSourceMappings: %w", err)
		}
		for _, k := range keys {
			// Skip index keys.
			if strings.HasPrefix(k, "esm_ids:") {
				continue
			}
			data, getErr := p.state.Get(bgCtx, lambdaNamespace, k)
			if getErr != nil || data == nil {
				continue
			}
			var esm ESMConfig
			if unmarshalErr := json.Unmarshal(data, &esm); unmarshalErr != nil {
				continue
			}
			if eventSourceArn != "" && esm.EventSourceARN != eventSourceArn {
				continue
			}
			esms = append(esms, esm)
		}
	}

	// The page is cut after the whole answer is filtered, not during the scan, so which
	// mappings EventSourceArn selects does not depend on the page a walk is on.
	page, nextMarker := pageByOffsetToken(esms, offset, pageSize)
	if page == nil {
		page = []ESMConfig{}
	}

	type response struct {
		EventSourceMappings []ESMConfig `json:"EventSourceMappings"`
		NextMarker          string      `json:"NextMarker,omitempty"`
	}
	return lambdaJSONResponse(http.StatusOK, response{EventSourceMappings: page, NextMarker: nextMarker})
}

// lambdaESMPageSize reads ListEventSourceMappings' MaxItems and returns the number of mappings
// one response may carry.
//
// The published range is 1 to 10000 and the published per-response cap is 100 — "Note that
// ListEventSourceMappings returns a maximum of 100 items in each response, even if you set the
// number higher" — so the two are separate rules: a value outside the range is refused, and a
// value inside it is applied only up to the cap. An absent MaxItems is the cap, which is
// substrate's reading, since the page publishes no default and states the cap against every
// response.
//
// A non-integer is refused for the same reason a zero is: MaxItems is an Integer with a
// published minimum, and coercing an unparseable value would answer a page size the caller did
// not ask for. The code is the page's own InvalidParameterValueException; the message wording is
// substrate's, since AWS publishes only "one of the parameters in the request is not valid".
func lambdaESMPageSize(raw string) (int, *AWSError) {
	if raw == "" {
		return lambdaESMMaxItemsCap, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lambdaESMMinMaxItems || n > lambdaESMMaxMaxItems {
		return 0, lambdaInvalidParameterValue("MaxItems must be between " +
			strconv.Itoa(lambdaESMMinMaxItems) + " and " + strconv.Itoa(lambdaESMMaxMaxItems))
	}
	return min(n, lambdaESMMaxItemsCap), nil
}

func (p *LambdaPlugin) getEventSourceMapping(_ *RequestContext, uuid string) (*AWSResponse, error) {
	esm, err := p.loadESM(context.Background(), uuid)
	if err != nil {
		return nil, err
	}
	if esm == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "The event source mapping " + uuid + " was not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}
	return lambdaJSONResponse(http.StatusOK, esm)
}

func (p *LambdaPlugin) updateEventSourceMapping(_ *RequestContext, req *AWSRequest, uuid string) (*AWSResponse, error) {
	// Parsed before the lookup; see updateFunctionCode above. Both members are optional, so an absent body
	// is still a no-op update rather than a refusal — only a body that is present and will not parse is
	// refused (#1007).
	var input struct {
		BatchSize int   `json:"BatchSize"`
		Enabled   *bool `json:"Enabled"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, lambdaInvalidBody()
		}
	}

	esm, err := p.loadESM(context.Background(), uuid)
	if err != nil {
		return nil, err
	}
	if esm == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "The event source mapping " + uuid + " was not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	prevState := esm.State

	if input.BatchSize > 0 {
		esm.BatchSize = input.BatchSize
	}
	if input.Enabled != nil {
		if *input.Enabled {
			esm.State = "Enabled"
		} else {
			esm.State = "Disabled"
		}
	}

	if err := p.saveESM(context.Background(), esm); err != nil {
		return nil, fmt.Errorf("lambda updateEventSourceMapping saveESM: %w", err)
	}

	// Polling follows the state transition: enabling starts the cadence again from now,
	// disabling stops it.
	if p.registry != nil && strings.Contains(esm.EventSourceARN, ":sqs:") {
		if prevState != "Enabled" && esm.State == "Enabled" {
			if err := p.activatePolling(context.Background(), uuid); err != nil {
				return nil, err
			}
		} else if prevState == "Enabled" && esm.State != "Enabled" {
			if err := p.deactivatePolling(context.Background(), uuid); err != nil {
				return nil, err
			}
		}
	}

	return lambdaJSONResponse(http.StatusOK, esm)
}

func (p *LambdaPlugin) deleteEventSourceMapping(_ *RequestContext, uuid string) (*AWSResponse, error) {
	esm, err := p.loadESM(context.Background(), uuid)
	if err != nil {
		return nil, err
	}
	if esm == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "The event source mapping " + uuid + " was not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	if delErr := p.state.Delete(context.Background(), lambdaNamespace, p.esmKey(uuid)); delErr != nil {
		return nil, fmt.Errorf("lambda deleteEventSourceMapping: %w", delErr)
	}

	if err := p.deactivatePolling(context.Background(), uuid); err != nil {
		return nil, err
	}

	// Remove from function's ESM list.
	ids, err := p.loadESMIDs(context.Background(), esm.FunctionARN)
	if err != nil {
		return nil, err
	}
	filtered := ids[:0]
	for _, id := range ids {
		if id != uuid {
			filtered = append(filtered, id)
		}
	}
	_ = p.saveESMIDs(context.Background(), esm.FunctionARN, filtered)

	return lambdaJSONResponse(http.StatusOK, esm)
}
