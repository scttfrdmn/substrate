package emulator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// LambdaPlugin emulates the AWS Lambda REST API.
// It handles CreateFunction, GetFunction, DeleteFunction, ListFunctions,
// UpdateFunctionCode, UpdateFunctionConfiguration, Invoke, InvokeAsync,
// AddPermission, RemovePermission, GetPolicy, PutFunctionEventInvokeConfig,
// TagResource, UntagResource, ListTags, and event source mappings.
// When a LambdaExecutor is configured (Docker enabled) real function code is
// executed; otherwise stub responses are returned.
type LambdaPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	executor *LambdaExecutor // nil when Docker is disabled
	registry *PluginRegistry // for SQS ESM poller
	esmMu    sync.Mutex
	esmStop  map[string]chan struct{} // UUID → stop channel
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
	if exec, ok := cfg.Options["lambda_exec"].(*LambdaExecutor); ok {
		p.executor = exec
	}
	if reg, ok := cfg.Options["registry"].(*PluginRegistry); ok {
		p.registry = reg
	}
	p.esmStop = make(map[string]chan struct{})
	return nil
}

// Shutdown stops all ESM pollers and the Docker executor (if active).
func (p *LambdaPlugin) Shutdown(_ context.Context) error {
	p.esmMu.Lock()
	for _, ch := range p.esmStop {
		close(ch)
	}
	p.esmStop = make(map[string]chan struct{})
	p.esmMu.Unlock()

	if p.executor != nil {
		p.executor.StopAll()
	}
	return nil
}

// HandleRequest dispatches a Lambda REST API request to the appropriate handler.
func (p *LambdaPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, name, subResource := parseLambdaOperation(requestMethod(req), req.Path)
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
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("LambdaPlugin: unknown operation %q (path %q)", op, req.Path),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseLambdaOperation parses a Lambda REST path to extract the operation name,
// function name, and optional sub-resource (e.g., statement ID for RemovePermission).
func parseLambdaOperation(method, path string) (op, name, subResource string) {
	// Strip leading /2015-03-31
	const prefix = "/2015-03-31"
	p := strings.TrimPrefix(path, prefix)

	// /functions
	if p == "/functions" || p == "/functions/" {
		switch method {
		case "POST":
			return "CreateFunction", "", ""
		case "GET":
			return "ListFunctions", "", ""
		}
		return "ListFunctions", "", ""
	}

	// /functions/{name}/...
	if strings.HasPrefix(p, "/functions/") {
		rest := p[len("/functions/"):]
		slashIdx := strings.Index(rest, "/")
		if slashIdx < 0 {
			// /functions/{name}
			fn := rest
			switch method {
			case "GET":
				return "GetFunction", fn, ""
			case "DELETE":
				return "DeleteFunction", fn, ""
			}
			return "GetFunction", fn, ""
		}
		fn := rest[:slashIdx]
		sub := rest[slashIdx+1:]

		switch {
		case sub == "code" && method == "PUT":
			return "UpdateFunctionCode", fn, ""
		case sub == "configuration" && method == "PUT":
			return "UpdateFunctionConfiguration", fn, ""
		case sub == "invocations" && method == "POST":
			return "Invoke", fn, ""
		case sub == "invoke-async" && method == "POST":
			return "InvokeAsync", fn, ""
		case sub == "event-invoke-config" && method == "PUT":
			return "PutFunctionEventInvokeConfig", fn, ""
		case strings.HasPrefix(sub, "policy"):
			// POST /policy → AddPermission
			// GET /policy → GetPolicy
			// DELETE /policy/{statementId} → RemovePermission
			switch method {
			case "POST":
				return "AddPermission", fn, ""
			case "GET":
				return "GetPolicy", fn, ""
			case "DELETE":
				// sub = "policy/{statementId}"
				stmtID := ""
				if idx := strings.Index(sub, "/"); idx >= 0 {
					stmtID = sub[idx+1:]
				}
				return "RemovePermission", fn, stmtID
			}
		}
	}

	// /tags/{resourceArn}
	if strings.HasPrefix(p, "/tags/") {
		arn := p[len("/tags/"):]
		switch method {
		case "POST":
			return "TagResource", arn, ""
		case "GET":
			return "ListTags", arn, ""
		case "DELETE":
			return "UntagResource", arn, ""
		}
	}

	// /event-source-mappings[/{uuid}]
	if strings.HasPrefix(p, "/event-source-mappings") {
		rest := strings.TrimPrefix(p, "/event-source-mappings")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			switch method {
			case "POST":
				return "CreateEventSourceMapping", "", ""
			case "GET":
				return "ListEventSourceMappings", "", ""
			}
		} else {
			uuid := rest
			switch method {
			case "GET":
				return "GetEventSourceMapping", uuid, ""
			case "PUT":
				return "UpdateEventSourceMapping", uuid, ""
			case "DELETE":
				return "DeleteEventSourceMapping", uuid, ""
			}
		}
	}

	return "Unknown", "", ""
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
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.FunctionName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "FunctionName is required", HTTPStatus: http.StatusBadRequest}
	}

	// Check for existing function.
	existing, err := p.state.Get(context.Background(), lambdaNamespace, "function:"+body.FunctionName)
	if err != nil {
		return nil, fmt.Errorf("lambda createFunction state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceConflictException", Message: "Function already exists", HTTPStatus: http.StatusConflict}
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
		RevisionID:    generateLambdaRevisionID(),
		State:         "Active",
		PackageType:   pkgType,
		Architectures: archs,
		Tags:          body.Tags,
		ImageURI:      imageURI,
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
			_ = p.state.Put(context.Background(), lambdaNamespace, "function_zip:"+body.FunctionName, decoded)
		}
	} else if body.Code.S3Bucket != "" {
		// The bytes are not staged for execution — that is what ZipStored records and
		// it stays false — but the object is usually in substrate's own S3, so its
		// length is knowable even though the package is not fetched.
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
	if err := p.state.Put(context.Background(), lambdaNamespace, "function:"+body.FunctionName, data); err != nil {
		return nil, fmt.Errorf("lambda createFunction state.Put: %w", err)
	}

	// Auto-create the /aws/lambda/{name} log group to match real AWS behavior.
	// We write directly to state (no registry call) to avoid a circular dependency
	// on the CloudWatchLogsPlugin. See issue #73.
	p.autoCreateLambdaLogGroup(ctx, body.FunctionName)

	return lambdaJSONResponse(http.StatusCreated, buildFunctionConfig(fn))
}

func (p *LambdaPlugin) getFunction(_ *RequestContext, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(name)
	if err != nil {
		return nil, err
	}

	type codeLocation struct {
		RepositoryType   string `json:"RepositoryType"`
		Location         string `json:"Location,omitempty"`
		ImageURI         string `json:"ImageUri,omitempty"`
		ResolvedImageURI string `json:"ResolvedImageUri,omitempty"`
	}
	type response struct {
		Configuration interface{}  `json:"Configuration"`
		Code          codeLocation `json:"Code"`
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
	})
}

func (p *LambdaPlugin) updateFunctionCode(_ *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(name)
	if err != nil {
		return nil, err
	}

	var body struct {
		ZipFile         string `json:"ZipFile"`
		S3Bucket        string `json:"S3Bucket"`
		S3Key           string `json:"S3Key"`
		S3ObjectVersion string `json:"S3ObjectVersion"`
		ImageURI        string `json:"ImageUri"`
	}
	_ = json.Unmarshal(req.Body, &body)

	// CodeSha256 is "the SHA256 hash of the function's deployment package", so it is
	// derived from the package below when there are bytes to hash rather than being a
	// fresh random value on every update — a caller comparing it across two updates
	// is asking whether the code changed, and a random value answers "always". A
	// RevisionID is not a digest of anything and stays random.
	fn.RevisionID = generateLambdaRevisionID()
	fn.LastModified = p.tc.Now()

	switch {
	case body.ZipFile != "":
		decoded, decErr := base64.StdEncoding.DecodeString(body.ZipFile)
		if decErr == nil && len(decoded) > 0 {
			fn.CodeSize = int64(len(decoded))
			fn.CodeSha256 = lambdaCodeSha256(decoded)
			fn.ZipStored = true
			_ = p.state.Put(context.Background(), lambdaNamespace, "function_zip:"+name, decoded)
		}
	case body.S3Bucket != "":
		// The bytes are not staged for execution — that is what ZipStored records and
		// it stays false — but the object is usually in substrate's own S3, so its
		// length is knowable even though the package is not fetched.
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
		// docs/services.md.
		fn.CodeSha256 = lambdaCodeSha256([]byte(body.ImageURI))
	}

	return p.saveFunctionAndRespond(fn, http.StatusOK)
}

func (p *LambdaPlugin) updateFunctionConfiguration(_ *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(name)
	if err != nil {
		return nil, err
	}

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
	}
	_ = json.Unmarshal(req.Body, &body)

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
	fn.RevisionID = generateLambdaRevisionID()
	fn.LastModified = p.tc.Now()

	return p.saveFunctionAndRespond(fn, http.StatusOK)
}

func (p *LambdaPlugin) deleteFunction(_ *RequestContext, name string) (*AWSResponse, error) {
	existing, err := p.state.Get(context.Background(), lambdaNamespace, "function:"+name)
	if err != nil {
		return nil, fmt.Errorf("lambda deleteFunction state.Get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Function not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	if err := p.state.Delete(context.Background(), lambdaNamespace, "function:"+name); err != nil {
		return nil, fmt.Errorf("lambda deleteFunction state.Delete: %w", err)
	}
	// Also delete policy and invoke config.
	_ = p.state.Delete(context.Background(), lambdaNamespace, "function_policy:"+name)
	_ = p.state.Delete(context.Background(), lambdaNamespace, "function_invoke_config:"+name)
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *LambdaPlugin) listFunctions(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	keys, err := p.state.List(context.Background(), lambdaNamespace, "function:")
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
	fn, err := p.loadFunction(name)
	if err != nil {
		return nil, err
	}

	payload := req.Body

	// A seeded failure short-circuits every path below: substrate never runs the
	// handler, so seeding is the only way a caller's error branch becomes
	// reachable. The status stays 200 — per the Invoke reference, "the status code
	// in the API response doesn't reflect function errors".
	seed, seedErr := p.resolveSeededError(fn.FunctionName)
	if seedErr != nil {
		return nil, seedErr
	}
	if seed != nil {
		body := seed.errorPayload(ctx.RequestID)
		return invokeResponse(body, seed.functionErrorHeader(),
			lambdaLogTail(req, fn, ctx.RequestID, true)), nil
	}

	// Stub path: no executor or Docker unavailable.
	if p.executor == nil || !p.executor.isDockerAvailable() {
		stubPayload := []byte(`{"statusCode":200,"body":"null"}`)
		return invokeResponse(stubPayload, "", lambdaLogTail(req, fn, ctx.RequestID, false)), nil
	}

	// Recorded replay: return cached result when available.
	if p.executor.cfg.ReplayMode == "recorded" {
		if cached, ok := p.loadReplay(fn.FunctionArn, payload); ok {
			return invokeResponse(cached, "", lambdaLogTail(req, fn, ctx.RequestID, false)), nil
		}
	}

	// Load stored ZIP bytes (nil for Image-type functions).
	var zipBytes []byte
	if fn.ZipStored {
		zipBytes, _ = p.state.Get(context.Background(), lambdaNamespace, "function_zip:"+name)
	}

	result, funcErr, execErr := p.executor.Execute(context.Background(), fn, zipBytes, payload)
	if execErr != nil {
		return nil, fmt.Errorf("lambda invoke execute: %w", execErr)
	}

	// Cache result for future "recorded" replays.
	p.saveReplay(fn.FunctionArn, payload, result)

	return invokeResponse(result, funcErr, lambdaLogTail(req, fn, ctx.RequestID, funcErr != "")), nil
}

func (p *LambdaPlugin) invokeAsync(_ string) (*AWSResponse, error) {
	return &AWSResponse{
		StatusCode: http.StatusAccepted,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       []byte(`{"Status":202}`),
	}, nil
}

func (p *LambdaPlugin) addPermission(_ *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(name)
	if err != nil {
		return nil, err
	}

	var body struct {
		StatementID      string `json:"StatementId"`
		Action           string `json:"Action"`
		Principal        string `json:"Principal"`
		SourceArn        string `json:"SourceArn"`
		SourceAccount    string `json:"SourceAccount"`
		PrincipalOrgID   string `json:"PrincipalOrgID"`
		EventSourceToken string `json:"EventSourceToken"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StatementID == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "StatementId is required", HTTPStatus: http.StatusBadRequest}
	}

	// Load existing policy or create new one.
	policy, loadErr := p.loadPolicy(name)
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

	stmt := LambdaPermissionStatement{
		Sid:       body.StatementID,
		Effect:    "Allow",
		Principal: map[string]string{"Service": body.Principal},
		Action:    body.Action,
		Resource:  fn.FunctionArn,
	}
	if body.SourceArn != "" {
		stmt.Condition = map[string]map[string]string{
			"ArnLike": {"AWS:SourceArn": body.SourceArn},
		}
	}
	policy.Statement = append(policy.Statement, stmt)

	policyData, marshalErr := json.Marshal(policy)
	if marshalErr != nil {
		return nil, fmt.Errorf("lambda addPermission marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(context.Background(), lambdaNamespace, "function_policy:"+name, policyData); putErr != nil {
		return nil, fmt.Errorf("lambda addPermission state.Put: %w", putErr)
	}

	stmtData, _ := json.Marshal(stmt)
	return lambdaJSONResponse(http.StatusCreated, map[string]json.RawMessage{"Statement": stmtData})
}

func (p *LambdaPlugin) removePermission(_ *RequestContext, name, statementID string) (*AWSResponse, error) {
	if _, err := p.loadFunction(name); err != nil {
		return nil, err
	}

	policy, err := p.loadPolicy(name)
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
	if err := p.state.Put(context.Background(), lambdaNamespace, "function_policy:"+name, policyData); err != nil {
		return nil, fmt.Errorf("lambda removePermission state.Put: %w", err)
	}

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *LambdaPlugin) getPolicy(_ *RequestContext, name string) (*AWSResponse, error) {
	if _, err := p.loadFunction(name); err != nil {
		return nil, err
	}

	policy, err := p.loadPolicy(name)
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

func (p *LambdaPlugin) putFunctionEventInvokeConfig(_ *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	if _, err := p.loadFunction(name); err != nil {
		return nil, err
	}

	var body struct {
		MaximumRetryAttempts     int `json:"MaximumRetryAttempts"`
		MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds"`
	}
	_ = json.Unmarshal(req.Body, &body)

	cfg := LambdaEventInvokeConfig{
		FunctionName:             name,
		MaximumRetryAttempts:     body.MaximumRetryAttempts,
		MaximumEventAgeInSeconds: body.MaximumEventAgeInSeconds,
	}
	data, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		return nil, fmt.Errorf("lambda putFunctionEventInvokeConfig marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(context.Background(), lambdaNamespace, "function_invoke_config:"+name, data); putErr != nil {
		return nil, fmt.Errorf("lambda putFunctionEventInvokeConfig state.Put: %w", putErr)
	}

	return lambdaJSONResponse(http.StatusOK, cfg)
}

// --- Tagging operations ----------------------------------------------------

// findFunctionByARN returns the LambdaFunction whose FunctionArn matches arn.
// Returns ResourceNotFoundException when no match is found.
func (p *LambdaPlugin) findFunctionByARN(arn string) (*LambdaFunction, error) {
	ctx := context.Background()
	names, err := p.state.List(ctx, lambdaNamespace, "function:")
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

func (p *LambdaPlugin) tagResource(_ *RequestContext, req *AWSRequest, arn string) (*AWSResponse, error) {
	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	var body struct {
		Tags map[string]string `json:"Tags"`
	}
	if unmarshalErr := json.Unmarshal(req.Body, &body); unmarshalErr != nil {
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if fn.Tags == nil {
		fn.Tags = make(map[string]string)
	}
	for k, v := range body.Tags {
		fn.Tags[k] = v
	}
	if _, saveErr := p.saveFunctionAndRespond(*fn, http.StatusOK); saveErr != nil {
		return nil, saveErr
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}, Body: nil}, nil
}

func (p *LambdaPlugin) untagResource(_ *RequestContext, req *AWSRequest, arn string) (*AWSResponse, error) {
	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	if fn.Tags != nil {
		if keys, ok := req.Params["tagKeys"]; ok {
			for _, k := range strings.Split(keys, ",") {
				delete(fn.Tags, strings.TrimSpace(k))
			}
		}
	}
	if _, saveErr := p.saveFunctionAndRespond(*fn, http.StatusOK); saveErr != nil {
		return nil, saveErr
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}, Body: nil}, nil
}

func (p *LambdaPlugin) listTags(_ *RequestContext, arn string) (*AWSResponse, error) {
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

func (p *LambdaPlugin) loadFunction(name string) (LambdaFunction, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, "function:"+name)
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

func (p *LambdaPlugin) loadPolicy(name string) (*LambdaResourcePolicy, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, "function_policy:"+name)
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

func (p *LambdaPlugin) saveFunctionAndRespond(fn LambdaFunction, status int) (*AWSResponse, error) {
	data, err := json.Marshal(fn)
	if err != nil {
		return nil, fmt.Errorf("lambda saveFunctionAndRespond marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace, "function:"+fn.FunctionName, data); err != nil {
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
func (p *LambdaPlugin) autoCreateLambdaLogGroup(ctx *RequestContext, name string) {
	goCtx := context.Background()
	lgName := "/aws/lambda/" + name
	lgKey := cwLogGroupKey(ctx.AccountID, ctx.Region, lgName)
	if existing, _ := p.state.Get(goCtx, cloudwatchLogsNamespace, lgKey); existing != nil {
		return
	}
	lg := CWLogGroup{
		LogGroupName: lgName,
		ARN:          cwLogGroupARN(ctx.Region, ctx.AccountID, lgName),
		CreationTime: p.tc.Now().UnixMilli(),
	}
	b, err := json.Marshal(lg)
	if err != nil {
		return
	}
	if putErr := p.state.Put(goCtx, cloudwatchLogsNamespace, lgKey, b); putErr != nil {
		return
	}
	idxKey := cwLogGroupNamesKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey, lgName)
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
// SHA256 real Lambda would report, and docs/services.md says so.
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

// generateLambdaRevisionID generates a unique revision/code ID.
func generateLambdaRevisionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
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

	// FunctionARN is the Lambda function that will receive events.
	FunctionARN string `json:"FunctionARN"`

	// EventSourceARN is the ARN of the event source (DynamoDB stream or Kinesis stream).
	EventSourceARN string `json:"EventSourceARN"`

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
		return nil, &AWSError{Code: "ValidationException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if input.FunctionName == "" || input.EventSourceArn == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "FunctionName and EventSourceArn are required", HTTPStatus: http.StatusBadRequest}
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

	uuid := generateLambdaRevisionID()
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

	// Start SQS poller when ESM is for an SQS source and registry is available.
	if esm.State == "Enabled" && p.registry != nil && strings.Contains(input.EventSourceArn, ":sqs:") {
		stopCh := make(chan struct{})
		p.esmMu.Lock()
		p.esmStop[uuid] = stopCh
		p.esmMu.Unlock()
		go p.sqsPollerLoop(*esm, stopCh)
	}

	return lambdaJSONResponse(http.StatusCreated, esm)
}

func (p *LambdaPlugin) listEventSourceMappings(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	functionName := req.Params["FunctionName"]
	eventSourceArn := req.Params["EventSourceArn"]

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

	if esms == nil {
		esms = []ESMConfig{}
	}

	return lambdaJSONResponse(http.StatusOK, map[string]interface{}{
		"EventSourceMappings": esms,
	})
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

	var input struct {
		BatchSize int   `json:"BatchSize"`
		Enabled   *bool `json:"Enabled"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
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

	// Manage SQS poller lifecycle based on state transition.
	if p.registry != nil && strings.Contains(esm.EventSourceARN, ":sqs:") {
		if prevState != "Enabled" && esm.State == "Enabled" {
			// Start poller.
			stopCh := make(chan struct{})
			p.esmMu.Lock()
			p.esmStop[uuid] = stopCh
			p.esmMu.Unlock()
			go p.sqsPollerLoop(*esm, stopCh)
		} else if prevState == "Enabled" && esm.State != "Enabled" {
			// Stop poller.
			p.esmMu.Lock()
			if ch, ok := p.esmStop[uuid]; ok {
				close(ch)
				delete(p.esmStop, uuid)
			}
			p.esmMu.Unlock()
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

	// Stop SQS poller if running.
	p.esmMu.Lock()
	if ch, ok := p.esmStop[uuid]; ok {
		close(ch)
		delete(p.esmStop, uuid)
	}
	p.esmMu.Unlock()

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

// --- SQS ESM poller --------------------------------------------------------

// sqsPollerLoop polls an SQS queue and invokes the Lambda function with each
// batch of messages. It runs until stopCh is closed.
func (p *LambdaPlugin) sqsPollerLoop(esm ESMConfig, stopCh <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			p.pollAndInvoke(esm)
		}
	}
}

// pollAndInvoke fetches messages from SQS, invokes Lambda, and deletes
// successfully processed messages.
func (p *LambdaPlugin) pollAndInvoke(esm ESMConfig) {
	// Derive queue URL from ARN: arn:aws:sqs:{region}:{acct}:{name}
	arnParts := strings.Split(esm.EventSourceARN, ":")
	if len(arnParts) < 6 {
		return
	}
	region, acct, queueName := arnParts[3], arnParts[4], arnParts[5]
	queueURL := "http://sqs." + region + ".localhost/" + acct + "/" + queueName

	batchSize := esm.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}

	// ReceiveMessage.
	receiveReq := &AWSRequest{
		Service:   "sqs",
		Operation: "ReceiveMessage",
		Params: map[string]string{
			"Action":              "ReceiveMessage",
			"QueueUrl":            queueURL,
			"MaxNumberOfMessages": fmt.Sprintf("%d", batchSize),
			"WaitTimeSeconds":     "0",
		},
		Headers: map[string]string{},
		Path:    "/",
		Body:    nil,
	}
	rxCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: acct,
		Region:    region,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
	rxResp, err := p.registry.RouteRequest(rxCtx, receiveReq)
	if err != nil || rxResp == nil {
		return
	}

	// Parse ReceiveMessageResponse XML.
	type sqsMessage struct {
		MessageID     string `xml:"MessageId"`
		ReceiptHandle string `xml:"ReceiptHandle"`
		Body          string `xml:"Body"`
	}
	type rxResult struct {
		Messages []sqsMessage `xml:"ReceiveMessageResult>Message"`
	}
	var parsed rxResult
	if xmlErr := xml.Unmarshal(rxResp.Body, &parsed); xmlErr != nil || len(parsed.Messages) == 0 {
		return
	}

	// Build Lambda event JSON.
	type sqsRecord struct {
		MessageID      string            `json:"messageId"`
		ReceiptHandle  string            `json:"receiptHandle"`
		Body           string            `json:"body"`
		Attributes     map[string]string `json:"attributes"`
		EventSource    string            `json:"eventSource"`
		EventSourceARN string            `json:"eventSourceARN"`
		AWSRegion      string            `json:"awsRegion"`
	}
	records := make([]sqsRecord, len(parsed.Messages))
	for i, m := range parsed.Messages {
		records[i] = sqsRecord{
			MessageID:      m.MessageID,
			ReceiptHandle:  m.ReceiptHandle,
			Body:           m.Body,
			Attributes:     map[string]string{},
			EventSource:    "aws:sqs",
			EventSourceARN: esm.EventSourceARN,
			AWSRegion:      region,
		}
	}
	eventJSON, marshalErr := json.Marshal(map[string]interface{}{"Records": records})
	if marshalErr != nil {
		return
	}

	// Derive Lambda function name from ARN.
	fnParts := strings.Split(esm.FunctionARN, ":")
	fnName := fnParts[len(fnParts)-1]

	// Invoke Lambda.
	invokeReq := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/functions/" + fnName + "/invocations",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      eventJSON,
	}
	invokeCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: acct,
		Region:    region,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
	invokeResp, invokeErr := p.registry.RouteRequest(invokeCtx, invokeReq)
	if invokeErr != nil {
		p.logger.Warn("lambda ESM: invoke failed", "function", fnName, "err", invokeErr)
		return
	}
	if invokeResp != nil && invokeResp.StatusCode >= 300 {
		p.logger.Warn("lambda ESM: invoke non-2xx", "function", fnName, "status", invokeResp.StatusCode)
		return
	}

	// Delete successfully processed messages.
	for _, m := range parsed.Messages {
		deleteReq := &AWSRequest{
			Service:   "sqs",
			Operation: "DeleteMessage",
			Params: map[string]string{
				"Action":        "DeleteMessage",
				"QueueUrl":      queueURL,
				"ReceiptHandle": m.ReceiptHandle,
			},
			Headers: map[string]string{},
			Path:    "/",
			Body:    nil,
		}
		_, _ = p.registry.RouteRequest(rxCtx, deleteReq)
	}
}
