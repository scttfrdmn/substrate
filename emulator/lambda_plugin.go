package emulator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LambdaPlugin emulates the AWS Lambda REST API.
// It handles CreateFunction, GetFunction, GetFunctionConfiguration, DeleteFunction,
// ListFunctions, UpdateFunctionCode, UpdateFunctionConfiguration, Invoke, InvokeAsync,
// AddPermission, RemovePermission, GetPolicy, the event-invoke configuration,
// TagResource, UntagResource, ListTags, versions, aliases, layers and event source
// mappings.
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
	// versionMu serializes the operations that read, check and write a function's versions and
	// aliases: PublishVersion's counter, an alias's existence and targets, and the alias check that
	// guards deleting a version.
	versionMu sync.Mutex
	// invokeConfigMu serializes the event-invoke configuration writes, so an Update's merge reads
	// the configuration it replaces.
	invokeConfigMu sync.Mutex
	// esmTagMu serializes the tag operations on event-source mappings.
	esmTagMu sync.Mutex
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
		return p.getFunction(ctx, req, name)
	case "GetFunctionConfiguration":
		return p.getFunctionConfiguration(ctx, req, name)
	case "UpdateFunctionCode":
		return p.updateFunctionCode(ctx, req, name)
	case "UpdateFunctionConfiguration":
		return p.updateFunctionConfiguration(ctx, req, name)
	case "DeleteFunction":
		return p.deleteFunction(ctx, req, name)
	case "ListFunctions":
		return p.listFunctions(ctx, req)
	case "Invoke":
		return p.invoke(ctx, req, name)
	case "ListVersionsByFunction":
		return p.listVersionsByFunction(ctx, req, name)
	case "PublishVersion":
		return p.publishVersion(ctx, req, name)
	case "CreateAlias":
		return p.createAlias(ctx, req, name)
	case "GetAlias":
		return p.getAlias(ctx, name, subResource)
	case "UpdateAlias":
		return p.updateAlias(ctx, req, name, subResource)
	case "DeleteAlias":
		return p.deleteAlias(ctx, name, subResource)
	case "ListAliases":
		return p.listAliasesResponse(ctx, req, name)
	case "GetFunctionCodeSigningConfig":
		return p.getFunctionCodeSigningConfig(ctx, name)
	case "InvokeAsync":
		return p.invokeAsync(name)
	case "AddPermission":
		return p.addPermission(ctx, req, name)
	case "RemovePermission":
		return p.removePermission(ctx, req, name, subResource)
	case "GetPolicy":
		return p.getPolicy(ctx, req, name)
	case "PutFunctionEventInvokeConfig":
		return p.putFunctionEventInvokeConfig(ctx, req, name)
	case "UpdateFunctionEventInvokeConfig":
		return p.updateFunctionEventInvokeConfig(ctx, req, name)
	case "GetFunctionEventInvokeConfig":
		return p.getFunctionEventInvokeConfig(ctx, req, name)
	case "DeleteFunctionEventInvokeConfig":
		return p.deleteFunctionEventInvokeConfig(ctx, req, name)
	case "ListFunctionEventInvokeConfigs":
		return p.listFunctionEventInvokeConfigs(ctx, req, name)
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
		TracingConfig    lambdaTracingConfig     `json:"TracingConfig"`
		ImageConfig      *LambdaImageConfig      `json:"ImageConfig"`
		EphemeralStorage *lambdaEphemeralStorage `json:"EphemeralStorage"`
		LoggingConfig    *LambdaLoggingConfig    `json:"LoggingConfig"`
		// Publish creates version 1 as part of the create.
		Publish bool `json:"Publish"`
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
	ephemeral, awsErr := lambdaResolveEphemeralStorage(0, body.EphemeralStorage)
	if awsErr != nil {
		return nil, awsErr
	}
	logging, awsErr := lambdaResolveLoggingConfig(nil, body.LoggingConfig, body.FunctionName)
	if awsErr != nil {
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

		ImageConfig:          lambdaResolveImageConfig(nil, body.ImageConfig),
		EphemeralStorageSize: ephemeral,
		LoggingConfig:        logging,
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
		fn.CodeS3Location = lambdaCodeS3Location(body.Code.S3Bucket, body.Code.S3Key, body.Code.S3ObjectVersion)
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

	if body.Publish {
		return p.publishAndRespond(ctx, fn, http.StatusCreated)
	}
	return lambdaJSONResponse(http.StatusCreated, buildFunctionConfig(fn))
}

// publishAndRespond publishes fn, just written as $LATEST, for a CreateFunction or
// UpdateFunctionCode whose Publish is true — "set to true to publish a new version of the function
// after updating the code" — and answers the published version's configuration, whose Version and
// qualified FunctionArn name the version the call created.
func (p *LambdaPlugin) publishAndRespond(ctx *RequestContext, fn LambdaFunction, status int) (*AWSResponse, error) {
	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	v, err := p.publish(ctx, ctx.AccountID, ctx.Region, fn, "")
	if err != nil {
		return nil, err
	}
	return lambdaJSONResponse(status, buildVersionConfig(v))
}

// getFunction answers GetFunction for the function, or with a Qualifier, or a FunctionName carrying
// one, for a version or an alias. Tags are the function's whichever version is named: a version has
// none of its own.
func (p *LambdaPlugin) getFunction(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], "lambda:GetFunction")
	if err != nil {
		return nil, err
	}
	fn := t.function()
	name := fn.FunctionName

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
		Configuration: t.configuration(),
		Code:          loc,
		Tags:          t.latest.Tags,
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
		// Publish publishes a version from the updated code and configuration.
		Publish bool `json:"Publish"`
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
			fn.CodeS3Location = ""
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
		fn.CodeS3Location = lambdaCodeS3Location(body.S3Bucket, body.S3Key, body.S3ObjectVersion)
	}

	if body.ImageURI != "" {
		fn.ImageURI = body.ImageURI
		fn.CodeS3Location = ""
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
	if body.Publish {
		return p.publishAndRespond(ctx, fn, http.StatusOK)
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
		TracingConfig    lambdaTracingConfig     `json:"TracingConfig"`
		ImageConfig      *LambdaImageConfig      `json:"ImageConfig"`
		EphemeralStorage *lambdaEphemeralStorage `json:"EphemeralStorage"`
		LoggingConfig    *LambdaLoggingConfig    `json:"LoggingConfig"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	if awsErr := body.TracingConfig.validate(); awsErr != nil {
		return nil, awsErr
	}
	ephemeral, awsErr := lambdaResolveEphemeralStorage(0, body.EphemeralStorage)
	if awsErr != nil {
		return nil, awsErr
	}

	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	// Merged into what the function holds, so this check runs after the lookup.
	logging, awsErr := lambdaResolveLoggingConfig(fn.LoggingConfig, body.LoggingConfig, fn.FunctionName)
	if awsErr != nil {
		return nil, awsErr
	}
	if body.EphemeralStorage != nil {
		fn.EphemeralStorageSize = ephemeral
	}
	fn.LoggingConfig = logging
	fn.ImageConfig = lambdaResolveImageConfig(fn.ImageConfig, body.ImageConfig)
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

// deleteFunction answers DeleteFunction. "To delete a specific function version, use the Qualifier
// parameter. Otherwise, all versions and aliases are deleted." A Qualifier must name a published
// version: an alias, or $LATEST, is not a version DeleteFunction deletes on its own, and is refused
// with InvalidParameterValueException, whose messages here are substrate's own.
func (p *LambdaPlugin) deleteFunction(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	acct, region, name, derived, awsErr := lambdaFunctionScope(ctx, ref, "lambda:DeleteFunction")
	if awsErr != nil {
		return nil, awsErr
	}
	qualifier, awsErr := lambdaMergeQualifier(derived, req.Params["Qualifier"])
	if awsErr != nil {
		return nil, awsErr
	}
	if qualifier != "" {
		fn, err := p.loadFunction(acct, region, name)
		if err != nil {
			return nil, err
		}
		switch {
		case qualifier == lambdaLatestVersion:
			return nil, lambdaInvalidParameterValue("$LATEST cannot be deleted on its own; delete the function without a Qualifier")
		case !lambdaIsVersionNumber(qualifier):
			return nil, lambdaInvalidParameterValue("Qualifier '" + qualifier +
				"' names an alias; DeleteFunction deletes a version, and DeleteAlias an alias")
		}
		return p.deleteFunctionVersion(acct, region, fn, qualifier)
	}

	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	existing, err := p.state.Get(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(acct, region, name))
	if err != nil {
		return nil, fmt.Errorf("lambda deleteFunction state.Get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Function not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	if err := p.state.Delete(context.Background(), lambdaNamespace,
		lambdaFunctionStateKey(acct, region, name)); err != nil {
		return nil, fmt.Errorf("lambda deleteFunction state.Delete: %w", err)
	}
	// Also delete policy, invoke config and the uploaded deployment package.
	_ = p.state.Delete(context.Background(), lambdaNamespace,
		lambdaPolicyStateKey(acct, region, name))
	_ = p.state.Delete(context.Background(), lambdaNamespace,
		lambdaInvokeConfigStateKey(acct, region, name))
	// A deployment package is part of the function AWS says this operation deletes (#1035).
	_ = p.state.Delete(context.Background(), lambdaNamespace,
		lambdaZipStateKey(acct, region, name))
	if err := p.deleteFunctionChildren(acct, region, name); err != nil {
		return nil, err
	}
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
//
// X-Amz-Executed-Version is "the version of the function that executed": $LATEST, the version the
// request named, or the version an alias routed it to.
func invokeResponse(body []byte, version, functionError, logTail string) *AWSResponse {
	headers := map[string]string{
		"Content-Type":           "application/json",
		"X-Amz-Executed-Version": version,
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
func lambdaLogTail(req *AWSRequest, fn LambdaFunction, version, requestID string, errored bool) string {
	if !strings.EqualFold(headerValueFold(req.Headers, "X-Amz-Log-Type"), "Tail") {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "START RequestId: %s Version: %s\n", requestID, version)
	if errored {
		fmt.Fprintf(&b, "[ERROR] Invocation failed\n")
	}
	fmt.Fprintf(&b, "END RequestId: %s\n", requestID)
	fmt.Fprintf(&b, "REPORT RequestId: %s\tDuration: 0.00 ms\tBilled Duration: 1 ms\tMemory Size: %d MB\tMax Memory Used: %d MB\n",
		requestID, fn.MemorySize, fn.MemorySize)
	return b.String()
}

// invoke answers Invoke for the function, or with a Qualifier, or a FunctionName carrying one, for a
// version or an alias. An alias runs the version it points at, or with a routing configuration the
// version [lambdaRouteAlias] draws for this request.
func (p *LambdaPlugin) invoke(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], "lambda:InvokeFunction")
	if err != nil {
		return nil, err
	}
	fn, version := t.function(), t.version
	if t.alias != nil {
		if routed := lambdaRouteAlias(ctx.IDs, t.alias); routed != version {
			v, err := p.loadVersion(t.accountID, t.region, fn.FunctionName, routed)
			if err != nil {
				return nil, err
			}
			if v == nil {
				return nil, lambdaQualifiedNotFound(t.latest.FunctionArn, routed)
			}
			fn, version = v.LambdaFunction, routed
		}
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
		return invokeResponse(body, version, seededErr.functionErrorHeader(),
			lambdaLogTail(req, fn, version, ctx.RequestID, true)), nil
	}
	if seededResult != nil {
		return invokeResponse(seededResult.body(), version, "", lambdaLogTail(req, fn, version, ctx.RequestID, false)), nil
	}

	stubPayload := []byte(`{"statusCode":200,"body":"null"}`)
	return invokeResponse(stubPayload, version, "", lambdaLogTail(req, fn, version, ctx.RequestID, false)), nil
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

	// "Qualifier — Specify a version or alias to add permissions to a published version of the
	// function": the version or alias must exist, its statements live in a policy of its own, and
	// each statement's Resource is the qualified ARN.
	t, err := p.resolveFunction(ctx, name, req.Params["Qualifier"], "lambda:AddPermission")
	if err != nil {
		return nil, err
	}
	fn := t.latest
	policyKey := lambdaQualifiedPolicyStateKey(t.accountID, t.region, fn.FunctionName, t.qualifier)

	// Load existing policy or create new one.
	policy, loadErr := p.loadPolicy(policyKey)
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

	resource := fn.FunctionArn
	if t.qualifier != "" {
		resource += ":" + t.qualifier
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
	if putErr := p.state.Put(context.Background(), lambdaNamespace, policyKey, policyData); putErr != nil {
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

// removePermission removes a statement from the function's resource policy, or with a Qualifier
// from the version's or alias's.
//
// A statement stored before versions and aliases had policies of their own sits in the function's
// policy with the qualified ARN as its Resource. A qualified RemovePermission that does not find the
// statement in the qualifier's policy looks for it there, so a stack deployed with such a statement
// still deletes.
func (p *LambdaPlugin) removePermission(ctx *RequestContext, req *AWSRequest, ref, statementID string) (*AWSResponse, error) {
	acct, region, name, derived, awsErr := lambdaFunctionScope(ctx, ref, "lambda:RemovePermission")
	if awsErr != nil {
		return nil, awsErr
	}
	qualifier, awsErr := lambdaMergeQualifier(derived, req.Params["Qualifier"])
	if awsErr != nil {
		return nil, awsErr
	}
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	key := lambdaQualifiedPolicyStateKey(acct, region, name, qualifier)
	found, hadPolicy, err := p.removeStatement(key, statementID, "")
	if err != nil {
		return nil, err
	}
	if unqualified := lambdaPolicyStateKey(acct, region, name); !found && key != unqualified {
		var legacyPolicy bool
		found, legacyPolicy, err = p.removeStatement(unqualified, statementID, fn.FunctionArn+":"+qualifier)
		if err != nil {
			return nil, err
		}
		hadPolicy = hadPolicy || legacyPolicy
	}
	switch {
	case !found && !hadPolicy:
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "No policy found for function: " + name, HTTPStatus: http.StatusNotFound}
	case !found:
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Statement not found: " + statementID, HTTPStatus: http.StatusNotFound}
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// removeStatement removes statement statementID from the policy at key, reporting whether it was
// there and whether there was a policy at all. A non-empty resource also requires the statement's
// Resource to be that ARN.
func (p *LambdaPlugin) removeStatement(key, statementID, resource string) (found, hadPolicy bool, err error) {
	policy, err := p.loadPolicy(key)
	if err != nil || policy == nil {
		return false, false, err
	}
	newStmts := make([]LambdaPermissionStatement, 0, len(policy.Statement))
	for _, stmt := range policy.Statement {
		if stmt.Sid == statementID && (resource == "" || stmt.Resource == resource) {
			found = true
			continue
		}
		newStmts = append(newStmts, stmt)
	}
	if !found {
		return false, true, nil
	}
	policy.Statement = newStmts
	policyData, err := json.Marshal(policy)
	if err != nil {
		return false, true, fmt.Errorf("lambda removePermission marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace, key, policyData); err != nil {
		return false, true, fmt.Errorf("lambda removePermission state.Put: %w", err)
	}
	return true, true, nil
}

// getPolicy answers the function's resource policy, or with a Qualifier the version's or alias's.
func (p *LambdaPlugin) getPolicy(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], "lambda:GetPolicy")
	if err != nil {
		return nil, err
	}
	name := t.latest.FunctionName
	policy, err := p.loadPolicy(lambdaQualifiedPolicyStateKey(t.accountID, t.region, name, t.qualifier))
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
	if _, isESM := lambdaESMUUIDFromARN(arn); isESM {
		return p.tagESM(ctx, arn, body.Tags)
	}

	if awsErr := lambdaCheckFunctionScope(ctx, arn, "lambda:TagResource"); awsErr != nil {
		return nil, awsErr
	}
	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	// Saved in the function's own scope, which the check above made the caller's.
	fnAcct, fnRegion := lambdaARNScope(fn.FunctionArn)
	fn.EverTagged = taggingEverTagged(fn.EverTagged, len(fn.Tags), len(body.Tags))
	if fn.Tags == nil {
		fn.Tags = make(map[string]string)
	}
	for k, v := range body.Tags {
		fn.Tags[k] = v
	}
	if _, saveErr := p.saveFunctionAndRespond(fnAcct, fnRegion, *fn, http.StatusOK); saveErr != nil {
		return nil, saveErr
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}, Body: nil}, nil
}

func (p *LambdaPlugin) untagResource(ctx *RequestContext, req *AWSRequest, arn string) (*AWSResponse, error) {
	if _, isESM := lambdaESMUUIDFromARN(arn); isESM {
		var keys []string
		if raw, ok := req.Params["tagKeys"]; ok {
			for _, k := range strings.Split(raw, ",") {
				keys = append(keys, strings.TrimSpace(k))
			}
		}
		return p.untagESM(ctx, arn, keys)
	}
	if awsErr := lambdaCheckFunctionScope(ctx, arn, "lambda:UntagResource"); awsErr != nil {
		return nil, awsErr
	}
	fn, err := p.findFunctionByARN(arn)
	if err != nil {
		return nil, err
	}
	// Saved in the function's own scope, which the check above made the caller's.
	fnAcct, fnRegion := lambdaARNScope(fn.FunctionArn)
	fn.EverTagged = taggingEverTagged(fn.EverTagged, len(fn.Tags), 0)
	if fn.Tags != nil {
		if keys, ok := req.Params["tagKeys"]; ok {
			for _, k := range strings.Split(keys, ",") {
				delete(fn.Tags, strings.TrimSpace(k))
			}
		}
	}
	if _, saveErr := p.saveFunctionAndRespond(fnAcct, fnRegion, *fn, http.StatusOK); saveErr != nil {
		return nil, saveErr
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}, Body: nil}, nil
}

func (p *LambdaPlugin) listTags(ctx *RequestContext, arn string) (*AWSResponse, error) {
	if _, isESM := lambdaESMUUIDFromARN(arn); isESM {
		return p.listESMTags(ctx, arn)
	}
	if awsErr := lambdaCheckFunctionScope(ctx, arn, "lambda:ListTags"); awsErr != nil {
		return nil, awsErr
	}
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

// loadPolicy reads the resource policy stored at key, nil when there is none.
func (p *LambdaPlugin) loadPolicy(key string) (*LambdaResourcePolicy, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, key)
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
		"FunctionName": fn.FunctionName,
		"FunctionArn":  fn.FunctionArn,
		"Runtime":      fn.Runtime,
		"Role":         fn.Role,
		"Handler":      fn.Handler,
		"Description":  fn.Description,
		"Timeout":      fn.Timeout,
		"MemorySize":   fn.MemorySize,
		"CodeSize":     fn.CodeSize,
		"CodeSha256":   fn.CodeSha256,
		"RevisionId":   fn.RevisionID,
		"State":        fn.State,
		// "The status of the last update that was performed on the function. This is first set
		// to Successful after function creation completes." Substrate applies every update
		// within its request, so the last one has always completed; the Terraform AWS provider
		// waits for this value after each code or configuration update (#1660).
		"LastUpdateStatus": "Successful",
		"PackageType":      fn.PackageType,
		"Architectures":    fn.Architectures,
		"LastModified":     fn.LastModified.Format(time.RFC3339),
		// FunctionConfiguration's Version: this is the function's own record, the unpublished
		// version; buildVersionConfig sets a published one's.
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
	// No Tags: FunctionConfiguration has no such member, and GetFunction answers a function's tags
	// beside Configuration rather than inside it (#1660).
	renderLambdaFunctionFields(cfg, fn)
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

	// FunctionResponseTypes is the response types the mapping applies, at most
	// ReportBatchItemFailures. Recorded intent: a poll deletes the whole batch or none of it.
	FunctionResponseTypes []string `json:"FunctionResponseTypes,omitempty"`

	// ScalingConfig is the SQS mapping's concurrency limit; nil when it sets none. Recorded intent.
	ScalingConfig *LambdaESMScalingConfig `json:"ScalingConfig,omitempty"`

	// Tags are the mapping's tags, answered only by ListTags.
	Tags map[string]string `json:"Tags,omitempty"`

	// EverTagged records that this mapping has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
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
		FunctionName          string                  `json:"FunctionName"`
		EventSourceArn        string                  `json:"EventSourceArn"`
		BatchSize             int                     `json:"BatchSize"`
		StartingPosition      string                  `json:"StartingPosition"`
		Enabled               *bool                   `json:"Enabled"`
		FunctionResponseTypes []string                `json:"FunctionResponseTypes"`
		ScalingConfig         *LambdaESMScalingConfig `json:"ScalingConfig"`
		Tags                  map[string]string       `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, lambdaInvalidBody()
	}
	if input.FunctionName == "" || input.EventSourceArn == "" {
		return nil, lambdaInvalidParameterValue("FunctionName and EventSourceArn are required")
	}
	isSQS := lambdaIsSQSSource(input.EventSourceArn)
	if isSQS && input.StartingPosition != "" {
		// The wording Lambda answers, observed in boto/boto3#1613 and
		// hashicorp/terraform-provider-aws#5025; StartingPosition is "Required for Amazon Kinesis and
		// Amazon DynamoDB Stream event sources" and an SQS queue has no position to start from.
		return nil, lambdaInvalidParameterValue("StartingPosition is not valid for SQS event sources.")
	}
	if awsErr := lambdaValidateResponseTypes(input.FunctionResponseTypes); awsErr != nil {
		return nil, awsErr
	}
	scaling, awsErr := lambdaValidateScalingConfig(input.ScalingConfig, input.EventSourceArn)
	if awsErr != nil {
		return nil, awsErr
	}

	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = lambdaESMBatchSizeDefault
		if isSQS {
			batchSize = lambdaESMBatchSizeSQS
		}
	}

	state := "Enabled"
	if input.Enabled != nil && !*input.Enabled {
		state = "Disabled"
	}

	// A stream source starts from TRIM_HORIZON when the request names no position, which is
	// substrate's default: AWS requires one for Kinesis and DynamoDB Streams.
	startingPosition := input.StartingPosition
	if startingPosition == "" && !isSQS {
		startingPosition = "TRIM_HORIZON"
	}

	functionARN, awsErr := lambdaESMFunctionARN(ctx, input.FunctionName)
	if awsErr != nil {
		return nil, awsErr
	}
	if isSQS {
		if err := p.checkQueueVisibility(ctx, input.FunctionName, input.EventSourceArn); err != nil {
			return nil, err
		}
	}

	uuid := ctx.IDs.HexUUID()
	esm := &ESMConfig{
		UUID:                  uuid,
		FunctionARN:           functionARN,
		EventSourceARN:        input.EventSourceArn,
		BatchSize:             batchSize,
		State:                 state,
		StartingPosition:      startingPosition,
		FunctionResponseTypes: input.FunctionResponseTypes,
		ScalingConfig:         scaling,
		Tags:                  input.Tags,
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

	return lambdaJSONResponse(http.StatusCreated, esm.wire())
}

// moveESMIndex moves mapping uuid from one function's mapping index to another's, for an update
// that re-points the mapping.
func (p *LambdaPlugin) moveESMIndex(ctx context.Context, uuid, from, to string) error {
	old, err := p.loadESMIDs(ctx, from)
	if err != nil {
		return err
	}
	if err := p.saveESMIDs(ctx, from, slices.DeleteFunc(old, func(id string) bool { return id == uuid })); err != nil {
		return fmt.Errorf("lambda moveESMIndex save %s: %w", from, err)
	}
	ids, err := p.loadESMIDs(ctx, to)
	if err != nil {
		return err
	}
	if err := p.saveESMIDs(ctx, to, append(ids, uuid)); err != nil {
		return fmt.Errorf("lambda moveESMIndex save %s: %w", to, err)
	}
	return nil
}

// lambdaESMFunctionARN resolves an event source mapping's FunctionName — a name, a qualified name, a
// partial ARN or a full ARN — to the function ARN the mapping invokes, with any version or alias.
//
// A mapping invokes its function as the caller, so a function in another account, or a full ARN in
// another Region, is refused as [lambdaCheckFunctionScope] refuses it: substrate does not evaluate a
// function's resource policy for a cross-account invoke.
func lambdaESMFunctionARN(ctx *RequestContext, ref string) (string, *AWSError) {
	acct, region, name, qualifier, awsErr := lambdaFunctionScope(ctx, ref, "lambda:InvokeFunction")
	if awsErr != nil {
		return "", awsErr
	}
	arn := lambdaFunctionARN(region, acct, name)
	if qualifier != "" {
		arn += ":" + qualifier
	}
	return arn, nil
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

	type response struct {
		EventSourceMappings []lambdaESMWire `json:"EventSourceMappings"`
		NextMarker          string          `json:"NextMarker,omitempty"`
	}
	out := response{EventSourceMappings: make([]lambdaESMWire, 0, len(page)), NextMarker: nextMarker}
	for _, esm := range page {
		out.EventSourceMappings = append(out.EventSourceMappings, esm.wire())
	}
	return lambdaJSONResponse(http.StatusOK, out)
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
		return nil, lambdaESMNotFound(uuid)
	}
	return lambdaJSONResponse(http.StatusOK, esm.wire())
}

func (p *LambdaPlugin) updateEventSourceMapping(ctx *RequestContext, req *AWSRequest, uuid string) (*AWSResponse, error) {
	// Parsed before the lookup; see updateFunctionCode above. Both members are optional, so an absent body
	// is still a no-op update rather than a refusal — only a body that is present and will not parse is
	// refused (#1007).
	var input struct {
		BatchSize    int    `json:"BatchSize"`
		Enabled      *bool  `json:"Enabled"`
		FunctionName string `json:"FunctionName"`
		// A pointer, so an empty list clears the response types and an absent one keeps them.
		FunctionResponseTypes *[]string `json:"FunctionResponseTypes"`
		// "To remove the configuration, pass an empty value."
		ScalingConfig *LambdaESMScalingConfig `json:"ScalingConfig"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, lambdaInvalidBody()
		}
	}
	if input.FunctionResponseTypes != nil {
		if awsErr := lambdaValidateResponseTypes(*input.FunctionResponseTypes); awsErr != nil {
			return nil, awsErr
		}
	}
	// FunctionName re-points the mapping, and is refused for another account's or Region's function
	// as CreateEventSourceMapping refuses it.
	newFunctionARN := ""
	if input.FunctionName != "" {
		arn, awsErr := lambdaESMFunctionARN(ctx, input.FunctionName)
		if awsErr != nil {
			return nil, awsErr
		}
		newFunctionARN = arn
	}

	esm, err := p.loadESM(context.Background(), uuid)
	if err != nil {
		return nil, err
	}
	if esm == nil {
		return nil, lambdaESMNotFound(uuid)
	}
	if input.ScalingConfig != nil {
		scaling, awsErr := lambdaValidateScalingConfig(input.ScalingConfig, esm.EventSourceARN)
		if awsErr != nil {
			return nil, awsErr
		}
		esm.ScalingConfig = scaling
	}
	if input.FunctionResponseTypes != nil {
		esm.FunctionResponseTypes = *input.FunctionResponseTypes
		if len(esm.FunctionResponseTypes) == 0 {
			esm.FunctionResponseTypes = nil
		}
	}

	prevState := esm.State

	if newFunctionARN != "" && newFunctionARN != esm.FunctionARN {
		if err := p.moveESMIndex(context.Background(), uuid, esm.FunctionARN, newFunctionARN); err != nil {
			return nil, err
		}
		esm.FunctionARN = newFunctionARN
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

	return lambdaJSONResponse(http.StatusOK, esm.wire())
}

func (p *LambdaPlugin) deleteEventSourceMapping(_ *RequestContext, uuid string) (*AWSResponse, error) {
	esm, err := p.loadESM(context.Background(), uuid)
	if err != nil {
		return nil, err
	}
	if esm == nil {
		return nil, lambdaESMNotFound(uuid)
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

	return lambdaJSONResponse(http.StatusOK, esm.wire())
}
