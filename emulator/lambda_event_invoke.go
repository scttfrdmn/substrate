package emulator

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Event-invoke configuration (#1685): Put, Get, Update, Delete and List FunctionEventInvokeConfig,
// all under `/2019-09-25/functions/{FunctionName}/event-invoke-config`.
//
// A configuration belongs to a function, a version or an alias — "Configures options for
// asynchronous invocation on a function, version, or alias" — named by a Qualifier or a qualified
// FunctionName, and $LATEST is the function's own. Put "overwrites" an existing configuration, and
// "if you exclude any settings, they are removed"; Update sets the settings it names "without
// affecting existing settings for other options" (both quoted from the AWS CLI reference for
// put-function-event-invoke-config). Everything is recorded intent: substrate invokes nothing
// asynchronously, so nothing retries and no destination receives a record.

// The bounds of the two numeric settings, from the API reference: MaximumRetryAttempts "Minimum
// value of 0. Maximum value of 2." and MaximumEventAgeInSeconds "Minimum value of 60. Maximum
// value of 21600.".
const (
	lambdaMaxRetryAttempts   = 2
	lambdaMinEventAgeSeconds = 60
	lambdaMaxEventAgeSeconds = 21600
)

// lambdaEventInvokePageSize is ListFunctionEventInvokeConfigs' largest MaxItems, "Maximum value of
// 50", and its page size when MaxItems is absent.
const lambdaEventInvokePageSize = 50

// lambdaEventInvokeRequest is the body of a Put or Update.
type lambdaEventInvokeRequest struct {
	MaximumRetryAttempts     *int `json:"MaximumRetryAttempts"`
	MaximumEventAgeInSeconds *int `json:"MaximumEventAgeInSeconds"`
	DestinationConfig        *struct {
		OnSuccess *lambdaDestination `json:"OnSuccess"`
		OnFailure *lambdaDestination `json:"OnFailure"`
	} `json:"DestinationConfig"`
}

// lambdaDestination is one side of a DestinationConfig.
type lambdaDestination struct {
	// Destination is the destination resource's ARN.
	Destination string `json:"Destination,omitempty"`
}

// lambdaEventInvokeConfigWire is FunctionEventInvokeConfig, the shape every one of the operations
// answers.
type lambdaEventInvokeConfigWire struct {
	FunctionArn              string                     `json:"FunctionArn"`
	LastModified             EpochSeconds               `json:"LastModified,omitzero"`
	MaximumRetryAttempts     *int                       `json:"MaximumRetryAttempts,omitempty"`
	MaximumEventAgeInSeconds *int                       `json:"MaximumEventAgeInSeconds,omitempty"`
	DestinationConfig        lambdaDestinationConfigOut `json:"DestinationConfig"`
}

// lambdaDestinationConfigOut renders both sides, empty ones as `{}`, as the CLI reference's sample
// output shows: `"DestinationConfig": {"OnSuccess": {}, "OnFailure": {}}`.
type lambdaDestinationConfigOut struct {
	OnSuccess lambdaDestination `json:"OnSuccess"`
	OnFailure lambdaDestination `json:"OnFailure"`
}

// parseLambdaEventInvokeRequest decodes and validates a Put or Update body. A setting outside its
// published range, and an S3 bucket as an on-success destination — "S3 buckets are supported only
// for on-failure destinations" — are InvalidParameterValueException; the messages are substrate's
// own.
func parseLambdaEventInvokeRequest(req *AWSRequest) (lambdaEventInvokeRequest, *AWSError) {
	var body lambdaEventInvokeRequest
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return body, lambdaInvalidBody()
		}
	}
	if n := body.MaximumRetryAttempts; n != nil && (*n < 0 || *n > lambdaMaxRetryAttempts) {
		return body, lambdaInvalidParameterValue("MaximumRetryAttempts must be between 0 and " +
			strconv.Itoa(lambdaMaxRetryAttempts) + ", not " + strconv.Itoa(*n))
	}
	if n := body.MaximumEventAgeInSeconds; n != nil && (*n < lambdaMinEventAgeSeconds || *n > lambdaMaxEventAgeSeconds) {
		return body, lambdaInvalidParameterValue("MaximumEventAgeInSeconds must be between " +
			strconv.Itoa(lambdaMinEventAgeSeconds) + " and " + strconv.Itoa(lambdaMaxEventAgeSeconds) + ", not " + strconv.Itoa(*n))
	}
	if d := body.DestinationConfig; d != nil && d.OnSuccess != nil && strings.HasPrefix(d.OnSuccess.Destination, "arn:aws:s3:") {
		return body, lambdaInvalidParameterValue("an S3 bucket is supported only as an OnFailure destination")
	}
	return body, nil
}

// apply writes the settings the request names onto cfg. With replace, a setting the request leaves
// out is removed, as Put removes it; without, it is kept, as Update keeps it.
func (b lambdaEventInvokeRequest) apply(cfg *LambdaEventInvokeConfig, replace bool) {
	if replace {
		cfg.MaximumRetryAttempts, cfg.MaximumEventAgeInSeconds = nil, nil
		cfg.OnSuccessDestination, cfg.OnFailureDestination = "", ""
	}
	if b.MaximumRetryAttempts != nil {
		cfg.MaximumRetryAttempts = b.MaximumRetryAttempts
	}
	if b.MaximumEventAgeInSeconds != nil {
		cfg.MaximumEventAgeInSeconds = b.MaximumEventAgeInSeconds
	}
	if d := b.DestinationConfig; d != nil {
		if d.OnSuccess != nil {
			cfg.OnSuccessDestination = d.OnSuccess.Destination
		}
		if d.OnFailure != nil {
			cfg.OnFailureDestination = d.OnFailure.Destination
		}
	}
}

// wire renders cfg for the function whose unqualified ARN is functionArn. FunctionArn is qualified,
// $LATEST for the function's own configuration, as the CLI reference's sample shows
// ("arn:aws:lambda:us-west-2:123456789012:function:my-function:$LATEST").
func (c LambdaEventInvokeConfig) wire(functionArn string) lambdaEventInvokeConfigWire {
	return lambdaEventInvokeConfigWire{
		FunctionArn:              functionArn + ":" + cmp.Or(c.Qualifier, lambdaLatestVersion),
		LastModified:             EpochSeconds(c.LastModified),
		MaximumRetryAttempts:     c.MaximumRetryAttempts,
		MaximumEventAgeInSeconds: c.MaximumEventAgeInSeconds,
		DestinationConfig: lambdaDestinationConfigOut{
			OnSuccess: lambdaDestination{Destination: c.OnSuccessDestination},
			OnFailure: lambdaDestination{Destination: c.OnFailureDestination},
		},
	}
}

// lambdaEventInvokeConfigNotFound is the ResourceNotFoundException for a function, version or alias
// with no configuration. The wording is observed rather than published: the AWS CLI answer quoted in
// mobilefirstdev/serverless-aws-alias-v4#5 for get-function-event-invoke-config on an alias,
// "... doesn't have an EventInvokeConfig", and LocalStack's emulation of the same answer, which
// names the qualified function ARN.
func lambdaEventInvokeConfigNotFound(qualifiedArn string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The function " + qualifiedArn + " doesn't have an EventInvokeConfig",
		HTTPStatus: http.StatusNotFound,
	}
}

// lambdaInvokeConfigQualifier is the qualifier a configuration is stored under: empty for $LATEST.
func lambdaInvokeConfigQualifier(t *lambdaTarget) string {
	if t.qualifier == lambdaLatestVersion {
		return ""
	}
	return t.qualifier
}

// loadEventInvokeConfig reads the configuration at key, nil when there is none.
func (p *LambdaPlugin) loadEventInvokeConfig(key string) (*LambdaEventInvokeConfig, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("lambda loadEventInvokeConfig state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var cfg LambdaEventInvokeConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("lambda loadEventInvokeConfig unmarshal: %w", err)
	}
	return &cfg, nil
}

// writeFunctionEventInvokeConfig serves Put (replace) and Update (merge). The body is parsed before
// the function is looked up, as for every Lambda write ([LambdaPlugin.updateFunctionCode]). Update
// of a function, version or alias with no configuration is the not-found Get answers.
//
// The target is resolved and the configuration written under [LambdaPlugin.versionMu], the lock
// DeleteFunction, deleting a version and DeleteAlias hold while they delete the configurations they
// own: otherwise a write could resolve an alias, lose the race to its delete, and leave a
// configuration that an alias re-created under the name would inherit.
func (p *LambdaPlugin) writeFunctionEventInvokeConfig(ctx *RequestContext, req *AWSRequest, ref string, replace bool) (*AWSResponse, error) {
	body, awsErr := parseLambdaEventInvokeRequest(req)
	if awsErr != nil {
		return nil, awsErr
	}
	action := "lambda:UpdateFunctionEventInvokeConfig"
	if replace {
		action = "lambda:PutFunctionEventInvokeConfig"
	}
	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], action)
	if err != nil {
		return nil, err
	}
	qualifier := lambdaInvokeConfigQualifier(t)
	key := lambdaQualifiedInvokeConfigStateKey(t.accountID, t.region, t.latest.FunctionName, qualifier)

	existing, err := p.loadEventInvokeConfig(key)
	if err != nil {
		return nil, err
	}
	cfg := LambdaEventInvokeConfig{FunctionName: t.latest.FunctionName, Qualifier: qualifier}
	switch {
	case existing != nil:
		cfg = *existing
		cfg.Qualifier = qualifier
	case !replace:
		return nil, lambdaEventInvokeConfigNotFound(t.latest.FunctionArn + ":" + cmp.Or(qualifier, lambdaLatestVersion))
	}
	body.apply(&cfg, replace)
	cfg.LastModified = p.tc.Now()

	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("lambda writeFunctionEventInvokeConfig marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace, key, data); err != nil {
		return nil, fmt.Errorf("lambda writeFunctionEventInvokeConfig state.Put: %w", err)
	}
	return lambdaJSONResponse(http.StatusOK, cfg.wire(t.latest.FunctionArn))
}

// putFunctionEventInvokeConfig answers PutFunctionEventInvokeConfig.
func (p *LambdaPlugin) putFunctionEventInvokeConfig(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	return p.writeFunctionEventInvokeConfig(ctx, req, ref, true)
}

// updateFunctionEventInvokeConfig answers UpdateFunctionEventInvokeConfig.
func (p *LambdaPlugin) updateFunctionEventInvokeConfig(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	return p.writeFunctionEventInvokeConfig(ctx, req, ref, false)
}

// getFunctionEventInvokeConfig answers GetFunctionEventInvokeConfig.
func (p *LambdaPlugin) getFunctionEventInvokeConfig(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], "lambda:GetFunctionEventInvokeConfig")
	if err != nil {
		return nil, err
	}
	qualifier := lambdaInvokeConfigQualifier(t)
	cfg, err := p.loadEventInvokeConfig(lambdaQualifiedInvokeConfigStateKey(t.accountID, t.region, t.latest.FunctionName, qualifier))
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, lambdaEventInvokeConfigNotFound(t.latest.FunctionArn + ":" + cmp.Or(qualifier, lambdaLatestVersion))
	}
	cfg.Qualifier = qualifier
	return lambdaJSONResponse(http.StatusOK, cfg.wire(t.latest.FunctionArn))
}

// deleteFunctionEventInvokeConfig answers DeleteFunctionEventInvokeConfig with the 204 its page
// publishes, or the not-found Get answers when there is nothing to delete. It holds
// [LambdaPlugin.versionMu] as the writes do.
func (p *LambdaPlugin) deleteFunctionEventInvokeConfig(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], "lambda:DeleteFunctionEventInvokeConfig")
	if err != nil {
		return nil, err
	}
	qualifier := lambdaInvokeConfigQualifier(t)
	key := lambdaQualifiedInvokeConfigStateKey(t.accountID, t.region, t.latest.FunctionName, qualifier)

	cfg, err := p.loadEventInvokeConfig(key)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, lambdaEventInvokeConfigNotFound(t.latest.FunctionArn + ":" + cmp.Or(qualifier, lambdaLatestVersion))
	}
	if err := p.state.Delete(context.Background(), lambdaNamespace, key); err != nil {
		return nil, fmt.Errorf("lambda deleteFunctionEventInvokeConfig state.Delete: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// listFunctionEventInvokeConfigs answers ListFunctionEventInvokeConfigs: every configuration of one
// function, its own ($LATEST) first and then its versions' and aliases' in qualifier order.
// MaxItems publishes "Minimum value of 1. Maximum value of 50."; an absent one is 50, which is
// substrate's reading, since no default is published. FunctionName names the function and never one
// of its versions or aliases.
func (p *LambdaPlugin) listFunctionEventInvokeConfigs(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:ListFunctionEventInvokeConfigs")
	if awsErr != nil {
		return nil, awsErr
	}
	pageSize := lambdaEventInvokePageSize
	if raw := req.Params["MaxItems"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > lambdaEventInvokePageSize {
			return nil, lambdaInvalidParameterValue("MaxItems must be between 1 and " + strconv.Itoa(lambdaEventInvokePageSize))
		}
		pageSize = n
	}
	offset, ok := decodeOffsetPaginationToken(req.Params["Marker"])
	if !ok {
		return nil, lambdaInvalidParameterValue("the Marker '" + req.Params["Marker"] + "' is invalid")
	}
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}

	base := lambdaInvokeConfigStateKey(acct, region, name)
	keys, err := p.state.List(context.Background(), lambdaNamespace, base)
	if err != nil {
		return nil, fmt.Errorf("lambda listFunctionEventInvokeConfigs state.List: %w", err)
	}
	// The prefix also selects a longer function name ("a" selects "ab"), so only the function's own
	// key and its qualified ones are kept.
	keys = slices.DeleteFunc(keys, func(k string) bool { return k != base && !strings.HasPrefix(k, base+":") })
	sort.Strings(keys)

	var configs []lambdaEventInvokeConfigWire
	var readErr error
	for _, k := range keys {
		cfg, err := p.loadEventInvokeConfig(k)
		if err != nil {
			readErr = errors.Join(readErr, err)
			continue
		}
		if cfg == nil {
			continue
		}
		cfg.Qualifier = strings.TrimPrefix(strings.TrimPrefix(k, base), ":")
		configs = append(configs, cfg.wire(fn.FunctionArn))
	}
	if readErr != nil {
		return nil, readErr
	}
	pageItems, next := pageByOffsetToken(configs, offset, pageSize)
	if pageItems == nil {
		pageItems = []lambdaEventInvokeConfigWire{}
	}
	return lambdaJSONResponse(http.StatusOK, struct {
		FunctionEventInvokeConfigs []lambdaEventInvokeConfigWire `json:"FunctionEventInvokeConfigs"`
		NextMarker                 string                        `json:"NextMarker,omitempty"`
	}{FunctionEventInvokeConfigs: pageItems, NextMarker: next})
}
