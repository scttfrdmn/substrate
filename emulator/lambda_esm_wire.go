package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Event-source-mapping responses, fields and tags (#1659, #1660).
//
// Every event-source-mapping operation answers EventSourceMappingConfiguration, rendered from the
// stored [ESMConfig] through [ESMConfig.wire], so the record's bookkeeping — its tags and its
// previously-tagged flag — never reaches a response: AWS answers a mapping's tags only through
// ListTags, and EventSourceMappingConfiguration has no Tags member.

// The SQS-only bounds API_ScalingConfig publishes for MaximumConcurrency: "Minimum value of 2.
// Maximum value of 1000.".
const (
	lambdaESMMinConcurrency = 2
	lambdaESMMaxConcurrency = 1000
)

// The default BatchSize of a mapping, which "varies by service. For Amazon SQS, the default is 10.
// For all other services, the default is 100." (API_CreateEventSourceMapping, BatchSize).
const (
	lambdaESMBatchSizeSQS     = 10
	lambdaESMBatchSizeDefault = 100
)

// lambdaESMReportBatchItemFailures is the one value FunctionResponseTypes publishes.
const lambdaESMReportBatchItemFailures = "ReportBatchItemFailures"

// LambdaESMScalingConfig is a mapping's ScalingConfig, which applies to an SQS source only.
type LambdaESMScalingConfig struct {
	// MaximumConcurrency limits the concurrent invocations the source can drive, 2 to 1000.
	MaximumConcurrency *int `json:"MaximumConcurrency,omitempty"`
}

// lambdaESMWire is EventSourceMappingConfiguration, the shape every event-source-mapping operation
// answers.
type lambdaESMWire struct {
	UUID                           string                  `json:"UUID"`
	EventSourceMappingArn          string                  `json:"EventSourceMappingArn,omitempty"`
	FunctionArn                    string                  `json:"FunctionArn"`
	EventSourceArn                 string                  `json:"EventSourceArn"`
	BatchSize                      int                     `json:"BatchSize"`
	State                          string                  `json:"State"`
	StartingPosition               string                  `json:"StartingPosition,omitempty"`
	MaximumBatchingWindowInSeconds int                     `json:"MaximumBatchingWindowInSeconds,omitempty"`
	FunctionResponseTypes          []string                `json:"FunctionResponseTypes,omitempty"`
	ScalingConfig                  *LambdaESMScalingConfig `json:"ScalingConfig,omitempty"`
}

// wire renders the mapping as EventSourceMappingConfiguration.
//
// StartingPosition is left out for an SQS source, which takes none; a record an older build wrote
// carries the TRIM_HORIZON substrate defaulted every mapping to.
func (e ESMConfig) wire() lambdaESMWire {
	w := lambdaESMWire{
		UUID:                           e.UUID,
		EventSourceMappingArn:          e.arn(),
		FunctionArn:                    e.FunctionARN,
		EventSourceArn:                 e.EventSourceARN,
		BatchSize:                      e.BatchSize,
		State:                          e.State,
		StartingPosition:               e.StartingPosition,
		MaximumBatchingWindowInSeconds: e.MaximumBatchingWindowInSeconds,
		FunctionResponseTypes:          e.FunctionResponseTypes,
		ScalingConfig:                  e.ScalingConfig,
	}
	if lambdaIsSQSSource(e.EventSourceARN) {
		w.StartingPosition = ""
	}
	return w
}

// arn is the mapping's EventSourceMappingArn,
// `arn:aws:lambda:{region}:{account}:event-source-mapping:{uuid}` as API_CreateEventSourceMapping's
// pattern gives it, in the account and Region of the function it invokes, which are the caller's:
// a mapping on another account's or Region's function is refused ([lambdaESMFunctionARN]). Empty
// when the function ARN is unreadable.
func (e ESMConfig) arn() string {
	acct, region := lambdaARNScope(e.FunctionARN)
	if acct == "" {
		return ""
	}
	return lambdaESMARN(region, acct, e.UUID)
}

// lambdaESMARN builds an event-source-mapping ARN.
func lambdaESMARN(region, accountID, uuid string) string {
	return "arn:aws:lambda:" + region + ":" + accountID + ":event-source-mapping:" + uuid
}

// lambdaESMUUIDFromARN returns the UUID an event-source-mapping ARN names, and false for any other
// ARN.
func lambdaESMUUIDFromARN(arn string) (string, bool) {
	parts := strings.Split(arn, ":")
	if len(parts) != 7 || parts[0] != "arn" || parts[2] != "lambda" || parts[5] != "event-source-mapping" || parts[6] == "" {
		return "", false
	}
	return parts[6], true
}

// lambdaIsSQSSource reports whether an event source ARN names an SQS queue.
func lambdaIsSQSSource(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) >= 6 && parts[0] == "arn" && parts[2] == "sqs"
}

// lambdaESMNotFound is the ResourceNotFoundException for a mapping that does not exist.
func lambdaESMNotFound(uuid string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The event source mapping " + uuid + " was not found.",
		HTTPStatus: http.StatusNotFound,
	}
}

// lambdaValidateResponseTypes refuses a FunctionResponseTypes outside its published shape — at most
// one item, "Valid Values: ReportBatchItemFailures" — with InvalidParameterValueException; the
// message is substrate's own.
func lambdaValidateResponseTypes(types []string) *AWSError {
	if len(types) > 1 {
		return lambdaInvalidParameterValue("FunctionResponseTypes takes at most one value")
	}
	for _, t := range types {
		if t != lambdaESMReportBatchItemFailures {
			return lambdaInvalidParameterValue("FunctionResponseTypes must be ReportBatchItemFailures, not " + t)
		}
	}
	return nil
}

// lambdaValidateScalingConfig refuses a ScalingConfig on a source other than SQS — the member is
// "(Amazon SQS only)" — and a MaximumConcurrency outside 2–1000, with
// InvalidParameterValueException; the messages are substrate's own. It returns the configuration
// the record keeps, nil when it limits nothing.
func lambdaValidateScalingConfig(cfg *LambdaESMScalingConfig, sourceARN string) (*LambdaESMScalingConfig, *AWSError) {
	if cfg == nil {
		return nil, nil
	}
	if !lambdaIsSQSSource(sourceARN) {
		return nil, lambdaInvalidParameterValue("ScalingConfig is supported only for Amazon SQS event sources")
	}
	if cfg.MaximumConcurrency == nil {
		return nil, nil
	}
	if n := *cfg.MaximumConcurrency; n < lambdaESMMinConcurrency || n > lambdaESMMaxConcurrency {
		return nil, lambdaInvalidParameterValue("ScalingConfig.MaximumConcurrency must be between " +
			strconv.Itoa(lambdaESMMinConcurrency) + " and " + strconv.Itoa(lambdaESMMaxConcurrency) + ", not " + strconv.Itoa(n))
	}
	n := *cfg.MaximumConcurrency
	return &LambdaESMScalingConfig{MaximumConcurrency: &n}, nil
}

// checkQueueVisibility refuses an SQS mapping whose queue's visibility timeout is lower than the
// function's timeout. The message is the one Lambda answers, which is observed rather than
// published: "Queue visibility timeout: 30 seconds is less than Function timeout: 60 seconds",
// reported from CreateEventSourceMapping in hashicorp/terraform-provider-aws#18099 and
// aws/serverless-application-model#1424.
//
// The function's timeout is the one the mapping invokes: a version's, or the version an alias points
// at. The queue is read from SQS's state, as CloudFormation reads it, rather than through a routed
// GetQueueAttributes, so the check records no event of its own; its effective VisibilityTimeout is
// the one GetQueueAttributes reports ([sqsEffectiveAttribute]). A function or queue that does not
// exist is not checked: substrate does not refuse a mapping on either.
func (p *LambdaPlugin) checkQueueVisibility(ctx *RequestContext, functionRef, queueARN string) error {
	t, err := p.resolveFunction(ctx, functionRef, "", "lambda:InvokeFunction")
	var awsErr *AWSError
	if errors.As(err, &awsErr) && awsErr.Code == "ResourceNotFoundException" {
		return nil
	}
	if err != nil {
		return err
	}
	parts := strings.Split(queueARN, ":")
	region, acct, name := parts[3], parts[4], parts[5]
	data, err := p.state.Get(context.Background(), sqsNamespace, sqsQueueStateKey(acct, region, name))
	if err != nil {
		return fmt.Errorf("lambda checkQueueVisibility state.Get: %w", err)
	}
	if data == nil {
		return nil
	}
	var q SQSQueue
	if err := json.Unmarshal(data, &q); err != nil {
		return fmt.Errorf("lambda checkQueueVisibility unmarshal: %w", err)
	}
	raw, _ := sqsEffectiveAttribute(&q, "VisibilityTimeout")
	visibility, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("lambda checkQueueVisibility VisibilityTimeout %q: %w", raw, err)
	}
	timeout := t.function().Timeout
	if visibility < timeout {
		return lambdaInvalidParameterValue("Queue visibility timeout: " + strconv.Itoa(visibility) +
			" seconds is less than Function timeout: " + strconv.Itoa(timeout) + " seconds")
	}
	return nil
}

// loadESMByARN resolves an event-source-mapping ARN named by a tag operation to its mapping.
//
// Another account's mapping is the AccessDeniedException [lambdaCheckFunctionScope] answers for
// another account's function, and another Region's is ResourceNotFoundException: the ARN's account
// and Region are read from the ARN itself, and the mapping must be the one the ARN names in them.
func (p *LambdaPlugin) loadESMByARN(ctx *RequestContext, arn, action string) (*ESMConfig, error) {
	uuid, _ := lambdaESMUUIDFromARN(arn)
	acct, region := lambdaARNScope(arn)
	if acct != ctx.AccountID {
		return nil, lambdaLayerDenied(ctx, action, arn)
	}
	esm, err := p.loadESM(context.Background(), uuid)
	if err != nil {
		return nil, err
	}
	if esm == nil || region != ctx.Region || esm.arn() != arn {
		return nil, lambdaESMNotFound(uuid)
	}
	return esm, nil
}

// tagESM merges a tag set onto a mapping: TagResource on an event-source-mapping ARN. The mapping is
// read under [LambdaPlugin.esmWriteMu], so a tag cannot re-save a mapping a delete has removed or be
// lost to an update's stale copy.
func (p *LambdaPlugin) tagESM(ctx *RequestContext, arn string, tags map[string]string) (*AWSResponse, error) {
	p.esmWriteMu.Lock()
	defer p.esmWriteMu.Unlock()
	esm, err := p.loadESMByARN(ctx, arn, "lambda:TagResource")
	if err != nil {
		return nil, err
	}
	esm.EverTagged = taggingEverTagged(esm.EverTagged, len(esm.Tags), len(tags))
	esm.Tags = mergeStringMap(esm.Tags, tags, nil)
	if err := p.saveESM(context.Background(), esm); err != nil {
		return nil, fmt.Errorf("lambda tagESM saveESM: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

// untagESM removes tag keys from a mapping: UntagResource on an event-source-mapping ARN.
func (p *LambdaPlugin) untagESM(ctx *RequestContext, arn string, keys []string) (*AWSResponse, error) {
	p.esmWriteMu.Lock()
	defer p.esmWriteMu.Unlock()
	esm, err := p.loadESMByARN(ctx, arn, "lambda:UntagResource")
	if err != nil {
		return nil, err
	}
	esm.EverTagged = taggingEverTagged(esm.EverTagged, len(esm.Tags), 0)
	esm.Tags = mergeStringMap(esm.Tags, nil, keys)
	if err := p.saveESM(context.Background(), esm); err != nil {
		return nil, fmt.Errorf("lambda untagESM saveESM: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

// listESMTags answers ListTags on an event-source-mapping ARN.
func (p *LambdaPlugin) listESMTags(ctx *RequestContext, arn string) (*AWSResponse, error) {
	esm, err := p.loadESMByARN(ctx, arn, "lambda:ListTags")
	if err != nil {
		return nil, err
	}
	tags := esm.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return lambdaJSONResponse(http.StatusOK, map[string]interface{}{"Tags": tags})
}

// lambdaTaggedEventSourceMappings lists the event-source mappings in one account and Region for the
// Resource Groups Tagging API's GetResources, as resource type `lambda:event-source-mapping`. The
// mapping key carries no scope, so each mapping's own ARN decides it. A mapping that cannot be read
// is skipped and its error returned beside the ones that could.
func lambdaTaggedEventSourceMappings(ctx context.Context, state StateManager, accountID, region string) ([]resourceTagMapping, error) {
	keys, err := state.List(ctx, lambdaNamespace, "esm:")
	if err != nil {
		return nil, fmt.Errorf("list lambda event source mappings: %w", err)
	}
	var out []resourceTagMapping
	var readErr error
	for _, k := range keys {
		raw, err := state.Get(ctx, lambdaNamespace, k)
		if err != nil {
			readErr = errors.Join(readErr, fmt.Errorf("get %s: %w", k, err))
			continue
		}
		if raw == nil {
			continue
		}
		var esm ESMConfig
		if err := json.Unmarshal(raw, &esm); err != nil {
			readErr = errors.Join(readErr, fmt.Errorf("decode %s: %w", k, err))
			continue
		}
		if acct, reg := lambdaARNScope(esm.FunctionARN); acct != accountID || reg != region {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: esm.arn(),
			Tags:        mapToTaggingTags(esm.Tags),
			everTagged:  esm.EverTagged,
		})
	}
	return out, readErr
}
