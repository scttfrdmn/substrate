package emulator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// What a due schedule calls (#1501).
//
// `API_Target` publishes two kinds of target, and `managing-targets-templated.html` and
// `managing-targets-universal.html` publish how each is invoked.
//
// # Universal targets
//
// A universal target's ARN is `arn:aws:scheduler:::aws-sdk:<service>:<apiAction>`, and its Input is
// "A well-formed JSON you specify with the request parameters that EventBridge Scheduler sends to
// the target API" — the operation's request in the SDK's shape. The service "must match the AWS SDK
// service identifier for the target service. This identifier can differ from the service's endpoint
// prefix. For example, for Amazon Cognito Identity Provider, use cognitoidentityprovider (not
// cognito-idp)." [schedulerSDKServices] maps those identifiers to substrate's plugins.
//
// The apiAction is the operation's name in lowerCamel (`putParameter`, `updateAutoScalingGroup`).
// It is resolved against the target plugin's routed operations case-insensitively, so an action
// whose acronym the camel form spells differently still names the operation; one that names none
// is sent with its first letter raised, and the plugin answers it as it answers any operation it
// does not route.
//
// The Input becomes a request in the target plugin's own protocol:
//
//   - JSON (awsJson1_0/1_1): the Input is the body, under the plugin's X-Amz-Target.
//   - Query (awsQuery): the Input is flattened into form parameters — a structure member becomes
//     `Parent.Member`, a list element `Parent.member.N`, and a scalar its string form — with the
//     operation's Action and the service's API Version. This is the generic rule botocore's query
//     serializer applies to a JSON-shaped input, so it serves every Query operation rather than
//     the two the consumer that asked for it uses.
//   - EC2 (the ec2 protocol): the same flattening with `Parent.N` for a list element. The ec2
//     protocol renames some list members (`TagSpecifications` is sent as `TagSpecification`), and
//     those renames live in the service model, which substrate does not carry; an Input relying on
//     one reaches EC2 under its JSON name.
//   - REST: only Lambda's Invoke is bound (FunctionName and Qualifier to the URI, InvocationType to
//     its header, Payload to the body), because a REST operation's URI and header bindings live in
//     the service model too. Any other REST action is recorded as intent and not dispatched.
//
// A universal target whose service substrate does not emulate is also recorded as intent.
//
// # Templated targets
//
// A templated target names a resource ARN, and "EventBridge Scheduler automatically assumes that you
// want to call the supported API operation for that service". Substrate dispatches the five whose
// services it emulates: Lambda Invoke (asynchronously — "EventBridge Scheduler invokes your Lambda
// function asynchronously", Lambda developer guide), SQS SendMessage (with SqsParameters'
// MessageGroupId), SNS Publish, Step Functions StartExecution and EventBridge PutEvents (with
// EventBridgeParameters' DetailType and Source). The Input is the payload each operation carries.
// The other templated targets — CodeBuild, CodePipeline, ECS, Inspector, Kinesis, Firehose and
// SageMaker — are recorded as intent.
//
// # Context attributes
//
// `managing-schedule-context-attributes.html` publishes four keywords "EventBridge Scheduler replaces
// each keyword with its respective value when your schedule invokes the target", and substrate
// replaces all four in the Input before it is sent.

// schedulerUniversalPrefix begins every universal target ARN.
const schedulerUniversalPrefix = ":scheduler:::aws-sdk:"

// schedSDKService maps an AWS SDK service identifier to the plugin that emulates it and, for a
// Query service, the API version its requests carry.
type schedSDKService struct {
	plugin  string
	version string
}

// schedulerSDKServices maps each AWS SDK service identifier substrate emulates to its plugin. The
// identifiers are the SDKs' service package names (the model's serviceId, lowercased, spaces
// removed), which is the form the universal-target guide asks for.
var schedulerSDKServices = map[string]schedSDKService{ //nolint:gochecknoglobals // immutable lookup table
	"acm":                      {plugin: "acm"},
	"athena":                   {plugin: "athena"},
	"autoscaling":              {plugin: "autoscaling", version: "2011-01-01"},
	"budgets":                  {plugin: "budgets"},
	"cloudformation":           {plugin: "cloudformation", version: "2010-05-15"},
	"cloudtrail":               {plugin: "cloudtrail"},
	"cloudwatch":               {plugin: "monitoring", version: "2010-08-01"},
	"cloudwatchlogs":           {plugin: "logs"},
	"codebuild":                {plugin: "codebuild"},
	"codedeploy":               {plugin: "codedeploy"},
	"codepipeline":             {plugin: "codepipeline"},
	"cognitoidentity":          {plugin: "cognito-identity"},
	"cognitoidentityprovider":  {plugin: "cognito-idp"},
	"configservice":            {plugin: "config"},
	"costexplorer":             {plugin: "ce"},
	"dynamodb":                 {plugin: "dynamodb"},
	"ec2":                      {plugin: "ec2", version: "2016-11-15"},
	"ecr":                      {plugin: "ecr"},
	"ecs":                      {plugin: "ecs"},
	"elasticache":              {plugin: "elasticache", version: "2015-02-02"},
	"elasticloadbalancing":     {plugin: "elasticloadbalancing", version: "2012-06-01"},
	"elasticloadbalancingv2":   {plugin: "elasticloadbalancing", version: "2015-12-01"},
	"eventbridge":              {plugin: "eventbridge"},
	"firehose":                 {plugin: "firehose"},
	"fsx":                      {plugin: "fsx"},
	"glue":                     {plugin: "glue"},
	"health":                   {plugin: "health"},
	"iam":                      {plugin: "iam", version: "2010-05-08"},
	"kinesis":                  {plugin: "kinesis"},
	"kms":                      {plugin: "kms"},
	"lambda":                   {plugin: "lambda"},
	"organizations":            {plugin: "organizations"},
	"pricing":                  {plugin: "pricing"},
	"rds":                      {plugin: "rds", version: "2014-10-31"},
	"redshift":                 {plugin: "redshift", version: "2012-12-01"},
	"redshiftdata":             {plugin: "redshift-data"},
	"resourcegroupstaggingapi": {plugin: "tagging"},
	"sagemaker":                {plugin: "sagemaker"},
	"secretsmanager":           {plugin: "secretsmanager"},
	"servicequotas":            {plugin: "servicequotas"},
	"sfn":                      {plugin: "states"},
	"sns":                      {plugin: "sns", version: "2010-03-31"},
	"sqs":                      {plugin: "sqs"},
	"ssm":                      {plugin: "ssm"},
	"ssoadmin":                 {plugin: "sso"},
	"sts":                      {plugin: "sts", version: "2011-06-15"},
	"timestreamquery":          {plugin: "timestream"},
	"timestreamwrite":          {plugin: "timestream"},
	"transfer":                 {plugin: "transfer"},
	"wafv2":                    {plugin: "wafv2"},
}

// schedTargetCall is one request a target invocation makes, or the reason it makes none.
type schedTargetCall struct {
	req       *AWSRequest
	accountID string
	region    string
	// intent, when set, says why the target is recorded rather than dispatched.
	intent string
	// invalid, when set, is a failure the target's own service would answer for this Input.
	invalid *schedFailure
}

// schedContextInput replaces the published context attributes in a target's Input.
func schedContextInput(input, scheduleARN, scheduledTime, executionID string, attempt int) string {
	return strings.NewReplacer(
		"<aws.scheduler.schedule-arn>", scheduleARN,
		"<aws.scheduler.scheduled-time>", scheduledTime,
		"<aws.scheduler.execution-id>", executionID,
		"<aws.scheduler.attempt-number>", strconv.Itoa(attempt),
	).Replace(input)
}

// buildTargetCall builds the request a schedule's target makes with input.
func buildTargetCall(rec *SchedulerRecord, input string) schedTargetCall {
	arn := rec.Target.ARN
	if i := strings.Index(arn, schedulerUniversalPrefix); i >= 0 && strings.HasPrefix(arn, "arn:") {
		return buildUniversalCall(rec, arn[i+len(schedulerUniversalPrefix):], input)
	}
	return buildTemplatedCall(rec, input)
}

// buildUniversalCall builds a universal target's request from its "<service>:<apiAction>".
func buildUniversalCall(rec *SchedulerRecord, spec, input string) schedTargetCall {
	sdkID, action, ok := strings.Cut(spec, ":")
	if !ok || sdkID == "" || action == "" {
		return schedTargetCall{intent: "universal target " + rec.Target.ARN + " names no service and action"}
	}
	svc, ok := schedulerSDKServices[sdkID]
	if !ok {
		return schedTargetCall{intent: "service " + sdkID + " is not emulated"}
	}
	op := schedResolveOperation(svc.plugin, action)
	call := schedTargetCall{accountID: rec.AccountID, region: rec.Region}
	if strings.TrimSpace(input) == "" {
		input = "{}"
	}
	routing := pluginRouting[svc.plugin]
	switch {
	case routing.Protocol == "JSON":
		call.req = schedJSONRequest(svc.plugin, routing.TargetPrefix, op, []byte(input))
	case strings.Contains(routing.Protocol, "Query"):
		params, failure := schedQueryParams(input, svc.plugin == "ec2")
		if failure != nil {
			call.invalid = failure
			return call
		}
		params["Action"] = op
		params["Version"] = svc.version
		call.req = &AWSRequest{
			Service: svc.plugin, Operation: op, HTTPMethod: http.MethodPost, Path: "/",
			Headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
			Params:  params,
		}
	case svc.plugin == "lambda" && op == "Invoke":
		var in struct {
			FunctionName   string          `json:"FunctionName"`
			Qualifier      string          `json:"Qualifier"`
			InvocationType string          `json:"InvocationType"`
			Payload        json.RawMessage `json:"Payload"`
		}
		if err := json.Unmarshal([]byte(input), &in); err != nil {
			call.invalid = schedInvalidInput(err)
			return call
		}
		payload := []byte(in.Payload)
		var s string
		if json.Unmarshal(in.Payload, &s) == nil {
			// The SDK's Payload is a blob; in a JSON Input it is a string holding the event.
			payload = []byte(s)
		}
		call.req = schedLambdaInvoke(lambdaFunctionNameFromRef(in.FunctionName), in.Qualifier, in.InvocationType, payload)
		if acct, region, ok := lambdaARNAccountRegion(in.FunctionName); ok {
			call.accountID, call.region = acct, region
		}
	default:
		return schedTargetCall{intent: "the " + routing.Protocol + " action " + sdkID + ":" + action + " has no URI binding in substrate"}
	}
	return call
}

// schedResolveOperation returns the routed operation an apiAction names, matched
// case-insensitively, or the action with its first letter raised.
func schedResolveOperation(plugin, action string) string {
	for _, op := range routedOperations[plugin] {
		if strings.EqualFold(op, action) {
			return op
		}
	}
	return strings.ToUpper(action[:1]) + action[1:]
}

// schedJSONRequest builds a JSON-protocol request.
func schedJSONRequest(service, targetPrefix, op string, body []byte) *AWSRequest {
	return &AWSRequest{
		Service: service, Operation: op, HTTPMethod: http.MethodPost, Path: "/",
		Headers: map[string]string{
			"Content-Type": "application/x-amz-json-1.0",
			"X-Amz-Target": targetPrefix + "." + op,
		},
		Body:   body,
		Params: map[string]string{},
	}
}

// schedLambdaInvoke builds a Lambda Invoke request.
func schedLambdaInvoke(name, qualifier, invocationType string, payload []byte) *AWSRequest {
	if invocationType == "" {
		invocationType = "Event"
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	params := map[string]string{}
	if qualifier != "" {
		params["Qualifier"] = qualifier
	}
	return &AWSRequest{
		Service: "lambda", Operation: "Invoke", HTTPMethod: http.MethodPost,
		Path: "/2015-03-31/functions/" + url.PathEscape(name) + "/invocations",
		Headers: map[string]string{
			"Content-Type":          "application/json",
			"X-Amz-Invocation-Type": invocationType,
		},
		Body:   payload,
		Params: params,
	}
}

// lambdaFunctionNameFromRef reduces a FunctionName — a name, a partial ARN or a full ARN — to the
// name the invoke path carries.
func lambdaFunctionNameFromRef(ref string) string {
	parts := strings.Split(ref, ":")
	for i, p := range parts {
		if p == "function" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ref
}

// lambdaARNAccountRegion reads the account and Region of a full function ARN.
func lambdaARNAccountRegion(ref string) (string, string, bool) {
	parts := strings.Split(ref, ":")
	if len(parts) >= 7 && parts[0] == "arn" && parts[2] == "lambda" {
		return parts[4], parts[3], true
	}
	return "", "", false
}

// schedInvalidInput is the failure a target answers for an Input that is not the JSON its
// operation takes.
func schedInvalidInput(err error) *schedFailure {
	return &schedFailure{
		Code:    "ValidationException",
		Message: "The Input is not a well-formed JSON request: " + err.Error(),
		Status:  http.StatusBadRequest,
	}
}

// schedQueryParams flattens a JSON Input into Query form parameters. ec2 selects the ec2
// protocol's list form.
func schedQueryParams(input string, ec2 bool) (map[string]string, *schedFailure) {
	dec := json.NewDecoder(bytes.NewReader([]byte(input)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, schedInvalidInput(err)
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, schedInvalidInput(fmt.Errorf("the Input is a %T, not an object", v))
	}
	out := map[string]string{}
	schedFlattenQuery(out, "", obj, ec2)
	return out, nil
}

// schedFlattenQuery writes v under prefix into out.
func schedFlattenQuery(out map[string]string, prefix string, v any, ec2 bool) {
	join := func(a, b string) string {
		if a == "" {
			return b
		}
		return a + "." + b
	}
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			schedFlattenQuery(out, join(prefix, k), t[k], ec2)
		}
	case []any:
		if len(t) == 0 {
			if !ec2 {
				// An empty list is sent as the bare member name, which is how botocore's query
				// serializer distinguishes "clear this list" from "leave it unchanged".
				out[prefix] = ""
			}
			return
		}
		for i, e := range t {
			idx := strconv.Itoa(i + 1)
			if ec2 {
				schedFlattenQuery(out, join(prefix, idx), e, ec2)
			} else {
				schedFlattenQuery(out, join(prefix, "member."+idx), e, ec2)
			}
		}
	case json.Number:
		out[prefix] = t.String()
	case bool:
		out[prefix] = strconv.FormatBool(t)
	case string:
		out[prefix] = t
	case nil:
	}
}

// buildTemplatedCall builds a templated target's request from its resource ARN.
func buildTemplatedCall(rec *SchedulerRecord, input string) schedTargetCall {
	arn := rec.Target.ARN
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return schedTargetCall{intent: "target " + arn + " is not an ARN substrate dispatches"}
	}
	service, region, account, resource := parts[2], parts[3], parts[4], parts[5]
	call := schedTargetCall{accountID: account, region: region}
	switch service {
	case "lambda":
		fn := strings.Split(resource, ":") // function:{name}[:{qualifier}]
		if len(fn) < 2 || fn[0] != "function" {
			return schedTargetCall{intent: "target " + arn + " names no function"}
		}
		qualifier := ""
		if len(fn) > 2 {
			qualifier = fn[2]
		}
		call.req = schedLambdaInvoke(fn[1], qualifier, "Event", []byte(input))
	case "sqs":
		body := map[string]string{"QueueUrl": sqsQueueURL(region, account, resource), "MessageBody": input}
		if rec.Target.SqsParameters != nil && rec.Target.SqsParameters.MessageGroupID != "" {
			body["MessageGroupId"] = rec.Target.SqsParameters.MessageGroupID
		}
		call.req = schedJSONRequest("sqs", pluginRouting["sqs"].TargetPrefix, "SendMessage", schedMustJSON(body))
	case "sns":
		call.req = &AWSRequest{
			Service: "sns", Operation: "Publish", HTTPMethod: http.MethodPost, Path: "/",
			Headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
			Params:  map[string]string{"Action": "Publish", "Version": "2010-03-31", "TopicArn": arn, "Message": input},
		}
	case "states":
		call.req = schedJSONRequest("states", pluginRouting["states"].TargetPrefix, "StartExecution",
			schedMustJSON(map[string]string{"stateMachineArn": arn, "input": input}))
	case "events":
		entry := map[string]string{"EventBusName": arn, "Detail": input}
		if p := rec.Target.EventBridgeParameters; p != nil {
			entry["DetailType"] = p.DetailType
			entry["Source"] = p.Source
		}
		call.req = schedJSONRequest("eventbridge", pluginRouting["eventbridge"].TargetPrefix, "PutEvents",
			schedMustJSON(map[string]any{"Entries": []map[string]string{entry}}))
	default:
		return schedTargetCall{intent: "templated target service " + service + " is recorded, not dispatched"}
	}
	return call
}

// schedMustJSON marshals a value whose shape cannot fail to marshal.
func schedMustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// schedFailure is a failed target invocation as the dead-letter event reports it.
type schedFailure struct {
	// Code is the error code the target's service answered.
	Code string
	// Message is the error message the target's service answered.
	Message string
	// Status is the HTTP status of the answer.
	Status int
	// Retryable overrides the classification of Code and Status when set.
	Retryable *bool
}

// retryable reports whether Scheduler retries the failure under its RetryPolicy.
//
// `configuring-schedule-dlq.html` distinguishes "a retryable error, and not a permanent error" and
// publishes no list; substrate reads a throttle, a timeout and any 5xx as retryable and every other
// answer — a validation error, a missing resource, an access denial — as permanent.
func (f *schedFailure) retryable() bool {
	if f.Retryable != nil {
		return *f.Retryable
	}
	if f.Status >= http.StatusInternalServerError || f.Status == http.StatusTooManyRequests {
		return true
	}
	return strings.Contains(f.Code, "Throttl") || f.Code == "RequestLimitExceeded" ||
		f.Code == "TooManyRequestsException" || f.Code == "ServiceUnavailable"
}

// schedTargetFailure reads the failure a dispatched target answered, nil when it succeeded.
//
// Plugins report a client error either as an *AWSError or as a 4xx/5xx response with the error in
// the body, so both are read.
func schedTargetFailure(resp *AWSResponse, err error) *schedFailure {
	if err != nil {
		if awsErr, ok := err.(*AWSError); ok { //nolint:errorlint // plugins return *AWSError unwrapped
			return &schedFailure{Code: awsErr.Code, Message: awsErr.Message, Status: awsErr.HTTPStatus}
		}
		return &schedFailure{
			Code:    "AWS.Scheduler.InternalServerError",
			Message: "Unexpected error occurred while processing the request",
			Status:  http.StatusInternalServerError,
		}
	}
	if resp == nil || resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	f := &schedFailure{Status: resp.StatusCode}
	var body map[string]any
	if json.Unmarshal(resp.Body, &body) == nil {
		for _, k := range []string{"__type", "code", "Code"} {
			if s, ok := body[k].(string); ok && s != "" {
				f.Code = s[strings.LastIndex(s, "#")+1:]
				break
			}
		}
		for _, k := range []string{"message", "Message"} {
			if s, ok := body[k].(string); ok && s != "" {
				f.Message = s
				break
			}
		}
	} else {
		f.Code = extractXMLField(resp.Body, "Code")
		f.Message = extractXMLField(resp.Body, "Message")
	}
	if f.Code == "" {
		if h := resp.Headers["X-Amzn-Errortype"]; h != "" {
			f.Code, _, _ = strings.Cut(h, ":")
		} else {
			f.Code = http.StatusText(resp.StatusCode)
		}
	}
	return f
}
