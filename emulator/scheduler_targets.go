package emulator

import (
	"bytes"
	"encoding/json"
	"errors"
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
//     operation's Action and the service's API Version. That is botocore's query serializer's
//     generic rule. Where an operation's input shape departs from it — a map
//     (`Parent.entry.N.key`/`.value`, with the model's key and value names), a flattened or
//     renamed list, a renamed member — [schedQueryModel] says so. That table is generated from
//     the botocore models by cmd/gen-scheduler-query-model, because a JSON object cannot say
//     whether it is a structure or a map, and substrate carries no shapes of its own.
//   - EC2 (the ec2 protocol): the same, with `Parent.N` for a list element and the ec2 protocol's
//     member names (queryName, or the capitalized locationName: `TagSpecifications` is sent as
//     `TagSpecification`), also from [schedQueryModel].
//   - REST: only Lambda's Invoke is bound (FunctionName and Qualifier to the URI, InvocationType to
//     its header, Payload to the body), because a REST operation's URI and header bindings live in
//     the service model too. A qualified FunctionName's version or alias is the Qualifier, and one
//     that contradicts an explicit Qualifier is refused as Lambda refuses it. Any other REST action
//     is recorded as intent and not dispatched.
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
		params, failure := schedQueryParams(input, sdkID, op, svc.plugin == "ec2")
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
		name, derived := lambdaParseFunctionRef(in.FunctionName)
		qualifier := in.Qualifier
		switch {
		case derived != "" && qualifier != "" && derived != qualifier:
			// The refusal the guide's troubleshooting section shows Lambda answering for
			// {"FunctionName": "MyFunction:2", "Qualifier": "1"}.
			call.invalid = &schedFailure{
				Code:    "InvalidParameterValueException",
				Message: "The derived qualifier from the function name does not match the specified qualifier.",
				Status:  http.StatusBadRequest,
			}
			return call
		case qualifier == "":
			qualifier = derived
		}
		if acct, region, ok := lambdaARNAccountRegion(in.FunctionName); ok && (acct != rec.AccountID || region != rec.Region) {
			return schedTargetCall{intent: schedForeignFunctionIntent(in.FunctionName)}
		}
		call.req = schedLambdaInvoke(name, qualifier, in.InvocationType, payload)
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

// lambdaParseFunctionRef splits a FunctionName — `name`, `name:qualifier`, a partial ARN
// (`account:function:name[:qualifier]`) or a full ARN — into the name the invoke path carries and
// the version or alias it names, empty when it names none (`API_Invoke`'s FunctionName forms).
func lambdaParseFunctionRef(ref string) (name, qualifier string) {
	parts := strings.Split(ref, ":")
	for i, p := range parts {
		if p == "function" && i+1 < len(parts) {
			if i+2 < len(parts) {
				return parts[i+1], parts[i+2]
			}
			return parts[i+1], ""
		}
	}
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return ref, ""
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

// schedQueryHint is one input path's departure from the generic Query rule, read from the
// botocore service model by cmd/gen-scheduler-query-model into [schedQueryModel].
//
// A path is the input's JSON member names joined by ".", with "[]" for a list element and "{}"
// for a map value: `Attributes`, `TagSpecifications[].Tags`.
type schedQueryHint struct {
	// Name is the member's wire name when it differs from its JSON name (awsQuery's locationName;
	// the ec2 protocol's queryName or capitalized locationName).
	Name string
	// List is a list's member name when it is not "member".
	List string
	// Flat marks a flattened list or map, whose elements carry no "member" or "entry" segment.
	Flat bool
	// Map marks a map, which serializes as `Parent.entry.N.Key` and `Parent.entry.N.Value`.
	Map bool
	// Key and Value are a map's key and value names.
	Key, Value string
}

// schedQueryParams flattens a JSON Input into the Query form parameters of sdkID's operation op.
// ec2 selects the ec2 protocol's rules.
//
// The generic rule — a structure member is `Parent.Member`, a list element `Parent.member.N`
// (`Parent.N` on ec2) — is botocore's query serializer's. Where the operation's input shape departs
// from it (a map, a flattened or renamed list, a renamed member), [schedQueryModel] says so,
// because a JSON object cannot say whether it is a structure or a map. An operation the table does
// not hold has no departures in the model, or is not in it at all, and gets the generic rule.
func schedQueryParams(input, sdkID, op string, ec2 bool) (map[string]string, *schedFailure) {
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
	f := schedQueryFlattener{out: map[string]string{}, hints: schedQueryModel[sdkID][op], ec2: ec2}
	f.value("", "", obj)
	return f.out, nil
}

// schedQueryFlattener writes one Input's form parameters.
type schedQueryFlattener struct {
	out   map[string]string
	hints map[string]schedQueryHint
	ec2   bool
}

func schedJoin(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

func schedSortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// value writes v, found at the model path path, under the wire prefix prefix.
func (f *schedQueryFlattener) value(prefix, path string, v any) {
	h := f.hints[path]
	switch t := v.(type) {
	case map[string]any:
		if h.Map {
			base := prefix + ".entry"
			if h.Flat {
				base = prefix
			}
			for i, k := range schedSortedKeys(t) {
				entry := base + "." + strconv.Itoa(i+1)
				f.out[entry+"."+h.Key] = k
				f.value(entry+"."+h.Value, path+"{}", t[k])
			}
			return
		}
		for _, k := range schedSortedKeys(t) {
			child := schedJoin(path, k)
			wire := k
			if ch := f.hints[child]; ch.Name != "" {
				wire = ch.Name
			}
			f.value(schedJoin(prefix, wire), child, t[k])
		}
	case []any:
		if f.ec2 {
			for i, e := range t {
				f.value(prefix+"."+strconv.Itoa(i+1), path+"[]", e)
			}
			return
		}
		if len(t) == 0 {
			// An empty list is sent as the bare member name, which is how botocore's query
			// serializer distinguishes "clear this list" from "leave it unchanged".
			f.out[prefix] = ""
			return
		}
		listPrefix := prefix + "." + schedOr(h.List, "member")
		if h.Flat {
			listPrefix = prefix
			if h.List != "" {
				// A flattened list with a named member replaces the last segment with that name.
				if i := strings.LastIndex(prefix, "."); i >= 0 {
					listPrefix = prefix[:i+1] + h.List
				} else {
					listPrefix = h.List
				}
			}
		}
		for i, e := range t {
			f.value(listPrefix+"."+strconv.Itoa(i+1), path+"[]", e)
		}
	case json.Number:
		f.out[prefix] = t.String()
	case bool:
		f.out[prefix] = strconv.FormatBool(t)
	case string:
		f.out[prefix] = t
	case nil:
	}
}

func schedOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
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
		if account != rec.AccountID || region != rec.Region {
			return schedTargetCall{intent: schedForeignFunctionIntent(arn)}
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

// schedulerCheckLambdaTarget refuses a schedule whose target invokes a Lambda function outside the
// schedule's account and Region: a templated target naming a function ARN, or the universal
// `aws-sdk:lambda:invoke` target naming one in its Input's FunctionName. The refusal is
// [lambdaCheckFunctionScope]'s. Invoking another account's function would take its resource policy
// to grant the schedule's role lambda:InvokeFunction, and substrate does not evaluate a function's
// resource policy for a cross-account caller (#1710). An Input that does not decode is left to the
// dispatch, which reports it.
func schedulerCheckLambdaTarget(ctx *RequestContext, t *schedulerTargetInput) *AWSError {
	if t == nil {
		return nil
	}
	ref := ""
	if parts := strings.Split(t.Arn, ":"); len(parts) >= 3 && parts[0] == "arn" && parts[2] == "lambda" {
		ref = t.Arn
	} else if _, spec, ok := strings.Cut(t.Arn, schedulerUniversalPrefix); ok {
		if sdkID, action, _ := strings.Cut(spec, ":"); sdkID == "lambda" && strings.EqualFold(action, "invoke") {
			var in struct {
				FunctionName string `json:"FunctionName"`
			}
			if json.Unmarshal([]byte(t.Input), &in) == nil {
				ref = in.FunctionName
			}
		}
	}
	if ref == "" {
		return nil
	}
	return lambdaCheckFunctionScope(ctx, ref, "lambda:InvokeFunction")
}

// schedForeignFunctionIntent is the recorded intent of a dispatch to a function outside the
// schedule's account and Region, which a schedule stored before CreateSchedule refused one can
// still name. It is not invoked; see [schedulerCheckLambdaTarget].
func schedForeignFunctionIntent(ref string) string {
	return "the function " + ref + " is outside the schedule's account and Region; a cross-account invoke " +
		"through the function's resource policy is not modeled"
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

// throttled reports whether the failure is the target throttling the call — HTTP 429, or a code
// naming a throttle — which is what TargetErrorThrottledCount counts. It reads the code and status
// alone, so a seed's retryable override does not make a validation error a throttle.
func (f *schedFailure) throttled() bool {
	return f.Status == http.StatusTooManyRequests || strings.Contains(f.Code, "Throttl") ||
		f.Code == "RequestLimitExceeded" || f.Code == "TooManyRequestsException"
}

// schedTargetFailure reads the failure a dispatched target answered, nil when it succeeded.
//
// Plugins report a client error either as an *AWSError or as a 4xx/5xx response with the error in
// the body, so both are read.
func schedTargetFailure(resp *AWSResponse, err error) *schedFailure {
	if err != nil {
		var awsErr *AWSError
		if errors.As(err, &awsErr) {
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
