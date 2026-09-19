package emulator

import "strings"

// Lambda's URIs are dated per operation, and substrate routed one date (#1142).
//
// `lambda.TagResource` against a function substrate had just created answered:
//
//	operation error Lambda: TagResource, https response error StatusCode: 404, RequestID: ,
//	api error UnknownOperationException: The action POST
//	/2017-03-31/tags/arn:aws:lambda:us-east-1:123456789012:function:… is not recognized.
//
// Everything behind the route was there — `HandleRequest` has a `case "TagResource"`,
// [LambdaPlugin.tagResource] merges the keys and saves the function, and the parser has a matching
// `/tags/{resourceArn}` arm. The parser reached it by trimming the literal prefix `/2015-03-31`, so a
// path under any other date kept its version segment, matched no arm, and fell through to
// [unknownRouteError]. Three operations were unreachable in a plugin that implements them.
//
// # One service, five API version dates
//
// Unlike a query-protocol service, whose `Version` parameter is one value for the whole API, a REST
// service dates each operation's URI at the version that operation was introduced, and Lambda has
// never renumbered. The dates substrate routes, each read from the operation's own published Request
// Syntax:
//
//   - `2014-11-13` — `InvokeAsync`, which AWS marks deprecated and still serializes;
//   - `2015-03-31` — the function CRUD, `Invoke`, the resource policy, and event source mappings;
//   - `2017-03-31` — `TagResource`, `UntagResource` and `ListTags`;
//   - `2019-09-25` — `PutFunctionEventInvokeConfig`.
//
// `aws-sdk-go-v2/service/lambda`'s serializer is the mechanical check on that list: its
// `httpbinding.SplitURI` calls enumerate every Lambda URI, and the 50 distinct ones carry 17
// different dates. Of the paths substrate routes, three families were on the wrong one — the tags
// family, `invoke-async` and `event-invoke-config` — and the rest were right.
//
// # Matching the date rather than stripping it
//
// The version is split off and compared against the one the operation publishes, so a request under
// an undocumented date answers the 404 the plugin already has rather than being accepted. The
// permissive alternative — strip any leading date segment, or match `/tags/` anywhere — would route
// `/2015-03-31/tags/…`, a path no AWS SDK emits and AWS itself does not serve, and substrate would
// then be the only implementation that accepts it. That matters because the whole point of the
// emulator is that code which passes here passes against AWS: accepting a URI AWS rejects hides the
// defect rather than reporting it, and a consumer would find out at deployment.
//
// The cost of matching is that substrate's own tests were written against the parser rather than
// against the API, and nine of them posted to the wrong date. They move with this change, which is
// the evidence that the routes were never exercised as an SDK drives them — and
// `lambda_warm_container_test.go` already used `/2017-03-31/tags/…`, asserted nothing about the
// status, and so passed against a 404.
//
// # Why one resolver decides more than routing
//
// [parseLambdaOperation] is also Lambda's entry in [operationNameResolvers], which names the
// operation for authorization, metering and fault injection alike. So `lambda:TagResource` resolved
// to `"Unknown"` for policy evaluation too: an IAM policy naming the action could neither allow nor
// deny it, and a fault seeded on it could not fire. Fixing the parser fixes all four at once, which
// is why [lambdaNameFromPath] moves here with it — the resource half of the same evaluation.

// The Lambda API version dates substrate routes. Each is the date in the operation's own published
// Request Syntax, and they are named rather than inlined because the same date appears in the parser
// and in the resource-ARN resolver.
const (
	// lambdaInvokeAsyncAPIVersion dates InvokeAsync, Lambda's oldest routed operation.
	lambdaInvokeAsyncAPIVersion = "2014-11-13"

	// lambdaFunctionsAPIVersion dates the function CRUD, Invoke, the resource policy and event
	// source mappings — every routed operation except the four below.
	lambdaFunctionsAPIVersion = "2015-03-31"

	// lambdaTagsAPIVersion dates TagResource, UntagResource and ListTags.
	lambdaTagsAPIVersion = "2017-03-31"

	// lambdaEventInvokeAPIVersion dates PutFunctionEventInvokeConfig.
	lambdaEventInvokeAPIVersion = "2019-09-25"
)

// lambdaUnknownOperation is what the parser reports for a path no routed operation publishes. The
// dispatch answers it with [unknownRouteError], and [operationNameResolvers] reports it as the
// operation name.
const lambdaUnknownOperation = "Unknown"

// lambdaSplitAPIVersion splits a Lambda REST path into its leading API version segment and the
// remainder, which keeps its leading slash.
//
// Reports false for a path with no version segment at all, which no SDK emits: every Lambda URI in
// the service model begins with one.
func lambdaSplitAPIVersion(path string) (version, rest string, ok bool) {
	if !strings.HasPrefix(path, "/") {
		return "", "", false
	}
	trimmed := path[1:]
	idx := strings.Index(trimmed, "/")
	if idx <= 0 {
		return "", "", false
	}
	return trimmed[:idx], trimmed[idx:], true
}

// parseLambdaOperation parses a Lambda REST path to extract the operation name, function name, and
// optional sub-resource (e.g., statement ID for RemovePermission).
//
// The path's API version has to be the one the operation publishes; see this file's comment for why
// the version is matched rather than stripped.
func parseLambdaOperation(method, path string) (op, name, subResource string) {
	version, p, ok := lambdaSplitAPIVersion(path)
	if !ok {
		return lambdaUnknownOperation, "", ""
	}

	switch {
	case version == lambdaTagsAPIVersion && strings.HasPrefix(p, "/tags/"):
		return lambdaTagOperation(method, p[len("/tags/"):])
	case version == lambdaFunctionsAPIVersion && strings.HasPrefix(p, "/event-source-mappings"):
		return lambdaEventSourceMappingOperation(method, p)
	case strings.HasPrefix(p, "/functions"):
		return lambdaFunctionOperation(version, method, p)
	}
	return lambdaUnknownOperation, "", ""
}

// lambdaTagOperation resolves the three operations under `/2017-03-31/tags/{Resource}`. The resource
// is an ARN, so it is returned whole rather than split on the slashes it does not contain.
func lambdaTagOperation(method, resource string) (op, name, subResource string) {
	switch method {
	case "POST":
		return "TagResource", resource, ""
	case "GET":
		return "ListTags", resource, ""
	case "DELETE":
		return "UntagResource", resource, ""
	}
	return lambdaUnknownOperation, "", ""
}

// lambdaEventSourceMappingOperation resolves the five operations under
// `/2015-03-31/event-source-mappings[/{UUID}]`.
func lambdaEventSourceMappingOperation(method, p string) (op, name, subResource string) {
	rest := strings.TrimPrefix(p, "/event-source-mappings")
	rest = strings.TrimPrefix(rest, "/")
	if rest == "" {
		switch method {
		case "POST":
			return "CreateEventSourceMapping", "", ""
		case "GET":
			return "ListEventSourceMappings", "", ""
		}
		return lambdaUnknownOperation, "", ""
	}
	switch method {
	case "GET":
		return "GetEventSourceMapping", rest, ""
	case "PUT":
		return "UpdateEventSourceMapping", rest, ""
	case "DELETE":
		return "DeleteEventSourceMapping", rest, ""
	}
	return lambdaUnknownOperation, "", ""
}

// lambdaFunctionOperation resolves everything under a `/functions` path, each sub-resource against
// the API version its own page publishes: `invoke-async` is `2014-11-13`,
// `event-invoke-config` is `2019-09-25`, and the rest are `2015-03-31`.
func lambdaFunctionOperation(version, method, p string) (op, name, subResource string) {
	// /functions
	if p == "/functions" || p == "/functions/" {
		if version != lambdaFunctionsAPIVersion {
			return lambdaUnknownOperation, "", ""
		}
		if method == "POST" {
			return "CreateFunction", "", ""
		}
		return "ListFunctions", "", ""
	}

	if !strings.HasPrefix(p, "/functions/") {
		return lambdaUnknownOperation, "", ""
	}
	rest := p[len("/functions/"):]

	// /functions/{name}
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		if version != lambdaFunctionsAPIVersion {
			return lambdaUnknownOperation, "", ""
		}
		if method == "DELETE" {
			return "DeleteFunction", rest, ""
		}
		return "GetFunction", rest, ""
	}

	// /functions/{name}/{sub}
	fn := rest[:slashIdx]
	sub := rest[slashIdx+1:]

	switch {
	case sub == "invoke-async" && method == "POST":
		if version != lambdaInvokeAsyncAPIVersion {
			return lambdaUnknownOperation, "", ""
		}
		return "InvokeAsync", fn, ""
	case sub == "event-invoke-config" && method == "PUT":
		if version != lambdaEventInvokeAPIVersion {
			return lambdaUnknownOperation, "", ""
		}
		return "PutFunctionEventInvokeConfig", fn, ""
	}

	if version != lambdaFunctionsAPIVersion {
		return lambdaUnknownOperation, "", ""
	}
	switch {
	case sub == "code" && method == "PUT":
		return "UpdateFunctionCode", fn, ""
	case sub == "configuration" && method == "PUT":
		return "UpdateFunctionConfiguration", fn, ""
	case sub == "invocations" && method == "POST":
		return "Invoke", fn, ""
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
			stmtID := ""
			if idx := strings.Index(sub, "/"); idx >= 0 {
				stmtID = sub[idx+1:]
			}
			return "RemovePermission", fn, stmtID
		}
	}
	return lambdaUnknownOperation, "", ""
}

// lambdaNameFromPath extracts the function name from a Lambda REST path, for the authorization
// resource ARN.
//
// It resolves the same paths the parser routes rather than the single date it used to assume: an
// `invoke-async` or `event-invoke-config` request names its function under a different API version,
// and reading `/2015-03-31/functions/` literally reported no name for either — so a policy scoped to
// one function ARN could not match a request that named it. A tags request names its resource as a
// whole ARN, which [lambdaAuthzResourceARN] uses directly.
func lambdaNameFromPath(path string) string {
	version, p, ok := lambdaSplitAPIVersion(path)
	if !ok {
		return ""
	}
	switch version {
	case lambdaFunctionsAPIVersion, lambdaInvokeAsyncAPIVersion, lambdaEventInvokeAPIVersion:
	default:
		return ""
	}
	rest, found := strings.CutPrefix(p, "/functions/")
	if !found {
		return ""
	}
	if idx := strings.Index(rest, "/"); idx >= 0 {
		return rest[:idx]
	}
	return rest
}

// lambdaAuthzResourceARN returns the resource ARN a Lambda request is authorized against.
//
// A tags request names its resource as an ARN in the path, and that ARN is the answer: it carries
// the account and Region of the function the caller named, which is what a policy scoped to one
// function is written against. Reassembling one from the caller's own account and Region — what a
// function-path request has to do, because it names only a function name — would silently retarget a
// cross-account ARN at the caller's own function of that name.
func lambdaAuthzResourceARN(path, region, accountID string) string {
	if version, p, ok := lambdaSplitAPIVersion(path); ok &&
		version == lambdaTagsAPIVersion && strings.HasPrefix(p, "/tags/") {
		if arn := p[len("/tags/"):]; arn != "" {
			return arn
		}
		return "*"
	}
	if name := lambdaNameFromPath(path); name != "" {
		return "arn:aws:lambda:" + region + ":" + accountID + ":function:" + name
	}
	return "*"
}
