package emulator

import "net/http"

// httpMethodOperations is the set of bare HTTP methods that stand in for an
// unresolved operation name. A request whose Operation is one of these was named
// by extractOperation's last-resort fallback, because the service supplied none
// of the three signals that carry an operation name (X-Amz-Target, an Action
// parameter, a Smithy RPC v2 path).
//
// This is the same set as s3HTTPMethods, kept separate because that one is part
// of parseS3Operation's idempotence contract and is consulted for a different
// question — whether the plugin was handed a verb — whereas this one gates the
// pipeline-wide resolution below.
var httpMethodOperations = map[string]bool{
	http.MethodGet:    true,
	http.MethodPut:    true,
	http.MethodPost:   true,
	http.MethodDelete: true,
	http.MethodHead:   true,
	http.MethodPatch:  true,
}

// requestMethod returns the HTTP verb of a request.
//
// AWSRequest.HTTPMethod carries it for anything that came off the wire. An
// in-process caller that builds an AWSRequest by hand sets only Operation, and
// by convention sets it to the verb — so fall back to it, but only when it holds
// a verb, never when it holds an already-resolved operation name.
func requestMethod(req *AWSRequest) string {
	if req.HTTPMethod != "" {
		return req.HTTPMethod
	}
	if httpMethodOperations[req.Operation] {
		return req.Operation
	}
	return ""
}

// operationResolvers maps a service name to the function that derives its
// semantic operation name from an AWSRequest.
//
// Every entry wraps a resolver the service's plugin already owns and already
// calls at the top of its HandleRequest. Wrapping rather than reimplementing is
// the point: there is one definition of "what operation is this", so the name
// the pipeline authorizes, meters and records cannot drift from the name the
// plugin routes on.
//
// The resolvers are pure functions of the method, path and params — the fields
// ParseAWSRequest has populated by the time it consults this table — which is
// what makes resolving there safe. See resolveOperationName.
var operationResolvers = map[string]func(req *AWSRequest) string{
	"account": func(req *AWSRequest) string {
		return parseAccountOperation(requestMethod(req), req.Path)
	},
	"s3": func(req *AWSRequest) string {
		_, _, op := parseS3Operation(req)
		return op
	},
	"lambda": func(req *AWSRequest) string {
		op, _, _ := parseLambdaOperation(requestMethod(req), req.Path)
		return op
	},
	"apigateway": func(req *AWSRequest) string {
		op, _ := parseAPIGatewayOperation(requestMethod(req), req.Path)
		return op
	},
	"apigatewayv2": func(req *AWSRequest) string {
		op, _ := parseAPIGatewayV2Operation(requestMethod(req), req.Path)
		return op
	},
	"appsync": func(req *AWSRequest) string {
		op, _, _, _ := parseAppSyncOperation(requestMethod(req), req.Path)
		return op
	},
	"backup": func(req *AWSRequest) string {
		op, _, _, _ := parseBackupOperation(requestMethod(req), req.Path)
		return op
	},
	"batch": func(req *AWSRequest) string {
		op, _ := parseBatchOperation(requestMethod(req), req.Path)
		return op
	},
	"bedrock-runtime": func(req *AWSRequest) string {
		op, _, _ := parseBedrockRuntimeOperation(requestMethod(req), req.Path)
		return op
	},
	"cloudfront": func(req *AWSRequest) string {
		op, _ := parseCloudFrontOperation(requestMethod(req), req.Path, req.Params)
		return op
	},
	"efs": func(req *AWSRequest) string {
		op, _ := parseEFSOperation(requestMethod(req), req.Path)
		return op
	},
	"emrserverless": func(req *AWSRequest) string {
		op, _, _ := parseEMRServerlessOperation(requestMethod(req), req.Path)
		return op
	},
	"msk": func(req *AWSRequest) string {
		op, _ := parseKafkaOperation(requestMethod(req), req.Path)
		return op
	},
	"omics": func(req *AWSRequest) string {
		op, _ := parseOmicsOperation(requestMethod(req), req.Path)
		return op
	},
	"quicksight": func(req *AWSRequest) string {
		op, _, _, _ := parseQuickSightOperation(requestMethod(req), req.Path)
		return op
	},
	"ram": func(req *AWSRequest) string {
		return parseRAMOperation(requestMethod(req), req.Path)
	},
	"route53": func(req *AWSRequest) string {
		op, _ := parseRoute53Operation(requestMethod(req), req.Path)
		return op
	},
	"scheduler": func(req *AWSRequest) string {
		return parseSchedulerOperation(requestMethod(req), req.Path)
	},
	"sesv2": func(req *AWSRequest) string {
		op, _ := parseSESv2Operation(requestMethod(req), req.Path)
		return op
	},
}

// resolveOperationName replaces a bare HTTP method in req.Operation with the
// service's semantic operation name.
//
// A REST-JSON or REST-XML service carries its operation in the shape of its URL
// and nothing else, so extractOperation falls through to the HTTP method and the
// request enters the pipeline named "POST". Authorization, fault injection, cost
// and consistency all read req.Operation before any plugin runs, so each of them
// saw the verb: a policy granting lambda:InvokeFunction was refused because the
// action evaluated was lambda:POST (#572), and a fault rule could not name an
// operation (#480, fixed for S3 alone at the time).
//
// Resolving here rather than in each plugin is what keeps the pipeline and the
// plugin in agreement. It is safe to call more than once: a resolver is only
// consulted when the operation is still a bare verb, and every resolver returns
// an already-resolved name unchanged.
//
// A resolver that cannot name the request leaves the verb in place, because a
// verb is a more honest answer than a wrong operation name — an unroutable path
// must reach the plugin and be refused there, not be authorized as something
// else. A resolver signals that by returning either "" or the verb it was handed,
// and both are treated the same way: leave req alone.
func resolveOperationName(req *AWSRequest) {
	if req == nil || !httpMethodOperations[req.Operation] {
		return
	}
	resolve := operationResolvers[req.Service]
	if resolve == nil {
		return
	}
	op := resolve(req)
	if op == "" || httpMethodOperations[op] {
		return
	}
	// Preserve the verb before overwriting it. An in-process caller — every
	// CloudFormation resource call — sets only Operation, so the verb lives there
	// and resolving would consume the one signal the plugin's own resolver needs:
	// it would then see no method at all and fail to route a request that used to
	// work.
	if req.HTTPMethod == "" {
		req.HTTPMethod = req.Operation
	}
	req.Operation = op
}
