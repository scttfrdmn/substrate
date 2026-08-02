package emulator

import (
	"encoding/json"
	"encoding/xml"
	"strings"
)

// awsErrorProtocol identifies how a service serializes an error response on the
// wire. AWS SDKs parse errors per-protocol, and each protocol carries the error
// code in a different place, so an emulator that picks the wrong one hands the
// caller an error it cannot match on. See #392.
type awsErrorProtocol int

const (
	// errProtoQueryXML is the AWS Query and REST-XML protocols. The code lives in
	// an <ErrorResponse><Error><Code> document.
	errProtoQueryXML awsErrorProtocol = iota

	// errProtoJSONRPC is the AWS JSON 1.0/1.1 RPC protocol. The code lives in the
	// body's "__type" member — botocore's BaseJSONParser reads "__type" and falls
	// back to the stringified HTTP status, never to "Code".
	errProtoJSONRPC

	// errProtoRESTJSON is the REST-JSON protocol. The code lives in the
	// x-amzn-errortype response header, which botocore's RestJSONParser prefers
	// over the body.
	errProtoRESTJSON

	// errProtoS3XML is S3's own REST-XML error document: a bare <Error> element
	// with an XML declaration and a <RequestId>, not the <ErrorResponse> wrapper
	// the Query protocol uses. S3's parser does not recover a code from the
	// wrapped form and falls back to the HTTP status, so an error raised outside
	// the S3 plugin — an injected fault, evaluated before any plugin runs — was
	// distinguishable from a genuine S3 error by the client, which is the one
	// property a fault injector must not have (#480).
	errProtoS3XML
)

// serviceErrorProtocols maps a service name (as reported by Plugin.Name) to the
// protocol its real AWS counterpart uses for errors. Services absent from this
// map fall back to the request's Content-Type; see errorProtocolFor.
//
// The classification follows each service's protocol trait, not its request
// Content-Type: REST-JSON services send "application/json" on the way in, which
// is indistinguishable from a plain JSON body, so the incoming header alone
// cannot decide this. Every entry below is the "protocol" field of the service's
// model in botocore/data/<service>/<version>/service-2.json, which is the same
// value that selects the SDK's error parser. Two classes read as one here:
//
//   - errProtoQueryXML covers the query, ec2 and rest-xml protocols. Their
//     on-the-wire error documents differ in the root element (ErrorResponse,
//     Response/Errors and Error respectively), but every one of their parsers
//     recovers the code from an <ErrorResponse><Error><Code> document, so a
//     single shape serves all three. S3 is the exception and has its own arm:
//     its parser does not read a code out of the wrapped form at all, so an
//     error leaving the pipeline before the plugin runs lost its code (#480).
//     errProtoS3XML delegates to s3ErrorResponseWith, which stays the single
//     source of truth for the byte shape.
//   - "monitoring" (CloudWatch) models as smithy-rpc-v2-cbor today, but query
//     remains in its supported protocol list and substrate's CloudWatch plugin
//     answers successes in query XML, so its errors match that.
var serviceErrorProtocols = map[string]awsErrorProtocol{
	// Query, ec2 and REST-XML.
	//
	// cloudfront and route53 are REST-XML like S3 but stay here deliberately:
	// their real error documents are <ErrorResponse>, so the Query shape is
	// already byte-correct for them. Only S3 returns a bare <Error>.
	"cloudformation":       errProtoQueryXML,
	"cloudfront":           errProtoQueryXML,
	"ec2":                  errProtoQueryXML,
	"elasticache":          errProtoQueryXML,
	"elasticloadbalancing": errProtoQueryXML,
	"iam":                  errProtoQueryXML,
	"monitoring":           errProtoQueryXML,
	"rds":                  errProtoQueryXML,
	"redshift":             errProtoQueryXML,
	"route53":              errProtoQueryXML,
	"sns":                  errProtoQueryXML,
	"sts":                  errProtoQueryXML,

	// S3's own REST-XML error document.
	"s3": errProtoS3XML,

	// JSON RPC (x-amz-json-1.0 / 1.1).
	"acm":              errProtoJSONRPC,
	"athena":           errProtoJSONRPC,
	"budgets":          errProtoJSONRPC,
	"ce":               errProtoJSONRPC,
	"cloudtrail":       errProtoJSONRPC,
	"codebuild":        errProtoJSONRPC,
	"codedeploy":       errProtoJSONRPC,
	"codepipeline":     errProtoJSONRPC,
	"cognito-identity": errProtoJSONRPC,
	"cognito-idp":      errProtoJSONRPC,
	"dynamodb":         errProtoJSONRPC,
	"ecr":              errProtoJSONRPC,
	"ecs":              errProtoJSONRPC,
	"eventbridge":      errProtoJSONRPC,
	"firehose":         errProtoJSONRPC,
	"fsx":              errProtoJSONRPC,
	"glue":             errProtoJSONRPC,
	"health":           errProtoJSONRPC,
	"kinesis":          errProtoJSONRPC,
	"kms":              errProtoJSONRPC,
	"logs":             errProtoJSONRPC,
	"organizations":    errProtoJSONRPC,
	"redshift-data":    errProtoJSONRPC,
	"sagemaker":        errProtoJSONRPC,
	"secretsmanager":   errProtoJSONRPC,
	"servicequotas":    errProtoJSONRPC,
	"sqs":              errProtoJSONRPC,
	"ssm":              errProtoJSONRPC,
	"states":           errProtoJSONRPC,
	"tagging":          errProtoJSONRPC,
	"timestream":       errProtoJSONRPC,
	"transfer":         errProtoJSONRPC,
	"wafv2":            errProtoJSONRPC,

	// REST-JSON.
	"apigateway":      errProtoRESTJSON,
	"apigatewayv2":    errProtoRESTJSON,
	"appsync":         errProtoRESTJSON,
	"backup":          errProtoRESTJSON,
	"batch":           errProtoRESTJSON,
	"bedrock-runtime": errProtoRESTJSON,
	"efs":             errProtoRESTJSON,
	"emrserverless":   errProtoRESTJSON,
	"execute-api":     errProtoRESTJSON,
	"lambda":          errProtoRESTJSON,
	"msk":             errProtoRESTJSON,
	"omics":           errProtoRESTJSON,
	"opensearch":      errProtoRESTJSON,
	"quicksight":      errProtoRESTJSON,
	"ram":             errProtoRESTJSON,
	"scheduler":       errProtoRESTJSON,
	"sesv2":           errProtoRESTJSON,
	"sso":             errProtoRESTJSON,
}

// errorProtocolFor returns the error protocol for a service. An unregistered
// service falls back to the request Content-Type: an x-amz-json body implies
// JSON RPC, anything else implies XML. That fallback preserves the behavior
// substrate had before #392 for any service not yet classified.
func errorProtocolFor(service, contentType string) awsErrorProtocol {
	if proto, ok := serviceErrorProtocols[service]; ok {
		return proto
	}
	if strings.HasPrefix(contentType, "application/x-amz-json") {
		return errProtoJSONRPC
	}
	return errProtoQueryXML
}

// marshalAWSError serializes err in the wire format the given protocol uses,
// returning the body, the Content-Type to send, and any extra response headers.
//
// The shapes are what the AWS SDKs actually parse:
//
//   - Query/REST-XML: <ErrorResponse><Error><Code>…</Code></Error></ErrorResponse>
//   - S3: a bare <Error><Code>…</Code><RequestId>…</RequestId></Error> document
//     with an XML declaration, built by the same function the S3 plugin uses so
//     an error the pipeline raises is byte-identical to one the plugin raises.
//   - JSON RPC: {"__type":"Code","message":"…"} — "__type" is the member
//     botocore reads; a body carrying only "Code" leaves the SDK to fall back to
//     the stringified HTTP status.
//   - REST-JSON: the x-amzn-errortype header carries the code, which botocore
//     prefers over the body.
//
// Both JSON forms also emit the lowercase "message" member the SDKs read, and
// the JSON-RPC form repeats the code in "Code" so existing callers that read
// that member keep working.
func marshalAWSError(e *AWSError, proto awsErrorProtocol, jsonContentType string) (body []byte, contentType string, headers map[string]string) {
	switch proto {
	case errProtoS3XML:
		resp := s3ErrorResponseWith(s3Error{Code: e.Code, Message: e.Message, Status: e.HTTPStatus})
		return resp.Body, resp.Headers["Content-Type"], nil

	case errProtoJSONRPC:
		ct := jsonContentType
		if !strings.HasPrefix(ct, "application/x-amz-json") {
			ct = "application/x-amz-json-1.1"
		}
		payload, err := json.Marshal(map[string]string{
			"__type":  e.Code,
			"message": e.Message,
			"Code":    e.Code,
			"Message": e.Message,
		})
		if err != nil {
			return nil, ct, nil
		}
		return payload, ct, map[string]string{"x-amzn-ErrorType": e.Code}

	case errProtoRESTJSON:
		payload, err := json.Marshal(map[string]string{
			"message": e.Message,
			"Message": e.Message,
			"Code":    e.Code,
		})
		if err != nil {
			return nil, "application/json", nil
		}
		return payload, "application/json", map[string]string{"x-amzn-ErrorType": e.Code}

	default:
		payload, err := xml.Marshal(struct {
			XMLName xml.Name `xml:"ErrorResponse"`
			Error   struct {
				Type    string `xml:"Type"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			} `xml:"Error"`
		}{
			Error: struct {
				Type    string `xml:"Type"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}{Type: "Sender", Code: e.Code, Message: e.Message},
		})
		if err != nil {
			return nil, "text/xml; charset=UTF-8", nil
		}
		return payload, "text/xml; charset=UTF-8", nil
	}
}
