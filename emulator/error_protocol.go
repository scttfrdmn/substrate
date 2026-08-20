package emulator

import (
	"encoding/json"
	"encoding/xml"
	"strings"
)

// substrateRequestID is the request id every error document carries. It is a fixed
// string rather than generateRequestID(), which derives from time.Now().UnixNano():
// an error body has to be byte-identical across two replays of one recorded run, and
// a caller diffing responses must not see a field that changes on its own.
const substrateRequestID = "SUBSTRATE"

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

	// errProtoEC2XML is the "ec2" protocol's error document:
	// <Response><Errors><Error><Code>…</Code><Message>…</Message></Error></Errors>
	// <RequestID>…</RequestID></Response>. Two details separate it from the Query
	// shape and both matter. The <Errors> wrapper is plural — the SDK's
	// ec2query.ErrorComponents reads the code at the XPath "Errors>Error>Code",
	// which finds nothing in a Query document, and the EC2 deserializer then keeps
	// its "UnknownError" default. And the request id is spelled <RequestID> with a
	// capital D where the Query protocol writes <RequestId>.
	//
	// EC2 was classified as Query until #591 because botocore tolerates both:
	// EC2QueryParser._get_error_root falls back to the document root when <Errors>
	// is absent, so the AWS CLI read the code correctly out of the wrong shape and
	// every CLI-driven test passed. Only an SDK caller saw UnknownError, for
	// injected and organic errors alike.
	errProtoEC2XML
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
//   - errProtoQueryXML covers the query and rest-xml protocols, whose parsers
//     both recover the code from an <ErrorResponse><Error><Code> document. The
//     other two XML protocols each need their own arm, because their parsers read
//     a different XPath and recover nothing from the wrapped form: S3's bare
//     <Error> document (errProtoS3XML, #480, delegating to s3ErrorResponseWith so
//     that function stays the single source of truth for those bytes) and the
//     "ec2" protocol's plural <Response><Errors><Error> (errProtoEC2XML, #591).
//     In both cases an error left the pipeline with a code the SDK could not
//     read, which is the one property a fault injector must not have — and for
//     EC2 that hit organic errors too, since every writeError call site shares
//     this serializer.
//
//     Neither was caught by substrate's own coverage because botocore is lenient
//     where the SDKs are strict: it recovers the code from the wrapped form in
//     both cases, so the AWS CLI reported the right error while an SDK caller saw
//     UnknownError or a bare HTTP status. A real-SDK test is the only kind that
//     can hold these arms honest; see test/e2e.
//
//   - "monitoring" (CloudWatch) models as smithy-rpc-v2-cbor today, but query
//     remains in its supported protocol list and substrate's CloudWatch plugin
//     answers successes in query XML, so its errors match that.
var serviceErrorProtocols = map[string]awsErrorProtocol{
	// Query and REST-XML.
	//
	// cloudfront and route53 are REST-XML like S3 but stay here deliberately:
	// their real error documents are <ErrorResponse>, so the Query shape is
	// already byte-correct for them. Only S3 returns a bare <Error>.
	"cloudformation":       errProtoQueryXML,
	"cloudfront":           errProtoQueryXML,
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

	// The "ec2" protocol's own error document. ec2 is the only service that uses
	// that protocol — checked against the "protocol" field of every service model
	// in botocore — so this arm has exactly one member and is expected to keep it.
	"ec2": errProtoEC2XML,

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
	"config":           errProtoJSONRPC,
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
	// pricing is "protocol": "json" with jsonVersion 1.1 and the targetPrefix
	// AWSPriceListService, and its model declares an AccessDeniedException shape
	// carrying "exception": true — the suffixed spelling this arm produces. That
	// shape is declared on GetPriceListFileUrl and ListPriceLists but *not* on
	// GetProducts, the operation #653 reports, so for GetProducts neither code is
	// modeled and the protocol rule alone decides it; the shape corroborates the
	// spelling rather than supplying it.
	"pricing":        errProtoJSONRPC,
	"redshift-data":  errProtoJSONRPC,
	"sagemaker":      errProtoJSONRPC,
	"secretsmanager": errProtoJSONRPC,
	"servicequotas":  errProtoJSONRPC,
	"sqs":            errProtoJSONRPC,
	"ssm":            errProtoJSONRPC,
	"states":         errProtoJSONRPC,
	"tagging":        errProtoJSONRPC,
	"timestream":     errProtoJSONRPC,
	"transfer":       errProtoJSONRPC,
	"wafv2":          errProtoJSONRPC,

	// REST-JSON.
	"account":         errProtoRESTJSON,
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

// accessDeniedCodeFor returns the error code AWS uses to refuse an authorization
// check on the given service: "AccessDenied" for the XML protocols, and
// "AccessDeniedException" for the JSON ones.
//
// The suffix tracks the wire protocol rather than the individual service (#595).
// Audited across every botocore model, deduplicated by service (botocore ships
// many versioned models per service, so counting files inflates the XML side
// nineteen-fold) and restricted to shapes carrying "exception": true:
//
//	family                                  bare AccessDenied  AccessDeniedException
//	XML   (query 17, rest-xml 4, ec2 1)     1 (cloudfront)     0
//	JSON  (json 149, rest-json 242, cbor 2) 0                  233
//
// The two halves rest on very different evidence. On the JSON side all 233
// services that model the shape use the suffixed form and none use the bare one,
// so that arm is settled by the models. On the XML side only CloudFront models
// the shape at all, so n=1.
//
// What decides the XML arm instead is that **no query-protocol service models an
// access-denied shape** — all 17, including STS, IAM, CloudFormation and SNS —
// and neither do S3 (rest-xml) or EC2 (ec2), which declare no exception shapes
// whatsoever. Every service where this code is observable in substrate is in that
// set, so the value is *observed AWS behavior, not modeled*: AWS's own CLI
// output for a refused sts:AssumeRole reports "AccessDenied" (quoted in #595),
// and s3ErrorResponseWith's callers already rely on the same for S3 (see
// s3_publicaccess.go). CloudFront and the unanimous JSON column corroborate the
// split; they are not its basis.
//
// contentType is only consulted for services absent from serviceErrorProtocols,
// via errorProtocolFor's fallback — callers that do not have it may pass "".
func accessDeniedCodeFor(service, contentType string) string {
	switch errorProtocolFor(service, contentType) {
	case errProtoJSONRPC, errProtoRESTJSON:
		return "AccessDeniedException"
	case errProtoQueryXML, errProtoS3XML, errProtoEC2XML:
		return "AccessDenied"
	}
	// Unreachable: errorProtocolFor returns one of the constants above. Defaulting
	// to the suffixed form keeps a hypothetical new protocol on the value the
	// overwhelming majority of AWS services use.
	return "AccessDeniedException"
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
//   - ec2: <Response><Errors><Error><Code>…</Code></Error></Errors>
//     <RequestID>…</RequestID></Response> — plural <Errors>, and <RequestID> with
//     a capital D. See errProtoEC2XML for why neither detail is cosmetic.
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

	case errProtoEC2XML:
		payload, err := xml.Marshal(struct {
			XMLName xml.Name `xml:"Response"`
			Errors  struct {
				Error struct {
					Code    string `xml:"Code"`
					Message string `xml:"Message"`
				} `xml:"Error"`
			} `xml:"Errors"`
			RequestID string `xml:"RequestID"`
		}{
			Errors: struct {
				Error struct {
					Code    string `xml:"Code"`
					Message string `xml:"Message"`
				} `xml:"Error"`
			}{
				Error: struct {
					Code    string `xml:"Code"`
					Message string `xml:"Message"`
				}{Code: e.Code, Message: e.Message},
			},
			RequestID: substrateRequestID,
		})
		if err != nil {
			return nil, "text/xml; charset=UTF-8", nil
		}
		return payload, "text/xml; charset=UTF-8", nil

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
