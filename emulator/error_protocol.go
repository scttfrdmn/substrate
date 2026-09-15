package emulator

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"
)

// substrateRequestID is the request id every error document carries. It is a fixed
// string rather than generateRequestID(), which derives from time.Now().UnixNano():
// an error body has to be byte-identical across two replays of one recorded run, and
// a caller diffing responses must not see a field that changes on its own.
//
// A success body cannot take this route, because a caller correlating a response
// with a log line needs the id to identify the request rather than the emulator.
// The same rule is met there by recording the value instead: [Event.RequestID]
// carries the id the request was served under and a replay is dispatched with it,
// so the bytes reproduce (#866). This constant is the fixed-value half of one rule,
// not a separate decision.
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

	// errProtoRPCV2CBOR is Smithy's RPC v2 CBOR protocol. The error is a CBOR map
	// carrying the operation's normal error members plus "__type", whose value is
	// the absolute shape ID; the specification is explicit that a "Code" or "code"
	// member MUST NOT be used to distinguish the error and that X-Amzn-ErrorType
	// SHOULD NOT be sent, so this arm emits neither. Unlike every other arm it needs
	// to know the *service* as well as the protocol, because the shape ID it must
	// name is not derivable from the Query code substrate's plugins raise; see
	// errorShapeFor.
	errProtoRPCV2CBOR
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
//   - "monitoring" (CloudWatch) is the one entry this map does not get the final say
//     over. Its model declares awsQuery, awsJson1_0 and rpcv2Cbor simultaneously, so
//     the *request* decides and the value here is only the answer for a request that
//     identifies itself as none of the others — see multiProtocolServices and
//     errorProtocolForRequest (#757).
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
	// sso was classified REST-JSON, which is what the *sso* service (the OIDC token
	// and account-list API) uses — but substrate's plugin does not emulate that
	// service. It emulates sso-admin: its routing declares the target prefix
	// SWBExternalService and it answers X-Amz-Target-dispatched requests, and
	// sso-admin's model is "protocol": "json" with jsonVersion 1.1. So its errors
	// belong in this arm, where the code travels in the body's "__type" member that
	// botocore's BaseJSONParser reads, rather than in the x-amzn-errortype header
	// RestJSONParser prefers (#758).
	"sso":        errProtoJSONRPC,
	"sqs":        errProtoJSONRPC,
	"ssm":        errProtoJSONRPC,
	"states":     errProtoJSONRPC,
	"tagging":    errProtoJSONRPC,
	"timestream": errProtoJSONRPC,
	"transfer":   errProtoJSONRPC,
	"wafv2":      errProtoJSONRPC,

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
}

// multiProtocolServices names the services whose model declares more than one wire
// protocol, so the incoming request rather than the service decides how its errors are
// shaped. See errorProtocolForRequest.
//
// The set is deliberately small, and enumerated rather than sniffed. Substrate stopped
// classifying by Content-Type in #392 for a good reason: a REST-JSON service sends
// "application/json" on the way in, which is indistinguishable from a plain JSON body,
// so letting the request speak for a single-protocol service would reclassify Lambda's
// REST-JSON errors as Query and undo that fix. A service earns a place here only when
// its model really does declare several protocols and substrate really does serve them.
var multiProtocolServices = map[string]bool{
	// CloudWatch carries aws.protocols#awsQuery, aws.protocols#awsJson1_0,
	// smithy.protocols#rpcv2Cbor and aws.protocols#awsQueryCompatible on one service
	// shape, and its clients disagree: aws-sdk-go-v2 sends CBOR, the AWS CLI and boto3
	// send JSON 1.0, and a hand-rolled client sends a Query form. #785, #757.
	"monitoring": true,
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

// errorProtocolForRequest returns the error protocol for a service, letting the request
// decide when — and only when — the service is one whose model declares several (#757).
//
// This is the classification the emulator uses in anger; errorProtocolFor remains the
// per-service answer underneath it and the fallback for everything else. Splitting them
// this way is what keeps #392 intact: the request is consulted for a service that has
// genuinely ambiguous protocol, and ignored for a service whose one protocol the
// incoming Content-Type cannot reliably identify.
//
// A nil r is the per-service answer, which is what an in-process caller with no HTTP
// request in hand should get.
func errorProtocolForRequest(service string, r *http.Request) awsErrorProtocol {
	contentType := ""
	if r != nil {
		contentType = r.Header.Get("Content-Type")
	}
	if r != nil && multiProtocolServices[service] {
		switch detectWireProtocol(r) {
		case WireRPCV2CBOR:
			return errProtoRPCV2CBOR
		case WireJSONRPC:
			return errProtoJSONRPC
		case WireQuery:
			// Fall through: the service's own entry is the Query answer, and it may be
			// a shape more specific than errProtoQueryXML.
		}
	}
	return errorProtocolFor(service, contentType)
}

// errorWireContext is everything the error serializer needs beyond the error itself.
//
// It is a struct rather than four parameters because two of the fields exist only for
// the newer protocols — the shape ID a CBOR or JSON client matches on depends on the
// service, and the query-compatibility header depends on what the client asked for —
// and threading them positionally through a function whose three XML arms ignore both
// reads as if every arm cared.
type errorWireContext struct {
	// Protocol is the wire form to serialize into.
	Protocol awsErrorProtocol

	// JSONContentType is the request's Content-Type, used by the JSON-RPC arm to echo
	// the caller's JSON version.
	JSONContentType string

	// Service is the service the error is about, which the JSON-RPC and CBOR arms need
	// in order to name the modeled error shape; see errorShapeFor.
	Service string

	// QueryMode reports whether the caller sent X-Amzn-Query-Mode: true and therefore
	// needs the Query protocol's error code in a response header.
	QueryMode bool
}

// errorWireContextFor builds the serializer's context from a service and the request
// that provoked the error. A nil r yields the per-service protocol with no query-mode
// bridging, which is the right answer for an in-process caller.
func errorWireContextFor(service string, r *http.Request) errorWireContext {
	ct := ""
	if r != nil {
		ct = r.Header.Get("Content-Type")
	}
	return errorWireContext{
		Protocol:        errorProtocolForRequest(service, r),
		JSONContentType: ct,
		Service:         service,
		QueryMode:       detectQueryMode(r),
	}
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
	// Unreachable: errorProtocolFor returns one of the five constants above and never
	// errProtoRPCV2CBOR, which only errorProtocolForRequest selects. That matters for
	// CloudWatch, the one service where CBOR is reachable: this function is called with
	// no request in hand (authz.go), so a refused CloudWatch call reports "AccessDenied"
	// on every protocol. That is deliberate — the value is observed AWS behavior for the
	// query-protocol services, CloudWatch's model declares no access-denied shape to
	// contradict it, and a code that changed with the caller's serialization would be
	// harder to assert against than one that does not. Defaulting to the suffixed form
	// below keeps a hypothetical new protocol on the value the overwhelming majority of
	// AWS services use.
	return "AccessDeniedException"
}

// marshalAWSError serializes err in the wire format wire.Protocol names, returning the
// body, the Content-Type to send, and any extra response headers.
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
//   - JSON RPC: {"__type":"Shape","message":"…"} — "__type" is the member
//     botocore reads; a body carrying only "Code" leaves the SDK to fall back to
//     the stringified HTTP status.
//   - REST-JSON: the x-amzn-errortype header carries the code, which botocore
//     prefers over the body.
//   - RPC v2 CBOR: a two-member CBOR map, {"__type": <absolute shape ID>,
//     <the shape's message member>: …}, and no error code anywhere else.
//
// Both JSON forms also emit the lowercase "message" member the SDKs read, and
// the JSON-RPC form repeats the code in "Code" so existing callers that read
// that member keep working.
//
// The two protocols that identify an error by shape rather than by code — JSON RPC and
// CBOR — name the *modeled shape* for a service substrate has a model for, which today
// means CloudWatch: an SDK matches "__type" against the shape names its codegen
// produced, so answering the Query code there leaves it with an unrecognized error. See
// errorShapeFor, and note that for every other service the shape and the code are the
// same string, so nothing about those services' bytes changes.
//
// When the caller set X-Amzn-Query-Mode both of those arms also emit
// x-amzn-query-error, which carries the Query code the SDK's own caller may still be
// matching on. That is the whole point of aws.protocols#awsQueryCompatible: the modeled
// shape name goes in the body for the SDK, and the legacy Query code goes in the header
// for whoever is reading the SDK's output.
func marshalAWSError(e *AWSError, wire errorWireContext) (body []byte, contentType string, headers map[string]string) {
	switch wire.Protocol {
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
		ct := wire.JSONContentType
		if !strings.HasPrefix(ct, "application/x-amz-json") {
			ct = "application/x-amz-json-1.1"
		}
		shape := errorShapeFor(wire.Service, e.Code, e.HTTPStatus)
		payload, err := json.Marshal(map[string]string{
			"__type":  shape.ShapeID(),
			"message": e.Message,
			"Code":    e.Code,
			"Message": e.Message,
		})
		if err != nil {
			return nil, ct, nil
		}
		// aws-sdk-go-v2's JSON deserializers prefer X-Amzn-ErrorType over the body when
		// it is present, so it has to name the same thing "__type" does or the two
		// disagree about which error this is.
		hdrs := map[string]string{"x-amzn-ErrorType": shape.ShapeID()}
		if wire.QueryMode {
			hdrs[headerQueryError] = queryErrorHeader(e.Code, shape.ServerFault)
		}
		return payload, ct, hdrs

	case errProtoRPCV2CBOR:
		shape := errorShapeFor(wire.Service, e.Code, e.HTTPStatus)
		payload, err := cborEncode(cborMap{
			{Key: "__type", Value: shape.ShapeID()},
			{Key: shape.MessageMember, Value: e.Message},
		})
		if err != nil {
			return nil, contentTypeCBOR, nil
		}
		hdrs := map[string]string{headerSmithyProtocol: smithyProtocolRPCV2CBOR}
		if wire.QueryMode {
			hdrs[headerQueryError] = queryErrorHeader(e.Code, shape.ServerFault)
		}
		return payload, contentTypeCBOR, hdrs

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
