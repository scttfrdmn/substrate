package emulator

import (
	"net/http"
	"strings"
)

// Per-request wire-protocol classification (#757).
//
// Substrate classified protocol per *service*: serviceErrorProtocols maps a plugin
// name to the one shape its errors take (error_protocol.go, #392). That is right for
// almost every service, because almost every service model declares one protocol.
//
// CloudWatch declares four traits at once — aws.protocols#awsQuery,
// aws.protocols#awsJson1_0, smithy.protocols#rpcv2Cbor and
// aws.protocols#awsQueryCompatible — and its clients disagree about which to use.
// aws-sdk-go-v2 sends rpc-v2-cbor, the AWS CLI and boto3 send JSON 1.0 with an
// X-Amz-Target, and a hand-rolled client sends a query form body. One answer cannot
// serve all three, so for a service like that the *request* has to decide, not the
// service name.
//
// The classification is kept deliberately narrow: it is consulted only for services
// listed as multi-protocol (see multiProtocolServices). A single-protocol service still
// answers per its model, because the incoming Content-Type genuinely cannot tell
// REST-JSON from a plain JSON body — which is the whole reason #392 stopped sniffing it.

// HTTP headers that identify a Smithy RPC v2 CBOR request, and the values substrate
// looks for in them.
const (
	// headerSmithyProtocol is required on every RPC v2 CBOR request and must be
	// echoed on the response.
	headerSmithyProtocol = "Smithy-Protocol"

	// smithyProtocolRPCV2CBOR is headerSmithyProtocol's value for this protocol.
	smithyProtocolRPCV2CBOR = "rpc-v2-cbor"

	// headerQueryMode is sent by a client talking to an aws.protocols#awsQueryCompatible
	// service, asking for errors it can match against the query protocol's codes.
	headerQueryMode = "X-Amzn-Query-Mode"

	// headerQueryError carries the query-protocol error code and fault type back to
	// such a client, as "<Code>;<Sender|Receiver>".
	headerQueryError = "x-amzn-query-error"

	// contentTypeCBOR is the media type an RPC v2 CBOR body carries.
	contentTypeCBOR = "application/cbor"
)

// WireProtocol identifies the serialization one request used.
//
// It exists for a service whose model declares more than one, where the answer's shape
// has to follow the question's. A plugin reads it from [AWSRequest.Protocol] to decide
// how to render its response; the zero value is [WireQuery], which is what a request
// carrying none of the newer protocols' markers is.
type WireProtocol int

const (
	// WireQuery is the AWS Query protocol: a form-encoded body naming the operation
	// in an "Action" parameter, answered with XML. It is the zero value, and the
	// answer for any request that does not identify itself as one of the others.
	WireQuery WireProtocol = iota

	// WireJSONRPC is the AWS JSON RPC protocol, 1.0 or 1.1: a JSON body with the
	// operation in an X-Amz-Target header, answered with JSON. The version is carried
	// by the Content-Type rather than by this value, because it is the shape of the
	// document that differs between protocols and not between the two versions.
	WireJSONRPC

	// WireRPCV2CBOR is Smithy's RPC v2 CBOR protocol: a CBOR body at
	// /service/{Service}/operation/{Operation}, answered with CBOR.
	WireRPCV2CBOR
)

// String returns the protocol's name as it appears in a Smithy-Protocol header or in
// documentation, so a log line or a test failure names it rather than an integer.
func (p WireProtocol) String() string {
	switch p {
	case WireJSONRPC:
		return "json-rpc"
	case WireRPCV2CBOR:
		return smithyProtocolRPCV2CBOR
	default:
		return "query"
	}
}

// detectWireProtocol classifies r's serialization. It is pure and total: a request
// that identifies itself as none of the newer protocols is [WireQuery].
//
// The signals are the ones the protocols themselves specify, in the order a
// conformant client makes them available:
//
//  1. Smithy-Protocol: rpc-v2-cbor. Required on every RPC v2 CBOR request, so it is
//     the primary signal and outranks everything else.
//  2. An /service/{Service}/operation/{Operation} path with a CBOR Content-Type. RPC
//     v2 CBOR is the only protocol that routes by that path shape, so a client that
//     omitted the header is still unambiguous — and refusing it would mean rejecting
//     a request whose intent is plain.
//  3. An X-Amz-Target header, which only the JSON RPC protocols send.
//  4. An application/x-amz-json Content-Type, for a JSON RPC client that routes by
//     host rather than by target.
func detectWireProtocol(r *http.Request) WireProtocol {
	if r == nil {
		return WireQuery
	}
	contentType := r.Header.Get("Content-Type")

	if strings.Contains(r.Header.Get(headerSmithyProtocol), smithyProtocolRPCV2CBOR) {
		return WireRPCV2CBOR
	}
	if strings.Contains(contentType, contentTypeCBOR) && isRPCV2CBORPath(r.URL.Path) {
		return WireRPCV2CBOR
	}
	if r.Header.Get("X-Amz-Target") != "" {
		return WireJSONRPC
	}
	if strings.HasPrefix(contentType, "application/x-amz-json") {
		return WireJSONRPC
	}
	return WireQuery
}

// isRPCV2CBORPath reports whether path has the shape RPC v2 CBOR routes by:
// {prefix?}/service/{serviceName}/operation/{operationName}.
//
// Only the last four segments are examined, which is what the specification requires —
// a client is free to mount the emulator under a path prefix, and the routing must not
// depend on there being none.
func isRPCV2CBORPath(path string) bool {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 4 {
		return false
	}
	tail := segments[len(segments)-4:]
	return tail[0] == "service" && tail[1] != "" && tail[2] == "operation" && tail[3] != ""
}

// detectQueryMode reports whether r asked for query-compatible error codes.
//
// A client sets X-Amzn-Query-Mode when it is talking to an
// aws.protocols#awsQueryCompatible service over a non-query protocol, because its
// callers may still be matching on the query protocol's error codes rather than on the
// modeled shape names. A server seeing it must answer errors with an
// x-amzn-query-error header carrying the query code; see [queryErrorHeader].
//
// Provenance: this header is normative but is documented in neither the rpcv2Cbor
// specification nor the awsQueryCompatible trait reference. It is specified only by
// the AWS protocol test suite
// (smithy-aws-protocol-tests/model/rpcv2Cbor/query-compatible.smithy), and both
// reference clients send it — smithy-go's transport/http/protocol/rpcv2 and botocore's
// serialize.py.
func detectQueryMode(r *http.Request) bool {
	if r == nil {
		return false
	}
	return strings.EqualFold(r.Header.Get(headerQueryMode), "true")
}

// rpcV2CBORTargetConflict reports whether r declares RPC v2 CBOR *and* carries a target
// header, which the specification requires a server to reject.
//
// The rule is not pedantry. X-Amz-Target names the operation for the JSON RPC
// protocols, and RPC v2 CBOR names it in the path; a request carrying both is telling
// the server two things about what to invoke, and they can disagree. Substrate routes
// on X-Amz-Target first (parser.go), so honoring such a request would mean answering
// CBOR to whatever the *target* named, silently ignoring the path the client actually
// addressed.
func rpcV2CBORTargetConflict(r *http.Request) bool {
	if r == nil {
		return false
	}
	if !strings.Contains(r.Header.Get(headerSmithyProtocol), smithyProtocolRPCV2CBOR) {
		return false
	}
	return r.Header.Get("X-Amz-Target") != "" || r.Header.Get("X-Amzn-Target") != ""
}

// queryErrorHeader builds the x-amzn-query-error value for an error: the query
// protocol's code, then a semicolon, then the fault — "Sender" for a client error and
// "Receiver" for a server one, which are the query protocol's own two fault names
// rather than Smithy's "client" and "server".
func queryErrorHeader(queryCode string, serverFault bool) string {
	fault := "Sender"
	if serverFault {
		fault = "Receiver"
	}
	return queryCode + ";" + fault
}
