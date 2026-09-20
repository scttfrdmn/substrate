package emulator

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// No ELB response carried the Query protocol's `ResponseMetadata` (#1149).
//
// Every sample response on every Elastic Load Balancing page, in both generations, closes with the
// envelope — e.g. the 2012-06-01 `API_CreateLoadBalancer`:
//
//	<CreateLoadBalancerResponse xmlns="http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/">
//	  <CreateLoadBalancerResult>
//	    <DNSName>my-vpc-loadbalancer-1234567890.us-east-1.elb.amazonaws.com</DNSName>
//	  </CreateLoadBalancerResult>
//	  <ResponseMetadata>
//	    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
//	  </ResponseMetadata>
//	</CreateLoadBalancerResponse>
//
// Substrate emitted none of it, uniformly: no ELB response struct declared the member, so every
// response — ELBv2 and classic, result-bearing and memberless — stopped after the result element. A
// consumer's logging wrapper reading `ResponseMetadata.RequestId` off a successful ELB call got the
// empty string, silently, on every operation. Seven plugins already emit the envelope
// (`cloudformation_plugin.go`, `cloudwatch_render.go`, `iam_xml.go`, `sns_result_envelope.go`,
// `sns_plugin.go`, `sqs_plugin.go`, `sts_plugin.go`), so this is the shape catching up, not a new one.
//
// # One envelope, and what had to change for it to be one
//
// [elbXMLResponse] took an already-built `any`, so there was nowhere in it to put a member: the
// element order and the document's root name lived in ~30 inline `response` structs, one per handler,
// each declaring its own `XMLName` tag and `xmlns` attribute. Adding `ResponseMetadata` to those 30
// would have made the member a convention rather than a rule — the next handler would omit it and
// nothing would notice, which is how it came to be absent from all 30 in the first place.
//
// So the envelope is built here and the handlers pass a *result* instead of a response. The root and
// result element names are derived from the operation name, which is why no call site spells either:
// [elbResultElement] is an [xml.Marshaler] that encodes its value under `<{Operation}Result>`, and a
// nil one omits the element entirely, which is the `smithy.api#Unit` case [snsUnitResponse] draws the
// same distinction for. No ELB operation is in that class today — every memberless ELB operation
// publishes the empty result element — but the two are not interchangeable and the builder should not
// be the place that forgets which is which.
//
// # The request ID is the context's, not a fresh one
//
// [elbOKResponse] renders `reqCtx.RequestID`, the value the server assigned and [Event.RequestID]
// records, which is the rule `request_id_replay_test.go` pins: a replay dispatches under
// [replayRequestID], so the body a replayed ELB handler renders is byte-identical to the recorded
// one. Minting a fresh ID here would have made every ELB response body differ on replay — #856's
// debt arriving in a new plugin — and a deterministic emulator that cannot reproduce its own response
// bytes has given up the property it exists for.
//
// # The error envelope is not here
//
// An ELB error answers the shared Query document `error_protocol.go` builds —
// `<ErrorResponse><Error><Code>…` — which carries no request ID either, for any of the plugins that
// share it. No ELB page publishes a sample error response, so what belongs inside an ELB
// `ErrorResponse` is not readable off an ELB page, and the fix is one envelope shared by every Query
// plugin rather than an ELB change. Filed as #1241 rather than widened into this one.

// elbResultElement renders an operation's result under `<{Operation}Result>`.
//
// The element name is a field rather than a struct tag because it is the operation's, and the whole
// point of this file is that the operation name is spelled once. It is an [xml.Marshaler] rather than
// a struct with an `XMLName xml.Name` field so that a handler's result type stays a plain struct: an
// `XMLName` field would have to be set at every call site, which is the per-handler convention this
// replaces.
type elbResultElement struct {
	// name is the element name, `{Operation}Result`.
	name string

	// value is the result struct, whose fields are encoded inside the element.
	value any
}

// MarshalXML implements [xml.Marshaler].
//
// The start element handed in is discarded: it carries the *field's* name, which is `Result` here,
// and the element AWS publishes is named for the operation.
func (r elbResultElement) MarshalXML(e *xml.Encoder, _ xml.StartElement) error {
	if err := e.EncodeElement(r.value, xml.StartElement{Name: xml.Name{Local: r.name}}); err != nil {
		return fmt.Errorf("elb encode %s: %w", r.name, err)
	}
	return nil
}

// elbResponseEnvelope is the document every ELB response is, in both generations.
//
// The two generations differ only in the namespace, which is why one type serves both: [elbXMLNS]
// carries 2015-12-01 and [elbClassicXMLNS] carries 2012-06-01, and the member order below —
// result then metadata — is the order every published sample shows.
type elbResponseEnvelope struct {
	XMLName          xml.Name
	XMLNS            string `xml:"xmlns,attr"`
	Result           *elbResultElement
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// elbOKResponse answers an ELB operation with its result and the published `ResponseMetadata`.
//
// operation names the action, from which both element names are derived, and xmlns selects the
// generation. result is the operation's result struct; pass [elbEmptyResult] for an operation whose
// output carries no members, which is every memberless ELB operation — none is in the
// `smithy.api#Unit` class that omits the element, and see this file's comment for why the builder
// still distinguishes them.
func elbOKResponse(reqCtx *RequestContext, operation, xmlns string, result any) (*AWSResponse, error) {
	return elbXMLResponse(http.StatusOK, elbResponseEnvelope{
		XMLName:          xml.Name{Local: operation + "Response"},
		XMLNS:            xmlns,
		Result:           &elbResultElement{name: operation + "Result", value: result},
		ResponseMetadata: responseMetadata{RequestID: reqCtx.RequestID},
	})
}

// elbEmptyResult is the result of an operation whose output shape declares no members.
//
// A named type rather than a bare `struct{}{}` at eight call sites, so the class is visible in the
// call: these are the operations whose whole published response is the empty result element and the
// metadata.
type elbEmptyResult struct{}

// elbEmptyOKResponse answers an ELBv2 operation whose output shape carries no members.
//
// The empty result element is not decoration: ELBv2 speaks the Query protocol, where every output
// shape declares a `resultWrapper` and botocore looks that wrapper up by name in the parsed body, so
// a bare `<OperationResponse/>` makes the AWS CLI and boto3 raise
// `KeyError: 'DeleteLoadBalancerResult'` instead of reporting success. Six operations answered that
// way and were unusable from a real client while passing substrate's own tests, which read the XML
// directly rather than through an SDK's parser (#748).
func elbEmptyOKResponse(reqCtx *RequestContext, operation string) (*AWSResponse, error) {
	return elbOKResponse(reqCtx, operation, elbXMLNS, elbEmptyResult{})
}

// elbClassicEmptyOKResponse answers a classic operation whose output shape carries no members.
//
// The 2012-06-01 twin of [elbEmptyOKResponse]; the two differ in the document namespace and in
// nothing else. `DeleteLoadBalancer` is the one routed operation in this class, and its published
// sample response is `<DeleteLoadBalancerResult/>` followed by the metadata.
func elbClassicEmptyOKResponse(reqCtx *RequestContext, operation string) (*AWSResponse, error) {
	return elbOKResponse(reqCtx, operation, elbClassicXMLNS, elbEmptyResult{})
}
