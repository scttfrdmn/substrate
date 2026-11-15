package emulator

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// The success document every awsQuery plugin answers, built in one place (#1149, #1208, #1242).
//
// The awsQuery protocol (smithy's `aws.protocols#awsQuery`) wraps a successful response in an
// `{Operation}Response` root that carries the service's `xmlNamespace` as its `xmlns` attribute, the
// operation's output under `{Operation}Result`, and a `ResponseMetadata` element whose `RequestId`
// identifies the request. Every sample response on the ELB, Redshift, Auto Scaling, RDS and
// ElastiCache pages is that document — e.g. RDS `API_CreateDBInstance`:
//
//	<CreateDBInstanceResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
//	  <CreateDBInstanceResult>
//	    <DBInstance>…</DBInstance>
//	  </CreateDBInstanceResult>
//	  <ResponseMetadata>
//	    <RequestId>523e3218-afc7-11c3-90f5-f90431260ab4</RequestId>
//	  </ResponseMetadata>
//	</CreateDBInstanceResponse>
//
// # Why one builder rather than one per plugin
//
// ELB's builder (#1149) was the first; Redshift (#1208) reused its types, and Auto Scaling declared a
// field-for-field copy. RDS and ElastiCache had none at all: each of their 37 handlers declared an
// inline `response` struct with its own `XMLName` and `xmlns`, handed to a marshal wrapper taking an
// already-built `any`, and not one of them declared `ResponseMetadata` — the member was a convention
// every handler could omit, and every one did (#1242). Building the document here, from the
// operation name, makes the member a rule instead: no call site spells the root, the result element
// or the metadata, so none can leave them out.
//
// # A nil result omits the result element
//
// Two classes of memberless output exist and they are not interchangeable. An operation whose output
// shape declares a `resultWrapper` but no members publishes the empty element — every memberless ELB
// operation is in this class, and botocore looks the wrapper up by name, so omitting it makes the AWS
// CLI raise `KeyError` (#748). An operation with no output shape publishes no result element at all:
// RDS `DeleteDBSubnetGroup`, `DeleteDBParameterGroup`, `AddTagsToResource` and
// `RemoveTagsFromResource`, ElastiCache `DeleteCacheSubnetGroup` and `DeleteCacheParameterGroup`, and
// the Auto Scaling operations [asgNoResultResponse] answers. Their sample responses are the root and
// the metadata and nothing else. So a nil result omits the element and an empty struct writes it
// empty; each caller reads the class off its own page rather than inheriting another service's rule.
//
// # The request ID is the context's, not a fresh one
//
// [queryOKResponse] renders `reqCtx.RequestID`, the value the server assigned and [Event.RequestID]
// records, which is the rule `request_id_replay_test.go` pins: a replay dispatches under
// [replayRequestID], so the body a replayed handler renders is byte-identical to the recorded one.
// Minting a fresh ID here would make every response body differ on replay — #856's debt — and a
// deterministic emulator that cannot reproduce its own response bytes has given up the property it
// exists for.
//
// # The error envelope is not here
//
// An error answers the shared Query document `error_protocol.go` builds —
// `<ErrorResponse><Error><Code>…</Error><RequestId>…` — whose RequestId is the same reqCtx.RequestID
// (#1241). It carries no namespace: smithy's awsQuery `ErrorResponse` example has no `xmlns`
// attribute, so a namespace constant here governs success documents only.

// queryResultElement renders an operation's result under `<{Operation}Result>`.
//
// The element name is a field rather than a struct tag because it is the operation's, and the whole
// point of this file is that the operation name is spelled once. It is an [xml.Marshaler] rather than
// a struct with an `XMLName xml.Name` field so that a handler's result type stays a plain struct: an
// `XMLName` field would have to be set at every call site, which is the per-handler convention this
// replaces.
type queryResultElement struct {
	// name is the element name, `{Operation}Result`.
	name string

	// value is the result struct, whose fields are encoded inside the element.
	value any
}

// MarshalXML implements [xml.Marshaler].
//
// The start element handed in is discarded: it carries the *field's* name, which is `Result` here,
// and the element AWS publishes is named for the operation.
func (r queryResultElement) MarshalXML(e *xml.Encoder, _ xml.StartElement) error {
	if err := e.EncodeElement(r.value, xml.StartElement{Name: xml.Name{Local: r.name}}); err != nil {
		return fmt.Errorf("query encode %s: %w", r.name, err)
	}
	return nil
}

// queryResponseEnvelope is the awsQuery success document: the result, when the operation has one,
// then the published `ResponseMetadata`, in the order every sample response shows.
type queryResponseEnvelope struct {
	XMLName          xml.Name
	XMLNS            string `xml:"xmlns,attr"`
	Result           *queryResultElement
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// queryOKResponse answers an awsQuery operation with its result and the published `ResponseMetadata`.
//
// operation names the action, from which both element names are derived, and xmlns is the service's
// document namespace, read off that service's own sample responses. result is the operation's result
// struct; a nil result omits the `{Operation}Result` element, which is the no-output-shape class this
// file's comment describes, and an empty struct writes the element empty.
func queryOKResponse(reqCtx *RequestContext, operation, xmlns string, result any) (*AWSResponse, error) {
	env := queryResponseEnvelope{
		XMLName:          xml.Name{Local: operation + "Response"},
		XMLNS:            xmlns,
		ResponseMetadata: responseMetadata{RequestID: reqCtx.RequestID},
	}
	if result != nil {
		env.Result = &queryResultElement{name: operation + "Result", value: result}
	}
	body, err := xml.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("query xml marshal %s: %w", operation, err)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
