package emulator

import (
	"encoding/xml"
	"net/http"
)

// The query protocol's result element, and the two SNS operations that were missing it (#1141).
//
// `TagResource` and `UntagResource` wrote the tag, saved the topic and answered 200 — and a caller
// using `aws-sdk-go-v2/service/sns` still saw the call fail:
//
//	operation error SNS: TagResource, https response error StatusCode: 200, RequestID: ,
//	deserialization failed, failed to decode response body, TagResourceResult node not found
//
// That is the worst shape a divergence can take. The write had already happened, so the state was
// right and only the envelope was wrong, while every caller that correctly checks its error treated
// a successful tag as a failure and stopped. A create → converge → tag deployment sequence cannot
// get past its last step.
//
// # An empty result is not the same as no result
//
// The query protocol wraps an operation's result in a `<{Operation}Result>` element inside
// `<{Operation}Response>`. Whether that element is there at all is decided by the operation's
// modeled output, and the two cases look identical from substrate's side because both carry no
// members:
//
//   - an output of `smithy.api#Unit` has **no** result element — `DeleteTopic`, `SetTopicAttributes`,
//     `Unsubscribe`, `SetSubscriptionAttributes`, `AddPermission` and `RemovePermission`;
//   - an output that is an empty *structure* has the element, empty — `TagResource` and
//     `UntagResource`.
//
// AWS publishes both halves as sample responses. `API_TagResource` shows
// `<TagResourceResponse><TagResourceResult/><ResponseMetadata>…`, `API_UntagResource` shows
// `<UntagResourceResult/>` in the same position, and `API_DeleteTopic`, `API_Unsubscribe` and
// `API_AddPermission` all show `<ResponseMetadata>` as the response's only child. Substrate emitted
// the second shape for all eight, which is right for six of them.
//
// A hand-written client cannot see the difference — the element carries nothing — which is why the
// omission survived until a real SDK reached it. The SDK does see it: its generated deserializer
// looks the element up by name before reading `ResponseMetadata`, and fails the operation when it is
// absent rather than treating a missing empty element as an empty one.
//
// # Why the sweep stops at two
//
// The other six operations substrate routes with an empty body need no change, and that is checked
// rather than assumed: `aws-sdk-go-v2/service/sns`'s deserializer asks for a result element for 31
// of SNS's operations and none of the six is among them, so no SDK can fail on their absence, and
// AWS's own samples for three of the six show no such element. The ten routed operations that do
// return members already emit theirs.
//
// The distinction itself is not new here. [cwRenderQueryXML] draws the same one for CloudWatch —
// "A Unit output has no result element at all, which is why result being nil is not the same as it
// being empty" — and [elbEmptyOKResponse] is ELBv2's empty-result answer. What was missing was SNS
// saying which of its own operations are in which class, which is what the two constructors below
// are for: a handler now names the class it is in, and the choice is visible at the call site rather
// than buried in an inline struct that looks like the seven beside it.

// snsUnitResponse answers an SNS operation whose modeled output is `smithy.api#Unit`: a
// `<{Operation}Response>` holding a `<ResponseMetadata>` and nothing else.
//
// Six routed operations are in this class — `DeleteTopic`, `SetTopicAttributes`, `Unsubscribe`,
// `SetSubscriptionAttributes`, `AddPermission` and `RemovePermission`. Use
// [snsEmptyResultResponse] for an operation whose output is an empty structure; see this file's
// comment for why the two are not interchangeable.
func snsUnitResponse(operation, requestID string) (*AWSResponse, error) {
	return snsEnvelope(operation, requestID, false)
}

// snsEmptyResultResponse answers an SNS operation whose modeled output is an empty structure: a
// `<{Operation}Response>` holding an empty `<{Operation}Result>` and a `<ResponseMetadata>`.
//
// `TagResource` and `UntagResource` are the two routed operations in this class, and AWS publishes
// the empty element in both of their sample responses.
func snsEmptyResultResponse(operation, requestID string) (*AWSResponse, error) {
	return snsEnvelope(operation, requestID, true)
}

// snsEnvelope renders a memberless query-protocol response, with or without the result element.
//
// The element names are built from the operation rather than declared in a struct tag, which is what
// lets one builder serve both classes; the rendered XML is byte-for-byte what the eight inline
// structs it replaced produced, apart from the result element itself.
func snsEnvelope(operation, requestID string, withResult bool) (*AWSResponse, error) {
	type result struct {
		XMLName xml.Name
	}
	type response struct {
		XMLName          xml.Name
		Xmlns            string `xml:"xmlns,attr"`
		Result           *result
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	env := response{
		XMLName:          xml.Name{Local: operation + "Response"},
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: requestID},
	}
	if withResult {
		env.Result = &result{XMLName: xml.Name{Local: operation + "Result"}}
	}
	return snsXMLResponse(http.StatusOK, env)
}
