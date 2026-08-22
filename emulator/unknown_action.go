package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// unknownActionError refuses an operation a plugin does not implement, in the
// code, message and HTTP status the service's own error reference publishes for
// that condition.
//
// Every plugin used to write this refusal itself — 59 sites in eight shapes, each
// naming the plugin's Go type in the message ("SSMPlugin: unknown operation
// \"Foo\"") — so a consumer's error branch read a substrate implementation detail
// that no real endpoint emits, and a caller could not tell "substrate has not
// implemented this" from any other refusal (#716). Routing them all here makes the
// answer a property of the service's protocol rather than of whoever wrote the
// plugin.
//
// The protocol decides it because AWS publishes two different answers:
//
//   - JSON-RPC and REST-JSON services publish UnknownOperationException with HTTP
//     404: "The action or operation isn't recognized. Verify that the action name
//     is spelled correctly and that it's supported by the API version you're
//     using." Confirmed on a JSON-RPC service's Common Errors page (DynamoDB) and
//     a REST-JSON one's (Lambda), which agree on both the code and the 404.
//
//   - Query, "ec2" and REST-XML services publish nothing for it. AWS's current
//     Common Errors pages carry no unknown-action code at all for them — the
//     InvalidAction entry they once had has been removed, checked on EC2's, IAM's
//     and SNS's pages — so neither the code nor the message below can be cited to
//     the live reference. Substrate keeps InvalidAction because it is the code the
//     Query protocol has always used, an SDK caller's error branch is written
//     against it, and the reference offers no replacement to move to.
//
// service is the plugin's own [Plugin.Name]. An unregistered service falls back to
// the Query answer, which is [errorProtocolFor]'s own default with no
// Content-Type to read.
//
// action is normalized to "(empty)" when blank, so a caller reading the message never
// sees the two spaces a missing name would leave behind. A blank name is reachable:
// a JSON-RPC service dispatches on an X-Amz-Target header a caller can omit.
func unknownActionError(service, action string) *AWSError {
	if action = strings.TrimSpace(action); action == "" {
		action = "(empty)"
	}
	switch errorProtocolFor(service, "") {
	case errProtoJSONRPC, errProtoRESTJSON:
		return &AWSError{
			Code:       "UnknownOperationException",
			Message:    fmt.Sprintf("The action %s is not recognized.", action),
			HTTPStatus: http.StatusNotFound,
		}
	default:
		return &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("The action %s is not valid for this endpoint.", action),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// unknownRouteError is [unknownActionError] for a REST plugin whose path resolver
// matched nothing, so there is no operation name to name.
//
// A REST operation is identified by verb *and* path, and these plugins reach their
// default arm precisely when the pair resolved to no operation — so the pair is
// what the refusal reports. Both halves are included because either alone is
// ambiguous: DELETE and POST on the same path are different operations, and the
// same verb on a mistyped path is the more common mistake.
func unknownRouteError(service, method, path string) *AWSError {
	return unknownActionError(service, method+" "+path)
}
