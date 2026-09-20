package emulator

import "net/http"

// WAFv2's three not-found refusals answer the status its pages publish (#1098).
//
// All three answered `WAFNonexistentItemException` at **404**. Every WAFv2 page that lists the code
// publishes it at **400** — `API_GetIPSet`'s Errors section reads *"WAFNonexistentItemException / AWS
// WAF couldn't perform the operation because your resource doesn't exist. If you've just created a
// resource that you're using in this operation, you might just need to wait a few minutes. It can take
// from a few seconds to a number of minutes for changes to propagate. / HTTP Status Code: 400"* — and no
// WAFv2 page publishes a 404. The same correction and the same reasoning as Glue's twelve; see
// glue_not_found.go, which carries the argument once for both services.
//
// This is three sites rather than twelve, and their messages name three different conditions — no web
// ACL associated with a resource, no web ACL with an Id, no IP set with an Id — so the constructor takes
// the message rather than composing it. What it fixes in one place is the pair that was wrong together:
// the code and its status.
//
// The `404` that stays is [UnknownOperationException] for an operation this plugin does not route, which
// is a protocol-level refusal published at 404 on the JSON protocol's own Common Errors page rather than
// anything in WAFv2's vocabulary — so the assertion at wafv2_plugin_test.go's default-arm test keeps its
// 404 and did *not* move with this change, contrary to what #1098's acceptance criteria assumed.

// wafv2NonexistentItem refuses a request naming a WAFv2 resource that has no record.
//
// message names which resource was not found, because the published gloss names neither the kind nor the
// identifier and a caller that passed both a web ACL and a resource ARN could not tell the two cases
// apart from it. The status is [http.StatusBadRequest] for the reason this file's comment gives.
func wafv2NonexistentItem(message string) *AWSError {
	return &AWSError{
		Code:       "WAFNonexistentItemException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
