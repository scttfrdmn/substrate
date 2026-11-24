package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

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
// The `404` that stays is `UnknownOperationException` for an operation this plugin does not route, which
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

// webACLAssociatedResource answers the first resource (in key order) associated with the web ACL
// webACLArn in the caller's account and Region, or "" when none is. Associations are keyed by resource
// ([wafv2AssocKey]) and carry no reverse index, so this scans the caller's associations; a store fault
// or an association that does not decode is an error, never "not associated".
func (p *WAFv2Plugin) webACLAssociatedResource(reqCtx *RequestContext, webACLArn string) (string, error) {
	prefix := wafv2AssocKey(reqCtx.AccountID, reqCtx.Region, "")
	keys, err := p.state.List(context.Background(), wafv2Namespace, prefix)
	if err != nil {
		return "", fmt.Errorf("wafv2 list associations: %w", err)
	}
	for _, key := range keys {
		data, err := p.state.Get(context.Background(), wafv2Namespace, key)
		if err != nil {
			return "", fmt.Errorf("wafv2 get association %s: %w", key, err)
		}
		if data == nil {
			continue
		}
		var arn string
		if err := json.Unmarshal(data, &arn); err != nil {
			return "", fmt.Errorf("wafv2 decode association %s: %w", key, err)
		}
		if arn == webACLArn {
			return strings.TrimPrefix(key, prefix), nil
		}
	}
	return "", nil
}

// wafv2AssociatedItem refuses deleting a web ACL that a resource is still associated with.
// API_DeleteWebACL publishes WAFAssociatedItemException at 400, "AWS WAF couldn't perform the operation
// because your resource is being used by another resource or it's associated with another resource."
// The message names the ACL and the resource; that wording is substrate's own.
func wafv2AssociatedItem(message string) *AWSError {
	return &AWSError{
		Code:       "WAFAssociatedItemException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
