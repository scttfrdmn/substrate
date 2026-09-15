package emulator

import (
	"fmt"
	"net/http"
)

// An out-of-range MaxItems is refused rather than silently rewritten (#868).
//
// Every paginated IAM operation declares `MaxItems` as the API model's `maxItemsType`,
// whose whole definition is "Valid Range: Minimum value of 1. Maximum value of 1000" plus
// "If you do not include this parameter, the number of items defaults to 100". IAM answers
// a value outside that range with a validation failure; substrate answered a page.
//
// [paginateIAMKeys] read `if maxItems <= 0 || maxItems > 1000 { maxItems = 100 }`, and
// [iamTagListingResponse] read `if limit <= 0 { limit = 100 }` with no upper bound at all —
// so `MaxItems=1001` produced a 100-item page at seven operations and a 1001-item page at
// four more. The consequence is the release's theme aimed at a request parameter: the page
// size a caller got came from neither the request nor a documented default. A consumer
// wiring up pagination against substrate saw its bad `MaxItems` accepted, wrote the walk
// around a page size it never asked for, and got a 400 from IAM the first time it ran for
// real.
//
// The guard therefore lives in the handler, before the shared paginator, and the coercion in
// [paginateIAMKeys] stays as the defaulting path for a value that was never sent. It is
// applied at every IAM operation that declares the parameter — including the ones that parse
// it and then have nothing to truncate, such as `ListAttachedUserPolicies` and
// `ListPolicyVersions` — because AWS validates a request parameter before it decides whether
// the request has enough results to use it, and an operation that accepts 1001 while its
// sibling refuses it is the inconsistency this exists to remove.
//
// The code is `ValidationError` with HTTP 400, which is `CommonErrors`' entry for a
// parameter that fails a model constraint. It differs deliberately from
// [parseSimulateRequest], which answers `InvalidInput` for the same range on the same
// parameter, and the difference is not an oversight to be tidied away later:
// `SimulatePrincipalPolicy` *publishes* `InvalidInput` in its own Errors section, while
// `ListUsers`, `ListUserTags` and every other operation guarded here publish only
// `NoSuchEntity` and `ServiceFailure` — so for them the code can only come from
// `CommonErrors`, which does not list `InvalidInput` at all. Only the code differs: the two
// simulate operations apply the same bounds and the same presence rule, so no IAM operation
// disagrees with another about which values are acceptable.

// iamMaxItemsMin and iamMaxItemsMax are the bounds of the API model's maxItemsType,
// shared by every IAM operation that paginates.
const (
	iamMaxItemsMin = 1
	iamMaxItemsMax = 1000
)

// iamMaxItemsDefault is the page size the reference gives for an absent MaxItems: "If you
// do not include this parameter, the number of items defaults to 100".
const iamMaxItemsDefault = 100

// iamValidateMaxItems returns a refusal when a request sent a MaxItems outside
// [iamMaxItemsMin]–[iamMaxItemsMax], and nil when it is in range or was not sent at all.
//
// Presence is read from req.Params rather than inferred from the decoded value, because
// [iamInt] cannot tell them apart: an absent parameter and an explicit `MaxItems=0` both
// decode to zero, and only one of them is an error. The query protocol delivers the
// distinction — parser.go flattens the form body into Params, so a parameter the caller sent
// is a key there whatever its value — which is what lets `MaxItems=0` be refused as the
// below-minimum value it is while an absent one still gets the documented default.
//
// A present-but-empty `MaxItems=` is in range by decision, not by omission: [iamInt]'s own
// doc comment records that such a caller expressed no limit and that AWS accepts the
// request, so refusing it here would fail a call that works against IAM.
//
// A request whose Params are nil — a hand-marshaled body in a unit test, never a client —
// falls back to treating zero as absent, which is what [parseSimulateRequest] has always
// done with the same parameter.
func iamValidateMaxItems(req *AWSRequest, maxItems iamInt) *AWSResponse {
	raw, sent := req.Params["MaxItems"]
	if !sent && maxItems == 0 {
		return nil
	}
	if sent && raw == "" {
		return nil
	}
	if maxItems < iamMaxItemsMin || maxItems > iamMaxItemsMax {
		return iamErrorResponse("ValidationError",
			fmt.Sprintf("MaxItems must be between %d and %d, got %d",
				iamMaxItemsMin, iamMaxItemsMax, maxItems.Int()),
			http.StatusBadRequest)
	}
	return nil
}
