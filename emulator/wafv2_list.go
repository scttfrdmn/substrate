package emulator

import (
	"errors"
	"strconv"
	"strings"
)

// WAFv2's two list operations page by `Limit` and `NextMarker`, and GetWebACLForResource answers the
// web ACL itself (#1161).
//
// # Pagination
//
// ListWebACLs declared a `Limit` it never read and ListIPSets did not declare one at all; neither read
// or emitted a `NextMarker`, so every call returned the whole set and a consumer's marker loop ran
// once whatever the data. Both pages publish the same pair — `Limit` "Valid Range: Minimum value of 1.
// Maximum value of 100." and `NextMarker` "When you request a list of objects with a `Limit` setting, if
// the number of objects that are still available for retrieval exceeds the limit, AWS WAF returns a
// `NextMarker` value in the response. To retrieve the next batch of objects, provide the marker from the
// prior call in your next request." — so one rule serves both:
//
//   - A `Limit` outside 1–100 is refused, the rule page_size_range.go records for every JSON list
//     operation in this release. The code is WAFInvalidParameterException/400, published on both pages
//     with the example "You specified a parameter name or value that isn't valid." — a present value
//     substrate read and rejected, which is #755's line between it and the common list's
//     ValidationError for an absent member. The message is substrate's.
//   - Neither page publishes a default. An absent `Limit` is 100, the published maximum and substrate's
//     reading, so a listing of up to 100 objects answers exactly what it did before.
//   - `NextMarker` is [encodeOffsetPaginationToken]'s offset over the ID index, which updateStringIndex
//     keeps sorted, and it is emitted only when objects remain. A marker substrate did not issue is
//     refused with the same WAFInvalidParameterException rather than read as page one (#915); the
//     published Length 1–256 and Pattern `.*\S.*` are subsumed by the round trip, since the encoder's
//     output is short and has no whitespace.
//   - A record that cannot be read fails the listing instead of being skipped, since a skipped record
//     shortens the page and reports success over a store failure. An index entry outliving its record
//     is not a fault and is passed over.
//
// # GetWebACLForResource
//
// The handler answered `{"WebACL": {"ARN": …}}` — the association's stored ARN and nothing else —
// where API_GetWebACLForResource publishes "WebACL / The web ACL that is associated with the resource."
// of type WebACL, whose DefaultAction and VisibilityConfig are Required: Yes. It now resolves the ARN to
// the stored record and answers it through [wafv2WebACLToWire], the projection GetWebACL uses, so the
// two cannot disagree about one ACL.
//
// An association whose web ACL no longer exists answers WAFNonexistentItemException/400 — published on
// this page as "your resource doesn't exist" — as a resource with no association already does. The page
// also says "If there is no associated resource, AWS WAF returns a null web ACL.", which substrate does
// not adopt for either case: the sentence names no association rather than a dangling one, and
// substrate's no-association answer predates this change and is pinned by tests. On AWS the dangling
// case should not arise at all, because API_DeleteWebACL publishes WAFAssociatedItemException for an
// ACL still associated with a resource; substrate's DeleteWebACL does not refuse that yet, which is
// what makes the case reachable here.

// wafv2ListLimit is the `Limit` constraint ListWebACLs and ListIPSets both publish.
var wafv2ListLimit = pageSizeRange{member: "Limit", min: 1, max: 100, def: 100}

// wafv2ListParams resolves a list request's `Limit` and `NextMarker`.
func wafv2ListParams(limit *int, marker string) (int, int, error) {
	size, ok := wafv2ListLimit.resolve(limit)
	if !ok {
		return 0, 0, wafv2InvalidParameter("Limit", strconv.Itoa(*limit), "must be between 1 and 100")
	}
	offset, ok := decodeOffsetPaginationToken(marker)
	if !ok {
		return 0, 0, wafv2InvalidParameter("NextMarker", marker, "is not a marker returned by a previous call")
	}
	return size, offset, nil
}

// wafv2IsNonexistent reports whether err is the not-found refusal a load answers for a missing record.
func wafv2IsNonexistent(err error) bool {
	var awsErr *AWSError
	return errors.As(err, &awsErr) && awsErr.Code == "WAFNonexistentItemException"
}

// wafv2ParseWebACLArn splits a web ACL ARN built by [wafv2ARN] into the parts its state key is built
// from, reporting false for anything that is not one.
//
// The scope segment is the lowercase Scope, so it is upper-cased back; any other segment, resource
// type or field count is not a web ACL substrate could have minted.
func wafv2ParseWebACLArn(arn string) (acct, region, scope, id string, ok bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "wafv2" {
		return "", "", "", "", false
	}
	res := strings.Split(parts[5], "/")
	if len(res) != 4 || res[1] != "webacl" || res[3] == "" {
		return "", "", "", "", false
	}
	scope = strings.ToUpper(res[0])
	if scope != "REGIONAL" && scope != "CLOUDFRONT" {
		return "", "", "", "", false
	}
	return parts[4], parts[3], scope, res[3], true
}
