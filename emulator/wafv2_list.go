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
//   - The page is cut from the sorted ID index before any record is loaded, so a listing loads only
//     the records it returns. A passed-over entry can leave a page short while a marker is still
//     emitted; the offset counts index entries, so the walk stays coherent.
//
// # GetWebACLForResource
//
// The handler answered `{"WebACL": {"ARN": …}}` — the association's stored ARN and nothing else —
// where API_GetWebACLForResource publishes "WebACL / The web ACL that is associated with the resource."
// of type WebACL, whose DefaultAction and VisibilityConfig are Required: Yes. It now resolves the ARN to
// the stored record through [WAFv2Plugin.loadWebACLByARN] — the path GetWebACL takes for its own `ARN`
// member — and answers it through [wafv2WebACLToWire], GetWebACL's projection, so the two cannot
// disagree about one ACL. The association keeps storing the bare ARN, the format every recorded run
// already holds, rather than gaining a second copy of the scope and ID that could drift from it.
//
// A resource with no association answers 200 with no WebACL member: the page says "If there is no
// associated resource, AWS WAF returns a null web ACL." (#1541). Before #1541 it answered
// WAFNonexistentItemException/400. An association whose web ACL no longer exists still answers
// WAFNonexistentItemException/400 — published on this page as "your resource doesn't exist" — but it is
// no longer reachable through the API: DeleteWebACL refuses an ACL that is still associated with
// WAFAssociatedItemException, as API_DeleteWebACL publishes, and AssociateWebACL refuses a WebACLArn
// naming no ACL.

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

// loadWebACLByARN loads the web ACL an ARN names, for the caller's account and Region.
//
// It is the one ARN-to-record path in the plugin: GetWebACL takes it when the request names `ARN`, and
// GetWebACLForResource takes it for the ARN an association stores, so the two cannot resolve one ARN
// differently. The parse is checked against the record it finds — the loaded ACL's own ARN, minted by
// [wafv2ARN], must equal the one asked for — so if the parse and the builder ever disagreed the answer
// would be a refusal rather than a different ACL. An ARN naming another account or Region, one that is
// not a web ACL ARN, and one naming no stored ACL are all WAFNonexistentItemException/400.
func (p *WAFv2Plugin) loadWebACLByARN(reqCtx *RequestContext, arn string) (*WAFv2WebACL, error) {
	notFound := wafv2NonexistentItem("Web ACL " + arn + " does not exist.")
	acct, region, scope, id, ok := wafv2ParseWebACLArn(arn)
	if !ok || acct != reqCtx.AccountID || region != reqCtx.Region {
		return nil, notFound
	}
	acl, err := p.loadWebACLByID(acct, region, scope, id)
	if err != nil {
		return nil, err
	}
	if acl.ARN != arn {
		return nil, notFound
	}
	return acl, nil
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
