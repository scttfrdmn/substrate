package emulator

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// GetResourceShares, ListPrincipals and ListResources page their answers (#1542).
//
// All three pages publish `maxResults` — "Specifies the total number of results that you want
// included on each page of the response. If you do not include this parameter, it defaults to a value
// that is specific to the operation. … Valid Range: Minimum value of 1. Maximum value of 500." — and
// `nextToken`, with InvalidNextTokenException/400 for "a value you received in the NextToken response
// of a previous call to this operation" that the caller did not. Substrate read neither and emitted no
// `nextToken`, so every listing was one page and a consumer's token loop ran once.
//
// # The page size
//
// A `maxResults` outside 1–500 is InvalidParameterException/400, the code all three pages publish for
// "a parameter you specified isn't valid"; the message is substrate's. An **absent** one answers the
// whole listing on one page. The pages publish no number for the default — only "a value that is
// specific to the operation" — so any page size substrate chose would be invented, and one page of
// everything is what every caller received before this change. A caller that wants pages asks for
// them.
//
// # The token, and refusing one substrate did not issue
//
// A truncated page carries a `nextToken`; the last page carries none, as the pages require ("You
// should repeat this until the NextToken response element comes back as null"). Every token is
// [encodeQueryMarker] of `{Operation}:{position}`, so it names the operation that issued it: the page
// says a token must be one "you received in the NextToken response of a previous call to this
// operation", and a ListPrincipals token handed to ListResources is not. A token that does not decode,
// names another operation, or carries a position this operation never emits is
// InvalidNextTokenException/400, refused before any state is read, so a caller looping on a bad token
// is told so rather than handed page one again. The message is the pages' own.
//
// # GetResourceShares pages on a value cursor
//
// Its listing is rendered in resource-share ARN order — the share index is kept sorted
// ([updateStringIndex]) and the handler sorts again so the order does not rest on that — so the
// [queryMarkerCursor] RDS and ElastiCache use fits it at no cost: the position is the ARN of the
// page's last share, and the next page is the shares whose ARN sorts strictly after it. That matters
// here because this listing is the one that shrinks on its own. A DELETED share is listed for its
// two-hour window (#1171) and the first listing after the window removes it from the index; an offset
// would then skip the share that moved into its position, where the value cursor resumes after the
// expired ARN as if it were still there.
//
// The position must be a resource-share ARN in the caller's own account and Region, which every token
// this operation issues carries. That is a check on the text, not on state, so a token naming a share
// that has since expired is accepted and resumes after it.
//
// # ListPrincipals and ListResources page on an offset
//
// Their items are not records with an identifier of their own in a sortable order: each listing is
// the principals or resources of every live share, in share-ARN order and then association order,
// deduplicated across shares. So the position is an offset, cut by [pageByOffsetToken]. The offset
// is never 0 — the first page is the absent token — so a token naming offset 0 is refused. An offset
// counts positions, so a delete, associate or disassociate between two pages shifts later pages and a
// walk spanning it skips or repeats one item; a token past a listing that has since shrunk clamps to
// an empty final page rather than erroring. An expiry does not shift them, because a DELETED share
// contributes nothing to either. A value cursor here would need the items rendered in an order they
// compare in, which reorders both responses; that is a follow-up (#1566), not part of this change.

const (
	// ramMaxResultsMin and ramMaxResultsMax are the Valid Range all three pages publish for maxResults.
	ramMaxResultsMin = 1
	ramMaxResultsMax = 500
)

// ramInvalidNextToken is the refusal for a nextToken substrate did not issue, in the words all three
// pages publish for the code.
func ramInvalidNextToken() *AWSError {
	return &AWSError{
		Code: "InvalidNextTokenException",
		// API_GetResourceShares / API_ListPrincipals / API_ListResources, Errors.
		Message: "The operation failed because the specified value for NextToken isn't valid. " +
			"You must specify a value you received in the NextToken response of a previous call to this operation.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramEncodeToken renders the nextToken that resumes operation's listing at position.
func ramEncodeToken(operation, position string) string {
	return encodeQueryMarker(operation + ":" + position)
}

// ramDecodeToken returns the position a nextToken carries, refusing one that does not decode or that
// operation did not issue. An empty token is the first page and decodes to "".
func ramDecodeToken(operation, raw string) (string, error) {
	cursor, perr := parseQueryMarker(raw)
	if perr != nil {
		return "", ramInvalidNextToken()
	}
	if cursor.after == "" {
		return "", nil
	}
	position, ok := strings.CutPrefix(cursor.after, operation+":")
	if !ok || position == "" {
		return "", ramInvalidNextToken()
	}
	return position, nil
}

// ramReadMaxResults validates maxResults, returning 0 — the whole listing — for an absent one.
func ramReadMaxResults(maxResults *int) (int, error) {
	if maxResults == nil {
		return 0, nil
	}
	if *maxResults < ramMaxResultsMin || *maxResults > ramMaxResultsMax {
		return 0, ramInvalidParameter("maxResults must be between " + strconv.Itoa(ramMaxResultsMin) +
			" and " + strconv.Itoa(ramMaxResultsMax) + ", was " + strconv.Itoa(*maxResults))
	}
	return *maxResults, nil
}

// ramPage is the decoded paging request of ListPrincipals or ListResources.
type ramPage struct {
	// operation is the listing's operation name, which its tokens carry.
	operation string

	// offset is where the page starts.
	offset int

	// size is the page size, or 0 for the whole remainder, which is an absent maxResults.
	size int
}

// ramReadPage validates an offset-paged listing's maxResults and nextToken, before any state is read.
//
// The offset must be the canonical decimal of a positive integer: substrate emits no other, and the
// first page is the absent token rather than offset 0.
func ramReadPage(operation string, maxResults *int, nextToken string) (ramPage, error) {
	size, err := ramReadMaxResults(maxResults)
	if err != nil {
		return ramPage{}, err
	}
	page := ramPage{operation: operation, size: size}
	position, err := ramDecodeToken(operation, nextToken)
	if err != nil || position == "" {
		return page, err
	}
	offset, convErr := strconv.Atoi(position)
	if convErr != nil || offset < 1 || strconv.Itoa(offset) != position {
		return ramPage{}, ramInvalidNextToken()
	}
	page.offset = offset
	return page, nil
}

// ramPaged cuts one page out of items and answers it under member, with `nextToken` when a further
// page exists. items must be in a stable order; both callers build them from the shares in ARN order.
func ramPaged[T any](member string, items []T, page ramPage) (*AWSResponse, error) {
	cut, next := pageByOffsetToken(items, page.offset, page.size)
	out := map[string]interface{}{member: cut}
	if next != "" {
		// pageByOffsetToken emits a token only when a further item exists; the token this listing
		// issues is its own, bound to the operation, at the same next offset.
		out["nextToken"] = ramEncodeToken(page.operation, strconv.Itoa(page.offset+len(cut)))
	}
	return ramJSONResponse(http.StatusOK, out)
}

// ramGetResourceSharesOp is the operation name GetResourceShares' tokens carry.
const ramGetResourceSharesOp = "GetResourceShares"

// ramShareCursor is GetResourceShares' decoded paging request.
type ramShareCursor struct {
	// marker is the ARN the page starts after; its zero value is the first page.
	marker queryMarkerCursor

	// size is the page size, or 0 for the whole remainder.
	size int
}

// ramReadShareCursor validates GetResourceShares' maxResults and nextToken, before any state is read.
//
// A token's position must be a resource-share ARN in the caller's account and Region; anything else
// is one substrate did not issue.
func ramReadShareCursor(maxResults *int, nextToken, acct, region string) (ramShareCursor, error) {
	size, err := ramReadMaxResults(maxResults)
	if err != nil {
		return ramShareCursor{}, err
	}
	after, err := ramDecodeToken(ramGetResourceSharesOp, nextToken)
	if err != nil {
		return ramShareCursor{}, err
	}
	if after != "" {
		prefix := "arn:aws:ram:" + region + ":" + acct + ":resource-share/"
		if !strings.HasPrefix(after, prefix) || len(after) == len(prefix) {
			return ramShareCursor{}, ramInvalidNextToken()
		}
	}
	return ramShareCursor{marker: queryMarkerCursor{after: after}, size: size}, nil
}

// ramPageShares cuts GetResourceShares' page out of shares, which must be in ARN order, returning the
// token that resumes after it when a further share exists.
func ramPageShares(shares []ramResourceShareOut, c ramShareCursor) ([]ramResourceShareOut, string) {
	start := 0
	for start < len(shares) && c.marker.skip(shares[start].ResourceShareArn) {
		start++
	}
	page := shares[start:]
	if c.size > 0 && len(page) > c.size {
		page = page[:c.size]
		return page, ramEncodeToken(ramGetResourceSharesOp, page[len(page)-1].ResourceShareArn)
	}
	return page, ""
}

// ramTagFilter is API_TagFilter: a tag key and the values it matches, any value when none is given.
type ramTagFilter struct {
	TagKey    string   `json:"tagKey"`
	TagValues []string `json:"tagValues"`
}

// ramValidateTagFilters refuses a filter with no key, which API_TagFilter forbids ("This must have a
// valid string value and can't be empty"). The message is substrate's.
func ramValidateTagFilters(filters []ramTagFilter) error {
	for _, f := range filters {
		if f.TagKey == "" {
			return ramInvalidParameter("tagFilters tagKey must not be empty")
		}
	}
	return nil
}

// ramMatchesTagFilters reports whether a share's tags satisfy filters. API_TagFilter: "Multiple tag
// filters are evaluated as an OR condition", and a filter with no values "matches any tag with the
// specified key, regardless of its value".
func ramMatchesTagFilters(filters []ramTagFilter, tags []RAMTag) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		for _, tag := range tags {
			if tag.Key == f.TagKey && (len(f.TagValues) == 0 || slices.Contains(f.TagValues, tag.Value)) {
				return true
			}
		}
	}
	return false
}
