package emulator

import (
	"net/http"
	"slices"
	"strconv"
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
// # The token
//
// The token is the offset form of [pageByOffsetToken], decoded by [decodeOffsetPaginationToken], so
// a token substrate could not have issued is refused, and refused before any state is read: a caller
// looping on a bad token is told so rather than handed page one again. A truncated page carries a
// `nextToken`; the last page carries none, as the pages require ("You should repeat this until the
// NextToken response element comes back as null").
//
// # What an offset does when the listing changes mid-walk
//
// An offset counts positions, so a listing that shrinks or grows between two pages shifts every later
// page. Three things change these listings, and the reader of a walk should know which:
//
//   - **A deleted share expiring.** GetResourceShares lists a DELETED share for its two-hour window
//     (#1171), and the first listing after the window removes it from the index. A walk spanning that
//     moment skips one share: the one that moves into the expired share's position. The offset past
//     the end of a shrunken listing clamps to an empty final page rather than erroring, which is
//     [decodeOffsetPaginationToken]'s rule.
//   - **A delete, an associate or a disassociate** changes what ListPrincipals and ListResources
//     report (a DELETED share contributes nothing to either), with the same shift.
//   - **A create** appends to the index, so it lands on the last page and shifts nothing before it.
//
// A value cursor — the record identifier the next page starts after, as
// `query_marker_pagination.go` uses for RDS and ElastiCache — would be immune to the first two. It
// does not fit cheaply here: that cursor is only well defined over a listing rendered in the order it
// compares in, and RAM's listings are rendered in creation order from an index whose entries' ARNs
// carry a random identifier, so adopting it would mean reordering all three responses. That is a
// separate, observable change and is recorded as a follow-up rather than folded into this one.

const (
	// ramMaxResultsMin and ramMaxResultsMax are the Valid Range all three pages publish for maxResults.
	ramMaxResultsMin = 1
	ramMaxResultsMax = 500
)

// ramPage is the decoded paging request of one RAM listing.
type ramPage struct {
	// offset is where the page starts.
	offset int

	// size is the page size, or 0 for the whole remainder, which is an absent maxResults.
	size int
}

// ramReadPage validates maxResults and nextToken, before any state is read.
func ramReadPage(maxResults *int, nextToken string) (ramPage, error) {
	page := ramPage{}
	if maxResults != nil {
		if *maxResults < ramMaxResultsMin || *maxResults > ramMaxResultsMax {
			return ramPage{}, ramInvalidParameter("maxResults must be between " + strconv.Itoa(ramMaxResultsMin) +
				" and " + strconv.Itoa(ramMaxResultsMax) + ", was " + strconv.Itoa(*maxResults))
		}
		page.size = *maxResults
	}
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return ramPage{}, &AWSError{
			Code: "InvalidNextTokenException",
			// API_GetResourceShares / API_ListPrincipals / API_ListResources, Errors.
			Message: "The operation failed because the specified value for NextToken isn't valid. " +
				"You must specify a value you received in the NextToken response of a previous call to this operation.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	page.offset = offset
	return page, nil
}

// ramPaged cuts one page out of items and answers it under member, with `nextToken` when a further
// page exists. items must be in a stable order; every caller builds them from the share index in
// index order.
func ramPaged[T any](member string, items []T, page ramPage) (*AWSResponse, error) {
	cut, next := pageByOffsetToken(items, page.offset, page.size)
	out := map[string]interface{}{member: cut}
	if next != "" {
		out["nextToken"] = next
	}
	return ramJSONResponse(http.StatusOK, out)
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
