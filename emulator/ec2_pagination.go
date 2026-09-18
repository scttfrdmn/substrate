package emulator

import (
	"net/http"
	"strconv"
)

// Offset pagination for the EC2 describes (#917).
//
// Two operations paginated before this file existed — DescribeTags and
// DescribeLaunchTemplateVersions — and each carried its own copy of the same three rules.
// **Sixteen** other routed describes published MaxResults and NextToken and implemented
// neither, so converting them one at a time would have meant a third, fourth and fifth copy.
// These four helpers are that one copy: the two original sites call them too, so the number of
// implementations went from two to one rather than from two to eighteen.
//
// Sixteen is audited rather than estimated, and this comment used to say "roughly twenty" —
// #1024's docs criterion, since an estimate invites the reader to assume the sweep was complete.
// #917 converted seven of the sixteen and #1024 converts the remaining nine, in three parts by
// published range: DescribeInstanceStatus, DescribeSpotPriceHistory and DescribeFleets — the
// three whose pages publish no range at all — are the first part. docs/services.md lists the
// rest with the range each publishes.
//
// What AWS publishes about the mechanism is stated once, in Query-Requests.html → Pagination,
// rather than per operation:
//
//	"With pagination, you specify a size for MaxResults and then each call returns 0 to
//	MaxResults items and sets nextToken. If there are additional items to iterate, nextToken is
//	non-null and you can specify its value in the NextToken parameter of a subsequent call to
//	get the next set of items. With pagination, you continue to call the action until nextToken
//	is null, even if you receive less than MaxResults items, including zero items."
//
// Two consequences of that paragraph shape these helpers. The last page must carry **no**
// token, because a caller is told to keep calling until the token is null and would otherwise
// loop forever. And a page may legitimately hold fewer items than asked for, so a short page
// is not itself the end of the listing — which is why the token is emitted from whether a
// further item exists rather than from whether the page filled up.
//
// The token is a plain decimal offset, which is the wire shape both original operations
// already issue. It is deliberately not the base64 form #915 introduced
// ([decodeOffsetPaginationToken]): AWS documents NextToken as opaque, the shape is part of
// every run already recorded against those two operations, and the property base64 buys —
// that a token substrate never issued is detectable — is worth less here than it was at #915's
// four sites, because an invented decimal offset resumes from that offset rather than silently
// restarting the listing. What is refused is a token that is not a non-negative integer, per
// #917's fourth criterion.

// ec2MinUnpublishedMaxResults is the smallest MaxResults substrate accepts at an operation
// whose page publishes no range.
//
// Seven pages publish no bound at all — API_DescribeInstances, API_DescribeImages,
// API_DescribeVolumes and API_DescribeSnapshots of the nine #917 names, plus
// API_DescribeInstanceStatus, API_DescribeSpotPriceHistory and API_DescribeFleets from #1024 —
// each saying only "The maximum number of items to return for this request", type Integer, no
// minimum and no maximum. Three of #917's nine publish "Valid Range: Minimum value of 5. Maximum
// value of 1000." and two state a range in prose, and #1024's remaining six publish three
// further ranges between them. Substrate does not borrow 5–1000 from the siblings, per #671:
// only what the API model states.
//
// The floor of one is therefore **substrate's reading**, and it is the one bound the published
// pagination rule forces. A caller is told to "continue to call the action until nextToken is
// null, even if you receive less than MaxResults items, including zero items", so a request for
// a page of zero items describes a walk that can never advance: every call would answer nothing
// and hand back a token. Refusing it is the only answer that does not invite an infinite loop.
const ec2MinUnpublishedMaxResults = 1

// ec2NoMaxResultsCeiling is the maxResults argument to [ec2MaxResults] meaning that the
// operation's page publishes no maximum, so no upper bound is enforced.
const ec2NoMaxResultsCeiling = 0

// ec2MinPublishedMaxResults and ec2MaxPublishedMaxResults are the range three of the pages #917
// names publish verbatim, as "Valid Range: Minimum value of 5. Maximum value of 1000."
//
// API_DescribeVpcs, API_DescribeSubnets and API_DescribeSecurityGroups carry that line
// identically, and the last states it in prose as well — "This value can be between 5 and 1000.
// If this parameter is not specified, then all items are returned." One pair of constants serves
// all three because the published fact is one fact, not three that happen to agree.
//
// DescribeTags' and DescribeLaunchTemplateVersions' bounds stay their own
// ([ec2MinTagResults], [ec2MinLaunchTemplateVersionResults]): the second publishes 1 to 200 and
// so is a different range, and the first doubles as that operation's default page size, which
// this pair is not. The seven pages publishing no range at all use
// [ec2MinUnpublishedMaxResults] with [ec2NoMaxResultsCeiling] instead.
const (
	ec2MinPublishedMaxResults = 5
	ec2MaxPublishedMaxResults = 1000
)

// ec2MaxResults reads MaxResults against the range minResults..maxResults, where a maxResults
// of [ec2NoMaxResultsCeiling] means the page publishes no maximum.
//
// It returns zero when the parameter is absent, which every caller reads as "no page limit":
// an absent MaxResults means the whole listing, not a default page size. API_DescribeSecurityGroups
// is the one page of the nine that states it outright — "If this parameter is not specified,
// then all items are returned" — and it is also what every converted operation did before it
// paginated, so a caller that sends no MaxResults sees no change. The two operations that
// paginated first keep their own defaults instead, since neither page publishes this sentence
// and both defaults were chosen deliberately.
//
// A value outside the range is refused rather than clamped, which is what both original
// operations did with their own ranges: a caller who asked for 2000 items asked for something
// the operation cannot do, and silently giving 1000 hides that.
func ec2MaxResults(params map[string]string, minResults, maxResults int) (int, *AWSError) {
	raw := params["MaxResults"]
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < minResults || (maxResults != ec2NoMaxResultsCeiling && n > maxResults) {
		message := "MaxResults must be at least " + strconv.Itoa(minResults)
		if maxResults != ec2NoMaxResultsCeiling {
			message = "MaxResults must be between " + strconv.Itoa(minResults) +
				" and " + strconv.Itoa(maxResults)
		}
		return 0, &AWSError{
			Code:       "InvalidParameterValue",
			Message:    message,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return n, nil
}

// ec2NextTokenOffset reads NextToken as the offset substrate issued it as, and returns zero
// when the request names none.
//
// A token that is not a non-negative integer is refused with InvalidParameterValue, because
// substrate cannot have issued it. An offset past the end of the listing is **not** refused —
// [ec2Page] clamps it — since a caller resuming a walk after a resource was deleted has a
// token that was valid when it was issued, and an empty last page is a better answer to that
// than an error.
//
// This runs before any state is read, which is the ordering #887 established and #915's
// sealed-store tests pin: a refusal must not depend on how many resources happen to exist.
func ec2NextTokenOffset(params map[string]string) (int, *AWSError) {
	raw := params["NextToken"]
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "The token '" + raw + "' is invalid",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return n, nil
}

// ec2Page returns the items at offset, at most maxResults of them, and the token for the page
// after it.
//
// A maxResults of zero means no limit, so the whole remainder is returned with no token — the
// unpaginated answer a request naming no MaxResults gets. The token is emitted only when a
// further item exists, so a listing that is an exact multiple of the page size costs no round
// trip to an empty page, and the last page always carries none.
//
// The offset is only meaningful because the items are in a stable order. Every caller builds
// its slice from [StateManager.List], whose lexicographic guarantee (#865) is what makes the
// same offset name the same item on two calls; DescribeTags sorts its own scan instead, because
// it spans resource types and its answer is not in state-key order.
//
// Paging here is a cut of an already-built slice rather than an early exit from the scan, and at
// DescribeSnapshots that is load-bearing rather than incidental: a seeded status progression
// (#715) advances once per observation, so stopping the scan at the page boundary would make a
// countdown advance by an amount that depended on the caller's page size.
func ec2Page[T any](items []T, offset, maxResults int) ([]T, string) {
	if offset > len(items) {
		offset = len(items)
	}
	page := items[offset:]
	if maxResults > 0 && len(page) > maxResults {
		return page[:maxResults], strconv.Itoa(offset + maxResults)
	}
	return page, ""
}

// ec2PageReservations cuts a DescribeInstances answer at an instance boundary rather than a
// reservation one, and returns the token for the page after it.
//
// It exists because that operation is the one converted describe whose answer is nested:
// reservationSet > item > instancesSet, where every other listing is flat and uses [ec2Page].
// MaxResults counts the instances, which is substrate's reading and is argued in
// [EC2Plugin.describeInstances]' doc comment — the page states only "the maximum number of items"
// and never says which of the two lists an item is.
//
// A reservation whose instances straddle the boundary is therefore reported on **both** pages,
// carrying only the instances belonging to each. AWS publishes nothing about that either, and the
// alternative — never splitting one — would have to answer either more items than MaxResults asked
// for or fewer than are available with a token, each of which contradicts the parameter more
// visibly than a reservation ID appearing twice does. A caller assembling instances across pages
// sees each exactly once regardless, which is what the walk is for.
//
// The offset counts instances too, so a token names a position in the flattened sequence: that is
// what NextToken's "Pagination continues from the end of the items returned by the previous
// request" describes. A maxResults of zero means no limit, as everywhere else.
func ec2PageReservations(reservations []ec2ReservationItem, offset, maxResults int) ([]ec2ReservationItem, string) {
	var page []ec2ReservationItem
	seen, taken := 0, 0
	for _, res := range reservations {
		// res is a copy, so trimming its Instances slice header cannot disturb the caller's.
		start := 0
		if offset > seen {
			start = min(offset-seen, len(res.Instances))
		}
		seen += len(res.Instances)
		remaining := res.Instances[start:]
		if len(remaining) == 0 {
			continue
		}
		if maxResults > 0 {
			room := maxResults - taken
			if room <= 0 {
				// A further instance exists, so the page is full and the walk continues. The
				// token is emitted here rather than at the end of the loop for the reason
				// [ec2Page] gives: a listing that is an exact multiple of the page size must
				// end without one.
				return page, strconv.Itoa(offset + taken)
			}
			if len(remaining) > room {
				res.Instances = remaining[:room]
				return append(page, res), strconv.Itoa(offset + taken + room)
			}
		}
		res.Instances = remaining
		page = append(page, res)
		taken += len(remaining)
	}
	return page, ""
}

// ec2RefuseIDsWithMaxResults answers InvalidParameterCombination for a request that names both
// a resource-ID list and MaxResults, where idParam is the list parameter's name and ids is the
// list as extracted from the request.
//
// The rule is published once for the whole service rather than per operation, in
// Query-Requests.html → Pagination: "If you call a describe API action with both a list of IDs
// and MaxResults, the request fails with the error InvalidParameterCombination." Two pages repeat
// it against their own parameter, in the same words — API_DescribeInstances and
// API_DescribeInstanceStatus: "You cannot specify this parameter and the instance IDs parameter in
// the same request." The **code** is therefore published and the **message wording** is
// substrate's, since no page gives one.
//
// This is the refusal that matters most in the divergence direction: without it a request
// combining the two answers 200 here and InvalidParameterCombination at AWS, so the code works
// against the emulator and fails in production.
//
// It is checked before the ID list's own syntax, because whether two parameters may appear
// together does not depend on either one being well formed, and AWS states the rule against the
// call rather than against a value. Which of the two refusals AWS answers first is not
// published, so the ordering is substrate's.
func ec2RefuseIDsWithMaxResults(params map[string]string, idParam string, ids []string) *AWSError {
	if len(ids) == 0 || params["MaxResults"] == "" {
		return nil
	}
	return &AWSError{
		Code:       "InvalidParameterCombination",
		Message:    "The parameter " + idParam + " cannot be used with the parameter MaxResults",
		HTTPStatus: http.StatusBadRequest,
	}
}
