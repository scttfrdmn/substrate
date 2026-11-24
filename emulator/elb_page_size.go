package emulator

import "strconv"

// One `PageSize` rule for both Elastic Load Balancing generations — #1150.
//
// Every paginated ELB operation, in both generations, publishes the same parameter under the same
// constraint: "PageSize … Type: Integer. Valid Range: Minimum value of 1. Maximum value of 400."
// Substrate answered it two ways. Classic `DescribeLoadBalancers` refused a value outside the range;
// ELBv2 `DescribeAccountLimits` substituted its default and answered 200. Same plugin, same signing
// name, same published range, two answers — which meant a harness could ask one operation for a page
// size real AWS refuses, read a 200, and conclude the value was legal.
//
// **The rule is to refuse**, and this file is the one place it is written.
//
// The fallback was not an oversight: `elbAccountLimitsPageSize` argued for it in writing, and the
// deciding step was that `DescribeAccountLimits` publishes no operation-specific error — its Errors
// section is Common Errors only, on both generations' pages — so refusing looked like inventing a
// code. #1064 removed that step. The consolidated Query-protocol Common Errors page that each
// operation's Errors section links as *its own* publishes `ValidationError` at 400 ("The input fails
// to satisfy the constraints specified by an AWS service."), and a code on the page an operation
// links is that operation's own vocabulary rather than a borrowing from a sibling — the distinction
// #671's scope decision turns on. So "Common Errors only" is where `ValidationError` comes from, not
// a reason there is nothing to answer.
//
// The second half of that argument was that because the default equals the published maximum, a
// `PageSize` above 400 is answered indistinguishably from a clamp to 400. True, and no defense of
// the low end: a `PageSize` of 0 or -1 was answered with 400 items, which is not a clamp of any kind
// but the largest page a caller can get in response to asking for the smallest.
//
// **The family survey is still the context this decision sits in, and it is genuinely split.** The
// Query-protocol family these operations belong to — RDS's and ElastiCache's `MaxRecords`,
// CloudWatch's — took any positive integer and quietly substituted its default for anything else,
// enforcing no maximum, until #913 made both refuse an out-of-range `MaxRecords` with
// `InvalidParameterValue`. EC2's `DescribeTags` refuses a `MaxResults` outside 5–1000 with the same
// code. So the tree's Query paginators now refuse, and ELB was the outlier rather than the precedent.
// The code differs because the page does: `InvalidParameterValue` is what RDS and ElastiCache
// publish, and `ValidationError` is what ELB's Common Errors page publishes.
//
// **Where the rule applies.** The plugin has six paginated operations — ELBv2 `DescribeLoadBalancers`,
// `DescribeTargetGroups`, `DescribeListeners`, `DescribeRules` and `DescribeAccountLimits`, plus
// classic `DescribeLoadBalancers` — each publishing `PageSize` 1–400 and `Marker`, and all six read
// `PageSize` through [elbPageSize]. The five resource describes read `Marker` through [elbMarker], a
// value cursor over the listing's ARN (ELBv2) or name (classic), because their listings change
// between pages. `DescribeAccountLimits` lists a fixed set and reads it through [elbPageCursor], an
// offset. ELBv2 `DescribeTags` publishes no pagination member and is unpaginated.

// elbMinPageSize and elbMaxPageSize are the published bounds of `PageSize`, and elbDefaultPageSize is
// the page size an absent member gets.
//
// AWS publishes the range on every paginated operation in both generations, in two spellings of one
// number: ELBv2's parameter tables give "Valid Range: Minimum value of 1. Maximum value of 400", and
// classic `DescribeLoadBalancers` gives "The maximum number of results to return with this call (a
// number from 1 to 400). The default is 400". The default is therefore the maximum — the one place
// the classic page is the more explicit of the two — so an unparameterized call answers everything in
// one page and reports no `NextMarker`, which is what the AWS CLI reference's own example output for
// `describe-account-limits` shows.
const (
	elbMinPageSize     = 1
	elbMaxPageSize     = 400
	elbDefaultPageSize = elbMaxPageSize
)

// elbPageSize resolves a request's `PageSize` against its published range, for either generation.
//
// An absent member is the published default. Anything else must be an integer within 1–400
// inclusive; the ends are accepted, because narrowing a published range would be substrate inventing
// a contract. A value outside it, or one that is not a number at all, is refused with
// `ValidationError`/400 and a message naming the range — see this file's comment for why refusing is
// the rule and where the code comes from.
func elbPageSize(raw string) (int, *AWSError) {
	if raw == "" {
		return elbDefaultPageSize, nil
	}
	size, err := strconv.Atoi(raw)
	if err != nil {
		return 0, elbValidationError("PageSize '%s' is not a number", raw)
	}
	if size < elbMinPageSize || size > elbMaxPageSize {
		return 0, elbValidationError("PageSize must be a number from %d to %d; the supplied value is %d",
			elbMinPageSize, elbMaxPageSize, size)
	}
	return size, nil
}

// elbPageCursor resolves `DescribeAccountLimits`' `Marker` and `PageSize`, whose listing is a fixed
// set and so pages by offset (#1245).
//
// The `PageSize` half is [elbPageSize]. The `Marker` half is [decodeOffsetPaginationToken], and a
// `Marker` substrate could not have issued — not base64, not an offset, a negative one — is refused
// with `ValidationError`/400, the consolidated Query Common Errors page's code (#1064); the message is
// substrate's own. An offset past the end is an empty last page, which [pageByOffsetToken]
// implements. Both members are checked before any state is read.
func elbPageCursor(req *AWSRequest) (offset, pageSize int, awsErr *AWSError) {
	pageSize, awsErr = elbPageSize(req.Params["PageSize"])
	if awsErr != nil {
		return 0, 0, awsErr
	}
	offset, ok := decodeOffsetPaginationToken(req.Params["Marker"])
	if !ok {
		return 0, 0, elbValidationError("Marker '%s' is not a marker this service issued",
			req.Params["Marker"])
	}
	return offset, pageSize, nil
}
