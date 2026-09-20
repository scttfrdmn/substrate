package emulator

// Batch pages its three resource describes by an array offset, and a token it never issued is
// refused (#1086).
//
// This is the last of the fifteen sites in that class, and the only one where the decode lived inside
// a shared helper: [batchPage] was called by `describeBatchResources`, which all three of
// `DescribeComputeEnvironments`, `DescribeJobQueues` and `DescribeJobDefinitions` route through. The
// helper carried the pre-#915 idiom — `base64.StdEncoding.DecodeString` then `strconv.Atoi`, both
// errors discarded — so a token substrate could not have issued left the offset at zero and every one
// of the three answered a well-formed **page one**. That is the one wrong answer a paginating caller
// cannot detect: a loop that runs until the token comes back empty is handed the first page again, so
// it either spins or describes the same resources twice, and nothing in the response says so.
//
// # What the pages publish, and what is substrate's reading
//
// All three pages describe `nextToken` in identical words: *"The `nextToken` value returned from a
// previous paginated `Describe…` request where `maxResults` was used and the results exceeded the
// value of that parameter."* — so each page states where a token comes from, and names its **own**
// operation as the source. A token no previous call to that operation returned is not the thing the
// parameter is documented to accept. That sentence is the footing for the refusal.
//
// The same paragraph adds *"Treat this token as an opaque identifier that's only used to retrieve the
// next items in a list and not for other programmatic purposes."* — which is addressed to the caller
// rather than to the service, so it is not itself a refusal rule; it is quoted here because it is the
// page's own statement that the token's contents are not a caller-constructible value.
//
// The code is [batchClientError]'s `ClientException`/400, published in all three operations' own
// Errors sections and glossed *"These errors are usually caused by a client action. … Another cause is
// specifying an identifier that's not valid."* Each page publishes exactly two errors —
// `ClientException`/400 and `ServerException`/500 — and Batch publishes no common-errors page, so
// those two are the whole published vocabulary and there is nothing to borrow from a sibling, which is
// what [#671]'s scope decision requires. What remains substrate's reading is only that a token is one
// of the identifiers that gloss covers: the page never joins the two sentences.
//
// No page publishes a Length or Pattern constraint on `nextToken`, so unlike KMS's 1–1024 there is no
// ceiling for the refusal to subsume and the issuability round trip is the entire rule. An empty
// `nextToken` is an *absent* token — the start of the listing — rather than an invalid one, because
// that is what an omitted optional member means.
//
// # Ordering
//
// The decode has moved **out** of the shared helper and up into each of the three handlers, which is
// #887's criterion: a refusal must not depend on how much state happens to exist, and a store failure
// must not be reported as a 500 for a request that was already refusable. Inside the helper it could
// not satisfy that for all three, because `DescribeJobDefinitions` loads the job-definition index
// *before* it calls the helper, to expand a `jobDefinitionName` into its revisions — so a decode in
// the helper would have sat below a state read on exactly one of the three, and only for one shape of
// request. Three call sites of two lines each is the price of the refusal being unconditional.
//
// The body decode keeps its precedence: a request whose JSON does not parse cannot have a token read
// out of it, so `ClientException` for an unreadable body is answered first. Both refusals carry the
// same published code, which is why the message is the only thing that distinguishes them.
//
// # What the refusal does not claim
//
// It refuses a token this service could not have minted. It does **not** make a token portable
// between the three operations, or between two calls to one operation with different filters: the
// token carries an offset and nothing else, so a `DescribeJobQueues` token decodes cleanly under
// `DescribeComputeEnvironments` and indexes into a listing the caller never asked for. That is why the
// message names the operation. Every offset paginator in the tree has the property; it is recorded
// rather than implied, because the refusal's name invites the stronger reading.
//
// A past-the-end offset still clamps to a final empty page rather than being refused, because a token
// substrate issued over a listing that has since shrunk is still a token it issued.
//
// `ListJobs` is the fourth Batch paginator and the fourth caller of [batchDecodeNextToken], which it
// became in #1236 — where the rest of that operation's request members are argued. Until then it was
// recorded below as "not fixed here", which is what the bullet was for.
//
// # Not fixed here, so that the conversion is not read as having fixed it
//
//   - **`maxResults` outside the published range of 1–100 is clamped, not refused.** All three pages
//     publish *"This value can be between 1 and 100"* and *"If this parameter isn't used, then
//     Describe… returns up to 100 results"*, so 100 is the published default for an absent value;
//     applying it to a zero or negative one as well is substrate's reading, and refusing an
//     out-of-range page size is its own class.
//   - **`DescribeJobDefinitions` applies its `status` filter after the page is cut**, which
//     [batchFilterByStatus]'s doc comment already records as substrate's reading of the page's
//     ordering. Unchanged.
//
// [#671]: https://github.com/scttfrdmn/substrate/issues/671

// batchDecodeNextToken resolves a describe's nextToken to an array offset, refusing one that no
// previous call to operation returned.
//
// It is called by each of the three describes rather than by the helper they share, so that the
// refusal sits above every state read on all three — see the ordering argument above. operation is the
// wire name of the caller, because a token is only honorable at the operation that issued it and the
// message has to say which one that was.
func batchDecodeNextToken(operation, nextToken string) (int, *AWSError) {
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return 0, batchClientError("nextToken is not a token returned by a previous " + operation + " request")
	}
	return offset, nil
}
