package emulator

import "net/http"

// CloudWatch Logs pages four operations by an array offset, and a token it never issued is refused
// (#1086).
//
// DescribeLogGroups, DescribeLogStreams, GetLogEvents and FilterLogEvents each carried its own copy
// of the pre-#915 idiom — base64.StdEncoding.DecodeString then strconv.Atoi, both errors discarded —
// so a token substrate could not have issued left the offset at zero and the operation answered a
// well-formed **page one**. That is the one wrong answer a paginating caller cannot detect: a loop
// that runs until the token comes back empty is handed the first page again, so it either spins or
// processes the same records twice, and nothing in the response says so. All four now share
// [decodeOffsetPaginationToken], which defines issuability as a round trip through the encoder, and
// [pageByOffsetToken], which was already the cut each had written out by hand.
//
// Four copies of one block is the largest concentration in this class, and it is why the conversion
// is worth more here than the count of operations suggests: a fix applied to one of them would have
// left the other three answering page one.
//
// # What the pages publish, and what is substrate's reading
//
// All four pages describe the request parameter identically — *"The token for the next set of items
// to return. (You received this token from a previous call.)"*, with FilterLogEvents saying "events"
// where the others say "items". The parenthesis is the whole footing for the refusal: it states where
// a token comes from, so a token no previous call returned is not the thing the parameter is
// documented to accept.
//
// The code is [cwLogsInvalidPaginationToken]'s InvalidParameterException/400, glossed *"A parameter
// is specified incorrectly."* and published in **all four** operations' own Errors sections — so it
// is each operation's own vocabulary and not one borrowed from a sibling, which is what [#671]'s
// scope decision requires. It is already this plugin's code for a malformed input at every other
// door it has.
//
// What remains substrate's reading is only that this particular input problem is the one that gloss
// covers: no page publishes a code AWS attributes to a pagination token.
//
// The published Length constraint is **minimum 1 and no maximum** — no Pattern either — so unlike
// Athena's and KMS's 1–1024 there is no ceiling for the refusal to subsume, and the round trip is
// the entire rule. The minimum of 1 is why an empty nextToken is an *absent* token, the start of the
// listing, rather than an invalid one.
//
// # Ordering
//
// The token is decoded before any state is read, which is #887's criterion: a refusal must not depend
// on how much state happens to exist, and a store failure must not be reported as a 500 for a request
// that was already refusable. Three of the four require a member first — logGroupName, and
// logStreamName as well at GetLogEvents — and that refusal keeps its precedence, because an absent
// required member is the more basic failure and neither ordering is published. Both answer the same
// published code, so the only thing the choice changes is the message.
//
// None of the four resolves the log group, so substrate has no ResourceNotFoundException at these
// doors whose precedence would have to be preserved; that absence is its own divergence, recorded
// below rather than fixed here.
//
// # Not fixed here, so that the conversion is not read as having fixed it
//
//   - **The published 24-hour expiry is not modeled.** All four response elements say *"The token
//     expires after 24 hours."* and no page publishes a code for presenting an expired one, so a
//     token substrate issues stays valid for the life of the store. Refusing an expired token would
//     need the token to carry its issue time and the simulated clock to judge it — a different
//     change, and one that needs a code decision this one does not.
//   - **GetLogEvents' two tokens.** AWS publishes nextBackwardToken and nextForwardToken, says *"The
//     returned tokens are never null"*, and documents termination as the returned token being equal
//     to the one passed in. Substrate emits nextForwardToken only when a further page exists and
//     never emits nextBackwardToken at all, so a caller following that documented rule cannot
//     terminate and has to use the empty-token rule instead. Left as it was.
// **ResourceNotFoundException** was one of these: published on three of the four pages (every one but
// DescribeLogGroups) and answered by none of them, while the sites elsewhere in the plugin that did
// answer it used 404 where every page publishes 400. It was fixed in #1224 — see
// cloudwatchlogs_not_found.go, which is where the argument for the refusal and for its precedence
// against the token refusal above now lives. Until then it was recorded here as "not fixed here",
// which is what this heading is for.
//
// [#671]: https://github.com/scttfrdmn/substrate/issues/671

// cwLogsDescribeDefaultLimit is the page size the two describes apply when limit is absent.
//
// Unlike Athena's, this figure is published: both pages say *"The maximum number of items returned.
// If you don't specify a value, the default is up to 50 items."*, which is also the maximum of their
// Valid Range of 1–50. Named rather than repeated as a literal so the two sites cannot drift.
const cwLogsDescribeDefaultLimit = 50

// cwLogsEventsDefaultLimit is the page size the two event readers apply when limit is absent.
//
// Also published, though only FilterLogEvents states it flatly (*"The default is 10,000 events."*);
// GetLogEvents publishes *"the default is as many log events as can fit in a response size of 1 MB
// (up to 10,000 log events)"*, and substrate models no response-size ceiling, so it applies the
// count alone. Both pages give limit a Valid Range of 1–10000.
const cwLogsEventsDefaultLimit = 10000

// cwLogsInvalidPaginationToken refuses a nextToken that no previous call to operation returned.
//
// The operation name is in the message because a token from one of the *other three* Logs paginators
// is one of the ways a caller arrives here: all four encode an offset the same way, so
// DescribeLogGroups' token decodes cleanly under FilterLogEvents and would otherwise index into the
// wrong listing. The member is spelled in the message in the lowercase the wire uses. The published
// gloss is not restated, because a caller told that a parameter is specified incorrectly cannot tell
// which parameter AWS means, and nextToken is the only one this refusal can be about.
func cwLogsInvalidPaginationToken(operation string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "nextToken is not a token returned by a previous " + operation + " request",
		HTTPStatus: http.StatusBadRequest,
	}
}
