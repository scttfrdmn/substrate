package emulator

// EventBridge Scheduler pages ListSchedules by an array offset, and a token it never issued is
// refused (#1086).
//
// listSchedules carried the pre-#915 idiom — base64.StdEncoding.DecodeString then strconv.Atoi, both
// errors discarded — so a token substrate could not have issued left the offset at zero and the
// operation answered a well-formed **page one**. That is the one wrong answer a paginating caller
// cannot detect: a loop that runs until the token comes back empty is handed the first page again, so
// it either spins or processes the same schedules twice, and nothing in the response says so. The
// decode is now [decodeOffsetPaginationToken], which defines issuability as a round trip through the
// encoder, and the cut is [pageByOffsetToken], which is what the handler had written out by hand.
//
// # What the page publishes, and what is substrate's reading
//
// `API_ListSchedules` describes the parameter as *"The token returned by a previous call to retrieve
// the next set of results."* — so the page states where a token comes from, and a token no previous
// call returned is not the thing the parameter is documented to accept. That sentence is the footing
// for the refusal, not the Errors section.
//
// The code is [schedulerValidation]'s ValidationException/400, glossed *"The input fails to satisfy
// the constraints specified by an AWS service."* and published in this operation's **own** Errors
// section, so it is the page's own vocabulary and not one borrowed from a sibling, which is what
// [#671]'s scope decision requires. It is already this plugin's only refusal for an input that fails
// a constraint, and the message therefore carries which input failed, exactly as every other
// Scheduler refusal does.
//
// What remains substrate's reading is only that this particular input problem is the one that gloss
// covers: the page publishes no code AWS attributes to a pagination token.
//
// The published Length constraint is **1–2048**, which the round trip subsumes: a base64 encoding of
// a decimal offset is far shorter than 2048 bytes, so every token that survives the round trip is
// in-length, and a refusal for length would be unreachable behind it. The minimum of 1 is why an
// empty NextToken is an *absent* token — the start of the listing — rather than an invalid one.
//
// # Ordering
//
// The token is decoded before the name index is read, which is #887's criterion: a refusal must not
// depend on how much state happens to exist, and a store failure must not be reported as a 500 for a
// request that was already refusable. There is no required member on this operation and no schedule
// to resolve, so nothing else competes for precedence — unlike the three Scheduler operations that
// check `Name` first.
//
// # What the refusal does not claim
//
// It refuses a token this service could not have minted. It does **not** make a token portable
// between two listings of *different* shape, because it cannot: the token carries an offset and
// nothing else, so a token issued for one `ScheduleGroup`, `NamePrefix` or `State` decodes cleanly
// against another and indexes into a listing the caller never asked for. Every offset paginator in
// the tree has that property; it is recorded here rather than implied, because the refusal's name
// invites the stronger reading.
//
// # Not fixed here, so that the conversion is not read as having fixed it
//
//   - **The `State` filter is applied after the page is cut**, not before, so a request filtering by
//     state can be answered a page shorter than `MaxResults` while still carrying a `NextToken`. AWS
//     publishes `State` as a filter on the listing, which would put it ahead of the cut. Left as it
//     was, and filed separately: it is a different defect from the token, and fixing it changes which
//     schedules a page contains rather than which tokens are accepted.
//   - **`MaxResults` above the published maximum of 100 is clamped, not refused**, and a value of
//     zero or below is silently ignored in favor of substrate's own default of 20 — see
//     scheduler_query_keys.go. Refusing an out-of-range page size is its own class.
//
// [#671]: https://github.com/scttfrdmn/substrate/issues/671

// schedInvalidPaginationToken refuses a NextToken that no previous ListSchedules call returned.
//
// The message uses the plugin's published validation-error shape, because ValidationException is the
// one refusal this service publishes and every other Scheduler message renders that shape — a caller
// parsing them should not find one exception. The member is spelled `nextToken` for the same reason
// the rest are: that is the form AWS's own validation messages use, whatever case the wire carries.
// The gloss is not restated, because a caller told that the input fails to satisfy a constraint
// cannot tell which input AWS means.
func schedInvalidPaginationToken() *AWSError {
	return schedulerValidation("1 validation error detected: Value at 'nextToken' failed to satisfy " +
		"constraint: Member must be a token returned by a previous ListSchedules request")
}
