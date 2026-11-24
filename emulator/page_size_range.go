package emulator

import "fmt"

// A published page-size range, and the one rule for a value outside it (#1131, #1185, #1161).
//
// Eight JSON list handlers in four services (Secrets Manager, Step Functions, Systems Manager and
// KMS) used one idiom: `if input.MaxResults <= 0 { input.MaxResults = <n> }`, and nothing compared
// the value against a maximum. So an explicit 0 or -1 was answered with substrate's default page, a
// value above the published maximum was honored, and KMS ListAliases' default was 100 where its page
// publishes 50 — two pages from KMS for sixty aliases, one from substrate, and the caller's token loop
// was never entered.
//
// **The rule is to refuse a value outside the published range**, as [elbPageSize] does and for the
// reason it records: an operation's Errors section links a Common Errors page as its own, so a
// validation code published there is that operation's vocabulary rather than one borrowed from a
// sibling, which is the distinction #671's scope decision turns on (#1064). The code is each
// service's, chosen at the call site, because they do not share one:
//
//   - Secrets Manager `ListSecrets` publishes InvalidParameterException/400 on its own page, glossed
//     "The parameter name or value is invalid."
//   - Step Functions `ListExecutions` publishes ValidationException/400 on its own page; the plugin's
//     other two list operations publish only InvalidToken and take ValidationError/400 from the
//     common list, as [sfnInvalidBody] does.
//   - Systems Manager and KMS take ValidationError/400 from their Common Errors pages ("The input
//     doesn't meet the required format or constraints."), the reading [ssmInvalidBody] and
//     [kmsInvalidPendingWindow] already record.
//
// No page in the set publishes a clamp, and none says a value above the maximum is honored, so
// neither is substrate's to choose. The messages are substrate's own; AWS publishes none for a range
// violation on these pages.
//
// **An absent member is not an out-of-range one.** The member is decoded as a pointer, so an omitted
// `MaxResults` takes the default while an explicit 0 is judged against the minimum. That is the
// difference the old idiom erased, and it is what keeps every recorded request that omits the member
// replaying byte-identically: only a request that sent an out-of-range value answers differently.
//
// **A published "0 uses the default" is honored, not refused.** The three Step Functions pages give
// `maxResults` "Valid Range: Minimum value of 0. Maximum value of 1000." and say "The default is 100
// and the maximum allowed page size is 1000. A value of 0 uses the default." So 0 is legal there and
// means the default, which is what the old idiom did — those sites were already conformant at the low
// end and are recorded with [pageSizeRange.zeroIsDefault] so that a later sweep does not "fix" them
// into a divergence. Only a negative value, or one above 1000, is refused.

// pageSizeRange is one operation's published page-size constraint.
type pageSizeRange struct {
	// member is the wire spelling of the page-size member, named in the refusal message.
	member string
	// min and max are the published Valid Range, inclusive.
	min, max int
	// def is the page size an absent member gets: the published default, or substrate's own where the
	// page publishes none, which each call site says.
	def int
	// zeroIsDefault records a page that publishes "a value of 0 uses the default".
	zeroIsDefault bool
}

// resolve returns the page size a request's member asks for, or false when the value is outside the
// published range.
func (r pageSizeRange) resolve(v *int) (int, bool) {
	if v == nil {
		return r.def, true
	}
	if *v == 0 && r.zeroIsDefault {
		return r.def, true
	}
	if *v < r.min || *v > r.max {
		return 0, false
	}
	return *v, true
}

// outOfRange renders the refusal message for a value outside the range. The wording is substrate's.
func (r pageSizeRange) outOfRange(v int) string {
	return fmt.Sprintf("%s must be between %d and %d, was %d", r.member, r.min, r.max, v)
}

// check resolves v as [pageSizeRange.resolve] does and renders the refusal for a value outside the
// range through refuse, the constructor of the calling service's published code.
func (r pageSizeRange) check(v *int, refuse func(message string) *AWSError) (int, *AWSError) {
	n, ok := r.resolve(v)
	if !ok {
		return 0, refuse(r.outOfRange(*v))
	}
	return n, nil
}
