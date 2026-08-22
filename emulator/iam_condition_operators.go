package emulator

import (
	"encoding/base64"
	"math"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

// condEvaluate reports whether one request-context value satisfies base, and whether
// base is an operator substrate recognizes at all.
//
// The two answers come from one function on purpose. Three callers need to know
// "recognized" *before* they can use "matched": a set qualifier over an absent key
// returns true vacuously, an `...IfExists` over an absent key returns true by
// definition, and a negated operator over an absent key returns true per AWS's rule
// below — so all three would grant an Allow written with an operator substrate cannot
// evaluate. A second list of operator names would drift from this switch the first
// time one was added to only one of them; [condOperatorRecognized] is derived from
// this function instead.
//
// AWS: "If the key that you specify in a policy condition is not present in the
// request context, the values do not match and the condition is *false*. If the
// policy condition requires that the key is *not* matched, such as StringNotLike or
// ArnNotLike, and the right key is not present, the condition is *true*." That rule
// is applied by [conditionKeyMatches], which is where presence is known; base is
// evaluated here against whatever value was found.
//
// Multiple values for the same key use OR semantics for a positive operator and AND
// semantics for a negated one — the same list read either way round, which is why a
// negated arm returns false on the first match rather than true.
//
// A policy value substrate cannot parse as the operator's type — "ten" under
// NumericEquals, "not-a-date" under DateLessThan — is skipped rather than failing the
// whole condition, so one unparseable element of a list does not decide the answer.
// A *context* value substrate cannot parse fails a positive operator and satisfies a
// negated one, on the reasoning that a comparison that cannot be made has not been
// satisfied. AWS documents neither case, on the policy side or the request side; both
// are substrate's recorded choices (#714).
func condEvaluate(base, ctxVal string, condValues StringOrSlice) (matched, recognized bool) {
	switch base {
	case "StringEquals":
		return condAnyValue(condValues, func(v string) bool { return ctxVal == v }), true
	case "StringNotEquals":
		return !condAnyValue(condValues, func(v string) bool { return ctxVal == v }), true
	case "StringEqualsIgnoreCase":
		return condAnyValue(condValues, func(v string) bool { return strings.EqualFold(ctxVal, v) }), true
	case "StringNotEqualsIgnoreCase":
		return !condAnyValue(condValues, func(v string) bool { return strings.EqualFold(ctxVal, v) }), true
	case "StringLike":
		return condAnyValue(condValues, func(v string) bool { return globMatch(v, ctxVal) }), true
	case "StringNotLike":
		return !condAnyValue(condValues, func(v string) bool { return globMatch(v, ctxVal) }), true

	case "ArnEquals", "ArnLike":
		return condAnyValue(condValues, func(v string) bool { return condARNMatches(v, ctxVal) }), true
	case "ArnNotEquals", "ArnNotLike":
		return !condAnyValue(condValues, func(v string) bool { return condARNMatches(v, ctxVal) }), true

	case "NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals",
		"NumericGreaterThan", "NumericGreaterThanEquals":
		return condNumericMatches(base, ctxVal, condValues), true

	case "DateEquals", "DateNotEquals", "DateLessThan", "DateLessThanEquals",
		"DateGreaterThan", "DateGreaterThanEquals":
		return condDateMatches(base, ctxVal, condValues), true

	case "IpAddress":
		return condAnyValue(condValues, func(v string) bool { return condIPMatches(v, ctxVal) }), true
	case "NotIpAddress":
		return !condAnyValue(condValues, func(v string) bool { return condIPMatches(v, ctxVal) }), true

	case "Bool":
		// AWS publishes only the lowercase forms, and substrate's own producers write
		// them, so the fold buys only a policy that spelled the value "True". It is
		// there because an unquoted JSON `true` and the string "TRUE" are the same
		// assertion by a caller, and [StringOrSlice.UnmarshalJSON] renders the first
		// as "true" — the two forms must not disagree.
		return condAnyValue(condValues, func(v string) bool { return strings.EqualFold(ctxVal, v) }), true

	case "BinaryEquals":
		// AWS: BinaryEquals "compares the value of the specified key byte for byte
		// against a base-64 encoded representation of the binary value in the policy",
		// and its own example table compares two base64 strings. So the comparison is
		// on the encoded text, and a policy value that is not valid base64 encodes no
		// binary value and cannot match. No substrate producer writes a binary context
		// key, so this operator is reachable only through a simulation's ContextEntries.
		return condAnyValue(condValues, func(v string) bool {
			_, err := base64.StdEncoding.DecodeString(v)
			return err == nil && ctxVal == v
		}), true

	case "Null":
		// Null is the one operator that must see an absent key rather than being told
		// about it, which is why [conditionKeyMatches] routes it past the presence
		// check. AWS: "use either true (the key doesn't exist — it is null) or false
		// (the key exists and its value is not null)" — so a key present with an empty
		// value is null, the reading substrate has always had here.
		isNull := ctxVal == ""
		return condAnyValue(condValues, func(v string) bool {
			return (strings.ToLower(v) == "true") == isNull
		}), true
	}

	return false, false
}

// condAnyValue reports whether any of the policy's values satisfies match.
//
// Every operator is expressed through it, including the negated ones — a negated
// operator is the same scan with the answer inverted, which is what makes AWS's "the
// ArnNotEquals and ArnNotLike condition operators behave identically" to the negation
// of their positive pair true by construction rather than by two parallel loops.
func condAnyValue(condValues StringOrSlice, match func(string) bool) bool {
	for _, v := range condValues {
		if match(v) {
			return true
		}
	}
	return false
}

// condOperatorRecognized reports whether base is an operator [condEvaluate] evaluates.
//
// It asks condEvaluate itself, with no value and no policy values, rather than
// carrying a second list of names: a list would answer "no" for an operator added to
// the switch and forgotten here, and that answer is what turns a vacuous set-qualifier
// truth into a grant. See [condEvaluate].
func condOperatorRecognized(base string) bool {
	_, recognized := condEvaluate(base, "", nil)
	return recognized
}

// condOperatorNegated reports whether base is one of AWS's negated operators — the
// ones whose answer is true when the key it names is absent.
//
// AWS: "If the policy condition requires that the key is *not* matched, such as
// StringNotLike or ArnNotLike, and the right key is not present, the condition is
// *true*."
//
// Bool is not in this set even though "false" reads like a negation: it is a positive
// comparison against the value false, and AWS's own table answers "No match" for an
// absent aws:SecureTransport under `"Bool": {"aws:SecureTransport": "false"}`.
// TestEvaluate_ConditionNegatedOperatorsAreTheNotNames pins this list against
// [condEvaluate]'s switch.
func condOperatorNegated(base string) bool {
	switch base {
	case "StringNotEquals", "StringNotEqualsIgnoreCase", "StringNotLike",
		"ArnNotEquals", "ArnNotLike", "NumericNotEquals", "DateNotEquals", "NotIpAddress":
		return true
	}
	return false
}

// condARNComponents is the number of colon-delimited components in an ARN:
// arn:partition:service:region:account-id:resource.
const condARNComponents = 6

// condARNMatches reports whether an ARN condition value matches an ARN from the
// request context.
//
// AWS: "Each of the six colon-delimited components of the ARN is checked separately
// and each can include multi-character match wildcards (*) or single-character match
// wildcards (?)." Substrate ran one [globMatch] over the whole string, so a `*` in one
// component could match across a colon into the next: `arn:aws:sns:*:topic` matched
// `arn:aws:sns:us-east-1:123456789012:topic`, letting a pattern that names five
// components authorize a resource whose account it never mentioned (#714).
//
// The resource component may itself contain colons — an SNS subscription ARN is
// arn:aws:sns:region:account:topic:sub-id — so the split is bounded at six and the
// remainder stays in the last component, where AWS's own wildcard examples put it.
//
// A pattern naming fewer than six components has the missing trailing ones filled with
// `*`. That is substrate's reading, not a documented rule for conditions: AWS states it
// for a Resource element ("When you specify an incomplete ARN … AWS automatically
// completes the ARN by adding wildcard characters (*) to all missing fields") and says
// nothing about an incomplete ARN in a condition. Refusing to match one instead would
// narrow both an Allow and a *Deny* written as `arn:aws:ec2:us-east-1:*`, and silently
// narrowing a Deny is the direction that cannot be recovered from. A *context* value
// with fewer than six components is not padded — it is the request's own ARN, and a
// short one is malformed rather than abbreviated.
func condARNMatches(pattern, value string) bool {
	valueParts := strings.SplitN(value, ":", condARNComponents)
	patternParts := strings.SplitN(pattern, ":", condARNComponents)
	for len(patternParts) < len(valueParts) {
		patternParts = append(patternParts, "*")
	}
	if len(patternParts) != len(valueParts) {
		return false
	}
	for i := range patternParts {
		if !globMatch(patternParts[i], valueParts[i]) {
			return false
		}
	}
	return true
}

// condNumericMatches evaluates the six numeric operators.
//
// AWS: numeric operators "restrict access based on comparing a key to an integer or
// decimal value". Both sides are read as decimals, so a policy that quotes its number
// and one that does not — legal IAM either way, see [StringOrSlice.UnmarshalJSON] —
// compare identically.
func condNumericMatches(base, ctxVal string, condValues StringOrSlice) bool {
	ctxNum, ok := condParseNumber(ctxVal)
	if !ok {
		return condOperatorNegated(base)
	}
	return condCompare(base, condValues, func(v string) (int, bool) {
		n, ok := condParseNumber(v)
		if !ok {
			return 0, false
		}
		switch {
		case ctxNum < n:
			return -1, true
		case ctxNum > n:
			return 1, true
		default:
			return 0, true
		}
	})
}

// condDateMatches evaluates the six date operators.
//
// AWS: "You must specify date/time values with one of the W3C implementations of the
// ISO 8601 date formats or in epoch (UNIX) time." Either notation is accepted on either
// side, so the two can be compared against each other; see [condParseTime].
func condDateMatches(base, ctxVal string, condValues StringOrSlice) bool {
	ctxTime, ok := condParseTime(ctxVal)
	if !ok {
		return condOperatorNegated(base)
	}
	return condCompare(base, condValues, func(v string) (int, bool) {
		t, ok := condParseTime(v)
		if !ok {
			return 0, false
		}
		return ctxTime.Compare(t), true
	})
}

// condCompare applies one of the six ordered comparisons to every policy value.
//
// cmp reports how the context value orders against one policy value (-1, 0, 1) and
// whether that value could be parsed at all. It is shared by the numeric and date
// families because the six operator suffixes mean the same thing in both, and writing
// them twice would let the two families disagree about, say, whether LessThanEquals
// includes equality.
func condCompare(base string, condValues StringOrSlice, cmp func(string) (int, bool)) bool {
	// A negated operator is satisfied unless some value matches, so its scan cannot
	// short-circuit on the first parse failure the way a positive one does.
	if strings.HasSuffix(base, "NotEquals") {
		for _, v := range condValues {
			if c, ok := cmp(v); ok && c == 0 {
				return false
			}
		}
		return true
	}
	for _, v := range condValues {
		c, ok := cmp(v)
		if !ok {
			continue
		}
		switch {
		case strings.HasSuffix(base, "LessThanEquals"):
			if c <= 0 {
				return true
			}
		case strings.HasSuffix(base, "GreaterThanEquals"):
			if c >= 0 {
				return true
			}
		case strings.HasSuffix(base, "LessThan"):
			if c < 0 {
				return true
			}
		case strings.HasSuffix(base, "GreaterThan"):
			if c > 0 {
				return true
			}
		default: // Equals
			if c == 0 {
				return true
			}
		}
	}
	return false
}

// condParseNumber parses one side of a numeric comparison.
//
// [strconv.ParseFloat] accepts three spellings that are not "an integer or decimal
// value": "NaN", "Inf" (so a policy value of "Inf" would satisfy NumericGreaterThan
// against every number in existence) and Go's hexadecimal float syntax, where "0x1p4"
// would compare equal to 16. The first two are caught by the IsInf/IsNaN checks below;
// the character filter is what catches the third, since a hex float parses to a perfectly
// finite number.
func condParseNumber(s string) (float64, bool) {
	if strings.ContainsAny(s, "xXpPnNiI") {
		return 0, false
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
		return 0, false
	}
	return n, true
}

// condTimeLayouts are the W3C ISO 8601 profile's date and date-time forms, longest
// first.
//
// The profile's complete forms carry a timezone designator; the two without one are a
// tolerance, read as UTC, because a policy author writing a bare local timestamp means
// a time rather than nothing at all.
var condTimeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02",
	"2006-01",
	"2006",
}

// condParseTime parses a W3C ISO 8601 date/time or an epoch (UNIX) timestamp.
//
// ISO forms are tried first, which is what settles the one ambiguity between the two
// notations: "2020" is a legal W3C year and a legal epoch second, and reading it as the
// year is the reading a policy author writing a date means. An epoch value large enough
// to be a real timestamp matches no ISO layout, so nothing else is affected. AWS
// documents both notations and no rule for choosing between them; this is substrate's
// recorded choice (#714).
//
// Epoch values may be fractional — aws:EpochTime is documented as seconds since
// 1970-01-01, and a sub-second value is still a time.
func condParseTime(s string) (time.Time, bool) {
	for _, layout := range condTimeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	if secs, ok := condParseNumber(s); ok {
		whole, frac := math.Modf(secs)
		return time.Unix(int64(whole), int64(frac*float64(time.Second))).UTC(), true
	}
	return time.Time{}, false
}

// condIPMatches reports whether an IP from the request context lies in the range a
// policy value names.
//
// AWS: "The value must be in the standard CIDR format (for example, 203.0.113.0/24 or
// 2001:DB8:1234:5678::/64). If you specify an IP address without the associated routing
// prefix, IAM uses the default prefix value of /32."
//
// A bare address is read as a single host — /32 for IPv4, as AWS says, and /128 for
// IPv6, which AWS's sentence does not cover. Taking "/32" literally for an IPv6
// address would turn one address into 2^96 of them, so the host route is read as the
// address family's own full width. That is substrate's reading of a sentence written
// for IPv4.
//
// A malformed policy value matches nothing, and a context value that is not an address
// matches nothing. Neither case is documented by AWS; the reasoning is the one
// [s3SourceIPValuePinsAccess] records — a value that is not an address pins no range —
// and it keeps NotIpAddress from being satisfied by a typo.
func condIPMatches(pattern, value string) bool {
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return false
	}
	prefix, err := netip.ParsePrefix(pattern)
	if err != nil {
		host, hostErr := netip.ParseAddr(pattern)
		if hostErr != nil {
			return false
		}
		prefix = netip.PrefixFrom(host, host.BitLen())
	}
	return prefix.Contains(addr)
}

// condIfExistsSuffix is the suffix AWS allows on every operator but Null.
const condIfExistsSuffix = "IfExists"

// condSplitIfExists separates an ...IfExists suffix from the operator it modifies.
//
// AWS: "You can add IfExists to the end of any condition operator name except the Null
// condition — for example, StringLikeIfExists." The exception is enforced by the
// caller: "NullIfExists" splits here and is then refused, because Null already tests
// for the absence IfExists would excuse.
func condSplitIfExists(operator string) (base string, ifExists bool) {
	return strings.CutSuffix(operator, condIfExistsSuffix)
}

// condAbsentMatches reports whether an operator is satisfied by a key the request
// context does not carry.
//
// Three rules meet here, and an unrecognized operator must not benefit from any of
// them — an Allow conditioned on an operator substrate cannot evaluate must not be
// granted merely because the key it names is missing, which is the shape of the hole
// #714 was filed about.
//
//   - ...IfExists: "If the key is not present, evaluate the condition element as true."
//   - A negated operator: "If the policy condition requires that the key is not
//     matched … and the right key is not present, the condition is true."
//   - Anything else: "the values do not match and the condition is false."
func condAbsentMatches(base string, ifExists bool) bool {
	if !condOperatorRecognized(base) {
		return false
	}
	return ifExists || condOperatorNegated(base)
}
