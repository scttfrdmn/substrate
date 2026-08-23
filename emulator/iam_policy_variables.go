package emulator

import "strings"

// policyVarOpen and policyVarClose delimit a policy variable.
//
// AWS: "Variables are marked using a $ prefix followed by a pair of curly braces
// ({ }) that include the variable name of the value from the request" — so the closing
// brace is a byte rather than a string, since that is how it is scanned for.
const (
	policyVarOpen  = "${"
	policyVarClose = '}'
)

// policyVarVersion is the policy language version that introduced variables.
//
// AWS: "In order to use policy variables, you must include the Version element in a
// statement, and the version must be set to a version that supports policy variables.
// Variables were introduced in version 2012-10-17. Earlier versions of the policy
// language don't support policy variables. If you don't include the Version element
// and set it to an appropriate version date, variables like ${aws:username} are
// treated as literal strings in the policy" — which is the reading substrate had for
// every document before #745, and keeps for a document that predates the element.
const policyVarVersion = "2012-10-17"

// policyVarsSupported reports whether a document's Version admits variables.
//
// The comparison is lexical rather than an equality test against the one version
// that exists today, because both published versions are dates and a third would
// sort after this one. A document with no Version element compares below it and is
// read literally, which is what AWS's sentence above says to do.
func policyVarsSupported(version string) bool {
	return version >= policyVarVersion
}

// policyVars is what a statement's ${…} variables resolve against.
//
// The context is the request's *single-valued* map alone. AWS: "You can use any
// single-valued condition key as a variable. You can't use a multivalued condition
// key as a variable" — so MultiContext is deliberately not consulted here, unlike in
// [condContextValue], where a multivalued key is a legitimate fallback for the
// operator that reads it.
type policyVars struct {
	// ctx is the request's single-valued condition context.
	ctx map[string]string

	// enabled is false for a document whose Version predates variables, and for
	// every element AWS does not substitute in.
	enabled bool
}

// resolve substitutes text's variables, or returns it verbatim when the document
// this statement came from does not admit variables.
func (v policyVars) resolve(text string) (policyPattern, bool) {
	if !v.enabled {
		return rawPolicyPattern(text), true
	}
	return substitutePolicyVariables(text, v.ctx)
}

// policyPattern is a policy string with its ${…} variables resolved, carrying which
// of its bytes are wildcards and which are literal text.
//
// Those are not the same question once a variable resolves. A `*` or `?` is a
// wildcard because the *policy author* wrote one; a `*` that arrived from a tag value,
// or from the `${*}` escape whose entire purpose is to denote a literal asterisk, is
// text. A plain string cannot hold that distinction, which is why substitution
// produces this instead: substituting `${*}` and handing the result to [globMatch]
// would turn AWS's escape for a literal `*` into a wildcard, widening the statement —
// the one direction a deliberate escape must never take (#745).
//
// A substituted *value* is literal for the same reason pointed the other way. AWS
// documents no rule for a resolved value that itself contains a `*`, so substrate
// reads it as text: a tag value is data the request carried, not pattern the policy
// author wrote, and reading it as pattern is the reading that can only widen.
type policyPattern struct {
	// text is the pattern after substitution.
	text string

	// literal reports, per byte of text, whether that byte matches itself even when
	// it is `*` or `?`.
	//
	// nil means no byte is exempt, which is every policy string containing no `${`
	// — so such a pattern matches exactly as it did before #745 and allocates
	// nothing for the distinction.
	literal []bool
}

// rawPolicyPattern is text as written, with every `*` and `?` a wildcard.
func rawPolicyPattern(text string) policyPattern {
	return policyPattern{text: text}
}

// matches reports whether p covers value under AWS's glob rules.
func (p policyPattern) matches(value string) bool {
	return globMatchMask(p.text, p.literal, value)
}

// splitN splits p at sep into at most n pieces, keeping each piece's mask aligned
// with its text.
//
// It exists for [condARNMatches], which compares an ARN component by component. A
// separator inside a substituted value is split on like any other: AWS's rule is
// about the six colon-delimited components of the pattern, and the pattern is what
// this is.
func (p policyPattern) splitN(sep string, n int) []policyPattern {
	parts := strings.SplitN(p.text, sep, n)
	out := make([]policyPattern, 0, len(parts))
	offset := 0
	for _, part := range parts {
		piece := policyPattern{text: part}
		if p.literal != nil {
			piece.literal = p.literal[offset : offset+len(part)]
		}
		out = append(out, piece)
		offset += len(part) + len(sep)
	}
	return out
}

// substitutePolicyVariables resolves every ${…} in text against the request context,
// and reports whether all of them resolved.
//
// A variable with no value makes the whole string unresolved, and no pattern is
// returned for it, because AWS's rule leaves nothing to compare: "When policy
// variables reference a condition context key that has no value or is not present in
// an authorization context for a request, the value is effectively null. There is no
// equal or like value." Every caller therefore drops the element rather than matching
// it — see [resourceMatches] and [condPolicyValues], which is where that rule is
// applied for each element AWS states it for.
//
// A key present with an empty value counts as having no value, which is the reading
// [condKeyPresent] has always taken of AWS's "resolves to a null dataset, such as an
// empty string".
//
// A `${` with no closing brace is not a variable and is passed through as text. AWS
// documents no such form, so it cannot be a variable with a value; treating it as an
// unresolved one instead would silently void a statement over a typo.
func substitutePolicyVariables(text string, ctx map[string]string) (policyPattern, bool) {
	if !strings.Contains(text, policyVarOpen) {
		return rawPolicyPattern(text), true
	}

	var out strings.Builder
	out.Grow(len(text))
	literal := make([]bool, 0, len(text))

	// Text the author wrote keeps its wildcards; text a variable resolved to does not.
	appendAuthored := func(s string) {
		out.WriteString(s)
		literal = append(literal, make([]bool, len(s))...)
	}
	appendResolved := func(s string) {
		out.WriteString(s)
		for i := 0; i < len(s); i++ {
			literal = append(literal, true)
		}
	}

	rest := text
	for {
		open := strings.Index(rest, policyVarOpen)
		if open < 0 {
			appendAuthored(rest)
			break
		}
		appendAuthored(rest[:open])
		body := rest[open+len(policyVarOpen):]
		end := strings.IndexByte(body, policyVarClose)
		if end < 0 {
			appendAuthored(rest[open:])
			break
		}
		value, ok := resolvePolicyVariable(body[:end], ctx)
		if !ok {
			return policyPattern{}, false
		}
		appendResolved(value)
		rest = body[end+1:]
	}
	return policyPattern{text: out.String(), literal: literal}, true
}

// resolvePolicyVariable resolves one variable's contents — what stands between `${`
// and `}` — to the text it stands for.
//
// The three special forms come first. AWS: "There are a few special predefined policy
// variables that have fixed values that enable you to represent characters that
// otherwise have special meaning… ${*} - use where you need an * (asterisk)
// character. ${?} - use where you need a ? (question mark) character. ${$} - use
// where you need a $ (dollar sign) character." They resolve to that character as
// text, never as a wildcard; see [policyPattern].
//
// A name resolves through [condResolveKey] rather than by indexing the context,
// because AWS's rule for a variable is the rule for a condition key it names: "Key
// names are case-insensitive. For example, aws:CurrentTime is equivalent to
// AWS:currenttime." A producer's spelling and a policy's need not agree.
func resolvePolicyVariable(expr string, ctx map[string]string) (string, bool) {
	switch expr {
	case "*", "?", "$":
		return expr, true
	}
	name, fallback, hasFallback := splitPolicyVariableDefault(expr)
	if key, ok := condResolveKey(name, ctx); ok && ctx[key] != "" {
		return ctx[key], true
	}
	if hasFallback {
		return fallback, true
	}
	return "", false
}

// splitPolicyVariableDefault separates a variable's name from its default value.
//
// AWS: "To add a default value to a variable, surround the default value with single
// quotes (' '), and separate the variable text and the default value with a comma and
// space (, )" — `${aws:PrincipalTag/team, 'company-wide'}`.
//
// The quotes are stripped when both are present and the value is used as written when
// they are not, which AWS does not specify: a name cannot contain a comma, so
// everything after the first one was meant as a default whether or not it was quoted,
// and reading it as part of the key name would make the variable unresolvable instead.
// That is substrate's reading (#745).
func splitPolicyVariableDefault(expr string) (name, fallback string, ok bool) {
	comma := strings.IndexByte(expr, ',')
	if comma < 0 {
		return expr, "", false
	}
	name = strings.TrimSpace(expr[:comma])
	fallback = strings.TrimSpace(expr[comma+1:])
	if len(fallback) >= 2 && strings.HasPrefix(fallback, "'") && strings.HasSuffix(fallback, "'") {
		fallback = fallback[1 : len(fallback)-1]
	}
	return name, fallback, true
}

// condOperatorSubstitutes reports whether AWS resolves policy variables in the values
// of a condition written with base.
//
// AWS: "You can use a policy variable for Condition values in any condition that
// involves the string operators or the ARN operators (StringEquals, StringLike,
// StringNotLike, ArnLike, ArnNotLike, and so on). You can't use a policy variable with
// other operators, such as Numeric, Date, Boolean, Binary, IP Address, or Null
// operators." That list is exhaustive on both sides, so the test is the operator
// family's prefix rather than a list of names that would drift from [condEvaluate]'s
// switch.
//
// base is the operator with its `...IfExists` suffix and set qualifier already stripped
// by [conditionKeyMatches], because neither changes which family the operator belongs
// to.
func condOperatorSubstitutes(base string) bool {
	return strings.HasPrefix(base, "String") || strings.HasPrefix(base, "Arn")
}

// condPolicyValues resolves a condition's policy values into the patterns
// [condEvaluate] compares against.
//
// A value whose variable had no value is *dropped* rather than compared, because AWS
// leaves nothing to compare it with: "When policy variables reference a condition
// context key that has no value or is not present in an authorization context for a
// request, the value is effectively null. There is no equal or like value." Dropping is
// what makes both directions of that rule fall out of one scan — a positive operator
// finds no match among the values that remain, and a negated one is satisfied, which is
// AWS's "Inverted condition operators like StringNotEquals or StringNotLike do match
// against a null value."
//
// An operator whose family AWS does not substitute in keeps its values verbatim, `${…}`
// and all, per [condOperatorSubstitutes] — a Numeric condition on the literal text
// "${aws:username}" is a comparison that cannot be made, which [condNumericMatches]
// already answers, and not an unresolved variable (#745).
func condPolicyValues(base string, condValues StringOrSlice, vars policyVars) []policyPattern {
	if !vars.enabled || !condOperatorSubstitutes(base) {
		out := make([]policyPattern, 0, len(condValues))
		for _, v := range condValues {
			out = append(out, rawPolicyPattern(v))
		}
		return out
	}
	out := make([]policyPattern, 0, len(condValues))
	for _, v := range condValues {
		if pattern, ok := vars.resolve(v); ok {
			out = append(out, pattern)
		}
	}
	return out
}

// globMatchMask is [globMatch] over a pattern whose wildcards are named by a mask.
//
// literal may be nil, meaning every `*` and `?` in pattern is a wildcard — which is
// every Action, and every Resource naming no variable, so the common path is
// unchanged by #745.
//
// The walk is iterative and remembers one `*`: on a mismatch it resumes from the last
// star having let it consume one more byte. That answers identically to the recursion
// it replaced for the `*`/`?` grammar AWS defines — "'*' matches any sequence of
// characters, '?' matches exactly one character" — without recursing once per star,
// and it is the only form in which the mask can be consulted, since a recursive call
// on a pattern suffix would have to carry the matching slice of the mask with it.
func globMatchMask(pattern string, literal []bool, value string) bool {
	wildcard := func(i int, c byte) bool {
		return pattern[i] == c && (literal == nil || !literal[i])
	}

	// star is where the last wildcard `*` was found and starValue how much of value
	// it had consumed at the time; -1 means there has been none to fall back to.
	p, v, star, starValue := 0, 0, -1, 0
	for v < len(value) {
		switch {
		case p < len(pattern) && wildcard(p, '?'):
			p++
			v++
		case p < len(pattern) && !wildcard(p, '*') && pattern[p] == value[v]:
			p++
			v++
		case p < len(pattern) && wildcard(p, '*'):
			star, starValue = p, v
			p++
		case star >= 0:
			starValue++
			p, v = star+1, starValue
		default:
			return false
		}
	}
	for p < len(pattern) && wildcard(p, '*') {
		p++
	}
	return p == len(pattern)
}
