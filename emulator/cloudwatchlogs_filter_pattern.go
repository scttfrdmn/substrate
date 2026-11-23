package emulator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// CloudWatch Logs filter patterns (#1596).
//
// FilterLogEvents matched its filterPattern as one literal substring of the message until #1596, so a
// JSON pattern such as `{ $.task_uuid = "<uuid>" }` matched nothing (no message contains that text), and
// `ERROR -ARGUMENTS` matched only a message containing that exact eleven-character run. The grammar is
// the one the CloudWatch Logs User Guide publishes on "Filter pattern syntax for metric filters,
// subscription filters, filter log events, and Live Tail" (FilterAndPatternSyntax.html), and this file
// implements two of its three families:
//
//   - **Unstructured terms.** A term matches when the message contains it; terms are case-sensitive, and
//     a quoted phrase is one term ("Enclose exact phrases and terms that include non-alphanumeric
//     characters in double quotation marks"). Several terms all have to match; `-term` excludes; `?term`
//     makes alternatives, and "If you combine "?" with other filter patterns, all question mark terms will
//     be ignored". `" "` matches everything, per the page's "Match everything" example.
//   - **JSON.** `{ selector op value }`, with `= != < > <= >=`, `&&` and `||` under the page's order of
//     operations `() > && > ||`, `*` wildcards in values, `IS NULL`/`IS TRUE`/`IS FALSE`, `NOT EXISTS`,
//     array indexes (`$.a[0]`), the wildcard selector (`$.*`, `$.a[*]`) and bracket notation for a key
//     with a period (`$.['cluster.name']`). A message that is not JSON never matches a JSON pattern.
//
// Not implemented, and refused rather than mismatched: space-delimited patterns (`[ip, user, ...]`) and
// regular expressions (`%...%`, standalone or as a JSON value). Both are #1638. A pattern substrate
// cannot evaluate is refused with InvalidParameterException so a test does not silently pass on an
// empty result set.
//
// The page publishes "The variables IS NOT and EXISTS currently aren't supported", so those are refused
// too, as are the selector quotas it publishes ("up to one wildcard selector in a property selector, and
// up to three wildcard selectors in a filter pattern with compound expressions").
//
// # Matching choices the page leaves open
//
//   - A term is a substring, not a whole word: the page's "messages contain the word ERROR" examples are
//     all satisfied either way, and substring is the reading substrate already had.
//   - `=` and `!=` compare numerically when both sides are numbers (so `1e+3` equals `1000`) and as text
//     otherwise, a number node's text being its literal as written in the event. A value with `*` is a
//     glob over that text, which is how the page's `{ $.sourceIPAddress != 123.123.* }` reads.
//   - `<`, `>`, `<=`, `>=` need a number on the pattern side (anything else is refused) and match only a
//     number node.
//   - A selector that resolves to nothing, to an object or array, or to null satisfies no comparison,
//     `!=` included: "If a property selector points to an array or object, the filter pattern won't
//     match the log format". A wildcard selector matches when any node it reaches does.
//
// Every refusal message in this file is substrate's own; AWS publishes none for an unparseable pattern.

// cwlFilterPattern is a compiled filterPattern. The zero value matches every event.
type cwlFilterPattern struct {
	// terms is the unstructured form; nil for a JSON pattern.
	terms *cwlTermPattern
	// expr is the JSON form; nil for an unstructured pattern.
	expr cwlJSONExpr
}

// cwlTermPattern is an unstructured pattern's terms, split by their prefix.
type cwlTermPattern struct {
	required []string
	excluded []string
	optional []string
}

// cwlInvalidFilterPattern is the refusal for a filterPattern substrate cannot parse or does not evaluate.
// InvalidParameterException/400 is the only code API_FilterLogEvents publishes for a bad parameter. The
// message is substrate's own.
func cwlInvalidFilterPattern(pattern, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    fmt.Sprintf("Invalid filterPattern %q: %s", pattern, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwlParseFilterPattern compiles a filterPattern, refusing one that does not parse or that names a
// family substrate does not evaluate yet.
func cwlParseFilterPattern(pattern string) (*cwlFilterPattern, error) {
	trimmed := strings.TrimSpace(pattern)
	switch {
	case trimmed == "":
		return &cwlFilterPattern{}, nil
	case strings.HasPrefix(trimmed, "["):
		return nil, cwlInvalidFilterPattern(pattern,
			"space-delimited filter patterns are not yet supported by substrate (#1638)")
	case strings.HasPrefix(trimmed, "%"):
		return nil, cwlInvalidFilterPattern(pattern,
			"regular-expression filter patterns are not yet supported by substrate (#1638)")
	case strings.HasPrefix(trimmed, "{"):
		if !strings.HasSuffix(trimmed, "}") {
			return nil, cwlInvalidFilterPattern(pattern, "a JSON filter pattern must end with '}'")
		}
		expr, reason := cwlParseJSONPattern(trimmed[1 : len(trimmed)-1])
		if reason != "" {
			return nil, cwlInvalidFilterPattern(pattern, reason)
		}
		return &cwlFilterPattern{expr: expr}, nil
	default:
		terms, reason := cwlParseTerms(trimmed)
		if reason != "" {
			return nil, cwlInvalidFilterPattern(pattern, reason)
		}
		return &cwlFilterPattern{terms: terms}, nil
	}
}

// Match reports whether a log event's message satisfies the pattern.
func (p *cwlFilterPattern) Match(message string) bool {
	switch {
	case p == nil:
		return true
	case p.terms != nil:
		return p.terms.match(message)
	case p.expr != nil:
		doc, ok := cwlDecodeJSONMessage(message)
		return ok && p.expr.eval(doc)
	default:
		return true
	}
}

// --- Unstructured terms -------------------------------------------------------------------------------

// cwlParseTerms splits an unstructured pattern into its terms. It returns a non-empty reason when the
// pattern cannot be parsed.
func cwlParseTerms(s string) (*cwlTermPattern, string) {
	out := &cwlTermPattern{}
	i := 0
	for i < len(s) {
		if cwlTermSpace(s[i]) {
			i++
			continue
		}
		prefix := byte(0)
		if (s[i] == '?' || s[i] == '-') && i+1 < len(s) && !cwlTermSpace(s[i+1]) {
			prefix = s[i]
			i++
		}
		var term string
		if s[i] == '"' {
			text, next, ok := cwlReadQuoted(s, i)
			if !ok {
				return nil, "unterminated quoted phrase"
			}
			term, i = text, next
		} else {
			start := i
			for i < len(s) && !cwlTermSpace(s[i]) {
				if s[i] == '"' {
					return nil, "a quotation mark may only open a quoted phrase"
				}
				i++
			}
			term = s[start:i]
			if len(term) >= 2 && strings.HasPrefix(term, "%") && strings.HasSuffix(term, "%") {
				return nil, "regular-expression filter patterns are not yet supported by substrate (#1638)"
			}
		}
		switch prefix {
		case '?':
			out.optional = append(out.optional, term)
		case '-':
			out.excluded = append(out.excluded, term)
		default:
			out.required = append(out.required, term)
		}
	}
	return out, ""
}

// cwlTermSpace reports whether c separates terms in an unstructured pattern.
func cwlTermSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// cwlReadQuoted reads the double-quoted string that opens at s[start], honoring backslash escapes of
// a quotation mark or a backslash. It returns the unescaped text and the index just past the closing
// quotation mark.
func cwlReadQuoted(s string, start int) (string, int, bool) {
	var b strings.Builder
	for i := start + 1; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\\'):
			b.WriteByte(s[i+1])
			i++
		case c == '"':
			return b.String(), i + 1, true
		default:
			b.WriteByte(c)
		}
	}
	return "", len(s), false
}

// cwlTermContained reports whether message contains term. A term that is empty or all whitespace is
// contained in every message: it is the page's `" "` "Match everything" example.
func cwlTermContained(message, term string) bool {
	if strings.TrimSpace(term) == "" {
		return true
	}
	return strings.Contains(message, term)
}

// match applies the terms. The `?` terms are an alternation only when they are the whole pattern; mixed
// with a required or excluded term they are ignored, as the page states.
func (t *cwlTermPattern) match(message string) bool {
	if len(t.required) == 0 && len(t.excluded) == 0 {
		for _, term := range t.optional {
			if cwlTermContained(message, term) {
				return true
			}
		}
		return len(t.optional) == 0
	}
	for _, term := range t.required {
		if !cwlTermContained(message, term) {
			return false
		}
	}
	for _, term := range t.excluded {
		if strings.TrimSpace(term) != "" && strings.Contains(message, term) {
			return false
		}
	}
	return true
}

// --- JSON patterns ------------------------------------------------------------------------------------

// cwlJSONExpr is a node of a compiled JSON filter pattern.
type cwlJSONExpr interface {
	eval(doc any) bool
}

// cwlJSONAnd is `left && right`.
type cwlJSONAnd struct{ left, right cwlJSONExpr }

func (e cwlJSONAnd) eval(doc any) bool { return e.left.eval(doc) && e.right.eval(doc) }

// cwlJSONOr is `left || right`.
type cwlJSONOr struct{ left, right cwlJSONExpr }

func (e cwlJSONOr) eval(doc any) bool { return e.left.eval(doc) || e.right.eval(doc) }

// cwlJSONStep is one step of a property selector: a key, an index, or the wildcard.
type cwlJSONStep struct {
	key      string
	index    int
	isIndex  bool
	wildcard bool
}

// cwlJSONCondition is one comparison: a selector and what it is tested against.
type cwlJSONCondition struct {
	steps []cwlJSONStep
	// kind is "op", "null", "true", "false" or "notexists".
	kind string
	op   string
	lit  cwlJSONLiteral
}

// cwlJSONLiteral is the value side of a comparison.
type cwlJSONLiteral struct {
	text   string
	isNum  bool
	num    float64
	isGlob bool
}

// cwlNumberLiteral is the published number form: an integer or decimal, with an optional sign and an
// optional exponent ("You can use integers that contain plus ("+") or minus ("-") symbols and follow
// scientific notation"). strconv.ParseFloat alone would also take "Inf" and "NaN".
var cwlNumberLiteral = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`) //nolint:gochecknoglobals // compiled once, read-only

// cwlDecodeJSONMessage decodes an event message for a JSON pattern, keeping numbers as their literal
// text so a glob reads what the event carried.
func cwlDecodeJSONMessage(message string) (any, bool) {
	dec := json.NewDecoder(strings.NewReader(message))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, false
	}
	// Anything after the value, a stray `]` or `}` included, makes the message not JSON. dec.More
	// alone reports false for a closing delimiter, so the stream must be at its end.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return doc, true
}

func (c cwlJSONCondition) eval(doc any) bool {
	nodes := cwlResolveSelector(doc, c.steps)
	if c.kind == "notexists" {
		return len(nodes) == 0
	}
	for _, n := range nodes {
		if c.test(n) {
			return true
		}
	}
	return false
}

// test applies the condition to one resolved node.
func (c cwlJSONCondition) test(node any) bool {
	switch c.kind {
	case "null":
		return node == nil
	case "true", "false":
		b, ok := node.(bool)
		return ok && b == (c.kind == "true")
	}
	text, isNum, num, scalar := cwlNodeScalar(node)
	if !scalar {
		return false
	}
	switch c.op {
	case "=", "!=":
		var equal bool
		switch {
		case c.lit.isGlob:
			equal = cwlGlobMatch(c.lit.text, text)
		case c.lit.isNum && isNum:
			equal = num == c.lit.num
		default:
			equal = text == c.lit.text
		}
		return equal == (c.op == "=")
	default:
		if !isNum {
			return false
		}
		switch c.op {
		case "<":
			return num < c.lit.num
		case ">":
			return num > c.lit.num
		case "<=":
			return num <= c.lit.num
		default: // ">="
			return num >= c.lit.num
		}
	}
}

// cwlNodeScalar reports a node's text and, for a number, its value. scalar is false for an object, an
// array and null, which satisfy no comparison.
func cwlNodeScalar(node any) (text string, isNum bool, num float64, scalar bool) {
	switch v := node.(type) {
	case string:
		return v, false, 0, true
	case json.Number:
		f, err := v.Float64()
		return v.String(), err == nil, f, true
	case bool:
		return strconv.FormatBool(v), false, 0, true
	default:
		return "", false, 0, false
	}
}

// cwlResolveSelector walks the selector's steps from the document root, returning every node it reaches.
func cwlResolveSelector(doc any, steps []cwlJSONStep) []any {
	nodes := []any{doc}
	for _, st := range steps {
		var next []any
		for _, n := range nodes {
			switch v := n.(type) {
			case map[string]any:
				switch {
				case st.wildcard:
					for _, child := range v {
						next = append(next, child)
					}
				case !st.isIndex:
					if child, ok := v[st.key]; ok {
						next = append(next, child)
					}
				}
			case []any:
				switch {
				case st.wildcard:
					next = append(next, v...)
				case st.isIndex && st.index < len(v):
					next = append(next, v[st.index])
				}
			}
		}
		nodes = next
	}
	return nodes
}

// cwlGlobMatch matches text against a pattern in which `*` stands for any run of characters.
func cwlGlobMatch(pattern, text string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == text
	}
	if !strings.HasPrefix(text, parts[0]) {
		return false
	}
	text = text[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, part := range parts[1 : len(parts)-1] {
		idx := strings.Index(text, part)
		if idx < 0 {
			return false
		}
		text = text[idx+len(part):]
	}
	return strings.HasSuffix(text, last)
}

// cwlJSONToken is a lexical token of a JSON pattern's body.
type cwlJSONToken struct {
	// kind is "(", ")", "&&", "||", "op", "selector", "string", "regex" or "word".
	kind string
	text string
}

// cwlLexJSONPattern splits the text between a JSON pattern's braces into tokens.
func cwlLexJSONPattern(s string) ([]cwlJSONToken, string) {
	var toks []cwlJSONToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(' || c == ')':
			toks = append(toks, cwlJSONToken{kind: string(c)})
			i++
		case strings.HasPrefix(s[i:], "&&") || strings.HasPrefix(s[i:], "||"):
			toks = append(toks, cwlJSONToken{kind: s[i : i+2]})
			i += 2
		case strings.HasPrefix(s[i:], "!=") || strings.HasPrefix(s[i:], "<=") || strings.HasPrefix(s[i:], ">="):
			toks = append(toks, cwlJSONToken{kind: "op", text: s[i : i+2]})
			i += 2
		case c == '=' || c == '<' || c == '>':
			toks = append(toks, cwlJSONToken{kind: "op", text: string(c)})
			i++
		case c == '"':
			text, next, ok := cwlReadQuoted(s, i)
			if !ok {
				return nil, "unterminated quoted string"
			}
			toks = append(toks, cwlJSONToken{kind: "string", text: text})
			i = next
		case c == '%':
			end := strings.IndexByte(s[i+1:], '%')
			if end < 0 {
				return nil, "unterminated regular expression"
			}
			toks = append(toks, cwlJSONToken{kind: "regex", text: s[i : i+end+2]})
			i += end + 2
		case c == '$':
			start := i
			for i < len(s) {
				if s[i] == '[' {
					end := strings.IndexByte(s[i:], ']')
					if end < 0 {
						return nil, "unterminated '[' in a property selector"
					}
					i += end + 1
					continue
				}
				if !cwlSelectorChar(s[i]) {
					break
				}
				i++
			}
			toks = append(toks, cwlJSONToken{kind: "selector", text: s[start:i]})
		default:
			start := i
			for i < len(s) && !strings.ContainsRune(" \t\n\r()&|=<>!\"", rune(s[i])) {
				i++
			}
			if i == start {
				return nil, fmt.Sprintf("unexpected character %q", s[i])
			}
			toks = append(toks, cwlJSONToken{kind: "word", text: s[start:i]})
		}
	}
	return toks, ""
}

// cwlSelectorChar reports whether c can continue a property selector outside brackets: the page
// publishes "alphanumeric strings that support hyphen ("-") and underscore ("_") characters", plus the
// period that separates steps, the dollar sign that opens one and the wildcard.
func cwlSelectorChar(c byte) bool {
	return c == '$' || c == '.' || c == '_' || c == '-' || c == '*' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// cwlJSONParser is a recursive-descent parser over a JSON pattern's tokens.
type cwlJSONParser struct {
	toks      []cwlJSONToken
	pos       int
	wildcards int
}

// cwlParseJSONPattern compiles the text between a JSON pattern's braces.
func cwlParseJSONPattern(body string) (cwlJSONExpr, string) {
	toks, reason := cwlLexJSONPattern(body)
	if reason != "" {
		return nil, reason
	}
	if len(toks) == 0 {
		return nil, "a JSON filter pattern needs at least one condition"
	}
	p := &cwlJSONParser{toks: toks}
	expr, reason := p.parseOr()
	if reason != "" {
		return nil, reason
	}
	if p.pos < len(p.toks) {
		return nil, fmt.Sprintf("unexpected %q", p.toks[p.pos].describe())
	}
	// "up to three wildcard selectors in a filter pattern with compound expressions."
	if p.wildcards > 3 {
		return nil, "a filter pattern may use at most three wildcard selectors"
	}
	return expr, ""
}

// describe renders a token for a refusal message.
func (t cwlJSONToken) describe() string {
	if t.text != "" {
		return t.text
	}
	return t.kind
}

func (p *cwlJSONParser) peek() *cwlJSONToken {
	if p.pos < len(p.toks) {
		return &p.toks[p.pos]
	}
	return nil
}

func (p *cwlJSONParser) next() *cwlJSONToken {
	t := p.peek()
	if t != nil {
		p.pos++
	}
	return t
}

func (p *cwlJSONParser) parseOr() (cwlJSONExpr, string) {
	left, reason := p.parseAnd()
	if reason != "" {
		return nil, reason
	}
	for t := p.peek(); t != nil && t.kind == "||"; t = p.peek() {
		p.pos++
		right, reason := p.parseAnd()
		if reason != "" {
			return nil, reason
		}
		left = cwlJSONOr{left: left, right: right}
	}
	return left, ""
}

func (p *cwlJSONParser) parseAnd() (cwlJSONExpr, string) {
	left, reason := p.parseUnary()
	if reason != "" {
		return nil, reason
	}
	for t := p.peek(); t != nil && t.kind == "&&"; t = p.peek() {
		p.pos++
		right, reason := p.parseUnary()
		if reason != "" {
			return nil, reason
		}
		left = cwlJSONAnd{left: left, right: right}
	}
	return left, ""
}

func (p *cwlJSONParser) parseUnary() (cwlJSONExpr, string) {
	t := p.next()
	if t == nil {
		return nil, "the pattern ends where a condition was expected"
	}
	switch t.kind {
	case "(":
		inner, reason := p.parseOr()
		if reason != "" {
			return nil, reason
		}
		if closing := p.next(); closing == nil || closing.kind != ")" {
			return nil, "missing ')'"
		}
		return inner, ""
	case "selector":
		return p.parseCondition(t.text)
	default:
		return nil, fmt.Sprintf("expected a property selector beginning with '$.', found %q", t.describe())
	}
}

// parseCondition parses what follows a selector: an operator and a value, IS NULL/TRUE/FALSE, or
// NOT EXISTS.
func (p *cwlJSONParser) parseCondition(selector string) (cwlJSONExpr, string) {
	steps, reason := cwlParseSelector(selector)
	if reason != "" {
		return nil, reason
	}
	wild := 0
	for _, st := range steps {
		if st.wildcard {
			wild++
		}
	}
	// "You can only use up to one wildcard selector in a property selector."
	if wild > 1 {
		return nil, fmt.Sprintf("the property selector %s uses more than one wildcard", selector)
	}
	p.wildcards += wild
	cond := cwlJSONCondition{steps: steps}

	t := p.next()
	if t == nil {
		return nil, fmt.Sprintf("the property selector %s has no condition", selector)
	}
	if t.kind == "word" {
		switch kw := strings.ToUpper(t.text); kw {
		case "IS":
			v := p.next()
			if v == nil || v.kind != "word" {
				return nil, "IS must be followed by NULL, TRUE or FALSE"
			}
			switch strings.ToUpper(v.text) {
			case "NULL", "TRUE", "FALSE":
				cond.kind = strings.ToLower(v.text)
				return cond, ""
			case "NOT":
				// "The variables IS NOT and EXISTS currently aren't supported."
				return nil, "IS NOT is not supported"
			default:
				return nil, "IS must be followed by NULL, TRUE or FALSE"
			}
		case "NOT":
			if v := p.next(); v == nil || v.kind != "word" || strings.ToUpper(v.text) != "EXISTS" {
				return nil, "NOT must be followed by EXISTS"
			}
			cond.kind = "notexists"
			return cond, ""
		case "EXISTS":
			return nil, "EXISTS is not supported; use NOT EXISTS"
		}
	}
	if t.kind != "op" {
		return nil, fmt.Sprintf("expected an operator after %s, found %q", selector, t.describe())
	}
	cond.kind, cond.op = "op", t.text

	v := p.next()
	if v == nil {
		return nil, fmt.Sprintf("the operator %s after %s has no value", t.text, selector)
	}
	switch v.kind {
	case "regex":
		return nil, "regular-expression values are not yet supported by substrate (#1638)"
	case "string":
		cond.lit = cwlJSONLiteral{text: v.text, isGlob: strings.Contains(v.text, "*")}
	case "word":
		cond.lit = cwlJSONLiteral{text: v.text, isGlob: strings.Contains(v.text, "*")}
		if cwlNumberLiteral.MatchString(v.text) {
			f, err := strconv.ParseFloat(v.text, 64)
			if err == nil {
				cond.lit.isNum, cond.lit.num = true, f
			}
		}
	default:
		return nil, fmt.Sprintf("expected a value after %s, found %q", t.text, v.describe())
	}
	if cond.op != "=" && cond.op != "!=" && !cond.lit.isNum {
		return nil, fmt.Sprintf("the operator %s needs a number, found %q", cond.op, v.text)
	}
	return cond, ""
}

// cwlParseSelector parses a property selector — `$.a.b`, `$.a[0]`, `$.a[*]`, `$.*`, `$.['k.ey']` — into
// its steps.
func cwlParseSelector(sel string) ([]cwlJSONStep, string) {
	if !strings.HasPrefix(sel, "$.") && !strings.HasPrefix(sel, "$[") {
		return nil, fmt.Sprintf("the property selector %s must begin with '$.'", sel)
	}
	var steps []cwlJSONStep
	s := sel[1:]
	for len(s) > 0 {
		switch s[0] {
		case '.':
			s = s[1:]
			if strings.HasPrefix(s, "[") {
				continue // `$.['cluster.name']`: the bracket is the step
			}
			end := 0
			for end < len(s) && s[end] != '.' && s[end] != '[' {
				end++
			}
			key := s[:end]
			switch {
			case key == "":
				return nil, fmt.Sprintf("the property selector %s has an empty step", sel)
			case key == "*":
				steps = append(steps, cwlJSONStep{wildcard: true})
			case strings.ContainsAny(key, "*$"):
				return nil, fmt.Sprintf("the property selector %s has an invalid step %q", sel, key)
			default:
				steps = append(steps, cwlJSONStep{key: key})
			}
			s = s[end:]
		case '[':
			end := strings.IndexByte(s, ']')
			inner := s[1:end]
			s = s[end+1:]
			switch {
			case inner == "*":
				steps = append(steps, cwlJSONStep{wildcard: true})
			case len(inner) >= 2 && inner[0] == '\'' && inner[len(inner)-1] == '\'':
				steps = append(steps, cwlJSONStep{key: inner[1 : len(inner)-1]})
			default:
				n, err := strconv.Atoi(inner)
				if err != nil || n < 0 {
					// "Double quotation marks (") aren't supported inside bracket notation and cause the
					// filter pattern to fail with a syntax error."
					return nil, fmt.Sprintf("the property selector %s has an invalid index [%s]", sel, inner)
				}
				steps = append(steps, cwlJSONStep{index: n, isIndex: true})
			}
		default:
			return nil, fmt.Sprintf("the property selector %s is malformed", sel)
		}
	}
	if len(steps) == 0 {
		return nil, fmt.Sprintf("the property selector %s selects nothing", sel)
	}
	return steps, ""
}
