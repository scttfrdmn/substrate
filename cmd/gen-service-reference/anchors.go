package main

import (
	_ "embed"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// pageAnchors returns every fragment a VitePress page answers: the id of each ATX heading,
// an id="…" on any HTML element, and a name="…" on an <a>.
//
// The heading ids reproduce VitePress's own pipeline (vitepress/dist/node): markdown-it's
// inline parse, the attrs plugin's trailing {#id}, the emoji plugin, markdown-it-anchor's
// text extraction (text and code_inline tokens only) and dedupe, and VitePress's slugify.
// testdata/headings.golden.json holds the ids VitePress rendered for testdata/headings.md,
// one heading per rule; `make docs-anchor-fixtures` regenerates it and emoji.json, and
// should be run whenever docs/package.json moves VitePress.
//
// Setext headings (a line underlined with `---` or `===`) are not recognized. Substrate's
// docs write ATX headings, and a paragraph that sits directly above a `---` rule becomes a
// setext heading by accident; a link to one is reported dead, which is the right outcome.
func pageAnchors(src string) map[string]bool {
	anchors := make(map[string]bool)
	for _, id := range headingIDs(src) {
		anchors[id] = true
	}
	for _, line := range proseLines(src) {
		for _, re := range htmlAnchors {
			for _, m := range re.FindAllStringSubmatch(codeSpan.ReplaceAllString(line, ""), -1) {
				anchors[m[1]+m[2]] = true
			}
		}
	}
	return anchors
}

// headingIDs returns the id of each ATX heading in src, in page order.
func headingIDs(src string) []string {
	lines := proseLines(src)
	refs := referenceLabels(lines)
	var ids []string
	used := make(map[string]bool)
	for _, line := range lines {
		h := atxHeading.FindStringSubmatch(line)
		if h == nil {
			continue
		}
		text, custom := headingText(h[2], refs)
		if custom != "" {
			// markdown-it-anchor takes a user-defined id as written, and VitePress's build
			// fails on a repeated one, so there is nothing to dedupe.
			used[custom] = true
			ids = append(ids, custom)
			continue
		}
		slug := slugify(text)
		unique := slug
		for i := 1; used[unique]; i++ {
			unique = slug + "-" + strconv.Itoa(i)
		}
		used[unique] = true
		ids = append(ids, unique)
	}
	return ids
}

var (
	atxHeading = regexp.MustCompile(`^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$`)
	openFence  = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})")
	refDef     = regexp.MustCompile(`^ {0,3}\[((?:[^\[\]\\]|\\.)+)\]:`)
	// htmlAnchors capture a fragment's value in group 1 (double-quoted) or 2 (single-quoted).
	htmlAnchors = []*regexp.Regexp{
		regexp.MustCompile(`<[A-Za-z][A-Za-z0-9-]*\s(?:[^>]*?\s)?id\s*=\s*(?:"([^"]*)"|'([^']*)')`),
		regexp.MustCompile(`<a\s(?:[^>]*?\s)?name\s*=\s*(?:"([^"]*)"|'([^']*)')`),
	}
)

// proseLines returns the lines of src that are outside fenced code blocks, which is where
// headings and links are Markdown rather than an example of it. A fence closes only on a
// run of its own character at least as long as the one that opened it.
func proseLines(src string) []string {
	var out []string
	fence := ""
	for _, line := range strings.Split(src, "\n") {
		if fence != "" {
			if t := strings.TrimSpace(line); strings.HasPrefix(t, fence) && strings.Trim(t, fence[:1]) == "" {
				fence = ""
			}
			continue
		}
		if m := openFence.FindStringSubmatch(line); m != nil {
			fence = m[1]
			continue
		}
		out = append(out, line)
	}
	return out
}

// referenceLabels returns the normalized label of every link reference definition, which
// is what makes `[text][label]` and `[label]` links rather than literal brackets.
func referenceLabels(lines []string) map[string]bool {
	refs := make(map[string]bool)
	for _, line := range lines {
		if m := refDef.FindStringSubmatch(line); m != nil {
			refs[normalizeLabel(m[1])] = true
		}
	}
	return refs
}

// normalizeLabel is markdown-it's normalizeReference: trimmed, whitespace collapsed, and
// case-folded by lowering then uppering.
func normalizeLabel(s string) string {
	return strings.ToUpper(strings.ToLower(strings.Join(strings.Fields(s), " ")))
}

// pieceKind says what a piece of a heading's inline content became.
type pieceKind int

const (
	pText     pieceKind = iota // a text token
	pEscape                    // a backslash escape (text_special until text_join)
	pCode                      // a code span's content
	pAutolink                  // an autolink's text, which the emoji plugin skips
	pBoundary                  // a token that contributes no text: a tag, link, image or emphasis marker
)

type piece struct {
	kind pieceKind
	text string
}

// delimiter is one character of a `*` or `_` run, or one `~~` pair, as markdown-it pushes
// them for balance_pairs.
type delimiter struct {
	marker      byte
	length      int
	open, close bool
	end         int
	piece       int
}

// headingText returns what markdown-it-anchor slugifies for a heading's inline content, and
// the id a trailing {#id} sets, if any.
func headingText(s string, refs map[string]bool) (text, id string) {
	pieces := joinText(parseInline(s, 0, len(s), refs))
	if n := len(pieces); n > 0 && pieces[n-1].kind == pText {
		pieces[n-1].text, id = trailingAttrs(pieces[n-1].text)
	}
	var b strings.Builder
	for _, p := range pieces {
		switch p.kind {
		case pText:
			b.WriteString(dropEmoji(p.text))
		case pEscape, pCode, pAutolink:
			b.WriteString(p.text)
		case pBoundary:
		}
	}
	return b.String(), id
}

// parseInline tokenizes src[start:end] the way markdown-it's inline rules do, as far as a
// heading's text depends on it. A link's label is parsed as its own scope, as markdown-it
// does, so emphasis cannot pair across the link's edge.
func parseInline(src string, start, end int, refs map[string]bool) []piece {
	var pieces []piece
	var delims []delimiter
	var buf strings.Builder
	flush := func() {
		if buf.Len() > 0 {
			pieces = append(pieces, piece{pText, buf.String()})
			buf.Reset()
		}
	}
	emit := func(p piece) {
		flush()
		pieces = append(pieces, p)
	}
	for i := start; i < end; {
		c := src[i]
		switch {
		case c == '`':
			n := runLength(src[i:end], '`')
			if e := closingRun(src[:end], i+n, n); e >= 0 {
				emit(piece{pCode, codeContent(src[i+n : e])})
				i = e + n
				continue
			}
			buf.WriteString(src[i : i+n])
			i += n
		case c == '\\' && i+1 < end && isASCIIPunct(src[i+1]):
			emit(piece{pEscape, src[i+1 : i+2]})
			i += 2
		case c == '!' && i+1 < end && src[i+1] == '[':
			if _, _, after, ok := parseLink(src, i+1, end, refs); ok {
				emit(piece{kind: pBoundary}) // an image's alt text is not a text token
				i = after
				continue
			}
			buf.WriteByte(c)
			i++
		case c == '[':
			if labelStart, labelEnd, after, ok := parseLink(src, i, end, refs); ok {
				emit(piece{kind: pBoundary})
				pieces = append(pieces, parseInline(src, labelStart, labelEnd, refs)...)
				pieces = append(pieces, piece{kind: pBoundary})
				i = after
				continue
			}
			buf.WriteByte(c)
			i++
		case c == '<':
			if m := autolink.FindString(src[i:end]); m != "" {
				emit(piece{pAutolink, m[1 : len(m)-1]})
				i += len(m)
				continue
			}
			if m := htmlTag.FindString(src[i:end]); m != "" {
				emit(piece{kind: pBoundary})
				i += len(m)
				continue
			}
			buf.WriteByte(c)
			i++
		case c == '*' || c == '_':
			n := runLength(src[i:end], c)
			open, closer := flanking(src, i, i+n, end, c == '*')
			flush()
			for k := 0; k < n; k++ {
				delims = append(delims, delimiter{marker: c, length: n, open: open, close: closer, end: -1, piece: len(pieces)})
				pieces = append(pieces, piece{pText, string(c)})
			}
			i += n
		case c == '~':
			n := runLength(src[i:end], c)
			if n < 2 {
				buf.WriteByte(c)
				i++
				continue
			}
			open, closer := flanking(src, i, i+n, end, true)
			flush()
			if n%2 == 1 {
				pieces = append(pieces, piece{pText, "~"})
			}
			for k := 0; k < n/2; k++ {
				delims = append(delims, delimiter{marker: c, open: open, close: closer, end: -1, piece: len(pieces)})
				pieces = append(pieces, piece{pText, "~~"})
			}
			i += n
		default:
			buf.WriteByte(c)
			i++
		}
	}
	flush()
	balancePairs(delims)
	for _, d := range delims {
		if d.end >= 0 {
			pieces[d.piece].kind = pBoundary
			pieces[delims[d.end].piece].kind = pBoundary
		}
	}
	return pieces
}

// codeContent strips the one space a code span may pad each side with.
func codeContent(code string) string {
	if len(code) >= 2 && code[0] == ' ' && code[len(code)-1] == ' ' && strings.Trim(code, " ") != "" {
		return code[1 : len(code)-1]
	}
	return code
}

// joinText merges adjacent text pieces, as markdown-it's fragments_join does once
// unmatched delimiters are plain text again.
func joinText(pieces []piece) []piece {
	var out []piece
	for _, p := range pieces {
		if p.kind == pText && len(out) > 0 && out[len(out)-1].kind == pText {
			out[len(out)-1].text += p.text
			continue
		}
		out = append(out, p)
	}
	return out
}

// flanking is markdown-it's scanDelims for the run src[start:stop], where end bounds the
// current scope (a link label ends a scope as the end of the line does).
func flanking(src string, start, stop, end int, canSplitWord bool) (canOpen, canClose bool) {
	last, next := ' ', ' '
	if start > 0 {
		last, _ = utf8.DecodeLastRuneInString(src[:start])
	}
	if stop < end {
		next, _ = utf8.DecodeRuneInString(src[stop:end])
	}
	lastPunct, nextPunct := isPunct(last), isPunct(next)
	lastSpace, nextSpace := isMarkdownSpace(last), isMarkdownSpace(next)
	left := !nextSpace && (!nextPunct || lastSpace || lastPunct)
	right := !lastSpace && (!lastPunct || nextSpace || nextPunct)
	return left && (canSplitWord || !right || lastPunct), right && (canSplitWord || !left || nextPunct)
}

func isPunct(r rune) bool {
	return (r < utf8.RuneSelf && isASCIIPunct(byte(r))) || unicode.In(r, unicode.P, unicode.S)
}

// isMarkdownSpace is markdown-it's isWhiteSpace.
func isMarkdownSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x202f, 0x205f, 0x3000:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

// balancePairs is markdown-it's balance_pairs: it pairs each closer with the nearest
// opener of its marker, under CommonMark's rule of three, and records the pair in the
// opener's end. The delimiter's piece index stands in for its token index.
func balancePairs(delims []delimiter) {
	if len(delims) == 0 {
		return
	}
	openersBottom := map[byte]*[6]int{}
	headerIdx, lastTokenIdx := 0, -2
	jumps := make([]int, 0, len(delims))
	for closerIdx := range delims {
		closer := &delims[closerIdx]
		jumps = append(jumps, 0)
		if delims[headerIdx].marker != closer.marker || lastTokenIdx != closer.piece-1 {
			headerIdx = closerIdx
		}
		lastTokenIdx = closer.piece
		if !closer.close {
			continue
		}
		bottom, ok := openersBottom[closer.marker]
		if !ok {
			bottom = &[6]int{-1, -1, -1, -1, -1, -1}
			openersBottom[closer.marker] = bottom
		}
		slot := closer.length % 3
		if closer.open {
			slot += 3
		}
		openerIdx := headerIdx - jumps[headerIdx] - 1
		newMin := openerIdx
		for ; openerIdx > bottom[slot]; openerIdx -= jumps[openerIdx] + 1 {
			opener := &delims[openerIdx]
			if opener.marker != closer.marker || !opener.open || opener.end >= 0 {
				continue
			}
			if (opener.close || closer.open) && (opener.length+closer.length)%3 == 0 &&
				(opener.length%3 != 0 || closer.length%3 != 0) {
				continue
			}
			lastJump := 0
			if openerIdx > 0 && !delims[openerIdx-1].open {
				lastJump = jumps[openerIdx-1] + 1
			}
			jumps[closerIdx] = closerIdx - openerIdx + lastJump
			jumps[openerIdx] = lastJump
			closer.open = false
			opener.end = closerIdx
			opener.close = false
			newMin = -1
			lastTokenIdx = -2
			break
		}
		if newMin != -1 {
			bottom[slot] = newMin
		}
	}
}

// parseLink reports whether src[open] (a '[') starts a link that markdown-it would parse:
// an inline link `[text](dest "title")`, or a full, collapsed or shortcut reference whose
// label is defined. It returns the label's bounds and the offset just past the link.
func parseLink(src string, open, end int, refs map[string]bool) (labelStart, labelEnd, after int, ok bool) {
	labelEnd = linkLabelEnd(src, open, end)
	if labelEnd < 0 {
		return 0, 0, 0, false
	}
	labelStart = open + 1
	pos := labelEnd + 1
	if pos < end && src[pos] == '(' {
		if a, ok := inlineDestination(src, pos+1, end); ok {
			return labelStart, labelEnd, a, true
		}
	}
	if len(refs) == 0 {
		return 0, 0, 0, false
	}
	label := src[labelStart:labelEnd]
	after = pos
	if pos < end && src[pos] == '[' {
		if e := linkLabelEnd(src, pos, end); e >= 0 {
			if e > pos+1 {
				label = src[pos+1 : e]
			}
			after = e + 1
		}
	}
	if !refs[normalizeLabel(label)] {
		return 0, 0, 0, false
	}
	return labelStart, labelEnd, after, true
}

// linkLabelEnd returns the offset of the ']' that closes the label opened at src[open],
// skipping code spans and escapes, or -1.
func linkLabelEnd(src string, open, end int) int {
	level := 1
	for i := open + 1; i < end; i++ {
		switch src[i] {
		case '\\':
			i++
		case '`':
			n := runLength(src[i:end], '`')
			if e := closingRun(src[:end], i+n, n); e >= 0 {
				i = e + n - 1
			} else {
				i += n - 1
			}
		case '[':
			level++
		case ']':
			level--
			if level == 0 {
				return i
			}
		}
	}
	return -1
}

// inlineDestination parses `dest "title")` from src[pos], returning the offset past ')'.
func inlineDestination(src string, pos, end int) (int, bool) {
	skip := func() {
		for pos < end && (src[pos] == ' ' || src[pos] == '\t' || src[pos] == '\n') {
			pos++
		}
	}
	skip()
	switch {
	case pos < end && src[pos] == '<':
		e := strings.IndexAny(src[pos+1:end], ">\n")
		if e < 0 || src[pos+1+e] != '>' {
			return 0, false
		}
		pos += e + 2
	default:
		depth := 0
		for pos < end {
			c := src[pos]
			if c == '\\' && pos+1 < end {
				pos += 2
				continue
			}
			if c == ' ' || c < 0x20 {
				break
			}
			if c == '(' {
				depth++
			}
			if c == ')' {
				if depth == 0 {
					break
				}
				depth--
			}
			pos++
		}
	}
	skip()
	if pos < end && (src[pos] == '"' || src[pos] == '\'' || src[pos] == '(') {
		closeQuote := src[pos]
		if closeQuote == '(' {
			closeQuote = ')'
		}
		e := strings.IndexByte(src[pos+1:end], closeQuote)
		if e < 0 {
			return 0, false
		}
		pos += e + 2
		skip()
	}
	if pos < end && src[pos] == ')' {
		return pos + 1, true
	}
	return 0, false
}

// trailingAttrs strips a trailing {…} the attrs plugin consumes from a heading's last text
// token, returning the remaining text and the id it sets, if any.
func trailingAttrs(s string) (text, id string) {
	open := strings.LastIndexByte(s, '{')
	if open < 0 || !strings.HasSuffix(s, "}") {
		return s, ""
	}
	// The plugin's own test: the first '}' at least two past '{' ends the string, and the
	// curly is {x} or longer ({#x} or {.x} for an id or class).
	closeAt := strings.IndexByte(s[min(open+2, len(s)):], '}')
	if closeAt < 0 || open+2+closeAt != len(s)-1 {
		return s, ""
	}
	curly := s[open:]
	if minLen := 3; len(curly) < minLen || ((curly[1] == '#' || curly[1] == '.') && len(curly) < minLen+1) {
		return s, ""
	}
	for _, field := range strings.Fields(curly[1 : len(curly)-1]) {
		if strings.HasPrefix(field, "#") {
			id = field[1:]
		}
	}
	// The plugin drops one space before the curly, not every one.
	return strings.TrimSuffix(s[:open], " "), id
}

//go:embed emoji.json
var emojiJSON []byte

// emojiPattern matches what the emoji plugin replaces: a :shortcode: from its table, or an
// emoticon shortcut, in the plugin's own alternation order.
var emojiPattern, emojiShortcuts = func() (*regexp.Regexp, map[string]bool) {
	var table struct {
		Names     []string `json:"names"`
		Shortcuts []string `json:"shortcuts"`
	}
	if err := json.Unmarshal(emojiJSON, &table); err != nil {
		panic("emoji.json: " + err.Error())
	}
	shortcuts := make(map[string]bool, len(table.Shortcuts))
	alts := make([]string, 0, len(table.Names)+len(table.Shortcuts))
	for _, n := range table.Names {
		alts = append(alts, ":"+n+":")
	}
	for _, s := range table.Shortcuts {
		shortcuts[s] = true
		alts = append(alts, s)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(alts)))
	for i, a := range alts {
		alts[i] = regexp.QuoteMeta(a)
	}
	return regexp.MustCompile(strings.Join(alts, "|")), shortcuts
}()

// dropEmoji removes what the emoji plugin turns into an emoji token, which contributes no
// text. A shortcut counts only between separators, punctuation or the token's edges.
func dropEmoji(s string) string {
	var b strings.Builder
	last := 0
	for _, m := range emojiPattern.FindAllStringIndex(s, -1) {
		if emojiShortcuts[s[m[0]:m[1]]] {
			if before, _ := utf8.DecodeLastRuneInString(s[:m[0]]); m[0] > 0 && !isEmojiEdge(before) {
				continue
			}
			if after, _ := utf8.DecodeRuneInString(s[m[1]:]); m[1] < len(s) && !isEmojiEdge(after) {
				continue
			}
		}
		b.WriteString(s[last:m[0]])
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

func isEmojiEdge(r rune) bool { return unicode.In(r, unicode.Z, unicode.P, unicode.Cc) }

var (
	autolink = regexp.MustCompile(`^<[A-Za-z][A-Za-z0-9+.-]{1,31}:[^<>\s]*>`)
	htmlTag  = regexp.MustCompile(`^(?:<[A-Za-z][A-Za-z0-9-]*(?:\s+[A-Za-z_:@][A-Za-z0-9:._-]*(?:\s*=\s*(?:[^"'=<>` + "`" + `\s]+|'[^']*'|"[^"]*"))?)*\s*/?>|</[A-Za-z][A-Za-z0-9-]*\s*>|<!--.*?-->)`)
)

// runLength is the length of the run of c at the start of s.
func runLength(s string, c byte) int {
	n := 0
	for n < len(s) && s[n] == c {
		n++
	}
	return n
}

// closingRun finds the next run of exactly n backticks at or after from, or -1.
func closingRun(s string, from, n int) int {
	for i := from; i < len(s); {
		if s[i] != '`' {
			i++
			continue
		}
		r := runLength(s[i:], '`')
		if r == n {
			return i
		}
		i += r
	}
	return -1
}

func isASCIIPunct(c byte) bool {
	return strings.IndexByte("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", c) >= 0
}

// slugSeparators is VitePress's rSpecial, with JavaScript's \s spelled out: Go's \s is
// ASCII-only, and JavaScript's also matches the Unicode space separators.
var slugSeparators = regexp.MustCompile(`[\t\n\v\f\r \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}` +
	"~`!@#$%^&*()\\-_+=\\[\\]{}|\\\\;:\"'“”‘’<>,.?/]+")

// slugify is VitePress's slugify: NFKD, combining marks and control characters dropped,
// each run of separators one hyphen, hyphens trimmed, a leading digit prefixed with an
// underscore, and lowercase.
func slugify(s string) string {
	s = strings.Map(func(r rune) rune {
		if (r >= 0x300 && r <= 0x36f) || r <= 0x1f {
			return -1
		}
		return r
	}, norm.NFKD.String(s))
	s = slugSeparators.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if s != "" && s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return strings.Map(unicode.ToLower, s)
}
