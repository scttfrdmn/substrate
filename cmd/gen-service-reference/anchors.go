package main

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// pageAnchors returns every fragment a VitePress page answers: the id of each ATX heading,
// and each id written as `<a id="…">` in the page's HTML.
//
// The heading ids reproduce VitePress's own rules (vitepress/dist/node, `slugify` and the
// markdown-it-anchor options it passes): the heading's text and inline-code content,
// slugified, and a repeated slug suffixed -1, -2, … in page order. The cases in
// anchors_test.go are ids VitePress 1.6 rendered; when the site moves to a new VitePress,
// re-render them, because a divergence here reports a live link dead or a dead one live.
//
// Setext headings (a line underlined with `---` or `===`) are not recognized. Substrate's
// docs write ATX headings, and a paragraph that sits directly above a `---` rule becomes a
// setext heading by accident; a link to one is reported dead, which is the right outcome.
func pageAnchors(src string) map[string]bool {
	anchors := make(map[string]bool)
	used := make(map[string]bool)
	for _, line := range proseLines(src) {
		if m := htmlAnchor.FindAllStringSubmatch(line, -1); m != nil {
			for _, id := range m {
				anchors[id[1]] = true
			}
		}
		h := atxHeading.FindStringSubmatch(line)
		if h == nil {
			continue
		}
		slug := slugify(headingText(h[2]))
		unique := slug
		for i := 1; used[unique]; i++ {
			unique = slug + "-" + strconv.Itoa(i)
		}
		used[unique] = true
		anchors[unique] = true
	}
	return anchors
}

var (
	atxHeading = regexp.MustCompile(`^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$`)
	htmlAnchor = regexp.MustCompile(`<a\s+(?:id|name)="([^"]+)"`)
	openFence  = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})")
)

// proseLines returns the lines of src that are outside fenced code blocks, which is where
// headings and links are Markdown rather than an example of it.
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

// headingText is what markdown-it-anchor slugifies: the content of a heading's text and
// code_inline tokens. Code spans keep their content verbatim; outside them a backslash
// escape is its character, a link or image contributes its text and not its destination,
// and HTML tags contribute nothing. An entity is left as written, which is what VitePress
// slugs (`&amp;` contributes "amp"). Emphasis markers are left in, because every one of
// them is a character slugify turns into a separator anyway.
func headingText(s string) string {
	var b strings.Builder
	var linkOpen []int // byte offsets in b of '[' that may open a link
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '`':
			n := runLength(s[i:], '`')
			if end := closingRun(s, i+n, n); end >= 0 {
				code := s[i+n : end]
				if len(code) >= 2 && code[0] == ' ' && code[len(code)-1] == ' ' && strings.Trim(code, " ") != "" {
					code = code[1 : len(code)-1]
				}
				b.WriteString(code)
				i = end + n
				continue
			}
			b.WriteString(s[i : i+n])
			i += n
		case c == '\\' && i+1 < len(s) && isASCIIPunct(s[i+1]):
			b.WriteByte(s[i+1])
			i += 2
		case c == '!' && i+1 < len(s) && s[i+1] == '[':
			linkOpen = append(linkOpen, b.Len())
			b.WriteString("![")
			i += 2
		case c == '[':
			linkOpen = append(linkOpen, b.Len())
			b.WriteByte('[')
			i++
		case c == ']' && i+1 < len(s) && s[i+1] == '(' && len(linkOpen) > 0:
			end := strings.IndexByte(s[i:], ')')
			if end < 0 {
				b.WriteByte(c)
				i++
				continue
			}
			open := linkOpen[len(linkOpen)-1]
			linkOpen = linkOpen[:len(linkOpen)-1]
			text := b.String()
			marker := 1
			if strings.HasPrefix(text[open:], "![") {
				marker = 2
			}
			b.Reset()
			b.WriteString(text[:open] + text[open+marker:])
			i += end + 1
		case c == '<':
			if m := autolink.FindString(s[i:]); m != "" {
				b.WriteString(m[1 : len(m)-1])
				i += len(m)
				continue
			}
			if m := htmlTag.FindString(s[i:]); m != "" {
				i += len(m)
				continue
			}
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

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
