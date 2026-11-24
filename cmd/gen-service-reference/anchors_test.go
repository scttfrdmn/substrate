package main

import (
	"sort"
	"strings"
	"testing"
)

// TestPageAnchors_MatchesVitePress pins pageAnchors to ids VitePress 1.6.4 rendered for this
// page, one heading per rule the slugger and the text extraction have to get right.
func TestPageAnchors_MatchesVitePress(t *testing.T) {
	page := strings.Join([]string{
		"# A `List<String>` and [a link](https://x.invalid) — done",
		"## The decoder's own error text",
		"## 400 is `ValidationError`",
		"## Cost",
		"## Cost",
		"## Cost",
		`## Foo &amp; bar\_baz <span>tag</span>`,
		"## `aws:` and `ec2:Subnet`",
		"## Café naïve",
		"```",
		"## Not a heading",
		"```",
		"## Closing hashes ##",
		`<a id="custom-anchor"></a>`,
	}, "\n\n")
	want := []string{
		"_400-is-validationerror",
		"a-list-string-and-a-link-—-done",
		"aws-and-ec2-subnet",
		"cafe-naive",
		"closing-hashes",
		"cost", "cost-1", "cost-2",
		"custom-anchor",
		"foo-amp-bar-baz-tag",
		"the-decoder-s-own-error-text",
	}
	var got []string
	for id := range pageAnchors(page) {
		got = append(got, id)
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("anchors:\n got %q\nwant %q", got, want)
	}
}

func TestHeadingText(t *testing.T) {
	tests := []struct{ in, want string }{
		{"plain", "plain"},
		{"`code [x](y)` after", "code [x](y) after"},
		{"`` a`b ``", "a`b"},
		{"an `unclosed span", "an `unclosed span"},
		{`\*escaped\*`, "*escaped*"},
		{"![alt text](img.png) and [link](u)", "alt text and link"},
		{"[not a link] here", "[not a link] here"},
		{"a ](no-open)", "a ](no-open)"},
		{"[unclosed](dest", "[unclosed](dest"},
		{"<https://x.invalid/a> autolink", "https://x.invalid/a autolink"},
		{"a <b>bold</b> tag", "a bold tag"},
		{"x < y", "x < y"},
	}
	for _, tc := range tests {
		if got := headingText(tc.in); got != tc.want {
			t.Errorf("headingText(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestProseLines_SkipsFencedBlocks(t *testing.T) {
	src := "a\n````md\n```\nstill inside\n````\nb\n~~~\n# no\n~~~\nc"
	if got := strings.Join(proseLines(src), ","); got != "a,b,c" {
		t.Errorf("proseLines = %q, want a,b,c", got)
	}
}
