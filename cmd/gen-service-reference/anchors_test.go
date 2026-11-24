package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestPageAnchors_MatchesVitePress compares pageAnchors with what VitePress rendered for
// testdata/headings.md. The golden file is written by VitePress itself (`make
// docs-anchor-fixtures`), so the expected ids are VitePress's, not a reading of it.
func TestPageAnchors_MatchesVitePress(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "headings.md"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "headings.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		VitePress string   `json:"vitepress"`
		Headings  []string `json:"headings"`
		IDs       []string `json:"ids"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}

	got := headingIDs(string(src))
	for i := 0; i < max(len(got), len(golden.Headings)); i++ {
		var g, w string
		if i < len(got) {
			g = got[i]
		}
		if i < len(golden.Headings) {
			w = golden.Headings[i]
		}
		if g != w {
			t.Errorf("heading %d: got %q, VitePress %s rendered %q", i+1, g, golden.VitePress, w)
		}
	}

	var all []string
	for id := range pageAnchors(string(src)) {
		all = append(all, id)
	}
	sort.Strings(all)
	want := append([]string(nil), golden.IDs...)
	sort.Strings(want)
	if strings.Join(all, "\n") != strings.Join(want, "\n") {
		t.Errorf("fragments:\n got %q\nwant %q", all, want)
	}
}

// TestEmojiTable_ComesFromTheGoldenVitePress guards the embedded table against a hand edit: it is
// written by the same script as the golden file, from the same VitePress.
func TestEmojiTable_ComesFromTheGoldenVitePress(t *testing.T) {
	var table, golden struct {
		VitePress string   `json:"vitepress"`
		Names     []string `json:"names"`
	}
	if err := json.Unmarshal(emojiJSON, &table); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "headings.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if table.VitePress == "" || table.VitePress != golden.VitePress {
		t.Errorf("emoji.json is from VitePress %q and the golden file from %q; run `make docs-anchor-fixtures`",
			table.VitePress, golden.VitePress)
	}
	if len(table.Names) == 0 {
		t.Error("emoji.json names no emoji")
	}
}

func TestHeadingText(t *testing.T) {
	refs := map[string]bool{"REF": true}
	tests := []struct{ in, want, id string }{
		{"plain", "plain", ""},
		{"`code [x](y)` after", "code [x](y) after", ""},
		{"`` a`b ``", "a`b", ""},
		{"an `unclosed span", "an `unclosed span", ""},
		{`\*escaped\*`, "*escaped*", ""},
		{"![alt text](img.png) and [link](u)", " and link", ""},
		{"[not a link] here", "[not a link] here", ""},
		{"[ref] and [x][ref] and [y][]", "ref and x and [y][]", ""},
		{"a ](no-open)", "a ](no-open)", ""},
		{"[unclosed](dest", "[unclosed](dest", ""},
		{"[dest](<a b>) and [t](u 'x')", "dest and t", ""},
		{"<https://x.invalid/a> autolink", "https://x.invalid/a autolink", ""},
		{"a <b>bold</b> tag", "a bold tag", ""},
		{"x < y", "x < y", ""},
		{"**Arn**s", "Arns", ""},
		{"Title {#custom}", "Title", "custom"},
		{"Title {.cls}", "Title", ""},
		{"Title {}", "Title {}", ""},
		{"Title {#}", "Title {#}", ""},
		{"Title {a} b", "Title {a} b", ""},
		{":smile: x:)y :)", " x:)y ", ""},
	}
	for _, tc := range tests {
		got, id := headingText(tc.in, refs)
		if got != tc.want || id != tc.id {
			t.Errorf("headingText(%q) = %q, %q; want %q, %q", tc.in, got, id, tc.want, tc.id)
		}
	}
}

func TestProseLines_SkipsFencedBlocks(t *testing.T) {
	src := "a\n````md\n```\nstill inside\n````\nb\n~~~\n# no\n~~~\nc"
	if got := strings.Join(proseLines(src), ","); got != "a,b,c" {
		t.Errorf("proseLines = %q, want a,b,c", got)
	}
}
