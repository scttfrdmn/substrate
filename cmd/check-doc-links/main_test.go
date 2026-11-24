package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is one package whose comments hold every link form the check must judge. Each
// line marked BAD must be reported and no other.
const fixture = `package fix

import (
	"github.com/go-chi/chi/v5"
	jsonx "encoding/json"
)

// Plugin is a type with a method and a field.
type Plugin struct {
	state int
	chi.Router
}

// Lister is an interface.
type Lister interface{ list() }

func (p *Plugin) handle() {}

var registry = map[string]int{}

const limit = 5

// helper links its neighbors: [Plugin], [*Plugin], [Plugin.handle], [Plugin.state],
// [Plugin.Router], [Lister.list], [handle], [registry], [limit], [error], [nil],
// [chi.Router.Group], [jsonx.Marshal], [strings.Cut], [other.Exported], and
// [TestHelper] from the external test package.
//
// A Markdown link [text](https://example.com) and an editorial "parameter [that]
// exceeds", spanning "a quote [is]
// open" across lines, are not links. Neither is x[i] or a[0], nor [API_Page], which
// the file defines as a URL link.
//
//	code := m[key] // [notALink] in a code block
//
// [API_Page]: https://docs.aws.amazon.com/
func helper() {}

// BAD [missingFunc].
// BAD [Lister.missingMethod]. ([Plugin.anyMethod] resolves: chi.Router may promote it.)
// BAD [Missing.handle].
// BAD [other.missing].
func bad() {}
`

const fixtureTest = `package fix_test

// TestHelper pins [helper], unqualified, from the external test package.
func TestHelper() {}
`

const other = `package other

// Exported is the target of a cross-package link.
func Exported() {}
`

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRun_ReportsExactlyTheDanglingLinks(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "fix", "fix.go"), fixture)
	writeFile(t, filepath.Join(root, "fix", "fix_test.go"), fixtureTest)
	writeFile(t, filepath.Join(root, "other", "other.go"), other)
	writeFile(t, filepath.Join(root, "node_modules", "skip", "skip.go"), "package skip\n\n// [nothing].\nfunc f() {}\n")

	var out bytes.Buffer
	n, err := run([]string{root}, &out)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"[missingFunc]", "[Lister.missingMethod]", "[Missing.handle]", "[other.missing]"}
	got := strings.Split(strings.TrimSpace(out.String()), "\n")
	if n != len(want) || len(got) != len(want) {
		t.Fatalf("want %d dangling links, got %d:\n%s", len(want), n, out.String())
	}
	for i, w := range want {
		if !strings.HasSuffix(got[i], w) || !strings.Contains(got[i], "fix.go:") {
			t.Errorf("line %d: want …fix.go:N: %s, got %s", i, w, got[i])
		}
	}
}

func TestRun_ParseErrorIsAnError(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "broken.go"), "package broken\nfunc {")
	if _, err := run([]string{root}, &bytes.Buffer{}); err == nil {
		t.Fatal("want a parse error")
	}
}

func TestRun_MissingRootIsAnError(t *testing.T) {
	if _, err := run([]string{filepath.Join(t.TempDir(), "absent")}, &bytes.Buffer{}); err == nil {
		t.Fatal("want a walk error")
	}
}

// TestRun_TheRepositoryHasNoDanglingLinks runs the check over the real tree, so a
// dangling link fails `make test` as well as the CI step.
func TestRun_TheRepositoryHasNoDanglingLinks(t *testing.T) {
	var out bytes.Buffer
	n, err := run([]string{filepath.Join("..", "..")}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d dangling doc links:\n%s", n, out.String())
	}
}

// danglingTargets runs the check over files (path relative to a fresh root → contents)
// and returns the reported targets, in report order.
func danglingTargets(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := t.TempDir()
	for path, body := range files {
		writeFile(t, filepath.Join(root, path), body)
	}
	var out bytes.Buffer
	n, err := run([]string{root}, &out)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		got = append(got, line[strings.LastIndex(line, " ")+1:])
	}
	if n != len(got) {
		t.Fatalf("run reported %d but printed %d lines:\n%s", n, len(got), out.String())
	}
	return got
}

func assertTargets(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("dangling links:\n got  %v\n want %v", got, want)
	}
}

// TestRun_SelfQualifiedLinksAreChecked pins that a link qualified by its own package's
// name is resolved, not waved through as a package link, from both the package and its
// external test package.
func TestRun_SelfQualifiedLinksAreChecked(t *testing.T) {
	got := danglingTargets(t, map[string]string{
		"foo/foo.go": "package foo\n\n// Real links [foo.Real]; BAD [foo.Missing].\nfunc Real() {}\n",
		"foo/foo_test.go": "package foo_test\n\nimport \"example.com/foo\"\n\n" +
			"// TestReal pins [foo.Real]; BAD [foo.Gone].\nfunc TestReal() { foo.Real() }\n",
	})
	assertTargets(t, got, []string{"[foo.Missing]", "[foo.Gone]"})
}

// TestRun_QuotesAreJudgedPerLine pins that an unbalanced quotation mark switches checking
// off for nothing beyond its own line, that a mark inside a backtick code span is not a
// quotation, and that a quotation closing on a later line still hides its bracket.
func TestRun_QuotesAreJudgedPerLine(t *testing.T) {
	got := danglingTargets(t, map[string]string{
		"q/q.go": "package q\n\n" +
			"// An inch mark 5\" opens nothing for the next line:\n" +
			"// BAD [missingA].\n" +
			"// The JSON key `\"Tags\"` is code, so BAD [missingB] is checked.\n" +
			"// AWS says \"Unresolved resource dependencies\n" +
			"// [X] in the Resources block\", which is quoted text.\n" +
			"// BAD [missingC] comes before \"a quotation that runs\n" +
			"// on\" to the next line.\n" +
			"func f() {}\n",
	})
	assertTargets(t, got, []string{"[missingA]", "[missingB]", "[missingC]"})
}

// TestRun_PromotedMembersResolve pins that a method or field promoted through an embedded
// type resolves, through any depth of local embedding and through a named field's type,
// that an embedding outside the repository accepts the link, and that an embedding cycle
// terminates.
func TestRun_PromotedMembersResolve(t *testing.T) {
	got := danglingTargets(t, map[string]string{
		"e/e.go": `package e

import "github.com/go-chi/chi/v5"

// Leaf is reached through a field.
type Leaf struct{}

// L is Leaf's method.
func (Leaf) L() {}

// Inner is embedded in Outer.
type Inner struct {
	F *Leaf
}

// M is promoted to Outer.
func (*Inner) M() {}

// Outer embeds Inner.
type Outer struct {
	*Inner
}

// Wrap embeds a type from outside the repository.
type Wrap struct {
	chi.Router
}

// A and B embed each other.
type A struct{ B }

// B embeds A.
type B struct{ A }

// Good: [Outer.M], [Outer.Inner.M], [Outer.F], [Outer.F.L], [Outer.Inner.F.L],
// [Wrap.Group], [Wrap.Router.Group].
//
// BAD [Outer.Nope].
// BAD [Outer.Inner.Nope].
// BAD [Outer.F.Nope].
// BAD [Outer.Nope.L].
// BAD [A.Nope].
func f() {}
`,
	})
	assertTargets(t, got, []string{"[Outer.Nope]", "[Outer.Inner.Nope]", "[Outer.F.Nope]", "[Outer.Nope.L]", "[A.Nope]"})
}
