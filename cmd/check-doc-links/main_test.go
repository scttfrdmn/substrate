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

// helper links its neighbours: [Plugin], [*Plugin], [Plugin.handle], [Plugin.state],
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
// BAD [Plugin.missingMethod].
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
	want := []string{"[missingFunc]", "[Plugin.missingMethod]", "[Missing.handle]", "[other.missing]"}
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
