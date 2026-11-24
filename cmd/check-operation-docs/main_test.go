package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// referenceDir is the directory of pages this check exists to hold, reached from the
// package directory.
var referenceDir = filepath.Join("..", "..", "docs", "reference")

// theReference runs against the real pages. A test that runs the real thing is worth having
// on top of the fixture tests: the fixtures pin the parser's behavior, and this pins that the
// parser's behavior still fits the pages it is pointed at — a page renamed by hand would
// otherwise only surface in CI.
var theReference = []string{
	"-reference", referenceDir,
	"-baseline", filepath.Join("..", "..", "scripts", "undocumented-operations.txt"),
}

func TestRun_TheReferenceAgreesWithTheRouter(t *testing.T) {
	var out bytes.Buffer
	if err := run(theReference, &out); err != nil {
		t.Fatalf("the reference disagrees with the router:\n%s\n%v", out.String(), err)
	}
	if !strings.Contains(out.String(), "no row names an unrouted operation") {
		t.Errorf("want the both-directions summary, got %q", out.String())
	}
}

func TestRun_ReportsAStaleBaselineLine(t *testing.T) {
	baseline := filepath.Join(t.TempDir(), "undocumented-operations.txt")
	// s3 routes CreateBucket and the reference has a row for it, so a baseline claiming it
	// undocumented is a record of a defect that is not there.
	if err := os.WriteFile(baseline, []byte("# header\ns3\tCreateBucket\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := run(append(append([]string{}, theReference[:2]...), "-baseline", baseline), &out)
	if err == nil {
		t.Fatal("want an error for a stale baseline line")
	}
	if !strings.Contains(out.String(), "s3 CreateBucket now has a row") {
		t.Errorf("want the stale line named, got %q", out.String())
	}
	// Every operation genuinely lacking a row is also absent from this baseline, so the run
	// must report those as new drift rather than only the stale line.
	if !strings.Contains(err.Error(), "stale baseline lines") {
		t.Errorf("want both counts in the summary, got %v", err)
	}
}

func TestRun_RefusesABaselineItCannotParse(t *testing.T) {
	baseline := filepath.Join(t.TempDir(), "undocumented-operations.txt")
	if err := os.WriteFile(baseline, []byte("s3 CreateBucket extra\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := run(append(append([]string{}, theReference[:2]...), "-baseline", baseline), &out)
	if err == nil || !strings.Contains(err.Error(), "want `plugin Operation`") {
		t.Fatalf("want a parse refusal naming the format, got %v", err)
	}
	if !strings.Contains(err.Error(), ":1:") {
		t.Errorf("want the line number, got %v", err)
	}
}

func TestRun_RefusesWhatItCannotRead(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	var out bytes.Buffer
	if err := run([]string{"-reference", missing}, &out); err == nil ||
		!strings.Contains(err.Error(), "read "+missing) {
		t.Fatalf("want a read failure naming the file, got %v", err)
	}
	if err := run(append(append([]string{}, theReference[:2]...),
		"-baseline", filepath.Join(t.TempDir(), "absent.txt")), &out); err == nil ||
		!strings.Contains(err.Error(), "absent.txt") {
		t.Fatalf("want a read failure naming the baseline, got %v", err)
	}
}

func TestRun_RefusesAnUnknownFlag(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"-nope"}, &out); err == nil || !strings.Contains(err.Error(), "parse flags") {
		t.Fatalf("want a flag parse failure, got %v", err)
	}
}

func TestRun_WriteRoundTrips(t *testing.T) {
	baseline := filepath.Join(t.TempDir(), "undocumented-operations.txt")
	args := append(append([]string{}, theReference[:2]...), "-baseline", baseline, "-write")
	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !strings.Contains(out.String(), "operations with no row") {
		t.Errorf("want the written count, got %q", out.String())
	}
	written, err := os.ReadFile(baseline) //nolint:gosec // a path this test made.
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(written), "# undocumented-operations.txt") {
		t.Error("want the header in the written file")
	}
	if !strings.Contains(string(written), "INVENTORY OF DEFECTS") {
		t.Error("want the file to say what it is; a bare list of names reads as an approved set")
	}
	// The file it just wrote must be the file the check accepts, or `make
	// operation-docs-write` would produce something that fails the next run.
	out.Reset()
	if err := run(args[:4], &out); err != nil {
		t.Fatalf("the check rejects the baseline -write produced: %v\n%s", err, out.String())
	}
	committed, err := os.ReadFile(filepath.Join("..", "..", "scripts", "undocumented-operations.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(written, committed) {
		t.Error("the committed baseline is not what -write produces; run `make operation-docs-write`")
	}
}

func TestRun_WriteRefusesAPathItCannotWrite(t *testing.T) {
	var out bytes.Buffer
	err := run(append(append([]string{}, theReference[:2]...),
		"-baseline", filepath.Join(t.TempDir(), "absent", "b.txt"), "-write"), &out)
	if err == nil || !strings.Contains(err.Error(), "write ") {
		t.Fatalf("want a write failure naming the file, got %v", err)
	}
}

// copyReference copies the real pages into a temporary directory, so a test can damage one
// and assert the check notices.
func copyReference(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(referenceDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		src, err := os.ReadFile(filepath.Join(referenceDir, e.Name())) //nolint:gosec // a test fixture path.
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), src, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func runOn(t *testing.T, dir string, extra ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	args := append([]string{"-reference", dir, "-baseline", theReference[3]}, extra...)
	err := run(args, &out)
	return out.String(), err
}

func TestRun_RefusesARemovedRow(t *testing.T) {
	dir := copyReference(t)
	page := filepath.Join(dir, "s3.md")
	src, err := os.ReadFile(page) //nolint:gosec // a path this test made.
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, line := range strings.Split(string(src), "\n") {
		if !strings.HasPrefix(line, "| CreateBucket |") {
			kept = append(kept, line)
		}
	}
	if len(kept) == len(strings.Split(string(src), "\n")) {
		t.Fatal("fixture drift: s3.md has no CreateBucket row to remove")
	}
	if err := os.WriteFile(page, []byte(strings.Join(kept, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runOn(t, dir)
	if err == nil {
		t.Fatal("want a refusal for a routed operation with no row")
	}
	if !strings.Contains(out, "s3.md: s3 routes CreateBucket and no row in its operation table names it") {
		t.Errorf("want the page and operation named, got %q", out)
	}
}

func TestRun_RefusesAnOrphanPage(t *testing.T) {
	dir := copyReference(t)
	if err := os.WriteFile(filepath.Join(dir, "glacier.md"), []byte("# Glacier\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := runOn(t, dir)
	if err == nil || !strings.Contains(err.Error(), "glacier.md names no registered plugin") {
		t.Fatalf("want the orphan named, got %v", err)
	}
}

func TestRun_RefusesAPluginWithNoPage(t *testing.T) {
	dir := copyReference(t)
	if err := os.Remove(filepath.Join(dir, "sqs.md")); err != nil {
		t.Fatal(err)
	}
	_, err := runOn(t, dir)
	if err == nil || !strings.Contains(err.Error(), `plugin "sqs" has no page`) ||
		!strings.Contains(err.Error(), "-scaffold sqs") {
		t.Fatalf("want the missing page named with the scaffold hint, got %v", err)
	}
}

func TestRun_ReadsOnlyTopLevelPages(t *testing.T) {
	// A concept page has no plugin and is not an orphan, because it lives one level down.
	dir := copyReference(t)
	if err := os.MkdirAll(filepath.Join(dir, "concepts"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "concepts", "routing.md"), []byte("# Routing\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runOn(t, dir); err != nil {
		t.Fatalf("a concepts/ page was read as a plugin page: %v\n%s", err, out)
	}
}

func TestScaffold(t *testing.T) {
	dir := copyReference(t)
	tests := []struct {
		name   string
		damage func(t *testing.T)
		plugin string
		want   []string
		absent []string
	}{
		{
			name:   "a complete page",
			plugin: "sqs",
			want:   []string{"sqs.md has a row for every operation sqs routes"},
		},
		{
			name:   "no page",
			damage: func(t *testing.T) { _ = os.Remove(filepath.Join(dir, "sqs.md")) },
			plugin: "sqs",
			want:   []string{"# SQS\n", "## Supported operations", "| Operation | Notes |", "| SendMessage | |"},
		},
		{
			name: "a page with no operation table",
			damage: func(t *testing.T) {
				if err := os.WriteFile(filepath.Join(dir, "sqs.md"), []byte("# SQS\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			plugin: "sqs",
			want:   []string{"| Operation | Notes |", "| SendMessage | |"},
			absent: []string{"## Supported operations"},
		},
		{
			name: "a page missing one row",
			damage: func(t *testing.T) {
				src := "# SQS\n\n| Operation | Notes |\n|---|---|\n"
				for _, op := range routedCatalog()["sqs"] {
					if op != "SendMessage" {
						src += "| " + op + " | |\n"
					}
				}
				if err := os.WriteFile(filepath.Join(dir, "sqs.md"), []byte(src), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			plugin: "sqs",
			want:   []string{"| SendMessage | |\n"},
			absent: []string{"| Operation |", "| ReceiveMessage |"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.damage != nil {
				tc.damage(t)
			}
			out, err := runOn(t, dir, "-scaffold", tc.plugin)
			if err != nil {
				t.Fatalf("scaffold: %v", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("want %q in:\n%s", w, out)
				}
			}
			for _, a := range tc.absent {
				if strings.Contains(out, a) {
					t.Errorf("want no %q in:\n%s", a, out)
				}
			}
		})
	}
	if _, err := runOn(t, dir, "-scaffold", "glacier"); err == nil ||
		!strings.Contains(err.Error(), "no registered plugin has that name") {
		t.Errorf("want an unknown plugin refused, got %v", err)
	}
}

func TestCheckPages_NamesBothDirections(t *testing.T) {
	catalog := map[string][]string{"alpha": {"DoThing"}, "beta": nil}
	err := checkPages("ref", catalog, map[string]string{"alpha": "", "gamma": ""})
	if err == nil {
		t.Fatal("want a refusal")
	}
	for _, want := range []string{`plugin "beta" has no page`, filepath.Join("ref", "gamma.md") + " names no registered plugin"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("want %q in %v", want, err)
		}
	}
	if err := checkPages("ref", catalog, map[string]string{"alpha": "", "beta": ""}); err != nil {
		t.Errorf("want agreement accepted, got %v", err)
	}
}

func TestParseOperationTable_ScopesToTheFirstOperationTable(t *testing.T) {
	tests := []struct {
		name  string
		src   string
		rows  []string
		found bool
	}{
		{
			name: "rows unwrapped, later tables ignored",
			src: `# Alpha

| Operation | Notes |
|-----------|-------|
| [DoThing](https://example.invalid/DoThing) | a linked row still counts |
| ` + "`DoOther`" + ` | so does a code-formatted one |
| DoThing (batch) | not an operation name |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Alpha::Thing | DoMissing | a later table cannot document an operation |
`,
			rows:  []string{"DoThing", "DoOther", "DoThing (batch)"},
			found: true,
		},
		{
			name: "a table that is not the operation table comes first",
			src: `# Beta

| Type | Notes |
|------|-------|
| DoElsewhere | |

| Operation | Notes |
|-----------|-------|
| DoBeta | |
`,
			rows:  []string{"DoBeta"},
			found: true,
		},
		{
			name: "a fenced table is an example, not a claim",
			src:  "# Gamma\n\n```\n| Operation | Notes |\n|---|---|\n| DoFenced | |\n```\n",
		},
		{
			name: "a longer fence holds a shorter one",
			src: "# Delta\n\n````md\n```\n| Operation | Notes |\n|---|---|\n| DoFenced | |\n````\n\n" +
				"| Operation | Notes |\n|---|---|\n| DoDelta | |\n",
			rows:  []string{"DoDelta"},
			found: true,
		},
		{
			name: "a tilde fence closes only on tildes",
			src:  "# Epsilon\n\n~~~\n```\n| Operation | Notes |\n|---|---|\n| DoFenced | |\n~~~\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, found := parseOperationTable(tc.src)
			if found != tc.found || !equal(rows, tc.rows) {
				t.Errorf("got %q, found=%v; want %q, found=%v", rows, found, tc.rows, tc.found)
			}
		})
	}
}

func TestCompare_BothDirections(t *testing.T) {
	catalog := map[string][]string{"alpha": {"DoThing", "DoUndocumented"}}
	tables := map[string][]string{"alpha": {"DoThing", "DoPhantom", "DoThing (batch)"}}
	undocumented, phantom := compare(catalog, tables)
	if len(undocumented) != 1 || undocumented[0].op != "DoUndocumented" {
		t.Errorf("undocumented = %v, want alpha DoUndocumented", undocumented)
	}
	// "DoThing (batch)" is not an operation name, so it is neither a row that documents one
	// nor a row that claims one that is missing.
	if len(phantom) != 1 || phantom[0].op != "DoPhantom" {
		t.Errorf("phantom = %v, want alpha DoPhantom", phantom)
	}
}

func TestCompare_SortsByPluginThenOperation(t *testing.T) {
	catalog := map[string][]string{"beta": {"Bb", "Aa"}, "alpha": {"Zz"}}
	undocumented, _ := compare(catalog, nil)
	var got []string
	for _, g := range undocumented {
		got = append(got, g.String())
	}
	want := []string{"alpha\tZz", "beta\tAa", "beta\tBb"}
	if !equal(got, want) {
		t.Errorf("order = %q, want %q; a map iteration order would make the baseline churn", got, want)
	}
}

func TestCells_UnwrapsAndTrims(t *testing.T) {
	tests := []struct {
		line string
		want []string
	}{
		{"| `DoThing` | a note |", []string{"DoThing", "a note"}},
		{"|[DoThing](#do-thing)|", []string{"DoThing"}},
		{"| | a note |", []string{"", "a note"}},
		{"||", nil}, // nothing between the pipes, so no cells and no first column to read.
		{"", nil},
		{"|", nil},
	}
	for _, tc := range tests {
		if got := cells(tc.line); !equal(got, tc.want) {
			t.Errorf("cells(%q) = %q, want %q", tc.line, got, tc.want)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
