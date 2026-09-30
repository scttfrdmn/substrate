package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// theReference is the reference this check exists to hold, reached from the package
// directory. A test that runs the real thing is worth having on top of the fixture tests:
// the fixtures pin the parser's behavior, and this pins that the parser's behavior still
// fits the file it is pointed at — a section renamed by hand would otherwise only surface
// in CI.
var theReference = []string{
	"-docs", filepath.Join("..", "..", "docs", "services.md"),
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
	missing := filepath.Join(t.TempDir(), "absent.md")
	var out bytes.Buffer
	if err := run([]string{"-docs", missing}, &out); err == nil ||
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

func TestDocSections_CoversEveryRoutedService(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "docs", "services.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkSections(routedCatalog(), docSections, parseOperationTables(string(src))); err != nil {
		t.Fatal(err)
	}
}

func TestCheckSections_NamesEveryWayTheMapCanRot(t *testing.T) {
	catalog := map[string][]string{"alpha": {"DoThing"}, "beta": nil}
	tables := map[string][]string{"Alpha": {"DoThing"}}
	tests := []struct {
		name     string
		sections map[string]string
		want     string
	}{
		{
			name:     "plugin with no entry",
			sections: map[string]string{"alpha": "Alpha"},
			want:     `plugin "beta" has no docSections entry`,
		},
		{
			name:     "entry naming a heading the file lacks",
			sections: map[string]string{"alpha": "Alpha", "beta": "Beta"},
			want:     `maps to heading "Beta", which the reference does not have`,
		},
		{
			name:     "entry for a plugin that is not registered",
			sections: map[string]string{"alpha": "Alpha", "beta": "Alpha", "gamma": "Alpha"},
			want:     `entry for "gamma", which is not a registered plugin`,
		},
		{
			name:     "two plugins claiming one heading",
			sections: map[string]string{"alpha": "Alpha", "beta": "Alpha"},
			want:     `heading "Alpha" is claimed by alpha and beta`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSections(catalog, tc.sections, tables)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestParseOperationTables_ScopesToTheFirstOperationTable(t *testing.T) {
	src := `# Reference

## Alpha

| Operation | Notes |
|-----------|-------|
| [DoThing](https://example.invalid/DoThing) | a linked row still counts |
| ` + "`DoOther`" + ` | so does a code-formatted one |
| DoThing (batch) | not an operation name |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Alpha::Thing | DoMissing | a later table cannot document an operation |

## Beta

Prose, then a table that is not the operation table, then the operation table.

| Type | Notes |
|------|-------|
| DoElsewhere | |

| Operation | Notes |
|-----------|-------|
| DoBeta | |

## Gamma

` + "```" + `
## NotASection

| Operation | Notes |
|-----------|-------|
| DoFenced | |
` + "```" + `
`
	tables := parseOperationTables(src)
	if got, want := tables["Alpha"], []string{"DoThing", "DoOther", "DoThing (batch)"}; !equal(got, want) {
		t.Errorf("Alpha rows = %q, want %q", got, want)
	}
	if got, want := tables["Beta"], []string{"DoBeta"}; !equal(got, want) {
		t.Errorf("Beta rows = %q, want %q", got, want)
	}
	if rows, ok := tables["Gamma"]; !ok || rows != nil {
		t.Errorf("Gamma = %q, ok=%v; a fenced block is an example, not a claim", rows, ok)
	}
	if _, ok := tables["NotASection"]; ok {
		t.Error("a heading inside a fenced block is not a section")
	}
}

func TestCompare_BothDirections(t *testing.T) {
	catalog := map[string][]string{"alpha": {"DoThing", "DoUndocumented"}}
	tables := map[string][]string{"Alpha": {"DoThing", "DoPhantom", "DoThing (batch)"}}
	undocumented, phantom := compare(catalog, map[string]string{"alpha": "Alpha"}, tables)
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
	undocumented, _ := compare(catalog, map[string]string{"alpha": "A", "beta": "B"}, nil)
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
