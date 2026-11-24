// Command check-operation-docs fails when the service reference pages and the operation
// catalog disagree about which operations substrate routes.
//
// Each registered plugin has one page, docs/reference/<plugin>.md, named by its registry
// name so the pairing needs no hand-kept map. A plugin with no page fails, and so does a
// page naming no registered plugin: an orphan is either a renamed plugin's leftover or a
// cross-cutting page in the wrong directory (those live in docs/reference/concepts/).
//
// It asks the question #1015's acceptance criterion states, in both directions:
//
//   - a routed operation with no row in its own service's operation table is an
//     undocumented operation, and must be recorded in the baseline or it fails;
//   - a row naming an operation no plugin routes is a phantom row, and always fails.
//
// Why a Go command when the tree's other four drift checks are shell: the operation set
// has to come from [emulator.RoutedOperations] rather than from a grep over the plugin
// sources. Taking it from a grep is what four disagreeing hand counts of "how many
// operations does substrate route" cost — 945, 849, 950 and 973 across #1015, #1093,
// #1094 and #1231, none of them right. The catalog is generated from each plugin's
// dispatch (#1095), so the number here is the router's own.
//
// # Asserting, not generating
//
// The operation tables are hand-written and stay that way. Their value is the notes
// column — what is modeled, what is not, and where substrate diverges — and none of that
// is derivable from the router. Generating the tables would put those notes inside a
// generated block, where the next hand edit is either lost on the next `make` or has to
// be threaded through a generator. So this checks the *set* of rows and never touches
// their content; `-scaffold <plugin>` prints the rows a page lacks, for a writer to paste
// in and annotate. The generated coverage matrix in docs/services.md and the hand-written
// pages it links to split the same way.
//
// # The baseline
//
// scripts/undocumented-operations.txt is an inventory of defects on the model of
// scripts/wire-bookkeeping-baseline.txt, not an allowlist: the check fails both on an
// undocumented operation missing from it (new drift) and on a line whose row now exists (a
// stale record). Writing a row must shrink the file in the same commit. #1231 is the issue
// that empties it.
//
// The alternative — hold this check back until every row is written — leaves the next
// routed operation unpoliced while 110 rows get written, and makes a documentation sitting
// a blocker for a check. The ratchet stops the recurrence today and lets the backlog drain
// on its own schedule.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/scttfrdmn/substrate/emulator"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "check-operation-docs:", err)
		os.Exit(1)
	}
}

// operationNamePattern is what an AWS operation name looks like. A first-column cell that
// does not match it — "PutObject (multipart)", a type name, a prose note — names no
// operation, so it is neither a row that documents one nor a phantom row.
var operationNamePattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]+$`)

// gap is one operation that is routed and has no row, or one row that names no routed
// operation. It is the baseline's line format: plugin, then operation, tab-separated.
type gap struct {
	plugin string
	op     string
}

func (g gap) String() string { return g.plugin + "\t" + g.op }

// routedCatalog reads the router's own operation set. [checkPages] and [compare] take it
// as an argument rather than reaching for it, so a test can hand them a small catalog and a
// fixture reference instead of the whole tree.
func routedCatalog() map[string][]string {
	services := emulator.RoutedServices()
	catalog := make(map[string][]string, len(services))
	for _, s := range services {
		catalog[s] = emulator.RoutedOperations(s)
	}
	return catalog
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("check-operation-docs", flag.ContinueOnError)
	fs.SetOutput(stdout)
	dir := fs.String("reference", filepath.Join("docs", "reference"), "directory holding one <plugin>.md page per plugin")
	baselinePath := fs.String("baseline", filepath.Join("scripts", "undocumented-operations.txt"),
		"path to the inventory of operations known to have no row")
	write := fs.Bool("write", false, "rewrite the baseline from the tree instead of checking it")
	scaffold := fs.String("scaffold", "", "print the operation rows the named plugin's page lacks, and exit")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	pages, err := readPages(*dir)
	if err != nil {
		return err
	}
	catalog := routedCatalog()
	if *scaffold != "" {
		return writeScaffold(stdout, *dir, *scaffold, catalog, pages)
	}
	if err := checkPages(*dir, catalog, pages); err != nil {
		return err
	}
	tables := make(map[string][]string, len(pages))
	for plugin, src := range pages {
		tables[plugin], _ = parseOperationTable(src)
	}
	undocumented, phantom := compare(catalog, tables)

	if *write {
		if err := writeBaseline(*baselinePath, undocumented); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "wrote %s (%d operations with no row)\n", *baselinePath, len(undocumented))
		return nil
	}

	known, err := readBaseline(*baselinePath)
	if err != nil {
		return err
	}
	return report(stdout, *dir, *baselinePath, undocumented, phantom, known)
}

// readPages returns the source of each <plugin>.md directly under dir, keyed by plugin.
// Subdirectories are not read: docs/reference/concepts/ holds the pages no plugin owns.
func readPages(dir string) (map[string]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	pages := make(map[string]string, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".md") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // a page under a directory named by a flag on a developer tool.
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", filepath.Join(dir, name), err)
		}
		pages[strings.TrimSuffix(name, ".md")] = string(src)
	}
	return pages, nil
}

// parseOperationTable returns the first-column cells of the first table in a page whose
// header's first column is exactly "Operation", and whether the page has one.
//
// Scoping to that one table is the difference between this check and every hand count
// before it. Asking only whether a name appears somewhere is how Lambda's AddPermission
// counted as documented because SNS has a row for it (#1084). Taking any table on the page
// would count a name appearing in a resource-type or error-code table. The operation table
// is the thing headed "Operation", and it is the claim a reader takes the page to be making.
func parseOperationTable(src string) ([]string, bool) {
	fenced := false
	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimRight(lines[i], " \t")
		if strings.HasPrefix(line, "```") {
			fenced = !fenced
			continue
		}
		if fenced || !strings.HasPrefix(line, "|") {
			continue
		}
		// A table starts here. Consume it whole either way, so a later table cannot be
		// mistaken for a continuation of this one.
		header := cells(line)
		start := i
		for i+1 < len(lines) && strings.HasPrefix(lines[i+1], "|") {
			i++
		}
		if len(header) == 0 || header[0] != "Operation" {
			continue
		}
		var rows []string
		// start is the header, start+1 the separator; rows run to the end of the table.
		for j := start + 2; j <= i; j++ {
			if c := cells(lines[j]); len(c) > 0 && c[0] != "" {
				rows = append(rows, c[0])
			}
		}
		return rows, true
	}
	return nil, false
}

// cells splits one Markdown table row, unwrapping links and stripping the backticks a row
// may use, so that a linked or code-formatted operation name reads the same as a bare one.
func cells(line string) []string {
	trimmed := strings.Trim(strings.TrimSpace(line), "|")
	if trimmed == "" {
		return nil
	}
	out := strings.Split(trimmed, "|")
	for i, c := range out {
		c = linkPattern.ReplaceAllString(c, "$1")
		out[i] = strings.TrimSpace(strings.ReplaceAll(c, "`", ""))
	}
	return out
}

// linkPattern matches a Markdown inline link, capturing its text.
var linkPattern = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)

// checkPages fails when the pages and the registry disagree about which plugins exist,
// before any operation is compared: a plugin with no page would otherwise report every one
// of its operations as undocumented, and a page with no plugin would be checked by nothing.
func checkPages(dir string, catalog map[string][]string, pages map[string]string) error {
	var problems []string
	for plugin := range catalog {
		if _, ok := pages[plugin]; !ok {
			problems = append(problems, fmt.Sprintf("plugin %q has no page %s; `go run ./cmd/check-operation-docs "+
				"-scaffold %s` prints one to start from", plugin, filepath.Join(dir, plugin+".md"), plugin))
		}
	}
	for plugin := range pages {
		if _, ok := catalog[plugin]; !ok {
			problems = append(problems, fmt.Sprintf("%s names no registered plugin; a page is named by the "+
				"plugin's registry name, and a page no plugin owns belongs in %s",
				filepath.Join(dir, plugin+".md"), filepath.Join(dir, "concepts")))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("the reference pages disagree with the registry:\n  %s", strings.Join(problems, "\n  "))
}

// writeScaffold prints what the named plugin's page lacks: the whole page skeleton when
// there is no page, otherwise the rows its operation table is missing (or the table
// itself). The rows carry no notes, because the notes are the part only a writer can add.
func writeScaffold(stdout io.Writer, dir, plugin string, catalog map[string][]string, pages map[string]string) error {
	ops, ok := catalog[plugin]
	if !ok {
		return fmt.Errorf("-scaffold %s: no registered plugin has that name", plugin)
	}
	src, hasPage := pages[plugin]
	rows, hasTable := parseOperationTable(src)
	documented := make(map[string]bool, len(rows))
	for _, r := range rows {
		documented[r] = true
	}
	var missing []string
	for _, op := range ops {
		if !documented[op] {
			missing = append(missing, op)
		}
	}
	sort.Strings(missing)

	var b strings.Builder
	switch {
	case !hasPage:
		fmt.Fprintf(&b, "<!-- %s -->\n# %s\n\n## Supported operations\n\n", filepath.Join(dir, plugin+".md"),
			emulator.PluginRoutingCatalog()[plugin].Display)
		b.WriteString("| Operation | Notes |\n|-----------|-------|\n")
	case !hasTable:
		b.WriteString("| Operation | Notes |\n|-----------|-------|\n")
	case len(missing) == 0:
		_, _ = fmt.Fprintf(stdout, "%s has a row for every operation %s routes\n", filepath.Join(dir, plugin+".md"), plugin)
		return nil
	}
	for _, op := range missing {
		fmt.Fprintf(&b, "| %s | |\n", op)
	}
	_, _ = io.WriteString(stdout, b.String())
	return nil
}

// compare returns the routed operations with no row, and the rows naming no routed
// operation, both sorted.
func compare(catalog map[string][]string, tables map[string][]string) (undocumented, phantom []gap) {
	for plugin, ops := range catalog {
		rows := tables[plugin]
		documented := make(map[string]bool, len(rows))
		for _, r := range rows {
			documented[r] = true
		}
		routed := make(map[string]bool, len(ops))
		for _, op := range ops {
			routed[op] = true
			if !documented[op] {
				undocumented = append(undocumented, gap{plugin, op})
			}
		}
		for _, r := range rows {
			if !routed[r] && operationNamePattern.MatchString(r) {
				phantom = append(phantom, gap{plugin, r})
			}
		}
	}
	sortGaps(undocumented)
	sortGaps(phantom)
	return undocumented, phantom
}

func sortGaps(gaps []gap) {
	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].plugin != gaps[j].plugin {
			return gaps[i].plugin < gaps[j].plugin
		}
		return gaps[i].op < gaps[j].op
	})
}

// baselineHeader is the preamble writeBaseline emits. It says what the file is for in the
// file itself, because a bare list of names reads as an approved set and this is the
// opposite of one.
const baselineHeader = `# undocumented-operations.txt — every operation substrate routes that has no row in
# its service's operation table in docs/reference/<plugin>.md, as of #1015.
#
# Columns, tab-separated:  registered-plugin-name  Operation
#
# THIS FILE IS AN INVENTORY OF DEFECTS, NOT A SUPPRESSION LIST. Each reference page heads
# its table "Supported operations", so a reader takes the table to be the
# list — and for these operations it was short. Every line was expected to be deleted as
# its row was written, which is why cmd/check-operation-docs fails both on an operation
# missing from this file (new drift: an operation routed without being documented) and
# on a line whose row now exists (a stale record). Writing a row must shrink this file
# in the same commit.
#
# #1231 emptied it: every routed operation has a row, so the file is this header and
# nothing else. A line reappearing here is a new defect, not inherited debt — the
# check adds one only when an operation is routed with no row, and refuses the run.
#
# The reverse direction — a row naming an operation no plugin routes — has no baseline
# and never will: the five that existed were fixed when this check landed, so a phantom
# row always fails. That is the defect class #1084 found, where the reference claimed
# Lambda routed InvokeFunction and it does not.
#
# Regenerate after writing rows:  make operation-docs-write
`

func writeBaseline(path string, undocumented []gap) error {
	var b strings.Builder
	b.WriteString(baselineHeader)
	for _, g := range undocumented {
		b.WriteString(g.String())
		b.WriteString("\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil { //nolint:gosec // a checked-in inventory.
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func readBaseline(path string) (map[gap]bool, error) {
	f, err := os.Open(path) //nolint:gosec // path comes from a flag on a developer tool.
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck // read-only.

	known := make(map[gap]bool)
	scan := bufio.NewScanner(f)
	for line := 1; scan.Scan(); line++ {
		text := strings.TrimSpace(scan.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s:%d: want `plugin Operation`, got %q", path, line, text)
		}
		known[gap{fields[0], fields[1]}] = true
	}
	if err := scan.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return known, nil
}

// report prints every disagreement and returns an error when there is one. It prints all
// three kinds before failing rather than stopping at the first, so one run tells a reader
// everything to fix.
func report(stdout io.Writer, dir, baselinePath string, undocumented, phantom []gap, known map[gap]bool) error {
	page := func(plugin string) string { return filepath.Join(dir, plugin+".md") }
	var newDrift []gap
	for _, g := range undocumented {
		if !known[g] {
			newDrift = append(newDrift, g)
		}
	}
	current := make(map[gap]bool, len(undocumented))
	for _, g := range undocumented {
		current[g] = true
	}
	var stale []gap
	for g := range known {
		if !current[g] {
			stale = append(stale, g)
		}
	}
	sortGaps(stale)

	for _, g := range newDrift {
		_, _ = fmt.Fprintf(stdout, "%s: %s routes %s and no row in its operation table names it\n",
			page(g.plugin), g.plugin, g.op)
	}
	for _, g := range stale {
		_, _ = fmt.Fprintf(stdout, "%s: %s %s now has a row; delete the line\n", baselinePath, g.plugin, g.op)
	}
	for _, g := range phantom {
		_, _ = fmt.Fprintf(stdout, "%s: the %s operation table has a row for %s, which no plugin routes\n",
			page(g.plugin), g.plugin, g.op)
	}

	switch {
	case len(newDrift) > 0 || len(stale) > 0 || len(phantom) > 0:
		return fmt.Errorf("%d operations routed without a row, %d stale baseline lines, %d rows naming no routed "+
			"operation; write the rows, or run `make operation-docs-write` if a line is stale",
			len(newDrift), len(stale), len(phantom))
	default:
		_, _ = fmt.Fprintf(stdout, "%s documents every routed operation bar the %d in %s; no row names an "+
			"unrouted operation\n", dir, len(undocumented), baselinePath)
		return nil
	}
}
