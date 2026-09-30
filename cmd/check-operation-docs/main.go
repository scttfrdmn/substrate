// Command check-operation-docs fails when docs/services.md and the operation catalog
// disagree about which operations substrate routes.
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
// their content. The generated coverage matrix at the top of the file and the hand-written
// sections below it already split the same way.
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

// docSections maps a registered plugin name to the "## " heading in docs/services.md under
// which that plugin's operations are documented.
//
// It is written out rather than derived from PluginRouting.Display because the two differ
// for seven services — ELBv2/"ELB v2", "API Gateway (HTTP)"/"API Gateway v2 (HTTP)",
// "Cognito Identity Provider"/"Cognito User Pools", Backup/"AWS Backup", SSM/"SSM
// Parameter Store", "EC2 / VPC"/EC2, and "API Gateway (execute-api)"/"execute-api (API
// Gateway data plane)" — so deriving it would silently drop those seven from the check.
// [checkSections] fails on a registered plugin with no entry, an entry naming a heading the
// file does not have, and two plugins claiming one heading, so the map cannot rot into a
// quiet exemption list.
//
// A "## " heading is not something the emulator package should know, which is why this
// lives here and not in emulator/routing.go. Making the coverage matrix link each row to
// its section would change that and is a separate question (#1015).
var docSections = map[string]string{
	"account":              "Account Management",
	"acm":                  "ACM",
	"apigateway":           "API Gateway (REST)",
	"apigatewayv2":         "API Gateway v2 (HTTP)",
	"appsync":              "AppSync",
	"athena":               "Athena",
	"backup":               "AWS Backup",
	"batch":                "Batch",
	"bedrock-runtime":      "Bedrock Runtime",
	"budgets":              "Budgets",
	"ce":                   "Cost Explorer",
	"cloudformation":       "CloudFormation",
	"cloudfront":           "CloudFront",
	"cloudtrail":           "CloudTrail",
	"codebuild":            "CodeBuild",
	"codedeploy":           "CodeDeploy",
	"codepipeline":         "CodePipeline",
	"cognito-identity":     "Cognito Identity",
	"cognito-idp":          "Cognito User Pools",
	"config":               "Config",
	"dynamodb":             "DynamoDB",
	"ec2":                  "EC2",
	"ecr":                  "ECR",
	"ecs":                  "ECS",
	"efs":                  "EFS",
	"elasticache":          "ElastiCache",
	"elasticloadbalancing": "ELB v2",
	"emrserverless":        "EMR Serverless",
	"eventbridge":          "EventBridge",
	"execute-api":          "execute-api (API Gateway data plane)",
	"firehose":             "Kinesis Data Firehose",
	"fsx":                  "FSx",
	"glue":                 "Glue",
	"health":               "Health",
	"iam":                  "IAM",
	"kinesis":              "Kinesis Data Streams",
	"kms":                  "KMS",
	"lambda":               "Lambda",
	"logs":                 "CloudWatch Logs",
	"monitoring":           "CloudWatch",
	"msk":                  "MSK",
	"omics":                "HealthOmics",
	"opensearch":           "OpenSearch",
	"organizations":        "Organizations",
	"pricing":              "Price List Query API",
	"quicksight":           "QuickSight",
	"ram":                  "RAM",
	"rds":                  "RDS",
	"redshift":             "Redshift",
	"redshift-data":        "Redshift Data API",
	"route53":              "Route 53",
	"s3":                   "S3",
	"sagemaker":            "SageMaker",
	"scheduler":            "EventBridge Scheduler",
	"secretsmanager":       "Secrets Manager",
	"servicequotas":        "Service Quotas",
	"sesv2":                "SES v2",
	"sns":                  "SNS",
	"sqs":                  "SQS",
	"ssm":                  "SSM Parameter Store",
	"sso":                  "SSO / Identity Store",
	"states":               "Step Functions",
	"sts":                  "STS",
	"tagging":              "Resource Groups Tagging",
	"timestream":           "Timestream",
	"transfer":             "Transfer Family",
	"wafv2":                "WAFv2",
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

// routedCatalog reads the router's own operation set. [checkSections] and [compare] take it
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
	docs := fs.String("docs", filepath.Join("docs", "services.md"), "path to the service reference")
	baselinePath := fs.String("baseline", filepath.Join("scripts", "undocumented-operations.txt"),
		"path to the inventory of operations known to have no row")
	write := fs.Bool("write", false, "rewrite the baseline from the tree instead of checking it")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	src, err := os.ReadFile(*docs)
	if err != nil {
		return fmt.Errorf("read %s: %w", *docs, err)
	}
	tables := parseOperationTables(string(src))
	catalog := routedCatalog()
	if err := checkSections(catalog, docSections, tables); err != nil {
		return err
	}
	undocumented, phantom := compare(catalog, docSections, tables)

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
	return report(stdout, *docs, *baselinePath, undocumented, phantom, known)
}

// parseOperationTables returns, for each "## " heading in the reference, the first-column
// cells of the first table under it whose header's first column is exactly "Operation".
//
// Scoping to that one table is the difference between this check and every hand count
// before it. Asking only whether a name appears somewhere in the file is how Lambda's
// AddPermission counted as documented because SNS has a row for it (#1084). Taking any
// table in the section would count a name appearing in a resource-type or error-code
// table. The section's operation table is the thing headed "Operation", and it is the
// claim a reader takes the section to be making.
//
// A nil entry means the section exists and has no such table, which is distinct from an
// entry holding no rows.
func parseOperationTables(src string) map[string][]string {
	tables := make(map[string][]string)
	section := ""
	fenced := false
	rows, found := []string(nil), false

	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimRight(lines[i], " \t")
		if strings.HasPrefix(line, "```") {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		if strings.HasPrefix(line, "## ") {
			section = strings.TrimSpace(strings.TrimPrefix(line, "## "))
			if _, seen := tables[section]; !seen {
				tables[section] = nil
			}
			found = false
			continue
		}
		if section == "" || found || !strings.HasPrefix(line, "|") {
			continue
		}
		// A table starts here. Consume it whole either way, so a later table in the same
		// section cannot be mistaken for a continuation of this one.
		header := cells(line)
		start := i
		for i+1 < len(lines) && strings.HasPrefix(lines[i+1], "|") {
			i++
		}
		if len(header) == 0 || header[0] != "Operation" {
			continue
		}
		rows = nil
		// start is the header, start+1 the separator; rows run to the end of the table.
		for j := start + 2; j <= i; j++ {
			if c := cells(lines[j]); len(c) > 0 && c[0] != "" {
				rows = append(rows, c[0])
			}
		}
		tables[section], found = rows, true
	}
	return tables
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

// checkSections fails when docSections and the reference have drifted apart, before any
// operation is compared: a mapping that silently misses a plugin would report that plugin's
// operations as documented, which is the one way this check could pass while being blind.
func checkSections(catalog map[string][]string, sections map[string]string, tables map[string][]string) error {
	var problems []string
	for plugin := range catalog {
		heading, ok := sections[plugin]
		if !ok {
			problems = append(problems, fmt.Sprintf("plugin %q has no docSections entry", plugin))
			continue
		}
		if _, ok := tables[heading]; !ok {
			problems = append(problems, fmt.Sprintf("plugin %q maps to heading %q, which the reference does not have",
				plugin, heading))
		}
	}
	byHeading := make(map[string][]string, len(sections))
	for plugin, heading := range sections {
		if _, ok := catalog[plugin]; !ok {
			problems = append(problems, fmt.Sprintf("docSections has an entry for %q, which is not a registered plugin",
				plugin))
		}
		byHeading[heading] = append(byHeading[heading], plugin)
	}
	for heading, plugins := range byHeading {
		if len(plugins) > 1 {
			sort.Strings(plugins)
			problems = append(problems, fmt.Sprintf("heading %q is claimed by %s", heading, strings.Join(plugins, " and ")))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("docSections disagrees with the reference:\n  %s", strings.Join(problems, "\n  "))
}

// compare returns the routed operations with no row, and the rows naming no routed
// operation, both sorted.
func compare(catalog map[string][]string, sections map[string]string, tables map[string][]string) (undocumented, phantom []gap) {
	for plugin, ops := range catalog {
		rows := tables[sections[plugin]]
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
# its service's operation table in docs/services.md, as of #1015.
#
# Columns, tab-separated:  registered-plugin-name  Operation
#
# THIS FILE IS AN INVENTORY OF DEFECTS, NOT A SUPPRESSION LIST. docs/services.md heads
# each of these tables "Supported operations", so a reader takes the table to be the
# list — and for these operations it is short. Every line is expected to be deleted as
# its row is written, which is why cmd/check-operation-docs fails both on an operation
# missing from this file (new drift: an operation routed without being documented) and
# on a line whose row now exists (a stale record). Writing a row must shrink this file
# in the same commit. #1231 is the issue that empties it.
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
func report(stdout io.Writer, docs, baselinePath string, undocumented, phantom []gap, known map[gap]bool) error {
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
			docs, g.plugin, g.op)
	}
	for _, g := range stale {
		_, _ = fmt.Fprintf(stdout, "%s: %s %s now has a row; delete the line\n", baselinePath, g.plugin, g.op)
	}
	for _, g := range phantom {
		_, _ = fmt.Fprintf(stdout, "%s: the %s operation table has a row for %s, which no plugin routes\n",
			docs, g.plugin, g.op)
	}

	switch {
	case len(newDrift) > 0 || len(stale) > 0 || len(phantom) > 0:
		return fmt.Errorf("%d operations routed without a row, %d stale baseline lines, %d rows naming no routed "+
			"operation; write the rows, or run `make operation-docs-write` if a line is stale",
			len(newDrift), len(stale), len(phantom))
	default:
		_, _ = fmt.Fprintf(stdout, "%s documents every routed operation bar the %d in %s; no row names an "+
			"unrouted operation\n", docs, len(undocumented), baselinePath)
		return nil
	}
}
