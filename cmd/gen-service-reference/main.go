// Command gen-service-reference keeps the coverage matrix in docs/services.md in
// sync with the live plugin registry so the documented service count and plugin
// list can never drift from the implementation (see issue #364).
//
// It boots the default plugin registry, enumerates the registered plugins, and
// renders a Markdown coverage table between the marker comments
//
//	<!-- BEGIN GENERATED COVERAGE MATRIX -->
//	<!-- END GENERATED COVERAGE MATRIX -->
//
// in docs/services.md. Everything outside the markers — including the
// hand-written per-service operation, CloudFormation, and cost detail — is left
// untouched. Per-plugin display names and protocols come from the maintained
// metadata map below; the tool fails if a registered plugin has no metadata
// entry (or an entry names a plugin that is not registered), which is what a CI
// drift check enforces.
//
// Usage:
//
//	go run ./cmd/gen-service-reference -out docs/services.md          # rewrite the matrix
//	go run ./cmd/gen-service-reference -out docs/services.md -check    # exit non-zero if out of date
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	emu "github.com/scttfrdmn/substrate/emulator"
)

const (
	beginMarker = "<!-- BEGIN GENERATED COVERAGE MATRIX -->"
	endMarker   = "<!-- END GENERATED COVERAGE MATRIX -->"
)

// pluginMeta is the maintained per-plugin documentation metadata.
type pluginMeta struct {
	// Display is the human-facing AWS service name.
	Display string
	// Protocol is the wire protocol the plugin speaks.
	Protocol string
}

// meta maps each registered plugin name to its documentation metadata. Adding a
// plugin to the registry without a matching entry here fails generation (and CI).
var meta = map[string]pluginMeta{
	"acm":                  {"ACM", "JSON"},
	"apigateway":           {"API Gateway (REST)", "REST/JSON"},
	"apigatewayv2":         {"API Gateway (HTTP)", "REST/JSON"},
	"appsync":              {"AppSync", "REST/JSON"},
	"athena":               {"Athena", "JSON"},
	"backup":               {"Backup", "REST/JSON"},
	"batch":                {"Batch", "REST/JSON"},
	"bedrock-runtime":      {"Bedrock Runtime", "REST/JSON"},
	"budgets":              {"Budgets", "JSON"},
	"ce":                   {"Cost Explorer", "JSON"},
	"cloudfront":           {"CloudFront", "REST/XML"},
	"cloudtrail":           {"CloudTrail", "JSON"},
	"codebuild":            {"CodeBuild", "JSON"},
	"codedeploy":           {"CodeDeploy", "JSON"},
	"codepipeline":         {"CodePipeline", "JSON"},
	"cognito-identity":     {"Cognito Identity", "JSON"},
	"cognito-idp":          {"Cognito Identity Provider", "JSON"},
	"dynamodb":             {"DynamoDB", "JSON"},
	"ec2":                  {"EC2 / VPC", "Query"},
	"ecr":                  {"ECR", "JSON"},
	"ecs":                  {"ECS", "JSON"},
	"efs":                  {"EFS", "REST/JSON"},
	"elasticache":          {"ElastiCache", "Query"},
	"elasticloadbalancing": {"ELBv2", "Query"},
	"emrserverless":        {"EMR Serverless", "REST/JSON"},
	"eventbridge":          {"EventBridge", "JSON"},
	"execute-api":          {"API Gateway (execute-api)", "REST/JSON"},
	"firehose":             {"Kinesis Data Firehose", "JSON"},
	"fsx":                  {"FSx", "JSON"},
	"glue":                 {"Glue", "JSON"},
	"health":               {"Health", "JSON"},
	"iam":                  {"IAM", "Query"},
	"kinesis":              {"Kinesis Data Streams", "JSON"},
	"kms":                  {"KMS", "JSON"},
	"lambda":               {"Lambda", "REST/JSON"},
	"logs":                 {"CloudWatch Logs", "JSON"},
	"monitoring":           {"CloudWatch", "Query"},
	"msk":                  {"MSK", "REST/JSON"},
	"omics":                {"HealthOmics", "REST/JSON"},
	"opensearch":           {"OpenSearch", "REST/JSON"},
	"organizations":        {"Organizations", "JSON"},
	"quicksight":           {"QuickSight", "REST/JSON"},
	"ram":                  {"RAM", "REST/JSON"},
	"rds":                  {"RDS", "Query"},
	"redshift":             {"Redshift", "Query"},
	"redshift-data":        {"Redshift Data API", "JSON"},
	"route53":              {"Route 53", "REST/XML"},
	"s3":                   {"S3", "REST/XML"},
	"sagemaker":            {"SageMaker", "JSON"},
	"scheduler":            {"EventBridge Scheduler", "REST/JSON"},
	"secretsmanager":       {"Secrets Manager", "JSON"},
	"servicequotas":        {"Service Quotas", "JSON"},
	"sesv2":                {"SES v2", "REST/JSON"},
	"sns":                  {"SNS", "Query"},
	"sqs":                  {"SQS", "JSON"},
	"ssm":                  {"SSM", "JSON"},
	"sso":                  {"SSO / Identity Store", "REST/JSON"},
	"states":               {"Step Functions", "JSON"},
	"sts":                  {"STS", "Query"},
	"tagging":              {"Resource Groups Tagging", "JSON"},
	"timestream":           {"Timestream", "JSON"},
	"transfer":             {"Transfer Family", "JSON"},
	"wafv2":                {"WAFv2", "JSON"},
}

func main() {
	check := flag.Bool("check", false, "exit non-zero if the file is out of date instead of writing it")
	out := flag.String("out", "docs/services.md", "path to the services reference file")
	flag.Parse()

	names, err := registeredPlugins()
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-service-reference:", err)
		os.Exit(1)
	}

	existing, err := os.ReadFile(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gen-service-reference: read %s: %v\n", *out, err)
		os.Exit(1)
	}

	updated, err := replaceMatrix(existing, names)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-service-reference:", err)
		os.Exit(1)
	}

	if *check {
		if !bytes.Equal(existing, updated) {
			fmt.Fprintf(os.Stderr, "gen-service-reference: %s coverage matrix is out of date; run `make docs-reference` and commit the result\n", *out)
			os.Exit(1)
		}
		return
	}

	if bytes.Equal(existing, updated) {
		fmt.Printf("%s already up to date (%d plugins)\n", *out, len(names))
		return
	}
	if err := os.WriteFile(*out, updated, 0o644); err != nil { //nolint:gosec // docs file, world-readable is fine.
		fmt.Fprintln(os.Stderr, "gen-service-reference:", err)
		os.Exit(1)
	}
	fmt.Printf("updated %s (%d plugins)\n", *out, len(names))
}

// registeredPlugins boots the default registry and returns the sorted plugin
// names, verifying that metadata is complete in both directions.
func registeredPlugins() ([]string, error) {
	reg := emu.NewPluginRegistry()
	state := emu.NewMemoryStateManager()
	tc := emu.NewTimeController(time.Unix(0, 0).UTC())
	logger := emu.NewDefaultLogger(slog.LevelError, false)
	store := emu.NewEventStore(emu.EventStoreConfig{Enabled: false})

	if err := emu.RegisterDefaultPlugins(context.Background(), reg, state, tc, logger, store, nil); err != nil {
		return nil, fmt.Errorf("register default plugins: %w", err)
	}
	names := reg.Names()

	registered := make(map[string]bool, len(names))
	for _, n := range names {
		registered[n] = true
		if _, ok := meta[n]; !ok {
			return nil, fmt.Errorf("plugin %q is registered but has no metadata entry in cmd/gen-service-reference/main.go; add one", n)
		}
	}
	for n := range meta {
		if !registered[n] {
			return nil, fmt.Errorf("metadata entry %q does not correspond to a registered plugin; remove it from cmd/gen-service-reference/main.go", n)
		}
	}
	sort.Strings(names)
	return names, nil
}

// replaceMatrix swaps the content between the begin/end markers in doc for a
// freshly rendered coverage matrix, leaving everything else untouched.
func replaceMatrix(doc []byte, names []string) ([]byte, error) {
	begin := bytes.Index(doc, []byte(beginMarker))
	end := bytes.Index(doc, []byte(endMarker))
	if begin < 0 || end < 0 || end < begin {
		return nil, fmt.Errorf("could not find %q ... %q markers in the document", beginMarker, endMarker)
	}

	var matrix bytes.Buffer
	fmt.Fprintln(&matrix, beginMarker)
	fmt.Fprintf(&matrix, "Substrate ships **%d built-in service plugins**. This section is generated\n", len(names))
	fmt.Fprintln(&matrix, "from the plugin registry (`make docs-reference`), so the count and plugin list")
	fmt.Fprintln(&matrix, "cannot drift from the implementation. The live count is also available from the")
	fmt.Fprintln(&matrix, "`/ready` endpoint (`curl http://localhost:4566/ready`). Per-service operation,")
	fmt.Fprintln(&matrix, "CloudFormation, and cost detail follows below the matrix.")
	fmt.Fprintln(&matrix)
	fmt.Fprintln(&matrix, "| # | Service | Plugin name | Protocol |")
	fmt.Fprintln(&matrix, "|---|---------|-------------|----------|")
	for i, n := range names {
		m := meta[n]
		fmt.Fprintf(&matrix, "| %d | %s | `%s` | %s |\n", i+1, m.Display, n, m.Protocol)
	}
	fmt.Fprint(&matrix, endMarker)

	var result bytes.Buffer
	result.Write(doc[:begin])
	result.Write(matrix.Bytes())
	result.Write(doc[end+len(endMarker):])
	return result.Bytes(), nil
}
