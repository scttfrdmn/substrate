package iac_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// tfProviderVersion is the AWS provider examples/terraform/versions.tf pins.
const tfProviderVersion = "6.55.0"

// tfAddresses is every address `terraform state list` reports after the apply: the
// fourteen kinds of resource the roadmap review applied (#1597), the bucket's public-access
// block and default encryption, and the two policy documents the IAM resources are built from.
var tfAddresses = []string{
	"data.aws_iam_policy_document.assume_lambda",
	"data.aws_iam_policy_document.handler",
	"aws_cloudwatch_event_rule.nightly",
	"aws_cloudwatch_log_group.handler",
	"aws_cloudwatch_metric_alarm.queue_depth",
	"aws_dynamodb_table.items",
	"aws_iam_policy.handler",
	"aws_iam_role.handler",
	"aws_iam_role_policy_attachment.handler",
	"aws_kms_key.main",
	"aws_lambda_function.handler",
	"aws_s3_bucket.artifacts",
	"aws_s3_bucket_public_access_block.artifacts",
	"aws_s3_bucket_server_side_encryption_configuration.artifacts",
	"aws_s3_bucket_versioning.artifacts",
	"aws_secretsmanager_secret.api",
	"aws_security_group.app",
	"aws_sns_topic.alerts",
	"aws_sqs_queue.jobs",
	"aws_subnet.main",
	"aws_vpc.main",
}

// terraformBinary returns the Terraform CLI to run: SUBSTRATE_IAC_TERRAFORM if set
// (OpenTofu's `tofu` works too), else `terraform` on PATH.
func terraformBinary(t *testing.T) string {
	t.Helper()
	name := os.Getenv("SUBSTRATE_IAC_TERRAFORM")
	if name == "" {
		name = "terraform"
	}
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s is not on PATH: %v", name, err)
	}
	return path
}

// terraformMirror returns the directory the AWS provider is installed from:
// TF_PLUGIN_CACHE_DIR, else ~/.terraform.d/plugin-cache. Either is populated by a
// `terraform init` run outside the test, which is the step that needs the network.
// The test reads it as a filesystem mirror, so init inside the test never contacts
// the registry.
func terraformMirror(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("TF_PLUGIN_CACHE_DIR")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("home directory: %v", err)
		}
		dir = filepath.Join(home, ".terraform.d", "plugin-cache")
	}
	platform := runtime.GOOS + "_" + runtime.GOARCH
	provider := filepath.Join(dir, "registry.terraform.io", "hashicorp", "aws", tfProviderVersion, platform)
	if _, err := os.Stat(provider); err != nil {
		t.Fatalf("the AWS provider %s for %s is not in %s; populate it once, with network, by running\n"+
			"  TF_PLUGIN_CACHE_DIR=%s terraform -chdir=examples/terraform init -backend=false\n(%v)",
			tfProviderVersion, platform, dir, dir, err)
	}
	return dir
}

// terraformCLIConfig writes a CLI configuration that installs every provider from
// mirror and from nowhere else, and returns its path.
func terraformCLIConfig(t *testing.T, mirror string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "terraformrc")
	cfg := fmt.Sprintf(`disable_checkpoint = true

provider_installation {
  filesystem_mirror {
    path    = %q
    include = ["registry.terraform.io/*/*"]
  }
}
`, mirror)
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write terraform CLI config: %v", err)
	}
	return path
}

// TestTerraformJourney applies examples/terraform against substrate, checks that a
// second plan finds nothing to change, destroys it, and checks the state is empty
// (#1519). The recorded run is then replayed into a second fresh deterministic-run server,
// and any response that differs fails the test (#1628). It is replayed rather than run twice
// because Terraform's walk order, and its wall-clock waiters, differ from run to run; see
// [emulator.RunThenReplay].
func TestTerraformJourney(t *testing.T) {
	requireIAC(t)
	tf := terraformBinary(t)
	mirror := terraformMirror(t)
	src := filepath.Join(repoRoot(t), "examples", "terraform")

	emulator.RunThenReplay(t, iacEpoch, func(t *testing.T, ts *emulator.TestServer) {
		reportRefusals(t, ts)
		dir := t.TempDir()
		copyTree(t, src, dir, ".terraform")
		for _, stale := range []string{"terraform.tfstate", "terraform.tfstate.backup"} {
			_ = os.Remove(filepath.Join(dir, stale))
		}
		env := toolEnv(t, ts,
			"TF_CLI_CONFIG_FILE="+terraformCLIConfig(t, mirror),
			"TF_DATA_DIR="+filepath.Join(t.TempDir(), "tfdata"),
			"TF_IN_AUTOMATION=1",
			"TF_INPUT=0",
			"CHECKPOINT_DISABLE=1",
			// Passed through so a failing call can be read off the provider's own log.
			"TF_LOG="+os.Getenv("TF_LOG"),
			"TF_LOG_PATH="+os.Getenv("TF_LOG_PATH"),
		)
		run := func(args ...string) toolResult {
			t.Helper()
			return mustRunTool(t, dir, env, tf, append(args, "-no-color")...)
		}

		// One operation at a time, so the two runs issue their calls in the same order
		// and their event logs pair up. SUBSTRATE_IAC_TF_PARALLELISM raises it, which
		// is useful for collecting every failure in one run and breaks the comparison.
		parallelism := "-parallelism=1"
		if p := os.Getenv("SUBSTRATE_IAC_TF_PARALLELISM"); p != "" {
			parallelism = "-parallelism=" + p
		}

		run("init", "-input=false")
		run("apply", "-input=false", "-auto-approve", parallelism)

		// Convergence: a plan straight after the apply must find nothing to do. Exit 2
		// is "changes present", which means substrate reads back something other
		// than what the apply wrote.
		plan := runTool(t, dir, env, tf, "plan", "-input=false", "-detailed-exitcode", parallelism, "-no-color")
		if plan.exitCode != 0 {
			t.Fatalf("second plan is not empty (exit %d):\n%s\n%s", plan.exitCode, plan.stdout, plan.stderr)
		}

		listed := strings.Fields(run("state", "list").stdout)
		slices.Sort(listed)
		want := slices.Clone(tfAddresses)
		slices.Sort(want)
		if !slices.Equal(listed, want) {
			t.Errorf("state list after apply:\n got %v\nwant %v", listed, want)
		}

		assertTerraformOutputs(t, run("output", "-json").stdout)
		assertTerraformResources(t, ts)

		run("destroy", "-input=false", "-auto-approve", parallelism)
		if left := strings.TrimSpace(run("state", "list").stdout); left != "" {
			t.Errorf("state after destroy is not empty:\n%s", left)
		}
		if status, body := awsCall(t, ts, "s3.amazonaws.com", "HEAD", "/substrate-tf-artifacts", "", nil, ""); status != 404 {
			t.Errorf("bucket after destroy: HeadBucket answered %d, want 404: %s", status, body)
		}
	})
}

// assertTerraformOutputs checks the values `terraform output -json` reports.
func assertTerraformOutputs(t *testing.T, raw string) {
	t.Helper()
	var outputs map[string]struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &outputs); err != nil {
		t.Fatalf("decode outputs: %v\n%s", err, raw)
	}
	checks := []struct {
		name, prefix string
	}{
		{"vpc_id", "vpc-"},
		{"subnet_id", "subnet-"},
		{"bucket", "substrate-tf-artifacts"},
		{"queue_url", "http"},
		{"topic_arn", "arn:aws:sns:" + iacRegion + ":" + iacAccount + ":substrate-tf-alerts"},
		{"table_name", "substrate-tf-items"},
		{"function_arn", "arn:aws:lambda:" + iacRegion + ":" + iacAccount + ":function:substrate-tf-handler"},
	}
	for _, c := range checks {
		if got := outputs[c.name].Value; !strings.HasPrefix(got, c.prefix) {
			t.Errorf("output %s = %q, want prefix %q", c.name, got, c.prefix)
		}
	}
	if got := outputs["queue_url"].Value; !strings.HasSuffix(got, "/"+iacAccount+"/substrate-tf-jobs") {
		t.Errorf("output queue_url = %q, want it to end /%s/substrate-tf-jobs", got, iacAccount)
	}
}

// assertTerraformResources reads a few of the applied resources back through the
// AWS API, so the journey checks substrate's state and not only Terraform's.
func assertTerraformResources(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	status, body := awsCall(t, ts, "s3.amazonaws.com", "GET", "/substrate-tf-artifacts?versioning", "", nil, "")
	if status != 200 || !strings.Contains(body, "<Status>Enabled</Status>") {
		t.Errorf("GetBucketVersioning: %d %s", status, body)
	}
	status, body = awsCall(t, ts, "dynamodb."+iacRegion+".amazonaws.com", "POST", "/",
		"application/x-amz-json-1.0", map[string]string{"X-Amz-Target": "DynamoDB_20120810.DescribeTable"},
		`{"TableName":"substrate-tf-items"}`)
	if status != 200 || !strings.Contains(body, `"TableStatus":"ACTIVE"`) {
		t.Errorf("DescribeTable: %d %s", status, body)
	}
	status, body = awsCall(t, ts, "lambda."+iacRegion+".amazonaws.com", "GET",
		"/2015-03-31/functions/substrate-tf-handler", "", nil, "")
	if status != 200 || !strings.Contains(body, `"Runtime":"nodejs22.x"`) {
		t.Errorf("GetFunction: %d %s", status, body)
	}
}
