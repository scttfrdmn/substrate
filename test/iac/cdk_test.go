package iac_test

import (
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// cdkStack is the stack examples/cdk/bin/app.ts defines.
const cdkStack = "SubstrateCdkExample"

// cdkBinary returns the CDK CLI to run: examples/cdk's own, pinned by its lockfile, when
// `npm ci` has installed it, else `cdk` on PATH. It fails when the app's dependencies are not
// installed, because the app cannot synthesize without aws-cdk-lib, and a skip under
// SUBSTRATE_IAC=1 would report a journey that never ran as passed.
func cdkBinary(t *testing.T, app string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(app, "node_modules", "aws-cdk-lib")); err != nil {
		t.Fatalf("SUBSTRATE_IAC=1 but examples/cdk has no node_modules; run `make iac-e2e-setup` first (%v)", err)
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Fatalf("SUBSTRATE_IAC=1 but node is not on PATH: %v", err)
	}
	local := filepath.Join(app, "node_modules", ".bin", "cdk")
	if _, err := os.Stat(local); err == nil {
		return local
	}
	path, err := exec.LookPath("cdk")
	if err != nil {
		t.Fatalf("SUBSTRATE_IAC=1 but cdk is not on PATH and examples/cdk has no local CLI: %v", err)
	}
	return path
}

// TestCDKJourney bootstraps the default CDK environment in substrate, deploys examples/cdk,
// checks the stack's outputs and resources, and destroys it (#1520). The recorded run is then
// replayed into a second fresh deterministic-run server, and any response that differs fails
// the test (#1628). It is replayed rather than run twice because the CLI's waiters and credential
// lookups make a timing-dependent number of calls; see [emulator.RunThenReplay].
func TestCDKJourney(t *testing.T) {
	requireIAC(t)
	app := filepath.Join(repoRoot(t), "examples", "cdk")
	cdk := cdkBinary(t, app)

	emulator.RunThenReplay(t, iacEpoch, func(t *testing.T, ts *emulator.TestServer) {
		reportRefusals(t, ts)
		out := t.TempDir()
		env := toolEnv(t, ts,
			// The CLI checks npm for a newer version and fetches notices unless told not to;
			// both are network calls.
			"CDK_DISABLE_VERSION_CHECK=true",
			"CDK_DISABLE_CLI_TELEMETRY=true",
			"CDK_DEFAULT_ACCOUNT="+iacAccount,
			"CDK_DEFAULT_REGION="+iacRegion,
		)
		run := func(args ...string) toolResult {
			t.Helper()
			return mustRunTool(t, app, env, cdk, append(args, "--no-notices", "--output", filepath.Join(out, "cdk.out"))...)
		}

		run("bootstrap", "aws://"+iacAccount+"/"+iacRegion)
		outputsFile := filepath.Join(out, "outputs.json")
		run("deploy", cdkStack, "--require-approval", "never", "--outputs-file", outputsFile)

		assertCDKOutputs(t, outputsFile)
		assertCDKResources(t, ts)

		run("destroy", cdkStack, "--force")
		status, body := cfnQuery(t, ts, "DescribeStacks", url.Values{"StackName": {cdkStack}})
		if status != 400 || !strings.Contains(body, "does not exist") {
			t.Errorf("DescribeStacks after destroy: %d %s", status, body)
		}
	})
}

// assertCDKOutputs checks the stack outputs `cdk deploy --outputs-file` wrote.
func assertCDKOutputs(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read outputs: %v", err)
	}
	var outputs map[string]map[string]string
	if err := json.Unmarshal(raw, &outputs); err != nil {
		t.Fatalf("decode outputs: %v\n%s", err, raw)
	}
	stack := outputs[cdkStack]
	for name, pattern := range map[string]string{
		"BucketName": `^substratecdkexample-artifacts[a-z0-9-]+$`,
		"QueueUrl":   `/` + iacAccount + `/SubstrateCdkExample-Jobs[A-Za-z0-9-]+$`,
		"TableName":  `^SubstrateCdkExample-Items[A-Za-z0-9-]+$`,
	} {
		if !regexp.MustCompile(pattern).MatchString(stack[name]) {
			t.Errorf("output %s = %q, want it to match %s", name, stack[name], pattern)
		}
	}
}

// assertCDKResources checks the stack's resources through CloudFormation: one of each kind the
// app declares, each CREATE_COMPLETE.
func assertCDKResources(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	status, body := cfnQuery(t, ts, "DescribeStackResources", url.Values{"StackName": {cdkStack}})
	if status != 200 {
		t.Fatalf("DescribeStackResources: %d %s", status, body)
	}
	for _, typ := range []string{"AWS::S3::Bucket", "AWS::SQS::Queue", "AWS::DynamoDB::Table"} {
		if !strings.Contains(body, "<ResourceType>"+typ+"</ResourceType>") {
			t.Errorf("DescribeStackResources lists no %s:\n%s", typ, body)
		}
	}
	if strings.Contains(body, "FAILED") {
		t.Errorf("a stack resource failed:\n%s", body)
	}
}

// cfnQuery sends one CloudFormation Query request.
func cfnQuery(t *testing.T, ts *emulator.TestServer, action string, params url.Values) (int, string) {
	t.Helper()
	params.Set("Action", action)
	params.Set("Version", "2010-05-15")
	return awsCall(t, ts, "cloudformation."+iacRegion+".amazonaws.com", "POST", "/",
		"application/x-www-form-urlencoded", nil, params.Encode())
}
