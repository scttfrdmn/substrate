// Package iac_test drives real infrastructure-as-code tools, Terraform and the AWS
// CDK, against an in-process substrate server (#1519, #1520).
//
// The journeys are opt-in. Each calls t.Skip unless SUBSTRATE_IAC=1 is set and its
// tool is installed, because neither tool is part of the Go toolchain and both
// fetch plugins or packages from the network when first set up. That setup happens
// outside the test: the Terraform journey installs the AWS provider from a local
// mirror (TF_PLUGIN_CACHE_DIR), and the CDK journey runs the CLI from
// examples/cdk/node_modules. Inside a test nothing leaves the machine; every AWS
// call goes to the substrate server the test started.
//
// `make iac-e2e` runs them, and so does the iac-e2e CI job.
package iac_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// iacEpoch is the instant the deterministic double runs pin the clock at.
var iacEpoch = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// iacAccount is the account every unsigned or unregistered key resolves to on a
// test server.
const iacAccount = "123456789012"

// iacRegion is the region both journeys deploy to.
const iacRegion = "us-east-1"

// iacCommandTimeout bounds one tool invocation. A provider that retries an error it
// considers transient would otherwise spin for the provider's own timeout, which
// for some resources is twenty minutes or more.
const iacCommandTimeout = 10 * time.Minute

// commandTimeout is iacCommandTimeout, or SUBSTRATE_IAC_TIMEOUT when set to a Go
// duration, so a run that is investigating a hang can give up sooner.
func commandTimeout(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv("SUBSTRATE_IAC_TIMEOUT")
	if raw == "" {
		return iacCommandTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("SUBSTRATE_IAC_TIMEOUT=%q: %v", raw, err)
	}
	return d
}

// reportRefusals logs, when the test has failed, every AWS call the server refused,
// once per service, operation and error. A tool's own error output names the
// resource that failed; this names the call, which is what a fix starts from.
//
// With SUBSTRATE_IAC_EVENTS set to a directory, every recorded event, bodies
// included, is also written there as NDJSON, one file per run.
func reportRefusals(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	t.Cleanup(func() {
		if dir := os.Getenv("SUBSTRATE_IAC_EVENTS"); dir != "" {
			name := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".ndjson")
			if f, err := os.Create(name); err != nil {
				t.Logf("SUBSTRATE_IAC_EVENTS: %v", err)
			} else {
				if _, err := ts.Store().ExportNDJSON(context.Background(), emulator.EventFilter{}, f); err != nil {
					t.Logf("SUBSTRATE_IAC_EVENTS: %v", err)
				}
				_ = f.Close()
				t.Logf("events written to %s", name)
			}
		}
		if !t.Failed() && os.Getenv("SUBSTRATE_IAC_VERBOSE") != "1" {
			return
		}
		events, err := ts.Store().GetEvents(context.Background(), emulator.EventFilter{})
		if err != nil {
			t.Logf("read events: %v", err)
			return
		}
		seen := map[string]int{}
		var order []string
		for _, e := range events {
			if e.StatusCode < 400 && e.Error == "" {
				continue
			}
			key := fmt.Sprintf("%s.%s %d %s", e.Service, e.Operation, e.StatusCode, e.Error)
			if seen[key] == 0 {
				order = append(order, key)
			}
			seen[key]++
		}
		t.Logf("%d calls recorded, %d distinct refusals", len(events), len(order))
		for _, key := range order {
			t.Logf("  refused x%d: %s", seen[key], key)
		}
	})
}

// requireIAC skips the test unless SUBSTRATE_IAC=1 is set.
func requireIAC(t *testing.T) {
	t.Helper()
	if os.Getenv("SUBSTRATE_IAC") != "1" {
		t.Skip("set SUBSTRATE_IAC=1 to run the Terraform and CDK journeys (see `make iac-e2e`)")
	}
}

// repoRoot returns the repository root, two directories above this module.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// toolEnv is the environment a tool runs with: the caller's PATH and nothing else of
// theirs, so a profile, a proxy or real credentials in the developer's shell cannot
// reach the run. HOME is a fresh temporary directory for the same reason.
func toolEnv(t *testing.T, ts *emulator.TestServer, extra ...string) []string {
	t.Helper()
	home := t.TempDir()
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"TMPDIR=" + t.TempDir(),
		"AWS_ENDPOINT_URL=" + ts.URL,
		"AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
		"AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"AWS_REGION=" + iacRegion,
		"AWS_DEFAULT_REGION=" + iacRegion,
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_CONFIG_FILE=" + filepath.Join(home, "aws-config"),
		"AWS_SHARED_CREDENTIALS_FILE=" + filepath.Join(home, "aws-credentials"),
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	}
	return append(env, extra...)
}

// toolResult is what one tool invocation produced.
type toolResult struct {
	stdout, stderr string
	exitCode       int
}

// runTool runs name with args in dir and returns its output and exit code. It fails
// the test only when the tool could not be started or timed out; a non-zero exit is
// the caller's to judge, because `terraform plan -detailed-exitcode` exits 2 on
// purpose.
func runTool(t *testing.T, dir string, env []string, name string, args ...string) toolResult {
	t.Helper()
	timeout := commandTimeout(t)
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	// Interrupt rather than kill on timeout: Terraform stops its provider plugins on
	// SIGINT, and a killed Terraform leaves them running.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 30 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if os.Getenv("SUBSTRATE_IAC_VERBOSE") == "1" {
		// Stream as well as capture, so a tool that is retrying can be watched
		// rather than waited out.
		cmd.Stdout = io.MultiWriter(&stdout, os.Stderr)
		cmd.Stderr = io.MultiWriter(&stderr, os.Stderr)
	}
	err := cmd.Run()
	res := toolResult{stdout: stdout.String(), stderr: stderr.String()}
	if ctx.Err() != nil {
		t.Fatalf("%s %s: timed out after %s\nstdout:\n%s\nstderr:\n%s",
			name, strings.Join(args, " "), timeout, res.stdout, res.stderr)
	}
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("%s %s: %v", name, strings.Join(args, " "), err)
		}
		res.exitCode = exitErr.ExitCode()
	}
	return res
}

// mustRunTool runs a tool and fails the test on a non-zero exit, printing both
// streams so the failing AWS call is in the test log.
func mustRunTool(t *testing.T, dir string, env []string, name string, args ...string) toolResult {
	t.Helper()
	res := runTool(t, dir, env, name, args...)
	if res.exitCode != 0 {
		t.Fatalf("%s %s: exit %d\nstdout:\n%s\nstderr:\n%s",
			name, strings.Join(args, " "), res.exitCode, res.stdout, res.stderr)
	}
	return res
}

// copyTree copies the regular files under src into dst, skipping any directory named
// in skip. The journeys run on a copy so that neither tool writes state, caches or a
// lock file into the checked-in example.
func copyTree(t *testing.T, src, dst string, skip ...string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			for _, s := range skip {
				if d.Name() == s {
					return filepath.SkipDir
				}
			}
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		out, err := os.Create(filepath.Join(dst, rel))
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
	if err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
}

// awsCall sends one unsigned AWS request to the test server, addressed to host so
// substrate routes it to the service the host names, and returns the status and body.
// The journeys use it to check what a tool created through the same API a caller
// would, rather than through substrate's internals.
func awsCall(t *testing.T, ts *emulator.TestServer, host, method, path, contentType string, headers map[string]string, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	return resp.StatusCode, string(out)
}
