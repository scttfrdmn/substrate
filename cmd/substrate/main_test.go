package main

import (
	"slices"
	"strings"
	"testing"

	substrate "github.com/scttfrdmn/substrate/emulator"
)

// TestBuildRootCmd verifies the root command is constructed without panic.
func TestBuildRootCmd(t *testing.T) {
	cmd := buildRootCmd()
	if cmd == nil {
		t.Fatal("buildRootCmd returned nil")
	}
	if !strings.HasPrefix(cmd.Use, "substrate") {
		t.Errorf("unexpected Use: %q", cmd.Use)
	}
}

// TestRunVersion verifies that --version exits cleanly.
func TestRunVersion(t *testing.T) {
	if err := run([]string{"--version"}); err != nil {
		t.Errorf("unexpected error from --version: %v", err)
	}
}

// TestRunHelp verifies that --help exits cleanly.
func TestRunHelp(_ *testing.T) {
	// --help causes cobra to print and return nil; we just verify it doesn't panic.
	_ = run([]string{"--help"})
}

// TestNewServerCmd verifies the server subcommand is constructed.
func TestNewServerCmd(t *testing.T) {
	cmd := newServerCmd()
	if cmd == nil {
		t.Fatal("newServerCmd returned nil")
	}
	if cmd.Use != "server" {
		t.Errorf("unexpected Use: %q", cmd.Use)
	}
}

// TestNewReplayCmd verifies the replay subcommand is constructed.
func TestNewReplayCmd(t *testing.T) {
	cmd := newReplayCmd()
	if cmd == nil {
		t.Fatal("newReplayCmd returned nil")
	}
	if cmd.Use != "replay <stream>" {
		t.Errorf("unexpected Use: %q", cmd.Use)
	}
}

// TestNewDebugCmd verifies the debug subcommand is constructed.
func TestNewDebugCmd(t *testing.T) {
	cmd := newDebugCmd()
	if cmd == nil {
		t.Fatal("newDebugCmd returned nil")
	}
	if cmd.Use != "debug <stream>" {
		t.Errorf("unexpected Use: %q", cmd.Use)
	}
}

// recordingLogger keeps every message so a test can assert what the binary told
// its operator, which for the credentials: section is half of the behavior (#736).
type recordingLogger struct {
	info []string
	warn []string
}

func (l *recordingLogger) Debug(string, ...any) {}

func (l *recordingLogger) Info(msg string, _ ...any) { l.info = append(l.info, msg) }

func (l *recordingLogger) Warn(msg string, _ ...any) { l.warn = append(l.warn, msg) }

func (l *recordingLogger) Error(string, ...any) {}

// TestNewCredentialWiring covers what `substrate server` does with the
// credentials: section, which was decorative until #736: the shipped default must
// change nothing, and the two flags must reach the server the way the section has
// always been documented.
func TestNewCredentialWiring(t *testing.T) {
	defaults := substrate.DefaultConfig()
	if defaults.Credentials.Enabled {
		t.Fatal("precondition: the default config must not enable the credentials section")
	}
	if !defaults.Credentials.VerifySignatures {
		t.Fatal("precondition: verify_signatures defaults to true, so `enabled: true` alone enforces signatures")
	}

	logger := &recordingLogger{}
	registry, verifySignatures := newCredentialWiring(defaults, logger)
	if registry != nil {
		t.Error("a default config must build no registry; every caller resolves to account.default")
	}
	if verifySignatures {
		t.Error("verify-without-registry must never reach the server, whatever verify_signatures says")
	}
	if len(logger.info) != 0 {
		t.Errorf("a default config logged %v; it configured nothing", logger.info)
	}

	// enabled: true alone attributes accounts and enforces signatures.
	cfg := substrate.DefaultConfig()
	cfg.Credentials.Enabled = true
	cfg.Credentials.Entries = []substrate.CredentialEntryCfg{
		{AccessKeyID: "AKIAEXAMPLE00000001", SecretAccessKey: "s", AccountID: "111122223333"},
	}
	logger = &recordingLogger{}
	registry, verifySignatures = newCredentialWiring(cfg, logger)
	if registry == nil || !verifySignatures {
		t.Fatalf("enabled: true gave registry=%v verify=%v, want a registry that verifies", registry != nil, verifySignatures)
	}
	entry, ok := registry.Lookup("AKIAEXAMPLE00000001")
	if !ok || entry.AccountID != "111122223333" {
		t.Errorf("configured entry resolved to %+v (found=%v), want account 111122223333", entry, ok)
	}
	if !slices.Contains(logger.info, "credential registry enabled") {
		t.Errorf("logged %v; an operator must be told the section is on", logger.info)
	}

	// verify_signatures: false is the combination #630 opened — attribution
	// without authentication.
	cfg.Credentials.VerifySignatures = false
	if registry, verifySignatures = newCredentialWiring(cfg, &recordingLogger{}); registry == nil || verifySignatures {
		t.Errorf("verify_signatures: false gave registry=%v verify=%v, want a registry that does not verify",
			registry != nil, verifySignatures)
	}
}

// TestReloadCredentials covers the reload split: an appended entry reaches the
// live registry, and the two settings a reload cannot apply say so rather than
// appearing to have been applied (#736).
func TestReloadCredentials(t *testing.T) {
	cfg := substrate.DefaultConfig()
	cfg.Credentials.Enabled = true
	registry, _ := newCredentialWiring(cfg, &recordingLogger{})
	if registry == nil {
		t.Fatal("precondition: an enabled section must build a registry")
	}

	newCfg := substrate.DefaultConfig()
	newCfg.Credentials.Enabled = true
	newCfg.Account.Default = "210987654321"
	newCfg.Credentials.Entries = []substrate.CredentialEntryCfg{
		{AccessKeyID: "AKIARELOADED00000001", SecretAccessKey: "s"},
	}
	logger := &recordingLogger{}
	reloadCredentials(registry, newCfg, cfg, logger)

	entry, ok := registry.Lookup("AKIARELOADED00000001")
	if !ok {
		t.Fatal("an appended entry must be reachable without a restart")
	}
	if entry.AccountID != "210987654321" {
		t.Errorf("reloaded entry account = %q, want the new account.default", entry.AccountID)
	}
	if len(logger.warn) != 0 {
		t.Errorf("warned %v about a change it did apply", logger.warn)
	}

	// Turning the section off is not something a reload can do.
	newCfg.Credentials.Enabled = false
	logger = &recordingLogger{}
	reloadCredentials(registry, newCfg, cfg, logger)
	if len(logger.warn) != 1 {
		t.Errorf("warned %v, want one warning that the flag is read at startup", logger.warn)
	}
	if _, ok := registry.Lookup("AKIARELOADED00000001"); !ok {
		t.Error("the entry must survive; a reload cannot take the registry away either")
	}
}

// TestNewFaultController_AlwaysBuiltAndSeeded covers the two wiring properties a
// default `substrate server` depends on. The controller used to be built only when
// the config named faults, so /v1/fault/rules answered 501 on a default server and
// a harness could not arm a rule at all; and its seed came from
// time.Now().UnixNano(), so a probabilistic rule was irreproducible between runs.
func TestNewFaultController_AlwaysBuiltAndSeeded(t *testing.T) {
	defaults := substrate.DefaultConfig()
	if defaults.Fault.Enabled {
		t.Fatal("precondition: the default config must not enable fault injection")
	}
	if len(defaults.Fault.Rules) != 0 {
		t.Fatal("precondition: the default config must carry no rules")
	}
	if defaults.Fault.Seed != 0 {
		t.Errorf("default seed = %d, want 0 — a wall-clock seed is not reproducible",
			defaults.Fault.Seed)
	}

	// Built even though nothing in the config asked for faults.
	fc := newFaultController(defaults.Fault)
	if fc == nil {
		t.Fatal("newFaultController returned nil for a default config; /v1/fault/rules would 501")
	}

	// Disabled until something arms it, so behavior is unchanged for such a run.
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	if awsErr, delay := fc.InjectFault(&substrate.RequestContext{}, req); awsErr != nil || delay != 0 {
		t.Errorf("a controller from a default config injected %v/%v, want nothing", awsErr, delay)
	}

	// Arming it makes the configured rule fire, which is what the endpoint does.
	fc.UpdateConfig(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{{
			Service: "s3", Operation: "PutObject",
			FaultType: "error", ErrorCode: "InternalError", Times: 1,
		}},
	})
	awsErr, _ := fc.InjectFault(&substrate.RequestContext{}, req)
	if awsErr == nil || awsErr.Code != "InternalError" {
		t.Errorf("armed rule did not fire: %v", awsErr)
	}

	// The configured seed reaches the controller: the same seed traces identically
	// and a different one does not.
	trace := func(seed int64) []bool {
		c := newFaultController(substrate.FaultCfg{
			Enabled: true,
			Seed:    seed,
			Rules: []substrate.FaultRuleCfg{{
				Service: "s3", FaultType: "error", ErrorCode: "InternalError",
				Probability: 0.5, Times: -1,
			}},
		})
		got := make([]bool, 24)
		for i := range got {
			e, _ := c.InjectFault(&substrate.RequestContext{}, req)
			got[i] = e != nil
		}
		return got
	}
	want := trace(77)
	if !slices.Equal(want, trace(77)) {
		t.Error("the configured seed must reproduce a run exactly")
	}
	if slices.Equal(want, trace(78)) {
		t.Error("a different configured seed must produce a different run")
	}
	if !slices.Contains(want, true) || !slices.Contains(want, false) {
		t.Error("the probe must both fire and decline, or reproducibility is vacuous")
	}
}
