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
