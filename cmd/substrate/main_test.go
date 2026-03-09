package main

import (
	"strings"
	"testing"
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
