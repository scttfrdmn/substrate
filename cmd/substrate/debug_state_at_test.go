package main

import (
	"encoding/json"
	"strings"
	"testing"

	substrate "github.com/scttfrdmn/substrate/emulator"
)

// TestDebugStateAtCmd_PrintsTheReplayedState covers `substrate debug state-at`, which
// replaced the "available in a later release" text (#1601). It replays a file-backed
// stream through the same sandbox the debug endpoint uses and prints its StateAt.
func TestDebugStateAtCmd_PrintsTheReplayedState(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)
	cfgPath := writeCLIConfig(t, dir)

	out := captureStdout(t, func() {
		if err := run([]string{"debug", "state-at", "1", "--config", cfgPath, "--stream", cliTestStream}); err != nil {
			t.Fatalf("debug state-at: %v", err)
		}
	})

	var at substrate.StateAt
	if err := json.Unmarshal([]byte(out), &at); err != nil {
		t.Fatalf("state-at printed something that is not a StateAt: %v\n%s", err, out)
	}
	if at.StreamID != cliTestStream || at.Sequence != 1 || at.EventsApplied != 1 {
		t.Errorf("state-at = stream %q seq %d applied %d, want %q 1 1", at.StreamID, at.Sequence, at.EventsApplied, cliTestStream)
	}
	// The recorded responses carry no body, so the replayed body is reported as a
	// difference; what matters here is that the event ran and the difference is printed.
	if at.FailedEvents != 0 || len(at.Skipped) != 0 {
		t.Errorf("the recorded GetCallerIdentity did not replay:\n%s", out)
	}
	if len(at.Differences) != 1 || at.Differences[0].Field != "response_body" {
		t.Errorf("want the one body difference, got:\n%s", out)
	}
	if len(at.State) == 0 {
		t.Error("no state printed")
	}
}

// TestDebugStateAtCmd_Refusals covers the two errors a caller can make.
func TestDebugStateAtCmd_Refusals(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 1)
	cfgPath := writeCLIConfig(t, dir)

	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"not a number":   {[]string{"debug", "state-at", "one", "--config", cfgPath}, "not a number"},
		"unknown stream": {[]string{"debug", "state-at", "0", "--config", cfgPath, "--stream", "nope"}, "no events in stream"},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) = %v, want an error containing %q", tc.args, err, tc.want)
			}
		})
	}
}
