package emulator

import (
	"context"
	"path"
	"sort"
	"testing"
)

// Replay replays the server's [DefaultStreamID] stream, which holds every request sent
// to [TestServer.URL], and returns what it found. It fails the test if the replay could
// not run.
//
// The stream is replayed into a [ReplaySandbox] built from the server's configuration,
// with a copy of its armed fault rules and a control plane, so recorded seeds are
// re-applied and the server's own state is left as it was. State hashes are compared
// when the server records them ([WithRecordedStateHashes]).
//
// A [TestServer.ResetState] call is not an event, so a stream that spans one replays as
// one continuous run. Use [TestServer.AssertReplay] to fail the test on any difference.
func (ts *TestServer) Replay(tb testing.TB) *ReplayResults {
	tb.Helper()
	results, err := ts.ReplaySandbox(tb).Engine().Replay(context.Background(), DefaultStreamID)
	if err != nil {
		tb.Fatalf("Replay: %v", err)
	}
	return results
}

// ReplaySandbox returns a [ReplaySandbox] over this server's event store, built as the
// server was: its configuration, and the quota, consistency and fault controllers it
// runs with, so a replay re-decides each request as the server did. It fails the test if
// the sandbox cannot be built. Use it for time travel over the server's stream:
// [ReplaySandbox.StateAt], or [ReplayEngine.JumpToEvent] and [ReplayEngine.StepBackward]
// on its Engine.
func (ts *TestServer) ReplaySandbox(tb testing.TB) *ReplaySandbox {
	tb.Helper()
	sandbox, err := NewReplaySandbox(context.Background(), &ts.srv.config, ts.store, ts.srv.logger,
		withSandboxGatesOf(ts.srv.opts),
		WithSandboxReplayConfig(ReplayConfig{ValidateState: ts.store.RecordsStateHashes()}))
	if err != nil {
		tb.Fatalf("ReplaySandbox: %v", err)
	}
	return sandbox
}

// AssertReplay replays the server's stream as [TestServer.Replay] does and fails the
// test, through [AssertReplayMatches], unless the replay reproduced it.
func (ts *TestServer) AssertReplay(tb testing.TB, allow ...string) *ReplayResults {
	tb.Helper()
	results := ts.Replay(tb)
	AssertReplayMatches(tb, results, allow...)
	return results
}

// ReplayEvents replays events into this server and returns what it found, failing the
// test if the replay could not run. The server's state is reset first and holds the
// replayed state afterwards, so a test can keep calling [TestServer.URL] from the point
// the events reached. This is what a fixture from [GenerateTestFixture] calls.
//
// Recorded seeds are re-issued against this server's control plane, and authorization
// and faults are re-decided by its own controllers.
func (ts *TestServer) ReplayEvents(tb testing.TB, events []*Event) *ReplayResults {
	tb.Helper()
	engine := NewReplayEngine(ts.store, ts.state, ts.tc, ts.registry,
		ReplayConfig{ValidateState: ts.store.RecordsStateHashes()}, ts.srv.logger,
		WithControlPlaneHandler(ts.srv),
		WithReplayPipeline(ReplayPipeline{Auth: ts.srv.opts.Auth, Fault: ts.srv.opts.Fault}))
	results, err := engine.ReplayEvents(context.Background(), events)
	if err != nil {
		tb.Fatalf("ReplayEvents: %v", err)
	}
	return results
}

// AssertReplayMatches fails the test for every difference in results whose
// [EventDifference.Field] matches none of the allow patterns, and for every event that
// was skipped for a reason that leaves it unchecked (see [ReplaySkipReason.Unverified]).
//
// A pattern uses [path.Match] syntax against the field, so "response_body/*/TableId"
// allows one member of any top-level object. No field is allowed by default; a replay
// of a recording reproduces request IDs, minted identifiers and the clock, so a
// difference is a real divergence unless the caller knows why it is not.
func AssertReplayMatches(tb testing.TB, results *ReplayResults, allow ...string) {
	tb.Helper()
	if results == nil {
		tb.Fatalf("AssertReplayMatches: no results")
		return
	}
	for _, d := range results.Differences {
		if fieldAllowed(d.Field, allow) {
			continue
		}
		tb.Errorf("replay diverged at seq %d (%s) %s: recorded %v, replayed %v",
			d.Sequence, d.Operation, d.Field, d.Expected, d.Actual)
	}
	reasons := make([]string, 0, len(results.SkipReasons))
	for reason := range results.SkipReasons {
		reasons = append(reasons, string(reason))
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		if r := ReplaySkipReason(reason); r.Unverified() {
			tb.Errorf("replay skipped %d event(s): %s", results.SkipReasons[r], r)
		}
	}
}

// fieldAllowed reports whether field matches one of the patterns.
func fieldAllowed(field string, patterns []string) bool {
	for _, p := range patterns {
		if ok, err := path.Match(p, field); err == nil && ok {
			return true
		}
	}
	return false
}
