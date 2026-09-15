package main

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate/emulator"
)

// cliTestStream is the stream every test here records into. It is not "default",
// so a test that reads the wrong stream reads nothing rather than accidentally
// finding events.
const cliTestStream = "cli-wiring-stream"

// writeCLIConfig writes a substrate.yaml naming a file-backed event store rooted
// at dir and returns its path.
//
// A file backend is the whole point of these tests: with the memory backend the
// defect is invisible, because a fresh in-process store legitimately holds nothing.
// include_bodies must be on, since an event recorded without a request cannot be
// re-executed and the replay would skip every one of them.
//
// Each extra section is appended verbatim, so a test can add a replay: or state:
// block without restating the event store (#880, #881).
func writeCLIConfig(t *testing.T, dir string, extra ...string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "substrate.yaml")
	body := "event_store:\n" +
		"  enabled: true\n" +
		"  backend: \"file\"\n" +
		"  persist_path: \"" + dir + "\"\n" +
		"  include_bodies: true\n" +
		strings.Join(extra, "")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// recordCLIStream records n replayable events into a file-backed store under dir
// and flushes them, leaving on disk exactly what a recorded `substrate server` run
// leaves behind.
//
// GetCallerIdentity is the operation because it routes on nothing but
// req.Operation, needs no prior state, and is not authorized — so an event that
// fails to replay failed because of the wiring under test and not because of the
// request.
// The state hashes recordCLIStream attaches under withCLIStateHashes. They are
// sentinels rather than real hashes on purpose: a replay of the stream computes the
// hash of its own state and cannot arrive at either, so the comparison
// replay.validate_state turns on reports a mismatch deterministically, without the
// test having to reproduce the hashing (#880).
const (
	cliRecordedHashBefore = "recorded-before-hash"
	cliRecordedHashAfter  = "recorded-after-hash"
)

// cliRecordSettings collects what a [recordCLIStream] option changes.
type cliRecordSettings struct {
	stateHashes bool
}

// withCLIStateHashes records a state hash with every event, which is what a
// replay's state validation has to have something to compare against.
func withCLIStateHashes(s *cliRecordSettings) { s.stateHashes = true }

func recordCLIStream(t *testing.T, dir string, n int, opts ...func(*cliRecordSettings)) {
	t.Helper()

	var settings cliRecordSettings
	for _, opt := range opts {
		opt(&settings)
	}

	cfg := substrate.DefaultConfig()
	cfg.EventStore.Enabled = true
	cfg.EventStore.Backend = "file"
	cfg.EventStore.PersistPath = dir
	cfg.EventStore.IncludeBodies = true
	cfg.EventStore.IncludeStateHashes = settings.stateHashes

	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	defer store.Close() //nolint:errcheck

	var recordOpts []substrate.EventRecordOption
	if settings.stateHashes {
		recordOpts = append(recordOpts,
			substrate.WithStateHashes(cliRecordedHashBefore, cliRecordedHashAfter))
	}

	ctx := context.Background()
	for i := 0; i < n; i++ {
		reqCtx := &substrate.RequestContext{
			RequestID: "req-recorded",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Metadata:  map[string]interface{}{"stream_id": cliTestStream},
		}
		req := &substrate.AWSRequest{
			Service:   "sts",
			Operation: "GetCallerIdentity",
			Headers:   map[string]string{},
			Params:    map[string]string{"Action": "GetCallerIdentity", "Version": "2011-06-15"},
			Path:      "/",
		}
		if err := store.RecordRequest(ctx, reqCtx, req, &substrate.AWSResponse{StatusCode: 200}, 0, 0, nil,
			recordOpts...); err != nil {
			t.Fatalf("record event %d: %v", i, err)
		}
	}
	if err := store.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// TestNewLoadedEventStore_LoadsWhatTheBackendHolds covers the one-line cause
// behind all three commands (#855, #879): NewEventStore builds a file backend
// eagerly "so Load can be called right away", and nothing in cmd/ ever called it.
//
// The first half of the test is the defect itself, kept as an assertion rather
// than a comment — a store built the way the three commands built it reports zero
// events for a stream that is sitting on disk, with no error to say so.
func TestNewLoadedEventStore_LoadsWhatTheBackendHolds(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)

	cfg, err := substrate.LoadConfig(writeCLIConfig(t, dir))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	ctx := context.Background()

	unloaded := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	defer unloaded.Close() //nolint:errcheck
	events, err := unloaded.GetStream(ctx, cliTestStream)
	if err != nil {
		t.Fatalf("get stream from unloaded store: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("precondition: an unloaded store held %d events; the test no longer covers the defect", len(events))
	}

	loaded, err := newLoadedEventStore(ctx, cfg, substrate.NewTimeController(time.Now()), &recordingLogger{})
	if err != nil {
		t.Fatalf("newLoadedEventStore: %v", err)
	}
	defer loaded.Close() //nolint:errcheck

	events, err = loaded.GetStream(ctx, cliTestStream)
	if err != nil {
		t.Fatalf("get stream from loaded store: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("loaded store holds %d events, want the 2 that were recorded", len(events))
	}
	if events[0].Request == nil {
		t.Error("loaded event carries no request; include_bodies did not survive the round trip and nothing could be replayed")
	}
}

// TestNewReplayEngineWiring_ReExecutesAFileBackedStream is the criterion #855
// exists for: a stream recorded to a file backend replays through the command's
// own wiring path and re-executes a non-zero number of events.
//
// It fails three separate ways against the old wiring — at "no events in stream"
// because the store was never loaded, then with every event failed because the
// registry was empty, and it could not distinguish either from success because the
// summary omitted Skipped.
func TestNewReplayEngineWiring_ReExecutesAFileBackedStream(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 3)

	cfg, err := substrate.LoadConfig(writeCLIConfig(t, dir))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	ctx := context.Background()

	wiring, err := newReplayEngineWiring(ctx, cfg, &recordingLogger{})
	if err != nil {
		t.Fatalf("newReplayEngineWiring: %v", err)
	}
	defer wiring.store.Close() //nolint:errcheck

	results, err := wiring.engine.Replay(ctx, cliTestStream)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if results.TotalEvents != 3 {
		t.Errorf("TotalEvents = %d, want 3", results.TotalEvents)
	}
	if results.SuccessEvents != 3 {
		t.Errorf("SuccessEvents = %d, want 3 — an empty registry answers ServiceNotAvailable for every event",
			results.SuccessEvents)
	}
	if results.FailedEvents != 0 {
		t.Errorf("FailedEvents = %d, want 0", results.FailedEvents)
	}
	if results.SkippedEvents != 0 {
		t.Errorf("SkippedEvents = %d, want 0 — a skip means the event carried no request and nothing was verified",
			results.SkippedEvents)
	}
}

// TestNewReplayEngineWiring_DoesNotWriteToTheStreamItReplays covers the hazard
// that wiring a real registry introduces: the CloudFormation deployer records its
// in-process resource calls, so a replay against the loaded store would append
// events that never happened in the recorded run — and the automatic flush would
// put them in the recording, so the next replay of the same stream would read a
// longer stream.
//
// The plugins therefore get a recording-disabled store. This asserts the store
// being replayed is unchanged by the replay, which is the property that keeps the
// event log immutable.
func TestNewReplayEngineWiring_DoesNotWriteToTheStreamItReplays(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)

	cfg, err := substrate.LoadConfig(writeCLIConfig(t, dir))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	ctx := context.Background()

	wiring, err := newReplayEngineWiring(ctx, cfg, &recordingLogger{})
	if err != nil {
		t.Fatalf("newReplayEngineWiring: %v", err)
	}
	defer wiring.store.Close() //nolint:errcheck

	before := wiring.store.GetStats(ctx).TotalEvents
	if _, err := wiring.engine.Replay(ctx, cliTestStream); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if after := wiring.store.GetStats(ctx).TotalEvents; after != before {
		t.Errorf("the replayed store went from %d to %d events; a replay must not write to the log it reads",
			before, after)
	}

	// And nothing reached the recording on disk either, which is what a second
	// replay would otherwise pick up.
	if err := wiring.store.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if n := countRecordedLines(t, dir); n != 2 {
		t.Errorf("the recording on disk holds %d events, want the 2 that were recorded", n)
	}
}

// countRecordedLines counts the NDJSON records the file backend wrote under dir.
func countRecordedLines(t *testing.T, dir string) int {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "events", "*.ndjson"))
	if err != nil {
		t.Fatalf("glob recordings: %v", err)
	}
	total := 0
	for _, m := range matches {
		f, err := os.Open(m) //nolint:gosec // a path this test just wrote
		if err != nil {
			t.Fatalf("open recording: %v", err)
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) != "" {
				total++
			}
		}
		if err := sc.Err(); err != nil {
			f.Close() //nolint:errcheck,gosec
			t.Fatalf("scan recording: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close recording: %v", err)
		}
	}
	return total
}

// TestReplayCmd_ReportsSkippedAndState covers the summary the command prints.
// Before #855 it printed Total, Success, Failed and Duration only — omitting
// Skipped, StateValid and StateErrors, which is to say the two fields
// ReplayResults' own doc comment names as the verdict.
func TestReplayCmd_ReportsSkippedAndState(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)
	cfgPath := writeCLIConfig(t, dir)

	out := captureStdout(t, func() {
		if err := run([]string{"replay", "--config", cfgPath, cliTestStream}); err != nil {
			t.Fatalf("replay command: %v", err)
		}
	})

	for _, want := range []string{"Total:", "Success:", "Skipped:", "State:"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary omits %q; printed:\n%s", want, out)
		}
	}
	// "not checked" rather than "valid": StateValid starts true and is only ever
	// falsified by a comparison ValidateState gates, so reporting it as valid when
	// validation never ran would be a verdict nothing produced (#880).
	if !strings.Contains(out, "State:    not checked") {
		t.Errorf("state line does not say validation was off; printed:\n%s", out)
	}
	if !strings.Contains(out, "Success:  2") {
		t.Errorf("summary reports no re-executed events; printed:\n%s", out)
	}
}

// TestReplayCmd_UnknownStreamNamesWhereItLooked covers the error #855 asks for:
// the bare "no events in stream" is true and useless, because the two likely
// causes — a memory backend and the wrong persist path — are both invisible
// without naming the backend and the path.
func TestReplayCmd_UnknownStreamNamesWhereItLooked(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 1)
	cfgPath := writeCLIConfig(t, dir)

	err := run([]string{"replay", "--config", cfgPath, "no-such-stream"})
	if err == nil {
		t.Fatal("replaying an unknown stream succeeded")
	}
	msg := err.Error()
	for _, want := range []string{"no-such-stream", "file", dir} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not name %q", msg, want)
		}
	}
	// It also says what the store *does* hold, so a wrong stream name is
	// distinguishable from an empty store.
	if !strings.Contains(msg, "holds 1 event(s)") {
		t.Errorf("error %q does not report what the store holds", msg)
	}
}

// TestExportCmd_ExportsAFileBackedStream covers #879's first half: export read
// GetEvents, which is memory only, so it wrote an empty document with exit status
// 0 — and on the default --output - it printed nothing at all to say so.
func TestExportCmd_ExportsAFileBackedStream(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 3)
	cfgPath := writeCLIConfig(t, dir)

	outPath := filepath.Join(t.TempDir(), "events.ndjson")
	if err := run([]string{"export", "--config", cfgPath, "--output", outPath}); err != nil {
		t.Fatalf("export command: %v", err)
	}

	data, err := os.ReadFile(outPath) //nolint:gosec // a path this test chose
	if err != nil {
		t.Fatalf("read export: %v", err)
	}
	lines := 0
	for _, l := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(l) != "" {
			lines++
		}
	}
	if lines != 3 {
		t.Errorf("export wrote %d records, want the 3 recorded events", lines)
	}
}

// TestExportCmd_EmptyResultSaysWhichEmptyItIs covers #879's fourth criterion. A
// zero-event export used to be indistinguishable from a full one; which of the two
// empties it is decides whether the user widens the filter or points at the right
// backend.
func TestExportCmd_EmptyResultSaysWhichEmptyItIs(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)
	cfgPath := writeCLIConfig(t, dir)

	outPath := filepath.Join(t.TempDir(), "events.ndjson")
	stderr := captureStderr(t, func() {
		if err := run([]string{"export", "--config", cfgPath, "--output", outPath, "--stream", "no-such-stream"}); err != nil {
			t.Fatalf("export command: %v", err)
		}
	})
	if !strings.Contains(stderr, "no event matched the filter") {
		t.Errorf("a filtered-out export said %q; it must not read as an empty store", stderr)
	}

	// The other empty: a store that holds nothing at all.
	emptyCfg := writeCLIConfig(t, t.TempDir())
	stderr = captureStderr(t, func() {
		if err := run([]string{"export", "--config", emptyCfg, "--output", outPath}); err != nil {
			t.Fatalf("export command: %v", err)
		}
	})
	if !strings.Contains(stderr, "holds none") {
		t.Errorf("an empty-store export said %q; it must not read as a filter miss", stderr)
	}
}

// TestDebugCmd_ListsAFileBackedStream covers #879's second half. debug loaded the
// config and discarded it, hardcoding a memory backend — so it printed "contains
// no events" for every stream that has ever existed, under help text promising to
// list them.
func TestDebugCmd_ListsAFileBackedStream(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)
	cfgPath := writeCLIConfig(t, dir)

	out := captureStdout(t, func() {
		if err := run([]string{"debug", "--config", cfgPath, cliTestStream}); err != nil {
			t.Fatalf("debug command: %v", err)
		}
	})

	if !strings.Contains(out, "(2 events)") {
		t.Errorf("debug did not list the recorded events; printed:\n%s", out)
	}
	if !strings.Contains(out, "sts/GetCallerIdentity") {
		t.Errorf("debug did not print the recorded operation; printed:\n%s", out)
	}
	if strings.Contains(out, "contains no events") {
		t.Errorf("debug still reports an empty stream; printed:\n%s", out)
	}
}

// TestDebugCmd_UnknownStreamSaysWhatTheStoreHolds covers the other half of the
// same message: an empty stream now says where it looked and what was there, so a
// mistyped stream ID is distinguishable from a store that holds nothing.
func TestDebugCmd_UnknownStreamSaysWhatTheStoreHolds(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 2)
	cfgPath := writeCLIConfig(t, dir)

	out := captureStdout(t, func() {
		if err := run([]string{"debug", "--config", cfgPath, "no-such-stream"}); err != nil {
			t.Fatalf("debug command: %v", err)
		}
	})
	if !strings.Contains(out, "holds 2 event(s) in 1 stream(s)") {
		t.Errorf("debug did not report what the store holds; printed:\n%s", out)
	}
}

// TestEventStoreLocation covers the message the three commands use to say where
// they looked. The memory case is the one that matters: it is the shipped default,
// and it is why a user who recorded a run in one process finds nothing in the next.
func TestEventStoreLocation(t *testing.T) {
	tests := []struct {
		name string
		cfg  substrate.EventStoreCfg
		want []string
	}{
		{
			name: "memory says it cannot hold anything across processes",
			cfg:  substrate.EventStoreCfg{Backend: "memory"},
			want: []string{"memory", "no events from a previous process"},
		},
		{
			name: "file names the persist path",
			cfg:  substrate.EventStoreCfg{Backend: "file", PersistPath: "/tmp/substrate-events"},
			want: []string{"file", "/tmp/substrate-events"},
		},
		{
			name: "file with no persist path says so rather than naming an empty path",
			cfg:  substrate.EventStoreCfg{Backend: "file"},
			want: []string{"file", "no persist_path"},
		},
		{
			name: "sqlite falls back to the documented default DSN",
			cfg:  substrate.EventStoreCfg{Backend: "sqlite"},
			want: []string{"sqlite", "substrate.db"},
		},
		{
			name: "sqlite names a configured DSN",
			cfg:  substrate.EventStoreCfg{Backend: "sqlite", DSN: "runs.db"},
			want: []string{"sqlite", "runs.db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := eventStoreLocation(tt.cfg)
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("eventStoreLocation(%+v) = %q, want it to name %q", tt.cfg, got, w)
				}
			}
		})
	}
}

// TestReplayStateSummary covers the distinction the summary has to draw.
// ReplayResults.StateValid starts true and is only falsified by a comparison
// ValidateState gates, so a replay that reached a completely different state still
// reports StateValid: true whenever no comparison ran. Printing that as "valid"
// would be a verdict nothing produced.
//
// The third state — validation on, but nothing in the stream to compare against —
// became reachable with #880, because event_store.include_state_hashes is off by
// default and a user who sets replay.validate_state: true is likelier to have a
// stream without hashes than one with them.
func TestReplayStateSummary(t *testing.T) {
	tests := []struct {
		name       string
		cfg        substrate.ReplayConfig
		results    *substrate.ReplayResults
		comparable bool
		want       string
	}{
		{
			name:       "validation off is not a passing verdict",
			cfg:        substrate.ReplayConfig{},
			results:    &substrate.ReplayResults{StateValid: true},
			comparable: true,
			want:       "not checked (replay.validate_state is off)",
		},
		{
			name:    "validation on over a stream with no recorded hashes compared nothing",
			cfg:     substrate.ReplayConfig{ValidateState: true},
			results: &substrate.ReplayResults{StateValid: true},
			want: "not checked (no event in the stream carries a recorded state hash; " +
				"record with event_store.include_state_hashes: true)",
		},
		{
			name:       "validation on and matching",
			cfg:        substrate.ReplayConfig{ValidateState: true},
			results:    &substrate.ReplayResults{StateValid: true},
			comparable: true,
			want:       "valid",
		},
		{
			name: "validation on and diverged reports how many",
			cfg:  substrate.ReplayConfig{ValidateState: true},
			results: &substrate.ReplayResults{
				StateValid:  false,
				StateErrors: []string{"seq 1", "seq 4"},
			},
			comparable: true,
			want:       "MISMATCH (2 error(s))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replayStateSummary(tt.cfg, tt.results, tt.comparable); got != tt.want {
				t.Errorf("replayStateSummary = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestStreamHasStateHashes covers the input that decision rests on: whether the
// recording carries anything to compare, which is a property of the events rather
// than of the config the replay runs under (#880).
func TestStreamHasStateHashes(t *testing.T) {
	tests := []struct {
		name   string
		events []*substrate.Event
		want   bool
	}{
		{name: "no events", events: nil, want: false},
		{
			name:   "a stream recorded without include_state_hashes",
			events: []*substrate.Event{{ID: "a"}, {ID: "b"}},
			want:   false,
		},
		{
			name:   "one hashed event is enough to compare",
			events: []*substrate.Event{{ID: "a"}, {ID: "b", StateHashAfter: "h"}},
			want:   true,
		},
		{
			name:   "a before hash counts too",
			events: []*substrate.Event{{ID: "a", StateHashBefore: "h"}},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := streamHasStateHashes(tt.events); got != tt.want {
				t.Errorf("streamHasStateHashes = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNewReplayEngineWiring_UsesTheConfiguredReplaySection is #880's core
// criterion: the engine is built from the replay: section rather than from
// substrate.ReplayConfig{}. Before it, no value written in a config file reached the
// replay engine at all — ValidateState in particular could not be switched on from
// the CLI by any means.
func TestNewReplayEngineWiring_UsesTheConfiguredReplaySection(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 1)

	cfgPath := writeCLIConfig(t, dir,
		"replay:\n"+
			"  speed_multiplier: 0\n"+
			"  stop_on_error: true\n"+
			"  validate_state: true\n"+
			"  use_snapshots: false\n"+
			"  random_seed: 4242\n")
	cfg, err := substrate.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	wiring, err := newReplayEngineWiring(context.Background(), cfg, &recordingLogger{})
	if err != nil {
		t.Fatalf("newReplayEngineWiring: %v", err)
	}
	defer wiring.store.Close() //nolint:errcheck

	want := substrate.ReplayConfig{StopOnError: true, ValidateState: true, RandomSeed: 4242}
	if wiring.config != want {
		t.Errorf("engine config = %+v, want %+v — the configured section did not reach the engine",
			wiring.config, want)
	}
}

// TestNewReplayEngineWiring_RefusesAnUnsupportedStateBackend covers #881 at the
// constructor rather than at load: a Config built in process can name a backend
// Validate never saw, and the answer must be an error rather than a memory manager
// the caller did not ask for.
func TestNewReplayEngineWiring_RefusesAnUnsupportedStateBackend(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 1)

	cfg, err := substrate.LoadConfig(writeCLIConfig(t, dir))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.State.Backend = "sqlite"

	wiring, err := newReplayEngineWiring(context.Background(), cfg, &recordingLogger{})
	if err == nil {
		wiring.store.Close() //nolint:errcheck,gosec
		t.Fatal("the replay engine was wired to a state manager the config did not ask for")
	}
	for _, want := range []string{"state manager", "sqlite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err.Error(), want)
		}
	}
}

// TestReplayCmd_StateVerdictFollowsTheConfiguredSection is the end-to-end shape of
// #880: the same recorded stream, replayed through the command's own RunE, produces
// a different state verdict for a different replay: section — which is only possible
// once the section reaches the engine.
//
// The MISMATCH case is also the assertion that ReplayResults.StateErrors is
// populated. Nothing ever appended to it, so with validation newly reachable the
// summary would have printed "MISMATCH (0 error(s))" and listed nothing.
func TestReplayCmd_StateVerdictFollowsTheConfiguredSection(t *testing.T) {
	tests := []struct {
		name          string
		stateHashes   bool
		replaySection string
		wantState     string
		wantLines     []string
	}{
		{
			name:        "no replay section leaves validation off",
			stateHashes: true,
			wantState:   "State:    not checked (replay.validate_state is off)",
		},
		{
			name:          "validate_state on reports the divergence and describes each one",
			stateHashes:   true,
			replaySection: "replay:\n  validate_state: true\n",
			wantState:     "State:    MISMATCH (2 error(s))",
			wantLines: []string{
				"state_hash_before: recorded " + cliRecordedHashBefore,
				"state_hash_after: recorded " + cliRecordedHashAfter,
			},
		},
		{
			name:          "validate_state on with no recorded hashes says so instead of passing",
			stateHashes:   false,
			replaySection: "replay:\n  validate_state: true\n",
			wantState:     "State:    not checked (no event in the stream carries a recorded state hash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.stateHashes {
				recordCLIStream(t, dir, 1, withCLIStateHashes)
			} else {
				recordCLIStream(t, dir, 1)
			}
			cfgPath := writeCLIConfig(t, dir, tt.replaySection)

			out := captureStdout(t, func() {
				if err := run([]string{"replay", "--config", cfgPath, cliTestStream}); err != nil {
					t.Fatalf("replay command: %v", err)
				}
			})

			if !strings.Contains(out, tt.wantState) {
				t.Errorf("summary does not report %q; printed:\n%s", tt.wantState, out)
			}
			for _, want := range tt.wantLines {
				if !strings.Contains(out, want) {
					t.Errorf("summary omits the mismatch description %q; printed:\n%s", want, out)
				}
			}
			// The event was re-executed either way; the verdict is the only thing
			// the replay: section changes here.
			if !strings.Contains(out, "Success:  1") {
				t.Errorf("summary reports no re-executed event; printed:\n%s", out)
			}
		})
	}
}

// TestReplayCmd_RefusesAnUnsupportedStateBackend and its server counterpart are
// #881's startup criterion: a config naming a backend nothing implements fails
// before anything is built, naming the backend, rather than running in memory.
func TestReplayCmd_RefusesAnUnsupportedStateBackend(t *testing.T) {
	dir := t.TempDir()
	recordCLIStream(t, dir, 1)
	cfgPath := writeCLIConfig(t, dir, "state:\n  backend: sqlite\n")

	err := run([]string{"replay", "--config", cfgPath, cliTestStream})
	if err == nil {
		t.Fatal("replay ran with a state backend the config did not ask for")
	}
	for _, want := range []string{"state.backend", "sqlite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err.Error(), want)
		}
	}
}

// TestServerCmd_RefusesAnUnsupportedStateBackend covers the site #881 names:
// `substrate server` called NewMemoryStateManager() unconditionally, so state: was
// configuration the server read nothing from.
//
// The address is deliberately unbindable. The refusal happens while loading the
// config, before anything listens, so a passing run never opens a socket — and if the
// refusal ever regresses, the run fails at the address instead of binding a port or
// hanging, and this test says which happened.
func TestServerCmd_RefusesAnUnsupportedStateBackend(t *testing.T) {
	cfgPath := writeCLIConfig(t, t.TempDir(), "state:\n  backend: sqlite\n")

	err := run([]string{"server", "--config", cfgPath, "--address", "256.256.256.256:0"})
	if err == nil {
		t.Fatal("the server started with a state backend the config did not ask for")
	}
	for _, want := range []string{"state.backend", "sqlite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q; the server got past the config", err.Error(), want)
		}
	}
}

// captureStdout runs fn with os.Stdout redirected and returns what it printed.
// The commands print with fmt.Printf rather than to a cobra writer, so this is
// what lets a test read the summary a user sees.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	return captureFile(t, &os.Stdout, fn)
}

// captureStderr runs fn with os.Stderr redirected and returns what it printed.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	return captureFile(t, &os.Stderr, fn)
}

// captureFile redirects *target for the duration of fn and returns what was
// written to it.
//
// The read happens after the write end is closed, and the pipe is drained in a
// goroutine, so output larger than the pipe buffer cannot deadlock the test.
func captureFile(t *testing.T, target **os.File, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	orig := *target
	*target = w

	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			sb.WriteString(sc.Text())
			sb.WriteByte('\n')
		}
		done <- sb.String()
	}()

	func() {
		defer func() {
			*target = orig
			if err := w.Close(); err != nil {
				t.Errorf("close pipe writer: %v", err)
			}
		}()
		fn()
	}()

	out := <-done
	if err := r.Close(); err != nil {
		t.Errorf("close pipe reader: %v", err)
	}
	return out
}
