package emulator_test

// Plugin-held mutable state and what a reset reaches (#886).
//
// ReplayEngine.resetState reset the StateManager and nothing else, while three
// plugins keep the state they mint identifiers from on their own struct: the S3
// version-ID counter, the SES v2 message counter, and the HealthOmics random
// source. None of them was reachable from a reset, so replaying one recorded
// stream twice in a single process minted different identifiers the second time
// from byte-identical events — the one property the event log exists to rule out.
//
// Every test here records over the wire through a [emulator.TestServer] and then
// replays that stream through the same registry the server serves from, which is
// how `substrate replay` reaches the defect (#855) and the only arrangement in
// which the counters are the *same* counters.
//
// The simulated clock is frozen (scale 0) before recording, for two reasons. The
// minted values are "<clock>-<counter>" pairs, so with the clock running the time
// half varies between two replays on wall-clock elapsed time and the test would
// assert a race rather than the counter; and a test must not depend on wall-clock
// time at all. Freezing leaves the counter as the only thing that can differ,
// which is exactly the defect under test.
//
// The hook then turned out to owe more than a counter (#902, #903). A plugin also
// holds state it *started* rather than minted — S3's object payloads, Lambda's
// event-source-mapping pollers and warm containers, RDS's Postgres containers — and
// none of it was reachable from a reset either, so the tests from
// TestPluginReset_StateResetEmptiesS3ObjectPayloads on cover that second class.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// resetFrozenClock is the instant every recording in this file runs at. Any fixed
// time works; a round value makes a failure message readable.
var resetFrozenClock = time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

// startFrozenServer starts a recording test server whose simulated clock does not
// advance, so a minted "<clock>-<counter>" identifier varies only in its counter.
//
// This asked for a scale of zero before #1217 gave the controller a named frozen
// state. The two compute the same Now(), but a scale of zero is a value the control
// plane's own SetScale endpoint refuses, so it was a state this file could reach and a
// caller could not.
func startFrozenServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.SetTime(resetFrozenClock)
	ts.FreezeTime()
	return ts
}

// resetDoRequest issues req against ts and returns the status, response headers and
// body, failing the test if the request cannot be made.
func resetDoRequest(t *testing.T, method, url, host string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rdr)
	require.NoError(t, err)
	req.Host = host
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header, got
}

// resetS3Host is the virtual-host-free S3 endpoint the recordings use.
const resetS3Host = "s3.us-east-1.amazonaws.com"

// recordVersionedPutObjects creates a versioning-enabled bucket and writes count
// object versions over the wire, returning the version IDs the recording minted in
// the order they were minted.
func recordVersionedPutObjects(t *testing.T, ts *emulator.TestServer, bucket string, count int) []string {
	t.Helper()
	status, _, _ := resetDoRequest(t, http.MethodPut, ts.URL+"/"+bucket, resetS3Host, nil)
	require.Equal(t, http.StatusOK, status, "CreateBucket")

	status, _, _ = resetDoRequest(t, http.MethodPut, ts.URL+"/"+bucket+"?versioning", resetS3Host,
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	require.Equal(t, http.StatusOK, status, "PutBucketVersioning")

	minted := make([]string, 0, count)
	for i := 0; i < count; i++ {
		status, header, _ := resetDoRequest(t, http.MethodPut, ts.URL+"/"+bucket+"/obj", resetS3Host,
			[]byte(fmt.Sprintf("body-%d", i)))
		require.Equal(t, http.StatusOK, status, "PutObject %d", i)
		id := header.Get("x-amz-version-id")
		require.NotEmpty(t, id, "PutObject %d minted no version ID", i)
		minted = append(minted, id)
	}
	return minted
}

// observeS3VersionIDs reads the version IDs S3 currently holds for bucket/obj.
//
// Read from the state manager rather than through ListObjectVersions on purpose: an
// AWS call over the wire would be *recorded*, so the second replay would be
// replaying a longer stream than the first and the two runs would no longer be
// comparable. This reads the same value the XML would render.
func observeS3VersionIDs(t *testing.T, ts *emulator.TestServer, bucket string) []string {
	t.Helper()
	data, err := ts.StateManager().Get(t.Context(), "s3", "object_versions:"+bucket+"/obj")
	require.NoError(t, err)
	require.NotNil(t, data, "no version list for %s/obj", bucket)

	var ids []string
	require.NoError(t, json.Unmarshal(data, &ids))
	return ids
}

// resetSendEmails sends count emails over the wire and returns the MessageIds the
// recording minted, in order.
//
// The clock half of a MessageId is the server's simulated clock, which
// [startFrozenServer] has frozen, so the counter half is the only thing that can
// differ between two replays — which is what a reset governs. Until #904 this file
// re-registered SES v2 by hand with the server's clock, because
// RegisterDefaultPlugins left it on a private wall-clock one; every registration now
// carries the clock, so the recording needs no help.
func resetSendEmails(t *testing.T, ts *emulator.TestServer, count int) []string {
	t.Helper()

	minted := make([]string, 0, count)
	for i := 0; i < count; i++ {
		body, err := json.Marshal(map[string]any{
			"FromEmailAddress": "sender@example.com",
			"Destination":      map[string]any{"ToAddresses": []string{fmt.Sprintf("rcpt%d@example.com", i)}},
			"Content": map[string]any{
				"Simple": map[string]any{
					"Subject": map[string]string{"Data": fmt.Sprintf("subject %d", i)},
					"Body":    map[string]any{"Text": map[string]string{"Data": "body"}},
				},
			},
		})
		require.NoError(t, err)

		status, _, got := resetDoRequest(t, http.MethodPost, ts.URL+"/v2/email/outbound-emails",
			"email.us-east-1.amazonaws.com", body)
		require.Equal(t, http.StatusOK, status, "SendEmail %d: %s", i, got)

		var out struct {
			MessageID string `json:"MessageId"`
		}
		require.NoError(t, json.Unmarshal(got, &out))
		require.NotEmpty(t, out.MessageID)
		minted = append(minted, out.MessageID)
	}
	return minted
}

// observeSESv2MessageIDs reads the captured MessageIds back over the wire through
// GET /v1/emails. That endpoint is control-plane, not an AWS API call, so reading it
// records no event and leaves the replayed stream untouched.
func observeSESv2MessageIDs(t *testing.T, ts *emulator.TestServer) []string {
	t.Helper()
	status, _, got := resetDoRequest(t, http.MethodGet, ts.URL+"/v1/emails", "", nil)
	require.Equal(t, http.StatusOK, status, "GET /v1/emails: %s", got)

	var out struct {
		Emails []emulator.SESv2CapturedEmail `json:"Emails"`
	}
	require.NoError(t, json.Unmarshal(got, &out))

	ids := make([]string, 0, len(out.Emails))
	for _, e := range out.Emails {
		ids = append(ids, e.MessageID)
	}
	sort.Strings(ids)
	return ids
}

// resetStartRuns starts count HealthOmics runs over the wire and returns the run IDs
// the recording minted, in order.
func resetStartRuns(t *testing.T, ts *emulator.TestServer, count int) []string {
	t.Helper()
	minted := make([]string, 0, count)
	for i := 0; i < count; i++ {
		body, err := json.Marshal(map[string]any{
			"workflowId":   "wf-1234",
			"workflowType": "PRIVATE",
			"name":         fmt.Sprintf("run-%d", i),
			"roleArn":      "arn:aws:iam::123456789012:role/OmicsRole",
		})
		require.NoError(t, err)

		status, _, got := resetDoRequest(t, http.MethodPost, ts.URL+"/run",
			"omics.us-east-1.amazonaws.com", body)
		require.Equal(t, http.StatusCreated, status, "StartRun %d: %s", i, got)

		var out struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(got, &out))
		require.NotEmpty(t, out.ID)
		minted = append(minted, out.ID)
	}
	return minted
}

// observeOmicsRunIDs reads the run IDs HealthOmics currently holds. Read from the
// state manager for the same reason as observeS3VersionIDs: a ListRuns call would be
// recorded into the stream under replay.
func observeOmicsRunIDs(t *testing.T, ts *emulator.TestServer) []string {
	t.Helper()
	keys, err := ts.StateManager().List(t.Context(), "omics", "run:")
	require.NoError(t, err)

	ids := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(k, "run_ids:") {
			continue
		}
		idx := strings.LastIndex(k, "/")
		require.Positive(t, idx, "unexpected run key %q", k)
		ids = append(ids, k[idx+1:])
	}
	require.NotEmpty(t, ids, "no runs in state")
	sort.Strings(ids)
	return ids
}

// replayOnce replays the recorded stream and asserts it actually re-executed, so an
// identifier comparison that follows cannot be satisfied by a replay that did
// nothing (#833).
func replayOnce(t *testing.T, ts *emulator.TestServer, label string) {
	t.Helper()
	engine := emulator.NewReplayEngine(
		ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
	)

	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err, "%s", label)
	require.Positive(t, results.TotalEvents, "%s: nothing was recorded", label)
	require.Zero(t, results.SkippedEvents, "%s: every recorded event must be re-executed", label)
	require.Zero(t, results.FailedEvents, "%s", label)
	require.Equal(t, results.TotalEvents, results.SuccessEvents, "%s", label)
}

// TestPluginReset_ReplayingTwiceMintsTheSameIdentifiers is the behavioral test for
// #886: one recorded stream, replayed twice in one process, must mint the same
// identifiers both times — and the same ones the recording minted.
//
// Before the fix each row failed on the first assertion below: the second replay
// continued the first replay's counter (or the first replay's position in the
// random source) and produced entirely different values from identical events.
//
// The recording is compared too, which is the stronger claim: a replay whose
// identifiers merely agree with each other could still agree on the wrong values.
func TestPluginReset_ReplayingTwiceMintsTheSameIdentifiers(t *testing.T) {
	tests := []struct {
		name string
		// record drives the workload over the wire and returns what it minted.
		record func(t *testing.T, ts *emulator.TestServer) []string
		// observe reads back the identifiers the emulator currently holds.
		observe func(t *testing.T, ts *emulator.TestServer) []string
		// field names the identifier, for the failure message.
		field string
	}{
		{
			name: "s3 object version IDs come from S3Plugin.versionSeq",
			record: func(t *testing.T, ts *emulator.TestServer) []string {
				return recordVersionedPutObjects(t, ts, "reset-versions", 3)
			},
			observe: func(t *testing.T, ts *emulator.TestServer) []string {
				return observeS3VersionIDs(t, ts, "reset-versions")
			},
			field: "x-amz-version-id",
		},
		{
			name: "sesv2 MessageIds come from SESv2Plugin.msgSeq",
			record: func(t *testing.T, ts *emulator.TestServer) []string {
				return resetSendEmails(t, ts, 3)
			},
			observe: observeSESv2MessageIDs,
			field:   "MessageId",
		},
		{
			name: "omics run IDs come from OmicsPlugin.rng",
			record: func(t *testing.T, ts *emulator.TestServer) []string {
				return resetStartRuns(t, ts, 3)
			},
			observe: observeOmicsRunIDs,
			field:   "run id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := startFrozenServer(t)

			recorded := tt.record(t, ts)
			require.Len(t, recorded, 3)
			require.Len(t, unique(recorded), 3, "the recording itself minted a duplicate %s", tt.field)

			replayOnce(t, ts, "first replay")
			first := tt.observe(t, ts)

			replayOnce(t, ts, "second replay")
			second := tt.observe(t, ts)

			assert.Equal(t, first, second,
				"two replays of one stream minted different %s values; the second continued the first run's counter", tt.field)

			assert.ElementsMatch(t, recorded, first,
				"a replay must reproduce the recording's %s values, not invent new ones", tt.field)
		})
	}
}

// unique returns the distinct members of in, used to assert a recording did not mint
// the same identifier twice — which would make the equality assertions vacuous.
func unique(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// TestPluginReset_StateResetRewindsAMintingCounter asserts the other door on the
// same defect: POST /v1/state/reset is documented as wiping emulator state and is
// what [emulator.TestServer.ResetState] calls between test cases, so a counter that
// survived it made the identifiers one test case observes depend on how many test
// cases ran before it.
//
// Entirely over the wire: identical requests before and after the reset must mint
// the identical version ID, because the clock is frozen and the counter is back
// where it started.
func TestPluginReset_StateResetRewindsAMintingCounter(t *testing.T) {
	ts := startFrozenServer(t)

	before := recordVersionedPutObjects(t, ts, "reset-endpoint", 2)
	ts.ResetState(t)

	after := recordVersionedPutObjects(t, ts, "reset-endpoint", 2)

	assert.Equal(t, before, after,
		"the same two writes after a state reset must mint the same version IDs; a surviving "+
			"counter makes them depend on everything that ran earlier in the process")
}

// resetCountingPlugin is a [emulator.Plugin] that records how many times it was
// reset, and optionally fails the reset, so the registry's iteration and error
// handling can be asserted without a real service.
type resetCountingPlugin struct {
	name  string
	err   error
	calls int
}

func (p *resetCountingPlugin) Name() string { return p.name }

func (p *resetCountingPlugin) Initialize(_ context.Context, _ emulator.PluginConfig) error {
	return nil
}

func (p *resetCountingPlugin) HandleRequest(_ *emulator.RequestContext, _ *emulator.AWSRequest) (*emulator.AWSResponse, error) {
	return &emulator.AWSResponse{StatusCode: http.StatusOK}, nil
}

func (p *resetCountingPlugin) Shutdown(_ context.Context) error { return nil }

func (p *resetCountingPlugin) ResetForRun(_ context.Context) error {
	p.calls++
	return p.err
}

// resetInertPlugin is a [emulator.Plugin] that does not implement
// [emulator.ResettablePlugin] — the shape 62 of the 67 registered plugins take,
// which is why the hook is an optional interface rather than a method on Plugin.
type resetInertPlugin struct{ name string }

func (p *resetInertPlugin) Name() string { return p.name }

func (p *resetInertPlugin) Initialize(_ context.Context, _ emulator.PluginConfig) error { return nil }

func (p *resetInertPlugin) HandleRequest(_ *emulator.RequestContext, _ *emulator.AWSRequest) (*emulator.AWSResponse, error) {
	return &emulator.AWSResponse{StatusCode: http.StatusOK}, nil
}

func (p *resetInertPlugin) Shutdown(_ context.Context) error { return nil }

// TestPluginRegistry_ResetPlugins covers the registry hook itself: who it calls, who
// it skips, and what it does with a failure.
func TestPluginRegistry_ResetPlugins(t *testing.T) {
	tests := []struct {
		name        string
		errs        map[string]error
		wantErrText []string
	}{
		{
			name: "every resettable plugin is reset and the rest are skipped",
		},
		{
			name: "a failing plugin is named and does not stop the others",
			errs: map[string]error{"beta": assert.AnError},
			wantErrText: []string{
				"reset plugin beta",
				assert.AnError.Error(),
			},
		},
		{
			name: "two failures are both reported",
			errs: map[string]error{"alpha": assert.AnError, "gamma": assert.AnError},
			wantErrText: []string{
				"reset plugin alpha",
				"reset plugin gamma",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := emulator.NewPluginRegistry()
			resettable := map[string]*resetCountingPlugin{}
			for _, name := range []string{"alpha", "beta", "gamma"} {
				p := &resetCountingPlugin{name: name, err: tt.errs[name]}
				resettable[name] = p
				registry.Register(p)
			}
			registry.Register(&resetInertPlugin{name: "inert"})

			err := registry.ResetPlugins(t.Context())

			if len(tt.wantErrText) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrText {
					assert.Contains(t, err.Error(), want)
				}
			}

			for name, p := range resettable {
				assert.Equal(t, 1, p.calls,
					"%s must be reset exactly once, whatever the other plugins did", name)
			}
		})
	}
}

// TestPluginRegistry_ResetPluginsIsSafeOnAFreshRegistry pins the two edge cases the
// replay engine and the reset endpoint rely on: an empty registry, and a plugin that
// has never handled a request.
func TestPluginRegistry_ResetPluginsIsSafeOnAFreshRegistry(t *testing.T) {
	require.NoError(t, emulator.NewPluginRegistry().ResetPlugins(t.Context()))

	registry := emulator.NewPluginRegistry()
	s3 := &emulator.S3Plugin{}
	require.NoError(t, s3.Initialize(t.Context(), emulator.PluginConfig{
		State:  emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(slog.LevelError, false),
	}))
	registry.Register(s3)

	require.NoError(t, registry.ResetPlugins(t.Context()),
		"resetting a plugin that has minted nothing must not fail")
}

// TestPluginReset_DefaultPluginsImplementingTheHook records the audit #886 asked for:
// the set of plugins that keep mutable state outside the StateManager is a known,
// asserted set rather than whatever happens to implement the interface.
//
// A new plugin that holds a counter and forgets the hook does not fail this test —
// nothing can detect that automatically — but a plugin that gains or loses the hook
// silently does, which is what makes the set reviewable.
func TestPluginReset_DefaultPluginsImplementingTheHook(t *testing.T) {
	ts := emulator.StartTestServer(t)

	var got []string
	for _, name := range ts.Registry().Names() {
		p, ok := ts.Registry().Plugin(name)
		require.True(t, ok, "%s is named by the registry but not registered in it", name)
		if _, ok := p.(emulator.ResettablePlugin); ok {
			got = append(got, name)
		}
	}

	assert.Equal(t, []string{"lambda", "omics", "rds", "s3", "sesv2"}, got,
		"the plugins holding mutable state outside the StateManager: S3's version-ID counter and "+
			"object payloads (#886, #902), SES v2's message counter and HealthOmics' random source "+
			"(#886), Lambda's event-source-mapping pollers and warm containers, and RDS's Postgres "+
			"containers (#903)")
}

// resetS3PluginOf returns the S3 plugin ts serves from, so a test can look at the
// object payloads (#902). They are not observable through any request: every read of
// the filesystem is gated on metadata the state manager holds, which a reset deletes.
func resetS3PluginOf(t *testing.T, ts *emulator.TestServer) *emulator.S3Plugin {
	t.Helper()
	p, ok := ts.Registry().Plugin("s3")
	require.True(t, ok, "the test server registers no s3 plugin")
	s3p, ok := p.(*emulator.S3Plugin)
	require.True(t, ok, "the registered s3 plugin is a %T", p)
	return s3p
}

// resetLambdaPluginOf returns the Lambda plugin ts serves from, for the same reason:
// a running poller has no representation in any response (#903).
func resetLambdaPluginOf(t *testing.T, ts *emulator.TestServer) *emulator.LambdaPlugin {
	t.Helper()
	p, ok := ts.Registry().Plugin("lambda")
	require.True(t, ok, "the test server registers no lambda plugin")
	lp, ok := p.(*emulator.LambdaPlugin)
	require.True(t, ok, "the registered lambda plugin is a %T", p)
	return lp
}

// TestPluginReset_StateResetEmptiesS3ObjectPayloads is the behavioral test for #902:
// object bytes live in an afero filesystem on the plugin, not in the StateManager, so
// a reset deleted every object's metadata and left every object's bytes behind. Nothing
// else releases them for the life of the process.
//
// The final third of the test is the one that matters most. Emptying the filesystem
// must leave it usable, and the obvious implementation — RemoveAll("/") — reports
// success, leaves every file beneath the root readable, and destroys the root entry so
// that nothing can be enumerated afterwards. A test that only asserted the paths are
// gone would pass against a filesystem that had silently kept them all.
func TestPluginReset_StateResetEmptiesS3ObjectPayloads(t *testing.T) {
	ts := startFrozenServer(t)
	s3 := resetS3PluginOf(t, ts)
	require.True(t, s3.OwnsFilesystemForTest(),
		"a server-hosted S3 plugin creates its own filesystem; nothing can inject one over HTTP")

	versions := recordVersionedPutObjects(t, ts, "reset-bytes", 2)
	require.Len(t, versions, 2)

	want := []string{"/reset-bytes/obj"}
	for _, v := range versions {
		want = append(want, "/reset-bytes/.versions/obj/"+v)
	}
	sort.Strings(want)

	before, err := s3.PayloadPathsForTest()
	require.NoError(t, err)
	require.Equal(t, want, before,
		"each PutObject writes the object body and one copy per version")

	ts.ResetState(t)

	after, err := s3.PayloadPathsForTest()
	require.NoError(t, err)
	assert.Empty(t, after,
		"the object bytes must not outlive the metadata that describes them: after a reset no "+
			"request can reach them and nothing else ever frees them (#902)")

	// Writing again over the wire has to work, and the bytes have to read back.
	recordVersionedPutObjects(t, ts, "reset-bytes", 1)
	status, _, body := resetDoRequest(t, http.MethodGet, ts.URL+"/reset-bytes/obj", resetS3Host, nil)
	require.Equal(t, http.StatusOK, status, "GetObject after a reset: %s", body)
	assert.Equal(t, "body-0", string(body),
		"emptying the filesystem must leave it writable and readable, which RemoveAll(\"/\") does not")
}

// resetS3ServerOnFS starts a single-plugin S3 server over fs and returns both the
// server and the plugin, so a test can write over the wire and then reset the very
// plugin that served the write.
//
// The wiring mirrors newS3TestServerWithFS rather than sharing it, because a reset
// assertion needs the plugin itself and that helper returns only the server.
func resetS3ServerOnFS(t *testing.T, fs afero.Fs) (*emulator.Server, *emulator.S3Plugin) {
	t.Helper()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(resetFrozenClock)

	p := &emulator.S3Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      fs,
		},
	}))

	registry := emulator.NewPluginRegistry()
	registry.Register(p)

	srv := emulator.NewServer(
		*emulator.DefaultConfig(),
		registry,
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"}),
		state,
		tc,
		logger,
		emulator.ServerOptions{Costs: emulator.NewCostController(emulator.CostConfig{Enabled: true})},
	)
	return srv, p
}

// TestPluginReset_AnInjectedFilesystemIsNotEmptied pins the one exception to #902: a
// filesystem handed to the plugin as Options["filesystem"] belongs to the caller, and
// only the caller knows whether the files under it are safe to remove — it may be an
// OsFs rooted at a directory holding fixtures the test means to reuse. Nothing can
// inject one over HTTP, so this is reachable only by an in-process embedder.
//
// The bytes are read back through the caller's own afero reference on purpose: the
// claim under test is that substrate did not touch a filesystem it does not own, which
// is a statement about the filesystem rather than about S3's own read path.
func TestPluginReset_AnInjectedFilesystemIsNotEmptied(t *testing.T) {
	fs := afero.NewMemMapFs()
	srv, p := resetS3ServerOnFS(t, fs)
	require.False(t, p.OwnsFilesystemForTest(),
		"a plugin handed a filesystem does not own it")

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/injected", nil, nil).Code,
		"CreateBucket")
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/injected/obj", []byte("payload"), nil).Code, "PutObject")

	before, err := p.PayloadPathsForTest()
	require.NoError(t, err)
	require.Equal(t, []string{"/injected/obj"}, before)

	require.NoError(t, p.ResetForRun(t.Context()))

	after, err := p.PayloadPathsForTest()
	require.NoError(t, err)
	assert.Equal(t, before, after, "a reset must not empty a filesystem the caller injected")

	got, err := afero.ReadFile(fs, "/injected/obj")
	require.NoError(t, err, "the caller's own reference must still read the file")
	assert.Equal(t, "payload", string(got))
}

// resetLambdaHost is the Lambda endpoint the poller recording uses.
const resetLambdaHost = "lambda.us-east-1.amazonaws.com"

// TestPluginReset_StateResetStopsEventSourceMappingPollers is the behavioral test for
// the first half of #903. CreateEventSourceMapping starts a goroutine that polls SQS
// once a wall-clock second and invokes the mapping's function with whatever it
// receives, and DeleteEventSourceMapping is the only call that closes its stop
// channel.
//
// A reset deletes the mapping record, so that call then answers 404 — asserted below —
// and the goroutine becomes unreachable through the API while still running. Both the
// queue URL and the function ARN are derived from names, so the next test case to use
// the same names has its messages received and deleted, and its function invoked, by a
// poller it never created: exactly the cross-case interference ResetState exists to
// remove.
//
// The assertion is a poller count rather than an absence of polls because a poll
// happens only on a one-second wall-clock tick, and no test here may depend on real
// elapsed time.
func TestPluginReset_StateResetStopsEventSourceMappingPollers(t *testing.T) {
	ts := startFrozenServer(t)
	lambda := resetLambdaPluginOf(t, ts)
	require.Zero(t, lambda.ESMPollerCountForTest(), "no poller runs before a mapping is created")

	body, err := json.Marshal(map[string]any{
		"FunctionName":   "reset-poller-fn",
		"EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:reset-poller-queue",
	})
	require.NoError(t, err)

	status, _, got := resetDoRequest(t, http.MethodPost,
		ts.URL+"/2015-03-31/event-source-mappings", resetLambdaHost, body)
	require.Equal(t, http.StatusCreated, status, "CreateEventSourceMapping: %s", got)

	var created struct {
		UUID string `json:"UUID"`
	}
	require.NoError(t, json.Unmarshal(got, &created))
	require.NotEmpty(t, created.UUID)
	require.Equal(t, 1, lambda.ESMPollerCountForTest(),
		"an enabled SQS mapping starts a poller")

	ts.ResetState(t)

	assert.Zero(t, lambda.ESMPollerCountForTest(),
		"a poller must not outlive the mapping record: it polls the queue the next run recreates "+
			"under the same name and invokes that run's function (#903)")

	status, _, got = resetDoRequest(t, http.MethodDelete,
		ts.URL+"/2015-03-31/event-source-mappings/"+created.UUID, resetLambdaHost, nil)
	assert.Equal(t, http.StatusNotFound, status,
		"the record a reset deletes is the one DeleteEventSourceMapping looks up, so after a reset "+
			"no API call could have stopped the poller: %s", got)
}

// TestPluginReset_DrainPoolLeavesTheExecutorUsable pins the second half of #903's
// Lambda story: a reset drops every warm container, because the pool is keyed by
// function ARN alone and a function recreated under the same name would otherwise run
// the previous run's code — but it must not use StopAll, which closes the channel the
// eviction loop selects on and so leaves every container started afterwards warm until
// Shutdown however long it sits idle.
//
// No container is injected, so nothing here shells out to Docker; the docker-gated
// tests in lambda_exec_docker_test.go cover the stop path itself.
func TestPluginReset_DrainPoolLeavesTheExecutorUsable(t *testing.T) {
	t.Parallel()
	e := emulator.NewLambdaExecutor(
		emulator.LambdaExecCfg{ReplayMode: "live", WarmPoolTTL: time.Hour},
		emulator.NewDefaultLogger(slog.LevelError, false),
	)
	t.Cleanup(e.StopAll)

	require.False(t, e.EvictionStoppedForTest(), "a fresh executor evicts idle containers")

	e.DrainPool()
	assert.False(t, e.EvictionStoppedForTest(),
		"a state reset must leave idle eviction running; the executor is reused for the next run")

	e.StopAll()
	assert.True(t, e.EvictionStoppedForTest(),
		"a shutdown ends it — which is why a reset cannot call StopAll")
}

// TestPluginReset_RDSResetIsSafeWithoutAContainer covers the RDS half of #903 at the
// plugin boundary, in the two configurations reachable without Docker: the default,
// where the plugin holds no executor at all, and an executor holding no container.
//
// The container path needs a Postgres container to stop, so it is asserted in
// rds_exec_test.go, where the existing tests are skipped when Docker is absent.
func TestPluginReset_RDSResetIsSafeWithoutAContainer(t *testing.T) {
	t.Parallel()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)

	stub := &emulator.RDSPlugin{}
	require.NoError(t, stub.Initialize(t.Context(), emulator.PluginConfig{
		State:  emulator.NewMemoryStateManager(),
		Logger: logger,
	}))
	require.NoError(t, stub.ResetForRun(t.Context()),
		"the default RDS plugin holds no executor, so a reset has nothing to stop")

	withExec := &emulator.RDSPlugin{}
	require.NoError(t, withExec.Initialize(t.Context(), emulator.PluginConfig{
		State:   emulator.NewMemoryStateManager(),
		Logger:  logger,
		Options: map[string]any{"rds_executor": emulator.NewRDSExecutor(logger)},
	}))
	require.NoError(t, withExec.ResetForRun(t.Context()),
		"an executor holding no container must reset cleanly too")
}
