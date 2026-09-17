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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// resetFrozenClock is the instant every recording in this file runs at. Any fixed
// time works; a round value makes a failure message readable.
var resetFrozenClock = time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

// startFrozenServer starts a recording test server whose simulated clock does not
// advance, so a minted "<clock>-<counter>" identifier varies only in its counter.
func startFrozenServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.SetTime(resetFrozenClock)
	ts.SetScale(0)
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
// [emulator.ResettablePlugin] — the shape 64 of the 67 registered plugins take,
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

	assert.Equal(t, []string{"omics", "s3", "sesv2"}, got,
		"the plugins holding mutable state outside the StateManager: S3's version-ID counter, "+
			"SES v2's message counter and HealthOmics' random source (#886)")
}
