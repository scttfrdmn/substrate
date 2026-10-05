package emulator_test

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Helpers the #1196 progression tests share: each drives a service over a [emulator.TestServer], so
// the seed reaches the plugin through its control-plane endpoint exactly as a consumer's does, and the
// recorded stream a replay test walks holds the seed as an event (#1140).

// lifecycleServer starts a [emulator.TestServer] on a frozen clock, so a rendered date is the same on
// every run and on replay; no test here reads the wall clock.
func lifecycleServer(t *testing.T, opts ...emulator.TestServerOption) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, opts...)
	ts.FreezeTimeAt(time.Unix(1700000000, 0).UTC())
	return ts
}

// frozenLifecycleClock returns a [emulator.TimeController] frozen at the instant lifecycleServer uses,
// for the plugin-direct fault tests.
func frozenLifecycleClock() *emulator.TimeController {
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	return tc
}

// lifecycleSeed POSTs body to a progression's control-plane path and requires 200.
func lifecycleSeed(t *testing.T, ts *emulator.TestServer, path, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "seed %s %s: %s", path, body, raw)
}

// lifecycleSeedStatus POSTs body to a progression's control-plane path and returns the status, for
// the refusal cases.
func lifecycleSeedStatus(t *testing.T, ts *emulator.TestServer, path, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// lifecycleClear DELETEs a progression's control-plane path, with query appended, and requires 200.
func lifecycleClear(t *testing.T, ts *emulator.TestServer, path, query string) {
	t.Helper()
	target := ts.URL + path
	if query != "" {
		target += "?" + query
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, target, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "clear %s?%s", path, query)
}

// lifecycleCall sends one AWS request and returns its status and body.
func lifecycleCall(t *testing.T, ts *emulator.TestServer, method, path, host string, headers map[string]string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = host
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, raw
}

// lifecycleQuery sends one Query-protocol action to host and returns its status and body.
func lifecycleQuery(t *testing.T, ts *emulator.TestServer, host string, form url.Values) (int, []byte) {
	t.Helper()
	return lifecycleCall(t, ts, http.MethodPost, "/", host,
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
}

// lifecycleTarget sends one awsJson1_1 operation to host by its X-Amz-Target and returns its status
// and body.
func lifecycleTarget(t *testing.T, ts *emulator.TestServer, host, target, body string) (int, []byte) {
	t.Helper()
	return lifecycleCall(t, ts, http.MethodPost, "/", host, map[string]string{
		"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": target,
	}, []byte(body))
}

// lifecycleReplayIdentically replays the server's recorded stream with every seed re-applied in
// position and requires that every replayed response matches its recording — the property a seed
// exists for (#1196's "replays the same event log to prove the sequence reproduces").
func lifecycleReplayIdentically(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Positive(t, results.TotalEvents, "nothing was recorded")
	require.Zero(t, results.SkippedEvents, "every event, the seed included, is re-executed")
	require.Zero(t, results.FailedEvents)
	require.Empty(t, results.Differences, "the replay reproduces the recorded sequence: %s", replayDifferenceSummary(results))
}
