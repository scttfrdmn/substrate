package emulator_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The tests in this file are the README's "Deterministic reproducibility" and
// "Time-travel debugging" snippets, run. Each README line appears here unchanged apart
// from error checks and the assertions after it, and createBuckets stands in for "your
// code under test". Change them together: a README snippet that no longer compiles
// against the API is what #1604 found.

// createBuckets is the README's stand-in for the code under test: two S3 buckets and an
// object, sent to the test server over HTTP as any SDK would.
func createBuckets(t *testing.T, endpoint string) {
	t.Helper()
	for _, call := range []struct{ method, path, body string }{
		{http.MethodPut, "/readme-logs", ""},
		{http.MethodPut, "/readme-data", ""},
		{http.MethodPut, "/readme-data/hello.txt", "hello"},
	} {
		req, err := http.NewRequestWithContext(t.Context(), call.method, endpoint+call.path,
			strings.NewReader(call.body))
		require.NoError(t, err)
		req.Host = "s3.us-east-1.amazonaws.com"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s", call.method, call.path)
	}
}

// TestREADME_DeterministicReplay is README section 1.
func TestREADME_DeterministicReplay(t *testing.T) {
	ts := emulator.StartTestServer(t) // records request and response bodies by default
	createBuckets(t, ts.URL)          // your code under test, pointed at ts.URL

	// Replay the recorded run into a fresh sandbox, under the recorded request IDs and
	// clock, and fail the test on any difference from what the live run answered.
	results := ts.AssertReplay(t)

	assert.Equal(t, 3, results.TotalEvents)
	assert.Empty(t, results.Differences)
}

// TestREADME_TimeTravel is README section 2.
func TestREADME_TimeTravel(t *testing.T) {
	ts := emulator.StartTestServer(t)
	createBuckets(t, ts.URL)
	ctx := context.Background()

	sb := ts.ReplaySandbox(t) // a replay of the server's stream, apart from its live state
	events, err := ts.Store().GetStream(ctx, emulator.DefaultStreamID)
	require.NoError(t, err)

	// The state before the last request ran: the event's Sequence names the point.
	at, err := sb.StateAt(ctx, emulator.DefaultStreamID, events[len(events)-1].Sequence)
	require.NoError(t, err)
	// at.State is the state snapshot; at.Differences is where replaying to it diverged.
	assert.Equal(t, 2, at.EventsApplied)
	assert.Empty(t, at.Differences)
	assert.Contains(t, string(at.State), "readme-data")

	// Or drive the replay engine: replay the stream, then walk back through it.
	engine := sb.Engine()
	results, err := engine.Replay(ctx, emulator.DefaultStreamID)
	require.NoError(t, err)
	ev, err := engine.StepBackward(ctx) // back one event; ev is the next to run
	require.NoError(t, err)
	err = engine.JumpToEvent(ctx, 1) // a stream position: before the second event
	require.NoError(t, err)
	s3State, err := engine.InspectState(ctx, "s3") // one namespace's keys at this point
	require.NoError(t, err)

	assert.Empty(t, results.Differences)
	assert.Equal(t, events[len(events)-1].Sequence, ev.Sequence)
	var keys []string
	for k := range s3State {
		keys = append(keys, k)
	}
	assert.NotEmpty(t, keys, "the first bucket exists before the second event")
	for _, k := range keys {
		assert.NotContains(t, k, "readme-data", "the second bucket does not exist yet")
	}
}
