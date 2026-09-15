package emulator_test

// A success body's RequestId used to come from the wall clock and nowhere else.
//
// generateRequestID is "req-" + time.Now().UnixNano(), 66 non-test sites render it
// into a response, and [emulator.Event] did not carry it — so a recorded run could
// not be reproduced even in principle, and a replay rendered event.ID instead: a
// third shape, neither the recorded id nor a freshly minted one (#866).
//
// The tests split along what each can observe. A replay returns counters and
// differences to its caller, not response bodies — deep-comparing bodies is #817's
// remaining work — so the byte-identity assertion is made by a plugin that keeps
// the bytes it rendered, dispatched once live and once by the replay. The
// real-service test then closes the gap that leaves: it proves the value the event
// now carries is the same value a shipped success body renders, over the wire,
// without any of the 66 sites being edited.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// requestIDEchoPlugin renders ctx.RequestID into a success body the way the 66 real
// sites do, and keeps every body it rendered.
//
// It exists because a replay hands its caller counters and differences, never a
// response body, so no assertion on the bytes a replayed handler produced is
// available from outside a plugin. Registering under a service name with no default
// plugin follows principalCapturePlugin, which captures the RequestContext for the
// same reason.
type requestIDEchoPlugin struct {
	bodies [][]byte
}

func (p *requestIDEchoPlugin) Name() string { return "dynamodb" }

func (p *requestIDEchoPlugin) Initialize(_ context.Context, _ emulator.PluginConfig) error {
	return nil
}

func (p *requestIDEchoPlugin) HandleRequest(ctx *emulator.RequestContext, _ *emulator.AWSRequest) (*emulator.AWSResponse, error) {
	body := []byte(fmt.Sprintf(`{"ResponseMetadata":{"RequestId":%q}}`, ctx.RequestID))
	p.bodies = append(p.bodies, body)

	return &emulator.AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
		Body:       body,
	}, nil
}

func (p *requestIDEchoPlugin) Shutdown(_ context.Context) error { return nil }

// newRequestIDEchoServer wires a server, its recording store and a replay engine
// over one set of components, with request bodies recorded — without them every
// event is skipped and a replay executes nothing (#833).
func newRequestIDEchoServer(t *testing.T) (*emulator.Server, *emulator.EventStore, *emulator.ReplayEngine, *requestIDEchoPlugin) {
	t.Helper()

	cfg := emulator.DefaultConfig()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled:       true,
		Backend:       "memory",
		IncludeBodies: true,
	}, emulator.WithTimeController(tc))

	echo := &requestIDEchoPlugin{}
	registry := emulator.NewPluginRegistry()
	registry.Register(echo)

	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)
	engine := emulator.NewReplayEngine(store, state, tc, registry, emulator.ReplayConfig{}, logger)

	return srv, store, engine, echo
}

// requestIDEchoCall issues one request to the echo plugin and returns its body.
func requestIDEchoCall(t *testing.T, srv *emulator.Server) []byte {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return raw
}

// TestReplayedSuccessBodyReproducesTheRecordedRequestID is the assertion #866 turns
// on: the bytes a replayed success body renders equal the bytes the recording
// rendered.
//
// It fails in a specific way before the fix rather than merely differing — the
// replayed body carries event.ID, i.e. "dynamodb-GetItem-1-<UnixNano>", so the
// assertion below that the replayed body does *not* contain the event id is what
// distinguishes the fix from a coincidence.
func TestReplayedSuccessBodyReproducesTheRecordedRequestID(t *testing.T) {
	srv, store, engine, echo := newRequestIDEchoServer(t)

	live := requestIDEchoCall(t, srv)

	events, err := store.GetStream(t.Context(), "default")
	require.NoError(t, err)
	require.Len(t, events, 1, "one call records one event")

	recorded := events[0]
	require.NotEmpty(t, recorded.RequestID, "the event must carry the id the request was served under")
	require.Contains(t, string(live), recorded.RequestID,
		"the recorded id must be the one the live body rendered")
	require.NotEqual(t, recorded.ID, recorded.RequestID,
		"an event id and a request id are different identifiers, minted by different code")

	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Equal(t, 1, results.SuccessEvents, "the event must be re-executed, not skipped")
	require.Zero(t, results.SkippedEvents)

	require.Len(t, echo.bodies, 2, "one live dispatch and one replayed dispatch")
	assert.True(t, bytes.Equal(echo.bodies[0], echo.bodies[1]),
		"a replayed success body must be byte-identical to the recording:\nlive:   %s\nreplay: %s",
		echo.bodies[0], echo.bodies[1])
	assert.NotContains(t, string(echo.bodies[1]), recorded.ID,
		"the replay must render the recorded request id, not the event id")
}

// TestReplayOfAStreamWithoutARecordedRequestIDFallsBackToTheEventID covers the
// streams that already exist on disk: recorded before [emulator.Event] had the
// field, so the value is gone and nothing can recover it.
//
// The fallback is the event id — what such a stream replayed to before — rather
// than an empty RequestId, so an older stream keeps replaying exactly as it did.
// The event is hand-built here because that is the only way to produce a stream the
// current recorder cannot write.
func TestReplayOfAStreamWithoutARecordedRequestIDFallsBackToTheEventID(t *testing.T) {
	_, store, engine, echo := newRequestIDEchoServer(t)

	event := &emulator.Event{
		StreamID:  "legacy",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Service:   "dynamodb",
		Operation: "GetItem",
		Request: &emulator.AWSRequest{
			Service:   "dynamodb",
			Operation: "GetItem",
			Body:      []byte(`{}`),
		},
	}
	require.NoError(t, store.RecordEvent(t.Context(), event))
	require.Empty(t, event.RequestID, "the point of the case is a stream with no recorded id")
	require.NotEmpty(t, event.ID, "the store mints an event id on record")

	results, err := engine.Replay(t.Context(), "legacy")
	require.NoError(t, err)
	require.Equal(t, 1, results.SuccessEvents)

	require.Len(t, echo.bodies, 1)
	assert.Contains(t, string(echo.bodies[0]), event.ID,
		"with no recorded request id the replay falls back to the event id")
}

// TestRecordedSQSSuccessBodyCarriesTheEventsRequestID proves the field is the value
// a *shipped* success body renders, which the echo plugin cannot: it is the reason
// none of the 66 sites needed editing.
//
// SQS is used because CreateQueue and SendMessage both render
// ResponseMetadata.RequestId from ctx.RequestID, and the assertion is on the raw
// recorded bytes rather than a decoded struct — a decoded ResponseMetadata would
// compare equal whether the element held the recorded id or nothing at all.
func TestRecordedSQSSuccessBodyCarriesTheEventsRequestID(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	form := url.Values{"Action": {"CreateQueue"}, "QueueName": {"request-id-queue"}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "sqs.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	live, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateQueue")

	events, err := ts.Store().GetStream(t.Context(), "default")
	require.NoError(t, err)
	require.Len(t, events, 1)

	recorded := events[0]
	require.NotEmpty(t, recorded.RequestID)
	require.NotNil(t, recorded.Response, "WithRecordedBodies records the response")

	want := "<RequestId>" + recorded.RequestID + "</RequestId>"
	assert.Contains(t, string(live), want,
		"the live body must render the id the event recorded")
	assert.Contains(t, string(recorded.Response.Body), want,
		"the recorded body must too, or the event describes a response nobody received")
}
