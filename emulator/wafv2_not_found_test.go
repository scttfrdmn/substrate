package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// newWAFv2TestServer starts a server with only the WAFv2 plugin registered.
//
// The rest of this plugin's tests call HandleRequest directly, which returns an *AWSError whose
// HTTPStatus a test can read but which nothing turns into a response line. That is why #1098's statuses
// could drift: the field was asserted, the wire was not. These assertions go through the server so that
// what they check is the status a caller actually receives.
func newWAFv2TestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Now())

	p := &emulator.WAFv2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)

	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// wafv2WireCall posts one WAFv2 operation and returns the status and the raw body.
func wafv2WireCall(t *testing.T, ts *httptest.Server, op string, body map[string]any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = "wafv2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSWAF_20190729."+op)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, raw
}

// TestWAFv2_NonexistentItemAnswersTheStatusItsPagesPublish is #1098's WAFv2 half, asserted over the
// wire for the reason [newWAFv2TestServer] gives.
//
// All three sites answered 404. Every WAFv2 page that lists `WAFNonexistentItemException` publishes it
// at **400** — `API_GetIPSet`'s Errors section among them — and no WAFv2 page publishes a 404 at all.
func TestWAFv2_NonexistentItemAnswersTheStatusItsPagesPublish(t *testing.T) {
	ts := newWAFv2TestServer(t)

	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
		want string
	}{
		{
			name: "no web ACL is associated with the resource",
			op:   "GetWebACLForResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/lb/1",
			},
			want: "No WebACL is associated with resource",
		},
		{
			name: "no web ACL has that Id",
			op:   "GetWebACL",
			body: map[string]any{"Name": "absent", "Scope": "REGIONAL", "Id": "no-such-id"},
			want: "Web ACL with ID no-such-id does not exist.",
		},
		{
			name: "no IP set has that Id",
			op:   "GetIPSet",
			body: map[string]any{"Name": "absent", "Scope": "REGIONAL", "Id": "no-such-id"},
			want: "IP set with ID no-such-id does not exist.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := wafv2WireCall(t, ts, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, status,
				"WAFv2 publishes WAFNonexistentItemException at 400, not 404")
			assert.Equal(t, "WAFNonexistentItemException", jsonErrorType(t, raw))
			assert.Contains(t, string(raw), tc.want, "the message names which resource was not found")
		})
	}
}

// TestWAFv2_UnknownOperationKeepsIts404 is the 404 that stays, and the reason #1098's acceptance
// criteria were wrong about one line: the assertion at wafv2_plugin_test.go's default-arm test pins
// `UnknownOperationException`, which the JSON protocol's own Common Errors page publishes at 404. It is
// not part of WAFv2's vocabulary and did not move with the three above.
func TestWAFv2_UnknownOperationKeepsIts404(t *testing.T) {
	ts := newWAFv2TestServer(t)

	status, raw := wafv2WireCall(t, ts, "TagResource", map[string]any{})
	assert.Equal(t, http.StatusNotFound, status,
		"an operation the plugin does not route is the protocol's 404, not a WAFv2 refusal")
	assert.Equal(t, "UnknownOperationException", jsonErrorType(t, raw))
}
