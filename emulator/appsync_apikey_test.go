package emulator_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1122: CreateApiKey decoded no `expires`, set the expiry a year out, and so could not answer the
// bound its own page publishes.
//
// Every assertion below runs against a clock fixed at a known instant, because the published values
// are offsets from the creation time and an assertion against `time.Now()` would be both imprecise
// and wall-clock dependent.

// appsyncAPIKeyClock is the instant the api-key server's simulated clock is fixed at.
//
// Deliberately not on an hour boundary: the published representation is "seconds since the epoch,
// rounded down to the nearest hour", so a clock at :37:11 is what makes the rounding visible. An
// instant already on the hour would let an unrounded implementation pass.
var appsyncAPIKeyClock = time.Date(2026, 3, 4, 9, 37, 11, 0, time.UTC)

// newAppSyncClockedServer is newAppSyncTestServer with the clock fixed at appsyncAPIKeyClock.
func newAppSyncClockedServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(appsyncAPIKeyClock)
	logger := emulator.NewDefaultLogger(slog.LevelError, false)

	p := &emulator.AppSyncPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// appsyncAPIKeyAPI creates an API on a clocked server and returns both.
func appsyncAPIKeyAPI(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	ts := newAppSyncClockedServer(t)
	status, raw, body := appSyncWire(t, ts, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "key_api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusOK, status, string(raw))
	api, _ := body["graphqlApi"].(map[string]any)
	apiID, _ := api["apiId"].(string)
	require.NotEmpty(t, apiID)
	return ts, apiID
}

// appsyncAPIKeyHourOf truncates to the hour, mirroring what the page publishes so the expected value
// is computed the way the assertion reads rather than hard-coded.
func appsyncAPIKeyHourOf(ts int64) int64 { return ts - ts%3600 }

// TestAppSyncAPIKey_DefaultExpiryIsSevenDays is #1122's first criterion.
//
// API_CreateApiKey: "The default value for this parameter is 7 days from creation time." Substrate
// answered 365 days, so a consumer asserting that a fresh key expires within the week — which AWS's
// default makes true — saw a year.
func TestAppSyncAPIKey_DefaultExpiryIsSevenDays(t *testing.T) {
	ts, apiID := appsyncAPIKeyAPI(t)

	status, raw, body := appSyncWire(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/apikeys",
		map[string]any{"description": "default expiry"})
	require.Equal(t, http.StatusOK, status, string(raw))

	key, ok := body["apiKey"].(map[string]any)
	require.True(t, ok, "no apiKey object: %s", raw)

	created := appsyncAPIKeyClock.Unix()
	wantExpires := appsyncAPIKeyHourOf(created + 7*24*3600)
	assert.InDelta(t, float64(wantExpires), key["expires"], 0,
		"an omitted expires takes the published 7-day default, rounded down to the hour")
	assert.InDelta(t, float64(appsyncAPIKeyHourOf(wantExpires+60*24*3600)), key["deletes"], 0,
		"API_ApiKey publishes deletes, and says an expired key is kept for 60 days")
	assert.Equal(t, "default expiry", key["description"])
	assert.NotEmpty(t, key["id"])
}

// TestAppSyncAPIKey_ARequestedExpiryIsHonoured is #1122's second criterion: a caller that sends
// `expires` gets the expiry it asked for, rounded down to the hour.
//
// The member was decoded nowhere, so a 30-day request was answered with a 365-day key and told it
// succeeded — a silent wrong answer rather than a refusal.
func TestAppSyncAPIKey_ARequestedExpiryIsHonoured(t *testing.T) {
	ts, apiID := appsyncAPIKeyAPI(t)
	created := appsyncAPIKeyClock.Unix()

	for _, tc := range []struct {
		name string
		days int64
	}{
		{"one day, the low bound", 1},
		{"thirty days", 30},
		{"365 days, the high bound", 365},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asked := created + tc.days*24*3600
			status, raw, body := appSyncWire(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/apikeys",
				map[string]any{"expires": asked})
			require.Equal(t, http.StatusOK, status, string(raw))

			key, _ := body["apiKey"].(map[string]any)
			assert.InDelta(t, float64(appsyncAPIKeyHourOf(asked)), key["expires"], 0,
				"the expiry the caller asked for, as the page represents it")
		})
	}
}

// TestAppSyncAPIKey_AnExpiryOutOfBoundsIsRefused is #1122's third criterion, and the reason the other
// two matter: the refusal cannot fire against a value that is never read.
//
// ApiKeyValidityOutOfBoundsException/400: "The API key expiration must be set to a value between 1
// and 365 days from creation (for CreateApiKey) or from update (for UpdateApiKey)".
func TestAppSyncAPIKey_AnExpiryOutOfBoundsIsRefused(t *testing.T) {
	ts, apiID := appsyncAPIKeyAPI(t)
	created := appsyncAPIKeyClock.Unix()

	for _, tc := range []struct {
		name    string
		expires int64
	}{
		{"an hour out is under the one-day floor", created + 3600},
		{"a year and a day out is over the 365-day ceiling", created + 366*24*3600},
		{"an expiry in the past", created - 24*3600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw, body := appSyncWire(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/apikeys",
				map[string]any{"expires": tc.expires})
			assert.Equal(t, http.StatusBadRequest, status, string(raw))
			// "Code" is the member substrate's REST-JSON error writer fills, as appsync_paths_test.go
			// already records for this service.
			assert.Equal(t, "ApiKeyValidityOutOfBoundsException", body["Code"],
				"the code the page publishes for this constraint: %s", raw)
			assert.Contains(t, body["Message"], "between 1 and 365 days",
				"the message states the bound, which is what a caller has to be told")
		})
	}
}

// TestAppSyncAPIKey_ListAnswersThePublishedShape pins that the projection reaches ListApiKeys too, and
// that a key created before #1122 — one whose record carries no `deletes`, which is every record,
// since `deletes` is derived rather than persisted — still reports the published value.
func TestAppSyncAPIKey_ListAnswersThePublishedShape(t *testing.T) {
	ts, apiID := appsyncAPIKeyAPI(t)

	status, raw, _ := appSyncWire(t, ts, http.MethodPost, "/v1/apis/"+apiID+"/apikeys",
		map[string]any{"description": "listed"})
	require.Equal(t, http.StatusOK, status, string(raw))

	status, raw, body := appSyncWire(t, ts, http.MethodGet, "/v1/apis/"+apiID+"/apikeys", nil)
	require.Equal(t, http.StatusOK, status, string(raw))

	list, ok := body["apiKeys"].([]any)
	require.True(t, ok, "no apiKeys list: %s", raw)
	require.Len(t, list, 1)
	key, _ := list[0].(map[string]any)

	for _, member := range []string{"id", "description", "expires", "deletes"} {
		assert.Contains(t, key, member, "API_ApiKey publishes %s", member)
	}
	expires, _ := key["expires"].(float64)
	deletes, _ := key["deletes"].(float64)
	assert.InDelta(t, expires+60*24*3600, deletes, 0, "deletes is 60 days past expires")
}
