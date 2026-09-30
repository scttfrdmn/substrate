package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// RestAPIState declares three members AWS publishes nowhere — AccountID, Region and
// EverTagged (#756) — and emulator/apigateway_wire.go's restAPIOut carries none of them.
// apigateway_wire_test.go's agwNoInternalFields covers the first two on every operation
// that answers a REST API, and it covers them non-vacuously: neither carries omitempty,
// so an unprojected record reports both unconditionally.
//
// EverTagged is the one that needs its own test. It is tagged `ever_tagged,omitempty`
// (#938), so while the flag is false the member is absent from the body whatever the
// projection does, and every absence assertion passes on a response that could not have
// carried it. That is the vacuous-assertion trap #1304 hit on EFS, and the fix is the
// same: make the flag true first, prove it is true, then assert.
//
// # Why the flag is set through state rather than through TagResources
//
// RestAPIState.EverTagged is set at exactly one place — TaggingPlugin's apigatewayNamespace
// merge arm — and that arm cannot currently reach an API a caller created. The tagging
// resolver builds the state key from the ARN's account field, and an API Gateway REST API
// ARN is `arn:aws:apigateway:{region}::/restapis/{id}`, whose account segment is empty by
// specification, so it looks up `api:/us-east-1/{id}` while the plugin wrote
// `api:123456789012/us-east-1/{id}`. TagResources answers InvalidParameterException.
// That is #1307, not #756.
//
// So this test writes the flag onto the record the plugin itself stored, which is what the
// merge arm would do if it could find it. What is under test here is the projection, and
// the projection cannot tell how the flag got set — which is the point: a record whose flag
// is true by any route must still answer a body without the member.
//
// Three of the four operations that answer a REST API, not four: CreateRestApi answers
// before anything can have tagged the API, so its record's flag is necessarily false and
// the site cannot carry the member. TestAPIGatewayWire_RestAPI covers CreateRestApi for
// AccountID and Region, which carry no omitempty and so need no such setup.
func TestAPIGatewayWire_RestAPIOmitsEverTaggedOnceItIsSet(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := &emulator.APIGatewayPlugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Now())},
	}), "APIGatewayPlugin.Initialize")
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "r1"}

	id := agwAPI(t, p, ctx, "flagged")

	// The key apigwAPIKey builds: "api:" + account + "/" + region + "/" + id. Written out
	// rather than called because it is unexported; a change to either side fails the Get
	// below rather than silently seeding nothing.
	const namespace = "apigateway"
	key := "api:123456789012/us-east-1/" + id

	goCtx := context.Background()
	raw, err := state.Get(goCtx, namespace, key)
	require.NoError(t, err, "the plugin's own record must be at %s/%s", namespace, key)

	var api emulator.RestAPIState
	require.NoError(t, json.Unmarshal(raw, &api), "decode RestAPIState: %s", raw)
	require.False(t, api.EverTagged, "the flag starts false — CreateRestApi with tags does not set it")
	api.EverTagged = true
	updated, err := json.Marshal(api)
	require.NoError(t, err, "marshal RestAPIState")
	require.NoError(t, state.Put(goCtx, namespace, key, updated), "seed EverTagged")

	// The anchor. A record that does not round-trip through the state encoding with the flag
	// set makes every assertion below vacuous, so the encoding is read back and asserted
	// before any response is inspected. It doubles as the ecr_wire.go invariant: the
	// projection changes the response and leaves the record alone.
	reread, err := state.Get(goCtx, namespace, key)
	require.NoError(t, err, "re-read seeded record")
	require.True(t, bytes.Contains(reread, []byte(`"ever_tagged":true`)),
		"the anchor: the record must carry ever_tagged=true, or nothing below is testing anything: %s", reread)

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{"GetRestApi", "GET", "/restapis/" + id, nil},
		{"GetRestApis", "GET", "/restapis", nil},
		{"UpdateRestApi", "PATCH", "/restapis/" + id, map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, body := agwWire(t, p, ctx, tc.method, tc.path, tc.body)
			require.Contains(t, string(body), `"`+id+`"`,
				"%s must answer the API whose flag is set: %s", tc.name, body)
			agwNoInternalFields(t, tc.name, body)
		})
	}

	// And the record still has it, so the projection removed the member from the response
	// without touching what a replay reads back.
	after, err := state.Get(goCtx, namespace, key)
	require.NoError(t, err, "re-read after the responses")
	require.True(t, bytes.Contains(after, []byte(`"ever_tagged":true`)),
		"the projection must leave the record's own encoding alone: %s", after)
}
