package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for EMR Serverless's two
// records (#756).
//
// EMRServerlessApp and EMRServerlessJobRun declare `json:"accountID"` and `json:"region"`, neither
// under omitempty, so a record that exists holds both and no absence below is vacuous.
// GetApplication and GetJobRun used to answer those records whole (#1199 recorded it); they answer
// emulator/emrserverless_wire.go's projections now. The other five routed operations answer maps of
// published members and are driven anyway, so the list of seven reads as complete.

// emrServerlessWireClock is the instant every fixture below starts the simulated clock at. No
// assertion equates a timestamp; this file only walks member names.
var emrServerlessWireClock = time.Unix(1700000000, 0).UTC()

// emrServerlessWireAccount and emrServerlessWireRegion scope every state key the plugin writes.
const (
	emrServerlessWireAccount = "123456789012"
	emrServerlessWireRegion  = "us-east-1"
)

// emrServerlessBookkeepingMembers are the members EMR Serverless's records declare and no EMR
// Serverless shape publishes.
var emrServerlessBookkeepingMembers = []string{"accountID", "region"}

// setupEMRServerlessWirePlugin returns the EMR Serverless plugin, a request context and the state
// manager behind it.
func setupEMRServerlessWirePlugin(t *testing.T) (*emulator.EMRServerlessPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.EMRServerlessPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(emrServerlessWireClock)},
	}), "emulator.EMRServerlessPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: emrServerlessWireAccount,
		Region:    emrServerlessWireRegion,
		RequestID: "req-emrserverless-wire",
		IDs:       emulator.NewIDMint("req-emrserverless-wire"),
	}, state
}

// emrServerlessWire issues one REST request and returns the raw response body, failing on anything
// but 200.
func emrServerlessWire(t *testing.T, p *emulator.EMRServerlessPlugin, ctx *emulator.RequestContext, method, path string, body map[string]any) []byte {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err, "marshal %s %s", method, path)
	}
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:    "emrserverless",
		HTTPMethod: method,
		Path:       path,
		Body:       raw,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Params:     map[string]string{},
	})
	require.NoError(t, err, "%s %s", method, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, resp.Body)
	return resp.Body
}

// emrServerlessWireRequireScoped requires that the stored record at key carries both scope members.
func emrServerlessWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "emrserverless", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+emrServerlessWireAccount+`"`, string(record["accountID"]), "%s must persist accountID", key)
	require.JSONEq(t, `"`+emrServerlessWireRegion+`"`, string(record["region"]), "%s must persist region", key)
}

func TestEMRServerlessWire_ResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEMRServerlessWirePlugin(t)

	created := emrServerlessWire(t, p, ctx, "POST", "/applications", map[string]any{
		"name": "wire-app", "type": "SPARK", "releaseLabel": "emr-7.0.0",
	})
	var app struct {
		ApplicationID string `json:"applicationId"`
	}
	require.NoError(t, json.Unmarshal(created, &app), "decode CreateApplication: %s", created)
	require.NotEmpty(t, app.ApplicationID, "CreateApplication must report an application id")
	emrServerlessWireRequireScoped(t, state, "app:"+emrServerlessWireAccount+"/"+emrServerlessWireRegion+"/"+app.ApplicationID)

	appPath := "/applications/" + app.ApplicationID
	started := emrServerlessWire(t, p, ctx, "POST", appPath+"/jobruns", map[string]any{"name": "wire-run"})
	var run struct {
		JobRunID string `json:"jobRunId"`
	}
	require.NoError(t, json.Unmarshal(started, &run), "decode StartJobRun: %s", started)
	require.NotEmpty(t, run.JobRunID, "StartJobRun must report a job run id")
	emrServerlessWireRequireScoped(t, state,
		"jobrun:"+emrServerlessWireAccount+"/"+emrServerlessWireRegion+"/"+app.ApplicationID+"/"+run.JobRunID)

	runPath := appPath + "/jobruns/" + run.JobRunID
	for _, tc := range []struct {
		op, method, path, anchor string
		held                     []byte
	}{
		{op: "CreateApplication", held: created, anchor: `"applicationId":"` + app.ApplicationID + `"`},
		{op: "GetApplication", method: "GET", path: appPath, anchor: `"releaseLabel":"emr-7.0.0"`},
		{op: "StartJobRun", held: started, anchor: `"jobRunId":"` + run.JobRunID + `"`},
		{op: "GetJobRun", method: "GET", path: runPath, anchor: `"jobRunId":"` + run.JobRunID + `"`},
		{op: "ListJobRuns", method: "GET", path: appPath + "/jobruns", anchor: `"id":"` + run.JobRunID + `"`},
		{op: "CancelJobRun", method: "DELETE", path: runPath, anchor: `"jobRunId":"` + run.JobRunID + `"`},
		// Last: it removes the record the cases above read.
		{op: "DeleteApplication", method: "DELETE", path: appPath, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = emrServerlessWire(t, p, ctx, tc.method, tc.path, nil)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, emrServerlessBookkeepingMembers)
		})
	}
}
