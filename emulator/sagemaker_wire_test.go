package emulator_test

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for SageMaker's two records
// (#756).
//
// SageMakerApp and SageMakerTrainingJob declare `json:"AccountID"` and `json:"Region"`, neither under
// omitempty, so a record that exists holds both and no absence below is vacuous. DescribeApp,
// ListApps and DescribeTrainingJob used to answer those records whole; they answer
// emulator/sagemaker_wire.go's projections now. The other seven routed operations answer maps of
// published members and are driven anyway, so the list of ten reads as complete.

// sagemakerWireClock is the instant every fixture below starts the simulated clock at. No assertion
// equates a timestamp; this file only walks member names.
var sagemakerWireClock = time.Unix(1700000000, 0).UTC()

// sagemakerWireAccount and sagemakerWireRegion scope every state key the plugin writes.
const (
	sagemakerWireAccount = "123456789012"
	sagemakerWireRegion  = "us-east-1"
)

// sagemakerBookkeepingMembers are the members SageMaker's records declare and no SageMaker shape
// publishes.
var sagemakerBookkeepingMembers = []string{"AccountID", "Region"}

// setupSageMakerWirePlugin returns the SageMaker plugin, a request context and the state manager
// behind it.
func setupSageMakerWirePlugin(t *testing.T) (*emulator.SageMakerPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.SageMakerPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(sagemakerWireClock)},
	}), "emulator.SageMakerPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: sagemakerWireAccount,
		Region:    sagemakerWireRegion,
		RequestID: "req-sagemaker-wire",
		IDs:       emulator.NewIDMint("req-sagemaker-wire"),
	}, state
}

// sagemakerWire issues one JSON-target operation and returns the raw response body, failing on
// anything but 200.
func sagemakerWire(t *testing.T, p *emulator.SageMakerPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "sagemaker",
		Operation: op,
		Path:      "/",
		Body:      raw,
		Headers:   map[string]string{"X-Amz-Target": "SageMaker." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// sagemakerWireRequireScoped requires that the stored record at key carries both scope members.
func sagemakerWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "sagemaker", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+sagemakerWireAccount+`"`, string(record["AccountID"]), "%s must persist AccountID", key)
	require.JSONEq(t, `"`+sagemakerWireRegion+`"`, string(record["Region"]), "%s must persist Region", key)
}

// sagemakerWireCase is one operation; a non-nil held reuses a response the test already has.
type sagemakerWireCase struct {
	op     string
	body   map[string]any
	held   []byte
	anchor string
}

// sagemakerWireRun drives each case as a subtest: the presence anchor first, then the walk.
func sagemakerWireRun(t *testing.T, p *emulator.SageMakerPlugin, ctx *emulator.RequestContext, cases []sagemakerWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = sagemakerWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, sagemakerBookkeepingMembers)
		})
	}
}

func TestSageMakerWire_AppResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSageMakerWirePlugin(t)

	app := map[string]any{"AppName": "wire-app", "AppType": "JupyterServer", "DomainId": "d-wire", "UserProfileName": "wire-user"}
	created := sagemakerWire(t, p, ctx, "CreateApp", app)
	sagemakerWireRequireScoped(t, state,
		"app:"+sagemakerWireAccount+"/"+sagemakerWireRegion+"/d-wire/wire-user/JupyterServer/wire-app")
	listed := sagemakerWire(t, p, ctx, "ListApps", nil)

	sagemakerWireRun(t, p, ctx, []sagemakerWireCase{
		{op: "CreateApp", held: created, anchor: `"AppArn"`},
		{op: "DescribeApp", body: app, anchor: `"AppName":"wire-app"`},
		{op: "ListApps", held: listed, anchor: `"AppName":"wire-app"`},
		{op: "ListDomains", anchor: `"Domains"`},
		{op: "CreatePresignedDomainUrl", body: map[string]any{"DomainId": "d-wire", "UserProfileName": "wire-user"}, anchor: `"AuthorizedUrl"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteApp", body: app, anchor: "{}"},
	})

	// After the walk. API_AppDetails, the element ListApps answers, publishes no AppArn — DescribeApp's
	// response does, which is why the two have different types.
	var out struct {
		Apps []map[string]json.RawMessage `json:"Apps"`
	}
	require.NoError(t, json.Unmarshal(listed, &out), "decode ListApps: %s", listed)
	require.Len(t, out.Apps, 1, "ListApps: %s", listed)
	require.NotContains(t, slices.Sorted(maps.Keys(out.Apps[0])), "AppArn", "API_AppDetails publishes no AppArn: %s", listed)
}

func TestSageMakerWire_TrainingJobResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSageMakerWirePlugin(t)

	const name = "wire-job"
	job := map[string]any{"TrainingJobName": name}
	created := sagemakerWire(t, p, ctx, "CreateTrainingJob", job)
	sagemakerWireRequireScoped(t, state, "trainingjob:"+sagemakerWireAccount+"/"+sagemakerWireRegion+"/"+name)

	sagemakerWireRun(t, p, ctx, []sagemakerWireCase{
		{op: "CreateTrainingJob", held: created, anchor: `"TrainingJobArn"`},
		{op: "DescribeTrainingJob", body: job, anchor: `"TrainingJobName":"` + name + `"`},
		{op: "ListTrainingJobs", anchor: `"TrainingJobName":"` + name + `"`},
		{op: "StopTrainingJob", body: job, anchor: "{}"},
	})
}
