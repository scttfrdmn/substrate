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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for CodePipeline's two
// records (#756), and the published form of their dates (#1338).
//
// CodePipelineState and CodePipelineExecution declare `json:"accountID"` and `json:"region"`, neither
// under omitempty, so a record that exists holds both and no absence below is vacuous. The pipeline's
// responses have always been built member by member; the execution was answered whole by
// GetPipelineExecution, and answers emulator/codepipeline_wire.go's projection now.

// codepipelineWireClock is the instant every fixture below runs at, on a frozen clock.
var codepipelineWireClock = time.Unix(1700000000, 0).UTC()

// codepipelineWireRendered is codepipelineWireClock as every published CodePipeline date renders it.
const codepipelineWireRendered = "1700000000.000"

// codepipelineWireAccount and codepipelineWireRegion scope every state key the plugin writes.
const (
	codepipelineWireAccount = "123456789012"
	codepipelineWireRegion  = "us-east-1"
)

// codepipelineBookkeepingMembers are the members CodePipeline's records declare and no CodePipeline
// shape publishes.
var codepipelineBookkeepingMembers = []string{"accountID", "region"}

// setupCodePipelineWirePlugin returns the CodePipeline plugin, a request context and the state manager
// behind it. Freeze then SetTime, in the order TimeController.Freeze documents.
func setupCodePipelineWirePlugin(t *testing.T) (*emulator.CodePipelinePlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(codepipelineWireClock)
	tc.Freeze()
	tc.SetTime(codepipelineWireClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.CodePipelinePlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.CodePipelinePlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: codepipelineWireAccount,
		Region:    codepipelineWireRegion,
		RequestID: "req-codepipeline-wire",
		IDs:       emulator.NewIDMint("req-codepipeline-wire"),
	}, state
}

// codepipelineWire issues one operation and returns the raw response body, failing on anything but
// 200.
func codepipelineWire(t *testing.T, p *emulator.CodePipelinePlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, codepipelineRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// codepipelineWireRequireScoped requires that the stored record at key carries both scope members.
func codepipelineWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "codepipeline", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+codepipelineWireAccount+`"`, string(record["accountID"]), "%s must persist accountID", key)
	require.JSONEq(t, `"`+codepipelineWireRegion+`"`, string(record["region"]), "%s must persist region", key)
}

// codepipelineWireCase is one operation; a non-nil held reuses a response the test already has.
type codepipelineWireCase struct {
	op     string
	body   map[string]any
	held   []byte
	anchor string
}

// codepipelineWireRun drives each case as a subtest: the presence anchor first, then the walk.
func codepipelineWireRun(t *testing.T, p *emulator.CodePipelinePlugin, ctx *emulator.RequestContext, cases []codepipelineWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = codepipelineWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, codepipelineBookkeepingMembers)
		})
	}
}

// codepipelineWirePipeline is the pipeline every test below creates.
func codepipelineWirePipeline(name string) map[string]any {
	return map[string]any{"pipeline": map[string]any{
		"name":    name,
		"roleArn": "arn:aws:iam::" + codepipelineWireAccount + ":role/codepipeline-wire",
		"stages": []map[string]any{
			{"name": "Source", "actions": []map[string]any{}},
			{"name": "Build", "actions": []map[string]any{}},
		},
	}}
}

func TestCodePipelineWire_PipelineResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodePipelineWirePlugin(t)

	const name = "wire-pipeline"
	created := codepipelineWire(t, p, ctx, "CreatePipeline", codepipelineWirePipeline(name))
	codepipelineWireRequireScoped(t, state, "pipeline:"+codepipelineWireAccount+"/"+codepipelineWireRegion+"/"+name)

	ref := map[string]any{"name": name}
	got := codepipelineWire(t, p, ctx, "GetPipeline", ref)
	listed := codepipelineWire(t, p, ctx, "ListPipelines", nil)
	stateBody := codepipelineWire(t, p, ctx, "GetPipelineState", ref)

	codepipelineWireRun(t, p, ctx, []codepipelineWireCase{
		{op: "CreatePipeline", held: created, anchor: `"name":"` + name + `"`},
		{op: "GetPipeline", held: got, anchor: `"name":"` + name + `"`},
		{op: "ListPipelines", held: listed, anchor: `"name":"` + name + `"`},
		{op: "GetPipelineState", held: stateBody, anchor: `"pipelineName":"` + name + `"`},
		{op: "UpdatePipeline", body: codepipelineWirePipeline(name), anchor: `"version":2`},
		// Last: it removes the record every case above reads.
		{op: "DeletePipeline", body: ref, anchor: "{}"},
	})

	// After the walk, so a regression is reported as a bookkeeping leak first. API_GetPipeline
	// publishes metadata's created and updated as numbers, and so do PipelineSummary and
	// GetPipelineState's response (#1338).
	for op, body := range map[string][]byte{"CreatePipeline": created, "GetPipeline": got, "ListPipelines": listed, "GetPipelineState": stateBody} {
		for _, member := range []string{"created", "updated"} {
			require.Containsf(t, string(body), `"`+member+`":`+codepipelineWireRendered,
				"%s must answer %s as epoch seconds: %s", op, member, body)
			require.NotContainsf(t, string(body), `"`+member+`":"`,
				"%s answered %s as a string, which awsJson1_1 does not publish: %s", op, member, body)
		}
	}
}

func TestCodePipelineWire_ExecutionResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodePipelineWirePlugin(t)

	const name = "wire-executed"
	codepipelineWire(t, p, ctx, "CreatePipeline", codepipelineWirePipeline(name))

	started := codepipelineWire(t, p, ctx, "StartPipelineExecution", map[string]any{"name": name})
	var start struct {
		PipelineExecutionID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(started, &start), "decode StartPipelineExecution: %s", started)
	require.NotEmpty(t, start.PipelineExecutionID, "StartPipelineExecution must report an execution id")
	codepipelineWireRequireScoped(t, state, "execution:"+codepipelineWireAccount+"/"+codepipelineWireRegion+"/"+start.PipelineExecutionID)

	got := codepipelineWire(t, p, ctx, "GetPipelineExecution", map[string]any{
		"pipelineName": name, "pipelineExecutionId": start.PipelineExecutionID,
	})

	codepipelineWireRun(t, p, ctx, []codepipelineWireCase{
		{op: "StartPipelineExecution", held: started, anchor: `"pipelineExecutionId":"` + start.PipelineExecutionID + `"`},
		{op: "GetPipelineExecution", held: got, anchor: `"pipelineExecutionId":"` + start.PipelineExecutionID + `"`},
	})

	// After the walk. API_PipelineExecution publishes no date member at all, so the record's startTime
	// is dropped rather than converted.
	var out struct {
		PipelineExecution map[string]json.RawMessage `json:"pipelineExecution"`
	}
	require.NoError(t, json.Unmarshal(got, &out), "decode GetPipelineExecution: %s", got)
	require.NotContains(t, slices.Sorted(maps.Keys(out.PipelineExecution)), "startTime",
		"API_PipelineExecution publishes no startTime: %s", got)
}
