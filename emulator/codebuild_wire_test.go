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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for CodeBuild's two records
// (#756), and the published form of their dates (#1338).
//
// CodeBuildProject and CodeBuildBuild declare `json:"accountID"` and `json:"region"`, neither under
// omitempty, so a record that exists holds both and no absence below is vacuous. Five sites used to
// answer those records whole; they answer emulator/codebuild_wire.go's projections now, and this file
// is what says so. The other two routed operations answer a name list and an empty object and are
// driven anyway, so the list of seven reads as complete rather than as a sample.

// codebuildWireClock is the instant every fixture below runs at, on a frozen clock, so a rendered
// date can be asserted exactly.
var codebuildWireClock = time.Unix(1700000000, 0).UTC()

// codebuildWireRendered is codebuildWireClock as every published CodeBuild date must render it.
const codebuildWireRendered = "1700000000.000"

// codebuildWireAccount and codebuildWireRegion scope every state key the plugin writes.
const (
	codebuildWireAccount = "123456789012"
	codebuildWireRegion  = "us-east-1"
)

// codebuildBookkeepingMembers are the members CodeBuild's records declare and no CodeBuild shape
// publishes. The fold in wireAssertNoMemberJSON covers any capitalization.
var codebuildBookkeepingMembers = []string{"accountID", "region"}

// setupCodeBuildWirePlugin returns the CodeBuild plugin, a request context and the state manager
// behind it. Freeze then SetTime, in the order TimeController.Freeze documents.
func setupCodeBuildWirePlugin(t *testing.T) (*emulator.CodeBuildPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(codebuildWireClock)
	tc.Freeze()
	tc.SetTime(codebuildWireClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.CodeBuildPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.CodeBuildPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: codebuildWireAccount,
		Region:    codebuildWireRegion,
		RequestID: "req-codebuild-wire",
		IDs:       emulator.NewIDMint("req-codebuild-wire"),
	}, state
}

// codebuildWire issues one operation and returns the raw response body, failing on anything but 200.
func codebuildWire(t *testing.T, p *emulator.CodeBuildPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, codebuildRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// codebuildWireRequireScoped requires that the stored record at key carries both scope members.
func codebuildWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "codebuild", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+codebuildWireAccount+`"`, string(record["accountID"]), "%s must persist accountID", key)
	require.JSONEq(t, `"`+codebuildWireRegion+`"`, string(record["region"]), "%s must persist region", key)
}

// codebuildWireRequireEpoch requires that body renders each member as the published epoch value and
// none of them as a string (#1338).
func codebuildWireRequireEpoch(t *testing.T, op string, body []byte, members ...string) {
	t.Helper()
	for _, member := range members {
		require.Containsf(t, string(body), `"`+member+`":`+codebuildWireRendered,
			"%s must answer %s as epoch seconds: %s", op, member, body)
		require.NotContainsf(t, string(body), `"`+member+`":"`,
			"%s answered %s as a string, which awsJson1_1 does not publish: %s", op, member, body)
	}
}

// codebuildWireCase is one operation; a non-nil held reuses a response the test already has.
type codebuildWireCase struct {
	op     string
	body   map[string]any
	held   []byte
	anchor string
}

// codebuildWireRun drives each case as a subtest: the presence anchor first, then the walk.
func codebuildWireRun(t *testing.T, p *emulator.CodeBuildPlugin, ctx *emulator.RequestContext, cases []codebuildWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = codebuildWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, codebuildBookkeepingMembers)
		})
	}
}

func TestCodeBuildWire_ProjectResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodeBuildWirePlugin(t)

	const name = "wire-project"
	created := codebuildWire(t, p, ctx, "CreateProject", map[string]any{
		"name":        name,
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": "version: 0.2"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"serviceRole": "arn:aws:iam::" + codebuildWireAccount + ":role/codebuild-wire",
	})
	codebuildWireRequireScoped(t, state, "project:"+codebuildWireAccount+"/"+codebuildWireRegion+"/"+name)

	got := codebuildWire(t, p, ctx, "BatchGetProjects", map[string]any{"names": []string{name}})
	updated := codebuildWire(t, p, ctx, "UpdateProject", map[string]any{"name": name, "description": "wire"})

	codebuildWireRun(t, p, ctx, []codebuildWireCase{
		{op: "CreateProject", held: created, anchor: `"name":"` + name + `"`},
		{op: "BatchGetProjects", held: got, anchor: `"name":"` + name + `"`},
		{op: "UpdateProject", held: updated, anchor: `"description":"wire"`},
		{op: "ListProjects", anchor: `"` + name + `"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteProject", body: map[string]any{"name": name}, anchor: "{}"},
	})

	// After the walk, so a regression is reported as a bookkeeping leak first.
	for op, body := range map[string][]byte{"CreateProject": created, "BatchGetProjects": got, "UpdateProject": updated} {
		codebuildWireRequireEpoch(t, op, body, "created", "lastModified")
	}
}

func TestCodeBuildWire_BuildResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodeBuildWirePlugin(t)

	const project = "wire-build-project"
	codebuildWire(t, p, ctx, "CreateProject", map[string]any{"name": project})

	started := codebuildWire(t, p, ctx, "StartBuild", map[string]any{"projectName": project})
	var start struct {
		Build struct {
			ID string `json:"id"`
		} `json:"build"`
	}
	require.NoError(t, json.Unmarshal(started, &start), "decode StartBuild: %s", started)
	require.NotEmpty(t, start.Build.ID, "StartBuild must report a build id")
	codebuildWireRequireScoped(t, state, "build:"+codebuildWireAccount+"/"+codebuildWireRegion+"/"+start.Build.ID)

	got := codebuildWire(t, p, ctx, "BatchGetBuilds", map[string]any{"ids": []string{start.Build.ID}})

	codebuildWireRun(t, p, ctx, []codebuildWireCase{
		{op: "StartBuild", held: started, anchor: `"id":"` + start.Build.ID + `"`},
		{op: "BatchGetBuilds", held: got, anchor: `"id":"` + start.Build.ID + `"`},
	})

	for op, body := range map[string][]byte{"StartBuild": started, "BatchGetBuilds": got} {
		codebuildWireRequireEpoch(t, op, body, "startTime", "endTime")
	}
}
