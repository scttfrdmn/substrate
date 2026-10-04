package emulator_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for CodeDeploy's three
// records (#756).
//
// Six baseline lines, two per record: CodeDeployApp, CodeDeployGroup and CodeDeployDeployment each
// declare `json:"accountID"` and `json:"region"`. Neither carries `,omitempty` on any of the three, so
// no record can lose a member by holding its zero value — which is why there is no counterpart here
// to the ever_tagged setup emulator/stepfunctions_wire_test.go needs. CodeDeploy routes none of the
// three published tag operations and no record declares EverTagged, so #1304's vacuity trap does not
// arise.
//
// CodeDeploy is the one service in this family where *every* record leaked, and the fix had to land
// before this file could say anything. getApplication answered the persisted CodeDeployApp under
// `application`, getDeploymentGroup the CodeDeployGroup under `deploymentGroupInfo`, and getDeployment
// the CodeDeployDeployment under `deploymentInfo`; emulator/codedeploy_wire.go's three projections are
// what they answer now, and #1327 shipped them with #1207, whose date conversion could not reach
// either dated site while the record was being marshaled whole. The other six routed operations build
// a map of one or two published members and have always rendered no record at all — but they are
// driven below anyway, for the reason the next section but one gives.
//
// # Why this asserts absence rather than exact membership
//
// emulator/rds_wire_test.go, the strongest form in the suite, walks into the element that holds a
// record and requires its members to be *exactly* the published ones. That form is not taken here:
// GetApplication answers four of API_ApplicationInfo's six members, GetDeploymentGroup four of
// API_GetDeploymentGroup's twenty-three and GetDeployment six of API_GetDeployment's thirty-one, so an
// exact-membership expectation would pin those gaps and have to be rewritten by the PR that closes
// them (#1199). The absence walk is indifferent to the gaps, states the one thing the projected
// inventory needs, and is the form emulator/configservice_wire_test.go,
// emulator/redshift_wire_test.go, emulator/stepfunctions_wire_test.go and
// emulator/backup_wire_test.go already use.
//
// # Why the operations that never leaked are driven anyway
//
// Six of the nine answer a map built member by member — an id, a name list, an empty object, an empty
// hooksNotCleanedUp — so there is nothing a projection could leak at any of them. They are driven
// because "every response that answers the record" is what the projected file asks for, and a reader
// should not have to work out whether the list of nine is complete or a sample. A control run confirms
// the walk would catch a member added at each, so the subtests are regression guards rather than
// decoration: the maps are hand-built, and hand-built is exactly what a later edit can add a field to.

// codedeployWireClock is the instant every fixture below starts the simulated clock at.
//
// A seeded baseline rather than setupCodeDeployPlugin's time.Now(), so nothing here reads the wall
// clock. No assertion below equates a timestamp: TimeController.Now advances from its baseline by the
// wall time elapsed since it was set, and pinning the *rendering* of a CodeDeploy date is
// emulator/codedeploy_dates_test.go's job (#1207), which freezes the clock for exactly that reason.
// This file only walks member names.
var codedeployWireClock = time.Unix(1700000000, 0).UTC()

// codedeployWireAccount and codedeployWireRegion scope every state key the plugin writes.
const (
	codedeployWireAccount = "123456789012"
	codedeployWireRegion  = "us-east-1"
)

// codedeployBookkeepingMembers are the members the three CodeDeploy records declare and no CodeDeploy
// shape publishes, each listed once in the spelling the stored record uses.
//
// Compared case-insensitively by codedeployWireAssertNoBookkeepingMember, because a leak could arrive
// under either spelling: the records declare `json:"accountID"` and `json:"region"` — CodeDeploy's
// own members are lowerCamelCase, so these were tagged to match and only the ID's capitalization
// gives them away — while a projection written in the house style of the other services might answer
// `AccountID`. A fold catches both. It is an equality rather than a substring test, so the account and
// the Region appearing *inside* an ARN value is not a collision; that is also why each assertion is on
// the member name and never on the body as a whole.
var codedeployBookkeepingMembers = []string{"accountID", "region"}

// setupCodeDeployWirePlugin returns the CodeDeploy plugin, a request context and the state manager
// behind it.
//
// The state manager is handed back because half of what each test asserts is that the record keeps the
// members its responses drop — and a record is the only place either can be read from, neither having
// a published home to read it back through.
//
// Its own harness rather than setupCodeDeployPlugin, which seeds the clock from time.Now(), returns no
// state handle and mints no IDs. The ID mint is set because the three creates each draw from it.
func setupCodeDeployWirePlugin(t *testing.T) (*emulator.CodeDeployPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.CodeDeployPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(codedeployWireClock)},
	}), "emulator.CodeDeployPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: codedeployWireAccount,
		Region:    codedeployWireRegion,
		RequestID: "req-codedeploy-wire",
		IDs:       emulator.NewIDMint("req-codedeploy-wire"),
	}, state
}

// codedeployWireRaw issues one operation and returns the raw response body, failing the test on
// anything but 200.
//
// The raw bytes are the point of this file. Every other CodeDeploy test decodes into a Go struct,
// which is exactly the step that hides a member the caller never asked about — and in this service's
// case the existing tests decode a struct of published members, so they would have passed on a record
// answered whole, which is what happened for as long as all three were.
func codedeployWireRaw(t *testing.T, p *emulator.CodeDeployPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, codedeployRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// codedeployWireAssertNoBookkeepingMember fails if any member in the response document is named for a
// bookkeeping member, at any depth, reporting the path so a failure names the member rather than the
// service.
//
// The whole document rather than a record's subtree, which is also strictly stronger: a member
// rendered on the envelope rather than on the record would fail here and pass a subtree walk. That
// matters for CodeDeploy specifically, because all three gets nest the record's published members
// under an envelope member — `application`, `deploymentGroupInfo`, `deploymentInfo` — while the creates
// answer theirs flat, so "the record's subtree" is not one place.
func codedeployWireAssertNoBookkeepingMember(t *testing.T, op string, body []byte) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal(body, &doc), "%s answered undecodable JSON: %s", op, body)
	codedeployWireWalkMembers(t, op, "$", doc)
}

// codedeployWireWalkMembers recurses through a decoded JSON document asserting on every member name it
// meets.
func codedeployWireWalkMembers(t *testing.T, op, path string, node any) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			child := path + "." + key
			for _, member := range codedeployBookkeepingMembers {
				assert.Falsef(t, strings.EqualFold(key, member),
					"%s answered %s: %s is substrate's bookkeeping and no CodeDeploy shape publishes it",
					op, child, member)
			}
			codedeployWireWalkMembers(t, op, child, value)
		}
	case []any:
		for i, value := range typed {
			codedeployWireWalkMembers(t, op, fmt.Sprintf("%s[%d]", path, i), value)
		}
	}
}

// codedeployWireRequireScoped requires that the stored record at key carries both scope members.
//
// This is the presence anchor the absence assertions need on the record side: an assertion that a
// response omits a member its record never held would pass without testing anything. The record is
// read as raw JSON rather than through its Go type, so a member with no published home can be observed
// without a struct deciding which members exist. Neither member carries `,omitempty` on any of the
// three records, so a record found at all has both.
func codedeployWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "codedeploy", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+codedeployWireAccount+`"`, string(record["accountID"]),
		"%s must persist accountID before an absence assertion on it means anything", key)
	require.JSONEq(t, `"`+codedeployWireRegion+`"`, string(record["region"]),
		"%s must persist region before an absence assertion on it means anything", key)
}

// codedeployWireCase is one operation driven by one of the three tests below.
//
// An empty body map with reuse set reuses a response the test already holds, rather than issuing the
// create a second time.
type codedeployWireCase struct {
	op     string
	body   map[string]any
	held   []byte
	anchor string
}

// codedeployWireRunCase drives one case as a subtest: the presence anchor first, so a body that did
// not render the record fails as a missing anchor rather than passing as an absence, then the walk.
func codedeployWireRunCase(t *testing.T, p *emulator.CodeDeployPlugin, ctx *emulator.RequestContext, tc codedeployWireCase) {
	t.Helper()
	t.Run(tc.op, func(t *testing.T) {
		body := tc.held
		if body == nil {
			body = codedeployWireRaw(t, p, ctx, tc.op, tc.body)
		}
		require.Containsf(t, string(body), tc.anchor,
			"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
		codedeployWireAssertNoBookkeepingMember(t, tc.op, body)
	})
}

func TestCodeDeployWire_ApplicationResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodeDeployWirePlugin(t)

	const name = "wire-app"
	created := codedeployWireRaw(t, p, ctx, "CreateApplication", map[string]any{
		"applicationName": name,
		"computePlatform": "Server",
	})

	var app struct {
		ApplicationID string `json:"applicationId"`
	}
	require.NoError(t, json.Unmarshal(created, &app), "decode CreateApplication: %s", created)
	require.NotEmpty(t, app.ApplicationID, "CreateApplication must report an application id")

	codedeployWireRequireScoped(t, state, "app:"+codedeployWireAccount+"/"+codedeployWireRegion+"/"+name)

	for _, tc := range []codedeployWireCase{
		{op: "CreateApplication", held: created, anchor: `"applicationId":"` + app.ApplicationID + `"`},
		{op: "GetApplication", body: map[string]any{"applicationName": name},
			anchor: `"applicationName":"` + name + `"`},
		// Answers names only, so the anchor is the name rather than a member of the record — which is
		// also the one place a leak could not hide, the element being a bare string.
		{op: "ListApplications", anchor: `"` + name + `"`},
	} {
		codedeployWireRunCase(t, p, ctx, tc)
	}

	// Last: it removes the record every case above reads. API_DeleteApplication answers "an HTTP 200
	// response with an empty HTTP body" (#1198), so there is no document to walk, and no member can
	// leak into one.
	deleted := codedeployWireRaw(t, p, ctx, "DeleteApplication", map[string]any{"applicationName": name})
	require.Empty(t, deleted, "DeleteApplication answers an empty body")
}

func TestCodeDeployWire_DeploymentGroupResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodeDeployWirePlugin(t)

	// CreateDeploymentGroup verifies the application exists, so the application comes first. Its own
	// responses are the application test's subject; here it is setup.
	const app = "wire-group-app"
	codedeployWireRaw(t, p, ctx, "CreateApplication", map[string]any{"applicationName": app})

	const name = "wire-group"
	created := codedeployWireRaw(t, p, ctx, "CreateDeploymentGroup", map[string]any{
		"applicationName":     app,
		"deploymentGroupName": name,
		"serviceRoleArn":      "arn:aws:iam::" + codedeployWireAccount + ":role/codedeploy-wire",
	})

	var group struct {
		DeploymentGroupID string `json:"deploymentGroupId"`
	}
	require.NoError(t, json.Unmarshal(created, &group), "decode CreateDeploymentGroup: %s", created)
	require.NotEmpty(t, group.DeploymentGroupID, "CreateDeploymentGroup must report a deployment group id")

	codedeployWireRequireScoped(t, state,
		"group:"+codedeployWireAccount+"/"+codedeployWireRegion+"/"+app+"/"+name)

	for _, tc := range []codedeployWireCase{
		{op: "CreateDeploymentGroup", held: created, anchor: `"deploymentGroupId":"` + group.DeploymentGroupID + `"`},
		{op: "GetDeploymentGroup", body: map[string]any{"applicationName": app, "deploymentGroupName": name},
			anchor: `"deploymentGroupName":"` + name + `"`},
		// Last, and answers the published hooksNotCleanedUp as an empty array: it removes the record
		// every case above reads.
		{op: "DeleteDeploymentGroup", body: map[string]any{"applicationName": app, "deploymentGroupName": name},
			anchor: `"hooksNotCleanedUp":[]`},
	} {
		codedeployWireRunCase(t, p, ctx, tc)
	}
}

func TestCodeDeployWire_DeploymentResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodeDeployWirePlugin(t)

	// CreateDeployment verifies both the application and the group, so both come first. There is no
	// DeleteDeployment to drive: CodeDeploy publishes none, and a deployment is not a deletable
	// resource.
	const app, group = "wire-deployment-app", "wire-deployment-group"
	codedeployWireRaw(t, p, ctx, "CreateApplication", map[string]any{"applicationName": app})
	codedeployWireRaw(t, p, ctx, "CreateDeploymentGroup", map[string]any{
		"applicationName":     app,
		"deploymentGroupName": group,
		"serviceRoleArn":      "arn:aws:iam::" + codedeployWireAccount + ":role/codedeploy-wire",
	})

	created := codedeployWireRaw(t, p, ctx, "CreateDeployment", map[string]any{
		"applicationName":     app,
		"deploymentGroupName": group,
	})

	var deployment struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(created, &deployment), "decode CreateDeployment: %s", created)
	require.NotEmpty(t, deployment.DeploymentID, "CreateDeployment must report a deployment id")

	codedeployWireRequireScoped(t, state,
		"deployment:"+codedeployWireAccount+"/"+codedeployWireRegion+"/"+deployment.DeploymentID)

	for _, tc := range []codedeployWireCase{
		{op: "CreateDeployment", held: created, anchor: `"deploymentId":"` + deployment.DeploymentID + `"`},
		{op: "GetDeployment", body: map[string]any{"deploymentId": deployment.DeploymentID},
			anchor: `"deploymentId":"` + deployment.DeploymentID + `"`},
	} {
		codedeployWireRunCase(t, p, ctx, tc)
	}
}
