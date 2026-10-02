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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for Step Functions'
// three records (#756).
//
// Every one of the plugin's eighteen routed operations answers through statesJSONResponse with
// either a hand-built map[string]interface{} or one of the two named projections, smToMap and
// execToMap; the three list operations each declare their own entry struct carrying only
// published members, and listTagsForResource decodes the stored record into a one-member struct
// and answers only its tags. No site marshals a record into a body — every json.Marshal of a
// StateMachineState, ExecutionState or ActivityState is in a save* helper writing to state.Put.
// So the projection already exists in code, and the eight baseline lines are AccountID, Region
// and EverTagged across the three records; what was missing was this file, the citation the
// projected inventory requires before it will accept a record.
//
// # Why this asserts absence rather than exact membership
//
// emulator/rds_wire_test.go, the strongest form in the suite, walks into the element that holds a
// record and requires its members to be *exactly* the published ones. That form is not taken
// here: smToMap answers seven of the fourteen members API_DescribeStateMachine publishes and
// describeActivity three of API_DescribeActivity's four, so an exact-membership expectation would
// pin that gap and have to be rewritten by the PR that closes it. The absence walk is indifferent
// to it, states the one thing the inventory needs, and is the form
// emulator/configservice_wire_test.go, emulator/glue_wire_test.go and
// emulator/redshift_wire_test.go already use.
//
// # Why six of the eighteen operations are driven and anchored differently
//
// UpdateStateMachine answers only updateDate, StopExecution only stopDate, and the two deletes and
// the two tag writes an empty object — none of the six reads a member off the record it addresses,
// so there is nothing a projection could leak and no record member to anchor on. They are still
// driven, because "every response that answers the record" is what the projected file asks for and
// a reader should not have to guess whether the list is complete; their anchor is the one member
// they do answer, or the empty document itself. A control run confirms the walk would catch a leak
// at each of the six if one were added, so the subtests are regression guards rather than
// decoration.

// sfnWireClock is the instant every fixture below starts the simulated clock at.
//
// A seeded baseline rather than setupStepFunctionsPlugin's time.Now(), so nothing here reads the
// wall clock. No assertion below equates a timestamp: TimeController.Now advances from its
// baseline by the wall time elapsed since it was set, so the only wall-clock-free thing to say
// about a projected date is that it is present — which the membership walk covers anyway.
var sfnWireClock = time.Unix(1700000000, 0).UTC()

// sfnWireAccount and sfnWireRegion scope every state key the plugin writes, and are the two
// segments every states ARN below is built from.
const (
	sfnWireAccount = "123456789012"
	sfnWireRegion  = "us-east-1"
)

// sfnBookkeepingMembers are the members the three Step Functions records declare and no Step
// Functions shape publishes, each listed once in the spelling the stored record uses.
//
// Compared case-insensitively by sfnWireAssertNoBookkeepingMember, because a leak could arrive
// under either spelling: StateMachineState, ExecutionState and ActivityState declare
// `json:"AccountID"` and `json:"Region"` — the Go identifier, capitalized — while every published
// member of every shape in this plugin is lowerCamelCase, so nothing in a correct body folds to
// any of these three. A fold is an equality rather than a substring test, so the account and the
// Region appearing *inside* an ARN value is not a collision.
var sfnBookkeepingMembers = []string{"AccountID", "Region", "ever_tagged"}

// setupStepFunctionsWirePlugin returns the Step Functions plugin, a request context and the state
// manager behind it.
//
// The state manager is handed back because half of what each test asserts is that the record keeps
// the members its responses drop — and a record is the only place any of the three can be read
// from, none having a published home to read it back through.
//
// Its own harness rather than setupStepFunctionsPlugin, which seeds the clock from time.Now() and
// returns no state handle. The ID mint is set because StartExecution and StartSyncExecution name
// an unnamed execution from it; every execution below is named explicitly, so the mint is there to
// keep an unnamed call from panicking rather than to be relied on.
func setupStepFunctionsWirePlugin(t *testing.T) (*emulator.StepFunctionsPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.StepFunctionsPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(sfnWireClock)},
	}), "emulator.StepFunctionsPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: sfnWireAccount,
		Region:    sfnWireRegion,
		RequestID: "req-sfn-wire",
		IDs:       emulator.NewIDMint("req-sfn-wire"),
	}, state
}

// sfnWireRaw issues one operation and returns the raw response body, failing the test on anything
// but 200.
//
// The raw bytes are the point of this file. Every other Step Functions test decodes into a Go map
// or struct, which is exactly the step that hides a member the caller never asked about.
func sfnWireRaw(t *testing.T, p *emulator.StepFunctionsPlugin, ctx *emulator.RequestContext, operation string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, sfnRequest(operation, body))
	require.NoError(t, err, "%s", operation)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", operation, resp.Body)
	return resp.Body
}

// sfnWireAssertNoBookkeepingMember fails if any member in the response document is named for a
// bookkeeping member, at any depth, reporting the path so a failure names the member rather than
// the service.
//
// The whole document rather than a record's subtree, which is also strictly stronger: a member
// rendered on the envelope rather than on the record would fail here and pass a subtree walk.
func sfnWireAssertNoBookkeepingMember(t *testing.T, site string, body []byte) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal(body, &doc), "%s answered undecodable JSON: %s", site, body)
	sfnWireWalkMembers(t, site, "$", doc)
}

// sfnWireWalkMembers recurses through a decoded JSON document asserting on every member name it
// meets.
func sfnWireWalkMembers(t *testing.T, site, path string, node any) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			child := path + "." + key
			for _, member := range sfnBookkeepingMembers {
				assert.Falsef(t, strings.EqualFold(key, member),
					"%s answered %s: %s is substrate's bookkeeping and no Step Functions shape publishes it",
					site, child, member)
			}
			sfnWireWalkMembers(t, site, child, value)
		}
	case []any:
		for i, value := range typed {
			sfnWireWalkMembers(t, site, fmt.Sprintf("%s[%d]", path, i), value)
		}
	}
}

// sfnWireRecord returns the record at key as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func sfnWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "states", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// sfnWireRequireScoped requires that the stored record carries both scope members.
//
// This is the presence anchor the absence assertions need on the record side: an assertion that a
// response omits a member its record never held would pass without testing anything. Neither
// member carries `,omitempty`, so a record found at all has both.
func sfnWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	record := sfnWireRecord(t, state, key)
	require.JSONEq(t, `"`+sfnWireAccount+`"`, string(record["AccountID"]),
		"%s must persist AccountID before an absence assertion on it means anything", key)
	require.JSONEq(t, `"`+sfnWireRegion+`"`, string(record["Region"]),
		"%s must persist Region before an absence assertion on it means anything", key)
}

// sfnWireTagAndRequireEverTagged tags the resource and requires the flag to be set on the record
// before any response is asserted to omit it.
//
// This is the step #1304 missed on EFS. ever_tagged is `,omitempty`, and neither
// createStateMachine nor createActivity sets it — only a tag write does, through
// mergeRecordStringMapTags and taggingStampRecordEverTagged — so an absence assertion against a
// never-tagged record would pass on a response that could not have carried the member.
func sfnWireTagAndRequireEverTagged(
	t *testing.T,
	p *emulator.StepFunctionsPlugin,
	ctx *emulator.RequestContext,
	state emulator.StateManager,
	arn, key string,
) {
	t.Helper()
	tagged := sfnWireRaw(t, p, ctx, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]string{{"key": "wire", "value": "pinned"}},
	})
	// TagResource's own response is one of the six that answer an empty object, so it is walked
	// here rather than as a case in the table: the tag write has to happen before the table runs.
	require.JSONEq(t, "{}", string(tagged), "TagResource answers an empty object")
	sfnWireAssertNoBookkeepingMember(t, "TagResource", tagged)
	record := sfnWireRecord(t, state, key)
	require.JSONEq(t, "true", string(record["ever_tagged"]),
		"%s must carry ever_tagged before an absence assertion on it means anything", key)
	sfnWireRequireScoped(t, state, key)
}

// sfnWireARN builds a states ARN of the given resource type, which is also where the account and
// the Region legitimately appear in every body below. That is why each assertion is on the member
// *name*: both values are published, inside an ARN, and only the names are not.
func sfnWireARN(resourceType, name string) string {
	return "arn:aws:states:" + sfnWireRegion + ":" + sfnWireAccount + ":" + resourceType + ":" + name
}

// sfnWireCase is one operation driven by one of the three tests below.
//
// A nil body reuses the create response the test already holds, rather than issuing the create a
// second time; name is spelled out rather than derived from operation because two cases per test
// drive the same operation down two rendering paths and a Go subtest name has to distinguish them.
type sfnWireCase struct {
	name      string
	operation string
	body      map[string]any
	anchor    string
}

// sfnWireRunCase drives one case as a subtest: the presence anchor first, so a body that did not
// render the record fails as a missing anchor rather than passing as an absence, then the walk.
func sfnWireRunCase(
	t *testing.T,
	p *emulator.StepFunctionsPlugin,
	ctx *emulator.RequestContext,
	tc sfnWireCase,
	created []byte,
) {
	t.Helper()
	t.Run(tc.name, func(t *testing.T) {
		body := created
		if tc.body != nil {
			body = sfnWireRaw(t, p, ctx, tc.operation, tc.body)
		}
		require.Containsf(t, string(body), tc.anchor,
			"presence anchor: %s has to render %s for an absence to mean anything", tc.operation, tc.anchor)
		sfnWireAssertNoBookkeepingMember(t, tc.operation, body)
	})
}

func TestStepFunctionsWire_StateMachineResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupStepFunctionsWirePlugin(t)

	const name = "WireSM"
	arn := sfnWireARN("stateMachine", name)
	role := "arn:aws:iam::" + sfnWireAccount + ":role/sfn-wire"

	created := sfnWireRaw(t, p, ctx, "CreateStateMachine", map[string]any{
		"name":       name,
		"definition": testSMDefinition,
		"roleArn":    role,
	})
	sfnWireTagAndRequireEverTagged(t, p, ctx, state, arn, "statemachine:"+sfnWireAccount+"/"+sfnWireRegion+"/"+name)

	require.Contains(t, string(created), arn,
		"CreateStateMachine must report the ARN, so the absence assertion is about the member name")

	for _, tc := range []sfnWireCase{
		{"CreateStateMachine", "CreateStateMachine", nil, arn},
		// The idempotent-repeat arm answers the stored record rather than the one just built, so
		// it is a second rendering path and not a repeat of the case above.
		{"CreateStateMachine_Repeat", "CreateStateMachine",
			map[string]any{"name": name, "definition": testSMDefinition, "roleArn": role}, arn},
		{"DescribeStateMachine", "DescribeStateMachine",
			map[string]any{"stateMachineArn": arn}, `"roleArn":"` + role + `"`},
		{"ListStateMachines", "ListStateMachines", map[string]any{}, `"name":"` + name + `"`},
		{"ListTagsForResource", "ListTagsForResource",
			map[string]any{"resourceArn": arn}, `{"key":"wire","value":"pinned"}`},
		// Reads nothing off the record: the one member is the clock's own reading.
		{"UpdateStateMachine", "UpdateStateMachine",
			map[string]any{"stateMachineArn": arn, "roleArn": role}, `"updateDate":`},
		// After ListTagsForResource, which reads the tag this removes.
		{"UntagResource", "UntagResource",
			map[string]any{"resourceArn": arn, "tagKeys": []string{"wire"}}, "{}"},
		// Last, and answers an empty object: it removes the record every case above reads.
		{"DeleteStateMachine", "DeleteStateMachine", map[string]any{"stateMachineArn": arn}, "{}"},
	} {
		sfnWireRunCase(t, p, ctx, tc, created)
	}
}

func TestStepFunctionsWire_ExecutionResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupStepFunctionsWirePlugin(t)

	const (
		standard = "WireStandardSM"
		express  = "WireExpressSM"
	)
	standardARN := sfnWireARN("stateMachine", standard)
	expressARN := sfnWireARN("stateMachine", express)
	for _, sm := range []struct {
		name    string
		smType  string
		machine string
	}{
		{standard, "STANDARD", standardARN},
		// StartSyncExecution refuses any other type (sfnStateMachineTypeNotSupported), so the one
		// operation that renders the most execution members needs its own machine.
		{express, "EXPRESS", expressARN},
	} {
		sfnWireRaw(t, p, ctx, "CreateStateMachine", map[string]any{
			"name":       sm.name,
			"definition": testSMDefinition,
			"roleArn":    "arn:aws:iam::" + sfnWireAccount + ":role/sfn-wire",
			"type":       sm.smType,
		})
	}

	// Two executions, so StopExecution's write of ABORTED does not disturb the arms that read a
	// SUCCEEDED one. Both are named rather than minted, so no assertion depends on the mint.
	started := sfnWireRaw(t, p, ctx, "StartExecution", map[string]any{
		"stateMachineArn": standardARN,
		"name":            "wire-exec",
		"input":           `{"seeded":true}`,
	})
	stopped := sfnWireRaw(t, p, ctx, "StartExecution", map[string]any{
		"stateMachineArn": standardARN,
		"name":            "wire-exec-stopped",
	})
	require.Contains(t, string(stopped), sfnWireARN("execution", standard+":wire-exec-stopped"))

	execKey := "execution:" + sfnWireAccount + "/" + sfnWireRegion + "/" + standard + "/wire-exec"
	sfnWireRequireScoped(t, state, execKey)

	execARN := sfnWireARN("execution", standard+":wire-exec")
	require.Contains(t, string(started), execARN,
		"StartExecution must report the ARN, so the absence assertion is about the member name")

	for _, tc := range []sfnWireCase{
		{"StartExecution", "StartExecution", nil, execARN},
		{"DescribeExecution", "DescribeExecution",
			map[string]any{"executionArn": execARN}, `"status":"SUCCEEDED"`},
		{"ListExecutions", "ListExecutions",
			map[string]any{"stateMachineArn": standardARN}, `"name":"wire-exec"`},
		{"GetExecutionHistory", "GetExecutionHistory",
			map[string]any{"executionArn": execARN}, `"type":"ExecutionStarted"`},
		{"StartSyncExecution", "StartSyncExecution", map[string]any{
			"stateMachineArn": expressARN,
			"name":            "wire-sync",
			"input":           `{"seeded":true}`,
		}, sfnWireARN("express", express+":wire-sync")},
		// Reads nothing off the record: the one member is the clock's own reading.
		{"StopExecution", "StopExecution", map[string]any{
			"executionArn": sfnWireARN("execution", standard+":wire-exec-stopped"),
		}, `"stopDate":`},
	} {
		sfnWireRunCase(t, p, ctx, tc, started)
	}
}

func TestStepFunctionsWire_ActivityResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupStepFunctionsWirePlugin(t)

	const name = "WireActivity"
	arn := sfnWireARN("activity", name)

	created := sfnWireRaw(t, p, ctx, "CreateActivity", map[string]any{"name": name})
	sfnWireTagAndRequireEverTagged(t, p, ctx, state, arn, "activity:"+sfnWireAccount+"/"+sfnWireRegion+"/"+name)

	require.Contains(t, string(created), arn,
		"CreateActivity must report the ARN, so the absence assertion is about the member name")

	for _, tc := range []sfnWireCase{
		{"CreateActivity", "CreateActivity", nil, arn},
		// The idempotency check is on the name alone, so a repeat answers the stored record — a
		// second rendering path rather than a repeat of the case above.
		{"CreateActivity_Repeat", "CreateActivity", map[string]any{"name": name}, arn},
		{"DescribeActivity", "DescribeActivity",
			map[string]any{"activityArn": arn}, `"name":"` + name + `"`},
		{"ListActivities", "ListActivities", map[string]any{}, `"activityArn":"` + arn + `"`},
		{"ListTagsForResource", "ListTagsForResource",
			map[string]any{"resourceArn": arn}, `{"key":"wire","value":"pinned"}`},
		// After ListTagsForResource, which reads the tag this removes.
		{"UntagResource", "UntagResource",
			map[string]any{"resourceArn": arn, "tagKeys": []string{"wire"}}, "{}"},
		// Last, and answers an empty object: it removes the record every case above reads.
		{"DeleteActivity", "DeleteActivity", map[string]any{"activityArn": arn}, "{}"},
	} {
		sfnWireRunCase(t, p, ctx, tc, created)
	}
}
