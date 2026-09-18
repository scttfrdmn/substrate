package emulator_test

// Step Functions definitions, and the code StartSyncExecution answers for a workflow type (#996).
//
// Two defects, and the second is what made the first hard to see.
//
// StartSyncExecution refused a STANDARD state machine with InvalidDefinition — a real Step Functions
// code, published at CreateStateMachine and UpdateStateMachine where a definition arrives in the
// request, but not on this page and not about this fact. API_StartSyncExecution publishes nine errors,
// all 400, and states the restriction outright: "StartSyncExecution is not available for STANDARD
// workflows." StateMachineTypeNotSupported is the code for it.
//
// It answered the same code a second time, for a stored definition it could not parse — and that path
// was live, because neither CreateStateMachine nor UpdateStateMachine checked the definition at all.
// Both stored whatever string arrived. So substrate could accept a definition, report 200, and then be
// unable to execute it, which it reported to the caller as the caller's fault.
//
// The CloudFormation deployer was a concrete producer of exactly that: DefinitionString is a
// CloudFormation string property, and marshaling a Go string to JSON quotes and escapes it, so a
// template's ASL document was stored as a JSON string literal rather than an object.
//
// Validating on the way in is what closes both. It also matches where AWS draws the line on this page:
// "Error codes are reserved for errors that prevent your execution from running, such as permissions
// errors, limit errors, or issues with your state machine code and configuration." AWS publishes no
// code for a stored definition it cannot read because AWS would never have stored one.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestSFNStartSyncExecution_AStandardWorkflowIsATypeNotSupported is the headline.
//
// The code is asserted on the raw body rather than through a decoded struct, and the old code is
// asserted absent: a consumer branching on InvalidDefinition here was told the ASL document was wrong
// when what was wrong was the workflow type, so the replacement has to be a replacement rather than an
// addition.
func TestSFNStartSyncExecution_AStandardWorkflowIsATypeNotSupported(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "batch", "STANDARD",
		sfnArnPassDefinition)

	status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "StartSyncExecution",
		map[string]any{"stateMachineArn": smARN, "input": "{}"})

	assert.Equal(t, "StateMachineTypeNotSupported", errCode, raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)
	assert.Contains(t, raw, "State machine type is not supported",
		"AWS's own message for this code")
	assert.NotContains(t, raw, "InvalidDefinition",
		"the code this operation does not publish must be gone, not merely joined")

	// The refusal did not start anything. ListExecutions is the reading that would show a record the
	// refusal left behind, which Describe on a name the test chose could not.
	listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
		map[string]any{"stateMachineArn": smARN})
	assert.NotContains(t, listed, ":express:", "a refused sync execution was recorded anyway")
}

// TestSFNStartSyncExecution_AnExpressWorkflowRuns is the other side of the type check: the operation
// still serves the type it is for, and reports a terminal status in the same response.
func TestSFNStartSyncExecution_AnExpressWorkflowRuns(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "fast", "EXPRESS",
		sfnArnPassDefinition)

	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartSyncExecution",
		map[string]any{"stateMachineArn": smARN, "input": "{}"})

	assert.Equal(t, "SUCCEEDED", sfnArnMember(t, raw, "status"), raw)
	assert.Contains(t, sfnArnMember(t, raw, "executionArn"), ":express:", raw)
}

// sfnBadDefinitions is the set of definitions substrate will not store, each named by the fault.
//
// The JSON-string case is first because it is not hypothetical: it is the shape the CloudFormation
// deployer produced for every template carrying a DefinitionString, which
// TestCFNStateMachine_ADefinitionStringIsStoredOnce covers from the other end.
//
//nolint:gochecknoglobals // table shared by the create and update tests
var sfnBadDefinitions = map[string]string{
	"a JSON string holding the document": `"{\"StartAt\":\"Start\"}"`,
	"not JSON at all":                    `StartAt: Start`,
	"truncated JSON":                     `{"StartAt":"Start","States":{`,
	"a JSON array":                       `[{"StartAt":"Start"}]`,
	"a JSON number":                      `42`,
	"a JSON null":                        `null`,
	"empty":                              ``,
	"whitespace only":                    `   `,
	"a member of the wrong type":         `{"StartAt":7}`,
}

// TestSFNCreateStateMachine_RefusesADefinitionItCouldNotReadBack checks the guard at the operation
// where the definition first arrives.
//
// The assertion that the state machine was not created matters as much as the code: a refusal that
// still stored the record would leave the defect in place behind a 400.
func TestSFNCreateStateMachine_RefusesADefinitionItCouldNotReadBack(t *testing.T) {
	ts := sfnArnServer(t)

	for label, definition := range sfnBadDefinitions {
		t.Run(label, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion,
				"CreateStateMachine", map[string]any{
					"name":       "refused",
					"definition": definition,
					"roleArn":    "arn:aws:iam::" + taggingTestAccount + ":role/StepFunctionsRole",
				})
			assert.Equal(t, "InvalidDefinition", errCode, raw)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, "The provided Amazon States Language definition is not valid",
				"AWS's own message for this code")

			listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListStateMachines",
				map[string]any{})
			assert.NotContains(t, listed, `"name":"refused"`, "a refused definition was stored anyway")
		})
	}
}

// TestSFNUpdateStateMachine_RefusesADefinitionItCouldNotReadBack checks the other writer.
//
// This is the worse of the two if it is missing: the caller's own 200 would be the last observation
// before a working state machine stopped being executable. So the test reads the stored definition back
// and asserts it did not move.
func TestSFNUpdateStateMachine_RefusesADefinitionItCouldNotReadBack(t *testing.T) {
	ts := sfnArnServer(t)

	original := sfnArnDefinition("original")
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD", original)

	for label, definition := range sfnBadDefinitions {
		t.Run(label, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion,
				"UpdateStateMachine", map[string]any{
					"stateMachineArn": smARN,
					"definition":      definition,
				})

			if definition == "" {
				// An empty definition is the one member value UpdateStateMachine is allowed to ignore:
				// API_UpdateStateMachine publishes definition as Required: No, so "absent" and "empty
				// string" are the same request on the wire and the update is a no-op, not a refusal.
				assert.Emptyf(t, errCode, "%s: %s", label, raw)
				assert.Equalf(t, http.StatusOK, status, "%s: %s", label, raw)
			} else {
				assert.Equalf(t, "InvalidDefinition", errCode, "%s: %s", label, raw)
				assert.Equalf(t, http.StatusBadRequest, status, "%s: %s", label, raw)
			}

			described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
				map[string]any{"stateMachineArn": smARN})
			assert.Equalf(t, original, sfnArnMember(t, described, "definition"),
				"%s: the stored definition moved", label)
		})
	}
}

// TestSFNDefinition_ASLConformanceIsNotChecked records the boundary of the guard, so a 200 here is not
// read as ASL approval.
//
// "{}" parses as an object and is therefore stored, although AWS refuses it: the Amazon States Language
// requires a state machine to carry a string field named StartAt and an object field named States, and
// substrate checks neither. What the guard is for is narrower — that substrate can read back what it
// stored — and that is the property the defect was about.
func TestSFNDefinition_ASLConformanceIsNotChecked(t *testing.T) {
	ts := sfnArnServer(t)

	for label, definition := range map[string]string{
		"an empty object":              `{}`,
		"no StartAt":                   `{"States":{"Start":{"Type":"Pass","End":true}}}`,
		"no States":                    `{"StartAt":"Start"}`,
		"StartAt naming no such state": `{"StartAt":"Nowhere","States":{"Start":{"Type":"Pass","End":true}}}`,
	} {
		t.Run(label, func(t *testing.T) {
			arn := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion,
				"lax-"+strings.ReplaceAll(label, " ", "-"), "STANDARD", definition)
			described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
				map[string]any{"stateMachineArn": arn})
			assert.Equal(t, definition, sfnArnMember(t, described, "definition"))
		})
	}
}

// TestCFNStateMachine_ADefinitionStringIsStoredOnce is the CloudFormation half.
//
// A template's DefinitionString was marshaled to JSON on the way into CreateStateMachine, which quoted
// and escaped a string that was already the document. The stored definition was then a JSON string
// literal — so every AWS::StepFunctions::StateMachine deployed from a DefinitionString held a definition
// substrate could not execute, and StartExecution reported the execution FAILED for a reason the
// template author could do nothing about.
//
// The read-backs go through the registry the deployer itself dispatches into, rather than over HTTP: the
// deploy in this test family is in-process, so a signed request would be reading a different world than
// the one the template built. Both are still the owning service's own operations, per #765 — no state is
// inspected directly.
func TestCFNStateMachine_ADefinitionStringIsStoredOnce(t *testing.T) {
	deployer, registry := sfnDefinitionDeployer(t)

	definition := `{"StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`
	tmpl := `{"Resources":{"SM":{"Type":"AWS::StepFunctions::StateMachine","Properties":{` +
		`"StateMachineName":"cfn-def-sm",` +
		`"DefinitionString":` + sfnJSONString(definition) + `,` +
		`"RoleArn":"arn:aws:iam::123456789012:role/svc"}}}}`

	result, err := deployer.Deploy(context.Background(), tmpl, "cfn-def-stack", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	smARN := "arn:aws:states:us-east-1:123456789012:stateMachine:cfn-def-sm"

	described := sfnDefinitionRoute(t, registry, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	assert.Equal(t, definition, described["definition"],
		"the deployed definition is the template's document, stored once")

	// And it executes, which is the consequence the double-encoding destroyed. States.Runtime is the
	// name the failure carried, so asserting the status is not FAILED is the assertion that matters.
	started := sfnDefinitionRoute(t, registry, "StartExecution",
		map[string]any{"stateMachineArn": smARN, "name": "from-cfn"})
	execArn, ok := started["executionArn"].(string)
	require.Truef(t, ok, "no executionArn in %v", started)

	execution := sfnDefinitionRoute(t, registry, "DescribeExecution",
		map[string]any{"executionArn": execArn})
	assert.Equal(t, "SUCCEEDED", execution["status"],
		"a CloudFormation-deployed definition could not be read back")
}

// sfnJSONString renders s as a JSON string literal, which is what a template author writes when they put
// an ASL document in a DefinitionString.
func sfnJSONString(s string) string {
	quoted, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(quoted)
}

// sfnDefinitionDeployer builds a deployer and hands back the registry it dispatches into, so a test can
// read a deployed resource through the owning service's own operation.
//
// newRefDeployer cannot serve here because it discards the registry, and the registry is the whole point:
// a CFN deploy and the Step Functions call that checks it have to be the same world.
func sfnDefinitionDeployer(t *testing.T) (*emulator.StackDeployer, *emulator.PluginRegistry) {
	t.Helper()
	ctx := context.Background()
	cfg := emulator.DefaultConfig()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	registry := emulator.NewPluginRegistry()
	require.NoError(t, emulator.RegisterDefaultPlugins(ctx, registry, state, tc, logger, store, nil))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})
	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), registry
}

// sfnDefinitionRoute calls one Step Functions operation through the registry and returns the decoded
// success body, failing the test on any refusal.
func sfnDefinitionRoute(t *testing.T, registry *emulator.PluginRegistry, op string,
	body map[string]any,
) map[string]any {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoErrorf(t, err, "marshal %s", op)

	resp, err := registry.RouteRequest(
		&emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: op},
		&emulator.AWSRequest{
			Service:   "states",
			Operation: op,
			Body:      data,
			Headers:   map[string]string{"x-amz-target": "AmazonStates." + op},
			Params:    map[string]string{},
		})
	require.NoErrorf(t, err, "%s", op)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)

	var out map[string]any
	require.NoErrorf(t, json.Unmarshal(resp.Body, &out), "%s: %s", op, resp.Body)
	return out
}
