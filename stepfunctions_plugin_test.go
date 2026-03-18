package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func setupStepFunctionsPlugin(t *testing.T) (*substrate.StepFunctionsPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.StepFunctionsPlugin{}
	require.NoError(t, p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-1",
	}
}

func sfnRequest(operation string, body map[string]any) *substrate.AWSRequest {
	b, _ := json.Marshal(body)
	return &substrate.AWSRequest{
		Service:   "states",
		Operation: operation,
		Body:      b,
		Headers:   map[string]string{"x-amz-target": "AmazonStates." + operation},
		Params:    map[string]string{},
	}
}

func sfnBody(t *testing.T, resp *substrate.AWSResponse) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return out
}

const testSMDefinition = `{"Comment":"test","StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`

func TestStepFunctions_CreateAndDescribeStateMachine(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	// Create a state machine.
	createReq := sfnRequest("CreateStateMachine", map[string]any{
		"name":       "MyStateMachine",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
		"type":       "STANDARD",
	})
	createResp, err := p.HandleRequest(ctx, createReq)
	require.NoError(t, err)
	assert.Equal(t, 200, createResp.StatusCode)

	createBody := sfnBody(t, createResp)
	arn, ok := createBody["stateMachineArn"].(string)
	require.True(t, ok, "stateMachineArn should be a string")
	assert.Contains(t, arn, "arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine")
	assert.NotEmpty(t, createBody["creationDate"])

	// Describe the state machine by ARN.
	descReq := sfnRequest("DescribeStateMachine", map[string]any{
		"stateMachineArn": arn,
	})
	descResp, err := p.HandleRequest(ctx, descReq)
	require.NoError(t, err)
	assert.Equal(t, 200, descResp.StatusCode)

	descBody := sfnBody(t, descResp)
	assert.Equal(t, arn, descBody["stateMachineArn"])
	assert.Equal(t, "MyStateMachine", descBody["name"])
	assert.Equal(t, "ACTIVE", descBody["status"])
	assert.Equal(t, "STANDARD", descBody["type"])
	assert.Equal(t, testSMDefinition, descBody["definition"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/sfn-role", descBody["roleArn"])
}

func TestStepFunctions_StartAndDescribeExecution(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	// Create a state machine first.
	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:ExecSM"
	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "ExecSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	// Start an execution with a specific name.
	startReq := sfnRequest("StartExecution", map[string]any{
		"stateMachineArn": smArn,
		"name":            "my-exec-1",
		"input":           `{"key":"value"}`,
	})
	startResp, err := p.HandleRequest(ctx, startReq)
	require.NoError(t, err)
	assert.Equal(t, 200, startResp.StatusCode)

	startBody := sfnBody(t, startResp)
	execArn, ok := startBody["executionArn"].(string)
	require.True(t, ok)
	assert.Contains(t, execArn, "arn:aws:states:us-east-1:123456789012:execution:ExecSM:my-exec-1")
	assert.NotEmpty(t, startBody["startDate"])

	// Describe the execution — it should transition from RUNNING to SUCCEEDED.
	descExecReq := sfnRequest("DescribeExecution", map[string]any{
		"executionArn": execArn,
	})
	descExecResp, err := p.HandleRequest(ctx, descExecReq)
	require.NoError(t, err)
	assert.Equal(t, 200, descExecResp.StatusCode)

	descExecBody := sfnBody(t, descExecResp)
	assert.Equal(t, execArn, descExecBody["executionArn"])
	assert.Equal(t, "SUCCEEDED", descExecBody["status"])
	assert.NotEmpty(t, descExecBody["stopDate"])
	// Pass state passes input through; input was {"key":"value"}.
	assert.JSONEq(t, `{"key":"value"}`, descExecBody["output"].(string))
}

func TestStepFunctions_ListStateMachines(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	// Create two state machines.
	for _, name := range []string{"SM-Alpha", "SM-Beta"} {
		_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
			"name":       name,
			"definition": testSMDefinition,
			"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
		}))
		require.NoError(t, err)
	}

	// List them.
	listResp, err := p.HandleRequest(ctx, sfnRequest("ListStateMachines", map[string]any{}))
	require.NoError(t, err)
	assert.Equal(t, 200, listResp.StatusCode)

	listBody := sfnBody(t, listResp)
	machines, ok := listBody["stateMachines"].([]interface{})
	require.True(t, ok)
	assert.Len(t, machines, 2)

	// Collect names from response.
	names := make([]string, 0, len(machines))
	for _, m := range machines {
		entry := m.(map[string]interface{})
		names = append(names, entry["name"].(string))
	}
	assert.ElementsMatch(t, []string{"SM-Alpha", "SM-Beta"}, names)
}

func TestStepFunctions_ListExecutions(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:ListExecSM"
	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "ListExecSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	// Start two executions.
	for _, name := range []string{"exec-a", "exec-b"} {
		_, err := p.HandleRequest(ctx, sfnRequest("StartExecution", map[string]any{
			"stateMachineArn": smArn,
			"name":            name,
		}))
		require.NoError(t, err)
	}

	listResp, err := p.HandleRequest(ctx, sfnRequest("ListExecutions", map[string]any{
		"stateMachineArn": smArn,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, listResp.StatusCode)

	listBody := sfnBody(t, listResp)
	executions, ok := listBody["executions"].([]interface{})
	require.True(t, ok)
	assert.Len(t, executions, 2)

	names := make([]string, 0, len(executions))
	for _, e := range executions {
		entry := e.(map[string]interface{})
		names = append(names, entry["name"].(string))
	}
	assert.ElementsMatch(t, []string{"exec-a", "exec-b"}, names)
}

func TestStepFunctions_GetExecutionHistory(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:HistSM"
	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "HistSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	startResp, err := p.HandleRequest(ctx, sfnRequest("StartExecution", map[string]any{
		"stateMachineArn": smArn,
		"name":            "hist-exec-1",
	}))
	require.NoError(t, err)
	startBody := sfnBody(t, startResp)
	execArn := startBody["executionArn"].(string)

	histResp, err := p.HandleRequest(ctx, sfnRequest("GetExecutionHistory", map[string]any{
		"executionArn": execArn,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, histResp.StatusCode)

	histBody := sfnBody(t, histResp)
	events, ok := histBody["events"].([]interface{})
	require.True(t, ok)
	// Real execution history: ExecutionStarted + StateEntered + StateExited + ExecutionSucceeded.
	assert.GreaterOrEqual(t, len(events), 2)

	firstEvent := events[0].(map[string]interface{})
	assert.Equal(t, "ExecutionStarted", firstEvent["type"])

	lastEvent := events[len(events)-1].(map[string]interface{})
	assert.Equal(t, "ExecutionSucceeded", lastEvent["type"])
}

func TestStepFunctions_Activity(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	// Create an activity.
	createResp, err := p.HandleRequest(ctx, sfnRequest("CreateActivity", map[string]any{
		"name": "MyActivity",
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, createResp.StatusCode)

	createBody := sfnBody(t, createResp)
	actArn, ok := createBody["activityArn"].(string)
	require.True(t, ok)
	assert.Contains(t, actArn, "arn:aws:states:us-east-1:123456789012:activity:MyActivity")

	// Describe it.
	descResp, err := p.HandleRequest(ctx, sfnRequest("DescribeActivity", map[string]any{
		"activityArn": actArn,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, descResp.StatusCode)

	descBody := sfnBody(t, descResp)
	assert.Equal(t, actArn, descBody["activityArn"])
	assert.Equal(t, "MyActivity", descBody["name"])

	// List activities.
	listResp, err := p.HandleRequest(ctx, sfnRequest("ListActivities", map[string]any{}))
	require.NoError(t, err)
	listBody := sfnBody(t, listResp)
	activities, ok := listBody["activities"].([]interface{})
	require.True(t, ok)
	assert.Len(t, activities, 1)
	assert.Equal(t, "MyActivity", activities[0].(map[string]interface{})["name"])

	// Delete it.
	delResp, err := p.HandleRequest(ctx, sfnRequest("DeleteActivity", map[string]any{
		"activityArn": actArn,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, delResp.StatusCode)

	// List again — should be empty.
	listResp2, err := p.HandleRequest(ctx, sfnRequest("ListActivities", map[string]any{}))
	require.NoError(t, err)
	listBody2 := sfnBody(t, listResp2)
	activities2 := listBody2["activities"].([]interface{})
	assert.Len(t, activities2, 0)
}

func TestStepFunctions_Tags(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	// Create a state machine.
	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "TaggedSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:TaggedSM"

	// Tag the resource.
	tagResp, err := p.HandleRequest(ctx, sfnRequest("TagResource", map[string]any{
		"resourceArn": smArn,
		"tags": map[string]string{
			"env":  "test",
			"team": "platform",
		},
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, tagResp.StatusCode)

	// List tags.
	listTagsResp, err := p.HandleRequest(ctx, sfnRequest("ListTagsForResource", map[string]any{
		"resourceArn": smArn,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, listTagsResp.StatusCode)

	listTagsBody := sfnBody(t, listTagsResp)
	tags, ok := listTagsBody["tags"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag one key.
	_, err = p.HandleRequest(ctx, sfnRequest("UntagResource", map[string]any{
		"resourceArn": smArn,
		"tagKeys":     []string{"team"},
	}))
	require.NoError(t, err)

	// Verify the tag is gone.
	listTagsResp2, err := p.HandleRequest(ctx, sfnRequest("ListTagsForResource", map[string]any{
		"resourceArn": smArn,
	}))
	require.NoError(t, err)
	listTagsBody2 := sfnBody(t, listTagsResp2)
	tags2 := listTagsBody2["tags"].(map[string]interface{})
	assert.Equal(t, "test", tags2["env"])
	_, hasTeam := tags2["team"]
	assert.False(t, hasTeam)
}

func TestStepFunctions_UpdateStateMachine(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "UpdateSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:UpdateSM"
	newDef := `{"Comment":"updated","StartAt":"End","States":{"End":{"Type":"Pass","End":true}}}`

	updateResp, err := p.HandleRequest(ctx, sfnRequest("UpdateStateMachine", map[string]any{
		"stateMachineArn": smArn,
		"definition":      newDef,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, updateResp.StatusCode)

	// Verify it updated.
	descResp, err := p.HandleRequest(ctx, sfnRequest("DescribeStateMachine", map[string]any{
		"stateMachineArn": smArn,
	}))
	require.NoError(t, err)
	descBody := sfnBody(t, descResp)
	assert.Equal(t, newDef, descBody["definition"])
}

func TestStepFunctions_DeleteStateMachine(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "DeleteSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:DeleteSM"

	delResp, err := p.HandleRequest(ctx, sfnRequest("DeleteStateMachine", map[string]any{
		"stateMachineArn": smArn,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, delResp.StatusCode)

	// Describe should fail now.
	_, err = p.HandleRequest(ctx, sfnRequest("DescribeStateMachine", map[string]any{
		"stateMachineArn": smArn,
	}))
	require.Error(t, err)
}

// --- ASL execution engine tests (v0.39.0) ---

// sfnCreate is a helper that creates a state machine and returns its ARN.
func sfnCreate(t *testing.T, p *substrate.StepFunctionsPlugin, ctx *substrate.RequestContext, name, def, smType string) string {
	t.Helper()
	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       name,
		"definition": def,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
		"type":       smType,
	}))
	require.NoError(t, err)
	return "arn:aws:states:us-east-1:123456789012:stateMachine:" + name
}

// sfnStartAndDescribe starts an execution and returns the DescribeExecution body.
func sfnStartAndDescribe(t *testing.T, p *substrate.StepFunctionsPlugin, ctx *substrate.RequestContext, smArn, execName, input string) map[string]any {
	t.Helper()
	body := map[string]any{"stateMachineArn": smArn, "name": execName}
	if input != "" {
		body["input"] = input
	}
	startResp, err := p.HandleRequest(ctx, sfnRequest("StartExecution", body))
	require.NoError(t, err)
	startBody := sfnBody(t, startResp)
	execArn := startBody["executionArn"].(string)

	descResp, err := p.HandleRequest(ctx, sfnRequest("DescribeExecution", map[string]any{
		"executionArn": execArn,
	}))
	require.NoError(t, err)
	b := sfnBody(t, descResp)
	b["executionArn"] = execArn
	return b
}

func TestSFN_PassState(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"answer":42},"End":true}}}`
	smArn := sfnCreate(t, p, ctx, "PassSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, p, ctx, smArn, "pass-exec-1", "")
	assert.Equal(t, "SUCCEEDED", desc["status"])
	assert.JSONEq(t, `{"answer":42}`, desc["output"].(string))
}

func TestSFN_ChoiceState(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{
		"StartAt":"Check",
		"States":{
			"Check":{
				"Type":"Choice",
				"Choices":[
					{"Variable":"$.color","StringEquals":"red","Next":"RedState"},
					{"Variable":"$.color","StringEquals":"blue","Next":"BlueState"}
				],
				"Default":"DefaultState"
			},
			"RedState":{"Type":"Pass","Result":"red-output","End":true},
			"BlueState":{"Type":"Pass","Result":"blue-output","End":true},
			"DefaultState":{"Type":"Pass","Result":"default-output","End":true}
		}
	}`
	smArn := sfnCreate(t, p, ctx, "ChoiceSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, p, ctx, smArn, "choice-red", `{"color":"red"}`)
	assert.Equal(t, "SUCCEEDED", desc["status"])
	assert.Equal(t, `"red-output"`, desc["output"])

	desc2 := sfnStartAndDescribe(t, p, ctx, smArn, "choice-blue", `{"color":"blue"}`)
	assert.Equal(t, "SUCCEEDED", desc2["status"])
	assert.Equal(t, `"blue-output"`, desc2["output"])

	desc3 := sfnStartAndDescribe(t, p, ctx, smArn, "choice-default", `{"color":"green"}`)
	assert.Equal(t, "SUCCEEDED", desc3["status"])
	assert.Equal(t, `"default-output"`, desc3["output"])
}

func TestSFN_WaitState(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)
	tc := substrate.NewTimeController(time.Now())
	_ = p.Initialize(context.Background(), substrate.PluginConfig{
		State:   substrate.NewMemoryStateManager(),
		Logger:  substrate.NewDefaultLogger(0, false),
		Options: map[string]any{"time_controller": tc},
	})

	def := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"Next":"Done"},"Done":{"Type":"Succeed"}}}`
	smArn := sfnCreate(t, p, ctx, "WaitSM", def, "STANDARD")

	before := tc.Now()
	desc := sfnStartAndDescribe(t, p, ctx, smArn, "wait-exec-1", "")
	after := tc.Now()

	assert.Equal(t, "SUCCEEDED", desc["status"])
	// Time controller should have been advanced by at least 30 seconds.
	assert.GreaterOrEqual(t, after.Sub(before), 30*time.Second)
}

func TestSFN_TaskState_Stub(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:us-east-1:123456789012:activity:my-activity","End":true}}}`
	smArn := sfnCreate(t, p, ctx, "TaskStubSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, p, ctx, smArn, "stub-exec-1", `{"x":1}`)
	assert.Equal(t, "SUCCEEDED", desc["status"])
	// Non-Lambda resource returns stub empty object.
	assert.JSONEq(t, `{}`, desc["output"].(string))
}

func TestSFN_TaskState_Lambda(t *testing.T) {
	// Create a full plugin registry so Lambda can be invoked.
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)
	registry := substrate.NewPluginRegistry()

	lambdaPlugin := &substrate.LambdaPlugin{}
	require.NoError(t, lambdaPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
		},
	}))
	registry.Register(lambdaPlugin)

	sfnPlugin := &substrate.StepFunctionsPlugin{}
	require.NoError(t, sfnPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
		},
	}))
	registry.Register(sfnPlugin)

	ctx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	// Create a Lambda function (Operation = HTTP method for the Lambda plugin).
	fnArn := "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
	_, err := lambdaPlugin.HandleRequest(ctx, &substrate.AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/functions",
		Headers:   map[string]string{},
		Params:    map[string]string{},
		Body:      []byte(`{"FunctionName":"my-fn","Runtime":"python3.12","Role":"arn:aws:iam::123456789012:role/r","Handler":"index.handler","Code":{"ZipFile":""}}`),
	})
	require.NoError(t, err)

	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + fnArn + `","End":true}}}`
	smArn := sfnCreate(t, sfnPlugin, ctx, "LambdaTaskSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, sfnPlugin, ctx, smArn, "lambda-exec-1", `{"input":"data"}`)
	assert.Equal(t, "SUCCEEDED", desc["status"])
	// Stub Lambda returns {"statusCode":200,"body":"null"}.
	assert.NotEmpty(t, desc["output"])
}

func TestSFN_FailState(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"MyError","Cause":"intentional failure"}}}`
	smArn := sfnCreate(t, p, ctx, "FailSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, p, ctx, smArn, "fail-exec-1", "")
	assert.Equal(t, "FAILED", desc["status"])
}

func TestSFN_ParallelState(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{
		"StartAt":"P",
		"States":{
			"P":{
				"Type":"Parallel",
				"End":true,
				"Branches":[
					{"StartAt":"A","States":{"A":{"Type":"Pass","Result":1,"End":true}}},
					{"StartAt":"B","States":{"B":{"Type":"Pass","Result":2,"End":true}}}
				]
			}
		}
	}`
	smArn := sfnCreate(t, p, ctx, "ParallelSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, p, ctx, smArn, "parallel-exec-1", "")
	assert.Equal(t, "SUCCEEDED", desc["status"])
	// Output should be an array of branch results.
	assert.JSONEq(t, `[1,2]`, desc["output"].(string))
}

func TestSFN_MapState(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{
		"StartAt":"M",
		"States":{
			"M":{
				"Type":"Map",
				"End":true,
				"Iterator":{
					"StartAt":"Double",
					"States":{"Double":{"Type":"Pass","End":true}}
				}
			}
		}
	}`
	smArn := sfnCreate(t, p, ctx, "MapSM", def, "STANDARD")

	desc := sfnStartAndDescribe(t, p, ctx, smArn, "map-exec-1", `[{"v":1},{"v":2},{"v":3}]`)
	assert.Equal(t, "SUCCEEDED", desc["status"])
	// Map with Pass iterator passes each item through.
	assert.JSONEq(t, `[{"v":1},{"v":2},{"v":3}]`, desc["output"].(string))
}

func TestSFN_RetryExhausted(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	// Task points at a Lambda that doesn't exist — invoke will fail.
	def := `{
		"StartAt":"T",
		"States":{
			"T":{
				"Type":"Task",
				"Resource":"arn:aws:lambda:us-east-1:123456789012:function:nonexistent",
				"Retry":[{"ErrorEquals":["States.ALL"],"MaxAttempts":2,"IntervalSeconds":1,"BackoffRate":1.0}],
				"End":true
			}
		}
	}`
	smArn := sfnCreate(t, p, ctx, "RetrySM", def, "STANDARD")

	// No registry → non-Lambda resource → stub succeeds. We need a registry for Lambda.
	// Reinstantiate with registry (no Lambda registered → RouteRequest returns error).
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)
	registry := substrate.NewPluginRegistry()
	sfnPlugin := &substrate.StepFunctionsPlugin{}
	require.NoError(t, sfnPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
		},
	}))
	registry.Register(sfnPlugin)

	ctx2 := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}
	smArn2 := sfnCreate(t, sfnPlugin, ctx2, "RetrySM2", def, "STANDARD")

	desc := sfnStartAndDescribe(t, sfnPlugin, ctx2, smArn2, "retry-exec-1", "")
	assert.Equal(t, "FAILED", desc["status"])
	_ = smArn // suppress unused warning
}

func TestSFN_CatchAfterRetry(t *testing.T) {
	p, _ := setupStepFunctionsPlugin(t)

	def := `{
		"StartAt":"T",
		"States":{
			"T":{
				"Type":"Task",
				"Resource":"arn:aws:lambda:us-east-1:123456789012:function:nonexistent",
				"Retry":[{"ErrorEquals":["States.ALL"],"MaxAttempts":1,"IntervalSeconds":1,"BackoffRate":1.0}],
				"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Fallback","ResultPath":"$.error"}],
				"Next":"Done"
			},
			"Fallback":{"Type":"Pass","Result":"caught","End":true},
			"Done":{"Type":"Succeed"}
		}
	}`

	// Need registry with no Lambda plugin so invoke fails.
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)
	registry := substrate.NewPluginRegistry()
	sfnPlugin := &substrate.StepFunctionsPlugin{}
	require.NoError(t, sfnPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
		},
	}))
	registry.Register(sfnPlugin)

	ctx2 := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}
	smArn := sfnCreate(t, sfnPlugin, ctx2, "CatchSM", def, "STANDARD")
	_ = p // suppress unused warning

	desc := sfnStartAndDescribe(t, sfnPlugin, ctx2, smArn, "catch-exec-1", `{}`)
	assert.Equal(t, "SUCCEEDED", desc["status"])
	assert.Equal(t, `"caught"`, desc["output"])
}

func TestSFN_ExpressWorkflow(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"S","States":{"S":{"Type":"Pass","Result":{"done":true},"End":true}}}`
	smArn := sfnCreate(t, p, ctx, "ExpressSM", def, "EXPRESS")

	syncResp, err := p.HandleRequest(ctx, sfnRequest("StartSyncExecution", map[string]any{
		"stateMachineArn": smArn,
		"name":            "sync-exec-1",
		"input":           `{}`,
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, syncResp.StatusCode)

	body := sfnBody(t, syncResp)
	assert.Equal(t, "SUCCEEDED", body["status"])
	assert.JSONEq(t, `{"done":true}`, body["output"].(string))
	assert.NotEmpty(t, body["startDate"])
	assert.NotEmpty(t, body["stopDate"])
}

func TestSFN_ExpressWorkflow_StandardFails(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`
	smArn := sfnCreate(t, p, ctx, "StdSM", def, "STANDARD")

	_, err := p.HandleRequest(ctx, sfnRequest("StartSyncExecution", map[string]any{
		"stateMachineArn": smArn,
	}))
	require.Error(t, err)
}

func TestSFN_GetExecutionHistory_RealEvents(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"B"},"B":{"Type":"Succeed"}}}`
	smArn := sfnCreate(t, p, ctx, "HistRealSM", def, "STANDARD")

	startResp, err := p.HandleRequest(ctx, sfnRequest("StartExecution", map[string]any{
		"stateMachineArn": smArn,
		"name":            "hist-real-exec-1",
	}))
	require.NoError(t, err)
	execArn := sfnBody(t, startResp)["executionArn"].(string)

	histResp, err := p.HandleRequest(ctx, sfnRequest("GetExecutionHistory", map[string]any{
		"executionArn": execArn,
	}))
	require.NoError(t, err)
	histBody := sfnBody(t, histResp)
	events := histBody["events"].([]interface{})

	// 2 states: ExecutionStarted + 2×(StateEntered+StateExited) + ExecutionSucceeded = 6 events.
	assert.GreaterOrEqual(t, len(events), 4)

	types := make([]string, 0, len(events))
	for _, ev := range events {
		types = append(types, ev.(map[string]interface{})["type"].(string))
	}
	assert.Equal(t, "ExecutionStarted", types[0])
	assert.Equal(t, "ExecutionSucceeded", types[len(types)-1])
}

func TestStepFunctions_StopExecution(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	_, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "StopSM",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/sfn-role",
	}))
	require.NoError(t, err)

	smArn := "arn:aws:states:us-east-1:123456789012:stateMachine:StopSM"
	startResp, err := p.HandleRequest(ctx, sfnRequest("StartExecution", map[string]any{
		"stateMachineArn": smArn,
		"name":            "stop-exec-1",
	}))
	require.NoError(t, err)
	startBody := sfnBody(t, startResp)
	execArn := startBody["executionArn"].(string)

	// StopExecution.
	stopResp, err := p.HandleRequest(ctx, sfnRequest("StopExecution", map[string]any{
		"executionArn": execArn,
		"cause":        "test stopped",
		"error":        "TestError",
	}))
	require.NoError(t, err)
	assert.Equal(t, 200, stopResp.StatusCode)
}

func TestSFN_ChoiceState_LogicalOperators(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{
		"StartAt":"C",
		"States":{
			"C":{
				"Type":"Choice",
				"Choices":[
					{
						"And":[
							{"Variable":"$.a","NumericGreaterThan":0},
							{"Variable":"$.b","StringLessThan":"z"}
						],
						"Next":"AndTrue"
					},
					{
						"Or":[
							{"Variable":"$.x","BooleanEquals":true},
							{"Variable":"$.y","NumericLessThan":0}
						],
						"Next":"OrTrue"
					},
					{
						"Not":{"Variable":"$.flag","BooleanEquals":true},
						"Next":"NotTrue"
					}
				],
				"Default":"Default"
			},
			"AndTrue":{"Type":"Pass","Result":"and","End":true},
			"OrTrue":{"Type":"Pass","Result":"or","End":true},
			"NotTrue":{"Type":"Pass","Result":"not","End":true},
			"Default":{"Type":"Pass","Result":"default","End":true}
		}
	}`
	smArn := sfnCreate(t, p, ctx, "LogicChoiceSM", def, "STANDARD")

	// And branch: a>0 AND b<"z" → true.
	d := sfnStartAndDescribe(t, p, ctx, smArn, "and-exec", `{"a":1,"b":"a","x":false,"y":1,"flag":true}`)
	assert.Equal(t, "SUCCEEDED", d["status"])
	assert.Equal(t, `"and"`, d["output"])

	// Or branch: a<=0 → And fails; x=true → Or succeeds.
	d2 := sfnStartAndDescribe(t, p, ctx, smArn, "or-exec", `{"a":0,"b":"a","x":true,"y":1,"flag":true}`)
	assert.Equal(t, "SUCCEEDED", d2["status"])
	assert.Equal(t, `"or"`, d2["output"])

	// Not branch: a<=0 → And fails; x=false, y>=0 → Or fails; flag=false → Not(false)=true.
	d3 := sfnStartAndDescribe(t, p, ctx, smArn, "not-exec", `{"a":0,"b":"a","x":false,"y":1,"flag":false}`)
	assert.Equal(t, "SUCCEEDED", d3["status"])
	assert.Equal(t, `"not"`, d3["output"])
}

func TestSFN_MapState_WithItemsPath(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{
		"StartAt":"M",
		"States":{
			"M":{
				"Type":"Map",
				"ItemsPath":"$.items",
				"End":true,
				"Iterator":{
					"StartAt":"I",
					"States":{"I":{"Type":"Pass","End":true}}
				}
			}
		}
	}`
	smArn := sfnCreate(t, p, ctx, "MapPathSM", def, "STANDARD")

	d := sfnStartAndDescribe(t, p, ctx, smArn, "map-path-exec", `{"items":[1,2,3]}`)
	assert.Equal(t, "SUCCEEDED", d["status"])
	assert.JSONEq(t, `[1,2,3]`, d["output"].(string))
}

func TestSFN_WaitState_SecondsPath(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"W","States":{"W":{"Type":"Wait","SecondsPath":"$.wait","Next":"D"},"D":{"Type":"Succeed"}}}`
	smArn := sfnCreate(t, p, ctx, "WaitPathSM", def, "STANDARD")

	d := sfnStartAndDescribe(t, p, ctx, smArn, "wait-path-exec", `{"wait":10}`)
	assert.Equal(t, "SUCCEEDED", d["status"])
}

func TestSFN_ResultPath_MergesIntoInput(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":42,"ResultPath":"$.result","End":true}}}`
	smArn := sfnCreate(t, p, ctx, "ResultPathSM", def, "STANDARD")

	d := sfnStartAndDescribe(t, p, ctx, smArn, "rp-exec", `{"existing":"value"}`)
	assert.Equal(t, "SUCCEEDED", d["status"])
	// Result 42 merged at $.result, existing field preserved.
	assert.JSONEq(t, `{"existing":"value","result":42}`, d["output"].(string))
}
