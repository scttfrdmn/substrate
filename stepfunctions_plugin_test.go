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
	assert.Equal(t, `"null"`, descExecBody["output"])
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
	assert.Len(t, events, 2)

	firstEvent := events[0].(map[string]interface{})
	assert.Equal(t, "ExecutionStarted", firstEvent["type"])

	secondEvent := events[1].(map[string]interface{})
	assert.Equal(t, "ExecutionSucceeded", secondEvent["type"])
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
