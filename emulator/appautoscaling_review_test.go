package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCFN_ApplicationAutoScalingCreateAdoptsNothing pins that a stack's create refuses a target or
// policy that already exists, so the stack's delete cannot remove another owner's, and that an
// update deletes only the scheduled actions the stack itself declared.
func TestCFN_ApplicationAutoScalingCreateAdoptsNothing(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ref := map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "owned",
		"ScalableDimension": "custom-resource:ResourceType:Property"}
	with := func(extra map[string]any) map[string]any {
		m := map[string]any{}
		for k, v := range ref {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	aasOK(t, ts, "RegisterScalableTarget", with(map[string]any{"MinCapacity": 0, "MaxCapacity": 4}))
	aasOK(t, ts, "PutScalingPolicy", with(map[string]any{"PolicyName": "theirs", "PolicyType": "StepScaling",
		"StepScalingPolicyConfiguration": map[string]any{"AdjustmentType": "ChangeInCapacity"}}))

	target := `"T":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{"ServiceNamespace":"custom-resource",
		"ScalableDimension":"custom-resource:ResourceType:Property","ResourceId":"owned","MinCapacity":1,"MaxCapacity":2}}`
	policy := `"P":{"Type":"AWS::ApplicationAutoScaling::ScalingPolicy","Properties":{"PolicyName":"theirs","PolicyType":"StepScaling",
		"ScalingTargetId":"owned|custom-resource:ResourceType:Property|custom-resource",
		"StepScalingPolicyConfiguration":{"AdjustmentType":"ExactCapacity"}}}`
	for name, resources := range map[string]string{"adopt-target": target, "adopt-policy": policy} {
		code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": name, "TemplateBody": `{"Resources":{` + resources + `}}`})
		require.Equal(t, http.StatusOK, code, body)
		status, reason := cfnStackStatusOf(t, ts, name)
		assert.Equal(t, "ROLLBACK_COMPLETE", status, name)
		assert.Contains(t, reason, "already exists", name)
	}
	got := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource"})["ScalableTargets"].([]any)
	require.Len(t, got, 1, "the existing target survives both rollbacks")
	assert.EqualValues(t, 4, got[0].(map[string]any)["MaxCapacity"])
	pols := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource"})["ScalingPolicies"].([]any)
	require.Len(t, pols, 1)
	assert.Contains(t, pols[0], "StepScalingPolicyConfiguration")
	assert.Equal(t, "ChangeInCapacity", pols[0].(map[string]any)["StepScalingPolicyConfiguration"].(map[string]any)["AdjustmentType"])

	// A stack's target with two declared actions; an action added outside the stack survives an
	// update that drops one of the declared ones, and an update that changes MaxCapacity and the
	// policy's configuration applies them in place.
	tmpl := func(maxCapacity, actions, adjustment string) string {
		return `{"Resources":{"T":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{"ServiceNamespace":"custom-resource",
			"ScalableDimension":"custom-resource:ResourceType:Property","ResourceId":"stacked","MinCapacity":0,"MaxCapacity":` + maxCapacity + `,
			"ScheduledActions":[` + actions + `]}},
			"P":{"Type":"AWS::ApplicationAutoScaling::ScalingPolicy","Properties":{"PolicyName":"mine","PolicyType":"StepScaling",
			"ScalingTargetId":{"Ref":"T"},"StepScalingPolicyConfiguration":{"AdjustmentType":"` + adjustment + `"}}}},
			"Outputs":{"Policy":{"Value":{"Ref":"P"}}}}`
	}
	a := `{"ScheduledActionName":"a","Schedule":"rate(1 day)"}`
	b := `{"ScheduledActionName":"b","Schedule":"rate(2 days)"}`
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "stacked", "TemplateBody": tmpl("2", a+","+b, "ChangeInCapacity")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "stacked")
	require.Equal(t, "CREATE_COMPLETE", status, reason)
	policyARN := cfnStackOutput(t, ts, "stacked", "Policy")
	aasOK(t, ts, "PutScheduledAction", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "stacked",
		"ScalableDimension": "custom-resource:ResourceType:Property", "ScheduledActionName": "outside", "Schedule": "rate(3 days)"})

	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "stacked", "TemplateBody": tmpl("3", a, "ExactCapacity")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "stacked")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)
	actions := aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "stacked"})["ScheduledActions"].([]any)
	var names []string
	for _, act := range actions {
		names = append(names, act.(map[string]any)["ScheduledActionName"].(string))
	}
	assert.ElementsMatch(t, []string{"a", "outside"}, names)
	targets := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource", "ResourceIds": []string{"stacked"}})["ScalableTargets"].([]any)
	assert.EqualValues(t, 3, targets[0].(map[string]any)["MaxCapacity"])
	assert.Equal(t, policyARN, cfnStackOutput(t, ts, "stacked", "Policy"), "the policy updates in place")
	pols = aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "stacked"})["ScalingPolicies"].([]any)
	require.Len(t, pols, 1)
	assert.Equal(t, "ExactCapacity", pols[0].(map[string]any)["StepScalingPolicyConfiguration"].(map[string]any)["AdjustmentType"])
}

// TestCFN_ApplicationAutoScalingFailedSyncLeavesNoTarget pins that a target whose scheduled actions
// fail on create is deregistered, so the rollback leaves nothing behind.
func TestCFN_ApplicationAutoScalingFailedSyncLeavesNoTarget(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "bad-actions",
		"TemplateBody": `{"Resources":{"T":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{"ServiceNamespace":"custom-resource",
			"ScalableDimension":"custom-resource:ResourceType:Property","ResourceId":"orphan","MinCapacity":0,"MaxCapacity":2,
			"ScheduledActions":[{"ScheduledActionName":"ok","Schedule":"rate(1 day)"},{"ScheduledActionName":"bad","Schedule":"never"}]}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	status, _ := cfnStackStatusOf(t, ts, "bad-actions")
	assert.Equal(t, "ROLLBACK_COMPLETE", status)
	assert.Empty(t, aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource"})["ScalableTargets"])
	assert.Empty(t, aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "custom-resource"})["ScheduledActions"])
}

// TestApplicationAutoScaling_TaggingAPIStaysInTheCallersAccount pins that the Resource Groups
// Tagging API cannot tag another account's scalable target by naming its ARN.
func TestApplicationAutoScaling_TaggingAPIStaysInTheCallersAccount(t *testing.T) {
	const owner, other = "111111111111", "222222222222"
	ts := emulator.StartTestServerWithAccounts(t, owner, other)
	signed := func(account, signingName, target string, body any) (int, []byte) {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
		require.NoError(t, err)
		req.Header.Set("X-Amz-Target", target)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		creds, ok := ts.CredentialsFor(account)
		require.True(t, ok)
		signAs(req, creds, signingName, "us-east-1", raw)
		return sjDo(t, req)
	}
	code, raw := signed(owner, "application-autoscaling", "AnyScaleFrontendService.RegisterScalableTarget", map[string]any{
		"ServiceNamespace": "custom-resource", "ResourceId": "r", "ScalableDimension": "custom-resource:ResourceType:Property",
		"MinCapacity": 0, "MaxCapacity": 1, "Tags": map[string]string{"team": "a"},
	})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	var out struct {
		ScalableTargetARN string `json:"ScalableTargetARN"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	for _, op := range []string{"TagResources", "UntagResources"} {
		body := map[string]any{"ResourceARNList": []string{out.ScalableTargetARN}, "Tags": map[string]string{"team": "stolen"}}
		if op == "UntagResources" {
			body = map[string]any{"ResourceARNList": []string{out.ScalableTargetARN}, "TagKeys": []string{"team"}}
		}
		code, raw = signed(other, "tagging", "ResourceGroupsTaggingAPI_20170126."+op, body)
		require.Equal(t, http.StatusOK, code, "%s", raw)
		assert.Contains(t, string(raw), out.ScalableTargetARN, "%s reports the foreign ARN failed: %s", op, raw)
	}
	code, raw = signed(owner, "application-autoscaling", "AnyScaleFrontendService.ListTagsForResource",
		map[string]any{"ResourceARN": out.ScalableTargetARN})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.JSONEq(t, `{"Tags":{"team":"a"}}`, string(raw))
}

// TestApplicationAutoScaling_FailuresUndoAndPassThrough pins that a policy whose record cannot be
// stored deletes the alarms it created, and that a store failure behind a target's resource check
// is returned as a failure rather than read as a missing resource.
func TestApplicationAutoScaling_FailuresUndoAndPassThrough(t *testing.T) {
	newEnv := func(faulted string) (*nsFaultState, *emulator.PluginRegistry, func(op string, body any) error) {
		state := &nsFaultState{StateManager: emulator.NewMemoryStateManager(), namespace: faulted}
		tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
		logger := emulator.NewDefaultLogger(slog.LevelError, false)
		registry := emulator.NewPluginRegistry()
		for _, p := range []emulator.Plugin{&emulator.CloudWatchPlugin{}, &emulator.DynamoDBPlugin{}, &emulator.ApplicationAutoScalingPlugin{}} {
			require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger,
				Options: map[string]any{"time_controller": tc, "registry": registry}}))
			registry.Register(p)
		}
		call := func(op string, body any) error {
			raw, err := json.Marshal(body)
			require.NoError(t, err)
			ctx := &emulator.RequestContext{RequestID: "req-" + op, AccountID: "123456789012", Region: "us-east-1", IDs: emulator.NewIDMint("req-" + op)}
			_, err = registry.RouteRequest(ctx, &emulator.AWSRequest{Service: "application-autoscaling", Operation: op, Body: raw,
				Headers: map[string]string{}, Params: map[string]string{}})
			return err
		}
		return state, registry, call
	}
	alarmCount := func(registry *emulator.PluginRegistry) int {
		ctx := &emulator.RequestContext{RequestID: "req-alarms", AccountID: "123456789012", Region: "us-east-1", IDs: emulator.NewIDMint("req-alarms")}
		resp, err := registry.RouteRequest(ctx, &emulator.AWSRequest{Service: "monitoring", Operation: "DescribeAlarms",
			Headers: map[string]string{}, Params: map[string]string{"Action": "DescribeAlarms"}})
		require.NoError(t, err)
		raw, err := io.ReadAll(bytes.NewReader(resp.Body))
		require.NoError(t, err)
		return strings.Count(string(raw), "<AlarmName>")
	}

	state, registry, call := newEnv("application-autoscaling")
	ref := map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "r", "ScalableDimension": "custom-resource:ResourceType:Property"}
	reg := map[string]any{"MinCapacity": 0, "MaxCapacity": 2}
	pol := map[string]any{"PolicyName": "t", "PolicyType": "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{"TargetValue": 5, "PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "X"}}}
	for k, v := range ref {
		reg[k], pol[k] = v, v
	}
	require.NoError(t, call("RegisterScalableTarget", reg))
	state.fail = "put"
	err := call("PutScalingPolicy", pol)
	require.ErrorIs(t, err, errInjectedStore)
	assert.Zero(t, alarmCount(registry), "the alarms the failed policy created are deleted")
	state.fail = ""
	require.NoError(t, call("PutScalingPolicy", pol))
	assert.Equal(t, 2, alarmCount(registry))

	state, _, call = newEnv("dynamodb")
	state.fail = "get"
	err = call("RegisterScalableTarget", map[string]any{"ServiceNamespace": "dynamodb", "ResourceId": "table/x",
		"ScalableDimension": "dynamodb:table:ReadCapacityUnits", "MinCapacity": 1, "MaxCapacity": 2})
	var awsErr *emulator.AWSError
	assert.False(t, errors.As(err, &awsErr), "a store failure is not a ValidationException: %v", err)
	assert.Error(t, err)

	// Without a DynamoDB plugin there is nothing to ask, and the target registers.
	p := &emulator.ApplicationAutoScalingPlugin{}
	registry = emulator.NewPluginRegistry()
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: emulator.NewMemoryStateManager(),
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1_800_000_000, 0)), "registry": registry}}))
	registry.Register(p)
	_, err = registry.RouteRequest(&emulator.RequestContext{RequestID: "req-x", AccountID: "123456789012", Region: "us-east-1",
		IDs: emulator.NewIDMint("req-x")}, &emulator.AWSRequest{Service: "application-autoscaling", Operation: "RegisterScalableTarget",
		Body:    []byte(`{"ServiceNamespace":"dynamodb","ResourceId":"table/x","ScalableDimension":"dynamodb:table:ReadCapacityUnits","MinCapacity":1,"MaxCapacity":2}`),
		Headers: map[string]string{}, Params: map[string]string{}})
	assert.NoError(t, err)
}

// TestCFN_ApplicationAutoScalingUpdates pins the updater: a Tags-only change, a removed
// SuspendedState and RoleARN reset, a failed scheduled-action sync whose rollback removes what it
// put, and a policy replacement that keeps its name and target updating in place.
func TestCFN_ApplicationAutoScalingUpdates(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(extra, actions, policyTarget string) string {
		return `{"Resources":{"T":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{"ServiceNamespace":"custom-resource",
			"ScalableDimension":"custom-resource:ResourceType:Property","ResourceId":"upd","MinCapacity":0,"MaxCapacity":2` + extra + `,
			"ScheduledActions":[` + actions + `]}},
			"P":{"Type":"AWS::ApplicationAutoScaling::ScalingPolicy","DependsOn":"T","Properties":{"PolicyName":"p","PolicyType":"StepScaling",` + policyTarget + `,
			"StepScalingPolicyConfiguration":{"AdjustmentType":"ChangeInCapacity"}}}},
			"Outputs":{"Policy":{"Value":{"Ref":"P"}}}}`
	}
	byRef := `"ScalingTargetId":{"Ref":"T"}`
	explicit := `"ServiceNamespace":"custom-resource","ScalableDimension":"custom-resource:ResourceType:Property","ResourceId":"upd"`
	a := `{"ScheduledActionName":"a","Schedule":"rate(1 day)"}`
	update := func(body, want string) {
		t.Helper()
		code, out := cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "upd", "TemplateBody": body})
		require.Equal(t, http.StatusOK, code, out)
		status, reason := cfnStackStatusOf(t, ts, "upd")
		require.Equal(t, want, status, reason)
	}
	target := func() map[string]any {
		got := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource"})["ScalableTargets"].([]any)
		require.Len(t, got, 1)
		return got[0].(map[string]any)
	}
	actionNames := func() []string {
		var names []string
		for _, act := range aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "custom-resource"})["ScheduledActions"].([]any) {
			names = append(names, act.(map[string]any)["ScheduledActionName"].(string))
		}
		return names
	}

	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "upd", "TemplateBody": tmpl(
		`,"Tags":[{"Key":"a","Value":"1"}],"SuspendedState":{"DynamicScalingInSuspended":true},"RoleARN":"arn:aws:iam::123456789012:role/custom"`,
		a, byRef)})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "upd")
	require.Equal(t, "CREATE_COMPLETE", status, reason)
	arn := target()["ScalableTargetARN"].(string)
	policyARN := cfnStackOutput(t, ts, "upd", "Policy")

	// Tags change on their own; SuspendedState and RoleARN removed are reset.
	update(tmpl(`,"Tags":[{"Key":"b","Value":"2"}]`, a, byRef), "UPDATE_COMPLETE")
	assert.Equal(t, map[string]any{"b": "2"}, aasOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": arn})["Tags"])
	got := target()
	assert.Equal(t, false, got["SuspendedState"].(map[string]any)["DynamicScalingInSuspended"])
	assert.Contains(t, got["RoleARN"], "AWSServiceRoleForApplicationAutoScaling_CustomResource")

	// A sync that fails partway rolls back, and the rollback removes the action it had put.
	update(tmpl(`,"Tags":[{"Key":"b","Value":"2"}]`, a+`,{"ScheduledActionName":"b","Schedule":"rate(2 days)"},{"ScheduledActionName":"c","Schedule":"never"}`, byRef),
		"UPDATE_ROLLBACK_COMPLETE")
	assert.ElementsMatch(t, []string{"a"}, actionNames())

	// ScalingTargetId swapped for the explicit three members names the same policy: it updates in
	// place, keeping its ARN, rather than being refused as one that already exists.
	update(tmpl(`,"Tags":[{"Key":"b","Value":"2"}]`, a, explicit), "UPDATE_COMPLETE")
	assert.Equal(t, policyARN, cfnStackOutput(t, ts, "upd", "Policy"))
	assert.Len(t, aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource"})["ScalingPolicies"], 1)
}

// TestApplicationAutoScaling_AlarmsFollowTheTarget pins that a capacity change redefines a DynamoDB
// policy's provisioned-capacity alarms, that an alarm's kind is read from its suffix even when the
// resource ID contains a kind's text, and that a policy changing type keeps only the new type's
// configuration.
func TestApplicationAutoScaling_AlarmsFollowTheTarget(t *testing.T) {
	ts := emulator.StartTestServer(t)
	aasCreateTable(t, ts, "cap", "")
	ref := map[string]any{"ServiceNamespace": "dynamodb", "ResourceId": "table/cap", "ScalableDimension": "dynamodb:table:WriteCapacityUnits"}
	with := func(extra map[string]any) map[string]any {
		m := map[string]any{}
		for k, v := range ref {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	aasOK(t, ts, "RegisterScalableTarget", with(map[string]any{"MinCapacity": 1, "MaxCapacity": 10}))
	alarms := aasOK(t, ts, "PutScalingPolicy", with(map[string]any{"PolicyName": "w", "PolicyType": "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{"TargetValue": 50,
			"PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "DynamoDBWriteCapacityUtilization"}}}))["Alarms"].([]any)
	high := alarms[2].(map[string]any)["AlarmName"].(string)
	threshold := func(name string) string {
		_, body := singleAccountQuery(t, ts, "monitoring", "", map[string]string{"Action": "DescribeAlarms", "Version": "2010-08-01",
			"AlarmNames.member.1": name})
		i := strings.Index(body, "<Threshold>")
		require.Positive(t, i, body)
		rest := body[i+len("<Threshold>"):]
		return strings.TrimSuffix(rest[:strings.Index(rest, "</Threshold>")], ".0")
	}
	assert.Equal(t, "10", threshold(high))
	aasOK(t, ts, "RegisterScalableTarget", with(map[string]any{"MaxCapacity": 20}))
	assert.Equal(t, "20", threshold(high), "the provisioned-capacity alarm follows MaxCapacity")

	// A resource ID holding a kind's text does not confuse the alarm kinds.
	odd := map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "x-AlarmLow-y",
		"ScalableDimension": "custom-resource:ResourceType:Property"}
	reg := map[string]any{"MinCapacity": 0, "MaxCapacity": 2}
	pol := map[string]any{"PolicyName": "t", "PolicyType": "TargetTrackingScaling", "TargetTrackingScalingPolicyConfiguration": map[string]any{
		"TargetValue": 40, "PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "X"}}}
	for k, v := range odd {
		reg[k], pol[k] = v, v
	}
	aasOK(t, ts, "RegisterScalableTarget", reg)
	oddAlarms := aasOK(t, ts, "PutScalingPolicy", pol)["Alarms"].([]any)
	require.Len(t, oddAlarms, 2)
	assert.Equal(t, "40", threshold(oddAlarms[0].(map[string]any)["AlarmName"].(string)), "AlarmHigh breaches at the target")

	// Step to tracking and back: the step configuration did not survive the change.
	step := map[string]any{"PolicyName": "s", "PolicyType": "StepScaling", "StepScalingPolicyConfiguration": map[string]any{"AdjustmentType": "ChangeInCapacity"}}
	for k, v := range odd {
		step[k] = v
	}
	aasOK(t, ts, "PutScalingPolicy", step)
	step["PolicyType"] = "TargetTrackingScaling"
	delete(step, "StepScalingPolicyConfiguration")
	step["TargetTrackingScalingPolicyConfiguration"] = pol["TargetTrackingScalingPolicyConfiguration"]
	aasOK(t, ts, "PutScalingPolicy", step)
	step["PolicyType"] = "StepScaling"
	delete(step, "TargetTrackingScalingPolicyConfiguration")
	code, doc, _ := aasCall(t, ts, "PutScalingPolicy", step)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationException", aasErrorCode(doc), "a type change needs the new type's configuration")
}
