package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfnAASTemplate is a provisioned table with a read target, a target tracking policy naming the
// target by Ref, and the given scheduled actions and maximum capacity.
func cfnAASTemplate(maxCapacity, scheduled string) string {
	return `{"Parameters":{"TrackingValue":{"Type":"Number","Default":"70"}},"Resources":{
		"T":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"cfn-scaled",
			"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
			"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
			"ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}}},
		"Target":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{
			"ServiceNamespace":"dynamodb","ScalableDimension":"dynamodb:table:ReadCapacityUnits",
			"ResourceId":{"Fn::Join":["",["table/",{"Ref":"T"}]]},"MinCapacity":1,"MaxCapacity":` + maxCapacity + `,
			"SuspendedState":{"DynamicScalingInSuspended":"true"},
			"Tags":[{"Key":"team","Value":"data"}],
			"ScheduledActions":[` + scheduled + `]}},
		"Policy":{"Type":"AWS::ApplicationAutoScaling::ScalingPolicy","Properties":{
			"PolicyName":"read","PolicyType":"TargetTrackingScaling","ScalingTargetId":{"Ref":"Target"},
			"TargetTrackingScalingPolicyConfiguration":{"TargetValue":{"Ref":"TrackingValue"},
				"PredefinedMetricSpecification":{"PredefinedMetricType":"DynamoDBReadCapacityUtilization"}}}}},
		"Outputs":{"TargetId":{"Value":{"Ref":"Target"}},"PolicyRef":{"Value":{"Ref":"Policy"}},
			"PolicyArn":{"Value":{"Fn::GetAtt":["Policy","Arn"]}}}}`
}

// TestCFN_ApplicationAutoScalingTypes deploys both types, updates the target in place, and deletes
// the stack.
func TestCFN_ApplicationAutoScalingTypes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	nightly := `{"ScheduledActionName":"nightly","Schedule":"cron(0 2 * * ? *)","StartTime":"2030-01-01T00:00:00Z",
		"ScalableTargetAction":{"MinCapacity":2,"MaxCapacity":"4"}}`
	weekly := `{"ScheduledActionName":"weekly","Schedule":"rate(7 days)"}`
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{
		"StackName": "scaled", "TemplateBody": cfnAASTemplate("10", nightly+","+weekly),
	})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "scaled")
	require.Equal(t, "CREATE_COMPLETE", status, reason)

	assert.Equal(t, "table/cfn-scaled|dynamodb:table:ReadCapacityUnits|dynamodb", cfnStackOutput(t, ts, "scaled", "TargetId"))
	policyARN := cfnStackOutput(t, ts, "scaled", "PolicyRef")
	assert.True(t, strings.Contains(policyARN, ":scalingPolicy:") && strings.HasSuffix(policyARN, ":policyName/read"), policyARN)
	assert.Equal(t, policyARN, cfnStackOutput(t, ts, "scaled", "PolicyArn"))

	targets := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "dynamodb"})["ScalableTargets"].([]any)
	require.Len(t, targets, 1)
	target := targets[0].(map[string]any)
	assert.EqualValues(t, 10, target["MaxCapacity"])
	assert.Equal(t, true, target["SuspendedState"].(map[string]any)["DynamicScalingInSuspended"])
	assert.Equal(t, map[string]any{"team": "data"},
		aasOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": target["ScalableTargetARN"]})["Tags"])
	pols := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "dynamodb"})["ScalingPolicies"].([]any)
	require.Len(t, pols, 1)
	assert.EqualValues(t, 70, pols[0].(map[string]any)["TargetTrackingScalingPolicyConfiguration"].(map[string]any)["TargetValue"],
		"a Ref to a Number parameter is sent as a number")
	actions := aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb"})["ScheduledActions"].([]any)
	require.Len(t, actions, 2)
	assert.EqualValues(t, 1893456000, actions[0].(map[string]any)["StartTime"])
	assert.EqualValues(t, 4, actions[0].(map[string]any)["ScalableTargetAction"].(map[string]any)["MaxCapacity"])

	// MaxCapacity is No interruption and ScheduledActions too: the target keeps its ID and ARN, and
	// the dropped action is deleted.
	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{
		"StackName": "scaled", "TemplateBody": cfnAASTemplate("20", nightly),
	})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "scaled")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)
	updated := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "dynamodb"})["ScalableTargets"].([]any)
	require.Len(t, updated, 1)
	assert.EqualValues(t, 20, updated[0].(map[string]any)["MaxCapacity"])
	assert.Equal(t, target["ScalableTargetARN"], updated[0].(map[string]any)["ScalableTargetARN"])
	actions = aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb"})["ScheduledActions"].([]any)
	require.Len(t, actions, 1)
	assert.Equal(t, "nightly", actions[0].(map[string]any)["ScheduledActionName"])
	assert.Equal(t, policyARN, cfnStackOutput(t, ts, "scaled", "PolicyRef"))

	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "scaled"})
	require.Equal(t, http.StatusOK, code, body)
	code, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "scaled"})
	assert.Equal(t, http.StatusBadRequest, code, "the stack deleted cleanly: %s", body)
	assert.Empty(t, aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "dynamodb"})["ScalableTargets"])
	assert.Empty(t, aasAlarmNames(t, ts))
}

// TestCFN_ScalingPolicyTargetForms pins the two ways a policy names its target, and the refusals
// for naming it both ways or neither.
func TestCFN_ScalingPolicyTargetForms(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(id, policyTarget string) string {
		return `{"Resources":{
			"Target":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{
				"ServiceNamespace":"custom-resource","ScalableDimension":"custom-resource:ResourceType:Property",
				"ResourceId":"` + id + `","MinCapacity":0,"MaxCapacity":5,
				"RoleARN":"arn:aws:iam::123456789012:role/r"}},
			"Policy":{"Type":"AWS::ApplicationAutoScaling::ScalingPolicy","DependsOn":"Target","Properties":{
				"PolicyName":"step","PolicyType":"StepScaling",` + policyTarget + `
				"StepScalingPolicyConfiguration":{"AdjustmentType":"ChangeInCapacity","Cooldown":"60",
					"StepAdjustments":[{"MetricIntervalLowerBound":"0","ScalingAdjustment":"1"}]}}}}}`
	}
	explicit := `"ServiceNamespace":"custom-resource","ScalableDimension":"custom-resource:ResourceType:Property",
		"ResourceId":"https://example.com/explicit",`
	for name, tc := range map[string]struct {
		target string
		status string
	}{
		"explicit": {strings.ReplaceAll(explicit, "explicit", "forms-explicit"), "CREATE_COMPLETE"},
		"both":     {`"ScalingTargetId":{"Ref":"Target"},` + explicit, "ROLLBACK_COMPLETE"},
		"neither":  {"", "ROLLBACK_COMPLETE"},
		"bad ID":   {`"ScalingTargetId":"not-an-id",`, "ROLLBACK_COMPLETE"},
	} {
		stack := "forms-" + strings.ReplaceAll(name, " ", "-")
		code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{
			"StackName": stack, "TemplateBody": tmpl("https://example.com/"+stack, tc.target),
		})
		require.Equal(t, http.StatusOK, code, body)
		status, reason := cfnStackStatusOf(t, ts, stack)
		assert.Equal(t, tc.status, status, "%s: %s", name, reason)
	}
	pols := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource"})["ScalingPolicies"].([]any)
	require.Len(t, pols, 1)
	step := pols[0].(map[string]any)["StepScalingPolicyConfiguration"].(map[string]any)
	assert.EqualValues(t, 60, step["Cooldown"])
}
