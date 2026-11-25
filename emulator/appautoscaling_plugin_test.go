package emulator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aasCall sends one Application Auto Scaling request the way the Terraform AWS provider and boto3
// do: application/x-amz-json-1.1 with the AnyScaleFrontendService target, to the emulator's own
// host, so the target is the only routing signal.
func aasCall(t *testing.T, ts *emulator.TestServer, op string, body any) (int, map[string]any, []byte) {
	t.Helper()
	return jsonTargetCall(t, ts, "AnyScaleFrontendService."+op, body)
}

// jsonTargetCall posts a JSON 1.1 body with an X-Amz-Target and no service host or signature.
func jsonTargetCall(t *testing.T, ts *emulator.TestServer, target string, body any) (int, map[string]any, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	code, got := sjDo(t, req)
	var doc map[string]any
	if len(got) > 0 {
		require.NoError(t, json.Unmarshal(got, &doc), "%s", got)
	}
	return code, doc, got
}

// aasOK asserts a 200 and returns the decoded body.
func aasOK(t *testing.T, ts *emulator.TestServer, op string, body any) map[string]any {
	t.Helper()
	code, doc, raw := aasCall(t, ts, op, body)
	require.Equal(t, http.StatusOK, code, "%s: %s", op, raw)
	return doc
}

// aasCreateTable creates a provisioned DynamoDB table, optionally with one global secondary index.
func aasCreateTable(t *testing.T, ts *emulator.TestServer, name, index string) {
	t.Helper()
	body := map[string]any{
		"TableName":             name,
		"AttributeDefinitions":  []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
		"KeySchema":             []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
		"ProvisionedThroughput": map[string]int{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
	}
	if index != "" {
		body["GlobalSecondaryIndexes"] = []map[string]any{{
			"IndexName":             index,
			"KeySchema":             []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
			"Projection":            map[string]string{"ProjectionType": "ALL"},
			"ProvisionedThroughput": map[string]int{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
		}}
	}
	code, _, raw := jsonTargetCall(t, ts, "DynamoDB_20120810.CreateTable", body)
	require.Equal(t, http.StatusOK, code, "%s", raw)
}

// aasErrorCode returns the __type of a JSON error body.
func aasErrorCode(doc map[string]any) string {
	s, _ := doc["__type"].(string)
	if i := strings.LastIndexByte(s, '#'); i >= 0 {
		s = s[i+1:]
	}
	return s
}

// aasAlarmNames returns the names of the CloudWatch alarms DescribeAlarms answers.
func aasAlarmNames(t *testing.T, ts *emulator.TestServer) []string {
	t.Helper()
	_, body := singleAccountQuery(t, ts, "monitoring", "", map[string]string{"Action": "DescribeAlarms", "Version": "2010-08-01"})
	var names []string
	for _, part := range strings.Split(body, "<AlarmName>")[1:] {
		names = append(names, part[:strings.Index(part, "</AlarmName>")])
	}
	return names
}

// TestApplicationAutoScaling_TerraformDynamoDBJourney walks the calls the Terraform AWS provider's
// aws_appautoscaling_target, aws_appautoscaling_policy and aws_appautoscaling_scheduled_action make
// on a DynamoDB table: create, read back, update, and destroy.
func TestApplicationAutoScaling_TerraformDynamoDBJourney(t *testing.T) {
	ts := emulator.StartTestServer(t)
	aasCreateTable(t, ts, "orders", "")
	target := map[string]any{
		"ServiceNamespace": "dynamodb", "ResourceId": "table/orders",
		"ScalableDimension": "dynamodb:table:ReadCapacityUnits",
	}

	// aws_appautoscaling_target: RegisterScalableTarget, then DescribeScalableTargets by the three keys.
	reg := map[string]any{"MinCapacity": 1, "MaxCapacity": 10, "Tags": map[string]string{"env": "test"}}
	for k, v := range target {
		reg[k] = v
	}
	out := aasOK(t, ts, "RegisterScalableTarget", reg)
	arn, _ := out["ScalableTargetARN"].(string)
	assert.Regexp(t, `^arn:aws:application-autoscaling:us-east-1:\d{12}:scalable-target/[0-9a-f]{36}$`, arn)

	describe := func() []any {
		doc := aasOK(t, ts, "DescribeScalableTargets", map[string]any{
			"ServiceNamespace": "dynamodb", "ResourceIds": []string{"table/orders"},
			"ScalableDimension": "dynamodb:table:ReadCapacityUnits",
		})
		list, _ := doc["ScalableTargets"].([]any)
		return list
	}
	targets := describe()
	require.Len(t, targets, 1)
	got := targets[0].(map[string]any)
	assert.Equal(t, arn, got["ScalableTargetARN"])
	assert.EqualValues(t, 1, got["MinCapacity"])
	assert.EqualValues(t, 10, got["MaxCapacity"])
	assert.Contains(t, got["RoleARN"], ":role/aws-service-role/dynamodb.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_DynamoDBTable")
	assert.Equal(t, map[string]any{
		"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false,
	}, got["SuspendedState"])
	assert.IsType(t, float64(0), got["CreationTime"], "an epoch-seconds timestamp")

	// An update changes only what it names, and keeps the ARN and the tags.
	upd := map[string]any{"MaxCapacity": 20, "SuspendedState": map[string]bool{"ScheduledScalingSuspended": true}}
	for k, v := range target {
		upd[k] = v
	}
	assert.Equal(t, arn, aasOK(t, ts, "RegisterScalableTarget", upd)["ScalableTargetARN"])
	got = describe()[0].(map[string]any)
	assert.EqualValues(t, 1, got["MinCapacity"])
	assert.EqualValues(t, 20, got["MaxCapacity"])
	assert.Equal(t, true, got["SuspendedState"].(map[string]any)["ScheduledScalingSuspended"])
	assert.Equal(t, map[string]any{"env": "test"}, aasOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": arn})["Tags"])

	// aws_appautoscaling_policy: a target tracking policy creates two CloudWatch alarms.
	pol := map[string]any{
		"PolicyName": "read-tracking", "PolicyType": "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{
			"TargetValue":                   70,
			"PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "DynamoDBReadCapacityUtilization"},
		},
	}
	for k, v := range target {
		pol[k] = v
	}
	out = aasOK(t, ts, "PutScalingPolicy", pol)
	policyARN, _ := out["PolicyARN"].(string)
	assert.Regexp(t, `^arn:aws:autoscaling:us-east-1:\d{12}:scalingPolicy:[0-9a-f-]{36}:resource/dynamodb/table/orders:policyName/read-tracking$`, policyARN)
	// A DynamoDB target tracking policy creates four alarms, as the DynamoDB developer guide states.
	alarms, _ := out["Alarms"].([]any)
	require.Len(t, alarms, 4)
	var names []string
	for i, kind := range []string{"AlarmHigh", "AlarmLow", "ProvisionedCapacityHigh", "ProvisionedCapacityLow"} {
		name := alarms[i].(map[string]any)["AlarmName"].(string)
		assert.True(t, strings.HasPrefix(name, "TargetTracking-table/orders-"+kind+"-"), name)
		names = append(names, name)
	}
	assert.ElementsMatch(t, names, aasAlarmNames(t, ts))

	// A re-put keeps the ARN and the alarm names.
	pol["TargetTrackingScalingPolicyConfiguration"] = map[string]any{
		"TargetValue":                   50,
		"PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "DynamoDBReadCapacityUtilization"},
	}
	again := aasOK(t, ts, "PutScalingPolicy", pol)
	assert.Equal(t, policyARN, again["PolicyARN"])
	assert.Equal(t, alarms, again["Alarms"])

	doc := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "dynamodb", "ResourceId": "table/orders", "PolicyNames": []string{"read-tracking"},
	})
	policies, _ := doc["ScalingPolicies"].([]any)
	require.Len(t, policies, 1)
	p := policies[0].(map[string]any)
	assert.Equal(t, "TargetTrackingScaling", p["PolicyType"])
	assert.EqualValues(t, 50, p["TargetTrackingScalingPolicyConfiguration"].(map[string]any)["TargetValue"])
	assert.NotContains(t, p, "StepScalingPolicyConfiguration")

	// aws_appautoscaling_scheduled_action.
	sched := map[string]any{
		"ScheduledActionName": "nightly", "Schedule": "cron(0 2 * * ? *)", "Timezone": "UTC",
		"StartTime": 1893456000, "ScalableTargetAction": map[string]int{"MinCapacity": 2, "MaxCapacity": 8},
	}
	for k, v := range target {
		sched[k] = v
	}
	aasOK(t, ts, "PutScheduledAction", sched)
	doc = aasOK(t, ts, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": "dynamodb", "ScheduledActionNames": []string{"nightly"},
	})
	actions, _ := doc["ScheduledActions"].([]any)
	require.Len(t, actions, 1)
	a := actions[0].(map[string]any)
	assert.Equal(t, "cron(0 2 * * ? *)", a["Schedule"])
	assert.EqualValues(t, 1893456000, a["StartTime"])
	assert.Regexp(t, `:scheduledAction:[0-9a-f-]{36}:resource/dynamodb/table/orders:scheduledActionName/nightly$`, a["ScheduledActionARN"])
	assert.EqualValues(t, 1, describe()[0].(map[string]any)["MinCapacity"], "a scheduled action changes no capacity")

	// An update without start and end times deletes them.
	delete(sched, "StartTime")
	aasOK(t, ts, "PutScheduledAction", sched)
	doc = aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb"})
	assert.NotContains(t, doc["ScheduledActions"].([]any)[0], "StartTime")

	// Destroy, in Terraform's order.
	aasOK(t, ts, "DeleteScheduledAction", map[string]any{
		"ServiceNamespace": "dynamodb", "ResourceId": "table/orders",
		"ScalableDimension": "dynamodb:table:ReadCapacityUnits", "ScheduledActionName": "nightly",
	})
	delPol := map[string]any{"PolicyName": "read-tracking"}
	for k, v := range target {
		delPol[k] = v
	}
	aasOK(t, ts, "DeleteScalingPolicy", delPol)
	assert.Empty(t, aasAlarmNames(t, ts), "the policy's alarms go with it")
	code, errDoc, _ := aasCall(t, ts, "DeleteScalingPolicy", delPol)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ObjectNotFoundException", aasErrorCode(errDoc))

	aasOK(t, ts, "DeregisterScalableTarget", target)
	assert.Empty(t, describe())
	code, errDoc, _ = aasCall(t, ts, "DeregisterScalableTarget", target)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ObjectNotFoundException", aasErrorCode(errDoc))
}

// TestApplicationAutoScaling_DeregisterDeletesPoliciesAndAlarms pins "Deregistering a scalable
// target deletes the scaling policies and the scheduled actions that are associated with it".
func TestApplicationAutoScaling_DeregisterDeletesPoliciesAndAlarms(t *testing.T) {
	ts := emulator.StartTestServer(t)
	aasCreateTable(t, ts, "events", "by-day")
	target := map[string]any{
		"ServiceNamespace": "dynamodb", "ResourceId": "table/events/index/by-day",
		"ScalableDimension": "dynamodb:index:WriteCapacityUnits",
	}
	reg := map[string]any{"MinCapacity": 1, "MaxCapacity": 4}
	for k, v := range target {
		reg[k] = v
	}
	aasOK(t, ts, "RegisterScalableTarget", reg)
	pol := map[string]any{
		"PolicyName": "w", "PolicyType": "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{
			"TargetValue": 60,
			"CustomizedMetricSpecification": map[string]any{
				"MetricName": "ConsumedWriteCapacityUnits", "Namespace": "AWS/DynamoDB", "Statistic": "Sum",
				"Dimensions": []map[string]string{{"Name": "TableName", "Value": "events"}},
			},
		},
	}
	for k, v := range target {
		pol[k] = v
	}
	aasOK(t, ts, "PutScalingPolicy", pol)
	require.Len(t, aasAlarmNames(t, ts), 4)
	sched := map[string]any{"ScheduledActionName": "s", "Schedule": "rate(1 hour)"}
	for k, v := range target {
		sched[k] = v
	}
	aasOK(t, ts, "PutScheduledAction", sched)

	aasOK(t, ts, "DeregisterScalableTarget", target)
	assert.Empty(t, aasAlarmNames(t, ts))
	assert.Empty(t, aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "dynamodb"})["ScalingPolicies"])
	assert.Empty(t, aasOK(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb"})["ScheduledActions"])

	// Re-registering it is a new target, with a new ARN and no policies.
	aasOK(t, ts, "RegisterScalableTarget", reg)
	assert.Empty(t, aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "dynamodb"})["ScalingPolicies"])
}

// TestApplicationAutoScaling_Refusals pins each refusal's code and status.
func TestApplicationAutoScaling_Refusals(t *testing.T) {
	ts := emulator.StartTestServer(t)
	aasCreateTable(t, ts, "t1", "")
	aasOK(t, ts, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace": "dynamodb", "ResourceId": "table/t1",
		"ScalableDimension": "dynamodb:table:ReadCapacityUnits", "MinCapacity": 1, "MaxCapacity": 5,
	})
	ddb := func(extra map[string]any) map[string]any {
		m := map[string]any{
			"ServiceNamespace": "dynamodb", "ResourceId": "table/t1", "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	tt := map[string]any{"TargetValue": 50, "PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "DynamoDBReadCapacityUtilization"}}
	cases := []struct {
		name, op string
		body     map[string]any
		code     string
		message  string
	}{
		{"unknown namespace", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "s3", "ResourceId": "x", "ScalableDimension": "dynamodb:table:ReadCapacityUnits", "MinCapacity": 1, "MaxCapacity": 2}, "ValidationException", ""},
		{"no namespace", "DescribeScalableTargets", map[string]any{}, "ValidationException", ""},
		{"dimension of another namespace", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "ecs", "ResourceId": "table/t1", "ScalableDimension": "dynamodb:table:ReadCapacityUnits", "MinCapacity": 1, "MaxCapacity": 2}, "ValidationException", ""},
		{"unknown dimension", "RegisterScalableTarget", ddb(map[string]any{"ScalableDimension": "dynamodb:table:Nope", "MinCapacity": 1, "MaxCapacity": 2}), "ValidationException", ""},
		{"index ID on a table dimension", "RegisterScalableTarget", ddb(map[string]any{"ResourceId": "table/t1/index/i", "MinCapacity": 1, "MaxCapacity": 2}), "ValidationException", ""},
		{"ECS ID form", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "ecs", "ResourceId": "cluster/x", "ScalableDimension": "ecs:service:DesiredCount", "MinCapacity": 1, "MaxCapacity": 2}, "ValidationException", ""},
		{"no resource ID", "RegisterScalableTarget", ddb(map[string]any{"ResourceId": "", "MinCapacity": 1, "MaxCapacity": 2}), "ValidationException", ""},
		{"new target without capacities", "RegisterScalableTarget", ddb(map[string]any{"ResourceId": "table/other"}), "ValidationException", ""},
		{"min above max", "RegisterScalableTarget", ddb(map[string]any{"MinCapacity": 9}), "ValidationException", ""},
		{"negative capacity", "RegisterScalableTarget", ddb(map[string]any{"MinCapacity": -1}), "ValidationException", ""},
		{"missing table", "RegisterScalableTarget", ddb(map[string]any{"ResourceId": "table/missing", "MinCapacity": 1, "MaxCapacity": 2}), "ValidationException", "DynamoDB resource doesn't exist: table/missing"},
		{"missing index", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "dynamodb", "ResourceId": "table/t1/index/none", "ScalableDimension": "dynamodb:index:ReadCapacityUnits", "MinCapacity": 1, "MaxCapacity": 2}, "ValidationException", ""},
		{"missing ECS service", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "ecs", "ResourceId": "service/c/s", "ScalableDimension": "ecs:service:DesiredCount", "MinCapacity": 0, "MaxCapacity": 2}, "ValidationException", "ECS service doesn't exist: service/c/s"},
		{"EMR without a role", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "elasticmapreduce", "ResourceId": "instancegroup/j-1/ig-1", "ScalableDimension": "elasticmapreduce:instancegroup:InstanceCount", "MinCapacity": 0, "MaxCapacity": 2}, "ValidationException", ""},
		{"reserved tag key", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "r", "ScalableDimension": "custom-resource:ResourceType:Property", "MinCapacity": 0, "MaxCapacity": 2, "Tags": map[string]string{"aws:x": "y"}}, "ValidationException", ""},
		{"policy on an unregistered target", "PutScalingPolicy", map[string]any{"PolicyName": "p", "PolicyType": "TargetTrackingScaling", "ServiceNamespace": "dynamodb", "ResourceId": "table/none", "ScalableDimension": "dynamodb:table:ReadCapacityUnits", "TargetTrackingScalingPolicyConfiguration": tt},
			"ObjectNotFoundException", "No scalable target registered for service namespace: dynamodb, resource ID: table/none, scalable dimension: dynamodb:table:ReadCapacityUnits"},
		{"step scaling on DynamoDB", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "PolicyType": "StepScaling", "StepScalingPolicyConfiguration": map[string]any{"AdjustmentType": "ChangeInCapacity"}}), "ValidationException", ""},
		{"predictive scaling on DynamoDB", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "PolicyType": "PredictiveScaling", "PredictiveScalingPolicyConfiguration": map[string]any{}}), "ValidationException", ""},
		{"no policy type", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p"}), "ValidationException", ""},
		{"unknown policy type", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "PolicyType": "Magic"}), "ValidationException", ""},
		{"tracking without its configuration", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "PolicyType": "TargetTrackingScaling"}), "ValidationException", ""},
		{"tracking without a target value", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "PolicyType": "TargetTrackingScaling", "TargetTrackingScalingPolicyConfiguration": map[string]any{"PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "DynamoDBReadCapacityUtilization"}}}), "ValidationException", ""},
		{"tracking without a metric", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "PolicyType": "TargetTrackingScaling", "TargetTrackingScalingPolicyConfiguration": map[string]any{"TargetValue": 5}}), "ValidationException", ""},
		{"no policy name", "PutScalingPolicy", ddb(map[string]any{"PolicyType": "TargetTrackingScaling"}), "ValidationException", ""},
		{"unprintable policy name", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "a\nb"}), "ValidationException", ""},
		{"delete an unknown scheduled action", "DeleteScheduledAction", ddb(map[string]any{"ScheduledActionName": "x"}), "ObjectNotFoundException", ""},
		{"scheduled action without a schedule", "PutScheduledAction", ddb(map[string]any{"ScheduledActionName": "x"}), "ValidationException", ""},
		{"bad schedule", "PutScheduledAction", ddb(map[string]any{"ScheduledActionName": "x", "Schedule": "every day"}), "ValidationException", ""},
		{"scheduled min above max", "PutScheduledAction", ddb(map[string]any{"ScheduledActionName": "x", "Schedule": "rate(1 day)", "ScalableTargetAction": map[string]int{"MinCapacity": 5, "MaxCapacity": 1}}), "ValidationException", ""},
		{"scheduled action on an unregistered target", "PutScheduledAction", ddb(map[string]any{"ResourceId": "table/none", "ScheduledActionName": "x", "Schedule": "rate(1 day)"}), "ObjectNotFoundException", ""},
		{"page size", "DescribeScalableTargets", map[string]any{"ServiceNamespace": "dynamodb", "MaxResults": 51}, "ValidationException", ""},
		{"policy page size", "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "dynamodb", "MaxResults": 11}, "ValidationException", ""},
		{"foreign token", "DescribeScalableTargets", map[string]any{"ServiceNamespace": "dynamodb", "NextToken": "bm90LWEtdG9rZW4="}, "InvalidNextTokenException", "The next token supplied was invalid."},
		{"activities token", "DescribeScalingActivities", map[string]any{"ServiceNamespace": "dynamodb", "NextToken": "x"}, "InvalidNextTokenException", ""},
		{"filter dimension", "DescribeScheduledActions", map[string]any{"ServiceNamespace": "ecs", "ScalableDimension": "dynamodb:table:ReadCapacityUnits"}, "ValidationException", ""},
		{"tag a bad ARN", "TagResource", map[string]any{"ResourceARN": "arn:aws:s3:::b", "Tags": map[string]string{"a": "b"}}, "ValidationException", ""},
		{"tag an unknown target", "TagResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/0123abcd", "Tags": map[string]string{"a": "b"}}, "ResourceNotFoundException", ""},
		{"tag without tags", "TagResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/0123abcd"}, "ValidationException", ""},
		{"untag without keys", "UntagResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/0123abcd"}, "ValidationException", ""},
		{"list tags of another Region", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:eu-west-1:123456789012:scalable-target/0123abcd"}, "ResourceNotFoundException", ""},
		{"unknown operation", "GetPredictiveScalingForecast", map[string]any{}, "UnknownOperationException", ""},
		{"delete a policy without a name", "DeleteScalingPolicy", ddb(nil), "ValidationException", ""},
		{"delete a policy on a bad target", "DeleteScalingPolicy", ddb(map[string]any{"PolicyName": "p", "ServiceNamespace": "nope"}), "ValidationException", ""},
		{"delete an unknown policy", "DeleteScalingPolicy", ddb(map[string]any{"PolicyName": "none"}), "ObjectNotFoundException", ""},
		{"delete a scheduled action without a name", "DeleteScheduledAction", ddb(nil), "ValidationException", ""},
		{"delete a scheduled action on a bad target", "DeleteScheduledAction", ddb(map[string]any{"ScheduledActionName": "s", "ScalableDimension": ""}), "ValidationException", ""},
		{"deregister a bad target", "DeregisterScalableTarget", ddb(map[string]any{"ResourceId": "nope"}), "ValidationException", ""},
		{"scheduled action without a name", "PutScheduledAction", ddb(map[string]any{"Schedule": "rate(1 day)"}), "ValidationException", ""},
		{"scheduled action on a bad target", "PutScheduledAction", ddb(map[string]any{"ScheduledActionName": "s", "ResourceId": "x"}), "ValidationException", ""},
		{"long policy name", "PutScalingPolicy", ddb(map[string]any{"PolicyName": strings.Repeat("p", 257)}), "ValidationException", ""},
		{"policy on a bad target", "PutScalingPolicy", ddb(map[string]any{"PolicyName": "p", "ScalableDimension": "ecs:service:DesiredCount"}), "ValidationException", ""},
		{"long resource ID", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": strings.Repeat("r", 1601), "ScalableDimension": "custom-resource:ResourceType:Property", "MinCapacity": 0, "MaxCapacity": 1}, "ValidationException", ""},
		{"negative maximum", "RegisterScalableTarget", ddb(map[string]any{"MaxCapacity": -1}), "ValidationException", ""},
		{"no dimension", "RegisterScalableTarget", ddb(map[string]any{"ScalableDimension": ""}), "ValidationException", ""},
		{"too many resource IDs", "DescribeScalableTargets", map[string]any{"ServiceNamespace": "dynamodb", "ResourceIds": make([]string, 51)}, "ValidationException", ""},
		{"too many policy names", "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "dynamodb", "PolicyNames": make([]string, 51)}, "ValidationException", ""},
		{"too many action names", "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb", "ScheduledActionNames": make([]string, 51)}, "ValidationException", ""},
		{"scheduled page size", "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb", "MaxResults": 51}, "ValidationException", ""},
		{"activities namespace", "DescribeScalingActivities", map[string]any{"ServiceNamespace": "nope"}, "ValidationException", ""},
		{"policies token", "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "dynamodb", "NextToken": "x"}, "InvalidNextTokenException", ""},
		{"actions token", "DescribeScheduledActions", map[string]any{"ServiceNamespace": "dynamodb", "NextToken": "x"}, "InvalidNextTokenException", ""},
		{"long tag key", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "t", "ScalableDimension": "custom-resource:ResourceType:Property", "MinCapacity": 0, "MaxCapacity": 1, "Tags": map[string]string{strings.Repeat("k", 129): "v"}}, "ValidationException", ""},
		{"long tag value", "RegisterScalableTarget", map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "t", "ScalableDimension": "custom-resource:ResourceType:Property", "MinCapacity": 0, "MaxCapacity": 1, "Tags": map[string]string{"k": strings.Repeat("v", 257)}}, "ValidationException", ""},
		{"ARN with a letter account", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:12345678901x:scalable-target/abc"}, "ValidationException", ""},
		{"ARN with a bad ID", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/a_b"}, "ValidationException", ""},
		{"ARN of another resource", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:123456789012:policy/abc"}, "ValidationException", ""},
		{"no ARN", "ListTagsForResource", map[string]any{}, "ValidationException", ""},
		{"untag an unknown target", "UntagResource", map[string]any{"ResourceARN": "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/abc", "TagKeys": []string{"a"}}, "ResourceNotFoundException", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, doc, raw := aasCall(t, ts, tc.op, tc.body)
			assert.NotEqual(t, http.StatusOK, code, "%s", raw)
			assert.Equal(t, tc.code, aasErrorCode(doc), "%s", raw)
			if tc.message != "" {
				assert.Equal(t, tc.message, doc["message"], "%s", raw)
			}
		})
	}

	code, doc, raw := aasCall(t, ts, "DescribeScalingActivities", map[string]any{"ServiceNamespace": "dynamodb"})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.Equal(t, []any{}, doc["ScalingActivities"])
}

// TestApplicationAutoScaling_PagingAndFilters pages DescribeScalableTargets and
// DescribeScalingPolicies with a cursor token.
func TestApplicationAutoScaling_PagingAndFilters(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for _, id := range []string{"r-c", "r-a", "r-b"} {
		aasOK(t, ts, "RegisterScalableTarget", map[string]any{
			"ServiceNamespace": "custom-resource", "ResourceId": id,
			"ScalableDimension": "custom-resource:ResourceType:Property", "MinCapacity": 0, "MaxCapacity": 3,
			"RoleARN": "arn:aws:iam::123456789012:role/custom",
		})
		aasOK(t, ts, "PutScalingPolicy", map[string]any{
			"ServiceNamespace": "custom-resource", "ResourceId": id,
			"ScalableDimension": "custom-resource:ResourceType:Property", "PolicyName": "step",
			"PolicyType": "StepScaling", "StepScalingPolicyConfiguration": map[string]any{
				"AdjustmentType": "ChangeInCapacity", "StepAdjustments": []map[string]any{{"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1}},
			},
		})
	}
	first := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource", "MaxResults": 2})
	page := first["ScalableTargets"].([]any)
	require.Len(t, page, 2)
	assert.Equal(t, "r-a", page[0].(map[string]any)["ResourceId"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/custom", page[0].(map[string]any)["RoleARN"])
	token, _ := first["NextToken"].(string)
	require.NotEmpty(t, token)
	second := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource", "MaxResults": 2, "NextToken": token})
	require.Len(t, second["ScalableTargets"], 1)
	assert.Equal(t, "r-c", second["ScalableTargets"].([]any)[0].(map[string]any)["ResourceId"])
	assert.NotContains(t, second, "NextToken")

	code, doc, _ := aasCall(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs", "NextToken": token})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidNextTokenException", aasErrorCode(doc), "a token is refused for another filter")

	pols := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource", "MaxResults": 2})
	require.Len(t, pols["ScalingPolicies"], 2)
	p := pols["ScalingPolicies"].([]any)[0].(map[string]any)
	assert.Equal(t, []any{}, p["Alarms"], "a step policy created no alarms")
	assert.Contains(t, p, "StepScalingPolicyConfiguration")
	rest := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource", "MaxResults": 2, "NextToken": pols["NextToken"]})
	require.Len(t, rest["ScalingPolicies"], 1)

	// A step policy changed to target tracking gains alarms, and changed back loses them.
	tracking := map[string]any{
		"ServiceNamespace": "custom-resource", "ResourceId": "r-a",
		"ScalableDimension": "custom-resource:ResourceType:Property", "PolicyName": "step",
		"PolicyType": "TargetTrackingScaling", "TargetTrackingScalingPolicyConfiguration": map[string]any{
			"TargetValue": 1, "PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "CustomMetric"},
		},
	}
	require.Len(t, aasOK(t, ts, "PutScalingPolicy", tracking)["Alarms"], 2)
	require.Len(t, aasAlarmNames(t, ts), 2)
	tracking["PolicyType"] = "StepScaling"
	tracking["StepScalingPolicyConfiguration"] = map[string]any{"AdjustmentType": "ChangeInCapacity"}
	assert.NotContains(t, aasOK(t, ts, "PutScalingPolicy", tracking), "Alarms")
	assert.Empty(t, aasAlarmNames(t, ts))
}

// TestApplicationAutoScaling_TagsAndTheTaggingAPI pins the three tag operations, the tagging API's
// view of a target, and that no response carries the record's bookkeeping.
func TestApplicationAutoScaling_TagsAndTheTaggingAPI(t *testing.T) {
	ts := emulator.StartTestServer(t)
	reg := map[string]any{
		"ServiceNamespace": "custom-resource", "ResourceId": "r",
		"ScalableDimension": "custom-resource:ResourceType:Property", "MinCapacity": 0, "MaxCapacity": 3,
	}
	arn := aasOK(t, ts, "RegisterScalableTarget", reg)["ScalableTargetARN"].(string)

	aasOK(t, ts, "TagResource", map[string]any{"ResourceARN": arn, "Tags": map[string]string{"team": "a", "tier": "1"}})
	aasOK(t, ts, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"tier"}})
	assert.Equal(t, map[string]any{"team": "a"}, aasOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": arn})["Tags"])

	many := map[string]string{}
	for i := range 50 {
		many["k"+string(rune('A'+i%26))+string(rune('a'+i/26))] = "v"
	}
	code, doc, _ := aasCall(t, ts, "TagResource", map[string]any{"ResourceARN": arn, "Tags": many})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "TooManyTagsException", aasErrorCode(doc))

	// The tagging API sees the target and writes its tags through the plugin.
	code, tagDoc, raw := jsonTargetCall(t, ts, "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{arn}, "Tags": map[string]string{"cost": "x"},
	})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.Empty(t, tagDoc["FailedResourcesMap"])
	assert.Equal(t, map[string]any{"team": "a", "cost": "x"}, aasOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": arn})["Tags"])

	code, _, raw = jsonTargetCall(t, ts, "ResourceGroupsTaggingAPI_20170126.GetResources", map[string]any{
		"ResourceTypeFilters": []string{"application-autoscaling"},
	})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.Contains(t, string(raw), arn)

	// Untagged entirely, it is still reported, as a previously tagged resource.
	aasOK(t, ts, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"team", "cost"}})
	_, _, raw = jsonTargetCall(t, ts, "ResourceGroupsTaggingAPI_20170126.GetResources", map[string]any{})
	assert.Contains(t, string(raw), arn)

	// An unknown target ARN fails in the tagging API rather than writing anything.
	missing := "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/ffff"
	_, tagDoc, raw = jsonTargetCall(t, ts, "ResourceGroupsTaggingAPI_20170126.TagResources", map[string]any{
		"ResourceARNList": []string{missing}, "Tags": map[string]string{"a": "b"},
	})
	assert.Contains(t, tagDoc["FailedResourcesMap"], missing, "%s", raw)
}

// TestApplicationAutoScaling_ResponsesCarryNoBookkeepingMember asserts on the raw bytes of every
// response that renders a target, after the target has been tagged and untagged so its record holds
// the previously-tagged flag, and holds a policy and a scheduled action.
func TestApplicationAutoScaling_ResponsesCarryNoBookkeepingMember(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ref := map[string]any{
		"ServiceNamespace": "custom-resource", "ResourceId": "r",
		"ScalableDimension": "custom-resource:ResourceType:Property",
	}
	reg := map[string]any{"MinCapacity": 0, "MaxCapacity": 3}
	for k, v := range ref {
		reg[k] = v
	}
	arn := aasOK(t, ts, "RegisterScalableTarget", reg)["ScalableTargetARN"].(string)
	aasOK(t, ts, "TagResource", map[string]any{"ResourceARN": arn, "Tags": map[string]string{"a": "b"}})
	aasOK(t, ts, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"a"}})
	pol := map[string]any{"PolicyName": "p", "PolicyType": "StepScaling", "StepScalingPolicyConfiguration": map[string]any{"AdjustmentType": "ChangeInCapacity"}}
	sched := map[string]any{"ScheduledActionName": "s", "Schedule": "at(2030-01-01T00:00:00)"}
	for k, v := range ref {
		pol[k], sched[k] = v, v
	}
	aasOK(t, ts, "PutScalingPolicy", pol)
	aasOK(t, ts, "PutScheduledAction", sched)

	for op, absent := range map[string][]string{
		"DescribeScalableTargets":  {"ever_tagged", "Tags", "Policies", "ScheduledActions"},
		"DescribeScalingPolicies":  {"ever_tagged", "Tags", "ScheduledActions", "ScalableTargetARN"},
		"DescribeScheduledActions": {"ever_tagged", "Tags", "Policies", "ScalableTargetARN"},
	} {
		_, _, raw := aasCall(t, ts, op, map[string]any{"ServiceNamespace": "custom-resource"})
		require.Contains(t, string(raw), `"ResourceId":"r"`, "%s answered the record: %s", op, raw)
		for _, member := range absent {
			assert.NotContains(t, string(raw), `"`+member+`"`, "%s: %s", op, raw)
		}
	}
	_, _, raw := aasCall(t, ts, "RegisterScalableTarget", reg)
	assert.Equal(t, `{"ScalableTargetARN":"`+arn+`"}`, string(raw))
}

// TestApplicationAutoScaling_RoutesOnTheHostAndSigningName reaches the plugin by its endpoint host,
// without a target prefix the alias table maps, and by a SigV4 credential scope.
func TestApplicationAutoScaling_RoutesOnTheHostAndSigningName(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := sjJSON(t, ts, "application-autoscaling.us-east-1.amazonaws.com", "AnyScaleFrontendService.DescribeScalableTargets",
		map[string]string{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, code, "%s", body)
	assert.JSONEq(t, `{"ScalableTargets":[]}`, string(body))
}

// TestApplicationAutoScaling_ECSServiceTarget registers an ECS service once ECS knows it, and maps
// its CPU tracking policy to the service's AWS/ECS alarms.
func TestApplicationAutoScaling_ECSServiceTarget(t *testing.T) {
	ts := emulator.StartTestServer(t)
	target := map[string]any{
		"ServiceNamespace": "ecs", "ResourceId": "service/web/api", "ScalableDimension": "ecs:service:DesiredCount",
	}
	reg := map[string]any{"MinCapacity": 0, "MaxCapacity": 4}
	for k, v := range target {
		reg[k] = v
	}
	code, doc, _ := aasCall(t, ts, "RegisterScalableTarget", reg)
	require.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ECS service doesn't exist: service/web/api", doc["message"])

	code, _, raw := jsonTargetCall(t, ts, "AmazonEC2ContainerServiceV20141113.CreateService", map[string]any{
		"cluster": "web", "serviceName": "api", "taskDefinition": "api:1", "desiredCount": 1,
	})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	aasOK(t, ts, "RegisterScalableTarget", reg)
	got := aasOK(t, ts, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs"})["ScalableTargets"].([]any)[0].(map[string]any)
	assert.Contains(t, got["RoleARN"], "ecs.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_ECSService")

	pol := map[string]any{
		"PolicyName": "cpu", "PolicyType": "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{
			"TargetValue": 75.0, "ScaleInCooldown": 60, "ScaleOutCooldown": 60,
			"PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "ECSServiceAverageCPUUtilization"},
		},
	}
	for k, v := range target {
		pol[k] = v
	}
	alarms := aasOK(t, ts, "PutScalingPolicy", pol)["Alarms"].([]any)
	require.Len(t, alarms, 2)
	_, body := singleAccountQuery(t, ts, "monitoring", "", map[string]string{"Action": "DescribeAlarms", "Version": "2010-08-01"})
	for _, want := range []string{"<Namespace>AWS/ECS</Namespace>", "<MetricName>CPUUtilization</MetricName>",
		"<Value>web</Value>", "<Value>api</Value>", "<Threshold>75", "<ComparisonOperator>LessThanThreshold</ComparisonOperator>"} {
		assert.Contains(t, body, want)
	}

	// A predictive policy is ECS's alone.
	pred := map[string]any{"PolicyName": "forecast", "PolicyType": "PredictiveScaling",
		"PredictiveScalingPolicyConfiguration": map[string]any{"MetricSpecifications": []any{}}}
	for k, v := range target {
		pred[k] = v
	}
	assert.NotContains(t, aasOK(t, ts, "PutScalingPolicy", pred), "Alarms")
	pols := aasOK(t, ts, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "ecs", "PolicyNames": []string{"forecast"}})["ScalingPolicies"].([]any)
	require.Len(t, pols, 1)
	assert.Contains(t, pols[0], "PredictiveScalingPolicyConfiguration")
}

// TestApplicationAutoScaling_MalformedBodies pins the refusal of a body that is not JSON, and that
// an empty body is read as the empty object.
func TestApplicationAutoScaling_MalformedBodies(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for _, op := range []string{
		"RegisterScalableTarget", "DescribeScalableTargets", "DeregisterScalableTarget", "PutScalingPolicy",
		"DescribeScalingPolicies", "DeleteScalingPolicy", "PutScheduledAction", "DescribeScheduledActions",
		"DeleteScheduledAction", "DescribeScalingActivities", "TagResource", "UntagResource", "ListTagsForResource",
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader("{not json"))
		require.NoError(t, err)
		req.Header.Set("X-Amz-Target", "AnyScaleFrontendService."+op)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		code, raw := sjDo(t, req)
		assert.Equal(t, http.StatusBadRequest, code, "%s: %s", op, raw)
		assert.Contains(t, string(raw), "SerializationException", op)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "AnyScaleFrontendService.DescribeScalingActivities")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	code, raw := sjDo(t, req)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, string(raw), "ValidationException", "an empty body names no namespace")
}

// TestApplicationAutoScaling_ScheduledActionWindow pins StartTime and EndTime round-tripping as
// epoch seconds, and an update that changes only the schedule.
func TestApplicationAutoScaling_ScheduledActionWindow(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ref := map[string]any{
		"ServiceNamespace": "lambda", "ResourceId": "function:f:live",
		"ScalableDimension": "lambda:function:ProvisionedConcurrency",
	}
	reg := map[string]any{"MinCapacity": 1, "MaxCapacity": 10}
	for k, v := range ref {
		reg[k] = v
	}
	aasOK(t, ts, "RegisterScalableTarget", reg)
	sched := map[string]any{
		"ScheduledActionName": "burst", "Schedule": "at(2031-06-01T09:00:00)",
		"StartTime": 1906000000, "EndTime": 1907000000,
	}
	for k, v := range ref {
		sched[k] = v
	}
	aasOK(t, ts, "PutScheduledAction", sched)
	sched["Schedule"] = "rate(5 minutes)"
	aasOK(t, ts, "PutScheduledAction", sched)
	a := aasOK(t, ts, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": "lambda", "ResourceId": "function:f:live", "MaxResults": 1,
	})["ScheduledActions"].([]any)[0].(map[string]any)
	assert.Equal(t, "rate(5 minutes)", a["Schedule"])
	assert.EqualValues(t, 1906000000, a["StartTime"])
	assert.EqualValues(t, 1907000000, a["EndTime"])
	assert.NotContains(t, a, "Timezone")
	assert.NotContains(t, a, "ScalableTargetAction")

	code, doc, _ := aasCall(t, ts, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "lambda", "MaxResults": 0})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationException", aasErrorCode(doc))
	code, doc, _ = aasCall(t, ts, "DescribeScalingActivities", map[string]any{"ServiceNamespace": "lambda", "MaxResults": 99})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationException", aasErrorCode(doc))
}
