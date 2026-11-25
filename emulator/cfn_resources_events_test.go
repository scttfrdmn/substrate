package emulator_test

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// AWS::Events::EventBus, and AWS::Events::Rule on the bus its EventBusName names (#1613).

// cfnEventsBus declares the orders bus with a tag, and optionally a policy and a LogConfig.
func cfnEventsBus(description, tagKey string, withPolicy bool) string {
	extra := ""
	if withPolicy {
		extra = `,"Policy":{"Version":"2012-10-17","Statement":[{"Sid":"all","Effect":"Allow","Principal":"*",` +
			`"Action":"events:PutEvents","Resource":"*"}]},"LogConfig":{"IncludeDetail":"FULL","Level":"INFO"}`
	}
	return `"Bus":{"Type":"AWS::Events::EventBus","Properties":{"Name":"orders","Description":"` + description + `",
		"Tags":[{"Key":"` + tagKey + `","Value":"v"}]` + extra + `}}`
}

// cfnEventsQueue is the ARN of a queue a rule target names; the target is not invoked.
func cfnEventsQueue(id string) string { return "arn:aws:sqs:us-east-1:123456789012:" + id }

// cfnEventsTemplate is the given resource declarations, an audit bus, and a rule on the bus ruleBus
// names by Ref, tagged team=ruleTag, with one target per id naming the queue of that name.
func cfnEventsTemplate(extra, ruleBus, ruleTag string, targetIDs ...string) string {
	if extra != "" {
		extra += ","
	}
	targets := make([]map[string]string, 0, len(targetIDs))
	for _, id := range targetIDs {
		targets = append(targets, map[string]string{"Id": id, "Arn": cfnEventsQueue(id)})
	}
	raw, err := json.Marshal(targets)
	if err != nil {
		panic(err)
	}
	return `{"Resources":{` + extra + `
		"Audit":{"Type":"AWS::Events::EventBus","Properties":{"Name":"audit"}},
		"Rule":{"Type":"AWS::Events::Rule","Properties":{"Name":"on-orders","EventBusName":{"Ref":"` + ruleBus + `"},
			"EventPattern":{"source":["orders"]},"Tags":[{"Key":"team","Value":"` + ruleTag + `"}],
			"Targets":` + string(raw) + `}}
	},"Outputs":{"RuleArn":{"Value":{"Fn::GetAtt":["Rule","Arn"]}}}}`
}

// cfnEventsTargetsOn returns the target Ids of the on-orders rule on bus, sorted.
func cfnEventsTargetsOn(t *testing.T, d *emulator.StackDeployer, bus string) []string {
	t.Helper()
	out := cfnStubDispatch(t, d, "eventbridge", "ListTargetsByRule", map[string]any{"Rule": "on-orders", "EventBusName": bus})
	targets, _ := out["Targets"].([]any)
	ids := make([]string, 0, len(targets))
	for _, tg := range targets {
		ids = append(ids, tg.(map[string]any)["Id"].(string))
	}
	sort.Strings(ids)
	return ids
}

// cfnEventsPutOutOfBand adds a target to the on-orders rule on bus outside the stack.
func cfnEventsPutOutOfBand(t *testing.T, d *emulator.StackDeployer, bus string) {
	t.Helper()
	cfnStubDispatch(t, d, "eventbridge", "PutTargets", map[string]any{"Rule": "on-orders", "EventBusName": bus,
		"Targets": []map[string]string{{"Id": "oob", "Arn": cfnEventsQueue("oob")}}})
}

// cfnEventsRemoveOutOfBand removes the out-of-band target, which DeleteStack does not manage.
func cfnEventsRemoveOutOfBand(t *testing.T, d *emulator.StackDeployer, bus string) {
	t.Helper()
	cfnStubDispatch(t, d, "eventbridge", "RemoveTargets", map[string]any{"Rule": "on-orders", "EventBusName": bus, "Ids": []string{"oob"}})
}

// cfnEventsAssertNothingLeft checks that only the default bus remains and it holds no rule.
func cfnEventsAssertNothingLeft(t *testing.T, d *emulator.StackDeployer) {
	t.Helper()
	assert.Equal(t, []any{map[string]any{"Name": "default", "Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default"}},
		cfnStubDispatch(t, d, "eventbridge", "ListEventBuses", map[string]any{})["EventBuses"], "DeleteStack leaves no bus")
	assert.Empty(t, cfnStubDispatch(t, d, "eventbridge", "ListRules", map[string]any{})["Rules"], "nor a rule")
}

// cfnEventsRuleOn answers whether a rule named on-orders exists on bus.
func cfnEventsRuleOn(t *testing.T, d *emulator.StackDeployer, bus string) bool {
	t.Helper()
	_, err := cfnRawDispatch(d, &emulator.AWSRequest{Service: "eventbridge", Operation: "DescribeRule",
		Body: []byte(`{"Name":"on-orders","EventBusName":"` + bus + `"}`)})
	return err == nil
}

// cfnEventsTagsOf returns the tags ListTagsForResource answers for arn.
func cfnEventsTagsOf(t *testing.T, d *emulator.StackDeployer, arn string) any {
	t.Helper()
	return cfnStubDispatch(t, d, "eventbridge", "ListTagsForResource", map[string]any{"ResourceARN": arn})["Tags"]
}

func TestCFN_EventsBusAndRuleOnIt(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const ordersARN = "arn:aws:events:us-east-1:123456789012:event-bus/orders"
	created, err := d.Deploy(ctx, cfnEventsTemplate(cfnEventsBus("first", "team", true), "Bus", "rules", "q1"), "events-stack", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", created.Status, created.StatusReason)
	bus := resourceByLogicalID(t, created, "Bus")
	rule := resourceByLogicalID(t, created, "Rule")
	require.Empty(t, bus.Error)
	require.Empty(t, rule.Error)
	assert.Equal(t, "orders", bus.PhysicalID)
	assert.Equal(t, ordersARN, bus.ARN)
	assert.Equal(t, "arn:aws:events:us-east-1:123456789012:rule/orders/on-orders", rule.ARN)
	assert.Equal(t, map[string]string{"RuleArn": rule.ARN}, created.Outputs)

	described := cfnStubDispatch(t, d, "eventbridge", "DescribeEventBus", map[string]any{"Name": "orders"})
	assert.Equal(t, "first", described["Description"])
	assert.Contains(t, described["Policy"], `"Sid":"all"`)
	assert.Contains(t, described, "LogConfig")
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "v"}}, cfnEventsTagsOf(t, d, bus.ARN))
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "rules"}}, cfnEventsTagsOf(t, d, rule.ARN))
	assert.Equal(t, []string{"q1"}, cfnEventsTargetsOn(t, d, "orders"), "the declared target is put")
	cfnEventsPutOutOfBand(t, d, "orders")

	// Every bus property but Name updates in place, and a dropped Policy or LogConfig is cleared. The
	// rule moves to the audit bus and is gone from orders, with the tags and targets its new
	// declaration names and the target added outside the stack; q1, which the declaration dropped,
	// is not carried over.
	updated, err := d.UpdateStack(ctx, cfnEventsTemplate(cfnEventsBus("second", "owner", false), "Audit", "moved", "q2"),
		"events-stack", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, "orders", resourceByLogicalID(t, updated, "Bus").PhysicalID)
	described = cfnStubDispatch(t, d, "eventbridge", "DescribeEventBus", map[string]any{"Name": "orders"})
	assert.Equal(t, "second", described["Description"])
	assert.NotContains(t, described, "Policy", "a dropped Policy is removed")
	assert.NotContains(t, described, "LogConfig", "a dropped LogConfig is cleared")
	assert.Equal(t, []any{map[string]any{"Key": "owner", "Value": "v"}}, cfnEventsTagsOf(t, d, bus.ARN))
	moved := resourceByLogicalID(t, updated, "Rule")
	assert.Equal(t, "arn:aws:events:us-east-1:123456789012:rule/audit/on-orders", moved.ARN)
	assert.True(t, cfnEventsRuleOn(t, d, "audit"))
	assert.False(t, cfnEventsRuleOn(t, d, "orders"), "the rule is deleted from the bus it left")
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "moved"}}, cfnEventsTagsOf(t, d, moved.ARN))
	assert.Equal(t, []string{"oob", "q2"}, cfnEventsTargetsOn(t, d, "audit"))

	// The orders bus, holding no rule now, deletes cleanly when the template drops it. On a rule
	// that stays on its bus a tag change is applied and a dropped target removed.
	updated, err = d.UpdateStack(ctx, cfnEventsTemplate("", "Audit", "final", "q3"), "events-stack", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	_, err = cfnRawDispatch(d, &emulator.AWSRequest{Service: "eventbridge", Operation: "DescribeEventBus", Body: []byte(`{"Name":"orders"}`)})
	require.Error(t, err, "the dropped bus is deleted")
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "final"}}, cfnEventsTagsOf(t, d, moved.ARN))
	assert.Equal(t, []string{"oob", "q3"}, cfnEventsTargetsOn(t, d, "audit"))

	// The stack's delete removes the targets it declared before the rule; one added outside the
	// stack still blocks DeleteRule, as it would at AWS, so it is removed here first.
	cfnEventsRemoveOutOfBand(t, d, "audit")
	require.NoError(t, d.DeleteStack(ctx, "events-stack"))
	cfnEventsAssertNothingLeft(t, d)
}

// TestCFN_EventsRuleMoveRollsBack moves a rule, with a declared target and one added outside the
// stack, to another bus in an update a later resource fails. The rollback moves it back: it is on
// its original bus with both targets, and gone from the bus the failed update moved it to.
func TestCFN_EventsRuleMoveRollsBack(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	orders := `"Bus":{"Type":"AWS::Events::EventBus","Properties":{"Name":"orders"}}`
	created, err := d.Deploy(ctx, cfnEventsTemplate(orders, "Bus", "rules", "q1"), "events-rollback", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", created.Status, created.StatusReason)
	cfnEventsPutOutOfBand(t, d, "orders")

	failing := orders + `,"Refused":{"Type":"AWS::S3::Bucket","DependsOn":"Rule","Properties":{"BucketName":"no"}}`
	result, err := d.UpdateStack(ctx, cfnEventsTemplate(failing, "Audit", "rules", "q1"), "events-rollback", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)

	assert.True(t, cfnEventsRuleOn(t, d, "orders"), "the rollback moves the rule back")
	assert.False(t, cfnEventsRuleOn(t, d, "audit"), "and deletes it from the bus the failed update moved it to")
	assert.Equal(t, []string{"oob", "q1"}, cfnEventsTargetsOn(t, d, "orders"))
	assert.Equal(t, "arn:aws:events:us-east-1:123456789012:rule/orders/on-orders", resourceByLogicalID(t, result, "Rule").ARN)

	cfnEventsRemoveOutOfBand(t, d, "orders")
	require.NoError(t, d.DeleteStack(ctx, "events-rollback"))
	cfnEventsAssertNothingLeft(t, d)
}

// TestCFN_EventsRuleMoveIsUndoneWhenAStepFails drives the last steps of a move with a failing step,
// and checks that the copy on the new bus is deleted and the old rule is as it was.
func TestCFN_EventsRuleMoveIsUndoneWhenAStepFails(t *testing.T) {
	setup := func(t *testing.T) *emulator.StackDeployer {
		t.Helper()
		d, _, _, _ := newSweepDeployer(t)
		for _, bus := range []string{"orders", "audit"} {
			cfnStubDispatch(t, d, "eventbridge", "CreateEventBus", map[string]any{"Name": bus})
			cfnStubDispatch(t, d, "eventbridge", "PutRule", map[string]any{"Name": "on-orders", "EventBusName": bus, "ScheduleExpression": "rate(1 hour)"})
		}
		cfnStubDispatch(t, d, "eventbridge", "PutTargets", map[string]any{"Rule": "on-orders", "EventBusName": "orders",
			"Targets": []map[string]string{{"Id": "a", "Arn": cfnEventsQueue("a")}, {"Id": "b", "Arn": cfnEventsQueue("b")}}})
		return d
	}
	t.Run("the old rule cannot be deleted", func(t *testing.T) {
		d := setup(t)
		// Only a is named as live, so b is left on the old rule and DeleteRule refuses it.
		err := d.MoveEventsRuleForTest("on-orders", "orders", "audit", []emulator.EBTarget{{Id: "a", ARN: cfnEventsQueue("a")}}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "move rule on-orders from bus orders to audit")
		assert.False(t, cfnEventsRuleOn(t, d, "audit"), "the copy is deleted")
		assert.Equal(t, []string{"a", "b"}, cfnEventsTargetsOn(t, d, "orders"), "the old rule's removed target is restored")
	})
	t.Run("the old rule is gone", func(t *testing.T) {
		d := setup(t)
		err := d.MoveEventsRuleForTest("on-orders", "absent", "audit", []emulator.EBTarget{{Id: "a", ARN: cfnEventsQueue("a")}}, nil)
		require.Error(t, err)
		assert.False(t, cfnEventsRuleOn(t, d, "audit"), "the copy is deleted")
		assert.Equal(t, []string{"a", "b"}, cfnEventsTargetsOn(t, d, "orders"))
	})
}

// TestCFN_EventsRefusalsFailTheResource checks that a refusal from EventBridge while a stack
// creates or updates a bus or rule fails that resource and rolls the stack back.
func TestCFN_EventsRefusalsFailTheResource(t *testing.T) {
	ctx := context.Background()
	t.Run("a bus policy that is not an object", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		result, err := d.Deploy(ctx, `{"Resources":{"Bus":{"Type":"AWS::Events::EventBus","Properties":{"Name":"orders","Policy":"[]"}}}}`, "bad-policy", nil)
		require.NoError(t, err)
		assert.Equal(t, "ROLLBACK_COMPLETE", result.Status, result.StatusReason)
	})
	update := func(t *testing.T, bad string) *emulator.DeployResult {
		t.Helper()
		d, _, _, _ := newSweepDeployer(t)
		_, err := d.Deploy(ctx, `{"Resources":{"Bus":{"Type":"AWS::Events::EventBus","Properties":{"Name":"orders"}}}}`, "bad-update", nil)
		require.NoError(t, err)
		result, err := d.UpdateStack(ctx, `{"Resources":{"Bus":{"Type":"AWS::Events::EventBus","Properties":{"Name":"orders",`+bad+`}}}}`,
			"bad-update", nil, emulator.CFNDeployOptions{})
		require.NoError(t, err)
		return result
	}
	for name, bad := range map[string]string{
		"an unknown log level": `"LogConfig":{"Level":"LOUD"}`,
		"a bad policy":         `"Policy":"[]"`,
		"a long tag key":       `"Tags":[{"Key":"` + strings.Repeat("k", 129) + `","Value":"v"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			result := update(t, bad)
			assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)
		})
	}
	t.Run("a rule target on a rule PutRule refused", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		result, err := d.Deploy(ctx, `{"Resources":{"Rule":{"Type":"AWS::Events::Rule","Properties":{"Name":"r","EventBusName":"absent",
			"ScheduleExpression":"rate(1 hour)","Targets":[{"Id":"q","Arn":"`+cfnEventsQueue("q")+`"}]}}}}`, "bad-rule", nil)
		require.NoError(t, err)
		assert.Equal(t, "ROLLBACK_COMPLETE", result.Status, result.StatusReason)
	})
}
