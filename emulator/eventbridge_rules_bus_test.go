package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Rules keyed per event bus (#1613): every rule operation honors EventBusName, a custom-bus rule
// has the rule/{bus}/{name} ARN, and a record written before rules were keyed per bus is still the
// default-bus rule it was.

const ebRuleARNPrefix = "arn:aws:events:us-east-1:" + taggingTestAccount + ":rule/"

// TestEBRules_ABusARNNamesItsOwnAccountAndRegion checks every operation that takes a bus ARN: one
// in another account or Region names a bus the caller cannot reach, whatever bus of that name the
// caller has, and a malformed one is refused.
func TestEBRules_ABusARNNamesItsOwnAccountAndRegion(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r", "EventBusName": "orders", "EventPattern": `{"source":["x"]}`})
	own := ebBusARNPrefix + "orders"
	for _, foreign := range []string{
		"arn:aws:events:us-east-1:210987654321:event-bus/orders",
		"arn:aws:events:us-west-2:" + taggingTestAccount + ":event-bus/orders",
	} {
		for _, tc := range []struct {
			op   string
			body map[string]any
		}{
			{"DescribeEventBus", map[string]any{"Name": foreign}},
			{"UpdateEventBus", map[string]any{"Name": foreign, "Description": "x"}},
			{"PutRule", map[string]any{"Name": "r", "EventBusName": foreign, "EventPattern": `{"source":["x"]}`}},
			{"DescribeRule", map[string]any{"Name": "r", "EventBusName": foreign}},
			{"DisableRule", map[string]any{"Name": "r", "EventBusName": foreign}},
			{"ListRules", map[string]any{"EventBusName": foreign}},
			{"ListTargetsByRule", map[string]any{"Rule": "r", "EventBusName": foreign}},
			{"ListRuleNamesByTarget", map[string]any{"TargetArn": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":q", "EventBusName": foreign}},
		} {
			ebRefused(t, ts, tc.op, tc.body, "ResourceNotFoundException")
		}
		out := ebOK(t, ts, "PutEvents", map[string]any{"Entries": []any{
			map[string]string{"Source": "s", "DetailType": "d", "Detail": "{}", "EventBusName": foreign},
			map[string]string{"Source": "s", "DetailType": "d", "Detail": "{}", "EventBusName": own},
		}})
		assert.EqualValues(t, 1, out["FailedEntryCount"], "only the foreign entry fails: %v", out)
		entries, _ := out["Entries"].([]any)
		require.Len(t, entries, 2)
		assert.Equal(t, "ResourceNotFoundException", entries[0].(map[string]any)["ErrorCode"])
	}
	assert.Equal(t, "orders", ebOK(t, ts, "DescribeRule", map[string]any{"Name": "r", "EventBusName": own})["EventBusName"],
		"the caller's own ARN still names its bus")
	ebRefused(t, ts, "DescribeEventBus", map[string]any{"Name": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":q"}, "ValidationError")
	ebRefused(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": "x", "EventBusName": "absent"}, "ResourceNotFoundException")

	// An entry naming a bus that does not exist fails on its own, as a foreign ARN's does.
	out := ebOK(t, ts, "PutEvents", map[string]any{"Entries": []any{
		map[string]string{"Source": "s", "DetailType": "d", "Detail": "{}", "EventBusName": "absent"},
		map[string]string{"Source": "s", "DetailType": "d", "Detail": "{}"},
	}})
	assert.EqualValues(t, 1, out["FailedEntryCount"], "%v", out)
	entries, _ := out["Entries"].([]any)
	require.Len(t, entries, 2)
	assert.Equal(t, map[string]any{"ErrorCode": "ResourceNotFoundException", "ErrorMessage": "Event bus absent does not exist."}, entries[0])
	assert.Contains(t, entries[1], "EventId")
}

// TestEBRules_AStoredNameNoOperationAcceptsIsNotListed: a release that did not check the name pattern
// could store a default-bus rule named "a/b", at the key that now reads as rule b on bus a. No rule
// operation accepts the name, so ListRules leaves it out rather than list a rule nothing can address.
func TestEBRules_AStoredNameNoOperationAcceptsIsNotListed(t *testing.T) {
	ts := smTagServer(t)
	ctx := context.Background()
	scope := taggingTestAccount + "/us-east-1"
	state := ts.StateManager()
	require.NoError(t, state.Put(ctx, "eventbridge", "ebrule:"+scope+"/a/b", []byte(`{"Name":"a/b","Arn":"`+ebRuleARNPrefix+`a/b","State":"ENABLED"}`)))
	require.NoError(t, state.Put(ctx, "eventbridge", "ebrule_names:"+scope, []byte(`["a/b"]`)))
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "ok", "EventPattern": `{"source":["x"]}`})
	assert.Equal(t, []string{"ok"}, ebRuleNames(t, ebOK(t, ts, "ListRules", map[string]any{})))
	ebRefused(t, ts, "DescribeRule", map[string]any{"Name": "a/b"}, "ValidationError")
}

// ebRuleNames returns the rule names ListRules answers for a request.
func ebRuleNames(t *testing.T, out map[string]any) []string {
	t.Helper()
	rules, _ := out["Rules"].([]any)
	names := make([]string, 0, len(rules))
	for _, r := range rules {
		names = append(names, r.(map[string]any)["Name"].(string))
	}
	return names
}

func TestEBRules_TwoBusesHoldRulesOfOneName(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	defaultARN := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r", "EventPattern": `{"source":["default"]}`})
	ordersARN := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r", "EventBusName": "orders", "EventPattern": `{"source":["orders"]}`})
	assert.Equal(t, ebRuleARNPrefix+"r", defaultARN)
	assert.Equal(t, ebRuleARNPrefix+"orders/r", ordersARN)

	onOrders := ebOK(t, ts, "DescribeRule", map[string]any{"Name": "r", "EventBusName": ebBusARNPrefix + "orders"})
	assert.Equal(t, `{"source":["orders"]}`, onOrders["EventPattern"])
	assert.Equal(t, "orders", onOrders["EventBusName"])
	assert.Equal(t, ordersARN, onOrders["Arn"])
	onDefault := ebOK(t, ts, "DescribeRule", map[string]any{"Name": "r"})
	assert.Equal(t, `{"source":["default"]}`, onDefault["EventPattern"])
	assert.Equal(t, "default", onDefault["EventBusName"])

	assert.Equal(t, []string{"r"}, ebRuleNames(t, ebOK(t, ts, "ListRules", map[string]any{"EventBusName": "orders"})))
	assert.Equal(t, []string{"r"}, ebRuleNames(t, ebOK(t, ts, "ListRules", map[string]any{})))
	ebRefused(t, ts, "ListRules", map[string]any{"EventBusName": "absent"}, "ResourceNotFoundException")

	target := map[string]string{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":q"}
	ebOK(t, ts, "PutTargets", map[string]any{"Rule": "r", "EventBusName": "orders", "Targets": []any{target}})
	assert.Len(t, ebOK(t, ts, "ListTargetsByRule", map[string]any{"Rule": "r", "EventBusName": "orders"})["Targets"], 1)
	assert.Empty(t, ebOK(t, ts, "ListTargetsByRule", map[string]any{"Rule": "r"})["Targets"], "targets belong to one bus's rule")

	ebOK(t, ts, "TagResource", map[string]any{"ResourceARN": ordersARN, "Tags": ebTagList("bus", "orders")})
	assert.Empty(t, ebTags(t, ts, defaultARN))

	// The default-bus rule deletes without touching its namesake, whose targets still hold.
	ebOK(t, ts, "DeleteRule", map[string]any{"Name": "r"})
	ebRefused(t, ts, "DescribeRule", map[string]any{"Name": "r"}, "ResourceNotFoundException")
	ebRefused(t, ts, "DeleteRule", map[string]any{"Name": "r", "EventBusName": "orders"}, "ValidationException")
	ebOK(t, ts, "RemoveTargets", map[string]any{"Rule": "r", "EventBusName": "orders", "Ids": []string{"t1"}})
	ebOK(t, ts, "DeleteRule", map[string]any{"Name": "r", "EventBusName": "orders"})
	assert.Empty(t, ebRuleNames(t, ebOK(t, ts, "ListRules", map[string]any{"EventBusName": "orders"})))
}

func TestEBRules_RefusalsNameTheBus(t *testing.T) {
	ts := smTagServer(t)
	raw := ebRefused(t, ts, "PutRule", map[string]any{"Name": "r", "EventBusName": "absent", "EventPattern": `{"source":["x"]}`}, "ResourceNotFoundException")
	assert.Contains(t, raw, "Event bus absent does not exist.")
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	raw = ebRefused(t, ts, "PutTargets", map[string]any{"Rule": "r", "EventBusName": "orders", "Targets": []any{}}, "ResourceNotFoundException")
	assert.Contains(t, raw, "Rule r does not exist on EventBus orders.")
	for _, op := range []string{"PutRule", "DescribeRule", "DeleteRule", "EnableRule", "DisableRule"} {
		ebRefused(t, ts, op, map[string]any{"Name": "orders/r"}, "ValidationError")
		ebRefused(t, ts, op, map[string]any{}, "ValidationError")
	}
	for _, op := range []string{"PutTargets", "RemoveTargets", "ListTargetsByRule"} {
		ebRefused(t, ts, op, map[string]any{"Rule": "a:b"}, "ValidationError")
	}
}

func TestEBRules_EnableAndDisableFlipState(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r", "EventBusName": "orders", "ScheduleExpression": "rate(1 hour)"})
	for _, step := range []struct{ op, want string }{
		{"DisableRule", "DISABLED"}, {"DisableRule", "DISABLED"}, {"EnableRule", "ENABLED"},
	} {
		assert.Empty(t, ebOK(t, ts, step.op, map[string]any{"Name": "r", "EventBusName": "orders"}), "%s answers an empty body", step.op)
		assert.Equal(t, step.want, ebOK(t, ts, "DescribeRule", map[string]any{"Name": "r", "EventBusName": "orders"})["State"])
	}
	ebRefused(t, ts, "EnableRule", map[string]any{"Name": "r"}, "ResourceNotFoundException")
	ebRefused(t, ts, "DisableRule", map[string]any{"Name": "absent", "EventBusName": "orders"}, "ResourceNotFoundException")
}

func TestEBRules_ListRuleNamesByTarget(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	queue := "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":q"
	other := "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":other"
	for _, r := range []struct {
		name, bus, arn string
	}{{"c", "", queue}, {"a", "", queue}, {"b", "", other}, {"d", "orders", queue}} {
		ebPutRule(t, ts, ebTarget, map[string]any{"Name": r.name, "EventBusName": r.bus, "EventPattern": `{"source":["x"]}`})
		ebOK(t, ts, "PutTargets", map[string]any{"Rule": r.name, "EventBusName": r.bus,
			"Targets": []any{map[string]string{"Id": "t", "Arn": r.arn}}})
	}

	assert.Equal(t, map[string]any{"RuleNames": []any{"a", "c"}}, ebOK(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": queue}))
	assert.Equal(t, map[string]any{"RuleNames": []any{"d"}}, ebOK(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": queue, "EventBusName": "orders"}))
	assert.Equal(t, map[string]any{"RuleNames": []any{}}, ebOK(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":none"}))

	first := ebOK(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": queue, "Limit": 1})
	assert.Equal(t, []any{"a"}, first["RuleNames"])
	token, _ := first["NextToken"].(string)
	require.NotEmpty(t, token)
	assert.Equal(t, map[string]any{"RuleNames": []any{"c"}}, ebOK(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": queue, "Limit": 1, "NextToken": token}))
	ebRefused(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": other, "NextToken": token}, "InvalidToken")
	ebRefused(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": queue, "NextToken": "junk"}, "InvalidToken")
	ebRefused(t, ts, "ListRuleNamesByTarget", map[string]any{}, "ValidationError")
	ebRefused(t, ts, "ListRuleNamesByTarget", map[string]any{"TargetArn": queue, "Limit": 101}, "ValidationError")
}

func TestEBRules_TheRuleQuotaIsPerBus(t *testing.T) {
	ts := smTagServer(t)
	ebOK(t, ts, "CreateEventBus", map[string]any{"Name": "orders"})
	for i := range 300 {
		ebPutRule(t, ts, ebTarget, map[string]any{"Name": fmt.Sprintf("r%03d", i), "EventPattern": `{"source":["x"]}`})
	}
	ebRefused(t, ts, "PutRule", map[string]any{"Name": "one-too-many", "EventPattern": `{"source":["x"]}`}, "LimitExceededException")
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r000", "EventPattern": `{"source":["y"]}`}) // an update is not a new rule
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "r000", "EventBusName": "orders", "EventPattern": `{"source":["x"]}`})

	// The quotas page gives two Regions 100.
	capeTown := signedRequestTarget{host: "events.af-south-1.amazonaws.com", target: "AWSEvents", signingName: "events"}
	for i := range 100 {
		ebPutRule(t, ts, capeTown, map[string]any{"Name": fmt.Sprintf("r%03d", i), "EventPattern": `{"source":["x"]}`})
	}
	_, raw, code := ebCall(t, ts, capeTown, "PutRule", map[string]any{"Name": "one-too-many", "EventPattern": `{"source":["x"]}`})
	assert.Equal(t, "LimitExceededException", code, raw)
}

// TestEBRules_APreV0134RecordIsTheDefaultBusRule loads state as a release before rules were keyed
// per bus wrote it — the record at ebrule:{account}/{region}/{name}, its targets at
// ebtargets:{account}/{region}/{name} and the unscoped name index — and checks that the rule still
// lists, describes, keeps its targets and tags, and deletes. Its EventBusName named a custom bus,
// which those releases accepted and did not key by; it is the default-bus rule its key makes it.
func TestEBRules_APreV0134RecordIsTheDefaultBusRule(t *testing.T) {
	ts := smTagServer(t)
	state := ts.StateManager()
	ctx := context.Background()
	scope := taggingTestAccount + "/us-east-1"
	put := func(key, value string) {
		require.NoError(t, state.Put(ctx, "eventbridge", key, []byte(value)))
	}
	put("ebrule:"+scope+"/legacy", `{"Name":"legacy","Arn":"`+ebRuleARNPrefix+`legacy","EventPattern":"{\"source\":[\"old\"]}",`+
		`"State":"ENABLED","EventBusName":"orders","CreatedBy":"`+taggingTestAccount+`","Tags":[{"Key":"team","Value":"events"}],"ever_tagged":true}`)
	put("ebtargets:"+scope+"/legacy", `[{"Id":"t1","Arn":"arn:aws:sqs:us-east-1:`+taggingTestAccount+`:q"}]`)
	put("ebrule_names:"+scope, `["legacy"]`)

	assert.Equal(t, []string{"legacy"}, ebRuleNames(t, ebOK(t, ts, "ListRules", map[string]any{})))
	described := ebOK(t, ts, "DescribeRule", map[string]any{"Name": "legacy"})
	assert.Equal(t, ebRuleARNPrefix+"legacy", described["Arn"])
	assert.Equal(t, "default", described["EventBusName"])
	assert.Equal(t, `{"source":["old"]}`, described["EventPattern"])
	targets := ebOK(t, ts, "ListTargetsByRule", map[string]any{"Rule": "legacy"})["Targets"]
	raw, err := json.Marshal(targets)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"Id":"t1","Arn":"arn:aws:sqs:us-east-1:`+taggingTestAccount+`:q"}]`, string(raw))
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "events"}},
		ebOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": ebRuleARNPrefix + "legacy"})["Tags"])
	assert.Contains(t, getResourcesARNs(t, ts, "events"), ebRuleARNPrefix+"legacy")

	// A write keeps the record where it was, so a later release reads it the same way.
	ebOK(t, ts, "DisableRule", map[string]any{"Name": "legacy"})
	stored, err := state.Get(ctx, "eventbridge", "ebrule:"+scope+"/legacy")
	require.NoError(t, err)
	assert.Contains(t, string(stored), `"State":"DISABLED"`)

	ebRefused(t, ts, "DeleteRule", map[string]any{"Name": "legacy"}, "ValidationException")
	ebOK(t, ts, "RemoveTargets", map[string]any{"Rule": "legacy", "Ids": []string{"t1"}})
	ebOK(t, ts, "DeleteRule", map[string]any{"Name": "legacy"})
	assert.Empty(t, ebRuleNames(t, ebOK(t, ts, "ListRules", map[string]any{})))
	for _, key := range []string{"ebrule:" + scope + "/legacy", "ebtargets:" + scope + "/legacy"} {
		left, err := state.Get(ctx, "eventbridge", key)
		require.NoError(t, err)
		assert.Nil(t, left, "%s is deleted", key)
	}
}
