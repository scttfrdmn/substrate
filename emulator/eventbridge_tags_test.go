package emulator_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// EventBridge rule tags: PutRule's Tags, TagResource, UntagResource, ListTagsForResource, and
// GetResources (#1607). Terraform's aws_cloudwatch_event_rule reads ListTagsForResource on every
// refresh, so the read path is asserted on raw bodies as well as decoded ones.

var (
	ebTarget      = signedRequestTarget{host: "events.us-east-1.amazonaws.com", target: "AWSEvents", signingName: "events"}
	ebWest2Target = signedRequestTarget{host: "events.us-west-2.amazonaws.com", target: "AWSEvents", signingName: "events"}
)

// ebCall posts an EventBridge operation and returns the status, the raw body and the bare error code.
func ebCall(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, op string, body map[string]any) (int, string, string) {
	t.Helper()
	resp := signedRequest(t, ts, tgt, taggingTestAccount, op, body)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var errShape struct {
		Type string `json:"__type"`
	}
	code := ""
	if json.Unmarshal(raw, &errShape) == nil {
		code = awsErrorCode(errShape.Type)
	}
	return resp.StatusCode, string(raw), code
}

// ebPutRule creates a rule and returns the RuleArn PutRule answered.
func ebPutRule(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, body map[string]any) string {
	t.Helper()
	status, raw, code := ebCall(t, ts, tgt, "PutRule", body)
	require.Empty(t, code, raw)
	require.Equal(t, http.StatusOK, status)
	var out struct {
		RuleArn string `json:"RuleArn"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	return out.RuleArn
}

// ebTags returns ListTagsForResource's tags for arn, in the order answered.
func ebTags(t *testing.T, ts *emulator.TestServer, arn string) []emulator.EBTag {
	t.Helper()
	status, raw, code := ebCall(t, ts, ebTarget, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Empty(t, code, raw)
	require.Equal(t, http.StatusOK, status)
	var out struct {
		Tags []emulator.EBTag `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	require.NotNil(t, out.Tags, "the Tags member is present even when empty: %s", raw)
	return out.Tags
}

func ebTagList(kv ...string) []map[string]string {
	out := make([]map[string]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, map[string]string{"Key": kv[i], "Value": kv[i+1]})
	}
	return out
}

func TestEBTags_PutRuleTagsAreListed(t *testing.T) {
	ts := smTagServer(t)
	arn := ebPutRule(t, ts, ebTarget, map[string]any{
		"Name": "tagged", "EventPattern": `{"source":["aws.ec2"]}`, "Tags": ebTagList("Stack", "Prod", "CostCenter", "12345"),
	})
	assert.Equal(t, []emulator.EBTag{{Key: "CostCenter", Value: "12345"}, {Key: "Stack", Value: "Prod"}}, ebTags(t, ts, arn),
		"the tags PutRule was given, sorted by key")

	// Neither rule read path renders the tags or the bookkeeping behind them.
	for _, op := range []string{"DescribeRule", "ListRules"} {
		_, raw, code := ebCall(t, ts, ebTarget, op, map[string]any{"Name": "tagged"})
		require.Empty(t, code)
		assert.Contains(t, raw, `"Name":"tagged"`, "presence anchor for %s", op)
		assert.NotContains(t, raw, "Tags", "%s publishes no Tags member", op)
		assert.NotContains(t, raw, "ever_tagged", "%s carries no bookkeeping", op)
	}
}

func TestEBTags_AnUpdatingPutRuleIgnoresItsTags(t *testing.T) {
	ts := smTagServer(t)
	arn := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "kept", "ScheduleExpression": "rate(5 minutes)", "Tags": ebTagList("env", "prod")})

	// "If you are updating an existing rule, any tags you specify in the PutRule operation are ignored."
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "kept", "ScheduleExpression": "rate(10 minutes)", "Tags": ebTagList("env", "dev", "extra", "x")})
	assert.Equal(t, []emulator.EBTag{{Key: "env", Value: "prod"}}, ebTags(t, ts, arn))

	_, raw, _ := ebCall(t, ts, ebTarget, "DescribeRule", map[string]any{"Name": "kept"})
	assert.Contains(t, raw, `"ScheduleExpression":"rate(10 minutes)"`, "the rest of the rule was replaced")
}

func TestEBTags_TagAndUntagMergeOntoTheRule(t *testing.T) {
	ts := smTagServer(t)
	arn := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "merged", "EventPattern": `{"source":["x"]}`})
	assert.Empty(t, ebTags(t, ts, arn), "an untagged rule answers an empty list")

	for _, step := range []struct {
		op   string
		body map[string]any
		want []emulator.EBTag
	}{
		{"TagResource", map[string]any{"ResourceARN": arn, "Tags": ebTagList("b", "1", "a", "2")}, []emulator.EBTag{{Key: "a", Value: "2"}, {Key: "b", Value: "1"}}},
		{"TagResource", map[string]any{"ResourceARN": arn, "Tags": ebTagList("a", "replaced")}, []emulator.EBTag{{Key: "a", Value: "replaced"}, {Key: "b", Value: "1"}}},
		{"UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"b", "never-there"}}, []emulator.EBTag{{Key: "a", Value: "replaced"}}},
		{"UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"a"}}, []emulator.EBTag{}},
	} {
		status, raw, code := ebCall(t, ts, ebTarget, step.op, step.body)
		require.Empty(t, code, raw)
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, step.want, ebTags(t, ts, arn), "after %s %v", step.op, step.body)
	}
}

func TestEBTags_RefusalsNameWhatIsWrong(t *testing.T) {
	ts := smTagServer(t)
	arn := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "refusals", "EventPattern": `{"source":["x"]}`})
	ruleARN := func(name string) string {
		return "arn:aws:events:us-east-1:" + taggingTestAccount + ":rule/" + name
	}
	fifty := make([]map[string]string, 0, 51)
	for i := range 51 {
		fifty = append(fifty, map[string]string{"Key": fmt.Sprintf("k%02d", i), "Value": "v"})
	}

	for _, tc := range []struct {
		name, op string
		body     map[string]any
		code     string
	}{
		{"an absent rule", "ListTagsForResource", map[string]any{"ResourceARN": ruleARN("absent")}, "ResourceNotFoundException"},
		{"a rule on another bus", "ListTagsForResource", map[string]any{"ResourceARN": ruleARN("orders/refusals")}, "ResourceNotFoundException"},
		{"an absent event bus", "TagResource", map[string]any{"ResourceARN": "arn:aws:events:us-east-1:" + taggingTestAccount + ":event-bus/absent", "Tags": ebTagList("a", "b")}, "ResourceNotFoundException"},
		{"another service's ARN", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":q"}, "ValidationError"},
		{"an untaggable EventBridge resource", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:events:us-east-1:" + taggingTestAccount + ":archive/a"}, "ValidationError"},
		{"no ARN", "ListTagsForResource", map[string]any{}, "ValidationError"},
		{"no Tags", "TagResource", map[string]any{"ResourceARN": arn}, "ValidationError"},
		{"no TagKeys", "UntagResource", map[string]any{"ResourceARN": arn}, "ValidationError"},
		{"an empty key", "TagResource", map[string]any{"ResourceARN": arn, "Tags": ebTagList("", "v")}, "ValidationError"},
		{"a value over 256 characters", "TagResource", map[string]any{"ResourceARN": arn, "Tags": ebTagList("k", strings.Repeat("v", 257))}, "ValidationError"},
		{"more than fifty tags", "TagResource", map[string]any{"ResourceARN": arn, "Tags": fifty}, "LimitExceededException"},
		{"more than fifty tags at creation", "PutRule", map[string]any{"Name": "too-many", "EventPattern": `{"source":["x"]}`, "Tags": fifty}, "LimitExceededException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw, code := ebCall(t, ts, ebTarget, tc.op, tc.body)
			assert.Equal(t, tc.code, code, raw)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
	assert.Empty(t, ebTags(t, ts, arn), "no refused call stored a tag")
}

func TestEBTags_TheARNsAccountAndRegionNameTheRule(t *testing.T) {
	ts := smTagServer(t)
	westARN := ebPutRule(t, ts, ebWest2Target, map[string]any{"Name": "west", "EventPattern": `{"source":["x"]}`})
	eastTwin := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "west", "EventPattern": `{"source":["x"]}`})
	require.Equal(t, strings.Replace(westARN, "us-west-2", "us-east-1", 1), eastTwin)

	// The ARN names the us-west-2 rule (#826), which a us-east-1 caller cannot reach: each tag
	// operation answers it not found rather than acting on the caller's rule of the same name.
	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"TagResource", map[string]any{"ResourceARN": westARN, "Tags": ebTagList("region", "west")}},
		{"UntagResource", map[string]any{"ResourceARN": westARN, "TagKeys": []string{"region"}}},
		{"ListTagsForResource", map[string]any{"ResourceARN": westARN}},
		{"TagResource", map[string]any{"ResourceARN": strings.Replace(eastTwin, taggingTestAccount, "210987654321", 1), "Tags": ebTagList("a", "b")}},
	} {
		status, raw, code := ebCall(t, ts, ebTarget, tc.op, tc.body)
		assert.Equal(t, "ResourceNotFoundException", code, "%s: %s", tc.op, raw)
		assert.Equal(t, http.StatusBadRequest, status)
	}
	assert.Empty(t, ebTags(t, ts, eastTwin), "the caller's own rule of that name is untouched")

	status, raw, code := ebCall(t, ts, ebWest2Target, "TagResource", map[string]any{"ResourceARN": westARN, "Tags": ebTagList("region", "west")})
	require.Empty(t, code, raw)
	require.Equal(t, http.StatusOK, status)
	_, raw, _ = ebCall(t, ts, ebWest2Target, "ListTagsForResource", map[string]any{"ResourceARN": westARN})
	assert.JSONEq(t, `{"Tags":[{"Key":"region","Value":"west"}]}`, raw, "a us-west-2 caller reaches it")
}

func TestEBTags_ACustomBusRuleARNNamesItsBus(t *testing.T) {
	ts := smTagServer(t)
	busARN := "arn:aws:events:us-east-1:" + taggingTestAccount + ":event-bus/orders"
	status, raw, code := ebCall(t, ts, ebTarget, "CreateEventBus", map[string]any{"Name": "orders"})
	require.Empty(t, code, raw)
	require.Equal(t, http.StatusOK, status)
	for _, tc := range []struct{ rule, bus string }{
		{"by-name", "orders"},
		{"by-arn", busARN},
	} {
		ebPutRule(t, ts, ebTarget, map[string]any{"Name": tc.rule, "EventBusName": tc.bus, "EventPattern": `{"source":["x"]}`})
		busForm := "arn:aws:events:us-east-1:" + taggingTestAccount + ":rule/orders/" + tc.rule
		status, raw, code := ebCall(t, ts, ebTarget, "TagResource", map[string]any{"ResourceARN": busForm, "Tags": ebTagList("bus", "orders")})
		require.Empty(t, code, "%s: %s", tc.rule, raw)
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, []emulator.EBTag{{Key: "bus", Value: "orders"}}, ebTags(t, ts, busForm),
			"the rule/{bus}/{name} form names a rule on that bus, whether PutRule named the bus or its ARN")
	}
}

func TestEBTags_TargetsOfAnAbsentRuleAreRefused(t *testing.T) {
	ts := smTagServer(t)
	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"RemoveTargets", map[string]any{"Rule": "absent", "Ids": []string{"t1"}}},
		{"PutTargets", map[string]any{"Rule": "absent", "Targets": []map[string]string{{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":q"}}}},
	} {
		status, raw, code := ebCall(t, ts, ebTarget, tc.op, tc.body)
		assert.Equal(t, "ResourceNotFoundException", code, "%s: %s", tc.op, raw)
		assert.Equal(t, http.StatusBadRequest, status)
	}

	// Nothing was written under the absent name: a rule created with it afterwards has no targets.
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "absent", "EventPattern": `{"source":["x"]}`})
	_, raw, code := ebCall(t, ts, ebTarget, "ListTargetsByRule", map[string]any{"Rule": "absent"})
	require.Empty(t, code, raw)
	var out struct {
		Targets []json.RawMessage `json:"Targets"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	assert.Empty(t, out.Targets)
}

func TestEBTags_DescribeRuleRendersRoleArnAndCreatedBy(t *testing.T) {
	ts := smTagServer(t)
	role := "arn:aws:iam::" + taggingTestAccount + ":role/events"
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "with-role", "ScheduleExpression": "rate(1 hour)", "RoleArn": role})

	_, raw, code := ebCall(t, ts, ebTarget, "DescribeRule", map[string]any{"Name": "with-role"})
	require.Empty(t, code)
	var described map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &described))
	assert.JSONEq(t, `"`+role+`"`, string(described["RoleArn"]))
	assert.JSONEq(t, `"`+taggingTestAccount+`"`, string(described["CreatedBy"]))

	_, raw, _ = ebCall(t, ts, ebTarget, "ListRules", map[string]any{})
	assert.Contains(t, raw, `"RoleArn":"`+role+`"`)
	assert.NotContains(t, raw, "CreatedBy", "ListRules' Rule type publishes no CreatedBy")
}

func TestEBTags_GetResourcesReportsTaggedRules(t *testing.T) {
	ts := smTagServer(t)
	tagged := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "reported", "EventPattern": `{"source":["x"]}`, "Tags": ebTagList("team", "events")})
	ebPutRule(t, ts, ebTarget, map[string]any{"Name": "never-tagged", "EventPattern": `{"source":["x"]}`})
	untagged := ebPutRule(t, ts, ebTarget, map[string]any{"Name": "once-tagged", "EventPattern": `{"source":["x"]}`})
	_, _, code := ebCall(t, ts, ebTarget, "TagResource", map[string]any{"ResourceARN": untagged, "Tags": ebTagList("tmp", "1")})
	require.Empty(t, code)
	_, _, code = ebCall(t, ts, ebTarget, "UntagResource", map[string]any{"ResourceARN": untagged, "TagKeys": []string{"tmp"}})
	require.Empty(t, code)

	assert.ElementsMatch(t, []string{tagged, untagged}, getResourcesARNs(t, ts, "events"),
		"a tagged rule and a previously-tagged one, and neither a never-tagged rule nor the name index (#938)")
	assert.Equal(t, map[string]string{"team": "events"}, getResourcesTags(t, ts, tagged))
	assert.Equal(t, map[string]string{}, getResourcesTags(t, ts, untagged))
}
