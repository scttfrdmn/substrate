package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Alarm tags (#1618, the CloudWatch half of #1607) and the PutMetricAlarm members DescribeAlarms
// renders (#1620).

// tags decodes a ListTagsForResource response.
func (e *cwEnv) tags(arn string) []map[string]string {
	e.t.Helper()
	out := e.ok("ListTagsForResource", map[string]any{"ResourceARN": arn})
	raw, err := json.Marshal(out["Tags"])
	require.NoError(e.t, err)
	var got []map[string]string
	require.NoError(e.t, json.Unmarshal(raw, &got))
	return got
}

func basicAlarm(name string, extra map[string]any) map[string]any {
	a := map[string]any{
		"AlarmName": name, "MetricName": "Tasks", "Namespace": "aegc", "Statistic": "Sum",
		"ComparisonOperator": "GreaterThanThreshold", "Threshold": 1, "EvaluationPeriods": 1, "Period": 60,
	}
	for k, v := range extra {
		a[k] = v
	}
	return a
}

// account resolves to the test credentials' account; the server picks it from the signature.
func (e *cwEnv) accountARN(name string) string {
	e.t.Helper()
	out := e.ok("DescribeAlarms", map[string]any{"AlarmNames": []string{name}})
	alarms := out["MetricAlarms"].([]any)
	require.Len(e.t, alarms, 1)
	return alarms[0].(map[string]any)["AlarmArn"].(string)
}

func TestCWTags_PutMetricAlarmTagsAndTheThreeOperations(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricAlarm", basicAlarm("owned", map[string]any{
		"Tags": []map[string]string{{"Key": "aegc:managed", "Value": "true"}, {"Key": "env", "Value": "dev"}},
	}))
	arn := e.accountARN("owned")
	assert.Equal(t, []map[string]string{{"Key": "aegc:managed", "Value": "true"}, {"Key": "env", "Value": "dev"}}, e.tags(arn))

	// An update ignores the request's tags and keeps the alarm's.
	e.ok("PutMetricAlarm", basicAlarm("owned", map[string]any{
		"Tags": []map[string]string{{"Key": "intruder", "Value": "x"}},
	}))
	assert.Equal(t, []map[string]string{{"Key": "aegc:managed", "Value": "true"}, {"Key": "env", "Value": "dev"}}, e.tags(arn))

	e.ok("TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{
		{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "hpc"},
	}})
	assert.Equal(t, []map[string]string{
		{"Key": "aegc:managed", "Value": "true"}, {"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "hpc"},
	}, e.tags(arn))

	e.ok("UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"env", "absent"}})
	assert.Equal(t, []map[string]string{{"Key": "aegc:managed", "Value": "true"}, {"Key": "team", "Value": "hpc"}}, e.tags(arn))

	e.ok("UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"aegc:managed", "team"}})
	assert.Equal(t, []map[string]string{}, e.tags(arn), "an alarm with no tags answers an empty list")

	// The tags never reach DescribeAlarms, whose MetricAlarm has no Tags member.
	out := e.ok("DescribeAlarms", map[string]any{})
	assert.NotContains(t, out["MetricAlarms"].([]any)[0], "Tags")

	// The query protocol renders the same list.
	resp := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded",
		"Action=TagResource&ResourceARN="+arn+"&Tags.member.1.Key=k&Tags.member.1.Value=v", nil)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp2 := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded",
		"Action=ListTagsForResource&ResourceARN="+arn, nil)
	defer resp2.Body.Close() //nolint:errcheck
	assert.Contains(t, string(cwReadBytes(t, resp2)),
		"<ListTagsForResourceResult><Tags><member><Key>k</Key><Value>v</Value></member></Tags></ListTagsForResourceResult>")
}

func TestCWTags_Refusals(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricAlarm", basicAlarm("a", nil))
	arn := e.accountARN("a")
	account := strings.Split(arn, ":")[4]
	fifty := make([]map[string]string, 50)
	for i := range fifty {
		fifty[i] = map[string]string{"Key": fmt.Sprintf("k%02d", i), "Value": "v"}
	}
	fiftyOne := append(append([]map[string]string{}, fifty...), map[string]string{"Key": "one-more", "Value": "v"})

	for _, tc := range []struct {
		name   string
		op     string
		body   map[string]any
		status int
		code   string
	}{
		{"missing ARN", "ListTagsForResource", map[string]any{}, 400, "MissingRequiredParameter"},
		{"not an ARN", "ListTagsForResource", map[string]any{"ResourceARN": "alarm-a"}, 400, "InvalidParameterValue"},
		{"another service", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:logs:us-east-1:" + account + ":log-group:a"}, 400, "InvalidParameterValue"},
		{"alarm with no name", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch:us-east-1:" + account + ":alarm:"}, 400, "InvalidParameterValue"},
		{"no such alarm", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch:us-east-1:" + account + ":alarm:nope"}, 404, "ResourceNotFoundException"},
		{"another account's alarm", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch:us-east-1:999999999999:alarm:a"}, 404, "ResourceNotFoundException"},
		{"another region's alarm", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch:us-west-2:" + account + ":alarm:a"}, 404, "ResourceNotFoundException"},
		{"a dashboard", "ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch::" + account + ":dashboard/d"}, 404, "ResourceNotFoundException"},
		{"a metric stream", "TagResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch:us-east-1:" + account + ":metric-stream/s",
			"Tags": []map[string]string{{"Key": "k", "Value": "v"}}}, 404, "ResourceNotFoundException"},
		{"no tags", "TagResource", map[string]any{"ResourceARN": arn}, 400, "MissingRequiredParameter"},
		{"no tag keys", "UntagResource", map[string]any{"ResourceARN": arn}, 400, "MissingRequiredParameter"},
		{"over the quota", "TagResource", map[string]any{"ResourceARN": arn, "Tags": fiftyOne}, 400, "InvalidParameterValue"},
		{"an empty key", "TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "", "Value": "v"}}}, 400, "InvalidParameterValue"},
		{"a long value", "TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}}}, 400, "InvalidParameterValue"},
		{"untag a missing alarm", "UntagResource", map[string]any{"ResourceARN": "arn:aws:cloudwatch:us-east-1:" + account + ":alarm:nope", "TagKeys": []string{"k"}}, 404, "ResourceNotFoundException"},
		{"create over the quota", "PutMetricAlarm", basicAlarm("b", map[string]any{"Tags": fiftyOne}), 400, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, out := e.do(tc.op, tc.body)
			assert.Equal(t, tc.status, status, "%v", out)
			assert.Contains(t, out["__type"], tc.code)
		})
	}

	// The quota counts the merged set: fifty fit, and one more on top does not.
	e.ok("TagResource", map[string]any{"ResourceARN": arn, "Tags": fifty})
	e.refused("TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "x", "Value": "y"}}}, "InvalidParameterValue")
	e.ok("TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "k00", "Value": "replaced"}}})
}

func TestCWTags_GetResourcesSeesAlarms(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricAlarm", basicAlarm("tagged", map[string]any{"Tags": []map[string]string{{"Key": "k", "Value": "v"}}}))
	e.ok("PutMetricAlarm", basicAlarm("untagged", nil))
	e.ok("PutMetricAlarm", basicAlarm("was-tagged", nil))
	wasTagged := e.accountARN("was-tagged")
	e.ok("TagResource", map[string]any{"ResourceARN": wasTagged, "Tags": []map[string]string{{"Key": "k", "Value": "v"}}})
	e.ok("UntagResource", map[string]any{"ResourceARN": wasTagged, "TagKeys": []string{"k"}})

	getResources := func(body string) map[string][]map[string]string {
		resp := cwPost(t, e.srv, "/", "application/x-amz-json-1.1", body,
			map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126.GetResources"})
		defer resp.Body.Close() //nolint:errcheck
		raw := cwReadBytes(t, resp)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s", raw)
		var out struct {
			ResourceTagMappingList []struct {
				ResourceARN string
				Tags        []map[string]string
			}
		}
		require.NoError(t, json.Unmarshal(raw, &out))
		got := map[string][]map[string]string{}
		for _, m := range out.ResourceTagMappingList {
			got[m.ResourceARN] = m.Tags
		}
		return got
	}
	got := getResources(`{"ResourceTypeFilters":["cloudwatch:alarm"]}`)
	assert.Equal(t, map[string][]map[string]string{
		e.accountARN("tagged"): {{"Key": "k", "Value": "v"}},
		wasTagged:              {},
	}, got, "a never-tagged alarm is absent and a previously tagged one has an empty set")

	// The tagging API writes through to the alarm, and CloudWatch reads what it wrote.
	rgta := func(target, body string) string {
		resp := cwPost(t, e.srv, "/", "application/x-amz-json-1.1", body,
			map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126." + target})
		defer resp.Body.Close() //nolint:errcheck
		raw := cwReadBytes(t, resp)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s", raw)
		return string(raw)
	}
	untagged := e.accountARN("untagged")
	rgta("TagResources", `{"ResourceARNList":["`+untagged+`"],"Tags":{"via":"rgta"}}`)
	assert.Equal(t, []map[string]string{{"Key": "via", "Value": "rgta"}}, e.tags(untagged))
	rgta("UntagResources", `{"ResourceARNList":["`+untagged+`"],"TagKeys":["via"]}`)
	assert.Equal(t, []map[string]string{}, e.tags(untagged))
	assert.Contains(t, getResources(`{}`), untagged, "a tag written through the tagging API stamps the history")
	many := make([]string, 51)
	for i := range many {
		many[i] = fmt.Sprintf(`"k%02d":"v"`, i)
	}
	out := rgta("TagResources", `{"ResourceARNList":["`+untagged+`"],"Tags":{`+strings.Join(many, ",")+`}}`)
	assert.Contains(t, out, "InvalidParameterValue", "CloudWatch's own quota answer")
	out = rgta("TagResources", `{"ResourceARNList":["arn:aws:cloudwatch::123456789012:dashboard/d"],"Tags":{"k":"v"}}`)
	assert.Contains(t, out, "FailedResourcesMap")

	// DeleteAlarms removes the tag history with the alarm, so a re-created alarm starts never-tagged.
	e.ok("DeleteAlarms", map[string]any{"AlarmNames": []string{"was-tagged"}})
	e.ok("PutMetricAlarm", basicAlarm("was-tagged", nil))
	got = getResources(`{"ResourceTypeFilters":["cloudwatch"]}`)
	assert.NotContains(t, got, wasTagged)
	assert.Contains(t, got, e.accountARN("tagged"))
}

func TestCWAlarms_DescribeAlarmsRendersEveryStoredMember(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricAlarm", map[string]any{
		"AlarmName": "endpoint-offline", "AlarmDescription": "no heartbeat",
		"Namespace": "aegc", "MetricName": "Heartbeat", "ExtendedStatistic": "p90", "Unit": "Count",
		"Dimensions":         []map[string]string{{"Name": "Endpoint", "Value": "e1"}},
		"ComparisonOperator": "LessThanThreshold", "Threshold": 1, "EvaluationPeriods": 5, "DatapointsToAlarm": 3,
		"Period": 60, "TreatMissingData": "breaching", "EvaluateLowSampleCountPercentile": "ignore",
		"ActionsEnabled": false, "OKActions": []string{"arn:aws:sns:us-east-1:123456789012:ok"},
		"InsufficientDataActions": []string{"arn:aws:sns:us-east-1:123456789012:insufficient"},
	})
	out := e.ok("DescribeAlarms", map[string]any{"AlarmNames": []string{"endpoint-offline"}})
	a := out["MetricAlarms"].([]any)[0].(map[string]any)
	for member, want := range map[string]any{
		"AlarmDescription": "no heartbeat", "ExtendedStatistic": "p90", "Unit": "Count",
		"DatapointsToAlarm": 3.0, "TreatMissingData": "breaching", "EvaluateLowSampleCountPercentile": "ignore",
		"EvaluationPeriods": 5.0, "ActionsEnabled": false, "StateValue": "INSUFFICIENT_DATA",
		"Dimensions":                         []any{map[string]any{"Name": "Endpoint", "Value": "e1"}},
		"OKActions":                          []any{"arn:aws:sns:us-east-1:123456789012:ok"},
		"InsufficientDataActions":            []any{"arn:aws:sns:us-east-1:123456789012:insufficient"},
		"AlarmConfigurationUpdatedTimestamp": float64(cwStoreNow.Unix()),
		"StateUpdatedTimestamp":              float64(cwStoreNow.Unix()),
		"StateTransitionedTimestamp":         float64(cwStoreNow.Unix()),
	} {
		assert.Equal(t, want, a[member], member)
	}
	assert.NotContains(t, a, "Statistic")

	// SetAlarmState moves both state timestamps; setting the same state again moves only one.
	e.tc.SetTime(cwStoreNow.Add(60e9))
	e.ok("SetAlarmState", map[string]any{"AlarmName": "endpoint-offline", "StateValue": "ALARM", "StateReason": "r"})
	e.tc.SetTime(cwStoreNow.Add(120e9))
	e.ok("SetAlarmState", map[string]any{"AlarmName": "endpoint-offline", "StateValue": "ALARM", "StateReason": "r"})
	e.tc.SetTime(cwStoreNow.Add(180e9))
	e.ok("PutMetricAlarm", basicAlarm("endpoint-offline", nil))
	a = e.ok("DescribeAlarms", map[string]any{})["MetricAlarms"].([]any)[0].(map[string]any)
	assert.Equal(t, "ALARM", a["StateValue"], "an update leaves the state")
	assert.Equal(t, float64(cwStoreNow.Unix()+60), a["StateTransitionedTimestamp"])
	assert.Equal(t, float64(cwStoreNow.Unix()+120), a["StateUpdatedTimestamp"])
	assert.Equal(t, float64(cwStoreNow.Unix()+180), a["AlarmConfigurationUpdatedTimestamp"])
	assert.NotContains(t, a, "TreatMissingData", "an update overwrites the whole configuration")
	assert.Equal(t, "Sum", a["Statistic"])
}
