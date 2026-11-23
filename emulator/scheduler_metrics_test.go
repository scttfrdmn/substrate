package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AWS/Scheduler metrics (#1513): CloudWatch derives them from the counters the Scheduler keeps as
// its invocations run on the simulated clock.

// cloudwatch sends one awsJson1_0 CloudWatch request through the test server.
func (e *schedEnv) cloudwatch(op string, body any) map[string]any {
	e.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(e.t, err)
	status, out := e.do(http.MethodPost, "monitoring.us-east-1.amazonaws.com", "/", "application/x-amz-json-1.0",
		"GraniteServiceVersion20100801."+op, raw)
	require.Equal(e.t, http.StatusOK, status, "%s: %s", op, out)
	doc := map[string]any{}
	require.NoError(e.t, json.Unmarshal(out, &doc))
	return doc
}

// schedSums reads each named AWS/Scheduler metric's hourly Sum over the test's two hours, keyed by
// metric name, for the given dimensions.
func (e *schedEnv) schedSums(dims []map[string]string, metrics ...string) map[string][]float64 {
	e.t.Helper()
	var queries []any
	for i, m := range metrics {
		queries = append(queries, map[string]any{"Id": "q" + string(rune('a'+i)), "Label": m, "MetricStat": map[string]any{
			"Metric": map[string]any{"Namespace": "AWS/Scheduler", "MetricName": m, "Dimensions": dims},
			"Period": 3600, "Stat": "Sum",
		}})
	}
	out := e.cloudwatch("GetMetricData", map[string]any{
		"StartTime": schedClockInstant.Unix(), "EndTime": schedClockInstant.Add(3 * time.Hour).Unix(),
		"MetricDataQueries": queries,
	})
	got := map[string][]float64{}
	for _, r := range out["MetricDataResults"].([]any) {
		res := r.(map[string]any)
		var vals []float64
		for _, v := range res["Values"].([]any) {
			vals = append(vals, v.(float64))
		}
		got[res["Label"].(string)] = vals
	}
	return got
}

func TestSchedulerMetrics_RetriesDropsAndDeadLetters(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.createLeaseGroup()
	_, queueARN := e.createQueue("lease-dlq")
	seed := `{"scheduleName": "*", "groupName": "aegc-dev", "errorCode": "ThrottlingException", "errorMessage": "Rate exceeded"}`
	status, out := e.do(http.MethodPost, "localhost", "/v1/scheduler/invocation-outcome", "application/json", "", []byte(seed))
	require.Equal(t, http.StatusOK, status, "%s", out)

	withTarget := func(name string, mutate func(map[string]any)) {
		target := leaseSchedule(nil)["Target"].(map[string]any)
		target["RetryPolicy"] = map[string]any{"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}
		mutate(target)
		e.mustSched(http.MethodPost, "/schedules/"+name, leaseSchedule(map[string]any{"Target": target}))
	}
	withTarget("to-dlq", func(t map[string]any) { t["DeadLetterConfig"] = map[string]any{"Arn": queueARN} })
	withTarget("no-dlq", func(map[string]any) {})
	withTarget("missing-dlq", func(t map[string]any) {
		t["DeadLetterConfig"] = map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:absent"}
	})
	// A schedule in another group, whose target succeeds.
	e.mustSched(http.MethodPost, "/schedule-groups/ok-group", map[string]any{})
	e.mustSched(http.MethodPost, "/schedules/fine", leaseSchedule(map[string]any{"GroupName": "ok-group"}))

	// The attempt at 12:00 and the two retries 60 s and 120 s after it.
	for _, step := range []time.Duration{time.Hour, 60 * time.Second, 120 * time.Second} {
		e.ts.AdvanceTime(step)
		_, _ = e.parameter("/aegc/dev/state")
	}

	group := []map[string]string{{"Name": "ScheduleGroup", "Value": "aegc-dev"}}
	got := e.schedSums(group, "InvocationAttemptCount", "TargetErrorCount", "TargetErrorThrottledCount",
		"InvocationDroppedCount", "InvocationsSentToDeadLetterCount", "InvocationsFailedToBeSentToDeadLetterCount",
		"InvocationThrottleCount")
	assert.Equal(t, map[string][]float64{
		"InvocationAttemptCount":                     {9},
		"TargetErrorCount":                           {9},
		"TargetErrorThrottledCount":                  {9},
		"InvocationDroppedCount":                     {3},
		"InvocationsSentToDeadLetterCount":           {1},
		"InvocationsFailedToBeSentToDeadLetterCount": {1},
		"InvocationThrottleCount":                    nil,
	}, got)

	// The account metric, with no dimensions, sums every group.
	got = e.schedSums(nil, "InvocationAttemptCount", "TargetErrorCount")
	assert.Equal(t, map[string][]float64{"InvocationAttemptCount": {10}, "TargetErrorCount": {9}}, got)
	got = e.schedSums([]map[string]string{{"Name": "ScheduleGroup", "Value": "ok-group"}}, "InvocationAttemptCount", "TargetErrorCount")
	assert.Equal(t, map[string][]float64{"InvocationAttemptCount": {1}, "TargetErrorCount": nil}, got)
	got = e.schedSums([]map[string]string{{"Name": "Other", "Value": "x"}}, "InvocationAttemptCount")
	assert.Equal(t, map[string][]float64{"InvocationAttemptCount": nil}, got, "another dimension selects nothing")

	// Each attempt counts in the minute it was due.
	out2 := e.cloudwatch("GetMetricStatistics", map[string]any{
		"Namespace": "AWS/Scheduler", "MetricName": "InvocationAttemptCount", "Dimensions": group,
		"StartTime": schedClockInstant.Unix(), "EndTime": schedClockInstant.Add(2 * time.Hour).Unix(),
		"Period": 60, "Statistics": []string{"Sum", "Average"},
	})
	var minutes []string
	for _, dp := range out2["Datapoints"].([]any) {
		d := dp.(map[string]any)
		assert.Equal(t, 1.0, d["Average"])
		assert.Equal(t, "Count", d["Unit"])
		minutes = append(minutes, time.Unix(int64(d["Timestamp"].(float64)), 0).UTC().Format("15:04"))
	}
	assert.Equal(t, []string{"12:00", "12:01", "12:03"}, minutes)

	// ListMetrics reports the account metric and each group's, by name; the suffixed dead-letter
	// failure names the queue's refusal.
	list := e.cloudwatch("ListMetrics", map[string]any{"Namespace": "AWS/Scheduler"})
	var listed []string
	for _, m := range list["Metrics"].([]any) {
		mm := m.(map[string]any)
		dims := ""
		for _, d := range mm["Dimensions"].([]any) {
			dims = d.(map[string]any)["Value"].(string)
		}
		listed = append(listed, mm["MetricName"].(string)+"|"+dims)
	}
	assert.Contains(t, listed, "InvocationAttemptCount|")
	assert.Contains(t, listed, "InvocationAttemptCount|aegc-dev")
	assert.Contains(t, listed, "InvocationAttemptCount|ok-group")
	assert.NotContains(t, listed, "TargetErrorCount|ok-group")
	suffixed := 0
	for _, l := range listed {
		if strings.HasPrefix(l, "InvocationsFailedToBeSentToDeadLetterCount_") && strings.HasSuffix(l, "|aegc-dev") {
			suffixed++
		}
	}
	assert.Equal(t, 1, suffixed, "%v", listed)
	filtered := e.cloudwatch("ListMetrics", map[string]any{"Namespace": "AWS/Scheduler", "MetricName": "InvocationDroppedCount",
		"Dimensions": []map[string]string{{"Name": "ScheduleGroup"}}})
	assert.Len(t, filtered["Metrics"].([]any), 1)
}
