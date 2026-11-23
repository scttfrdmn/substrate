package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Edge cases of the datapoint store, the percentile, metric math and the alarm-tag lock (#1595,
// #1618).

// extended reads the one datapoint's ExtendedStatistics from a GetMetricStatistics over the
// test's ten-minute window, nil when there is no datapoint.
func (e *cwEnv) extended(ns string, stats ...string) map[string]any {
	e.t.Helper()
	out := e.ok("GetMetricStatistics", map[string]any{
		"Namespace": ns, "MetricName": "V", "StartTime": at(-10), "EndTime": at(1), "Period": 660,
		"ExtendedStatistics": stats,
	})
	dps := out["Datapoints"].([]any)
	if len(dps) == 0 {
		return nil
	}
	require.Len(e.t, dps, 1)
	got, _ := dps[0].(map[string]any)["ExtendedStatistics"].(map[string]any)
	return got
}

// publishRange publishes the values 1..n in one minute, 150 to a datum.
func (e *cwEnv) publishRange(ns string, n int) {
	e.t.Helper()
	var data []map[string]any
	for lo := 1; lo <= n; lo += 150 {
		var values []float64
		for v := lo; v < lo+150 && v <= n; v++ {
			values = append(values, float64(v))
		}
		data = append(data, map[string]any{"MetricName": "V", "Values": values, "Timestamp": at(-1)})
	}
	e.ok("PutMetricData", map[string]any{"Namespace": ns, "MetricData": data})
}

func TestCWMetrics_NearestRankHasNoRoundingError(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.publishRange("hundred", 100)
	e.publishRange("thousand", 1000)
	for _, tc := range []struct {
		ns, stat string
		want     float64
	}{
		{"hundred", "p7", 7}, {"hundred", "p14", 14}, {"hundred", "p28", 28}, {"hundred", "p56", 56},
		{"hundred", "p0", 1}, {"hundred", "p100", 100}, {"hundred", "p99.5", 100}, {"hundred", "p0.5", 1},
		{"thousand", "p99.9", 999}, {"thousand", "p7", 70}, {"thousand", "p14", 140}, {"thousand", "p0.1", 1},
	} {
		t.Run(tc.ns+"/"+tc.stat, func(t *testing.T) {
			assert.Equal(t, map[string]any{tc.stat: tc.want}, e.extended(tc.ns, tc.stat))
		})
	}
}

func TestCWMetrics_PercentilesAndDatapointOmission(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricData", map[string]any{"Namespace": "neg", "MetricData": []map[string]any{
		{"MetricName": "V", "Values": []float64{-1, 5}, "Timestamp": at(-1)},
	}})
	assert.Nil(t, e.extended("neg", "p50"), "no percentile over a negative value, so no datapoint")
	e.ok("PutMetricData", map[string]any{"Namespace": "set", "MetricData": []map[string]any{
		{"MetricName": "V", "StatisticValues": map[string]any{"SampleCount": 2, "Sum": 3, "Minimum": 1, "Maximum": 2}, "Timestamp": at(-1)},
	}})
	assert.Nil(t, e.extended("set", "p50"), "a statistic set with Min below Max yields no percentile")

	for _, bad := range []string{"tm90", "p101", "p", "P50", "p-1", "IQM"} {
		e.refused("GetMetricStatistics", map[string]any{
			"Namespace": "neg", "MetricName": "V", "StartTime": at(-10), "EndTime": at(1), "Period": 60,
			"ExtendedStatistics": []string{bad},
		}, "InvalidParameterValue")
	}
}

func TestCWMetrics_NonFiniteStatisticsAreOmitted(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	// 1e108 is inside the published bound; 1e300 copies of it are not a finite Sum.
	e.ok("PutMetricData", map[string]any{"Namespace": "big", "MetricData": []map[string]any{
		{"MetricName": "V", "Values": []float64{1e108}, "Counts": []float64{1e300}, "Timestamp": at(-1)},
		{"MetricName": "W", "Value": 1e108, "Timestamp": at(-1)},
	}})
	for _, stat := range []string{"Sum", "Average", "SampleCount"} {
		got, _ := results(t, e.ok("GetMetricData", map[string]any{
			"StartTime": at(-10), "EndTime": at(1),
			"MetricDataQueries": []any{metricStat("v", "big", "V", stat, 60)},
		}))
		if stat == "SampleCount" {
			assert.Equal(t, []float64{1e300}, got[0].Values)
			continue
		}
		assert.Empty(t, got[0].Values, "%s overflows and is omitted", stat)
	}
	out := e.ok("GetMetricStatistics", map[string]any{"Namespace": "big", "MetricName": "V",
		"StartTime": at(-10), "EndTime": at(1), "Period": 60, "Statistics": []string{"Sum", "Maximum"}})
	dp := out["Datapoints"].([]any)[0].(map[string]any)
	assert.NotContains(t, dp, "Sum")
	assert.Equal(t, 1e108, dp["Maximum"])

	// Metric math that overflows drops the point, over the query protocol too, which is where a
	// non-finite value used to fail the render.
	got, _ := results(t, e.ok("GetMetricData", map[string]any{
		"StartTime": at(-10), "EndTime": at(1),
		"MetricDataQueries": []any{
			map[string]any{"Id": "x", "Expression": "w * w * w"},
			map[string]any{"Id": "w", "ReturnData": false, "MetricStat": metricStat("w", "big", "W", "Sum", 60)["MetricStat"]},
		},
	}))
	assert.Empty(t, got[0].Values)
	assert.Empty(t, got[0].Messages)
	resp := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded", strings.Join([]string{
		"Action=GetMetricData", fmt.Sprintf("StartTime=%d", int64(at(-10))), fmt.Sprintf("EndTime=%d", int64(at(1))),
		"MetricDataQueries.member.1.Id=x", "MetricDataQueries.member.1.Expression=w*w*w",
		"MetricDataQueries.member.2.Id=w", "MetricDataQueries.member.2.ReturnData=false",
		"MetricDataQueries.member.2.MetricStat.Metric.Namespace=big", "MetricDataQueries.member.2.MetricStat.Metric.MetricName=W",
		"MetricDataQueries.member.2.MetricStat.Period=60", "MetricDataQueries.member.2.MetricStat.Stat=Sum",
		"MetricDataQueries.member.3.Id=s", "MetricDataQueries.member.3.MetricStat.Metric.Namespace=big",
		"MetricDataQueries.member.3.MetricStat.Metric.MetricName=V",
		"MetricDataQueries.member.3.MetricStat.Period=60", "MetricDataQueries.member.3.MetricStat.Stat=Sum",
	}, "&"), nil)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode, "%s", cwReadBytes(t, resp))
}

func TestCWMetrics_ExpressionLimits(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricData", map[string]any{"Namespace": "n", "MetricData": []map[string]any{
		{"MetricName": "M", "Value": 2, "Timestamp": at(-1)},
	}})
	m := map[string]any{"Id": "m", "ReturnData": false, "MetricStat": metricStat("m", "n", "M", "Sum", 60)["MetricStat"]}
	query := func(expr string) cwResult {
		got, _ := results(t, e.ok("GetMetricData", map[string]any{
			"StartTime": at(-10), "EndTime": at(1),
			"MetricDataQueries": []any{map[string]any{"Id": "x", "Expression": expr}, m},
		}))
		return got[0]
	}

	deep := strings.Repeat("(", 65) + "m" + strings.Repeat(")", 65)
	r := query(deep)
	require.Len(t, r.Messages, 1)
	assert.Contains(t, r.Messages[0].Value, "nests deeper than 64")
	r = query(strings.Repeat("-", 65) + "m")
	require.Len(t, r.Messages, 1, "unary minus counts toward the depth too")
	assert.Equal(t, []float64{2}, query(strings.Repeat("(", 64)+"m"+strings.Repeat(")", 64)).Values)
	assert.Equal(t, []float64{-2}, query("--- m").Values)

	r = query("1/0")
	require.Len(t, r.Messages, 1)
	assert.Contains(t, r.Messages[0].Value, "constant")
	assert.Empty(t, query("m + 1/0").Values, "an undefined constant drops every point it combines with")
	assert.Empty(t, query("m / (m - m)").Values)

	long := "m" + strings.Repeat(" + m", 512)
	require.Greater(t, len(long), 2048)
	e.refused("GetMetricData", map[string]any{"StartTime": at(-10), "EndTime": at(1),
		"MetricDataQueries": []any{map[string]any{"Id": "x", "Expression": long}, m}}, "InvalidParameterValue")
	e.refused("PutMetricAlarm", map[string]any{"AlarmName": "a", "ComparisonOperator": "GreaterThanThreshold",
		"EvaluationPeriods": 1, "Threshold": 1, "Metrics": []any{map[string]any{"Id": "x", "Expression": long}}},
		"InvalidParameterValue")

	for _, bad := range []map[string]any{
		{"Id": "m", "MetricStat": map[string]any{"Metric": map[string]any{"Namespace": "n", "MetricName": "M"}, "Period": "abc", "Stat": "Sum"}},
		{"Id": "m", "MetricStat": map[string]any{"Metric": map[string]any{"Namespace": "n", "MetricName": "M"}, "Period": 0, "Stat": "Sum"}},
		{"Id": "x", "Expression": "1", "Period": -60},
	} {
		e.refused("GetMetricData", map[string]any{"StartTime": at(-10), "EndTime": at(1),
			"MetricDataQueries": []any{bad}}, "InvalidParameterValue")
	}
}

func TestCWTags_WritesToOneAlarmDoNotLoseEachOther(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricAlarm", basicAlarm("busy", nil))
	arn := e.accountARN("busy")

	const writers = 16
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(3)
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"ResourceARNList":["%s"],"Tags":{"rgta%02d":"v"}}`, arn, i)
			resp := cwPost(t, e.srv, "/", "application/x-amz-json-1.1", body,
				map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126.TagResources"})
			resp.Body.Close() //nolint:errcheck,gosec
		}()
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"ResourceARN":%q,"Tags":[{"Key":"cw%02d","Value":"v"}]}`, arn, i)
			resp := cwPost(t, e.srv, "/", "application/x-amz-json-1.0", body,
				map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801.TagResource"})
			resp.Body.Close() //nolint:errcheck,gosec
		}()
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"AlarmName":"busy","StateValue":"ALARM","StateReason":"r%d"}`, i)
			resp := cwPost(t, e.srv, "/", "application/x-amz-json-1.0", body,
				map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801.SetAlarmState"})
			resp.Body.Close() //nolint:errcheck,gosec
		}()
	}
	wg.Wait()
	assert.Len(t, e.tags(arn), 2*writers, "no write lost another's tag")

	// A delete racing tag writes leaves the alarm deleted: no tag write resurrects it.
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"ResourceARNList":["%s"],"Tags":{"late%02d":"v"}}`, arn, i)
			resp := cwPost(t, e.srv, "/", "application/x-amz-json-1.1", body,
				map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126.TagResources"})
			resp.Body.Close() //nolint:errcheck,gosec
		}()
	}
	e.ok("DeleteAlarms", map[string]any{"AlarmNames": []string{"busy"}})
	wg.Wait()
	out := e.ok("DescribeAlarms", map[string]any{})
	assert.Empty(t, out["MetricAlarms"])
	status, _ := e.do("ListTagsForResource", map[string]any{"ResourceARN": arn})
	assert.Equal(t, http.StatusNotFound, status)
}
