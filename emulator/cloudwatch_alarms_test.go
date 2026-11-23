package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCWAlarms_MetricMathAndAnomalyDetectionAlarms(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricAlarm", map[string]any{
		"AlarmName": "error-rate", "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 3, "Threshold": 40,
		"Metrics": []map[string]any{
			{"Id": "m1", "ReturnData": false, "MetricStat": map[string]any{
				"Metric": map[string]any{"Namespace": "svc", "MetricName": "Errors",
					"Dimensions": []map[string]string{{"Name": "Component", "Value": "c1"}}},
				"Period": 60, "Stat": "Sum", "Unit": "Count"}},
			{"Id": "e1", "Expression": "m1 * 100", "Label": "rate", "ReturnData": true, "Period": 60},
			{"Id": "x1", "Expression": "m1", "AccountId": "111122223333"},
		},
	})
	e.ok("PutMetricAlarm", map[string]any{
		"AlarmName": "anomaly", "ComparisonOperator": "LessThanLowerOrGreaterThanUpperThreshold",
		"EvaluationPeriods": 2, "ThresholdMetricId": "t1",
		"Metrics": []map[string]any{
			{"Id": "m1", "ReturnData": true, "MetricStat": map[string]any{
				"Metric": map[string]any{"Namespace": "AWS/EC2", "MetricName": "CPUUtilization"}, "Period": 60, "Stat": "Average"}},
			{"Id": "t1", "Expression": "ANOMALY_DETECTION_BAND(m1, 3)"},
		},
	})
	out := e.ok("DescribeAlarms", map[string]any{})
	alarms := out["MetricAlarms"].([]any)
	require.Len(t, alarms, 2)
	byName := map[string]map[string]any{}
	for _, a := range alarms {
		byName[a.(map[string]any)["AlarmName"].(string)] = a.(map[string]any)
	}

	rate := byName["error-rate"]
	for _, absent := range []string{"MetricName", "Namespace", "Period", "Statistic", "ThresholdMetricId"} {
		assert.NotContains(t, rate, absent, "a metric-math alarm names its metrics in Metrics")
	}
	assert.Equal(t, []any{
		map[string]any{"Id": "m1", "ReturnData": false, "MetricStat": map[string]any{
			"Metric": map[string]any{"Namespace": "svc", "MetricName": "Errors",
				"Dimensions": []any{map[string]any{"Name": "Component", "Value": "c1"}}},
			"Period": 60.0, "Stat": "Sum", "Unit": "Count"}},
		map[string]any{"Id": "e1", "Expression": "m1 * 100", "Label": "rate", "ReturnData": true, "Period": 60.0},
		map[string]any{"Id": "x1", "Expression": "m1", "AccountId": "111122223333"},
	}, rate["Metrics"])

	anomaly := byName["anomaly"]
	assert.Equal(t, "t1", anomaly["ThresholdMetricId"])
	assert.NotContains(t, anomaly, "Threshold", "an anomaly-detection alarm's threshold is its band")

	// The Query protocol carries the same Metrics, and "False" in the reference's own spelling.
	resp := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded", strings.Join([]string{
		"Action=PutMetricAlarm", "AlarmName=q", "ComparisonOperator=GreaterThanThreshold", "EvaluationPeriods=1",
		"Threshold=1", "Metrics.member.1.Id=m1", "Metrics.member.1.ReturnData=False",
		"Metrics.member.1.MetricStat.Metric.Namespace=n", "Metrics.member.1.MetricStat.Metric.MetricName=m",
		"Metrics.member.1.MetricStat.Period=60", "Metrics.member.1.MetricStat.Stat=Sum",
		"Metrics.member.2.Id=e1", "Metrics.member.2.Expression=m1", "Metrics.member.2.ReturnData=True",
	}, "&"), nil)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", cwReadBytes(t, resp))
	resp2 := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded", "Action=DescribeAlarms&AlarmNames.member.1=q", nil)
	defer resp2.Body.Close() //nolint:errcheck
	assert.Contains(t, string(cwReadBytes(t, resp2)),
		"<Metrics><member><Id>m1</Id><MetricStat><Metric><Namespace>n</Namespace><MetricName>m</MetricName></Metric>"+
			"<Period>60</Period><Stat>Sum</Stat></MetricStat><ReturnData>false</ReturnData></member>"+
			"<member><Id>e1</Id><Expression>m1</Expression><ReturnData>true</ReturnData></member></Metrics>")
}

func TestCWAlarms_PutMetricAlarmValidation(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	six := []string{"a", "b", "c", "d", "e", "f"}
	dims := make([]map[string]string, 31)
	for i := range dims {
		dims[i] = map[string]string{"Name": strings.Repeat("d", i+1), "Value": "v"}
	}
	metrics := []map[string]any{{"Id": "e1", "Expression": "1"}}
	for _, tc := range []struct {
		name  string
		extra map[string]any
		code  string
	}{
		{"statistic", map[string]any{"Statistic": "Median"}, "InvalidParameterValue"},
		{"operator", map[string]any{"ComparisonOperator": "GT"}, "InvalidParameterValue"},
		{"missing data", map[string]any{"TreatMissingData": "zero"}, "InvalidParameterValue"},
		{"low sample count", map[string]any{"EvaluateLowSampleCountPercentile": "skip"}, "InvalidParameterValue"},
		{"unit", map[string]any{"Unit": "Furlongs"}, "InvalidParameterValue"},
		{"both statistics", map[string]any{"ExtendedStatistic": "p99"}, "InvalidParameterCombination"},
		{"metrics with metric name", map[string]any{"Metrics": metrics}, "InvalidParameterCombination"},
		{"metrics with dimensions", map[string]any{"Metrics": metrics, "MetricName": "", "Namespace": "", "Statistic": "", "Period": nil,
			"Dimensions": []map[string]string{{"Name": "a", "Value": "b"}}}, "InvalidParameterCombination"},
		{"threshold", map[string]any{"Threshold": "high"}, "InvalidParameterValue"},
		{"evaluation periods", map[string]any{"EvaluationPeriods": 0}, "InvalidParameterValue"},
		{"datapoints to alarm", map[string]any{"DatapointsToAlarm": 0}, "InvalidParameterValue"},
		{"period", map[string]any{"Period": 45}, "InvalidParameterValue"},
		{"a week", map[string]any{"Period": 3600, "EvaluationPeriods": 169}, "InvalidParameterValue"},
		{"a day", map[string]any{"Period": 60, "EvaluationPeriods": 1441}, "InvalidParameterValue"},
		{"six actions", map[string]any{"OKActions": six}, "InvalidParameterValue"},
		{"31 dimensions", map[string]any{"Dimensions": dims}, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := basicAlarm("v", tc.extra)
			for k, v := range tc.extra {
				if v == nil || v == "" {
					delete(body, k)
				}
			}
			e.refused("PutMetricAlarm", body, tc.code)
		})
	}
	// A 20-second period and a long high-resolution window are within the published bounds.
	e.ok("PutMetricAlarm", basicAlarm("hr", map[string]any{"Period": 20, "EvaluationPeriods": 4320}))
	e.ok("PutMetricAlarm", basicAlarm("weekly", map[string]any{"Period": 3600, "EvaluationPeriods": 168}))
}

func TestCWAlarms_DescribeAlarmsForMetricFilters(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	dims := []map[string]string{{"Name": "Pool", "Value": "gpu"}, {"Name": "Deployment", "Value": "dev"}}
	e.ok("PutMetricAlarm", basicAlarm("sum-60", map[string]any{"Dimensions": dims, "Unit": "Count"}))
	e.ok("PutMetricAlarm", basicAlarm("max-300", map[string]any{"Statistic": "Maximum", "Period": 300}))
	e.ok("PutMetricAlarm", basicAlarm("p99", map[string]any{"Statistic": "", "ExtendedStatistic": "p99"}))
	names := func(body map[string]any) []string {
		out := e.ok("DescribeAlarmsForMetric", body)
		var got []string
		for _, a := range out["MetricAlarms"].([]any) {
			got = append(got, a.(map[string]any)["AlarmName"].(string))
		}
		return got
	}
	base := map[string]any{"Namespace": "aegc", "MetricName": "Tasks"}
	with := func(k string, v any) map[string]any {
		m := map[string]any{k: v}
		for bk, bv := range base {
			m[bk] = bv
		}
		return m
	}
	assert.ElementsMatch(t, []string{"sum-60", "max-300", "p99"}, names(base))
	assert.Equal(t, []string{"max-300"}, names(with("Statistic", "Maximum")))
	assert.Equal(t, []string{"p99"}, names(with("ExtendedStatistic", "p99")))
	assert.Equal(t, []string{"max-300"}, names(with("Period", 300)))
	assert.Equal(t, []string{"sum-60"}, names(with("Unit", "Count")))
	assert.Equal(t, []string{"sum-60"}, names(with("Dimensions", []map[string]string{dims[1], dims[0]})),
		"dimensions match as a set")
	assert.Empty(t, names(with("Dimensions", dims[:1])))
}
