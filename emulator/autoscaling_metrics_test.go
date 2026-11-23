package emulator_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// asgAllMetrics are the twenty group metrics, in API_DescribeMetricCollectionTypes' sample order.
var asgAllMetrics = []string{
	"GroupMinSize", "GroupMaxSize", "GroupDesiredCapacity", "GroupInServiceInstances",
	"GroupPendingInstances", "GroupTerminatingInstances", "GroupStandbyInstances", "GroupTotalInstances",
	"GroupInServiceCapacity", "GroupPendingCapacity", "GroupStandbyCapacity", "GroupTerminatingCapacity",
	"GroupTotalCapacity", "WarmPoolDesiredCapacity", "WarmPoolWarmedCapacity", "WarmPoolPendingCapacity",
	"WarmPoolTerminatingCapacity", "WarmPoolTotalCapacity", "GroupAndWarmPoolDesiredCapacity",
	"GroupAndWarmPoolTotalCapacity",
}

// asgEnabledMetrics describes a group and returns its EnabledMetrics, checking every one reports the
// only published granularity.
func (e *asgEnv) asgEnabledMetrics(name string) []string {
	e.t.Helper()
	body := e.mustASG("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": name})
	assert.Contains(e.t, string(body), "<EnabledMetrics", "EnabledMetrics is always present, empty or not")
	var out struct {
		Metrics []struct {
			Metric      string `xml:"Metric"`
			Granularity string `xml:"Granularity"`
		} `xml:"DescribeAutoScalingGroupsResult>AutoScalingGroups>member>EnabledMetrics>member"`
	}
	require.NoError(e.t, xml.Unmarshal(body, &out))
	names := []string{}
	for _, m := range out.Metrics {
		assert.Equal(e.t, "1Minute", m.Granularity)
		names = append(names, m.Metric)
	}
	return names
}

// EnableMetricsCollection answered InvalidAction until #1594. A consumer enables collection at deploy
// and reads EnabledMetrics back to skip the call when it is already on.
func TestAutoScaling_MetricsCollectionLifecycle(t *testing.T) {
	e := newASGEnv(t)
	e.createGroup("metrics", 0, 2, 0, nil)
	assert.Empty(t, e.asgEnabledMetrics("metrics"), "collection is off on a new group")

	body := e.mustASG("EnableMetricsCollection", map[string]string{
		"AutoScalingGroupName": "metrics", "Granularity": "1Minute",
		"Metrics.member.1": "GroupTotalInstances", "Metrics.member.2": "GroupDesiredCapacity",
	})
	assert.NotContains(t, string(body), "EnableMetricsCollectionResult", "the operation has no output shape")
	assert.Equal(t, []string{"GroupDesiredCapacity", "GroupTotalInstances"}, e.asgEnabledMetrics("metrics"),
		"reported in the published order, not request order")

	// An omitted Metrics enables every group metric.
	e.mustASG("EnableMetricsCollection", map[string]string{"AutoScalingGroupName": "metrics", "Granularity": "1Minute"})
	assert.Equal(t, asgAllMetrics, e.asgEnabledMetrics("metrics"))

	e.mustASG("DisableMetricsCollection", map[string]string{
		"AutoScalingGroupName": "metrics", "Metrics.member.1": "GroupMinSize",
	})
	assert.Equal(t, asgAllMetrics[1:], e.asgEnabledMetrics("metrics"), "a named metric is disabled alone")

	// An omitted Metrics disables every group metric.
	e.mustASG("DisableMetricsCollection", map[string]string{"AutoScalingGroupName": "metrics"})
	assert.Empty(t, e.asgEnabledMetrics("metrics"))
}

func TestAutoScaling_MetricsCollectionRefusals(t *testing.T) {
	e := newASGEnv(t)
	e.createGroup("metrics-refusals", 0, 2, 0, nil)

	cases := []struct {
		name   string
		action string
		params map[string]string
		code   string
	}{
		{"no group name", "EnableMetricsCollection", map[string]string{"Granularity": "1Minute"}, "ValidationError"},
		{"absent group", "EnableMetricsCollection",
			map[string]string{"AutoScalingGroupName": "nope", "Granularity": "1Minute"}, "ValidationError"},
		{"no granularity", "EnableMetricsCollection",
			map[string]string{"AutoScalingGroupName": "metrics-refusals"}, "ValidationError"},
		{"five-minute granularity", "EnableMetricsCollection",
			map[string]string{"AutoScalingGroupName": "metrics-refusals", "Granularity": "5Minute"}, "ValidationError"},
		{"unknown metric", "EnableMetricsCollection",
			map[string]string{"AutoScalingGroupName": "metrics-refusals", "Granularity": "1Minute", "Metrics.member.1": "CPUUtilization"},
			"ValidationError"},
		{"disable unknown metric", "DisableMetricsCollection",
			map[string]string{"AutoScalingGroupName": "metrics-refusals", "Metrics.member.1": "CPUUtilization"}, "ValidationError"},
		{"disable absent group", "DisableMetricsCollection", map[string]string{"AutoScalingGroupName": "nope"}, "ValidationError"},
	}
	for _, tc := range cases {
		status, code, _ := e.asgErr(tc.action, tc.params)
		assert.Equal(t, http.StatusBadRequest, status, tc.name)
		assert.Equal(t, tc.code, code, tc.name)
	}
	assert.Empty(t, e.asgEnabledMetrics("metrics-refusals"), "a refused request enables nothing")
}

func TestAutoScaling_DescribeMetricCollectionTypes(t *testing.T) {
	e := newASGEnv(t)
	body := e.mustASG("DescribeMetricCollectionTypes", nil)
	var out struct {
		Granularities []string `xml:"DescribeMetricCollectionTypesResult>Granularities>member>Granularity"`
		Metrics       []string `xml:"DescribeMetricCollectionTypesResult>Metrics>member>Metric"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), "%s", body)
	assert.Equal(t, []string{"1Minute"}, out.Granularities)
	assert.Equal(t, asgAllMetrics, out.Metrics)
}
