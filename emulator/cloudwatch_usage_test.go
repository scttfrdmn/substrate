package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cwUsageNow is the frozen clock the usage tests run at: 00:07:30, so the start of the
// 5-minute period containing it, laid from a minute-aligned start, is unambiguous.
var cwUsageNow = time.Date(2026, 3, 1, 0, 7, 30, 0, time.UTC)

// cwUsageAuth is the SigV4 header the usage tests sign with, so EC2 and CloudWatch resolve
// the same account and region.
func cwUsageAuth(service string) string {
	return "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20260301/us-east-1/" + service +
		"/aws4_request, SignedHeaders=host, Signature=fake"
}

// cwUsageEC2 sends an EC2 query request and returns the body, failing on a non-200.
func cwUsageEC2(t *testing.T, ts *emulator.TestServer, params map[string]string) string {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", cwUsageAuth("ec2"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", params["Action"], body)
	return string(body)
}

// cwUsageLaunch launches count instances of instanceType and returns their IDs.
func cwUsageLaunch(t *testing.T, ts *emulator.TestServer, instanceType string, count int, spot bool) []string {
	t.Helper()
	params := map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": instanceType,
		"MinCount": fmt.Sprint(count), "MaxCount": fmt.Sprint(count),
	}
	if spot {
		params["InstanceMarketOptions.MarketType"] = "spot"
	}
	var out struct {
		IDs []string `xml:"instancesSet>item>instanceId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(cwUsageEC2(t, ts, params)), &out))
	require.Len(t, out.IDs, count)
	return out.IDs
}

// cwUsageJSON sends a CloudWatch awsJson1_0 request, as boto3 does, and returns the status
// and body.
func cwUsageJSON(t *testing.T, ts *emulator.TestServer, op string, body any) (int, []byte) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(string(payload)))
	require.NoError(t, err)
	req.Host = "monitoring.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801."+op)
	req.Header.Set("Authorization", cwUsageAuth("monitoring"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, raw
}

// cwUsageDims is the dimension list of an EC2 vCPU usage metric.
func cwUsageDims(class string) []map[string]string {
	return []map[string]string{
		{"Name": "Service", "Value": "EC2"},
		{"Name": "Type", "Value": "Resource"},
		{"Name": "Resource", "Value": "vCPU"},
		{"Name": "Class", "Value": class},
	}
}

// cwUsageQuery is one GetMetricData query for an EC2 vCPU usage class.
func cwUsageQuery(id, class, stat string) map[string]any {
	return map[string]any{
		"Id": id,
		"MetricStat": map[string]any{
			"Metric": map[string]any{
				"Namespace": "AWS/Usage", "MetricName": "ResourceCount", "Dimensions": cwUsageDims(class),
			},
			"Period": 300, "Stat": stat,
		},
	}
}

// cwUsageResult is one decoded MetricDataResult.
type cwUsageResult struct {
	ID         string    `json:"Id"`
	Label      *string   `json:"Label"`
	Timestamps []float64 `json:"Timestamps"`
	Values     []float64 `json:"Values"`
	StatusCode string    `json:"StatusCode"`
}

// cwUsageGetMetricData runs GetMetricData over [start, end) and returns the results by Id.
func cwUsageGetMetricData(t *testing.T, ts *emulator.TestServer, start, end time.Time, queries ...map[string]any) map[string]cwUsageResult {
	t.Helper()
	status, raw := cwUsageJSON(t, ts, "GetMetricData", map[string]any{
		"StartTime": start.Unix(), "EndTime": end.Unix(), "MetricDataQueries": queries,
	})
	require.Equal(t, http.StatusOK, status, string(raw))
	var out struct {
		MetricDataResults []cwUsageResult `json:"MetricDataResults"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Len(t, out.MetricDataResults, len(queries), "one result per query")
	byID := make(map[string]cwUsageResult, len(queries))
	for _, r := range out.MetricDataResults {
		byID[r.ID] = r
	}
	return byID
}

// newCWUsageServer starts a server frozen at [cwUsageNow] running three m5.large Spot
// instances (6 vCPU — #1498's fleet, one over the 5-vCPU Spot default), one c5.xlarge and
// one g5.xlarge On-Demand (4 vCPU each), a stopped t3.micro and a terminated t3.large.
func newCWUsageServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cwUsageNow)

	cwUsageLaunch(t, ts, "m5.large", 3, true)
	cwUsageLaunch(t, ts, "c5.xlarge", 1, false)
	cwUsageLaunch(t, ts, "g5.xlarge", 1, false)
	stopped := cwUsageLaunch(t, ts, "t3.micro", 1, false)
	cwUsageEC2(t, ts, map[string]string{"Action": "StopInstances", "InstanceId.1": stopped[0]})
	terminated := cwUsageLaunch(t, ts, "t3.large", 1, false)
	cwUsageEC2(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": terminated[0]})
	return ts
}

// TestCWUsage_GetMetricDataReportsRunningVCPUs is #1498's repro: AWS/Usage ResourceCount
// for EC2 vCPUs reports the running instances' vCPUs, by quota class and purchase option.
func TestCWUsage_GetMetricDataReportsRunningVCPUs(t *testing.T) {
	t.Parallel()
	ts := newCWUsageServer(t)

	// [23:52:30, 00:07:30) rounds its start to 23:52:00; periods start at 23:52, 23:57,
	// 00:02 and 00:07, and 00:07 is the latest no later than the 00:07:30 clock.
	wantAt := float64(time.Date(2026, 3, 1, 0, 7, 0, 0, time.UTC).Unix())
	got := cwUsageGetMetricData(t, ts, cwUsageNow.Add(-15*time.Minute), cwUsageNow,
		cwUsageQuery("spot", "Standard/Spot", "Maximum"),
		cwUsageQuery("od", "Standard/OnDemand", "Maximum"),
		cwUsageQuery("gpu", "G/OnDemand", "Average"),
		cwUsageQuery("sum", "Standard/Spot", "Sum"),
		cwUsageQuery("samples", "Standard/Spot", "SampleCount"),
		cwUsageQuery("pct", "Standard/Spot", "p99"),
		// Nothing has ever run in these classes, so they have no metric at all.
		cwUsageQuery("p", "P/OnDemand", "Maximum"),
		cwUsageQuery("gspot", "G/Spot", "Maximum"),
		// A statistic substrate does not compute gets no datapoint.
		cwUsageQuery("rank", "Standard/Spot", "PR(1:5)"),
		map[string]any{"Id": "other", "MetricStat": map[string]any{
			"Metric": map[string]any{"Namespace": "AWS/EC2", "MetricName": "CPUUtilization"},
			"Period": 300, "Stat": "Average",
		}},
		map[string]any{"Id": "hidden", "ReturnData": false, "MetricStat": cwUsageQuery("x", "Standard/Spot", "Maximum")["MetricStat"]},
	)

	cases := []struct {
		id    string
		value float64
		empty bool
	}{
		{id: "spot", value: 6},
		{id: "od", value: 4}, // the c5.xlarge; the stopped t3.micro and terminated t3.large count nowhere
		{id: "gpu", value: 4},
		{id: "sum", value: 30}, // 6 vCPU in each of the period's five one-minute samples
		{id: "samples", value: 5},
		{id: "pct", value: 6},
		{id: "p", empty: true},
		{id: "gspot", empty: true},
		{id: "rank", empty: true},
		{id: "other", empty: true},
	}
	for _, tc := range cases {
		r := got[tc.id]
		assert.Equal(t, "Complete", r.StatusCode, tc.id)
		require.NotNil(t, r.Label, tc.id)
		if tc.empty {
			assert.Empty(t, r.Values, tc.id)
			assert.Empty(t, r.Timestamps, tc.id)
			continue
		}
		assert.Equal(t, []float64{tc.value}, r.Values, tc.id)
		assert.Equal(t, []float64{wantAt}, r.Timestamps, tc.id)
		assert.Equal(t, "ResourceCount", *r.Label, tc.id)
	}
	assert.Equal(t, "CPUUtilization", *got["other"].Label)

	// ReturnData=false carries only Id and StatusCode, as the reference's examples show.
	assert.Nil(t, got["hidden"].Label)
	assert.Nil(t, got["hidden"].Values)
	assert.Equal(t, "Complete", got["hidden"].StatusCode)
}

// TestCWUsage_UsageFollowsInstanceState checks that a terminated Spot fleet reads 0 rather
// than vanishing, and that a started instance counts again.
func TestCWUsage_UsageFollowsInstanceState(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cwUsageNow)

	ids := cwUsageLaunch(t, ts, "m5.large", 2, true)
	od := cwUsageLaunch(t, ts, "m5.xlarge", 1, false)
	window := func() map[string]cwUsageResult {
		return cwUsageGetMetricData(t, ts, cwUsageNow.Add(-5*time.Minute), cwUsageNow.Add(time.Minute),
			cwUsageQuery("spot", "Standard/Spot", "Maximum"),
			cwUsageQuery("od", "Standard/OnDemand", "Maximum"))
	}
	got := window()
	assert.Equal(t, []float64{4}, got["spot"].Values)
	assert.Equal(t, []float64{4}, got["od"].Values)

	cwUsageEC2(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0], "InstanceId.2": ids[1]})
	cwUsageEC2(t, ts, map[string]string{"Action": "StopInstances", "InstanceId.1": od[0]})
	got = window()
	assert.Equal(t, []float64{0}, got["spot"].Values, "a class that has been used reports 0, not nothing")
	assert.Equal(t, []float64{0}, got["od"].Values)

	cwUsageEC2(t, ts, map[string]string{"Action": "StartInstances", "InstanceId.1": od[0]})
	assert.Equal(t, []float64{4}, window()["od"].Values)
}

// TestCWUsage_DatapointWindows pins exactly which datapoint a window gets, through
// GetMetricStatistics.
func TestCWUsage_DatapointWindows(t *testing.T) {
	t.Parallel()
	ts := newCWUsageServer(t)
	// The clock is 2026-03-01T00:07:30Z, so "yesterday" is 28 February.
	today := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	yesterday := today.AddDate(0, 0, -1)
	at := func(day time.Time, h, m, sec int) time.Time {
		return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(sec)*time.Second)
	}

	cases := []struct {
		name       string
		start, end time.Time
		period     int
		want       *time.Time
	}{
		{"the period containing the clock", at(yesterday, 23, 52, 30), at(today, 0, 7, 30), 300, ptrTime(at(today, 0, 7, 0))},
		// A caller's EndTime=now is computed a moment before substrate reads its clock; the
		// window still reaches the current period, so it still gets that period's datapoint.
		{"EndTime a moment before the clock", at(yesterday, 23, 52, 30), at(today, 0, 7, 29), 300, ptrTime(at(today, 0, 7, 0))},
		// Periods from 23:50 put the clock in [00:05, 00:10); EndTime is exclusive, so a window
		// ending at 00:05 stops short of it.
		{"a window ending where the current period starts", at(yesterday, 23, 50, 0), at(today, 0, 5, 0), 300, nil},
		{"a window past the clock stops at the clock", at(today, 0, 0, 0), at(today, 1, 0, 0), 300, ptrTime(at(today, 0, 5, 0))},
		{"one-minute periods", at(today, 0, 0, 0), at(today, 0, 30, 0), 60, ptrTime(at(today, 0, 7, 0))},
		{"a window that opens after the clock", at(today, 0, 8, 0), at(today, 0, 20, 0), 60, nil},
		// No history is kept, so a window wholly in the past has no datapoint rather than
		// today's usage stamped on a period it was not measured in.
		{"a window wholly in the past", at(yesterday, 23, 0, 30), at(yesterday, 23, 2, 0), 60, nil},
		{"a window that closed an hour ago", at(yesterday, 22, 0, 0), at(yesterday, 23, 0, 0), 300, nil},
		// A start 15 to 63 days back rounds to the 5-minute clock interval, and one further
		// back to the hour.
		{"a start 20 days back", cwUsageNow.AddDate(0, 0, -20), cwUsageNow, 86400, ptrTime(at(today, 0, 5, 0))},
		{"a start 70 days back", cwUsageNow.AddDate(0, 0, -70), cwUsageNow, 86400, ptrTime(at(today, 0, 0, 0))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := cwUsageJSON(t, ts, "GetMetricStatistics", map[string]any{
				"Namespace": "AWS/Usage", "MetricName": "ResourceCount", "Dimensions": cwUsageDims("Standard/Spot"),
				"StartTime": tc.start.Unix(), "EndTime": tc.end.Unix(), "Period": tc.period,
				"Statistics": []string{"Maximum", "SampleCount"},
			})
			require.Equal(t, http.StatusOK, status, string(raw))
			var out struct {
				Label      string `json:"Label"`
				Datapoints []struct {
					Timestamp   float64  `json:"Timestamp"`
					Maximum     *float64 `json:"Maximum"`
					SampleCount *float64 `json:"SampleCount"`
					Sum         *float64 `json:"Sum"`
					Unit        string   `json:"Unit"`
				} `json:"Datapoints"`
			}
			require.NoError(t, json.Unmarshal(raw, &out))
			assert.Equal(t, "ResourceCount", out.Label)
			if tc.want == nil {
				assert.Empty(t, out.Datapoints)
				return
			}
			require.Len(t, out.Datapoints, 1)
			dp := out.Datapoints[0]
			assert.Equal(t, float64(tc.want.Unix()), dp.Timestamp)
			require.NotNil(t, dp.Maximum)
			assert.Equal(t, float64(6), *dp.Maximum)
			require.NotNil(t, dp.SampleCount)
			assert.Equal(t, float64(tc.period/60), *dp.SampleCount)
			assert.Nil(t, dp.Sum, "only the requested statistics are reported")
			assert.Equal(t, "None", dp.Unit)
		})
	}
}

// ptrTime returns a pointer to t.
func ptrTime(t time.Time) *time.Time { return &t }

// TestCWUsage_GetMetricStatisticsRefusals covers the input rules the reference publishes.
func TestCWUsage_GetMetricStatisticsRefusals(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cwUsageNow)

	base := func(edit func(map[string]any)) map[string]any {
		body := map[string]any{
			"Namespace": "AWS/Usage", "MetricName": "ResourceCount", "Dimensions": cwUsageDims("Standard/Spot"),
			"StartTime": cwUsageNow.Add(-time.Hour).Unix(), "EndTime": cwUsageNow.Unix(), "Period": 300,
			"Statistics": []string{"Maximum"},
		}
		edit(body)
		return body
	}
	cases := []struct {
		name string
		body map[string]any
		code string
	}{
		{"no Namespace", base(func(b map[string]any) { delete(b, "Namespace") }), "MissingRequiredParameterException"},
		{"no Period", base(func(b map[string]any) { delete(b, "Period") }), "MissingRequiredParameterException"},
		{"no statistics at all", base(func(b map[string]any) { delete(b, "Statistics") }), "MissingRequiredParameterException"},
		{"both kinds of statistic", base(func(b map[string]any) { b["ExtendedStatistics"] = []string{"p99"} }), "InvalidParameterCombinationException"},
		{"an unknown statistic", base(func(b map[string]any) { b["Statistics"] = []string{"Median"} }), "InvalidParameterValueException"},
		{"a period that is not a multiple of 60", base(func(b map[string]any) { b["Period"] = 90 }), "InvalidParameterValueException"},
		{"StartTime after EndTime", base(func(b map[string]any) { b["StartTime"] = cwUsageNow.Add(time.Hour).Unix() }), "InvalidParameterValueException"},
		{"more than 1,440 datapoints", base(func(b map[string]any) {
			b["StartTime"] = cwUsageNow.Add(-48 * time.Hour).Unix()
			b["Period"] = 60
		}), "InvalidParameterCombinationException"},
		// 00:07:30 yesterday rounds to 00:07:00, so [00:07:00, 00:07:30) the next day holds
		// 1,440 whole minutes and the start of a 1,441st.
		{"1,441 period starts, the last one partial", base(func(b map[string]any) {
			b["StartTime"] = cwUsageNow.Add(-24 * time.Hour).Unix()
			b["Period"] = 60
		}), "InvalidParameterCombinationException"},
	}
	t.Run("exactly 1,440 period starts", func(t *testing.T) {
		status, raw := cwUsageJSON(t, ts, "GetMetricStatistics", base(func(b map[string]any) {
			b["StartTime"] = cwUsageNow.Add(-24*time.Hour - 30*time.Second).Unix()
			b["EndTime"] = cwUsageNow.Add(-30 * time.Second).Unix()
			b["Period"] = 60
		}))
		assert.Equal(t, http.StatusOK, status, string(raw))
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := cwUsageJSON(t, ts, "GetMetricStatistics", tc.body)
			assert.Equal(t, http.StatusBadRequest, status, string(raw))
			assert.Contains(t, string(raw), tc.code)
		})
	}
}

// TestCWUsage_ListMetrics lists the classes the account has used, and honors a dimension
// filter.
func TestCWUsage_ListMetrics(t *testing.T) {
	t.Parallel()
	ts := newCWUsageServer(t)

	list := func(body map[string]any) []string {
		status, raw := cwUsageJSON(t, ts, "ListMetrics", body)
		require.Equal(t, http.StatusOK, status, string(raw))
		var out struct {
			Metrics []struct {
				Namespace  string `json:"Namespace"`
				MetricName string `json:"MetricName"`
				Dimensions []struct {
					Name  string `json:"Name"`
					Value string `json:"Value"`
				} `json:"Dimensions"`
			} `json:"Metrics"`
		}
		require.NoError(t, json.Unmarshal(raw, &out))
		var classes []string
		for _, m := range out.Metrics {
			require.Equal(t, "AWS/Usage", m.Namespace)
			require.Equal(t, "ResourceCount", m.MetricName)
			require.Len(t, m.Dimensions, 4)
			classes = append(classes, m.Dimensions[3].Value)
		}
		return classes
	}

	assert.Equal(t, []string{"Standard/OnDemand", "G/OnDemand", "Standard/Spot"},
		list(map[string]any{"Namespace": "AWS/Usage"}))
	assert.Equal(t, []string{"Standard/Spot"}, list(map[string]any{
		"Namespace": "AWS/Usage", "Dimensions": []map[string]string{{"Name": "Class", "Value": "Standard/Spot"}},
	}))
	assert.Empty(t, list(map[string]any{"Namespace": "AWS/Usage", "MetricName": "CallCount"}))
}

// TestCWUsage_QueryProtocolAndDeterminism renders GetMetricStatistics over the query
// protocol and checks the same request answers the same bytes twice.
func TestCWUsage_QueryProtocolAndDeterminism(t *testing.T) {
	t.Parallel()
	ts := newCWUsageServer(t)

	form := url.Values{
		"Action": {"GetMetricStatistics"}, "Namespace": {"AWS/Usage"}, "MetricName": {"ResourceCount"},
		"StartTime": {"2026-02-28T23:52:30Z"}, "EndTime": {"2026-03-01T00:07:30Z"}, "Period": {"300"},
		"Statistics.member.1": {"Maximum"},
	}
	for i, d := range cwUsageDims("Standard/OnDemand") {
		form.Set(fmt.Sprintf("Dimensions.member.%d.Name", i+1), d["Name"])
		form.Set(fmt.Sprintf("Dimensions.member.%d.Value", i+1), d["Value"])
	}
	send := func() string {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
		require.NoError(t, err)
		req.Host = "monitoring.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", cwUsageAuth("monitoring"))
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
		// The RequestId differs per request; everything else must not.
		text := string(body)
		return text[:strings.Index(text, "<ResponseMetadata>")]
	}
	first := send()
	assert.Contains(t, first, "<GetMetricStatisticsResult><Label>ResourceCount</Label><Datapoints><member>"+
		"<Timestamp>2026-03-01T00:07:00.000Z</Timestamp><Maximum>4</Maximum><Unit>None</Unit></member>")
	assert.Equal(t, first, send())
}

// cwUsageControl sends a control-plane request and fails on a non-2xx answer.
func cwUsageControl(t *testing.T, ts *emulator.TestServer, method, path, body string) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300, "%s %s: %s", method, path, raw)
}

// cwUsageSpot reads the Standard/Spot usage over the window around the frozen clock.
func cwUsageSpot(t *testing.T, ts *emulator.TestServer) []float64 {
	t.Helper()
	got := cwUsageGetMetricData(t, ts, cwUsageNow.Add(-5*time.Minute), cwUsageNow.Add(time.Minute),
		cwUsageQuery("spot", "Standard/Spot", "Maximum"))
	return got["spot"].Values
}

// TestCWUsage_OpenSpotRequestsCount checks that an open Spot request awaiting fulfillment
// counts against its Spot class, and stops counting once its instance runs — so a fulfilled
// request and its instance are not counted twice.
func TestCWUsage_OpenSpotRequestsCount(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cwUsageNow)

	// A capacity seed leaves the requests open with no instance.
	cwUsageControl(t, ts, http.MethodPost, "/v1/ec2/spot-request-outcomes",
		`{"instanceType":"*","statusCode":"capacity-not-available"}`)
	cwUsageEC2(t, ts, map[string]string{
		"Action": "RequestSpotInstances", "InstanceCount": "2", "Type": "one-time",
		"LaunchSpecification.ImageId": ec2TestImage, "LaunchSpecification.InstanceType": "m5.large",
	})
	assert.Equal(t, []float64{4}, cwUsageSpot(t, ts), "two open m5.large requests will use 4 vCPU")

	// With the seed gone, the next describe fulfills them; the running instances now carry the
	// 4 vCPU and the fulfilled requests carry none.
	cwUsageControl(t, ts, http.MethodDelete, "/v1/ec2/spot-request-outcomes", "")
	body := cwUsageEC2(t, ts, map[string]string{"Action": "DescribeSpotInstanceRequests"})
	require.Equal(t, 2, strings.Count(body, "<state>active</state>"), body)
	assert.Equal(t, []float64{4}, cwUsageSpot(t, ts))
}

// TestCWUsage_PendingInstancesDoNotCount checks that an instance an instance-state seed has
// still reporting pending does not count, and that reading usage spends none of the seed's
// observations.
func TestCWUsage_PendingInstancesDoNotCount(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cwUsageNow)

	cwUsageControl(t, ts, http.MethodPost, "/v1/ec2/instance-state",
		`{"instanceId":"*","transientObservations":2}`)
	ids := cwUsageLaunch(t, ts, "m5.large", 1, true)
	describe := func() string {
		var out struct {
			State string `xml:"reservationSet>item>instancesSet>item>instanceState>name"`
		}
		require.NoError(t, xml.Unmarshal([]byte(cwUsageEC2(t, ts, map[string]string{
			"Action": "DescribeInstances", "InstanceId.1": ids[0],
		})), &out))
		return out.State
	}

	// Usage is read three times before any describe; none of those reads may spend one of the
	// two pending observations.
	for range 3 {
		assert.Equal(t, []float64{0}, cwUsageSpot(t, ts), "a pending instance does not count")
	}
	assert.Equal(t, "pending", describe())
	assert.Equal(t, []float64{0}, cwUsageSpot(t, ts))
	assert.Equal(t, "pending", describe())
	assert.Equal(t, []float64{2}, cwUsageSpot(t, ts), "the countdown is spent, so it reports running")
	assert.Equal(t, "running", describe())
}
