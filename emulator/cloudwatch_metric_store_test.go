package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// PutMetricData's datapoints are stored and read back (#1595). Every test freezes the clock at
// cwStoreNow and publishes timestamps relative to it, so none depends on real elapsed time.

// cwStoreNow is the frozen instant the store tests run at.
var cwStoreNow = time.Date(2026, 11, 20, 12, 30, 0, 0, time.UTC)

// cwEnv is a server with CloudWatch and the tagging API on a frozen clock.
type cwEnv struct {
	t   *testing.T
	srv http.Handler
	tc  *emulator.TimeController
}

func newCWEnv(t *testing.T, options map[string]any) *cwEnv {
	t.Helper()
	return newCWEnvWithState(t, emulator.NewMemoryStateManager(), options)
}

// newCWEnvWithState is newCWEnv over a caller-supplied state store, for tests that need it to fail.
func newCWEnvWithState(t *testing.T, state emulator.StateManager, options map[string]any) *cwEnv {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(cwStoreNow)
	tc.Freeze()

	opts := map[string]any{"time_controller": tc}
	for k, v := range options {
		opts[k] = v
	}
	cw := &emulator.CloudWatchPlugin{}
	require.NoError(t, cw.Initialize(context.TODO(), emulator.PluginConfig{State: state, Logger: logger, Options: opts}))
	registry.Register(cw)
	tagging := &emulator.TaggingPlugin{}
	require.NoError(t, tagging.Initialize(context.TODO(), emulator.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc, "registry": registry},
	}))
	registry.Register(tagging)
	return &cwEnv{t: t, srv: emulator.NewServer(*cfg, registry, store, state, tc, logger), tc: tc}
}

// do sends one awsJson1_0 CloudWatch request and returns its status and decoded body.
func (e *cwEnv) do(op string, body any) (int, map[string]any) {
	e.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(e.t, err)
	resp := cwPost(e.t, e.srv, "/", "application/x-amz-json-1.0", string(raw),
		map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801." + op})
	defer resp.Body.Close() //nolint:errcheck
	out := map[string]any{}
	if b := cwReadBytes(e.t, resp); len(b) > 0 {
		require.NoError(e.t, json.Unmarshal(b, &out), "%s: %s", op, b)
	}
	return resp.StatusCode, out
}

// ok sends a request that must succeed.
func (e *cwEnv) ok(op string, body any) map[string]any {
	e.t.Helper()
	status, out := e.do(op, body)
	require.Equal(e.t, http.StatusOK, status, "%s: %v", op, out)
	return out
}

// refused sends a request that must fail with code, and returns the message.
func (e *cwEnv) refused(op string, body any, code string) string {
	e.t.Helper()
	status, out := e.do(op, body)
	require.Equal(e.t, http.StatusBadRequest, status, "%s: %v", op, out)
	assert.Contains(e.t, out["__type"], code, "%s: %v", op, out)
	msg, _ := out["message"].(string)
	return msg
}

// at is a timestamp minutes after cwStoreNow, in the epoch seconds awsJson1_0 sends.
func at(minutes float64) float64 {
	return float64(cwStoreNow.Add(time.Duration(minutes * float64(time.Minute))).Unix())
}

// cwResult is one decoded MetricDataResult.
type cwResult struct {
	ID         string    `json:"Id"`
	Label      string    `json:"Label"`
	Timestamps []float64 `json:"Timestamps"`
	Values     []float64 `json:"Values"`
	StatusCode string    `json:"StatusCode"`
	Messages   []struct {
		Code, Value string
	} `json:"Messages"`
}

// results decodes a GetMetricData response.
func results(t *testing.T, out map[string]any) ([]cwResult, string) {
	t.Helper()
	raw, err := json.Marshal(out)
	require.NoError(t, err)
	var doc struct {
		MetricDataResults []cwResult `json:"MetricDataResults"`
		NextToken         string     `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	return doc.MetricDataResults, doc.NextToken
}

func metricStat(id, ns, name, stat string, period int, dims ...map[string]string) map[string]any {
	return map[string]any{"Id": id, "MetricStat": map[string]any{
		"Metric": map[string]any{"Namespace": ns, "MetricName": name, "Dimensions": dims},
		"Period": period, "Stat": stat,
	}}
}

func TestCWMetrics_GetMetricDataReturnsPublishedValues(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	dimsAB := []map[string]string{{"Name": "Deployment", "Value": "dev"}, {"Name": "Pool", "Value": "gpu"}}
	dimsBA := []map[string]string{{"Name": "Pool", "Value": "gpu"}, {"Name": "Deployment", "Value": "dev"}}
	e.ok("PutMetricData", map[string]any{"Namespace": "aegc", "MetricData": []map[string]any{
		{"MetricName": "Tasks", "Dimensions": dimsAB, "Value": 2, "Timestamp": at(-10)},
		{"MetricName": "Tasks", "Dimensions": dimsBA, "Value": 4, "Timestamp": at(-9.5)},
		{"MetricName": "Tasks", "Dimensions": dimsAB, "Values": []float64{1, 10}, "Counts": []float64{3, 1}, "Timestamp": at(-5)},
		{"MetricName": "Tasks", "Dimensions": dimsAB, "StatisticValues": map[string]any{
			"SampleCount": 2, "Sum": 6, "Minimum": 3, "Maximum": 3}, "Timestamp": at(-14)},
		{"MetricName": "Tasks", "Value": 100, "Timestamp": at(-5)}, // a different series: no dimensions
	}})

	for _, tc := range []struct {
		stat       string
		wantValues []float64 // newest first
	}{
		{"Sum", []float64{13, 6, 6}},
		{"SampleCount", []float64{4, 2, 2}},
		{"Average", []float64{3.25, 3, 3}},
		{"Minimum", []float64{1, 2, 3}},
		{"Maximum", []float64{10, 4, 3}},
		{"p50", []float64{1, 2, 3}},
		{"p100", []float64{10, 4, 3}},
		{"p0", []float64{1, 2, 3}},
		{"p80", []float64{10, 4, 3}},
		{"tm90", nil},
	} {
		t.Run(tc.stat, func(t *testing.T) {
			out := e.ok("GetMetricData", map[string]any{
				"StartTime": at(-30), "EndTime": at(1),
				"MetricDataQueries": []any{metricStat("m1", "aegc", "Tasks", tc.stat, 300, dimsBA...)},
			})
			got, next := results(t, out)
			require.Len(t, got, 1)
			assert.Empty(t, next)
			assert.Equal(t, "Tasks", got[0].Label)
			assert.Equal(t, "Complete", got[0].StatusCode)
			if tc.wantValues == nil {
				assert.Empty(t, got[0].Values, "an extended statistic substrate does not compute reports nothing")
				return
			}
			assert.Equal(t, tc.wantValues, got[0].Values)
			// Periods are laid from the start rounded to the minute: 12:00, 12:05, ..., and the
			// default ScanBy is TimestampDescending.
			assert.Equal(t, []float64{at(-5), at(-10), at(-15)}, got[0].Timestamps)
		})
	}
}

func TestCWMetrics_ScanByUnitAndMissingTimestamp(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricData", map[string]any{"Namespace": "app", "MetricData": []map[string]any{
		{"MetricName": "Latency", "Value": 5, "Unit": "Milliseconds", "Timestamp": at(-3)},
		{"MetricName": "Latency", "Value": 7, "Unit": "Seconds", "Timestamp": at(-2)},
		{"MetricName": "Latency", "Value": 9}, // no Timestamp: the simulated clock
	}})
	query := func(scanBy, unit string) []cwResult {
		q := metricStat("l", "app", "Latency", "Sum", 60)
		if unit != "" {
			q["MetricStat"].(map[string]any)["Unit"] = unit
		}
		body := map[string]any{"StartTime": at(-10), "EndTime": at(10), "MetricDataQueries": []any{q}}
		if scanBy != "" {
			body["ScanBy"] = scanBy
		}
		got, _ := results(t, e.ok("GetMetricData", body))
		return got
	}
	got := query("TimestampAscending", "")
	assert.Equal(t, []float64{at(-3), at(-2), at(0)}, got[0].Timestamps)
	assert.Equal(t, []float64{5, 7, 9}, got[0].Values)

	got = query("", "Seconds")
	assert.Equal(t, []float64{7}, got[0].Values, "a Unit reads only the points published with it")
	got = query("", "None")
	assert.Equal(t, []float64{9}, got[0].Values, "a datum that named no unit was published with None")

	e.refused("GetMetricData", map[string]any{"StartTime": at(-1), "EndTime": at(1), "ScanBy": "Sideways",
		"MetricDataQueries": []any{metricStat("l", "app", "Latency", "Sum", 60)}}, "InvalidParameterValue")
}

func TestCWMetrics_GetMetricDataPaginates(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	var data []map[string]any
	for i := range 5 {
		data = append(data, map[string]any{"MetricName": "Hits", "Value": i + 1, "Timestamp": at(float64(-5 + i))})
	}
	e.ok("PutMetricData", map[string]any{"Namespace": "web", "MetricData": data})
	queries := []any{
		metricStat("a", "web", "Hits", "Sum", 60),
		map[string]any{"Id": "hidden", "ReturnData": false, "MetricStat": metricStat("x", "web", "Hits", "Sum", 60)["MetricStat"]},
		metricStat("b", "web", "Hits", "Maximum", 60),
	}
	var values []float64
	var statuses []string
	token := ""
	for page := 0; page < 10; page++ {
		body := map[string]any{"StartTime": at(-10), "EndTime": at(1), "MaxDatapoints": 4,
			"ScanBy": "TimestampAscending", "MetricDataQueries": queries}
		if token != "" {
			body["NextToken"] = token
		}
		got, next := results(t, e.ok("GetMetricData", body))
		require.Len(t, got, 3, "every query is answered on every page")
		assert.Equal(t, "hidden", got[1].ID)
		assert.Empty(t, got[1].Values)
		n := 0
		for _, r := range []cwResult{got[0], got[2]} {
			values = append(values, r.Values...)
			statuses = append(statuses, r.StatusCode)
			n += len(r.Values)
		}
		assert.LessOrEqual(t, n, 4)
		if next == "" {
			break
		}
		token = next
	}
	assert.Equal(t, []float64{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}, values)
	// A result with datapoints beyond the page reports PartialData, whether or not the page
	// carries any of its own.
	assert.Equal(t, []string{"PartialData", "PartialData", "Complete", "PartialData", "Complete", "Complete"}, statuses)

	e.refused("GetMetricData", map[string]any{"StartTime": at(-10), "EndTime": at(1), "NextToken": "bogus",
		"MetricDataQueries": queries}, "InvalidNextToken")
	e.refused("GetMetricData", map[string]any{"StartTime": at(-10), "EndTime": at(1), "MaxDatapoints": 0,
		"MetricDataQueries": queries}, "InvalidParameterValue")
}

func TestCWMetrics_Expressions(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricData", map[string]any{"Namespace": "svc", "MetricData": []map[string]any{
		{"MetricName": "Errors", "Value": 2, "Timestamp": at(-2)},
		{"MetricName": "Errors", "Value": 0, "Timestamp": at(-1)},
		{"MetricName": "Requests", "Value": 10, "Timestamp": at(-2)},
		{"MetricName": "Requests", "Value": 0, "Timestamp": at(-1)},
		{"MetricName": "Requests", "Value": 5, "Timestamp": at(-3)},
	}})
	hide := func(q map[string]any) map[string]any { q["ReturnData"] = false; return q }
	for _, tc := range []struct {
		name, expr  string
		wantTS      []float64
		wantValues  []float64
		wantMessage string
	}{
		{name: "rate", expr: "(e / r) * 100", wantTS: []float64{at(-2)}, wantValues: []float64{20}},
		{name: "sum", expr: "e + r", wantTS: []float64{at(-1), at(-2)}, wantValues: []float64{0, 12}},
		{name: "scalar left", expr: "100 - r", wantTS: []float64{at(-1), at(-2), at(-3)}, wantValues: []float64{100, 90, 95}},
		{name: "negation", expr: "-r * 2", wantTS: []float64{at(-1), at(-2), at(-3)}, wantValues: []float64{0, -20, -10}},
		{name: "chained", expr: "rate2 + 1", wantTS: []float64{at(-2)}, wantValues: []float64{21}},
		{name: "function", expr: "SUM(METRICS())", wantMessage: "the function SUM is not evaluated"},
		{name: "unknown id", expr: "e + zz", wantMessage: "the Id zz names no query"},
		{name: "constant", expr: "1 + 2", wantMessage: "constant"},
		{name: "cycle", expr: "loop + 1", wantMessage: "refers back to itself"},
		{name: "syntax", expr: "e +", wantMessage: "ends early"},
		{name: "trailing", expr: "e r", wantMessage: "unexpected"},
		{name: "paren", expr: "(e + r", wantMessage: "missing closing parenthesis"},
		{name: "symbol", expr: "e % r", wantMessage: "unexpected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := e.ok("GetMetricData", map[string]any{
				"StartTime": at(-10), "EndTime": at(1),
				"MetricDataQueries": []any{
					map[string]any{"Id": "x", "Expression": tc.expr, "Label": tc.name},
					hide(metricStat("e", "svc", "Errors", "Sum", 60)),
					hide(metricStat("r", "svc", "Requests", "Sum", 60)),
					hide(map[string]any{"Id": "rate2", "Expression": "(e / r) * 100"}),
					hide(map[string]any{"Id": "loop", "Expression": "loop + 1"}),
				},
			})
			got, _ := results(t, out)
			require.Len(t, got, 5)
			assert.Equal(t, tc.name, got[0].Label)
			if tc.wantMessage != "" {
				assert.Empty(t, got[0].Values)
				require.Len(t, got[0].Messages, 1)
				assert.Equal(t, "Unsupported", got[0].Messages[0].Code)
				assert.Contains(t, got[0].Messages[0].Value, tc.wantMessage)
				return
			}
			assert.Empty(t, got[0].Messages)
			assert.Equal(t, tc.wantTS, got[0].Timestamps)
			assert.Equal(t, tc.wantValues, got[0].Values)
		})
	}
}

func TestCWMetrics_PutMetricDataValidation(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	tooMany := make([]map[string]string, 31)
	for i := range tooMany {
		tooMany[i] = map[string]string{"Name": fmt.Sprintf("d%d", i), "Value": "v"}
	}
	values := make([]float64, 151)
	thousandAndOne := make([]map[string]any, 1001)
	for i := range thousandAndOne {
		thousandAndOne[i] = map[string]any{"MetricName": "M", "Value": 1}
	}
	datum := func(fields map[string]any) map[string]any {
		d := map[string]any{"MetricName": "M"}
		for k, v := range fields {
			d[k] = v
		}
		return d
	}
	for _, tc := range []struct {
		name string
		body map[string]any
		code string
	}{
		{"no namespace", map[string]any{"MetricData": []any{datum(nil)}}, "MissingRequiredParameter"},
		{"colon namespace", map[string]any{"Namespace": ":x", "MetricData": []any{datum(nil)}}, "InvalidParameterValue"},
		{"no metric name", map[string]any{"Namespace": "n", "MetricData": []any{map[string]any{"Value": 1}}}, "MissingRequiredParameter"},
		{"long metric name", map[string]any{"Namespace": "n", "MetricData": []any{
			map[string]any{"MetricName": strings.Repeat("m", 256)}}}, "InvalidParameterValue"},
		{"value too large", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{"Value": 1e109})}}, "InvalidParameterValue"},
		{"values too large", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{"Values": []float64{1e109}})}}, "InvalidParameterValue"},
		{"31 dimensions", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{"Dimensions": tooMany})}}, "InvalidParameterValue"},
		{"duplicate dimension", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Dimensions": []map[string]string{{"Name": "a", "Value": "1"}, {"Name": "a", "Value": "2"}}})}}, "InvalidParameterValue"},
		{"empty dimension value", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Dimensions": []map[string]string{{"Name": "a", "Value": ""}}})}}, "InvalidParameterValue"},
		{"long dimension name", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Dimensions": []map[string]string{{"Name": strings.Repeat("a", 256), "Value": "1"}}})}}, "InvalidParameterValue"},
		{"151 values", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{"Values": values})}}, "InvalidParameterValue"},
		{"counts mismatch", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Values": []float64{1, 2}, "Counts": []float64{1}})}}, "InvalidParameterValue"},
		{"negative count", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Values": []float64{1}, "Counts": []float64{-1}})}}, "InvalidParameterValue"},
		{"counts without values", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Counts": []float64{1}})}}, "InvalidParameterCombination"},
		{"value and values", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Value": 1, "Values": []float64{1}})}}, "InvalidParameterCombination"},
		{"values and statistics", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Values": []float64{1}, "StatisticValues": map[string]any{"SampleCount": 1, "Sum": 1, "Minimum": 1, "Maximum": 1}})}},
			"InvalidParameterCombination"},
		{"partial statistics", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"StatisticValues": map[string]any{"SampleCount": 1, "Sum": 1}})}}, "MissingRequiredParameter"},
		{"zero sample count", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"StatisticValues": map[string]any{"SampleCount": 0, "Sum": 1, "Minimum": 1, "Maximum": 1}})}}, "InvalidParameterValue"},
		{"min above max", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"StatisticValues": map[string]any{"SampleCount": 1, "Sum": 1, "Minimum": 2, "Maximum": 1}})}}, "InvalidParameterValue"},
		{"timestamp too old", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Value": 1, "Timestamp": at(-15 * 24 * 60)})}}, "InvalidParameterValue"},
		{"timestamp too new", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{
			"Value": 1, "Timestamp": at(121)})}}, "InvalidParameterValue"},
		{"bad unit", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{"Value": 1, "Unit": "Furlongs"})}}, "InvalidParameterValue"},
		{"bad resolution", map[string]any{"Namespace": "n", "MetricData": []any{datum(map[string]any{"Value": 1, "StorageResolution": 5})}}, "InvalidParameterValue"},
		{"1,001 metrics", map[string]any{"Namespace": "n", "MetricData": thousandAndOne}, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.refused("PutMetricData", tc.body, tc.code)
		})
	}

	// A refused request stores nothing, not even the valid datum before the invalid one.
	e.refused("PutMetricData", map[string]any{"Namespace": "atomic", "MetricData": []any{
		map[string]any{"MetricName": "Good", "Value": 1}, map[string]any{"MetricName": "Bad", "Value": 1e109},
	}}, "InvalidParameterValue")
	out := e.ok("ListMetrics", map[string]any{"Namespace": "atomic"})
	assert.Empty(t, out["Metrics"])

	// The query protocol spells NaN as text, and it is refused too.
	resp := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded",
		"Action=PutMetricData&Namespace=n&MetricData.member.1.MetricName=M&MetricData.member.1.Value=NaN", nil)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(cwReadBytes(t, resp)), "InvalidParameterValue")
}

func TestCWMetrics_HighResolutionAndEntityMetricData(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricData", map[string]any{"Namespace": "hr", "MetricData": []map[string]any{
		{"MetricName": "Q", "Value": 1, "StorageResolution": 1, "Timestamp": at(-1) + 12},
		{"MetricName": "Q", "Value": 2, "StorageResolution": 60, "Timestamp": at(-1) + 42},
	}, "EntityMetricData": []map[string]any{{
		"Entity":     map[string]any{"KeyAttributes": map[string]string{"Type": "Service", "Name": "svc"}},
		"MetricData": []map[string]any{{"MetricName": "Q", "Value": 4, "StorageResolution": 1, "Timestamp": at(-1) + 25}},
	}}, "StrictEntityValidation": true})
	got, _ := results(t, e.ok("GetMetricData", map[string]any{
		"StartTime": at(-1), "EndTime": at(0), "ScanBy": "TimestampAscending",
		"MetricDataQueries": []any{metricStat("q", "hr", "Q", "Sum", 10)},
	}))
	// The regular-resolution point counts at the start of its minute; the high-resolution ones at
	// their own seconds.
	assert.Equal(t, []float64{at(-1), at(-1) + 10, at(-1) + 20}, got[0].Timestamps)
	assert.Equal(t, []float64{2, 1, 4}, got[0].Values)
}

func TestCWMetrics_RetentionCap(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, map[string]any{"max_datapoints_per_series": 3})
	// Points across two hours, published out of order, so a trim both shortens an hour's chunk
	// and drops one whole.
	for _, m := range []float64{-70, -5, -62, -1, -2} {
		e.ok("PutMetricData", map[string]any{"Namespace": "cap", "MetricData": []map[string]any{
			{"MetricName": "C", "Value": m, "Timestamp": at(m)},
		}})
	}
	got, _ := results(t, e.ok("GetMetricData", map[string]any{
		"StartTime": at(-120), "EndTime": at(1), "ScanBy": "TimestampAscending",
		"MetricDataQueries": []any{metricStat("c", "cap", "C", "Sum", 60)},
	}))
	assert.Equal(t, []float64{-5, -2, -1}, got[0].Values, "the oldest points are dropped first")
}

func TestCWMetrics_GetMetricStatistics(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	dims := []map[string]string{{"Name": "Queue", "Value": "jobs"}}
	e.ok("PutMetricData", map[string]any{"Namespace": "q", "MetricData": []map[string]any{
		{"MetricName": "Depth", "Dimensions": dims, "Values": []float64{1, 2, 3, 4}, "Unit": "Count", "Timestamp": at(-4)},
		{"MetricName": "Depth", "Dimensions": dims, "Value": 9, "Unit": "Count", "Timestamp": at(-2)},
		{"MetricName": "Depth", "Dimensions": dims, "Value": 9, "Unit": "Percent", "Timestamp": at(-1)},
	}})
	out := e.ok("GetMetricStatistics", map[string]any{
		"Namespace": "q", "MetricName": "Depth", "Dimensions": dims,
		"StartTime": at(-10), "EndTime": at(1), "Period": 120, "Statistics": []string{"Sum", "SampleCount", "Maximum"},
	})
	assert.Equal(t, "Depth", out["Label"])
	dps := out["Datapoints"].([]any)
	require.Len(t, dps, 2)
	first, second := dps[0].(map[string]any), dps[1].(map[string]any)
	assert.Equal(t, at(-4), first["Timestamp"])
	assert.Equal(t, 10.0, first["Sum"])
	assert.Equal(t, 4.0, first["SampleCount"])
	assert.Equal(t, "Count", first["Unit"])
	assert.Equal(t, 18.0, second["Sum"])
	assert.Equal(t, "None", second["Unit"], "points of two units report None")
	assert.NotContains(t, first, "Average")

	out = e.ok("GetMetricStatistics", map[string]any{
		"Namespace": "q", "MetricName": "Depth", "Dimensions": dims, "Unit": "Count",
		"StartTime": at(-10), "EndTime": at(1), "Period": 600, "ExtendedStatistics": []string{"p50", "p99"},
	})
	dps = out["Datapoints"].([]any)
	require.Len(t, dps, 1)
	assert.Equal(t, map[string]any{"p50": 3.0, "p99": 9.0}, dps[0].(map[string]any)["ExtendedStatistics"])

	// The query protocol renders the map as entry/key/value.
	resp := cwPost(t, e.srv, "/", "application/x-www-form-urlencoded",
		"Action=GetMetricStatistics&Namespace=q&MetricName=Depth&Dimensions.member.1.Name=Queue&Dimensions.member.1.Value=jobs"+
			fmt.Sprintf("&StartTime=%d&EndTime=%d", int64(at(-10)), int64(at(1)))+
			"&Period=600&Unit=Count&ExtendedStatistics.member.1=p50", nil)
	defer resp.Body.Close() //nolint:errcheck
	body := string(cwReadBytes(t, resp))
	assert.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "<ExtendedStatistics><entry><key>p50</key><value>3</value></entry></ExtendedStatistics>")

	e.refused("GetMetricStatistics", map[string]any{"Namespace": "q", "MetricName": "Depth",
		"StartTime": at(-10), "EndTime": at(1), "Period": 60,
		"ExtendedStatistics": []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11"}}, "InvalidParameterValue")
}

func TestCWMetrics_ListMetrics(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutMetricData", map[string]any{"Namespace": "aegc", "MetricData": []map[string]any{
		{"MetricName": "Tasks", "Value": 1, "Dimensions": []map[string]string{{"Name": "Pool", "Value": "gpu"}, {"Name": "Deployment", "Value": "dev"}}},
		{"MetricName": "Tasks", "Value": 1, "Dimensions": []map[string]string{{"Name": "Pool", "Value": "cpu"}}},
		{"MetricName": "Idle"},
	}})
	e.ok("PutMetricData", map[string]any{"Namespace": "other", "MetricData": []map[string]any{
		{"MetricName": "Old", "Value": 1, "Timestamp": at(-60 * 5)},
	}})

	type metric struct {
		MetricName, Namespace string
		Dimensions            []struct{ Name, Value string }
	}
	list := func(body map[string]any) ([]metric, string) {
		out := e.ok("ListMetrics", body)
		raw, err := json.Marshal(out)
		require.NoError(t, err)
		var doc struct {
			Metrics   []metric
			NextToken string
		}
		require.NoError(t, json.Unmarshal(raw, &doc))
		return doc.Metrics, doc.NextToken
	}

	got, _ := list(map[string]any{"Namespace": "aegc"})
	require.Len(t, got, 3)
	assert.Equal(t, "Idle", got[0].MetricName, "a datum with no value still names its metric")
	assert.Equal(t, []struct{ Name, Value string }{{"Deployment", "dev"}, {"Pool", "gpu"}}, got[1].Dimensions,
		"dimensions are listed in name order")

	got, _ = list(map[string]any{"Namespace": "aegc", "Dimensions": []map[string]string{{"Name": "Pool", "Value": "cpu"}}})
	require.Len(t, got, 1)
	assert.Equal(t, "cpu", got[0].Dimensions[0].Value)
	got, _ = list(map[string]any{"Dimensions": []map[string]string{{"Name": "Deployment"}}})
	require.Len(t, got, 1, "a filter naming only a dimension's name matches any value")

	got, _ = list(map[string]any{})
	assert.Len(t, got, 4)

	// RecentlyActive keeps what received data in the last three hours of the simulated clock.
	e.tc.SetTime(cwStoreNow.Add(4 * time.Hour))
	got, _ = list(map[string]any{"RecentlyActive": "PT3H"})
	assert.Empty(t, got)
	e.tc.SetTime(cwStoreNow.Add(2 * time.Hour))
	got, _ = list(map[string]any{"RecentlyActive": "PT3H"})
	assert.Len(t, got, 4, "RecentlyActive reads when the data was published, not its timestamp")
	e.refused("ListMetrics", map[string]any{"RecentlyActive": "PT1H"}, "InvalidParameterValue")
	e.refused("ListMetrics", map[string]any{"NextToken": "bogus"}, "InvalidNextToken")

	// Pages of 500.
	big := make([]map[string]any, 501)
	for i := range big {
		big[i] = map[string]any{"MetricName": fmt.Sprintf("m%04d", i), "Value": 1}
	}
	e.ok("PutMetricData", map[string]any{"Namespace": "big", "MetricData": big})
	page, next := list(map[string]any{"Namespace": "big"})
	assert.Len(t, page, 500)
	require.NotEmpty(t, next)
	page, next = list(map[string]any{"Namespace": "big", "NextToken": next})
	assert.Len(t, page, 1)
	assert.Empty(t, next)
}

func TestCWMetrics_ConfigValidation(t *testing.T) {
	t.Parallel()
	cfg := emulator.DefaultConfig()
	assert.Equal(t, 10080, cfg.CloudWatch.MaxDatapointsPerSeries)
	cfg.CloudWatch.MaxDatapointsPerSeries = -1
	assert.ErrorContains(t, emulator.Validate(cfg), "cloudwatch.max_datapoints_per_series")
}

func TestCWMetrics_ConfiguredCapReachesThePlugin(t *testing.T) {
	t.Parallel()
	cfg := emulator.DefaultConfig()
	cfg.CloudWatch.MaxDatapointsPerSeries = 2
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(cwStoreNow)
	tc.Freeze()
	require.NoError(t, emulator.RegisterDefaultPlugins(context.TODO(), registry, state, tc, logger, store, cfg))
	e := &cwEnv{t: t, srv: emulator.NewServer(*cfg, registry, store, state, tc, logger), tc: tc}
	for _, m := range []float64{-3, -2, -1} {
		e.ok("PutMetricData", map[string]any{"Namespace": "cfg", "MetricData": []map[string]any{
			{"MetricName": "V", "Value": m, "Timestamp": at(m)},
		}})
	}
	got, _ := results(t, e.ok("GetMetricData", map[string]any{
		"StartTime": at(-5), "EndTime": at(1), "ScanBy": "TimestampAscending",
		"MetricDataQueries": []any{metricStat("v", "cfg", "V", "Sum", 60)},
	}))
	assert.Equal(t, []float64{-2, -1}, got[0].Values)
}
