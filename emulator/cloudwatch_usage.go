package emulator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AWS/Usage EC2 vCPU metrics, derived on read (#1498).
//
// Substrate produces no metric a workload would: an instance's CPUUtilization, a function's
// Duration, are what the software behind an API does, and nothing in substrate runs it. What
// a caller publishes through PutMetricData is stored and read back (#1595,
// cloudwatch_metric_store.go), because the caller handed CloudWatch those values through an
// API call. The EC2 vCPU usage metrics are a third case: their value is not produced by a
// workload and not published by a caller. "ResourceCount — The number of the specified
// resources running in your account" is a fact about EC2's API-observable state, the same fact
// DescribeInstances reports, so CloudWatch can answer it by reading that state at request time.
// Nothing is stored on the CloudWatch side, nothing runs between requests, and a replay that
// rebuilds the same EC2 state and clock reads back the same values. The AWS/Scheduler
// invocation metrics (scheduler_metrics.go) follow the same precedent.
//
// # Which datapoint is reported
//
// Substrate keeps no usage history, so it knows a class's usage only as of the request, and
// only the period containing the request can honestly carry it. A query therefore gets at
// most one datapoint — the current period's — and a window that does not reach the current
// period gets none, rather than today's usage stamped on a past period it was not measured in:
//
//   - The window's start is rounded down as CloudWatch rounds it: to the minute for a start
//     less than 15 days before the simulated clock, to the 5-minute clock interval for one
//     15 to 63 days before, and to the hour beyond that.
//   - Periods are laid end to end from that rounded start, as CloudWatch's own timestamps
//     are (the GetMetricData examples report StartTime, StartTime+Period, …).
//   - The current period is the one holding the simulated clock. Its datapoint is reported
//     when the window reaches it: the window opens no later than the clock, and its EndTime,
//     which "is exclusive", falls after the period's start. Otherwise — a window wholly in the
//     past, or one that opens after the clock — the result has no datapoint.
//
// The second condition asks that EndTime fall inside or after the current period, not after
// the clock itself. A caller computes EndTime from its own clock a moment before substrate
// reads the simulated one, so "EndTime=now" arrives a few milliseconds behind it; requiring
// EndTime past the clock would turn the commonest query there is into an empty one. The
// datapoint is still only ever the current period's.
//
// The statistic is computed as if the usage had been sampled every minute of that period at
// the value it has now — usage metrics "are collected every minute". Maximum, Minimum,
// Average and the percentile-style statistics are the value itself; SampleCount is the
// period's minute count, Period/60 and at least 1; Sum is the value times that count. A
// metric that "does not have a specified unit" is reported with unit None, so a query naming
// any other Unit gets no datapoint: "If you specify a unit that does not match the data
// collected, the results of the operation are null."

// cwDimension is one CloudWatch metric dimension.
type cwDimension struct {
	// Name is the dimension's name.
	Name string `json:"Name"`

	// Value is the dimension's value.
	Value string `json:"Value"`
}

// cwUsageUnit is the unit EC2 vCPU usage is reported in.
const cwUsageUnit = "None"

// cwMaxStatisticsDatapoints is GetMetricStatistics' ceiling: "The maximum number of data
// points returned from a single call is 1,440. If you request more than 1,440 data points,
// CloudWatch returns an error." A longer window is refused rather than truncated.
const cwMaxStatisticsDatapoints = 1440

// cwParseDimensions reads prefix.member.N.Name / .Value pairs, stopping at the first absent
// Name.
func cwParseDimensions(params map[string]string, prefix string) []cwDimension {
	var dims []cwDimension
	for i := 1; ; i++ {
		base := prefix + ".member." + strconv.Itoa(i) + "."
		name, ok := params[base+"Name"]
		if !ok || name == "" {
			return dims
		}
		dims = append(dims, cwDimension{Name: name, Value: params[base+"Value"]})
	}
}

// cwParseTimestamp reads a timestamp in any spelling CloudWatch's three protocols put into
// req.Params: epoch seconds, which is how awsJson1_0 sends one and how [cwNormalizeInput]
// flattens it, or ISO 8601, which is the query protocol's spelling and the one a CBOR
// timestamp is flattened to.
func cwParseTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		whole := int64(secs)
		return time.Unix(whole, int64((secs-float64(whole))*1e9)).UTC(), true
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// cwRoundStart rounds a window's start down as CloudWatch does, relative to now; see the
// file comment.
func cwRoundStart(start, now time.Time) time.Time {
	age := now.Sub(start)
	switch {
	case age >= 63*24*time.Hour:
		return start.Truncate(time.Hour)
	case age >= 15*24*time.Hour:
		return start.Truncate(5 * time.Minute)
	default:
		return start.Truncate(time.Minute)
	}
}

// cwUsageDatapointAt returns the timestamp of the one datapoint a usage query reports, and
// false when the window holds none; see the file comment.
func cwUsageDatapointAt(now, start, end time.Time, period time.Duration) (time.Time, bool) {
	if period <= 0 || !end.After(start) {
		return time.Time{}, false
	}
	s := cwRoundStart(start, now)
	if now.Before(s) {
		return time.Time{}, false
	}
	current := s.Add(now.Sub(s) / period * period)
	if !end.After(current) {
		return time.Time{}, false
	}
	return current, true
}

// cwPeriodCount is the number of period starts in [start, end): the ceiling of the window's
// length over the period, since a partial period at the end still starts before EndTime.
func cwPeriodCount(start, end time.Time, period time.Duration) int64 {
	span := end.Sub(start)
	if span <= 0 || period <= 0 {
		return 0
	}
	return int64((span + period - 1) / period)
}

// cwDimensionMap returns dims keyed by name.
func cwDimensionMap(dims []cwDimension) map[string]string {
	m := make(map[string]string, len(dims))
	for _, d := range dims {
		m[d.Name] = d.Value
	}
	return m
}

// cwUsageSamples is the number of one-minute samples a period holds, and at least one.
func cwUsageSamples(period time.Duration) float64 {
	if n := float64(period / time.Minute); n >= 1 {
		return n
	}
	return 1
}

// cwUsageStatistic computes stat over a period sampled every minute at value, and reports
// false for a statistic substrate does not compute.
func cwUsageStatistic(stat string, value float64, period time.Duration) (float64, bool) {
	switch stat {
	case "Maximum", "Minimum", "Average", "IQM":
		return value, true
	case "Sum":
		return value * cwUsageSamples(period), true
	case "SampleCount":
		return cwUsageSamples(period), true
	}
	// Percentile (p99), trimmed mean (tm90) and winsorized mean (wm90) of a constant series
	// are the constant. Percentile rank, trimmed count and trimmed sum are not, and fall
	// through to the refusal.
	for _, prefix := range []string{"p", "tm", "wm"} {
		if rest, ok := strings.CutPrefix(stat, prefix); ok && rest != "" && (rest[0] >= '0' && rest[0] <= '9' || rest[0] == '(') {
			return value, true
		}
	}
	return 0, false
}

// cwUsageReader loads EC2 vCPU usage at most once per request, and only if a query asks for
// it, so a GetMetricData naming only other metrics never reads EC2 state.
type cwUsageReader struct {
	state             StateManager
	accountID, region string
	loaded            bool
	usage             ec2VCPUUsage
}

// get returns the account's usage, reading it on first use.
func (r *cwUsageReader) get() (ec2VCPUUsage, error) {
	if !r.loaded {
		usage, err := ec2VCPUUsageFor(context.Background(), r.state, r.accountID, r.region)
		if err != nil {
			return ec2VCPUUsage{}, fmt.Errorf("cloudwatch usage: %w", err)
		}
		r.usage, r.loaded = usage, true
	}
	return r.usage, nil
}

// newUsageReader returns a reader for the caller's account and region.
func (p *CloudWatchPlugin) newUsageReader(ctx *RequestContext) *cwUsageReader {
	return &cwUsageReader{state: p.state, accountID: ctx.AccountID, region: ctx.Region}
}

// cwUsagePoint is one reported usage datapoint.
type cwUsagePoint struct {
	// At is the datapoint's timestamp.
	At time.Time

	// VCPUs is the usage it reports.
	VCPUs float64

	// Period is the period it summarizes.
	Period time.Duration
}

// usagePoint returns the datapoint a usage metric reports for one window, and false when
// the metric is not an EC2 vCPU usage metric, is not published in this account, or has no
// datapoint in the window.
func (p *CloudWatchPlugin) usagePoint(r *cwUsageReader, namespace, metricName string, dims []cwDimension,
	unit string, start, end time.Time, period time.Duration) (cwUsagePoint, bool, error) {
	class, ok := ec2VCPUUsageClassFromDimensions(namespace, metricName, dims)
	if !ok || (unit != "" && unit != cwUsageUnit) {
		return cwUsagePoint{}, false, nil
	}
	usage, err := r.get()
	if err != nil {
		return cwUsagePoint{}, false, err
	}
	if !usage.Published[class] {
		return cwUsagePoint{}, false, nil
	}
	at, ok := cwUsageDatapointAt(p.tc.Now().UTC(), start, end, period)
	if !ok {
		return cwUsagePoint{}, false, nil
	}
	return cwUsagePoint{At: at, VCPUs: usage.VCPUs[class], Period: period}, true, nil
}

// --- ListMetrics -------------------------------------------------------------

// cwDimensionFiltersMatch reports whether dims satisfies every filter: a filter names a
// dimension the metric must have and, when its Value is set, the value it must have.
func cwDimensionFiltersMatch(filters, dims []cwDimension) bool {
	for _, f := range filters {
		matched := false
		for _, d := range dims {
			if d.Name == f.Name && (f.Value == "" || d.Value == f.Value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// cwDimensionList renders dimensions as a Dimensions list.
func cwDimensionList(dims []cwDimension) cwList {
	list := make(cwList, 0, len(dims))
	for _, d := range dims {
		list = append(list, cwDoc{}.with("Name", d.Name).with("Value", d.Value))
	}
	return list
}

// usageMetrics returns the EC2 vCPU usage metrics ListMetrics reports for the caller: one
// per published class, in [ec2VCPUUsageClassNames] order, that pass the request's
// namespace, metric-name and dimension filters.
func (p *CloudWatchPlugin) usageMetrics(ctx *RequestContext, namespace, metricName string, filters []cwDimension) (cwList, error) {
	if (namespace != "" && namespace != awsUsageNamespace) || (metricName != "" && metricName != awsUsageResourceCount) {
		return nil, nil
	}
	usage, err := p.newUsageReader(ctx).get()
	if err != nil {
		return nil, err
	}
	var metrics cwList
	for _, class := range ec2VCPUUsageClassNames {
		if !usage.Published[class] {
			continue
		}
		dims := ec2VCPUUsageDimensions(class)
		if !cwDimensionFiltersMatch(filters, dims) {
			continue
		}
		metrics = append(metrics, cwDoc{}.
			with("MetricName", awsUsageResourceCount).
			with("Namespace", awsUsageNamespace).
			with("Dimensions", cwDimensionList(dims)))
	}
	return metrics, nil
}
