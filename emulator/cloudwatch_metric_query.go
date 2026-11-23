package emulator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Reading stored datapoints back: GetMetricData, GetMetricStatistics and ListMetrics (#1595).
//
// # How a window is cut into periods
//
// Every statistic is computed at read time, deterministically, from the points a series holds:
//
//   - The window's start is rounded down as CloudWatch rounds it — "to the nearest whole minute"
//     for a start less than 15 days before the simulated clock, to the 5-minute clock interval for
//     one 15 to 63 days before, and to the hour beyond that ([cwRoundStart]). A Period under 60
//     seconds rounds the start to that period instead: "If you set Period to 5, 10, 20, or 30, the
//     start time of your request is rounded down to the nearest time that corresponds to even 5-,
//     10-, 20-, or 30-second divisions of a minute."
//   - Periods are laid end to end from the rounded start, and a point falls in the period its
//     timestamp does. EndTime "is exclusive". A regular-resolution point is stored "at 1-minute
//     resolution", so it counts at the start of its minute; a point published with
//     StorageResolution 1 counts at its own second.
//   - A period with no point reports no datapoint, as CloudWatch reports none.
//
// # Which statistics
//
// SampleCount, Sum, Average, Minimum and Maximum are computed from every point: a Value counts
// once, a Values element counts its Counts entry (1 when Counts is omitted, as MetricDatum says),
// and a StatisticValues set contributes its own four members. A percentile pNN (p0 to p100, with
// decimals) is computed by **nearest rank**: the values are ordered, each weighted by its count,
// and the statistic is the smallest value whose cumulative weight reaches NN percent of the total
// — p0 is the minimum and p100 the maximum. AWS publishes that it computes percentiles but not the
// method, so this one is substrate's, chosen because it always answers a value that was published.
// A StatisticValues set takes part in a percentile only in the two cases PutMetricData names —
// SampleCount 1 with Min, Max and Sum equal, or Min equal to Max with Sum equal to Min times
// SampleCount — and a period holding any other set reports no percentile. Trimmed means,
// winsorized means, IQM, percentile rank and the other extended statistics are not computed, and
// a query naming one reports no datapoint.
//
// # Units
//
// A query that names a Unit reads only the points published with it — "If you specify a unit that
// does not match the data collected, the results of the operation are null" — and a datum that
// named none was published with None. A query that names none reads every point. A
// GetMetricStatistics datapoint reports the unit its points share, and None when they were
// published with different units; AWS aggregates different units separately, which substrate does
// not.

// cwQueryStart rounds a window's start for period; see the file comment.
func cwQueryStart(start, now time.Time, period time.Duration) time.Time {
	if period < time.Minute {
		return start.Truncate(period)
	}
	return cwRoundStart(start, now)
}

// cwSample is one value with its weight, for a percentile.
type cwSample struct {
	value, weight float64
}

// cwAgg accumulates the points of one period.
type cwAgg struct {
	sum, count, min, max float64
	samples              []cwSample
	noPercentile         bool
	units                map[string]bool
}

func (a *cwAgg) observe(v, weight float64) {
	if a.count == 0 || v < a.min {
		a.min = v
	}
	if a.count == 0 || v > a.max {
		a.max = v
	}
	a.sum += v * weight
	a.count += weight
	a.samples = append(a.samples, cwSample{value: v, weight: weight})
}

// add accumulates one stored point.
func (a *cwAgg) add(pt cwPoint) {
	if a.units == nil {
		a.units = map[string]bool{}
	}
	a.units[pt.unit()] = true
	switch {
	case pt.Value != nil:
		a.observe(*pt.Value, 1)
	case len(pt.Values) > 0:
		for i, v := range pt.Values {
			w := 1.0
			if i < len(pt.Counts) {
				w = pt.Counts[i]
			}
			if w > 0 {
				a.observe(v, w)
			}
		}
	case pt.Stats != nil:
		s := *pt.Stats
		if a.count == 0 || s.Minimum < a.min {
			a.min = s.Minimum
		}
		if a.count == 0 || s.Maximum > a.max {
			a.max = s.Maximum
		}
		a.sum += s.Sum
		a.count += s.SampleCount
		// PutMetricData's two conditions under which a statistic set yields percentiles.
		if (s.SampleCount == 1 && s.Minimum == s.Maximum && s.Maximum == s.Sum) ||
			(s.Minimum == s.Maximum && s.Sum == s.Minimum*s.SampleCount) {
			a.samples = append(a.samples, cwSample{value: s.Minimum, weight: s.SampleCount})
		} else {
			a.noPercentile = true
		}
	}
}

// unit is the unit the period's points share, None when they differ.
func (a *cwAgg) unit() string {
	if len(a.units) == 1 {
		for u := range a.units {
			return u
		}
	}
	return cwUsageUnit
}

// cwParsePercentile reads a pNN statistic, reporting false for anything else.
func cwParsePercentile(stat string) (float64, bool) {
	rest, ok := strings.CutPrefix(stat, "p")
	if !ok || rest == "" || rest[0] < '0' || rest[0] > '9' {
		return 0, false
	}
	p, err := strconv.ParseFloat(rest, 64)
	if err != nil || p < 0 || p > 100 {
		return 0, false
	}
	return p, true
}

// stat computes one statistic over the period, reporting false when it cannot.
func (a *cwAgg) stat(name string) (float64, bool) {
	if a.count <= 0 {
		return 0, false
	}
	switch name {
	case "SampleCount":
		return a.count, true
	case "Sum":
		return a.sum, true
	case "Average":
		return a.sum / a.count, true
	case "Minimum":
		return a.min, true
	case "Maximum":
		return a.max, true
	}
	p, ok := cwParsePercentile(name)
	if !ok || a.noPercentile || len(a.samples) == 0 {
		return 0, false
	}
	return cwNearestRank(a.samples, p), true
}

// cwNearestRank is the nearest-rank percentile over weighted samples; see the file comment.
func cwNearestRank(samples []cwSample, p float64) float64 {
	sorted := append([]cwSample(nil), samples...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].value < sorted[j].value })
	total := 0.0
	for _, s := range sorted {
		total += s.weight
	}
	target := p / 100 * total
	cum := 0.0
	for _, s := range sorted {
		cum += s.weight
		if cum >= target && s.weight > 0 {
			return s.value
		}
	}
	return sorted[len(sorted)-1].value
}

// cwBucket is one period that holds points.
type cwBucket struct {
	at  time.Time
	agg *cwAgg
}

// cwBuckets lays points into periods from start; see the file comment. The result is in
// ascending timestamp order.
func cwBuckets(points []cwPoint, start, end time.Time, period time.Duration, unit string) []cwBucket {
	byIndex := map[int64]*cwAgg{}
	for _, pt := range points {
		if unit != "" && pt.unit() != unit {
			continue
		}
		at := pt.at()
		if at.Before(start) || !at.Before(end) {
			continue
		}
		idx := int64(at.Sub(start) / period)
		agg := byIndex[idx]
		if agg == nil {
			agg = &cwAgg{}
			byIndex[idx] = agg
		}
		agg.add(pt)
	}
	idxs := make([]int64, 0, len(byIndex))
	for i := range byIndex {
		idxs = append(idxs, i)
	}
	slices.Sort(idxs)
	out := make([]cwBucket, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, cwBucket{at: start.Add(time.Duration(i) * period), agg: byIndex[i]})
	}
	return out
}

// seriesPoints returns the points of one metric in [from, to): the stored points, plus the
// AWS/Scheduler points derived from the Scheduler's counters.
func (p *CloudWatchPlugin) seriesPoints(ctx context.Context, store *cwMetricStore, namespace, metricName string,
	dims []cwDimension, from, to time.Time) ([]cwPoint, error) {
	points, err := store.points(ctx, namespace, metricName, dims, from, to)
	if err != nil {
		return nil, err
	}
	if namespace == schedMetricsNamespace {
		derived, err := cwSchedulerPoints(ctx, p.state, store.accountID, store.region, metricName, dims, from, to)
		if err != nil {
			return nil, err
		}
		points = append(points, derived...)
	}
	return points, nil
}

// --- PutMetricData -----------------------------------------------------------

// putMetricData handles PutMetricData: it validates every datum, then stores each in its series;
// see cloudwatch_metric_store.go. A refused request stores nothing.
func (p *CloudWatchPlugin) putMetricData(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	now := p.tc.Now().UTC()
	namespace, data, awsErr := cwParsePutMetricData(req.Params, now)
	if awsErr != nil {
		return nil, awsErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.newMetricStore(ctx)
	goCtx := context.Background()
	for _, d := range data {
		if err := store.publish(goCtx, namespace, d, now); err != nil {
			return nil, fmt.Errorf("cloudwatch putMetricData: %w", err)
		}
	}
	return cwUnitResponse(req, "PutMetricData", ctx.RequestID)
}

// --- GetMetricData -----------------------------------------------------------

// cwDefaultMaxDatapoints is MaxDatapoints' default: "If you omit this, the default of 100,800 is
// used", per API_GetMetricData.
const cwDefaultMaxDatapoints = 100800

// cwQueryResult is one MetricDataResult before it is paged.
type cwQueryResult struct {
	id, label string
	returned  bool
	series    cwSeriesValues
	message   string
}

// getMetricData handles GetMetricData.
//
// Each MetricStat query reads its metric's points and computes its statistic per period, as the
// file comment describes; an EC2 vCPU usage metric is answered from EC2's state instead
// (cloudwatch_usage.go). An Expression is evaluated when it is in the arithmetic subset
// cloudwatch_metric_math.go describes, and otherwise answers no values and a message. Every query
// gets a MetricDataResult: one whose ReturnData is false carries only its Id and StatusCode, as the
// reference's examples show, and every other carries its Label, Timestamps, Values and StatusCode.
// A Label the query omits defaults to the metric's name, or to the query's Id for an expression;
// the reference says only that "CloudWatch generates a default", so this choice is substrate's.
//
// Values are ordered by ScanBy, TimestampDescending when omitted. Paging counts datapoints across
// the returned results in query order: a page carries at most MaxDatapoints of them, a result
// with datapoints beyond the page reports StatusCode PartialData, and the response's NextToken is
// an offset token naming where the next page starts. A token substrate did not issue is refused
// with InvalidNextToken, the one error the reference declares.
func (p *CloudWatchPlugin) getMetricData(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	offset, tokenOK := decodeOffsetPaginationToken(req.Params["NextToken"])
	if !tokenOK {
		return nil, cwInvalidNextToken()
	}
	scanBy := req.Params["ScanBy"]
	if awsErr := cwCheckEnum("ScanBy", scanBy, []string{"TimestampDescending", "TimestampAscending"}); awsErr != nil {
		return nil, awsErr
	}
	maxPoints := cwDefaultMaxDatapoints
	if raw := req.Params["MaxDatapoints"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return nil, cwInvalidParameterValue("The parameter MaxDatapoints must be a positive integer.")
		}
		maxPoints = n
	}

	start, startOK := cwParseTimestamp(req.Params["StartTime"])
	end, endOK := cwParseTimestamp(req.Params["EndTime"])
	queries := cwParseMetricDataQueries(req.Params, "MetricDataQueries")
	results, err := p.evaluateQueries(ctx, queries, start, end, startOK && endOK)
	if err != nil {
		return nil, err
	}

	descending := scanBy != "TimestampAscending"
	total := 0
	for _, r := range results {
		if r.returned {
			total += len(r.series.ts)
		}
	}
	if offset > total {
		offset = total
	}
	pageEnd := min(offset+maxPoints, total)

	docs := cwList{}
	cursor := 0
	for _, r := range results {
		if !r.returned {
			docs = append(docs, cwDoc{}.with("Id", r.id).with("StatusCode", "Complete"))
			continue
		}
		ts, vals := r.series.ts, r.series.vals
		if descending {
			ts, vals = slices.Clone(ts), slices.Clone(vals)
			slices.Reverse(ts)
			slices.Reverse(vals)
		}
		n := len(ts)
		lo, hi := max(offset-cursor, 0), min(pageEnd-cursor, n)
		timestamps, values := cwList{}, cwList{}
		for i := lo; i < hi; i++ {
			timestamps = append(timestamps, ts[i])
			values = append(values, vals[i])
		}
		status := "Complete"
		if cursor+n > pageEnd {
			status = "PartialData"
		}
		cursor += n
		doc := cwDoc{}.
			with("Id", r.id).
			with("Label", r.label).
			with("Timestamps", timestamps).
			with("Values", values).
			with("StatusCode", status)
		if r.message != "" {
			doc = doc.with("Messages", cwList{cwDoc{}.
				with("Code", cwUnsupportedExpressionCode).
				with("Value", r.message)})
		}
		docs = append(docs, doc)
	}

	result := cwDoc{}.
		with("MetricDataResults", docs).
		with("Messages", cwList{})
	if pageEnd < total {
		result = result.with("NextToken", encodeOffsetPaginationToken(pageEnd))
	}
	return cwRespond(req, "GetMetricData", ctx.RequestID, result)
}

// evaluateQueries computes every query's series, in query order. A MetricStat query is computed
// once and an expression evaluated once, whichever order the queries reference each other in.
func (p *CloudWatchPlugin) evaluateQueries(ctx *RequestContext, queries []CWMetricDataQuery,
	start, end time.Time, windowOK bool) ([]cwQueryResult, error) {
	byID := make(map[string]CWMetricDataQuery, len(queries))
	for _, q := range queries {
		byID[q.ID] = q
	}
	computed := map[string]cwSeriesValues{}
	failed := map[string]string{}
	visiting := map[string]bool{}
	store := p.newMetricStore(ctx)
	reader := p.newUsageReader(ctx)
	now := p.tc.Now().UTC()

	var compute func(id string) (cwSeriesValues, error)
	compute = func(id string) (cwSeriesValues, error) {
		if s, ok := computed[id]; ok {
			return s, nil
		}
		if msg, ok := failed[id]; ok {
			return cwSeriesValues{}, fmt.Errorf("%s", msg)
		}
		q, ok := byID[id]
		if !ok {
			return cwSeriesValues{}, fmt.Errorf("the Id %s names no query in this request", id)
		}
		if visiting[id] {
			return cwSeriesValues{}, fmt.Errorf("the expression for %s refers back to itself", id)
		}
		visiting[id] = true
		defer delete(visiting, id)

		var s cwSeriesValues
		switch {
		case q.MetricStat != nil:
			var err error
			if s, err = p.metricStatSeries(ctx, store, reader, *q.MetricStat, start, end, now, windowOK); err != nil {
				return cwSeriesValues{}, &cwStoreError{err: err}
			}
		case q.Expression != "":
			var err error
			if s, err = cwEvalExpression(q.Expression, compute); err != nil {
				if storeErr := (*cwStoreError)(nil); errors.As(err, &storeErr) {
					return cwSeriesValues{}, storeErr
				}
				failed[id] = err.Error()
				return cwSeriesValues{}, err
			}
		}
		computed[id] = s
		return s, nil
	}

	results := make([]cwQueryResult, 0, len(queries))
	for _, q := range queries {
		r := cwQueryResult{id: q.ID, label: q.Label, returned: q.ReturnData == nil || *q.ReturnData}
		if r.label == "" && q.MetricStat != nil {
			r.label = q.MetricStat.MetricName
		}
		if r.label == "" {
			r.label = q.ID
		}
		s, err := compute(q.ID)
		if storeErr := (*cwStoreError)(nil); errors.As(err, &storeErr) {
			return nil, storeErr.err
		}
		if err != nil {
			r.message = "substrate did not evaluate this expression: " + err.Error() +
				". It evaluates + - * / over numbers and the Ids of this request's queries."
		}
		r.series = s
		results = append(results, r)
	}
	return results, nil
}

// cwStoreError carries a state-store failure through the expression evaluator, so it is returned
// as an error rather than reported as an expression substrate did not evaluate.
type cwStoreError struct{ err error }

// Error implements error.
func (e *cwStoreError) Error() string { return e.err.Error() }

// metricStatSeries computes one MetricStat query's series.
func (p *CloudWatchPlugin) metricStatSeries(ctx *RequestContext, store *cwMetricStore, reader *cwUsageReader,
	ms CWMetricStat, start, end, now time.Time, windowOK bool) (cwSeriesValues, error) {
	var s cwSeriesValues
	if !windowOK || ms.MetricName == "" || ms.Period <= 0 || !end.After(start) {
		return s, nil
	}
	period := time.Duration(ms.Period) * time.Second
	if _, isUsage := ec2VCPUUsageClassFromDimensions(ms.Namespace, ms.MetricName, ms.Dimensions); isUsage {
		point, ok, err := p.usagePoint(reader, ms.Namespace, ms.MetricName, ms.Dimensions, ms.Unit, start, end, period)
		if err != nil || !ok {
			return s, err
		}
		if v, computed := cwUsageStatistic(ms.Stat, point.VCPUs, period); computed {
			s.ts, s.vals = []time.Time{point.At}, []float64{v}
		}
		return s, nil
	}
	from := cwQueryStart(start, now, period)
	points, err := p.seriesPoints(context.Background(), store, ms.Namespace, ms.MetricName, ms.Dimensions, from, end)
	if err != nil {
		return s, err
	}
	for _, b := range cwBuckets(points, from, end, period, ms.Unit) {
		if v, ok := b.agg.stat(ms.Stat); ok {
			s.ts, s.vals = append(s.ts, b.at), append(s.vals, v)
		}
	}
	return s, nil
}

// --- GetMetricStatistics -----------------------------------------------------

// cwStatisticNames are GetMetricStatistics' Statistics valid values, in the order a
// Datapoint reports them.
var cwStatisticNames = []string{"SampleCount", "Average", "Sum", "Minimum", "Maximum"}

// cwMaxExtendedStatistics is ExtendedStatistics' Array Members maximum, "Maximum number of 10
// items", per API_GetMetricStatistics.
const cwMaxExtendedStatistics = 10

// getMetricStatistics handles the GetMetricStatistics operation.
//
// Its input is validated as the reference specifies: Namespace, MetricName, StartTime,
// EndTime and Period are required (MissingParameter); Statistics and ExtendedStatistics are
// exclusive — "you must specify either Statistics or ExtendedStatistics, but not both" —
// (InvalidParameterCombination for both, MissingParameter for neither); a Statistics value
// must be one of the five; Period must be at least 1 and, as for every metric, one of 1, 5,
// 10, 20, 30 or a multiple of 60; StartTime must precede EndTime; and a window holding more
// than 1,440 periods is refused with InvalidParameterCombination.
//
// The answer is the metric name as Label and one Datapoint per period holding points, in
// ascending timestamp order, each carrying the requested Statistics, the requested
// ExtendedStatistics as a map, and its Unit; see the file comment for how periods and statistics
// are computed. An EC2 vCPU usage metric reports the one datapoint cloudwatch_usage.go describes.
func (p *CloudWatchPlugin) getMetricStatistics(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	namespace, metricName := req.Params["Namespace"], req.Params["MetricName"]
	for _, member := range []string{"Namespace", "MetricName", "StartTime", "EndTime", "Period"} {
		if req.Params[member] == "" {
			return nil, cwMissingParameter(member)
		}
	}
	start, ok := cwParseTimestamp(req.Params["StartTime"])
	if !ok {
		return nil, cwInvalidParameterValue("StartTime is not a valid timestamp")
	}
	end, ok := cwParseTimestamp(req.Params["EndTime"])
	if !ok {
		return nil, cwInvalidParameterValue("EndTime is not a valid timestamp")
	}
	if !start.Before(end) {
		return nil, cwInvalidParameterValue("The parameter StartTime must be less than the parameter EndTime.")
	}
	periodSecs, err := strconv.Atoi(req.Params["Period"])
	if err != nil || periodSecs < 1 {
		return nil, cwInvalidParameterValue("The parameter Period must be a positive integer.")
	}
	switch {
	case periodSecs%60 == 0:
	case periodSecs == 1, periodSecs == 5, periodSecs == 10, periodSecs == 20, periodSecs == 30:
	default:
		return nil, cwInvalidParameterValue("The parameter Period must be 1, 5, 10, 20, 30, or a multiple of 60.")
	}
	period := time.Duration(periodSecs) * time.Second

	statistics := parseMemberList(req.Params, "Statistics")
	extended := parseMemberList(req.Params, "ExtendedStatistics")
	switch {
	case len(statistics) > 0 && len(extended) > 0:
		return nil, cwInvalidParameterCombination("Specify either Statistics or ExtendedStatistics, but not both.")
	case len(statistics) == 0 && len(extended) == 0:
		return nil, cwMissingParameter("Statistics or ExtendedStatistics")
	case len(statistics) > len(cwStatisticNames):
		return nil, cwInvalidParameterValue("Statistics may name at most 5 statistics.")
	case len(extended) > cwMaxExtendedStatistics:
		return nil, cwInvalidParameterValue("ExtendedStatistics may name at most 10 statistics.")
	}
	wanted := make(map[string]bool, len(statistics))
	for _, s := range statistics {
		if !slices.Contains(cwStatisticNames, s) {
			return nil, cwInvalidParameterValue(fmt.Sprintf(
				"The value %s for parameter Statistics is not valid; it must be one of %s.",
				s, strings.Join(cwStatisticNames, ", ")))
		}
		wanted[s] = true
	}
	now := p.tc.Now().UTC()
	if n := cwPeriodCount(cwRoundStart(start, now), end, period); n > cwMaxStatisticsDatapoints {
		return nil, cwInvalidParameterCombination(fmt.Sprintf(
			"You have requested up to %d datapoints, which exceeds the limit of %d.", n, cwMaxStatisticsDatapoints))
	}

	dims := cwParseDimensions(req.Params, "Dimensions")
	unit := req.Params["Unit"]
	datapoints := cwList{}
	if _, isUsage := ec2VCPUUsageClassFromDimensions(namespace, metricName, dims); isUsage {
		point, found, err := p.usagePoint(p.newUsageReader(ctx), namespace, metricName, dims, unit, start, end, period)
		if err != nil {
			return nil, err
		}
		if found {
			stat := func(s string) (float64, bool) { return cwUsageStatistic(s, point.VCPUs, period) }
			datapoints = append(datapoints, cwDatapointDoc(point.At, wanted, extended, stat, cwUsageUnit))
		}
	} else {
		from := cwQueryStart(start, now, period)
		points, err := p.seriesPoints(context.Background(), p.newMetricStore(ctx), namespace, metricName, dims, from, end)
		if err != nil {
			return nil, err
		}
		for _, b := range cwBuckets(points, from, end, period, unit) {
			datapoints = append(datapoints, cwDatapointDoc(b.at, wanted, extended, b.agg.stat, b.agg.unit()))
		}
	}

	result := cwDoc{}.with("Label", metricName).with("Datapoints", datapoints)
	return cwRespond(req, "GetMetricStatistics", ctx.RequestID, result)
}

// cwDatapointDoc renders one Datapoint with the statistics a request asked for. An extended
// statistic substrate cannot compute is omitted from the map rather than reported as a number.
func cwDatapointDoc(at time.Time, wanted map[string]bool, extended []string,
	stat func(string) (float64, bool), unit string) cwDoc {
	dp := cwDoc{}.with("Timestamp", at)
	for _, s := range cwStatisticNames {
		if wanted[s] {
			if v, ok := stat(s); ok {
				dp = dp.with(s, v)
			}
		}
	}
	dp = dp.with("Unit", unit)
	if len(extended) > 0 {
		m := cwMap{}
		for _, s := range extended {
			if v, ok := stat(s); ok && !math.IsNaN(v) {
				m = append(m, cwMember{Name: s, Value: v})
			}
		}
		dp = dp.with("ExtendedStatistics", m)
	}
	return dp
}

// cwMissingParameter is the refusal for an absent required member. MissingParameter is one
// of the four errors GetMetricStatistics declares, at HTTP 400.
func cwMissingParameter(member string) *AWSError {
	return &AWSError{
		Code:       "MissingParameter",
		Message:    "The parameter " + member + " is required.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwInvalidParameterValue is the refusal for "The value of an input parameter is bad or
// out-of-range", at HTTP 400.
func cwInvalidParameterValue(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterValue", Message: message, HTTPStatus: http.StatusBadRequest}
}

// --- ListMetrics -------------------------------------------------------------

// cwListMetricsPageSize is ListMetrics' page: "Up to 500 results are returned for any one call",
// per API_ListMetrics.
const cwListMetricsPageSize = 500

// cwListedMetric is one metric ListMetrics may report.
type cwListedMetric struct {
	namespace, name string
	dims            []cwDimension
	// last is when the metric last received data, for RecentlyActive.
	last time.Time
}

// listMetrics handles the ListMetrics operation. It reports every metric the caller has published
// through PutMetricData, with its dimensions (#1595), then the AWS/Scheduler metrics the caller's
// schedules have produced (#1513), then the EC2 vCPU usage metrics in AWS/Usage the caller's
// instances have counted against (#1498).
//
// Namespace and MetricName narrow the listing, and a Dimensions filter keeps a metric only when it
// has every named dimension, with the named value when one is given. RecentlyActive PT3H keeps a
// metric that received data in the three hours before the simulated clock; a usage metric is
// always current. Results are paged 500 at a time with an offset NextToken, and a token substrate
// did not issue is refused with InvalidNextToken.
func (p *CloudWatchPlugin) listMetrics(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	offset, tokenOK := decodeOffsetPaginationToken(req.Params["NextToken"])
	if !tokenOK {
		return nil, cwInvalidNextToken()
	}
	recently := req.Params["RecentlyActive"]
	if awsErr := cwCheckEnum("RecentlyActive", recently, []string{"PT3H"}); awsErr != nil {
		return nil, awsErr
	}
	goCtx := context.Background()
	namespace := req.Params["Namespace"]
	filterName := req.Params["MetricName"]
	dimensionFilters := cwParseDimensions(req.Params, "Dimensions")
	now := p.tc.Now().UTC()

	var candidates []cwListedMetric
	series, err := p.newMetricStore(ctx).listSeries(goCtx, namespace)
	if err != nil {
		return nil, err
	}
	for _, s := range series {
		candidates = append(candidates, cwListedMetric{
			namespace: s.Namespace, name: s.MetricName, dims: s.Dimensions,
			last: time.UnixMilli(s.LastPublished).UTC(),
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.name != b.name {
			return a.name < b.name
		}
		return cwDimensionsKey(a.dims) < cwDimensionsKey(b.dims)
	})
	if namespace == "" || namespace == schedMetricsNamespace {
		sched, err := cwSchedulerMetrics(goCtx, p.state, ctx.AccountID, ctx.Region)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, sched...)
	}

	metrics := cwList{}
	for _, m := range candidates {
		if filterName != "" && m.name != filterName {
			continue
		}
		if !cwDimensionFiltersMatch(dimensionFilters, m.dims) {
			continue
		}
		if recently != "" && m.last.Before(now.Add(-3*time.Hour)) {
			continue
		}
		metrics = append(metrics, cwDoc{}.
			with("MetricName", m.name).
			with("Namespace", m.namespace).
			with("Dimensions", cwDimensionList(m.dims)))
	}

	usageMetrics, err := p.usageMetrics(ctx, namespace, filterName, dimensionFilters)
	if err != nil {
		return nil, err
	}
	metrics = append(metrics, usageMetrics...)

	page, next := pageByOffsetToken(metrics, offset, cwListMetricsPageSize)
	result := cwDoc{}.with("Metrics", cwList(page)).withNonEmpty("NextToken", next)
	return cwRespond(req, "ListMetrics", ctx.RequestID, result)
}
