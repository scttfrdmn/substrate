package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// The datapoints PutMetricData publishes, stored (#1595).
//
// Substrate used to keep a published metric's name and discard its values, on the reading that
// producing datapoints is what the workload behind an API does. That reading was wrong for this
// operation: the caller hands CloudWatch the values through an API call, so reading them back
// through GetMetricData or GetMetricStatistics is an API observation, as reading back an S3
// object is. What stays out of scope is a workload producing metrics on its own — an EC2
// instance's CPUUtilization, a Lambda's Duration — because nothing in substrate runs that
// workload. The derived exceptions are the EC2 vCPU usage metrics (cloudwatch_usage.go) and the
// AWS/Scheduler invocation metrics (cloudwatch_scheduler_metrics.go), whose values are facts
// about API-observable state rather than workload output.
//
// # What a series is
//
// A metric's identity is its namespace, its name and its full dimension set: "a dimension is a
// name/value pair that is part of the identity of a metric". A series is stored per account and
// Region under that identity, with the dimensions sorted by name so the order a caller lists
// them in does not make two series of one metric:
//
//	metric_series:{account}/{region}/{escaped namespace}/{series id}         the identity
//	metric_points:{account}/{region}/{escaped namespace}/{series id}/{hour}  one hour's points
//
// The series id is a hash of the identity, because a metric name and a dimension value may hold
// any character a key separator could be; the identity record carries the identity itself. The
// namespace is path-escaped rather than hashed so a ListMetrics for one namespace lists one key
// prefix. Points are kept in hour chunks so a PutMetricData rewrites one hour of one series rather
// than the series' whole history.
//
// Each point keeps what the datum carried — a Value, a Values/Counts pair, or a StatisticValues
// set — with its Unit and StorageResolution, and the timestamp it was published at, which a datum
// that omits Timestamp takes from the simulated clock. Nothing is aggregated on write: every
// statistic is computed at read time from the points, which is what lets the same points answer a
// Sum over a minute and a p99 over an hour.
//
// # Retention
//
// AWS keeps a metric's data for fifteen months, aggregated to coarser periods as it ages. Substrate
// keeps at most a fixed number of points per series, and drops the oldest when a write takes a
// series past it. The default is [cwDefaultMaxDatapointsPerSeries], a week of one-minute data, and
// `cloudwatch.max_datapoints_per_series` (SUBSTRATE_CLOUDWATCH_MAX_DATAPOINTS_PER_SERIES) sets
// another. AWS's age-based roll-up is not modeled: a point reads back at full resolution for as
// long as it is kept.
//
// # Validation
//
// PutMetricData refuses, before it stores anything, what its reference and the MetricDatum page
// publish as invalid: a missing Namespace or MetricName; a Value or a Values element outside
// -2^360 to 2^360, or NaN or an infinity; more than 30 dimensions, or a dimension outside its
// published lengths; more than 150 Values; a Counts array whose length differs from Values'; a
// Value together with Values or StatisticValues; a StatisticValues set missing one of its four
// required members; a Timestamp more than two weeks before the simulated clock or more than two
// hours after it; a Unit outside its Valid Values; a StorageResolution other than 1 or 60; and more
// than 1,000 metrics in one request. The codes are the operation's published MissingParameter,
// InvalidParameterValue and InvalidParameterCombination at 400; the messages are substrate's own.

// cwDefaultMaxDatapointsPerSeries is the number of points a series keeps when nothing configures
// another: a week at one-minute resolution.
const cwDefaultMaxDatapointsPerSeries = 7 * 24 * 60

// cwMaxValuesPerDatum is the Values limit: "You can include up to 150 unique values in each
// PutMetricData action that specifies a Values array", per API_MetricDatum.
const cwMaxValuesPerDatum = 150

// cwMaxMetricsPerPut is the per-request limit: "Each request is also limited to no more than 1000
// different metrics", per API_PutMetricData.
const cwMaxMetricsPerPut = 1000

// cwValueBound is the magnitude CloudWatch accepts, "Values must be in the range of -2^360 to
// 2^360", per API_MetricDatum.
var cwValueBound = math.Ldexp(1, 360)

// cwSeriesPrefix and cwPointsPrefix begin the two key kinds the store writes.
const (
	cwSeriesPrefix = "metric_series:"
	cwPointsPrefix = "metric_points:"
)

// cwStatSet is a datum's StatisticValues.
type cwStatSet struct {
	SampleCount float64 `json:"n"`
	Sum         float64 `json:"sum"`
	Minimum     float64 `json:"min"`
	Maximum     float64 `json:"max"`
}

// cwPoint is one stored datum: exactly one of Value, Values (with Counts) or Stats is set.
type cwPoint struct {
	// T is the datum's timestamp in Unix milliseconds.
	T int64 `json:"t"`
	// Value is a single value.
	Value *float64 `json:"v,omitempty"`
	// Values and Counts are a Values/Counts pair; Counts is nil when every count is 1.
	Values []float64 `json:"vs,omitempty"`
	Counts []float64 `json:"cs,omitempty"`
	// Stats is a StatisticValues set.
	Stats *cwStatSet `json:"s,omitempty"`
	// Unit is the datum's Unit, empty for None.
	Unit string `json:"u,omitempty"`
	// HighRes records a StorageResolution of 1.
	HighRes bool `json:"hr,omitempty"`
}

// at is the point's timestamp as read: a regular-resolution point is stored "at 1-minute
// resolution", so it reads back at the start of its minute.
func (pt cwPoint) at() time.Time {
	t := time.UnixMilli(pt.T).UTC()
	if !pt.HighRes {
		t = t.Truncate(time.Minute)
	}
	return t
}

// unit is the point's unit, None when the datum named none.
func (pt cwPoint) unit() string {
	if pt.Unit == "" {
		return cwUsageUnit
	}
	return pt.Unit
}

// cwChunkRef is one hour chunk of a series, as its identity record lists it.
type cwChunkRef struct {
	// Hour is the chunk's hour, in Unix seconds.
	Hour int64 `json:"h"`
	// N is the number of points the chunk holds.
	N int `json:"n"`
}

// cwSeries is a series' identity record.
type cwSeries struct {
	Namespace  string        `json:"ns"`
	MetricName string        `json:"name"`
	Dimensions []cwDimension `json:"dims,omitempty"`
	// Chunks are the series' hour chunks, oldest first.
	Chunks []cwChunkRef `json:"chunks,omitempty"`
	// LastPublished is when a PutMetricData last wrote the series, in Unix milliseconds of the
	// simulated clock; ListMetrics' RecentlyActive reads it.
	LastPublished int64 `json:"last,omitempty"`
}

// cwSortDimensions returns dims sorted by name, which is the identity's canonical order.
func cwSortDimensions(dims []cwDimension) []cwDimension {
	out := append([]cwDimension(nil), dims...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// cwDimensionsKey renders a dimension set canonically, so two sets compare equal whatever order
// they were listed in.
func cwDimensionsKey(dims []cwDimension) string {
	var b strings.Builder
	for _, d := range cwSortDimensions(dims) {
		b.WriteString(d.Name)
		b.WriteByte(0)
		b.WriteString(d.Value)
		b.WriteByte(1)
	}
	return b.String()
}

// cwSeriesKeys returns the identity key of a series and the prefix its chunk keys share.
func cwSeriesKeys(accountID, region, namespace, metricName string, dims []cwDimension) (series, points string) {
	sum := sha256.Sum256([]byte(namespace + "\x00" + metricName + "\x00" + cwDimensionsKey(dims)))
	tail := accountID + "/" + region + "/" + url.PathEscape(namespace) + "/" + hex.EncodeToString(sum[:16])
	return cwSeriesPrefix + tail, cwPointsPrefix + tail + "/"
}

// cwSeriesListPrefix is the key prefix of every series in an account and Region, narrowed to one
// namespace when namespace is set.
func cwSeriesListPrefix(accountID, region, namespace string) string {
	prefix := cwSeriesPrefix + accountID + "/" + region + "/"
	if namespace != "" {
		prefix += url.PathEscape(namespace) + "/"
	}
	return prefix
}

// cwChunkKey is the key of one hour chunk.
func cwChunkKey(pointsPrefix string, hour int64) string {
	return pointsPrefix + fmt.Sprintf("%012d", hour)
}

// cwDatum is one validated MetricDatum.
type cwDatum struct {
	name  string
	dims  []cwDimension
	point *cwPoint // nil for a datum that carries no value
}

// cwDatumIndices returns the member indices present under prefix ("MetricData.member."), in
// ascending order. Every present index is returned, so a datum missing its MetricName is found
// and refused rather than ending the list.
func cwDatumIndices(params map[string]string, prefix string) []int {
	seen := map[int]bool{}
	for k := range params {
		rest, ok := strings.CutPrefix(k, prefix)
		if !ok {
			continue
		}
		head, _, _ := strings.Cut(rest, ".")
		if n, err := strconv.Atoi(head); err == nil && n > 0 {
			seen[n] = true
		}
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

// cwParseFloatList reads prefix.member.N doubles, stopping at the first absent element.
func cwParseFloatList(params map[string]string, prefix string) ([]float64, bool) {
	var out []float64
	for i := 1; ; i++ {
		raw, ok := params[prefix+".member."+strconv.Itoa(i)]
		if !ok {
			return out, true
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, false
		}
		out = append(out, v)
	}
}

// cwCheckValue refuses a value outside the published range, or a special value.
func cwCheckValue(member string, v float64) *AWSError {
	if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > cwValueBound {
		return cwInvalidParameterValue(fmt.Sprintf(
			"The value of %s must be a number between -2^360 and 2^360.", member))
	}
	return nil
}

// cwCheckDimensions applies the Dimension shape's limits: at most 30, a Name of 1 to 255
// characters, a Value of 1 to 1,024, and no name twice — a dimension is a name/value pair of the
// metric's identity, so a repeated name has no meaning.
func cwCheckDimensions(member string, dims []cwDimension) *AWSError {
	if len(dims) > cwMaxDimensions {
		return cwInvalidParameterValue(fmt.Sprintf("%s may hold at most %d dimensions.", member, cwMaxDimensions))
	}
	seen := map[string]bool{}
	for _, d := range dims {
		if n := utf8.RuneCountInString(d.Name); n < 1 || n > 255 {
			return cwInvalidParameterValue(member + ": a dimension Name must be 1 to 255 characters long.")
		}
		if n := utf8.RuneCountInString(d.Value); n < 1 || n > 1024 {
			return cwInvalidParameterValue(member + ": a dimension Value must be 1 to 1024 characters long.")
		}
		if seen[d.Name] {
			return cwInvalidParameterValue(member + ": the dimension " + d.Name + " is named more than once.")
		}
		seen[d.Name] = true
	}
	return nil
}

// cwParseDatum reads and validates the MetricDatum under base ("MetricData.member.N.").
func cwParseDatum(params map[string]string, base string, now time.Time) (cwDatum, *AWSError) {
	member := strings.TrimSuffix(base, ".")
	name := params[base+"MetricName"]
	if name == "" {
		return cwDatum{}, cwMissingParameter(member + ".MetricName")
	}
	if utf8.RuneCountInString(name) > 255 {
		return cwDatum{}, cwInvalidParameterValue(member + ".MetricName must be at most 255 characters long.")
	}
	dims := cwParseDimensions(params, base+"Dimensions")
	if awsErr := cwCheckDimensions(member+".Dimensions", dims); awsErr != nil {
		return cwDatum{}, awsErr
	}
	pt := cwPoint{Unit: params[base+"Unit"]}
	if awsErr := cwCheckEnum(member+".Unit", pt.Unit, cwUnits); awsErr != nil {
		return cwDatum{}, awsErr
	}
	if pt.Unit == cwUsageUnit {
		pt.Unit = ""
	}
	switch res := params[base+"StorageResolution"]; res {
	case "", "60":
	case "1":
		pt.HighRes = true
	default:
		return cwDatum{}, cwInvalidParameterValue(member + ".StorageResolution must be 1 or 60.")
	}

	at := now
	if raw := params[base+"Timestamp"]; raw != "" {
		t, ok := cwParseTimestamp(raw)
		if !ok {
			return cwDatum{}, cwInvalidParameterValue(member + ".Timestamp is not a valid timestamp.")
		}
		// "You can specify time stamps that are as much as two weeks before the current date,
		// and as much as 2 hours after the current day and time."
		if t.Before(now.Add(-14*24*time.Hour)) || t.After(now.Add(2*time.Hour)) {
			return cwDatum{}, cwInvalidParameterValue(member +
				".Timestamp must be no more than two weeks before, and no more than two hours after, the current time.")
		}
		at = t
	}
	pt.T = at.UnixMilli()

	rawValue, hasValue := params[base+"Value"]
	values, valuesOK := cwParseFloatList(params, base+"Values")
	counts, countsOK := cwParseFloatList(params, base+"Counts")
	if !valuesOK || !countsOK {
		return cwDatum{}, cwInvalidParameterValue(member + ": Values and Counts must be lists of numbers.")
	}
	stats, hasStats, awsErr := cwParseStatSet(params, base+"StatisticValues.", member)
	if awsErr != nil {
		return cwDatum{}, awsErr
	}
	switch {
	case hasValue && (len(values) > 0 || hasStats):
		return cwDatum{}, cwInvalidParameterCombination(member + ": specify only one of Value, Values and StatisticValues.")
	case len(values) > 0 && hasStats:
		return cwDatum{}, cwInvalidParameterCombination(member + ": specify only one of Value, Values and StatisticValues.")
	case len(counts) > 0 && len(values) == 0:
		return cwDatum{}, cwInvalidParameterCombination(member + ": Counts may be specified only with Values.")
	}

	d := cwDatum{name: name, dims: dims}
	switch {
	case hasValue:
		v, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return cwDatum{}, cwInvalidParameterValue(member + ".Value must be a number.")
		}
		if awsErr := cwCheckValue(member+".Value", v); awsErr != nil {
			return cwDatum{}, awsErr
		}
		pt.Value = &v
		d.point = &pt
	case len(values) > 0:
		if len(values) > cwMaxValuesPerDatum {
			return cwDatum{}, cwInvalidParameterValue(fmt.Sprintf(
				"%s.Values may hold at most %d values.", member, cwMaxValuesPerDatum))
		}
		// "If you include a Counts array, it must include the same amount of values as the
		// Values array."
		if len(counts) > 0 && len(counts) != len(values) {
			return cwDatum{}, cwInvalidParameterValue(member + ": Values and Counts must have the same number of elements.")
		}
		for _, v := range values {
			if awsErr := cwCheckValue(member+".Values", v); awsErr != nil {
				return cwDatum{}, awsErr
			}
		}
		for _, c := range counts {
			if c < 0 || math.IsNaN(c) || math.IsInf(c, 0) {
				return cwDatum{}, cwInvalidParameterValue(member + ".Counts must be non-negative numbers.")
			}
		}
		pt.Values, pt.Counts = values, counts
		d.point = &pt
	case hasStats:
		pt.Stats = &stats
		d.point = &pt
	}
	return d, nil
}

// cwParseStatSet reads a StatisticValues set, whose four members are each Required: Yes.
func cwParseStatSet(params map[string]string, base, member string) (cwStatSet, bool, *AWSError) {
	names := []string{"SampleCount", "Sum", "Minimum", "Maximum"}
	present := 0
	for _, n := range names {
		if params[base+n] != "" {
			present++
		}
	}
	if present == 0 {
		return cwStatSet{}, false, nil
	}
	vals := make([]float64, len(names))
	for i, n := range names {
		raw := params[base+n]
		if raw == "" {
			return cwStatSet{}, false, cwMissingParameter(member + ".StatisticValues." + n)
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return cwStatSet{}, false, cwInvalidParameterValue(member + ".StatisticValues." + n + " must be a number.")
		}
		if awsErr := cwCheckValue(member+".StatisticValues."+n, v); awsErr != nil {
			return cwStatSet{}, false, awsErr
		}
		vals[i] = v
	}
	s := cwStatSet{SampleCount: vals[0], Sum: vals[1], Minimum: vals[2], Maximum: vals[3]}
	if s.SampleCount <= 0 {
		return cwStatSet{}, false, cwInvalidParameterValue(member + ".StatisticValues.SampleCount must be greater than 0.")
	}
	if s.Minimum > s.Maximum {
		return cwStatSet{}, false, cwInvalidParameterValue(member + ".StatisticValues.Minimum must not exceed Maximum.")
	}
	return s, true, nil
}

// cwParsePutMetricData reads and validates a whole PutMetricData request: the MetricData list and
// each EntityMetricData element's MetricData list. The entity itself is accepted and not stored.
func cwParsePutMetricData(params map[string]string, now time.Time) (string, []cwDatum, *AWSError) {
	namespace := params["Namespace"]
	if namespace == "" {
		return "", nil, cwMissingParameter("Namespace")
	}
	if utf8.RuneCountInString(namespace) > 255 || strings.HasPrefix(namespace, ":") {
		// "Length Constraints: Minimum length of 1. Maximum length of 255. Pattern: [^:].*"
		return "", nil, cwInvalidParameterValue("The parameter Namespace must be 1 to 255 characters and must not begin with a colon.")
	}
	var bases []string
	for _, i := range cwDatumIndices(params, "MetricData.member.") {
		bases = append(bases, "MetricData.member."+strconv.Itoa(i)+".")
	}
	for _, e := range cwDatumIndices(params, "EntityMetricData.member.") {
		entity := "EntityMetricData.member." + strconv.Itoa(e) + ".MetricData.member."
		for _, i := range cwDatumIndices(params, entity) {
			bases = append(bases, entity+strconv.Itoa(i)+".")
		}
	}
	if len(bases) > cwMaxMetricsPerPut {
		return "", nil, cwInvalidParameterValue(fmt.Sprintf(
			"A PutMetricData request may hold at most %d metrics.", cwMaxMetricsPerPut))
	}
	data := make([]cwDatum, 0, len(bases))
	for _, base := range bases {
		d, awsErr := cwParseDatum(params, base, now)
		if awsErr != nil {
			return "", nil, awsErr
		}
		data = append(data, d)
	}
	return namespace, data, nil
}

// cwMetricStore reads and writes the series of one account and Region.
type cwMetricStore struct {
	state             StateManager
	accountID, region string
	maxPoints         int
}

// newMetricStore returns the store for the caller's account and Region.
func (p *CloudWatchPlugin) newMetricStore(ctx *RequestContext) *cwMetricStore {
	return &cwMetricStore{state: p.state, accountID: ctx.AccountID, region: ctx.Region, maxPoints: p.maxPoints}
}

// loadSeries reads a series' identity record, nil when the series does not exist.
func (s *cwMetricStore) loadSeries(ctx context.Context, key string) (*cwSeries, error) {
	data, err := s.state.Get(ctx, monitoringNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch load series: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var series cwSeries
	if err := json.Unmarshal(data, &series); err != nil {
		return nil, fmt.Errorf("cloudwatch decode series %s: %w", key, err)
	}
	return &series, nil
}

// loadChunk reads one hour chunk's points.
func (s *cwMetricStore) loadChunk(ctx context.Context, key string) ([]cwPoint, error) {
	data, err := s.state.Get(ctx, monitoringNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch load points: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var points []cwPoint
	if err := json.Unmarshal(data, &points); err != nil {
		return nil, fmt.Errorf("cloudwatch decode points %s: %w", key, err)
	}
	return points, nil
}

// put writes a JSON value.
func (s *cwMetricStore) put(ctx context.Context, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cloudwatch marshal %s: %w", key, err)
	}
	if err := s.state.Put(ctx, monitoringNamespace, key, data); err != nil {
		return fmt.Errorf("cloudwatch store %s: %w", key, err)
	}
	return nil
}

// publish adds a datum to its series, creating the series if it is new, and trims the series to
// the retention cap. The caller holds the plugin's lock.
func (s *cwMetricStore) publish(ctx context.Context, namespace string, d cwDatum, now time.Time) error {
	dims := cwSortDimensions(d.dims)
	seriesKey, pointsPrefix := cwSeriesKeys(s.accountID, s.region, namespace, d.name, dims)
	series, err := s.loadSeries(ctx, seriesKey)
	if err != nil {
		return err
	}
	if series == nil {
		series = &cwSeries{Namespace: namespace, MetricName: d.name, Dimensions: dims}
	}
	series.LastPublished = now.UnixMilli()
	if d.point != nil {
		if err := s.addPoint(ctx, series, pointsPrefix, *d.point); err != nil {
			return err
		}
		if err := s.trim(ctx, series, pointsPrefix); err != nil {
			return err
		}
	}
	return s.put(ctx, seriesKey, series)
}

// addPoint inserts pt into its hour chunk, after any point with the same timestamp, so points
// read back in timestamp order and, within one timestamp, in publication order.
func (s *cwMetricStore) addPoint(ctx context.Context, series *cwSeries, pointsPrefix string, pt cwPoint) error {
	hour := time.UnixMilli(pt.T).Truncate(time.Hour).Unix()
	key := cwChunkKey(pointsPrefix, hour)
	points, err := s.loadChunk(ctx, key)
	if err != nil {
		return err
	}
	i := sort.Search(len(points), func(i int) bool { return points[i].T > pt.T })
	points = append(points, cwPoint{})
	copy(points[i+1:], points[i:])
	points[i] = pt
	if err := s.put(ctx, key, points); err != nil {
		return err
	}
	j := sort.Search(len(series.Chunks), func(j int) bool { return series.Chunks[j].Hour >= hour })
	if j < len(series.Chunks) && series.Chunks[j].Hour == hour {
		series.Chunks[j].N++
		return nil
	}
	series.Chunks = append(series.Chunks, cwChunkRef{})
	copy(series.Chunks[j+1:], series.Chunks[j:])
	series.Chunks[j] = cwChunkRef{Hour: hour, N: 1}
	return nil
}

// trim drops a series' oldest points until it holds no more than the retention cap.
func (s *cwMetricStore) trim(ctx context.Context, series *cwSeries, pointsPrefix string) error {
	total := 0
	for _, c := range series.Chunks {
		total += c.N
	}
	for total > s.maxPoints && len(series.Chunks) > 0 {
		oldest := series.Chunks[0]
		key := cwChunkKey(pointsPrefix, oldest.Hour)
		excess := total - s.maxPoints
		if excess >= oldest.N {
			if err := s.state.Delete(ctx, monitoringNamespace, key); err != nil {
				return fmt.Errorf("cloudwatch drop points: %w", err)
			}
			series.Chunks = series.Chunks[1:]
			total -= oldest.N
			continue
		}
		points, err := s.loadChunk(ctx, key)
		if err != nil {
			return err
		}
		points = points[min(excess, len(points)):]
		if err := s.put(ctx, key, points); err != nil {
			return err
		}
		series.Chunks[0].N = len(points)
		total -= excess
	}
	return nil
}

// points returns the stored points of one series whose timestamp, as read, falls in [from, to),
// in timestamp order. A series that does not exist has none.
func (s *cwMetricStore) points(ctx context.Context, namespace, metricName string, dims []cwDimension, from, to time.Time) ([]cwPoint, error) {
	seriesKey, pointsPrefix := cwSeriesKeys(s.accountID, s.region, namespace, metricName, cwSortDimensions(dims))
	series, err := s.loadSeries(ctx, seriesKey)
	if err != nil || series == nil {
		return nil, err
	}
	var out []cwPoint
	for _, c := range series.Chunks {
		start := time.Unix(c.Hour, 0)
		if !start.Before(to) || !start.Add(time.Hour).After(from) {
			continue
		}
		chunk, err := s.loadChunk(ctx, cwChunkKey(pointsPrefix, c.Hour))
		if err != nil {
			return nil, err
		}
		for _, pt := range chunk {
			if at := pt.at(); !at.Before(from) && at.Before(to) {
				out = append(out, pt)
			}
		}
	}
	return out, nil
}

// listSeries returns every series in the account and Region, narrowed to one namespace when
// namespace is set, in key order.
func (s *cwMetricStore) listSeries(ctx context.Context, namespace string) ([]cwSeries, error) {
	keys, err := s.state.List(ctx, monitoringNamespace, cwSeriesListPrefix(s.accountID, s.region, namespace))
	if err != nil {
		return nil, fmt.Errorf("cloudwatch list series: %w", err)
	}
	out := make([]cwSeries, 0, len(keys))
	for _, k := range keys {
		series, err := s.loadSeries(ctx, k)
		if err != nil {
			return nil, err
		}
		if series != nil {
			out = append(out, *series)
		}
	}
	return out, nil
}
