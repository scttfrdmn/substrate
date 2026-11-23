package emulator

import (
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// PutMetricAlarm's members, stored and rendered whole (#1620).
//
// DescribeAlarms used to echo the fourteen members the first CWAlarm carried and drop the rest,
// so an alarm put with TreatMissingData "breaching" read back with no TreatMissingData at all — a
// typed client sees nil, which is the published default "missing", the opposite of what was set.
// Every PutMetricAlarm member a caller can set and API_MetricAlarm publishes is now stored and
// rendered: Dimensions, ExtendedStatistic, Unit, DatapointsToAlarm, TreatMissingData,
// EvaluateLowSampleCountPercentile, ThresholdMetricId and Metrics (metric-math alarms), plus the
// three timestamps CloudWatch maintains. The members API_PutMetricAlarm publishes that this file
// does not store — ActionConfiguration, EvaluationCriteria, EvaluationInterval, EvaluationWindow
// and WarmUpConfiguration, which serve PromQL alarms, wall-clock windows and warm-up — are
// accepted and not rendered.
//
// Storing is not evaluating. The alarm's StateValue still moves only by SetAlarmState; evaluating
// the stored configuration against stored datapoints is #1637.
//
// # The update rule
//
// "When you update an existing alarm, its state is left unchanged, but the update completely
// overwrites the previous configuration of the alarm" — so an update keeps StateValue,
// StateReason, StateReasonData and the two state timestamps, and replaces everything else. Tags
// are the exception the Tags member's own text makes: "If you are using this operation to update
// an existing alarm, any tags you specify in this parameter are ignored." An update keeps the
// alarm's tags and ignores the request's.
//
// # Validation
//
// Each refusal below rests on a published constraint of API_PutMetricAlarm — a Valid Values list,
// a Valid Range, an Array Members maximum, or one of the page's "not both" rules — and answers the
// common InvalidParameterValue or InvalidParameterCombination code at 400. The messages are
// substrate's own; the page publishes the constraints, not the wording of their refusals.

// cwComparisonOperators are ComparisonOperator's Valid Values.
var cwComparisonOperators = []string{
	"GreaterThanOrEqualToThreshold", "GreaterThanThreshold", "LessThanThreshold",
	"LessThanOrEqualToThreshold", "LessThanLowerOrGreaterThanUpperThreshold",
	"LessThanLowerThreshold", "GreaterThanUpperThreshold",
}

// cwTreatMissingDataValues are TreatMissingData's Valid Values.
var cwTreatMissingDataValues = []string{"breaching", "notBreaching", "ignore", "missing"}

// cwLowSampleCountValues are EvaluateLowSampleCountPercentile's Valid Values.
var cwLowSampleCountValues = []string{"evaluate", "ignore"}

// cwUnits are the Valid Values of every Unit member CloudWatch publishes (MetricDatum,
// PutMetricAlarm, MetricStat): one list, the same on every page.
var cwUnits = []string{
	"Seconds", "Microseconds", "Milliseconds", "Bytes", "Kilobytes", "Megabytes", "Gigabytes",
	"Terabytes", "Bits", "Kilobits", "Megabits", "Gigabits", "Terabits", "Percent", "Count",
	"Bytes/Second", "Kilobytes/Second", "Megabytes/Second", "Gigabytes/Second", "Terabytes/Second",
	"Bits/Second", "Kilobits/Second", "Megabits/Second", "Gigabits/Second", "Terabits/Second",
	"Count/Second", "None",
}

// cwMaxAlarmActions is the Array Members maximum of AlarmActions, OKActions and
// InsufficientDataActions: "Maximum number of 5 items", on all three.
const cwMaxAlarmActions = 5

// cwMaxDimensions is the Array Members maximum of every Dimensions member, "Maximum number of 30
// items", on every page that publishes one.
const cwMaxDimensions = 30

// cwMaxTagsPerResource is "You can associate as many as 50 tags with a CloudWatch resource"
// (API_TagResource), which API_PutMetricAlarm's Tags member repeats for an alarm.
const cwMaxTagsPerResource = 50

// cwInvalidParameterCombination is the refusal for members "used together that cannot be used
// together", at HTTP 400.
func cwInvalidParameterCombination(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterCombination", Message: message, HTTPStatus: http.StatusBadRequest}
}

// cwCheckEnum refuses a set member whose value is not one of valid.
func cwCheckEnum(member, value string, valid []string) *AWSError {
	if value == "" || slices.Contains(valid, value) {
		return nil
	}
	return cwInvalidParameterValue(fmt.Sprintf("The value %s for parameter %s is not valid; it must be one of %s.",
		value, member, strings.Join(valid, ", ")))
}

// cwOptionalInt reads an integer member, reporting false when it is absent and refusing one
// that is not an integer at least min.
func cwOptionalInt(params map[string]string, member string, minimum int) (int, bool, *AWSError) {
	raw, ok := params[member]
	if !ok || raw == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < minimum {
		return 0, false, cwInvalidParameterValue(fmt.Sprintf(
			"The parameter %s must be an integer of at least %d.", member, minimum))
	}
	return n, true, nil
}

// cwParseBool reads a boolean member in any spelling a protocol sends: JSON and CBOR flatten to
// "true"/"false", and the API reference's own PutMetricAlarm example sends "False".
func cwParseBool(raw string) (value, ok bool) {
	switch strings.ToLower(raw) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

// cwParseMetricStat reads one MetricStat structure under prefix, nil when it is absent.
func cwParseMetricStat(params map[string]string, prefix string) *CWMetricStat {
	ns, name := params[prefix+"Metric.Namespace"], params[prefix+"Metric.MetricName"]
	stat, period := params[prefix+"Stat"], params[prefix+"Period"]
	if ns == "" && name == "" && stat == "" && period == "" {
		return nil
	}
	p, _ := strconv.Atoi(period)
	return &CWMetricStat{
		Namespace:  ns,
		MetricName: name,
		Dimensions: cwParseDimensions(params, prefix+"Metric.Dimensions"),
		Period:     p,
		Stat:       stat,
		Unit:       params[prefix+"Unit"],
	}
}

// cwParseMetricDataQueries reads a list of MetricDataQuery structures under prefix — the
// PutMetricAlarm Metrics member and the GetMetricData MetricDataQueries member are the same
// shape — stopping at the first element with no Id.
func cwParseMetricDataQueries(params map[string]string, prefix string) []CWMetricDataQuery {
	var queries []CWMetricDataQuery
	for i := 1; ; i++ {
		base := prefix + ".member." + strconv.Itoa(i) + "."
		id, ok := params[base+"Id"]
		if !ok || id == "" {
			return queries
		}
		q := CWMetricDataQuery{
			ID:            id,
			Expression:    params[base+"Expression"],
			Label:         params[base+"Label"],
			SourceAccount: params[base+"AccountId"],
			MetricStat:    cwParseMetricStat(params, base+"MetricStat."),
		}
		if v, ok := cwParseBool(params[base+"ReturnData"]); ok {
			q.ReturnData = &v
		}
		q.Period, _ = strconv.Atoi(params[base+"Period"])
		queries = append(queries, q)
	}
}

// cwParseTagList reads a list of Tag structures under prefix, reporting whether the member was
// present at all.
func cwParseTagList(params map[string]string, prefix string) (map[string]string, bool) {
	var tags map[string]string
	present := false
	for i := 1; ; i++ {
		base := prefix + ".member." + strconv.Itoa(i) + "."
		key, ok := params[base+"Key"]
		if !ok {
			return tags, present
		}
		present = true
		if tags == nil {
			tags = map[string]string{}
		}
		tags[key] = params[base+"Value"]
	}
}

// cwParseAlarm reads and validates a PutMetricAlarm request into the alarm it configures. The
// name, ARN and state are the caller's to fill in.
func cwParseAlarm(params map[string]string) (CWAlarm, *AWSError) {
	a := CWAlarm{
		AlarmName:                        params["AlarmName"],
		AlarmDescription:                 params["AlarmDescription"],
		MetricName:                       params["MetricName"],
		Namespace:                        params["Namespace"],
		Statistic:                        params["Statistic"],
		ExtendedStatistic:                params["ExtendedStatistic"],
		ComparisonOperator:               params["ComparisonOperator"],
		Unit:                             params["Unit"],
		TreatMissingData:                 params["TreatMissingData"],
		EvaluateLowSampleCountPercentile: params["EvaluateLowSampleCountPercentile"],
		ThresholdMetricID:                params["ThresholdMetricId"],
		ActionsEnabled:                   params["ActionsEnabled"] != "false" && params["ActionsEnabled"] != "False",
		AlarmActions:                     parseMemberList(params, "AlarmActions"),
		OKActions:                        parseMemberList(params, "OKActions"),
		InsufficientDataActions:          parseMemberList(params, "InsufficientDataActions"),
		Dimensions:                       cwParseDimensions(params, "Dimensions"),
		Metrics:                          cwParseMetricDataQueries(params, "Metrics"),
	}
	for _, check := range []*AWSError{
		cwCheckEnum("Statistic", a.Statistic, cwStatisticNames),
		cwCheckEnum("ComparisonOperator", a.ComparisonOperator, cwComparisonOperators),
		cwCheckEnum("TreatMissingData", a.TreatMissingData, cwTreatMissingDataValues),
		cwCheckEnum("EvaluateLowSampleCountPercentile", a.EvaluateLowSampleCountPercentile, cwLowSampleCountValues),
		cwCheckEnum("Unit", a.Unit, cwUnits),
	} {
		if check != nil {
			return CWAlarm{}, check
		}
	}
	if a.Statistic != "" && a.ExtendedStatistic != "" {
		// "you must specify either Statistic or ExtendedStatistic but not both."
		return CWAlarm{}, cwInvalidParameterCombination("Specify either Statistic or ExtendedStatistic, but not both.")
	}
	if len(a.Metrics) > 0 {
		// "If you use the Metrics parameter, you cannot include the Namespace, MetricName,
		// Dimensions, Period, Unit, Statistic, or ExtendedStatistic parameters".
		for _, member := range []string{"Namespace", "MetricName", "Period", "Unit", "Statistic", "ExtendedStatistic"} {
			if params[member] != "" {
				return CWAlarm{}, cwInvalidParameterCombination("The parameter " + member + " cannot be used with Metrics.")
			}
		}
		if len(a.Dimensions) > 0 {
			return CWAlarm{}, cwInvalidParameterCombination("The parameter Dimensions cannot be used with Metrics.")
		}
	}
	if raw := params["Threshold"]; raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return CWAlarm{}, cwInvalidParameterValue("The parameter Threshold must be a number.")
		}
		a.Threshold = v
	}
	var awsErr *AWSError
	if a.EvaluationPeriods, _, awsErr = cwOptionalInt(params, "EvaluationPeriods", 1); awsErr != nil {
		return CWAlarm{}, awsErr
	}
	if n, ok, err := cwOptionalInt(params, "DatapointsToAlarm", 1); err != nil {
		return CWAlarm{}, err
	} else if ok {
		a.DatapointsToAlarm = &n
	}
	if a.Period, _, awsErr = cwOptionalInt(params, "Period", 1); awsErr != nil {
		return CWAlarm{}, awsErr
	}
	if awsErr := cwCheckAlarmPeriod(a.Period, a.EvaluationPeriods); awsErr != nil {
		return CWAlarm{}, awsErr
	}
	for _, actions := range []struct {
		member string
		arns   []string
	}{
		{"AlarmActions", a.AlarmActions}, {"OKActions", a.OKActions}, {"InsufficientDataActions", a.InsufficientDataActions},
	} {
		if len(actions.arns) > cwMaxAlarmActions {
			return CWAlarm{}, cwInvalidParameterValue(fmt.Sprintf(
				"The parameter %s may name at most %d actions.", actions.member, cwMaxAlarmActions))
		}
	}
	if len(a.Dimensions) > cwMaxDimensions {
		return CWAlarm{}, cwInvalidParameterValue(fmt.Sprintf(
			"The parameter Dimensions may hold at most %d dimensions.", cwMaxDimensions))
	}
	return a, nil
}

// cwCheckAlarmPeriod applies Period's published rules: "Valid values are 10, 20, 30, and any
// multiple of 60", and "Period multiplied by EvaluationPeriods can't be more than 604,800
// seconds. For alarms with a period of less than one hour (3,600 seconds), the total evaluation
// period can't be longer than one day (86,400 seconds)." A zero period is an unset one.
func cwCheckAlarmPeriod(period, evaluationPeriods int) *AWSError {
	if period == 0 {
		return nil
	}
	if period%60 != 0 && period != 10 && period != 20 && period != 30 {
		return cwInvalidParameterValue("The parameter Period must be 10, 20, 30, or a multiple of 60.")
	}
	total := period * max(evaluationPeriods, 1)
	switch {
	case total > 604800:
		return cwInvalidParameterValue("Period multiplied by EvaluationPeriods must not exceed 604,800 seconds.")
	case period < 3600 && total > 86400:
		return cwInvalidParameterValue(
			"For a Period below 3,600 seconds, Period multiplied by EvaluationPeriods must not exceed 86,400 seconds.")
	}
	return nil
}

// cwAlarmDoc renders one alarm as a MetricAlarm structure.
//
// The first fourteen members are in the order substrate's XML struct used, not the model's, so
// that the Query responses substrate already served for an alarm carrying only those members are
// byte-for-byte unchanged; see the note in cloudwatch_render.go on why order is free to be either.
// The members #1620 added follow them. Every member is present only when it was set: a typed
// client reads an absent one as nil and can tell it was never set.
//
// MetricName and Namespace are absent for a metric-math alarm, which names its metrics in
// Metrics, and Threshold is absent for an anomaly-detection alarm, whose threshold is the band
// ThresholdMetricId names.
func cwAlarmDoc(a CWAlarm) cwDoc {
	doc := cwDoc{}.
		with("AlarmName", a.AlarmName).
		with("AlarmArn", a.AlarmARN).
		withNonEmpty("AlarmDescription", a.AlarmDescription).
		withNonEmpty("MetricName", a.MetricName).
		withNonEmpty("Namespace", a.Namespace).
		withNonEmpty("Statistic", a.Statistic).
		withNonEmpty("ComparisonOperator", a.ComparisonOperator)
	if a.ThresholdMetricID == "" {
		doc = doc.with("Threshold", a.Threshold)
	}
	if a.EvaluationPeriods > 0 {
		doc = doc.with("EvaluationPeriods", a.EvaluationPeriods)
	}
	if a.Period > 0 {
		doc = doc.with("Period", a.Period)
	}
	doc = doc.
		with("StateValue", a.StateValue).
		withNonEmpty("StateReason", a.StateReason).
		withNonEmpty("StateReasonData", a.StateReasonData).
		with("ActionsEnabled", a.ActionsEnabled).
		withStrings("AlarmActions", a.AlarmActions).
		withStrings("OKActions", a.OKActions).
		withStrings("InsufficientDataActions", a.InsufficientDataActions)
	if len(a.Dimensions) > 0 {
		doc = doc.with("Dimensions", cwDimensionList(a.Dimensions))
	}
	doc = doc.
		withNonEmpty("ExtendedStatistic", a.ExtendedStatistic).
		withNonEmpty("Unit", a.Unit)
	if a.DatapointsToAlarm != nil {
		doc = doc.with("DatapointsToAlarm", *a.DatapointsToAlarm)
	}
	doc = doc.
		withNonEmpty("TreatMissingData", a.TreatMissingData).
		withNonEmpty("EvaluateLowSampleCountPercentile", a.EvaluateLowSampleCountPercentile).
		withNonEmpty("ThresholdMetricId", a.ThresholdMetricID)
	if len(a.Metrics) > 0 {
		doc = doc.with("Metrics", cwMetricDataQueryList(a.Metrics))
	}
	for _, ts := range []struct {
		name string
		at   time.Time
	}{
		{"AlarmConfigurationUpdatedTimestamp", a.AlarmConfigurationUpdatedTimestamp},
		{"StateUpdatedTimestamp", a.StateUpdatedTimestamp},
		{"StateTransitionedTimestamp", a.StateTransitionedTimestamp},
	} {
		if !ts.at.IsZero() {
			doc = doc.with(ts.name, ts.at.UTC())
		}
	}
	return doc
}

// cwMetricDataQueryList renders a metric-math alarm's Metrics.
func cwMetricDataQueryList(queries []CWMetricDataQuery) cwList {
	list := make(cwList, 0, len(queries))
	for _, q := range queries {
		doc := cwDoc{}.with("Id", q.ID)
		if ms := q.MetricStat; ms != nil {
			metric := cwDoc{}.with("Namespace", ms.Namespace).with("MetricName", ms.MetricName)
			if len(ms.Dimensions) > 0 {
				metric = metric.with("Dimensions", cwDimensionList(ms.Dimensions))
			}
			stat := cwDoc{}.with("Metric", metric).with("Period", ms.Period).with("Stat", ms.Stat).
				withNonEmpty("Unit", ms.Unit)
			doc = doc.with("MetricStat", stat)
		}
		doc = doc.withNonEmpty("Expression", q.Expression).withNonEmpty("Label", q.Label)
		if q.ReturnData != nil {
			doc = doc.with("ReturnData", *q.ReturnData)
		}
		if q.Period > 0 {
			doc = doc.with("Period", q.Period)
		}
		list = append(list, doc.withNonEmpty("AccountId", q.SourceAccount))
	}
	return list
}

// cwAlarmMatchesMetric reports whether an alarm watches the metric a DescribeAlarmsForMetric
// request names. MetricName and Namespace are matched as before; Dimensions, Statistic,
// ExtendedStatistic, Period and Unit narrow the match when the request sets them, since the alarm
// now stores each (#1620). A request's Dimensions must equal the alarm's as a set.
func cwAlarmMatchesMetric(a CWAlarm, params map[string]string) bool {
	for member, have := range map[string]string{
		"MetricName": a.MetricName, "Namespace": a.Namespace, "Statistic": a.Statistic,
		"ExtendedStatistic": a.ExtendedStatistic, "Unit": a.Unit,
	} {
		if want := params[member]; want != "" && want != have {
			return false
		}
	}
	if want := params["Period"]; want != "" && want != strconv.Itoa(a.Period) {
		return false
	}
	if dims := cwParseDimensions(params, "Dimensions"); len(dims) > 0 {
		return cwDimensionsKey(dims) == cwDimensionsKey(a.Dimensions)
	}
	return true
}
