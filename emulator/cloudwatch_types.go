package emulator

import "time"

// monitoringNamespace is the state namespace used by CloudWatchPlugin.
const monitoringNamespace = "monitoring"

// cloudwatchXMLNS is the XML namespace for CloudWatch monitoring responses.
const cloudwatchXMLNS = "http://monitoring.amazonaws.com/doc/2010-08-01/"

// CWAlarm represents an emulated Amazon CloudWatch metric alarm.
type CWAlarm struct {
	// AlarmName is the name of the alarm.
	AlarmName string `json:"AlarmName"`

	// AlarmARN is the Amazon Resource Name for the alarm.
	AlarmARN string `json:"AlarmARN,omitempty"`

	// AlarmDescription is a human-readable description of the alarm.
	AlarmDescription string `json:"AlarmDescription,omitempty"`

	// MetricName is the name of the CloudWatch metric the alarm monitors.
	MetricName string `json:"MetricName"`

	// Namespace is the namespace of the CloudWatch metric.
	Namespace string `json:"Namespace"`

	// Statistic is the statistic for the alarm (e.g., "Average", "Sum").
	Statistic string `json:"Statistic,omitempty"`

	// ComparisonOperator is the comparison operator (e.g., "GreaterThanThreshold").
	ComparisonOperator string `json:"ComparisonOperator"`

	// Threshold is the value to compare the statistic against.
	Threshold float64 `json:"Threshold"`

	// EvaluationPeriods is the number of periods over which data is compared.
	EvaluationPeriods int `json:"EvaluationPeriods"`

	// Period is the period in seconds over which the statistic is applied.
	Period int `json:"Period"`

	// StateValue is the current alarm state: "OK", "ALARM", or "INSUFFICIENT_DATA".
	StateValue string `json:"StateValue"`

	// StateReason is a human-readable explanation for the current state.
	StateReason string `json:"StateReason,omitempty"`

	// StateReasonData is machine-readable data for the state reason as JSON.
	StateReasonData string `json:"StateReasonData,omitempty"`

	// ActionsEnabled indicates whether alarm actions are enabled.
	ActionsEnabled bool `json:"ActionsEnabled"`

	// AlarmActions is a list of ARNs to notify when the alarm transitions to ALARM state.
	AlarmActions []string `json:"AlarmActions,omitempty"`

	// OKActions is a list of ARNs to notify when the alarm transitions to OK state.
	OKActions []string `json:"OKActions,omitempty"`

	// InsufficientDataActions is a list of ARNs to notify for INSUFFICIENT_DATA state.
	InsufficientDataActions []string `json:"InsufficientDataActions,omitempty"`

	// Dimensions are the dimensions of the metric named by MetricName (#1620).
	Dimensions []cwDimension `json:"Dimensions,omitempty"`

	// ExtendedStatistic is the percentile statistic, set instead of Statistic.
	ExtendedStatistic string `json:"ExtendedStatistic,omitempty"`

	// Unit is the unit of the metric the alarm watches.
	Unit string `json:"Unit,omitempty"`

	// DatapointsToAlarm is the M of an "M out of N" alarm; nil when the caller set none.
	DatapointsToAlarm *int `json:"DatapointsToAlarm,omitempty"`

	// TreatMissingData is breaching, notBreaching, ignore or missing; empty when unset.
	TreatMissingData string `json:"TreatMissingData,omitempty"`

	// EvaluateLowSampleCountPercentile is evaluate or ignore; empty when unset.
	EvaluateLowSampleCountPercentile string `json:"EvaluateLowSampleCountPercentile,omitempty"`

	// ThresholdMetricId names the ANOMALY_DETECTION_BAND query of an anomaly-detection alarm.
	ThresholdMetricID string `json:"ThresholdMetricId,omitempty"`

	// Metrics are the metric-math queries of an alarm built on an expression.
	Metrics []CWMetricDataQuery `json:"Metrics,omitempty"`

	// AlarmConfigurationUpdatedTimestamp is when PutMetricAlarm last wrote the configuration.
	AlarmConfigurationUpdatedTimestamp time.Time `json:"AlarmConfigurationUpdatedTimestamp,omitzero"`

	// StateUpdatedTimestamp is when StateValue was last written.
	StateUpdatedTimestamp time.Time `json:"StateUpdatedTimestamp,omitzero"`

	// StateTransitionedTimestamp is when StateValue last changed value.
	StateTransitionedTimestamp time.Time `json:"StateTransitionedTimestamp,omitzero"`

	// Tags are the alarm's tags (#1618). They are set by PutMetricAlarm only when it creates the
	// alarm, and by TagResource and UntagResource after that; DescribeAlarms never renders them.
	Tags map[string]string `json:"Tags,omitempty"`
}

// CWMetricDataQuery is one MetricDataQuery of a metric-math alarm's Metrics, as stored.
type CWMetricDataQuery struct {
	// ID is the query's short name.
	ID string `json:"Id"`

	// Expression is the math expression, empty for a query that retrieves a metric.
	Expression string `json:"Expression,omitempty"`

	// Label is the query's human-readable label.
	Label string `json:"Label,omitempty"`

	// ReturnData reports whether the query's result is the one the alarm watches; nil when unset.
	ReturnData *bool `json:"ReturnData,omitempty"`

	// Period is the expression's period in seconds, 0 when unset.
	Period int `json:"Period,omitempty"`

	// SourceAccount is the query's AccountId: the account a cross-account query reads, empty
	// when unset. It is the caller's member, not substrate's scoping bookkeeping.
	SourceAccount string `json:"AccountId,omitempty"`

	// MetricStat is the metric the query retrieves, nil for an expression.
	MetricStat *CWMetricStat `json:"MetricStat,omitempty"`
}

// CWMetricStat is the metric, period, statistic and unit of one [CWMetricDataQuery].
type CWMetricStat struct {
	// Namespace is the metric's namespace.
	Namespace string `json:"Namespace"`

	// MetricName is the metric's name.
	MetricName string `json:"MetricName"`

	// Dimensions are the metric's dimensions.
	Dimensions []cwDimension `json:"Dimensions,omitempty"`

	// Period is the statistic's period in seconds.
	Period int `json:"Period"`

	// Stat is the statistic.
	Stat string `json:"Stat"`

	// Unit is the unit filter, empty when unset.
	Unit string `json:"Unit,omitempty"`
}

// cwAlarmARN constructs the ARN for a CloudWatch alarm.
func cwAlarmARN(region, accountID, alarmName string) string {
	return "arn:aws:cloudwatch:" + region + ":" + accountID + ":alarm:" + alarmName
}
