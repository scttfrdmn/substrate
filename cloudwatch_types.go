package substrate

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
}

// cwAlarmARN constructs the ARN for a CloudWatch alarm.
func cwAlarmARN(region, accountID, alarmName string) string {
	return "arn:aws:cloudwatch:" + region + ":" + accountID + ":alarm:" + alarmName
}
