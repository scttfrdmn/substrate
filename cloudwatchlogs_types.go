package substrate

// cloudwatchLogsNamespace is the state namespace used by CloudWatchLogsPlugin.
const cloudwatchLogsNamespace = "logs"

// CWLogGroup represents an emulated Amazon CloudWatch Logs log group.
type CWLogGroup struct {
	// LogGroupName is the name of the log group.
	LogGroupName string `json:"LogGroupName"`

	// ARN is the Amazon Resource Name for the log group.
	ARN string `json:"ARN,omitempty"`

	// CreationTime is the log group creation time in milliseconds since epoch.
	CreationTime int64 `json:"CreationTime"`

	// RetentionInDays is the number of days to retain log events (0 = never expire).
	RetentionInDays int `json:"RetentionInDays,omitempty"`
}

// CWLogStream represents an emulated Amazon CloudWatch Logs log stream.
type CWLogStream struct {
	// LogStreamName is the name of the log stream.
	LogStreamName string `json:"LogStreamName"`

	// ARN is the Amazon Resource Name for the log stream.
	ARN string `json:"ARN,omitempty"`

	// CreationTime is the log stream creation time in milliseconds since epoch.
	CreationTime int64 `json:"CreationTime"`

	// LastIngestionTime is the time of the most recent log event in ms since epoch.
	LastIngestionTime int64 `json:"LastIngestionTime,omitempty"`

	// UploadSequenceToken is the sequence token for the next PutLogEvents call.
	UploadSequenceToken string `json:"UploadSequenceToken,omitempty"`
}

// CWLogEvent represents a single CloudWatch Logs event stored in the emulator.
type CWLogEvent struct {
	// Timestamp is the event time in milliseconds since epoch.
	Timestamp int64 `json:"Timestamp"`

	// Message is the event message text.
	Message string `json:"Message"`

	// IngestionTime is the time the event was ingested in ms since epoch.
	IngestionTime int64 `json:"IngestionTime"`
}

// cwLogGroupARN constructs the ARN for a CloudWatch Logs log group.
func cwLogGroupARN(region, accountID, logGroupName string) string {
	return "arn:aws:logs:" + region + ":" + accountID + ":log-group:" + logGroupName
}

// cwLogStreamARN constructs the ARN for a CloudWatch Logs log stream.
func cwLogStreamARN(region, accountID, logGroupName, logStreamName string) string {
	return "arn:aws:logs:" + region + ":" + accountID +
		":log-group:" + logGroupName +
		":log-stream:" + logStreamName
}
