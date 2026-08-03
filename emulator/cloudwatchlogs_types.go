package emulator

import "strings"

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

// The three types above are the *state* encoding: they are what
// CloudWatchLogsPlugin marshals into the state manager, what
// MemoryStateManager.Snapshot round-trips, and what
// LambdaPlugin.autoCreateLambdaLogGroup writes directly. Their PascalCase tags
// are therefore a persisted format and are deliberately left alone.
//
// The wire is a different thing. CloudWatch Logs is a JSON-1.1 service whose
// members are camelCase, and botocore matches response members against the
// service model case-sensitively: a PascalCase member does not fail to parse, it
// parses to *nothing*, so the caller gets one empty dict per resource with an
// HTTP 200 and no error (#528). Reusing one struct for both jobs is what made
// three of the four reads silently unreadable while FilterLogEvents — which
// already declared its own response element — stayed correct.
//
// So each read projects state onto a response-only element type below. Keeping
// the two encodings separate is the point: retagging the state structs would
// have fixed the wire and changed the persisted format in the same edit.

// cwLogGroupOut is the wire encoding of a log group, per the API's LogGroup type.
type cwLogGroupOut struct {
	LogGroupName string `json:"logGroupName"`

	// LogGroupARN is the ARN without a trailing ":*". The reference documents
	// `arn` and `logGroupArn` as *distinct* members differing only in that
	// suffix, and substrate's builder produces the unsuffixed form, so that
	// value belongs under this name.
	LogGroupARN string `json:"logGroupArn,omitempty"`

	// ARN is the same ARN with the trailing ":*" the reference specifies for
	// this member. It is the version a caller puts in an IAM policy for most
	// actions, so reporting only the unsuffixed one under both names would hand
	// out a policy resource the real service rejects.
	ARN string `json:"arn,omitempty"`

	CreationTime    int64 `json:"creationTime"`
	RetentionInDays int   `json:"retentionInDays,omitempty"`
}

// cwLogStreamOut is the wire encoding of a log stream, per the API's LogStream
// type. LogStream documents a single `arn` member with no suffix variant, so
// unlike a log group there is only one ARN to report.
type cwLogStreamOut struct {
	LogStreamName       string `json:"logStreamName"`
	ARN                 string `json:"arn,omitempty"`
	CreationTime        int64  `json:"creationTime"`
	LastIngestionTime   int64  `json:"lastIngestionTime,omitempty"`
	UploadSequenceToken string `json:"uploadSequenceToken,omitempty"`
}

// cwOutputLogEventOut is the wire encoding of a log event, per the API's
// OutputLogEvent type.
type cwOutputLogEventOut struct {
	Timestamp     int64  `json:"timestamp"`
	Message       string `json:"message"`
	IngestionTime int64  `json:"ingestionTime"`
}

// cwLogGroupWire projects a stored log group onto its wire encoding.
func cwLogGroupWire(lg CWLogGroup) cwLogGroupOut {
	return cwLogGroupOut{
		LogGroupName:    lg.LogGroupName,
		LogGroupARN:     lg.ARN,
		ARN:             cwLogGroupPolicyARN(lg.ARN),
		CreationTime:    lg.CreationTime,
		RetentionInDays: lg.RetentionInDays,
	}
}

// cwLogStreamWire projects a stored log stream onto its wire encoding.
//
// A conversion rather than a field-by-field literal: the two types are
// field-identical and differ only in their tags, so this is the projection that
// stops compiling if a field is added to only one of them — which is exactly the
// drift #528 came from.
func cwLogStreamWire(ls CWLogStream) cwLogStreamOut {
	return cwLogStreamOut(ls)
}

// cwOutputLogEventWire projects a stored log event onto its wire encoding. A
// conversion, for the reason given on cwLogStreamWire.
func cwOutputLogEventWire(ev CWLogEvent) cwOutputLogEventOut {
	return cwOutputLogEventOut(ev)
}

// cwLogGroupPolicyARN returns the trailing-":*" form of a log group ARN, which
// is the API's `arn` member. An empty ARN stays empty rather than becoming a
// bare ":*", and an ARN that already carries the suffix is returned unchanged so
// the function is idempotent over its own output.
func cwLogGroupPolicyARN(arn string) string {
	if arn == "" || strings.HasSuffix(arn, ":*") {
		return arn
	}
	return arn + ":*"
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
