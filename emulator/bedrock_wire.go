package emulator

import (
	"encoding/json"
	"math"
	"time"
)

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// BedrockModelInvocationJob (bedrock_runtime_plugin.go) is a persisted state record, and
// getModelInvocationJob handed it straight to the caller as the whole response body. Two of its
// fields are substrate's own and neither carries omitempty, so every GetModelInvocationJob answered
// accountID and region, which neither API_GetModelInvocationJob nor API_ModelInvocationJobSummary
// publishes (#756). CreateModelInvocationJob answers a one-member map and StopModelInvocationJob an
// empty object; ListModelInvocationJobs built a summary of its own, and now shares this projection.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves
// the next one to be remembered rather than prevented, and it changes the format of every recorded
// run, because MemoryStateManager snapshots those bytes and a replay reads them back. For the same
// reason SubmitTime stays a float64 of epoch seconds in the record and is converted on projection.
//
// # Why submitTime is an ISO 8601 string
//
// Bedrock speaks restJson1, whose default body timestamp is epoch seconds, but Bedrock's model
// overrides it: API_GetModelInvocationJob renders `"submitTime": "string"`, as it renders
// endTime, lastModifiedTime and jobExpirationTime, where the reference renders an epoch-seconds
// member `number`. The record's float marshaled to a JSON number, which a typed SDK refuses to
// decode as a date-time timestamp, so both reads answered a body that failed before a consumer's
// assertion ran. It is rendered with millisecond precision in UTC, the form AWS's
// date-time timestamps take.

// bedrockDateTimeLayout is the date-time form a Bedrock Timestamp member is published in.
const bedrockDateTimeLayout = "2006-01-02T15:04:05.000Z"

// bedrockInvocationJobOut is GetModelInvocationJob's response body, and one element of
// ListModelInvocationJobs' invocationJobSummaries.
//
// Every member is published by both API_GetModelInvocationJob and API_ModelInvocationJobSummary,
// and the record holds each one. The summary's Required members — jobArn, jobName, modelId,
// roleArn, inputDataConfig, outputDataConfig and submitTime — are all here; the list used to
// answer four of the seven. The two data configurations are echoed as the caller sent them.
// clientRequestToken, endTime, lastModifiedTime, the record counts, timeoutDurationInHours,
// modelInvocationType, jobExpirationTime and vpcConfig are not modeled and are absent rather than
// present and empty.
type bedrockInvocationJobOut struct {
	JobArn           string          `json:"jobArn"`
	JobName          string          `json:"jobName"`
	ModelID          string          `json:"modelId"`
	RoleArn          string          `json:"roleArn,omitempty"`
	Status           string          `json:"status"`
	Message          string          `json:"message,omitempty"`
	InputDataConfig  json.RawMessage `json:"inputDataConfig,omitempty"`
	OutputDataConfig json.RawMessage `json:"outputDataConfig,omitempty"`
	SubmitTime       string          `json:"submitTime"`
}

// bedrockInvocationJobToWire projects a persisted job, its seeded status already applied, onto
// the published shape.
func bedrockInvocationJobToWire(job BedrockModelInvocationJob) bedrockInvocationJobOut {
	return bedrockInvocationJobOut{
		JobArn:           job.JobArn,
		JobName:          job.JobName,
		ModelID:          job.ModelID,
		RoleArn:          job.RoleArn,
		Status:           job.Status,
		Message:          job.Message,
		InputDataConfig:  job.InputDataConfig,
		OutputDataConfig: job.OutputDataConfig,
		SubmitTime:       bedrockDateTime(job.SubmitTime),
	}
}

// bedrockDateTime renders a record's epoch-seconds float as a published date-time.
func bedrockDateTime(epoch float64) string {
	return time.Unix(0, int64(math.Round(epoch*1e9))).UTC().Format(bedrockDateTimeLayout)
}
