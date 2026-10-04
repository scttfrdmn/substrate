package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// CodePipelineExecution (codepipeline_types.go) is a persisted state record, and
// getPipelineExecution handed it straight to the caller under `pipelineExecution`. That answered
// accountID and region, which no CodePipeline shape publishes (#756), and startTime, which
// API_PipelineExecution does not publish either: it has no date member at all, so the fix there is to
// drop the member rather than to convert it. CodePipelineState's own responses have always been built
// member by member, so it needs no type here — only its dates needed converting (#1338).
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back.
//
// # Why CodePipeline's dates are EpochSeconds
//
// CodePipeline speaks awsJson1_1, where a Timestamp is published as epoch seconds with fractional
// precision: API_GetPipeline renders metadata's `"created": number` and `"updated": number`, and its
// own sample response is `"created": 1501626591.112`. A Go time.Time marshals to RFC3339, which every
// pipeline response answered, so a typed SDK failed to decode them (#1338). The record keeps its
// time.Time; each response site converts with EpochSeconds.

// codepipelineExecutionOut is the pipelineExecution element of GetPipelineExecution's response.
//
// Four of API_PipelineExecution's members — the ones the record models. The rest are absent rather
// than present and empty (#1013's rule, #1199's gap).
type codepipelineExecutionOut struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
	PipelineName        string `json:"pipelineName"`
	PipelineVersion     int    `json:"pipelineVersion"`
	Status              string `json:"status"`
	// StatusSummary is reported only on a seeded final observation (#1155).
	StatusSummary string `json:"statusSummary,omitempty"`
}

// codepipelineExecutionToWire projects a persisted execution onto the published shape.
func codepipelineExecutionToWire(exec CodePipelineExecution) codepipelineExecutionOut {
	return codepipelineExecutionOut{
		PipelineExecutionID: exec.PipelineExecutionID,
		PipelineName:        exec.PipelineName,
		PipelineVersion:     exec.PipelineVersion,
		Status:              exec.Status,
	}
}
