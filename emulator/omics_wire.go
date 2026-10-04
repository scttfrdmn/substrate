package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// OmicsRun (omics_plugin.go) is a persisted state record, and getRun handed it straight to the caller
// as the GetRun body. Two of its fields are substrate's own: AccountID and Region, which scope the
// record's state key. Neither is a member of GetRun's response (#756).
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back.

// omicsRunOut is one run as GetRun answers it.
//
// Eight of GetRun's published members — the ones the record models. The rest (arn, creationTime,
// startTime, stopTime, uuid and the others) are absent rather than present and empty (#1013's rule,
// #1199's gap).
type omicsRunOut struct {
	ID            string `json:"id"`
	Name          string `json:"name,omitempty"`
	OutputURI     string `json:"outputUri,omitempty"`
	RoleArn       string `json:"roleArn,omitempty"`
	Status        string `json:"status"`
	StatusMessage string `json:"statusMessage,omitempty"`
	WorkflowID    string `json:"workflowId,omitempty"`
	WorkflowType  string `json:"workflowType,omitempty"`
	// FailureReason is API_GetRun's failureReason, answered only for a run a seed settled as FAILED.
	FailureReason string `json:"failureReason,omitempty"`
}

// omicsRunToWire projects a persisted run onto GetRun's published shape.
func omicsRunToWire(r OmicsRun, obs omicsRunObservation) omicsRunOut {
	return omicsRunOut{
		ID:            r.ID,
		Name:          r.Name,
		OutputURI:     r.OutputURI,
		RoleArn:       r.RoleArn,
		Status:        obs.status,
		FailureReason: obs.failureReason,
		StatusMessage: r.StatusMessage,
		WorkflowID:    r.WorkflowID,
		WorkflowType:  r.WorkflowType,
	}
}
