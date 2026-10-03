package emulator

// The wire is a different thing from the state, and the two types below exist to keep them
// apart.
//
// EMRServerlessApp and EMRServerlessJobRun (emrserverless_plugin.go) are persisted state records,
// and each was handed straight to the caller at the one site that answers it: getApplication under
// `application`, getJobRun under `jobRun`. Two fields of each are substrate's own, accountID and
// region, and neither API_Application nor API_JobRun publishes either (#756; #1199 recorded it).
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves
// the next one to be remembered rather than prevented, and it changes the format of every recorded
// run, because MemoryStateManager snapshots those bytes and a replay reads them back. Projecting
// leaves the stored bytes unchanged, so a run recorded before this replays identically.

// emrServerlessAppOut is the application element of GetApplication's response.
//
// Six of API_Application's members — the ones the record models. The rest are absent rather than
// present and empty (#1013's rule, #1199's gap).
type emrServerlessAppOut struct {
	ApplicationID string `json:"applicationId"`
	Arn           string `json:"arn"`
	Name          string `json:"name"`
	ReleaseLabel  string `json:"releaseLabel"`
	State         string `json:"state"`
	Type          string `json:"type"`
}

// emrServerlessAppToWire projects a persisted application onto the published shape.
func emrServerlessAppToWire(app EMRServerlessApp) emrServerlessAppOut {
	return emrServerlessAppOut{
		ApplicationID: app.ApplicationID,
		Arn:           app.Arn,
		Name:          app.Name,
		ReleaseLabel:  app.ReleaseLabel,
		State:         app.State,
		Type:          app.Type,
	}
}

// emrServerlessJobRunOut is the jobRun element of GetJobRun's response.
//
// Five of API_JobRun's members — the ones the record models.
type emrServerlessJobRunOut struct {
	ApplicationID string `json:"applicationId"`
	Arn           string `json:"arn"`
	JobRunID      string `json:"jobRunId"`
	Name          string `json:"name,omitempty"`
	State         string `json:"state"`
}

// emrServerlessJobRunToWire projects a persisted job run onto the published shape.
func emrServerlessJobRunToWire(run EMRServerlessJobRun) emrServerlessJobRunOut {
	return emrServerlessJobRunOut{
		ApplicationID: run.ApplicationID,
		Arn:           run.Arn,
		JobRunID:      run.JobRunID,
		Name:          run.Name,
		State:         run.State,
	}
}
