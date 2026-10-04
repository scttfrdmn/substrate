package emulator

import "encoding/json"

// The wire is a different thing from the state, and the types below exist to keep them apart.
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
//
// # The dates
//
// EMR Serverless speaks restJson1, and its pages render every Timestamp as `number` (ListJobRuns'
// Response Syntax), so createdAt and updatedAt are [EpochSeconds]. A record written before #1199
// holds neither, and both render as null rather than as an invented instant.

// emrServerlessAppOut is the application element of GetApplication's response.
//
// API_Application marks seven members Required: Yes — applicationId, arn, createdAt, releaseLabel,
// state, type, updatedAt — and all seven are here; until #1199 the two dates were missing. Of the
// optional members, name, architecture, tags, initialCapacity, maximumCapacity and
// networkConfiguration are answered when the create request sent them. The rest
// (autoStartConfiguration, autoStopConfiguration, imageConfiguration, monitoringConfiguration,
// runtimeConfiguration, stateDetails and the other configuration objects) are not recorded, so they
// are absent rather than present and empty (#1013's rule).
type emrServerlessAppOut struct {
	ApplicationID        string            `json:"applicationId"`
	Architecture         string            `json:"architecture,omitempty"`
	Arn                  string            `json:"arn"`
	Created              EpochSeconds      `json:"createdAt"`
	InitialCapacity      json.RawMessage   `json:"initialCapacity,omitempty"`
	MaximumCapacity      json.RawMessage   `json:"maximumCapacity,omitempty"`
	Name                 string            `json:"name,omitempty"`
	NetworkConfiguration json.RawMessage   `json:"networkConfiguration,omitempty"`
	ReleaseLabel         string            `json:"releaseLabel"`
	State                string            `json:"state"`
	Tags                 map[string]string `json:"tags,omitempty"`
	Type                 string            `json:"type"`
	Updated              EpochSeconds      `json:"updatedAt"`
}

// emrServerlessAppToWire projects a persisted application onto the published shape.
func emrServerlessAppToWire(app EMRServerlessApp) emrServerlessAppOut {
	return emrServerlessAppOut{
		ApplicationID:        app.ApplicationID,
		Architecture:         app.Architecture,
		Arn:                  app.Arn,
		Created:              EpochSeconds(app.Created),
		InitialCapacity:      app.InitialCapacity,
		MaximumCapacity:      app.MaximumCapacity,
		Name:                 app.Name,
		NetworkConfiguration: app.NetworkConfiguration,
		ReleaseLabel:         app.ReleaseLabel,
		State:                app.State,
		Tags:                 app.Tags,
		Type:                 app.Type,
		Updated:              EpochSeconds(app.Updated),
	}
}

// emrServerlessJobRunOut is the jobRun element of GetJobRun's response.
//
// API_JobRun marks eleven members Required: Yes — applicationId, arn, createdAt, createdBy,
// executionRole, jobDriver, jobRunId, releaseLabel, state, stateDetails, updatedAt — and until #1199
// four were answered. All eleven are now, with one condition: jobDriver is Required: No on
// StartJobRun's request, so a run started without one has none to report, and none is invented.
// stateDetails is substrate's wording for the state ([emrJobRunStateDetails]); the page publishes
// none.
//
// Of the optional members, mode, name and tags are answered. The rest are not modeled:
// billedResourceUtilization, totalResourceUtilization, totalExecutionDurationSeconds and
// queuedDurationMilliseconds describe a workload substrate does not run (CLAUDE.md's scope), and
// configurationOverrides, retryPolicy, networkConfiguration and the attempt members are not recorded.
type emrServerlessJobRunOut struct {
	ApplicationID string            `json:"applicationId"`
	Arn           string            `json:"arn"`
	Created       EpochSeconds      `json:"createdAt"`
	CreatedBy     string            `json:"createdBy"`
	ExecutionRole string            `json:"executionRole"`
	JobDriver     json.RawMessage   `json:"jobDriver,omitempty"`
	JobRunID      string            `json:"jobRunId"`
	Mode          string            `json:"mode,omitempty"`
	Name          string            `json:"name,omitempty"`
	ReleaseLabel  string            `json:"releaseLabel"`
	State         string            `json:"state"`
	StateDetails  string            `json:"stateDetails"`
	Tags          map[string]string `json:"tags,omitempty"`
	Updated       EpochSeconds      `json:"updatedAt"`
}

// emrServerlessJobRunToWire projects a persisted job run onto the published shape.
func emrServerlessJobRunToWire(run EMRServerlessJobRun) emrServerlessJobRunOut {
	return emrServerlessJobRunOut{
		ApplicationID: run.ApplicationID,
		Arn:           run.Arn,
		Created:       EpochSeconds(run.Created),
		CreatedBy:     run.CreatedBy,
		ExecutionRole: run.ExecutionRole,
		JobDriver:     run.JobDriver,
		JobRunID:      run.JobRunID,
		Mode:          emrJobRunMode(run),
		Name:          run.Name,
		ReleaseLabel:  run.ReleaseLabel,
		State:         run.State,
		StateDetails:  emrJobRunStateDetails(run.State),
		Tags:          run.Tags,
		Updated:       EpochSeconds(run.Updated),
	}
}

// emrJobRunSummaryOut is one element of ListJobRuns' jobRuns, API_JobRunSummary.
//
// The summary's ten Required members are all here: applicationId, arn, createdAt, createdBy,
// executionRole, id, releaseLabel, state, stateDetails, updatedAt. Until #1199 six were missing. Of
// the optional members, mode and name are answered; type and the attempt members are not.
type emrJobRunSummaryOut struct {
	ApplicationID string       `json:"applicationId"`
	Arn           string       `json:"arn"`
	Created       EpochSeconds `json:"createdAt"`
	CreatedBy     string       `json:"createdBy"`
	ExecutionRole string       `json:"executionRole"`
	ID            string       `json:"id"`
	Mode          string       `json:"mode,omitempty"`
	Name          string       `json:"name,omitempty"`
	ReleaseLabel  string       `json:"releaseLabel"`
	State         string       `json:"state"`
	StateDetails  string       `json:"stateDetails"`
	Updated       EpochSeconds `json:"updatedAt"`
}

// emrServerlessJobRunToSummary projects a persisted job run onto API_JobRunSummary. The ID member
// is `id`, as the summary publishes it (#1204).
func emrServerlessJobRunToSummary(run EMRServerlessJobRun) emrJobRunSummaryOut {
	return emrJobRunSummaryOut{
		ApplicationID: run.ApplicationID,
		Arn:           run.Arn,
		Created:       EpochSeconds(run.Created),
		CreatedBy:     run.CreatedBy,
		ExecutionRole: run.ExecutionRole,
		ID:            run.JobRunID,
		Mode:          emrJobRunMode(run),
		Name:          run.Name,
		ReleaseLabel:  run.ReleaseLabel,
		State:         run.State,
		StateDetails:  emrJobRunStateDetails(run.State),
		Updated:       EpochSeconds(run.Updated),
	}
}

// emrJobRunMode is a run's mode: what StartJobRun named, or BATCH for a run that named none or was
// recorded before the mode was. STREAMING is the mode a caller opts into.
func emrJobRunMode(run EMRServerlessJobRun) string {
	if run.Mode == "" {
		return "BATCH"
	}
	return run.Mode
}

// emrJobRunStateDetails is the stateDetails a job run reports. API_JobRun and API_JobRunSummary mark
// it Required, 1–256 characters and not blank, and publish no wording, so the text is substrate's.
func emrJobRunStateDetails(state string) string {
	switch state {
	case "SUCCESS":
		return "The job run completed."
	case "CANCELLED":
		return "The job run was cancelled."
	case "FAILED":
		return "The job run failed."
	default:
		return "The job run is " + state + "."
	}
}
