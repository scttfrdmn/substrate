package emulator

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

// EMR Serverless's seeded job-run and application progressions (#1196).
//
// A job run was recorded SUCCESS and an application CREATED in the request that made them, so
// GetJobRun and GetApplication reported the settled state on their first read and the transitions
// API_JobRun and API_Application publish could not be observed: a poll-until-done loop exited without
// polling, and a FAILED run could not be produced. Substrate does not run the job — that is the
// workload CLAUDE.md puts out of scope — so what it models is the observation.
//
// Both kinds are the shared progression in emulator/progression.go: a seed keyed by ID or "*" says how
// many observations report a non-terminal state before the settled one, counted per resource and
// recorded and replayed as a control-plane write. Unseeded, both read exactly as before.
//
// # Job runs
//
// API_JobRun publishes SUBMITTED, PENDING, SCHEDULED, RUNNING, SUCCESS, FAILED, CANCELLING, CANCELLED
// and QUEUED. A seed names the transient state (RUNNING by default) and the final one (the record's
// SUCCESS by default; FAILED or CANCELLED to end otherwise), plus the stateDetails that final state
// reports. GetJobRun and ListJobRuns both observe, so the two agree: a run listed while RUNNING is
// RUNNING on the next GetJobRun too, a step further on.
//
// CancelJobRun restarts the countdown, and while it runs the run reports CANCELLING — the transient a
// cancel passes through — whatever the seed's own transient state; it then reports CANCELLED. A
// cancelled run cannot end SUCCESS or FAILED, so a cancelled run's final state is always CANCELLED.
//
// # Applications
//
// API_Application publishes CREATING, CREATED, STARTING, STARTED, STOPPING, STOPPED and TERMINATED.
// A seed names the transient (CREATING by default) and the final state (the record's CREATED by
// default). StartApplication and StopApplication are not routed, so STARTED and STOPPED are reachable
// only as a seed's final state; the issue asks for the states to be reachable, not for the two
// operations, which stay recorded as unrouted. The application publishes no failure state.

// emrJobRunStatesPublished is API_JobRun's state Valid Values, in the page's order.
var emrJobRunStatesPublished = []string{"SUBMITTED", "PENDING", "SCHEDULED", "RUNNING", "SUCCESS", "FAILED", "CANCELLING", "CANCELLED", "QUEUED"}

// emrJobRunTerminalStates are the job-run states that end the run.
var emrJobRunTerminalStates = []string{"SUCCESS", "FAILED", "CANCELLED"}

// emrApplicationStates is API_Application's state Valid Values.
var emrApplicationStates = []string{"CREATING", "CREATED", "STARTING", "STARTED", "STOPPING", "STOPPED", "TERMINATED"}

// emrJobRunStatusSeed is the body of POST /v1/emr-serverless/job-run-status.
type emrJobRunStatusSeed struct {
	// JobRunID is the run the seed targets; "" or "*" means every run.
	JobRunID string `json:"jobRunId"`
	// PendingObservations is how many observations report State before FinalState.
	PendingObservations int `json:"pendingObservations"`
	// State is the non-terminal state those observations report; RUNNING when empty.
	State string `json:"state"`
	// FinalState is the state reported once the countdown is spent; the record's own when empty.
	FinalState string `json:"finalState"`
	// StateDetails is the stateDetails the final state reports; substrate's wording when empty.
	StateDetails string `json:"stateDetails"`
}

// emrJobRunProgressions is the job-run kind's seeded countdown.
var emrJobRunProgressions = newProgression[emrJobRunStatusSeed]("emrserverless-jobrun-ctrl", "jobRunId", "pendingObservations")

// progressionID implements [progressionSeed].
func (seed emrJobRunStatusSeed) progressionID() string { return seed.JobRunID }

// progressionObservations implements [progressionSeed].
func (seed emrJobRunStatusSeed) progressionObservations() int { return seed.PendingObservations }

// validateProgression implements [progressionSeed]: both states must be published, the transient one
// non-terminal and the final one terminal, and stateDetails must satisfy API_JobRun's 1–256 length
// and non-blank pattern.
func (seed emrJobRunStatusSeed) validateProgression() error {
	if err := progressionStates("job run", emrJobRunStatesPublished, seed.State, seed.FinalState); err != nil {
		return err
	}
	if seed.State != "" && slices.Contains(emrJobRunTerminalStates, seed.State) {
		return fmt.Errorf("state %q is terminal; a transient state must not be one of %v", seed.State, emrJobRunTerminalStates)
	}
	if seed.FinalState != "" && !slices.Contains(emrJobRunTerminalStates, seed.FinalState) {
		return fmt.Errorf("finalState %q is not terminal, want one of %v", seed.FinalState, emrJobRunTerminalStates)
	}
	if seed.StateDetails != "" && (len(seed.StateDetails) > 256 || !emrNonBlankPattern.MatchString(seed.StateDetails)) {
		return fmt.Errorf("stateDetails must be 1-256 characters and not blank")
	}
	return nil
}

// emrApplicationStatusSeed is the body of POST /v1/emr-serverless/application-status.
type emrApplicationStatusSeed struct {
	// ApplicationID is the application the seed targets; "" or "*" means every application.
	ApplicationID string `json:"applicationId"`
	// PendingObservations is how many GetApplication observations report State before FinalState.
	PendingObservations int `json:"pendingObservations"`
	// State is the transient state those observations report; CREATING when empty.
	State string `json:"state"`
	// FinalState is the state reported once the countdown is spent; the record's own when empty.
	FinalState string `json:"finalState"`
}

// emrApplicationProgressions is the application kind's seeded countdown.
var emrApplicationProgressions = newProgression[emrApplicationStatusSeed]("emrserverless-app-ctrl", "applicationId", "pendingObservations")

// progressionID implements [progressionSeed].
func (seed emrApplicationStatusSeed) progressionID() string { return seed.ApplicationID }

// progressionObservations implements [progressionSeed].
func (seed emrApplicationStatusSeed) progressionObservations() int {
	return seed.PendingObservations
}

// validateProgression implements [progressionSeed]: both states must be in API_Application's list.
func (seed emrApplicationStatusSeed) validateProgression() error {
	return progressionStates("application", emrApplicationStates, seed.State, seed.FinalState)
}

// observeJobRun returns the run as this observation reports it, spending the observation.
func (p *EMRServerlessPlugin) observeJobRun(run EMRServerlessJobRun) (EMRServerlessJobRun, string, error) {
	seed, seen, err := emrJobRunProgressions.observe(context.Background(), p.state, &p.seedMu, run.JobRunID)
	if err != nil {
		return run, "", fmt.Errorf("emrserverless observeJobRun: %w", err)
	}
	if seed == nil {
		return run, "", nil
	}
	if run.State == "CANCELLED" {
		state, _ := countdownState(seen, seed.PendingObservations, "CANCELLING", "CANCELLED", "CANCELLING", "CANCELLED")
		run.State = state
		return run, "", nil
	}
	state, terminal := countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "RUNNING", run.State)
	run.State = state
	if terminal {
		return run, seed.StateDetails, nil
	}
	return run, "", nil
}

// observeApplication returns the application as this observation reports it, spending it.
func (p *EMRServerlessPlugin) observeApplication(app EMRServerlessApp) (EMRServerlessApp, error) {
	seed, seen, err := emrApplicationProgressions.observe(context.Background(), p.state, &p.seedMu, app.ApplicationID)
	if err != nil {
		return app, fmt.Errorf("emrserverless observeApplication: %w", err)
	}
	if seed == nil {
		return app, nil
	}
	app.State, _ = countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "CREATING", app.State)
	return app, nil
}

// handleEMRServerlessSeedJobRunStatus handles POST /v1/emr-serverless/job-run-status. Body:
// {"jobRunId","pendingObservations","state","finalState","stateDetails"}.
func (s *Server) handleEMRServerlessSeedJobRunStatus(w http.ResponseWriter, r *http.Request) {
	emrJobRunProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleEMRServerlessClearJobRunStatus handles DELETE /v1/emr-serverless/job-run-status; with
// ?jobRunId=… it removes that seed, without it every one.
func (s *Server) handleEMRServerlessClearJobRunStatus(w http.ResponseWriter, r *http.Request) {
	emrJobRunProgressions.serveClear(w, r, s.state, s.logger)
}

// handleEMRServerlessSeedApplicationStatus handles POST /v1/emr-serverless/application-status. Body:
// {"applicationId","pendingObservations","state","finalState"}.
func (s *Server) handleEMRServerlessSeedApplicationStatus(w http.ResponseWriter, r *http.Request) {
	emrApplicationProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleEMRServerlessClearApplicationStatus handles DELETE /v1/emr-serverless/application-status;
// with ?applicationId=… it removes that seed, without it every one.
func (s *Server) handleEMRServerlessClearApplicationStatus(w http.ResponseWriter, r *http.Request) {
	emrApplicationProgressions.serveClear(w, r, s.state, s.logger)
}
