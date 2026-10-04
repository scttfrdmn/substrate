package emulator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// A HealthOmics run's status progression (#1371, #1165).
//
// API_GetRun publishes a run's status as `PENDING | STARTING | RUNNING | STOPPING | COMPLETED |
// DELETED | CANCELLED | FAILED`. Substrate runs no workflow, so a run is COMPLETED in the request
// that starts it — the nominal success path, and still what an unseeded run reports. A seed, written
// through POST /v1/omics/run-status, makes the transition observable instead: a seeded run reports
// the published start-up statuses for a countdown of observations, then settles as COMPLETED or as
// FAILED with the seeded failureReason. The countdown is the shared one in progression.go, and its
// rules — keyed by run ID or "*", counted per run, GetRun and ListRuns observe while StartRun,
// CancelRun and DeleteRun peek — are the ones every progressing resource keeps.
//
// # Cancelling
//
// CancelRun is only applicable to a run that has not settled: API_CancelRun publishes
// ConflictException, "the request cannot be applied to the target resource in its current state",
// and a COMPLETED, FAILED or CANCELLED run is in no state a cancel applies to. A cancel of an active
// run writes CANCELLED into the record and restarts the run's countdown, so the next
// stoppingObservations observations report STOPPING and the run then reports CANCELLED. An unseeded
// run is COMPLETED from its first observation, so it is never active and a cancel of it is refused.

// omicsRunStatus values, spelled as API_GetRun and API_RunListItem publish them.
const (
	omicsRunStatusPending   = "PENDING"
	omicsRunStatusStarting  = "STARTING"
	omicsRunStatusRunning   = "RUNNING"
	omicsRunStatusStopping  = "STOPPING"
	omicsRunStatusCompleted = "COMPLETED"
	omicsRunStatusFailed    = "FAILED"
)

// omicsRunStartupStatuses are the statuses a seeded run reports while its countdown runs, in the
// order a real run passes through them. The countdown's n-th observation reports the n-th, and every
// observation past the third reports RUNNING.
var omicsRunStartupStatuses = []string{omicsRunStatusPending, omicsRunStatusStarting, omicsRunStatusRunning}

// omicsRunFinalStatuses are the settled statuses a seed may name: the two a run that was not
// cancelled can end in.
var omicsRunFinalStatuses = []string{omicsRunStatusCompleted, omicsRunStatusFailed}

// omicsRunDefaultStoppingObservations is how many observations a cancelled run reports STOPPING for
// when its seed does not say. One, so a cancel always passes through STOPPING, as a real run does.
const omicsRunDefaultStoppingObservations = 1

// omicsRunStatusSeed is the body of POST /v1/omics/run-status.
type omicsRunStatusSeed struct {
	// RunID is the run the seed applies to, or "*" (or empty) for every run. A run started after a
	// "*" seed is in place progresses from its first observation, which is what makes the
	// StartRun → poll GetRun → settled sequence testable end to end.
	RunID string `json:"runId"`

	// PendingObservations is how many observations report a start-up status before the run settles.
	// Zero means the run is settled from its first observation.
	PendingObservations int `json:"pendingObservations"`

	// State pins the start-up status every pending observation reports. Empty walks the published
	// order — PENDING, STARTING, then RUNNING.
	State string `json:"state"`

	// FinalState is the status the run settles in: COMPLETED (the default) or FAILED.
	FinalState string `json:"finalState"`

	// FailureReason is GetRun's failureReason once the run settles as FAILED. It has no default:
	// inventing a diagnostic would be indistinguishable from an observation.
	FailureReason string `json:"failureReason"`

	// StoppingObservations is how many observations report STOPPING after CancelRun before the run
	// reports CANCELLED. Absent means one; zero makes the cancel immediate.
	StoppingObservations *int `json:"stoppingObservations,omitempty"`
}

func (s omicsRunStatusSeed) progressionID() string        { return s.RunID }
func (s omicsRunStatusSeed) progressionObservations() int { return s.PendingObservations }

func (s omicsRunStatusSeed) validateProgression() error {
	if err := progressionStates("run start-up", omicsRunStartupStatuses, s.State); err != nil {
		return err
	}
	if err := progressionStates("run final", omicsRunFinalStatuses, s.FinalState); err != nil {
		return err
	}
	if s.StoppingObservations != nil && *s.StoppingObservations < 0 {
		return errors.New("stoppingObservations must be >= 0")
	}
	// API_GetRun constrains failureReason to 1–64 characters.
	if len(s.FailureReason) > 64 {
		return errors.New("failureReason must be at most 64 characters, as API_GetRun publishes")
	}
	return nil
}

// stopping returns how many observations a cancelled run governed by s reports STOPPING for.
func (s omicsRunStatusSeed) stopping() int {
	if s.StoppingObservations == nil {
		return omicsRunDefaultStoppingObservations
	}
	return *s.StoppingObservations
}

// omicsRunProgressions is the run-status countdown. It holds configuration only (see
// [progression]); the plugin owns the state and the mutex.
var omicsRunProgressions = newProgression[omicsRunStatusSeed]("omics-run-ctrl", "runId", "pendingObservations")

// omicsRunObservation is what one observation of a run reports.
type omicsRunObservation struct {
	status        string
	failureReason string
}

// settled reports whether the observation is a status no CancelRun applies to, and which DeleteRun
// requires: API_DeleteRun allows only a run "that has reached a COMPLETED, FAILED, or CANCELLED
// stage".
func (o omicsRunObservation) settled() bool {
	switch o.status {
	case omicsRunStatusCompleted, omicsRunStatusFailed, omicsRunStatusCancelled:
		return true
	}
	return false
}

// omicsRunReport returns what the seen-th observation of run reports under seed (nil when no seed
// applies). It is pure arithmetic over the record, the seed and the count; [OmicsPlugin.observeRun]
// and [OmicsPlugin.peekRun] supply them.
func omicsRunReport(run OmicsRun, seed *omicsRunStatusSeed, seen int) omicsRunObservation {
	if run.Status == omicsRunStatusCancelled {
		if seed != nil && seen < seed.stopping() {
			return omicsRunObservation{status: omicsRunStatusStopping}
		}
		return omicsRunObservation{status: omicsRunStatusCancelled}
	}
	if seed == nil {
		return omicsRunObservation{status: run.Status}
	}
	if seen < seed.PendingObservations {
		if seed.State != "" {
			return omicsRunObservation{status: seed.State}
		}
		return omicsRunObservation{status: omicsRunStartupStatuses[min(seen, len(omicsRunStartupStatuses)-1)]}
	}
	final := seed.FinalState
	if final == "" {
		final = omicsRunStatusCompleted
	}
	obs := omicsRunObservation{status: final}
	if final == omicsRunStatusFailed {
		obs.failureReason = seed.FailureReason
	}
	return obs
}

// omicsRunCountdown is the length of the countdown governing run: the start-up countdown, or — once
// the run is cancelled — the STOPPING one.
func omicsRunCountdown(run OmicsRun, seed omicsRunStatusSeed) int {
	if run.Status == omicsRunStatusCancelled {
		return seed.stopping()
	}
	return seed.PendingObservations
}

// observeRun reports one observation of run and spends it. The countdown's length depends on the
// run's phase — start-up or STOPPING — so it is assembled from the progression's primitives under
// the plugin's seed mutex, as EC2 instances do, rather than through [progression.observe], whose
// count is the seed's fixed one.
func (p *OmicsPlugin) observeRun(ctx context.Context, run OmicsRun) (omicsRunObservation, error) {
	p.seedMu.Lock()
	defer p.seedMu.Unlock()
	seed, err := omicsRunProgressions.resolve(ctx, p.state, run.ID)
	if err != nil || seed == nil {
		return omicsRunReport(run, nil, 0), err
	}
	seen, err := omicsRunProgressions.observations(ctx, p.state, run.ID)
	if err != nil {
		return omicsRunObservation{}, err
	}
	if seen < omicsRunCountdown(run, *seed) {
		if err := omicsRunProgressions.advance(ctx, p.state, run.ID, seen); err != nil {
			return omicsRunObservation{}, err
		}
	}
	return omicsRunReport(run, seed, seen), nil
}

// peekRun reports what the next observation of run would, without spending it.
func (p *OmicsPlugin) peekRun(ctx context.Context, run OmicsRun) (omicsRunObservation, error) {
	seed, seen, err := omicsRunProgressions.peek(ctx, p.state, run.ID)
	if err != nil {
		return omicsRunObservation{}, err
	}
	return omicsRunReport(run, seed, seen), nil
}

// handleOmicsSeedRunStatus handles POST /v1/omics/run-status. Body:
// {"runId","pendingObservations","state","finalState","failureReason","stoppingObservations"}.
func (s *Server) handleOmicsSeedRunStatus(w http.ResponseWriter, r *http.Request) {
	omicsRunProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleOmicsClearRunStatus handles DELETE /v1/omics/run-status. With ?runId=… it removes that
// seed and the run's countdown; without it, every run-status seed and counter.
func (s *Server) handleOmicsClearRunStatus(w http.ResponseWriter, r *http.Request) {
	omicsRunProgressions.serveClear(w, r, s.state, s.logger)
}

// omicsConflict is the ConflictException API_CancelRun and API_DeleteRun publish for a run "in its
// current state" the operation cannot be applied to.
func omicsConflict(op, runID, status string) error {
	return &AWSError{
		Code:       "ConflictException",
		Message:    fmt.Sprintf("%s cannot be applied to run %s in status %s", op, runID, status),
		HTTPStatus: http.StatusConflict,
	}
}
