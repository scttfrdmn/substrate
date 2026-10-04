package emulator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
)

// A Batch job's status, as an observation reports it (#1248).
//
// The Batch user guide's Job states page: "When you submit a job to an AWS Batch job queue, the
// job enters the SUBMITTED state. It then passes through the following states until it succeeds
// … or fails". API_ListJobs publishes the seven values of jobStatus. Substrate used to store
// SUCCEEDED in the request that submitted the job, so six of the seven were unreachable, a
// consumer's waiter exited on its first poll, and ListJobs' published RUNNING default (#1236) was
// empty on every run.
//
// A submitted job's record now holds SUBMITTED, and what DescribeJobs and ListJobs report is the
// job's progression ([progression], #1155, #1196):
//
//   - **Unseeded**, the first observation reports SUCCEEDED. That is the default an unsized
//     progression settles to, and it keeps every fixture written before the fix meaning what it
//     meant: one describe, one success.
//   - **Seeded**, the job spends the seed's count of observations in each transient state it names,
//     walked in the published order SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING, and then reports
//     the seed's terminal state: SUCCEEDED, or FAILED with the seed's statusReason.
//   - A record that is already terminal — a job TerminateJob or CancelJob ended — reports its record,
//     whatever the seed says. A seed governs what an observation reports and never rewrites the
//     record; only the two operations a caller sends to end a job do.
//
// How many observations a default progression spends in each state is unpublished: substrate's
// choice is none, recorded here and in docs/services.md.

// batchJobTransientStates are the five non-terminal values of API_ListJobs' jobStatus, in the order
// the Job states page draws them.
var batchJobTransientStates = []string{"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}

// batchJobTerminalStates are the two terminal values of jobStatus.
var batchJobTerminalStates = []string{"SUCCEEDED", "FAILED"}

// batchJobDefaultFinalState is what an unsized progression settles to.
const batchJobDefaultFinalState = "SUCCEEDED"

// batchJobStatusSeed is the body of POST /v1/batch/job-status.
type batchJobStatusSeed struct {
	// JobID is the job the seed targets; "" or "*" seeds every job.
	JobID string `json:"jobId"`
	// TransientObservations is how many observations the job spends in each transient state it
	// names, keyed by state. A state absent or at zero is passed over.
	TransientObservations map[string]int `json:"transientObservations"`
	// FinalState is the terminal state the job settles to: SUCCEEDED (the default) or FAILED.
	FinalState string `json:"finalState,omitempty"`
	// StatusReason is the statusReason the settled job reports — the failure branch's message.
	StatusReason string `json:"statusReason,omitempty"`
}

func (s batchJobStatusSeed) progressionID() string { return s.JobID }

func (s batchJobStatusSeed) progressionObservations() int {
	total := 0
	for _, n := range s.TransientObservations {
		total += n
	}
	return total
}

func (s batchJobStatusSeed) validateProgression() error {
	// Sorted, so a seed naming two bad states is refused naming the same one every time.
	for _, state := range slices.Sorted(maps.Keys(s.TransientObservations)) {
		if state == "" {
			return errors.New("transientObservations names an empty state")
		}
		if err := progressionStates("transient job", batchJobTransientStates, state); err != nil {
			return err
		}
		if n := s.TransientObservations[state]; n < 0 {
			return fmt.Errorf("transientObservations[%s] must be >= 0", state)
		}
	}
	return progressionStates("terminal job", batchJobTerminalStates, s.FinalState)
}

// stateAt returns the status the seen-th observation (counting from zero) reports under the seed,
// and the statusReason it carries. Only a settled observation carries the seed's reason.
func (s batchJobStatusSeed) stateAt(seen int) (status, reason string) {
	for _, state := range batchJobTransientStates {
		n := s.TransientObservations[state]
		if seen < n {
			return state, ""
		}
		seen -= n
	}
	if s.FinalState == "" {
		return batchJobDefaultFinalState, s.StatusReason
	}
	return s.FinalState, s.StatusReason
}

// batchJobProgressions is the job kind's progression. It holds no state; see [progression].
var batchJobProgressions = newProgression[batchJobStatusSeed]("batch-job-ctrl", "jobId", "transientObservations")

// batchJobTerminal reports whether status is one a job never leaves.
func batchJobTerminal(status string) bool {
	return status == "SUCCEEDED" || status == "FAILED"
}

// batchJobObserved returns job as an observation reports it. observe spends one of the job's
// observations, as DescribeJobs and ListJobs do; otherwise it peeks, as TerminateJob and CancelJob
// do before deciding what to end.
func batchJobObserved(ctx context.Context, state StateManager, mu *sync.Mutex, job BatchJob, observe bool) (BatchJob, error) {
	if batchJobTerminal(job.Status) {
		return job, nil
	}
	var (
		seed *batchJobStatusSeed
		seen int
		err  error
	)
	if observe {
		seed, seen, err = batchJobProgressions.observe(ctx, state, mu, job.JobID)
	} else {
		seed, seen, err = batchJobProgressions.peek(ctx, state, job.JobID)
	}
	if err != nil {
		return BatchJob{}, err
	}
	if seed == nil {
		job.Status = batchJobDefaultFinalState
		return job, nil
	}
	job.Status, job.StatusReason = seed.stateAt(seen)
	return job, nil
}

// handleBatchSeedJobStatus handles POST /v1/batch/job-status: it seeds how many observations a job
// spends in each transient state, and what it settles to.
func (s *Server) handleBatchSeedJobStatus(w http.ResponseWriter, r *http.Request) {
	batchJobProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleBatchClearJobStatus handles DELETE /v1/batch/job-status: ?jobId=… clears one seed and its
// job's countdown; with no query it clears every Batch job seed.
func (s *Server) handleBatchClearJobStatus(w http.ResponseWriter, r *http.Request) {
	batchJobProgressions.serveClear(w, r, s.state, s.logger)
}
