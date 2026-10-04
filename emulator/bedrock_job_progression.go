package emulator

import (
	"context"
	"fmt"
	"slices"
)

// A Bedrock batch-inference job's lifecycle, observable through GetModelInvocationJob and
// ListModelInvocationJobs (#1174).
//
// A job is born `Submitted`, the first status API_GetModelInvocationJob documents ("submitted to a
// queue for validation"), and an unseeded job stays there: substrate runs no inference, so no
// transition beyond it happens on its own (CLAUDE.md's scope). Two seeded progressions, both on
// the shared helper in emulator/progression.go (see docs/services.md, "How a progression is
// seeded"), make the rest observable:
//
//   - **The status seed** (`POST`/`DELETE /v1/bedrock/model-invocation-job-status`) reports a
//     pending status (default `InProgress`) for `pendingObservations` reads, then the seeded
//     terminal `status` with its `message`. A seed with no `pendingObservations` settles at once,
//     which is exactly the static override this endpoint was before #1174, so an existing seed
//     body reads as it did.
//   - **The stop countdown** (`POST`/`DELETE /v1/bedrock/model-invocation-job-stop`).
//     StopModelInvocationJob leaves the job `Stopping` — the page's "This job is being stopped by
//     a user" — for `stoppingObservations` reads (one when unseeded), then `Stopped`. Once a job is
//     stopping, the status seed no longer governs it: the stop is the newer transition.
//
// Get and List observe — each read spends one observation of the job's own counter, so one List
// over N jobs under a wildcard seed spends one observation of each, not N of one (#582). The stop
// handler's precondition check peeks, so a refused stop spends nothing.

// bedrockJobStatuses is API_GetModelInvocationJob's published `status` enum, in the page's order.
var bedrockJobStatuses = []string{
	"Submitted", "InProgress", "Completed", "Failed", "Stopping", "Stopped",
	"PartiallyCompleted", "Expired", "Validating", "Scheduled",
}

// bedrockJobTerminalStatuses are the statuses a job does not leave, so StopModelInvocationJob
// refuses them with the page's ConflictException. `Stopping` is not among them: a job already
// being stopped answers a repeated stop with success and is left as it is.
var bedrockJobTerminalStatuses = []string{"Completed", "Failed", "Stopped", "PartiallyCompleted", "Expired"}

const (
	// bedrockJobStatusCtrlNamespace holds status-seed progressions.
	bedrockJobStatusCtrlNamespace = "bedrock-job-status-ctrl"
	// bedrockJobStopCtrlNamespace holds stop countdowns, separate from the status seed so a
	// prefix-sweeping DELETE on one cannot clear the other.
	bedrockJobStopCtrlNamespace = "bedrock-job-stop-ctrl"

	// bedrockDefaultPendingStatus is what a seeded job reports before its seeded status.
	bedrockDefaultPendingStatus = "InProgress"
	// bedrockDefaultStoppingObservations is how many reads report `Stopping` after an unseeded stop.
	bedrockDefaultStoppingObservations = 1
)

// bedrockJobStatusSeed is the body of POST /v1/bedrock/model-invocation-job-status.
type bedrockJobStatusSeed struct {
	// JobID is the job the seed governs; empty or "*" governs every job.
	JobID string `json:"jobId"`
	// PendingObservations is how many reads report PendingStatus before Status.
	PendingObservations int `json:"pendingObservations"`
	// PendingStatus is reported while pending; empty reports InProgress.
	PendingStatus string `json:"pendingStatus,omitempty"`
	// Status is the seeded terminal status, reported once the countdown ends. Required.
	Status string `json:"status"`
	// Message is reported with Status, as the page's failure message.
	Message string `json:"message,omitempty"`
}

func (s bedrockJobStatusSeed) progressionID() string        { return s.JobID }
func (s bedrockJobStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression requires a status, as the endpoint did before #1174, and refuses any status
// outside the published enum (#1162's class).
func (s bedrockJobStatusSeed) validateProgression() error {
	if s.Status == "" {
		return fmt.Errorf("status is required")
	}
	return progressionStates("model invocation job", bedrockJobStatuses, s.PendingStatus, s.Status)
}

// bedrockJobStopSeed is the body of POST /v1/bedrock/model-invocation-job-stop.
type bedrockJobStopSeed struct {
	// JobID is the job the seed governs; empty or "*" governs every job.
	JobID string `json:"jobId"`
	// StoppingObservations is how many reads after a stop report Stopping before Stopped. Zero
	// settles a stop at Stopped on its first read.
	StoppingObservations int `json:"stoppingObservations"`
}

func (s bedrockJobStopSeed) progressionID() string        { return s.JobID }
func (s bedrockJobStopSeed) progressionObservations() int { return s.StoppingObservations }
func (s bedrockJobStopSeed) validateProgression() error   { return nil }

// bedrockJobStatusProgressions and bedrockJobStopProgressions are configuration only: each holds
// three strings, and every observation and seed lives in the StateManager.
var (
	bedrockJobStatusProgressions = newProgression[bedrockJobStatusSeed](bedrockJobStatusCtrlNamespace, "jobId", "pendingObservations")
	bedrockJobStopProgressions   = newProgression[bedrockJobStopSeed](bedrockJobStopCtrlNamespace, "jobId", "stoppingObservations")
)

// bedrockJobView is what one read of a job reports.
type bedrockJobView struct {
	status  string
	message string
}

// bedrockJobObservedStatus reports the status a read of job answers. When spend is true the read
// is an observation (Get, List) and advances the governing countdown; when false it is a peek (the
// stop handler's precondition) and spends nothing. Reads never rewrite the job record.
func (p *BedrockRuntimePlugin) bedrockJobObservedStatus(ctx context.Context, jobID string, job *BedrockModelInvocationJob, spend bool) (bedrockJobView, error) {
	if job.Status == "Stopping" {
		return p.bedrockJobStopStatus(ctx, jobID, job, spend)
	}
	var (
		seed *bedrockJobStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = bedrockJobStatusProgressions.observe(ctx, p.state, &p.seedMu, jobID)
	} else {
		seed, seen, err = bedrockJobStatusProgressions.peek(ctx, p.state, jobID)
	}
	if err != nil {
		return bedrockJobView{}, err
	}
	if seed == nil {
		return bedrockJobView{status: job.Status, message: job.Message}, nil
	}
	status, terminal := countdownState(seen, seed.PendingObservations, seed.PendingStatus, seed.Status,
		bedrockDefaultPendingStatus, seed.Status)
	if !terminal {
		return bedrockJobView{status: status}, nil
	}
	return bedrockJobView{status: status, message: seed.Message}, nil
}

// bedrockJobStopStatus reports a stopping job: Stopping for the countdown, then Stopped. The
// countdown runs without a seed (bedrockDefaultStoppingObservations), so it is built from the
// helper's primitives rather than observe, which advances only a seeded resource.
func (p *BedrockRuntimePlugin) bedrockJobStopStatus(ctx context.Context, jobID string, job *BedrockModelInvocationJob, spend bool) (bedrockJobView, error) {
	if spend {
		p.seedMu.Lock()
		defer p.seedMu.Unlock()
	}
	total := bedrockDefaultStoppingObservations
	seed, err := bedrockJobStopProgressions.resolve(ctx, p.state, jobID)
	if err != nil {
		return bedrockJobView{}, err
	}
	if seed != nil {
		total = seed.StoppingObservations
	}
	seen, err := bedrockJobStopProgressions.observations(ctx, p.state, jobID)
	if err != nil {
		return bedrockJobView{}, err
	}
	if seen >= total {
		return bedrockJobView{status: "Stopped", message: job.Message}, nil
	}
	if spend {
		if err := bedrockJobStopProgressions.advance(ctx, p.state, jobID, seen); err != nil {
			return bedrockJobView{}, err
		}
	}
	return bedrockJobView{status: "Stopping", message: job.Message}, nil
}

// bedrockJobIsTerminal reports whether status is one StopModelInvocationJob refuses.
func bedrockJobIsTerminal(status string) bool {
	return slices.Contains(bedrockJobTerminalStatuses, status)
}
