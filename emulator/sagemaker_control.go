package emulator

import (
	"context"
	"fmt"
)

// sagemakerCtrlNamespace is the state namespace for SageMaker's training-job status seeds and their
// per-job observation counters.
const sagemakerCtrlNamespace = "sagemaker-ctrl"

// sagemakerTrainingJobStatuses is TrainingJobStatus' published enumeration, on both
// API_DescribeTrainingJob and TrainingJobSummary.
var sagemakerTrainingJobStatuses = []string{"InProgress", "Completed", "Failed", "Stopping", "Stopped"}

// sagemakerTrainingJobTransientStatuses are the two TrainingJobStatus values a job passes through
// before it settles: InProgress while it runs, Stopping after StopTrainingJob.
var sagemakerTrainingJobTransientStatuses = []string{"InProgress", "Stopping"}

// sagemakerTrainingJobSeed is the body of POST /v1/sagemaker/training-job-status: how many
// observations of a training job report a transient status before it settles, and what it settles
// to. docs/services.md, "How a progression is seeded", states the rules every progression shares.
//
// The pre-#1155 body — trainingJobName, status, failureReason — still means what it meant: with no
// pendingObservations the job reports status from its first observation.
type sagemakerTrainingJobSeed struct {
	// TrainingJobName is the job the seed governs; empty or "*" governs every job.
	TrainingJobName string `json:"trainingJobName"`

	// PendingObservations is how many observations report TransientStatus before the job settles.
	PendingObservations int `json:"pendingObservations"`

	// TransientStatus is what the countdown reports: InProgress, or Stopping. Empty reports
	// InProgress for a running job and Stopping for one StopTrainingJob has stopped.
	TransientStatus string `json:"transientStatus,omitempty"`

	// Status is the TrainingJobStatus the job settles to. Empty settles to the job's own recorded
	// status.
	Status string `json:"status"`

	// FailureReason is reported alongside a settled Failed status, as API_DescribeTrainingJob
	// publishes it: "If the training job failed, the reason it failed."
	FailureReason string `json:"failureReason,omitempty"`
}

func (s sagemakerTrainingJobSeed) progressionID() string        { return s.TrainingJobName }
func (s sagemakerTrainingJobSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a status outside the published enumeration (#1162), a transient
// status that is not one of the two a job passes through, a failure reason on a status that is not
// Failed, and a seed that would change nothing.
func (s sagemakerTrainingJobSeed) validateProgression() error {
	if s.Status == "" && s.PendingObservations == 0 {
		return fmt.Errorf("status is required when pendingObservations is 0")
	}
	if err := progressionStates("training job", sagemakerTrainingJobStatuses, s.Status); err != nil {
		return err
	}
	if err := progressionStates("transient training job", sagemakerTrainingJobTransientStatuses, s.TransientStatus); err != nil {
		return err
	}
	if s.FailureReason != "" && s.Status != "Failed" {
		return fmt.Errorf("failureReason applies only to status Failed, got %q", s.Status)
	}
	return nil
}

// sagemakerTrainingJobProgressions is the training-job progression. It holds configuration only.
var sagemakerTrainingJobProgressions = newProgression[sagemakerTrainingJobSeed](sagemakerCtrlNamespace, "trainingJobName", "pendingObservations")

// observeTrainingJob reports what one observation of job sees, spending it (#1155). Describe and
// List both call it, so the two answer the same status for the same job (#1162).
//
// A stopped job keeps its recorded Stopped: the seed's countdown still applies, reporting Stopping
// while it runs, but the seed's settled status and failure reason do not override a stop the caller
// made, because StopTrainingJob is itself the transition being observed.
func (p *SageMakerPlugin) observeTrainingJob(job *SageMakerTrainingJob) error {
	seed, seen, err := sagemakerTrainingJobProgressions.observe(context.Background(), p.state, &p.seedMu, job.TrainingJobName)
	if err != nil {
		return fmt.Errorf("sagemaker observe training job %s: %w", job.TrainingJobName, err)
	}
	if seed == nil {
		return nil
	}
	stopped := job.TrainingJobStatus == "Stopped"
	defaultTransient, final := "InProgress", seed.Status
	if stopped {
		defaultTransient, final = "Stopping", ""
	}
	status, terminal := countdownState(seen, seed.PendingObservations, seed.TransientStatus, final, defaultTransient, job.TrainingJobStatus)
	job.TrainingJobStatus = status
	job.FailureReason = ""
	if terminal && !stopped && status == "Failed" {
		job.FailureReason = seed.FailureReason
	}
	return nil
}
