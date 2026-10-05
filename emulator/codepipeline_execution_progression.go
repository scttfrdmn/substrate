package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// A pipeline execution's seeded progression (#1155).
//
// StartPipelineExecution completes the execution in the request that starts it, so an unseeded
// execution is Succeeded on its first GetPipelineExecution. A seed written to
// POST /v1/codepipeline/execution-status makes the published in-flight states observable:
// GetPipelineExecution reports the seed's transient status — InProgress or Stopping — for a seeded
// number of observations, then its final one, with the statusSummary API_PipelineExecution
// publishes alongside it. See [progression] and docs/services.md "How a progression is seeded".

// codepipelineExecCtrlNamespace is the [StateManager] namespace the execution progression lives in.
const codepipelineExecCtrlNamespace = "codepipeline-exec-ctrl"

// codepipelineExecTransientStatuses are the two of API_PipelineExecution's status values an
// execution reports while it has not settled.
var codepipelineExecTransientStatuses = []string{"InProgress", "Stopping"}

// codepipelineExecFinalStatuses are the five it settles in. Together with the two above they are
// the published Valid Values: Cancelled | InProgress | Stopped | Stopping | Succeeded | Superseded |
// Failed.
var codepipelineExecFinalStatuses = []string{"Succeeded", "Failed", "Stopped", "Superseded", "Cancelled"}

// codepipelineExecStatusSeed is the body of POST /v1/codepipeline/execution-status.
type codepipelineExecStatusSeed struct {
	// PipelineExecutionID is the execution the seed targets; empty or "*" targets every execution.
	PipelineExecutionID string `json:"pipelineExecutionId"`
	// PendingObservations is how many GetPipelineExecution calls report Status before FinalStatus.
	PendingObservations int `json:"pendingObservations"`
	// Status is the transient status; InProgress or Stopping, default InProgress.
	Status string `json:"status"`
	// FinalStatus is the status reported once the countdown is exhausted; default the record's.
	FinalStatus string `json:"finalStatus"`
	// StatusSummary is API_PipelineExecution's statusSummary on the final observation.
	StatusSummary string `json:"statusSummary"`
}

func (s codepipelineExecStatusSeed) progressionID() string        { return s.PipelineExecutionID }
func (s codepipelineExecStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a transient or final status outside its half of the enumeration.
func (s codepipelineExecStatusSeed) validateProgression() error {
	if err := progressionStates("execution transient", codepipelineExecTransientStatuses, s.Status); err != nil {
		return err
	}
	return progressionStates("execution final", codepipelineExecFinalStatuses, s.FinalStatus)
}

// codepipelineExecProgressions is the execution kind's seeded countdown, in
// [codepipelineExecCtrlNamespace].
var codepipelineExecProgressions = newProgression[codepipelineExecStatusSeed](codepipelineExecCtrlNamespace, "pipelineExecutionId", "pendingObservations")

// observedExecution returns the execution as one GetPipelineExecution observation reports it,
// spending one observation of a seeded countdown.
func (p *CodePipelinePlugin) observedExecution(ctx context.Context, exec CodePipelineExecution) (codepipelineExecutionOut, error) {
	out := codepipelineExecutionToWire(exec)
	seed, seen, err := codepipelineExecProgressions.observe(ctx, p.state, &p.seedMu, exec.PipelineExecutionID)
	if err != nil {
		return codepipelineExecutionOut{}, fmt.Errorf("codepipeline execution status: %w", err)
	}
	if seed == nil {
		return out, nil
	}
	status, terminal := countdownState(seen, seed.PendingObservations, seed.Status, seed.FinalStatus, "InProgress", exec.Status)
	out.Status = status
	if terminal {
		out.StatusSummary = seed.StatusSummary
	}
	return out, nil
}

// handleCodePipelineSeedExecutionStatus handles POST /v1/codepipeline/execution-status. It is
// mounted inside server.go's control-plane group, so the seed is recorded and a replay re-issues it
// (#1140).
func (s *Server) handleCodePipelineSeedExecutionStatus(w http.ResponseWriter, r *http.Request) {
	codepipelineExecProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleCodePipelineClearExecutionStatus handles DELETE /v1/codepipeline/execution-status:
// ?pipelineExecutionId=… removes one seed and its countdown, and no query removes every one.
func (s *Server) handleCodePipelineClearExecutionStatus(w http.ResponseWriter, r *http.Request) {
	codepipelineExecProgressions.serveClear(w, r, s.state, s.logger)
}
