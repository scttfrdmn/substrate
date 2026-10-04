package emulator

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

// A build's seeded progression (#1155).
//
// StartBuild completes the build in the request that starts it, so an unseeded build is SUCCEEDED,
// with currentPhase COMPLETED, on its first BatchGetBuilds. A seed written to
// POST /v1/codebuild/build-status makes the published in-progress state observable: BatchGetBuilds
// reports buildStatus IN_PROGRESS, with the seed's currentPhase and no endTime, for a seeded number
// of observations, then the final status. A failed final status may carry the diagnostic API_Build
// publishes for it, which lives in a phase's contexts rather than on the build: the seed's message
// is rendered as the failed phase's PhaseContext. See [progression] and docs/services.md "How a
// progression is seeded".
//
// StartBuild's own response is a create-time read: it peeks the seed, reporting its first state
// without spending an observation, so pendingObservations counts BatchGetBuilds polls only.

// codebuildBuildCtrlNamespace is the [StateManager] namespace the build progression lives in.
const codebuildBuildCtrlNamespace = "codebuild-build-ctrl"

// codebuildBuildFinalStatuses are API_Build's buildStatus values a build settles in; the sixth,
// IN_PROGRESS, is what a seeded countdown reports before it.
var codebuildBuildFinalStatuses = []string{"SUCCEEDED", "FAILED", "FAULT", "TIMED_OUT", "STOPPED"}

// codebuildBuildFailureStatuses are the final statuses a phase reports as failed, which is when a
// seeded diagnostic is rendered.
var codebuildBuildFailureStatuses = []string{"FAILED", "FAULT", "TIMED_OUT", "STOPPED"}

// codebuildPhaseTypes is API_BuildPhase's phaseType Valid Values, less COMPLETED, which names the
// end of a build rather than a phase it can be in.
var codebuildPhaseTypes = []string{
	"SUBMITTED", "QUEUED", "PROVISIONING", "DOWNLOAD_SOURCE", "INSTALL",
	"PRE_BUILD", "BUILD", "POST_BUILD", "UPLOAD_ARTIFACTS", "FINALIZING",
}

// codebuildBuildStatusSeed is the body of POST /v1/codebuild/build-status.
type codebuildBuildStatusSeed struct {
	// BuildID is the build the seed targets ("project:uuid"); empty or "*" targets every build.
	BuildID string `json:"buildId"`
	// PendingObservations is how many BatchGetBuilds observations report IN_PROGRESS.
	PendingObservations int `json:"pendingObservations"`
	// CurrentPhase is the phase reported while in progress, and the phase a seeded failure is
	// reported in; default BUILD.
	CurrentPhase string `json:"currentPhase"`
	// FinalStatus is the buildStatus reported once the countdown is exhausted; default the record's.
	FinalStatus string `json:"finalStatus"`
	// StatusCode and Message are the failed phase's PhaseContext, rendered when FinalStatus is a
	// failure and either is set.
	StatusCode string `json:"statusCode"`
	Message    string `json:"message"`
}

func (s codebuildBuildStatusSeed) progressionID() string        { return s.BuildID }
func (s codebuildBuildStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a phase or final status outside API_BuildPhase's and API_Build's
// published values.
func (s codebuildBuildStatusSeed) validateProgression() error {
	if err := progressionStates("build phase", codebuildPhaseTypes, s.CurrentPhase); err != nil {
		return err
	}
	return progressionStates("build final", codebuildBuildFinalStatuses, s.FinalStatus)
}

// codebuildBuildProgressions is the build kind's seeded countdown, in [codebuildBuildCtrlNamespace].
var codebuildBuildProgressions = newProgression[codebuildBuildStatusSeed](codebuildBuildCtrlNamespace, "buildId", "pendingObservations")

// codebuildPhaseContextOut is API_PhaseContext.
type codebuildPhaseContextOut struct {
	Message    string `json:"message,omitempty"`
	StatusCode string `json:"statusCode,omitempty"`
}

// codebuildBuildPhaseOut is the one API_BuildPhase element a seeded failure reports: the phase the
// build failed in, with its diagnostic.
type codebuildBuildPhaseOut struct {
	Contexts    []codebuildPhaseContextOut `json:"contexts,omitempty"`
	PhaseStatus string                     `json:"phaseStatus"`
	PhaseType   string                     `json:"phaseType"`
}

// observedBuild returns the build as one read reports it. spend selects observe (BatchGetBuilds,
// which counts) or peek (StartBuild's own response).
func (p *CodeBuildPlugin) observedBuild(ctx context.Context, build CodeBuildBuild, spend bool) (codebuildBuildOut, error) {
	out := codebuildBuildToWire(build)
	var (
		seed *codebuildBuildStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = codebuildBuildProgressions.observe(ctx, p.state, &p.seedMu, build.ID)
	} else {
		seed, seen, err = codebuildBuildProgressions.peek(ctx, p.state, build.ID)
	}
	if err != nil {
		return codebuildBuildOut{}, fmt.Errorf("codebuild build status: %w", err)
	}
	if seed == nil {
		return out, nil
	}
	phase := seed.CurrentPhase
	if phase == "" {
		phase = "BUILD"
	}
	status, terminal := countdownState(seen, seed.PendingObservations, "IN_PROGRESS", seed.FinalStatus, "IN_PROGRESS", build.BuildStatus)
	out.BuildStatus = status
	if !terminal {
		// endTime is "when the build process ended", so a build still in progress has none.
		out.CurrentPhase = phase
		out.EndTime = EpochSeconds{}
		return out, nil
	}
	if slices.Contains(codebuildBuildFailureStatuses, status) && (seed.StatusCode != "" || seed.Message != "") {
		out.Phases = []codebuildBuildPhaseOut{{
			Contexts:    []codebuildPhaseContextOut{{Message: seed.Message, StatusCode: seed.StatusCode}},
			PhaseStatus: status,
			PhaseType:   phase,
		}}
	}
	return out, nil
}

// handleCodeBuildSeedBuildStatus handles POST /v1/codebuild/build-status. It is mounted inside
// server.go's control-plane group, so the seed is recorded and a replay re-issues it (#1140).
func (s *Server) handleCodeBuildSeedBuildStatus(w http.ResponseWriter, r *http.Request) {
	codebuildBuildProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleCodeBuildClearBuildStatus handles DELETE /v1/codebuild/build-status: ?buildId=… removes one
// seed and its countdown, and no query removes every one.
func (s *Server) handleCodeBuildClearBuildStatus(w http.ResponseWriter, r *http.Request) {
	codebuildBuildProgressions.serveClear(w, r, s.state, s.logger)
}
