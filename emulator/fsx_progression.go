package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// An FSx file system's seeded lifecycle progression (#1196).
//
// API_CreateFileSystem publishes the initial state outright: it creates a file system "with … an
// initial lifecycle state of CREATING", and "the CreateFileSystem call returns while the file
// system's lifecycle state is still CREATING". Substrate recorded and answered AVAILABLE, so a
// wait-until-available loop exited before it polled, and FAILED and MISCONFIGURED — the states the
// consumer's error handling exists for — could not be produced.
//
// # What is reported, and when
//
//   - CreateFileSystem answers CREATING, as the page says, whatever is seeded. The create's own
//     answer is not an observation; the record itself is the settled file system, AVAILABLE.
//   - DescribeFileSystems observes each file system it reports, through the shared progression in
//     emulator/progression.go: a seed keyed by file system ID or "*" holds pendingObservations
//     observations at a transient state (CREATING by default; UPDATING may be named) and then reports
//     the final state — the record's AVAILABLE, or FAILED, MISCONFIGURED or
//     MISCONFIGURED_UNAVAILABLE, with FailureDetails.Message when the seed gives one. Unseeded, a file
//     system reads AVAILABLE from its first describe, exactly as before.
//
// # Deleting
//
// API_DeleteFileSystem says a successful delete reports DELETING, and DeleteFileSystem answers that.
// Unseeded, the file system is gone from the next describe, as #1210 made it. Under a seed with
// observations to spend, the delete instead keeps the record, marked DELETING, and restarts its
// countdown: the next pendingObservations describes report DELETING, and the one after finishes the
// delete and answers FileSystemNotFound. That is the "deleting state for at least one observation
// before the record becomes unreadable" #1196 asks for, and it is what lets a wait-until-deleted loop
// spend a poll. The record is removed by the observation that ends the window, which is the delete
// completing — not the seed rewriting the record, which a seed never does.
//
// A delete of a file system already DELETING answers DELETING again and does not restart the window.

// fsxLifecycles is API_FileSystem's Lifecycle Valid Values.
var fsxLifecycles = []string{"AVAILABLE", "CREATING", "FAILED", "DELETING", "MISCONFIGURED", "UPDATING", "MISCONFIGURED_UNAVAILABLE"}

// fsxTransientLifecycles are the states a seed may hold a live file system at.
var fsxTransientLifecycles = []string{"CREATING", "UPDATING"}

// fsxFinalLifecycles are the states a seed may settle a live file system at.
var fsxFinalLifecycles = []string{"AVAILABLE", "FAILED", "MISCONFIGURED", "MISCONFIGURED_UNAVAILABLE"}

// fsxFileSystemStatusSeed is the body of POST /v1/fsx/file-system-status.
type fsxFileSystemStatusSeed struct {
	// FileSystemID is the file system the seed targets; "" or "*" means every file system.
	FileSystemID string `json:"fileSystemId"`
	// PendingObservations is how many describes report the transient state — or DELETING, after a
	// delete — before the final state, or before the deleted file system is gone.
	PendingObservations int `json:"pendingObservations"`
	// State is the transient state; CREATING when empty.
	State string `json:"state"`
	// FinalState is the settled state; the record's AVAILABLE when empty.
	FinalState string `json:"finalState"`
	// FailureMessage is the FailureDetails.Message a failed final state reports.
	FailureMessage string `json:"failureMessage"`
}

// fsxFileSystemProgressions is the file-system kind's seeded countdown.
var fsxFileSystemProgressions = newProgression[fsxFileSystemStatusSeed]("fsx-fs-ctrl", "fileSystemId", "pendingObservations")

// progressionID implements [progressionSeed].
func (seed fsxFileSystemStatusSeed) progressionID() string { return seed.FileSystemID }

// progressionObservations implements [progressionSeed].
func (seed fsxFileSystemStatusSeed) progressionObservations() int { return seed.PendingObservations }

// validateProgression implements [progressionSeed]: the states must be published, the transient one
// CREATING or UPDATING, the final one a settled state, and a failure message only beside a failed one.
func (seed fsxFileSystemStatusSeed) validateProgression() error {
	if err := progressionStates("file system", fsxLifecycles, seed.State, seed.FinalState); err != nil {
		return err
	}
	if seed.State != "" && !slices.Contains(fsxTransientLifecycles, seed.State) {
		return fmt.Errorf("state %q is not a transient state, want one of %v", seed.State, fsxTransientLifecycles)
	}
	if seed.FinalState != "" && !slices.Contains(fsxFinalLifecycles, seed.FinalState) {
		return fmt.Errorf("finalState %q is not a settled state, want one of %v", seed.FinalState, fsxFinalLifecycles)
	}
	if seed.FailureMessage != "" && (seed.FinalState == "" || seed.FinalState == "AVAILABLE") {
		return fmt.Errorf("failureMessage describes a FAILED or MISCONFIGURED file system, not %q", seed.FinalState)
	}
	return nil
}

// fsxObservation is what one observation of a file system reports.
type fsxObservation struct {
	// Gone is true when the observation ended a deleting window: the file system no longer exists.
	Gone bool
	// Wire is the file system as reported; nil when Gone.
	Wire map[string]interface{}
}

// observeFileSystem reports one describe of fs, spending the observation. When it ends a deleting
// window it completes the delete and reports the file system gone.
func (p *FSxPlugin) observeFileSystem(ctx *RequestContext, fs FSxFileSystem) (fsxObservation, error) {
	goCtx := context.Background()
	seed, seen, err := fsxFileSystemProgressions.observe(goCtx, p.state, &p.seedMu, fs.FileSystemID)
	if err != nil {
		return fsxObservation{}, fmt.Errorf("fsx observeFileSystem: %w", err)
	}
	wire := fsxToWire(fs)
	if fs.Lifecycle == fsxLifecycleDeleting {
		if seed != nil && seen < seed.PendingObservations {
			return fsxObservation{Wire: wire}, nil
		}
		if err := p.finishDelete(ctx, fs.FileSystemID); err != nil {
			return fsxObservation{}, err
		}
		return fsxObservation{Gone: true}, nil
	}
	if seed == nil {
		return fsxObservation{Wire: wire}, nil
	}
	state, terminal := countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "CREATING", fs.Lifecycle)
	wire["Lifecycle"] = state
	if terminal && seed.FailureMessage != "" {
		wire["FailureDetails"] = map[string]string{"Message": seed.FailureMessage}
	}
	return fsxObservation{Wire: wire}, nil
}

// beginDelete decides how a delete of fs takes effect. Under a seed with observations to spend it
// marks the record DELETING and restarts the countdown, and reports true; otherwise it reports false
// and the caller removes the record at once.
func (p *FSxPlugin) beginDelete(ctx *RequestContext, fs FSxFileSystem) (bool, error) {
	goCtx := context.Background()
	seed, err := fsxFileSystemProgressions.resolve(goCtx, p.state, fs.FileSystemID)
	if err != nil {
		return false, fmt.Errorf("fsx beginDelete: %w", err)
	}
	if seed == nil || seed.PendingObservations == 0 {
		return false, nil
	}
	fs.Lifecycle = fsxLifecycleDeleting
	if err := p.putFileSystem(ctx, fs); err != nil {
		return false, err
	}
	if err := fsxFileSystemProgressions.reset(goCtx, p.state, fs.FileSystemID); err != nil {
		return false, fmt.Errorf("fsx beginDelete: %w", err)
	}
	return true, nil
}

// putFileSystem stores fs's record.
func (p *FSxPlugin) putFileSystem(ctx *RequestContext, fs FSxFileSystem) error {
	data, err := json.Marshal(fs)
	if err != nil {
		return fmt.Errorf("fsx put %s marshal: %w", fs.FileSystemID, err)
	}
	if err := p.state.Put(context.Background(), fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, fs.FileSystemID), data); err != nil {
		return fmt.Errorf("fsx put %s: %w", fs.FileSystemID, err)
	}
	return nil
}

// finishDelete removes a file system's record and its index entry.
func (p *FSxPlugin) finishDelete(ctx *RequestContext, id string) error {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, id)); err != nil {
		return fmt.Errorf("fsx delete %s: %w", id, err)
	}
	removeFromStringIndex(goCtx, p.state, fsxNamespace, fsxIDsKey(ctx.AccountID, ctx.Region), id)
	return nil
}

// handleFSxSeedFileSystemStatus handles POST /v1/fsx/file-system-status. Body:
// {"fileSystemId","pendingObservations","state","finalState","failureMessage"}.
func (s *Server) handleFSxSeedFileSystemStatus(w http.ResponseWriter, r *http.Request) {
	fsxFileSystemProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleFSxClearFileSystemStatus handles DELETE /v1/fsx/file-system-status; with ?fileSystemId=… it
// removes that seed, without it every one.
func (s *Server) handleFSxClearFileSystemStatus(w http.ResponseWriter, r *http.Request) {
	fsxFileSystemProgressions.serveClear(w, r, s.state, s.logger)
}
