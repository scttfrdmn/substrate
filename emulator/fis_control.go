package emulator

// The seedable experiment outcome (#1619).
//
// An experiment defaults to its nominal path: its actions run and it completes. A test that
// needs another ending — the failure a consumer's poll loop must report, or a completion without
// waiting out the actions' durations — seeds it:
//
//	POST /v1/fis/experiment-result
//	{"id": "EXT123AbCdEfGhIjK", "status": "failed", "reason": "Insufficient permissions."}
//
// id is an experiment template's ID (every experiment started from it), an experiment's ID, or
// "*" for every experiment; the most specific applies: the experiment's own, its template's, the
// wildcard. status is failed or completed, and reason the state's reason, optional. A seeded
// experiment ends at its next observation (fis_clock.go): effects its actions already had stay,
// and no further action starts. DELETE /v1/fis/experiment-result?id=… clears one seed, and with
// no id clears them all. The seeds live in the state manager, so a reset clears them and a
// replay re-applies them from the recorded control-plane writes.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// fisCtrlNamespace is the state namespace of the FIS seeds.
const fisCtrlNamespace = "fis-ctrl"

// fisSeedKeyPrefix begins every seed's state key.
const fisSeedKeyPrefix = "result:"

// fisSeededResult is one seeded experiment outcome.
type fisSeededResult struct {
	// ID is the template ID, experiment ID or "*" the seed applies to.
	ID string `json:"id"`
	// Status is failed or completed.
	Status string `json:"status"`
	// Reason is the experiment state's reason; empty takes the default for the status.
	Reason string `json:"reason,omitempty"`
}

// resolveSeededResult returns the seed that applies to an experiment, nil when none does.
func (p *FISPlugin) resolveSeededResult(e *FISExperiment) (*fisSeededResult, error) {
	for _, id := range []string{e.ID, e.TemplateID, "*"} {
		var seed fisSeededResult
		found, err := p.loadSeed(id, &seed)
		if err != nil {
			return nil, err
		}
		if found {
			return &seed, nil
		}
	}
	return nil, nil //nolint:nilnil // (nil, nil) is "no seed applies", which the caller handles.
}

// seededIDs returns the IDs a seed names, so RunDue can observe a seeded experiment that has
// nothing else due.
func (p *FISPlugin) seededIDs() (map[string]bool, error) {
	keys, err := p.state.List(context.Background(), fisCtrlNamespace, fisSeedKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("fis list result seeds: %w", err)
	}
	ids := make(map[string]bool, len(keys))
	for _, k := range keys {
		ids[strings.TrimPrefix(k, fisSeedKeyPrefix)] = true
	}
	return ids, nil
}

// loadSeed reads the seed for id.
func (p *FISPlugin) loadSeed(id string, seed *fisSeededResult) (bool, error) {
	data, err := p.state.Get(context.Background(), fisCtrlNamespace, fisSeedKeyPrefix+id)
	if err != nil {
		return false, fmt.Errorf("fis resolve result seed: %w", err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, seed); err != nil {
		return false, fmt.Errorf("fis decode result seed: %w", err)
	}
	return true, nil
}

// handleFISSeedExperimentResult serves POST /v1/fis/experiment-result.
func (s *Server) handleFISSeedExperimentResult(w http.ResponseWriter, r *http.Request) {
	var seed fisSeededResult
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.ID == "" {
		http.Error(w, `{"error":"id is required (an experiment template ID, an experiment ID or \"*\")"}`, http.StatusBadRequest)
		return
	}
	if seed.Status != fisStatusFailed && seed.Status != fisStatusCompleted {
		http.Error(w, `{"error":"status must be failed or completed"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), fisCtrlNamespace, fisSeedKeyPrefix+seed.ID, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "key": fisSeedKeyPrefix + seed.ID})
}

// handleFISClearExperimentResult serves DELETE /v1/fis/experiment-result.
func (s *Server) handleFISClearExperimentResult(w http.ResponseWriter, r *http.Request) {
	keys := []string{fisSeedKeyPrefix + r.URL.Query().Get("id")}
	if r.URL.Query().Get("id") == "" {
		var err error
		if keys, err = s.state.List(r.Context(), fisCtrlNamespace, fisSeedKeyPrefix); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), fisCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
