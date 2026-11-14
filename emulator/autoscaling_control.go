package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// The launch-failure seed: how a test makes a group's launches fail (#1460).
//
// A real group's launch fails for reasons substrate does not model — no Spot capacity in a
// zone, an exhausted subnet, a quota — and reports it in one place: a Failed scaling activity
// whose StatusMessage says why, with the capacity left unmet. That is the observation a
// consumer's "did my pool actually scale?" check is written against, and seeding it is how a
// test reaches it instantly and reproducibly.
//
//	POST   /v1/autoscaling/launch-failure  {"autoScalingGroupName":"pool-a","statusMessage":"…"}
//	DELETE /v1/autoscaling/launch-failure[?autoScalingGroupName=pool-a]
//
// The seed is keyed by group name or the "*" wildcard (the default). While it is present,
// every launch attempt the group makes — at create, on a capacity change, on an observation —
// records one Failed activity carrying statusMessage and launches nothing; clearing it lets
// the next attempt succeed. The seed is written to the StateManager inside the control-plane
// group in server.go, so a replay re-issues it in position (#1140).

// autoscalingCtrlNamespace is the state namespace for Auto Scaling seeds.
const autoscalingCtrlNamespace = "autoscaling-ctrl"

// asgDefaultLaunchFailureMessage is the StatusMessage a seed naming none reports: EC2's
// InsufficientInstanceCapacity message, which is the commonest launch failure a group sees,
// with the activity suffix the published sample carries.
const asgDefaultLaunchFailureMessage = "We currently do not have sufficient capacity in the Availability Zone you requested. Launching EC2 instance failed."

// asgLaunchFailure is one seeded launch failure.
type asgLaunchFailure struct {
	// AutoScalingGroupName is the group the seed applies to, or "*" for every group.
	AutoScalingGroupName string `json:"autoScalingGroupName"`

	// StatusMessage is the Failed activity's StatusMessage; a default is used when empty.
	StatusMessage string `json:"statusMessage"`
}

// message returns the StatusMessage the seed reports.
func (s *asgLaunchFailure) message() string {
	if strings.TrimSpace(s.StatusMessage) == "" {
		return asgDefaultLaunchFailureMessage
	}
	return s.StatusMessage
}

// asgLaunchFailureKey is the state key of a seed.
func asgLaunchFailureKey(group string) string {
	if group == "" {
		group = "*"
	}
	return "launch-failure:" + group
}

// launchFailureSeed returns the seed that applies to a group — its own, then the wildcard —
// or nil.
func (p *AutoScalingPlugin) launchFailureSeed(group string) (*asgLaunchFailure, error) {
	for _, key := range []string{asgLaunchFailureKey(group), asgLaunchFailureKey("*")} {
		data, err := p.state.Get(context.Background(), autoscalingCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("autoscaling get launch-failure seed: %w", err)
		}
		if data == nil {
			continue
		}
		var seed asgLaunchFailure
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("autoscaling unmarshal launch-failure seed: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = no seed applies; launches proceed.
}

// handleAutoScalingSeedLaunchFailure handles POST /v1/autoscaling/launch-failure.
func (s *Server) handleAutoScalingSeedLaunchFailure(w http.ResponseWriter, r *http.Request) {
	var seed asgLaunchFailure
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.AutoScalingGroupName == "" {
		seed.AutoScalingGroupName = "*"
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := asgLaunchFailureKey(seed.AutoScalingGroupName)
	if err := s.state.Put(r.Context(), autoscalingCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "autoScalingGroupName": seed.AutoScalingGroupName})
}

// handleAutoScalingClearLaunchFailure handles DELETE /v1/autoscaling/launch-failure. With
// ?autoScalingGroupName=... it removes that seed; without it, every seed.
func (s *Server) handleAutoScalingClearLaunchFailure(w http.ResponseWriter, r *http.Request) {
	if name := r.URL.Query().Get("autoScalingGroupName"); name != "" {
		if err := s.state.Delete(r.Context(), autoscalingCtrlNamespace, asgLaunchFailureKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), autoscalingCtrlNamespace, "launch-failure:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), autoscalingCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
