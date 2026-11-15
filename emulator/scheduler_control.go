package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// The seedable invocation outcome (#1501).
//
// A target invocation defaults to its nominal path: the target is called and its own answer
// decides success. A test that needs the failure path — the retries, the dead-letter event, a
// schedule that is not deleted until its retries end — seeds it:
//
//	POST /v1/scheduler/invocation-outcome
//	{"groupName": "aegc-dev", "scheduleName": "lease-state", "errorCode": "ThrottlingException",
//	 "errorMessage": "Rate exceeded", "retryable": true, "failAttempts": 2}
//
// scheduleName is a schedule's name or "*" for every schedule; groupName is optional and, absent,
// matches any group, so {"groupName": "g", "scheduleName": "*"} affects only group g's schedules.
// The most specific seed applies: the schedule's own, its name in any group, its group's wildcard,
// the global wildcard. Each matching attempt fails with errorCode and errorMessage without calling
// the target. failAttempts bounds how many attempts of each invocation fail — the next one calls
// the target — and zero or absent fails every attempt. retryable overrides the classification
// substrate gives errorCode (a throttle or a 5xx code is retryable, anything else permanent).
// DELETE /v1/scheduler/invocation-outcome clears one seed by the same two query keys, a group's
// seeds by groupName alone, or every seed with neither. The seeds live in the state manager, so a reset clears them and a
// replay re-applies them from the recorded control-plane writes.

// schedulerCtrlNamespace is the state namespace of the Scheduler seeds.
const schedulerCtrlNamespace = "scheduler-ctrl"

// schedulerSeededOutcome is one seeded invocation outcome.
type schedulerSeededOutcome struct {
	// GroupName is the schedule group the seed applies to; empty for any group.
	GroupName string `json:"groupName,omitempty"`
	// ScheduleName is the schedule the seed applies to, or "*".
	ScheduleName string `json:"scheduleName"`
	// ErrorCode is the error each failed attempt reports.
	ErrorCode string `json:"errorCode"`
	// ErrorMessage is the message each failed attempt reports.
	ErrorMessage string `json:"errorMessage,omitempty"`
	// Retryable overrides whether the error is retried; nil classifies ErrorCode.
	Retryable *bool `json:"retryable,omitempty"`
	// FailAttempts is how many attempts of each invocation fail; zero fails every attempt.
	FailAttempts int `json:"failAttempts,omitempty"`
}

// failure renders the seed as the failure an attempt reports.
func (s *schedulerSeededOutcome) failure() *schedFailure {
	return &schedFailure{Code: s.ErrorCode, Message: s.ErrorMessage, Status: http.StatusBadRequest, Retryable: s.retryableFor()}
}

// retryableFor classifies the seeded code when the seed does not say, as a target's own answer
// would be classified.
func (s *schedulerSeededOutcome) retryableFor() *bool {
	if s.Retryable != nil {
		return s.Retryable
	}
	r := (&schedFailure{Code: s.ErrorCode}).retryable() || s.ErrorCode == "InternalServerException" ||
		s.ErrorCode == "AWS.Scheduler.InternalServerError"
	return &r
}

// schedulerCtrlKeyPrefix begins every seed's state key.
const schedulerCtrlKeyPrefix = "outcome:"

// schedulerCtrlKey is the state key of a seed: outcome:{group}/{name}, with "*" for an absent
// group (any group) and for a wildcard name, so a group-scoped wildcard (`outcome:g/*`) is a key
// of its own and affects only that group.
func schedulerCtrlKey(groupName, scheduleName string) string {
	if groupName == "" {
		groupName = "*"
	}
	if scheduleName == "" {
		scheduleName = "*"
	}
	return schedulerCtrlKeyPrefix + groupName + "/" + scheduleName
}

// resolveSeededOutcome returns the seed that applies to a schedule, nil when none does, most
// specific first: the schedule's own, its name in any group, its group's wildcard, the wildcard.
func (p *SchedulerPlugin) resolveSeededOutcome(ctx context.Context, groupName, name string) (*schedulerSeededOutcome, error) {
	for _, key := range []string{
		schedulerCtrlKey(groupName, name), schedulerCtrlKey("", name),
		schedulerCtrlKey(groupName, "*"), schedulerCtrlKey("", "*"),
	} {
		data, err := p.state.Get(ctx, schedulerCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("scheduler resolve outcome seed: %w", err)
		}
		if data == nil {
			continue
		}
		var seed schedulerSeededOutcome
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("scheduler decode outcome seed: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by the caller.
}

// handleSchedulerSeedInvocationOutcome serves POST /v1/scheduler/invocation-outcome.
func (s *Server) handleSchedulerSeedInvocationOutcome(w http.ResponseWriter, r *http.Request) {
	var seed schedulerSeededOutcome
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.ScheduleName == "" {
		http.Error(w, `{"error":"scheduleName is required (a name or \"*\")"}`, http.StatusBadRequest)
		return
	}
	if seed.ErrorCode == "" {
		http.Error(w, `{"error":"errorCode is required"}`, http.StatusBadRequest)
		return
	}
	if seed.FailAttempts < 0 {
		http.Error(w, `{"error":"failAttempts must not be negative"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := schedulerCtrlKey(seed.GroupName, seed.ScheduleName)
	if err := s.state.Put(r.Context(), schedulerCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "key": key})
}

// handleSchedulerClearInvocationOutcome serves DELETE /v1/scheduler/invocation-outcome.
func (s *Server) handleSchedulerClearInvocationOutcome(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	group := q.Get("groupName")
	if name := q.Get("scheduleName"); name != "" {
		if err := s.state.Delete(r.Context(), schedulerCtrlNamespace, schedulerCtrlKey(group, name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	// A groupName alone clears that group's seeds, its wildcard included; neither clears them all.
	prefix := schedulerCtrlKeyPrefix
	if group != "" {
		prefix += group + "/"
	}
	keys, err := s.state.List(r.Context(), schedulerCtrlNamespace, prefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), schedulerCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
