package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// A query execution's seeded progression (#1155).
//
// StartQueryExecution completes the query in the request that submits it, so an unseeded query is
// SUCCEEDED on its first GetQueryExecution and a consumer's wait-for-terminal loop exits at once.
// A seed written to POST /v1/athena/query-status makes the published transient states observable:
// GetQueryExecution reports the seed's transient state — QUEUED or RUNNING — for a seeded number of
// observations, then its final one, with the StateChangeReason and AthenaError a failed query
// reports. See [progression] for the rules every progressing resource shares, and docs/services.md
// "How a progression is seeded".
//
// The seed governs what an observation reports; it never rewrites the query record. The one record
// state that outranks it is CANCELLED, which only StopQueryExecution writes: a stop is the caller's
// own action, and the query must read as stopped whatever a seed would otherwise report.

// athenaQueryCtrlNamespace is the [StateManager] namespace the query progression's seeds and
// counters live in.
const athenaQueryCtrlNamespace = "athena-query-ctrl"

// athenaQueryTransientStates are the two states a query reports before it completes. With
// [athenaQueryFinalStates] they are QueryExecutionStatus.State's Valid Values, as
// API_QueryExecutionStatus publishes them: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED.
var athenaQueryTransientStates = []string{"QUEUED", "RUNNING"}

// athenaQueryFinalStates are the three states a query settles in.
var athenaQueryFinalStates = []string{"SUCCEEDED", "FAILED", "CANCELLED"}

// athenaQueryCancelled is the state StopQueryExecution writes, spelled as the enumeration spells it
// (#1154).
const athenaQueryCancelled = "CANCELLED"

// athenaQueryStatusSeed is the body of POST /v1/athena/query-status.
type athenaQueryStatusSeed struct {
	// QueryExecutionID is the query the seed targets; empty or "*" targets every query.
	QueryExecutionID string `json:"queryExecutionId"`
	// PendingObservations is how many GetQueryExecution calls report State before FinalState.
	PendingObservations int `json:"pendingObservations"`
	// State is the transient state reported while the countdown runs; QUEUED or RUNNING, default
	// RUNNING.
	State string `json:"state"`
	// FinalState is the state reported once the countdown is exhausted; SUCCEEDED, FAILED or
	// CANCELLED, default the record's own.
	FinalState string `json:"finalState"`
	// StateChangeReason is QueryExecutionStatus.StateChangeReason on the final observation.
	StateChangeReason string `json:"stateChangeReason"`
	// AthenaError is QueryExecutionStatus.AthenaError on the final observation.
	AthenaError *athenaErrorSeed `json:"athenaError,omitempty"`
}

// athenaErrorSeed is the AthenaError a seeded final observation reports, in API_AthenaError's
// shape. Every member is Required: No; a nil field is omitted.
type athenaErrorSeed struct {
	// ErrorCategory is 1 (System), 2 (User) or 3 (Other).
	ErrorCategory *int `json:"ErrorCategory,omitempty"`
	// ErrorType is the error-type code, 0 to 9999.
	ErrorType *int `json:"ErrorType,omitempty"`
	// Retryable is true when the query might succeed if resubmitted.
	Retryable *bool `json:"Retryable,omitempty"`
	// ErrorMessage is a short description of the error.
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

func (s athenaQueryStatusSeed) progressionID() string        { return s.QueryExecutionID }
func (s athenaQueryStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a transient or final state outside its half of the enumeration, and
// an AthenaError outside API_AthenaError's published ranges.
func (s athenaQueryStatusSeed) validateProgression() error {
	if err := progressionStates("query transient", athenaQueryTransientStates, s.State); err != nil {
		return err
	}
	if err := progressionStates("query final", athenaQueryFinalStates, s.FinalState); err != nil {
		return err
	}
	if e := s.AthenaError; e != nil {
		if e.ErrorCategory != nil && (*e.ErrorCategory < 1 || *e.ErrorCategory > 3) {
			return fmt.Errorf("athenaError.ErrorCategory %d is outside 1-3", *e.ErrorCategory)
		}
		if e.ErrorType != nil && (*e.ErrorType < 0 || *e.ErrorType > 9999) {
			return fmt.Errorf("athenaError.ErrorType %d is outside 0-9999", *e.ErrorType)
		}
	}
	return nil
}

// athenaQueryProgressions is the query kind's seeded countdown, in [athenaQueryCtrlNamespace].
var athenaQueryProgressions = newProgression[athenaQueryStatusSeed](athenaQueryCtrlNamespace, "queryExecutionId", "pendingObservations")

// athenaQueryObserved is what one read of a query reports about its status.
type athenaQueryObserved struct {
	// State is QueryExecutionStatus.State.
	State string
	// Terminal is false while a seeded countdown runs, when CompletionDateTime is not yet known.
	Terminal bool
	// StateChangeReason and AthenaError are reported on a seeded final observation only.
	StateChangeReason string
	AthenaError       *athenaErrorSeed
}

// observedQueryStatus returns what a read of q reports. spend selects observe (GetQueryExecution,
// which counts) or peek (GetQueryResults, which checks the state but is not a poll of it).
func (p *AthenaPlugin) observedQueryStatus(ctx context.Context, q AthenaQuery, spend bool) (athenaQueryObserved, error) {
	record := athenaQueryObserved{State: q.State, Terminal: true}
	if q.State == athenaQueryCancelled {
		return record, nil
	}
	var (
		seed *athenaQueryStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = athenaQueryProgressions.observe(ctx, p.state, &p.seedMu, q.QueryExecutionID)
	} else {
		seed, seen, err = athenaQueryProgressions.peek(ctx, p.state, q.QueryExecutionID)
	}
	if err != nil {
		return athenaQueryObserved{}, fmt.Errorf("athena query status: %w", err)
	}
	if seed == nil {
		return record, nil
	}
	state, terminal := countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "RUNNING", q.State)
	out := athenaQueryObserved{State: state, Terminal: terminal}
	if terminal {
		out.StateChangeReason = seed.StateChangeReason
		out.AthenaError = seed.AthenaError
	}
	return out, nil
}

// athenaQueryNotSucceeded is GetQueryResults' refusal of a query whose observed state is not
// SUCCEEDED. API_GetQueryResults publishes InvalidRequestException/400 for "something wrong with the
// input to the request" and no sentence for this case; the two messages are substrate's reading.
func athenaQueryNotSucceeded(observed athenaQueryObserved) *AWSError {
	msg := "Query has not yet finished. Current state: " + observed.State
	if observed.Terminal {
		msg = "Query did not finish successfully. Final query state: " + observed.State
	}
	return &AWSError{Code: "InvalidRequestException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// handleAthenaSeedQueryStatus handles POST /v1/athena/query-status. It is mounted inside server.go's
// control-plane group, so the seed is recorded and a replay re-issues it (#1140).
func (s *Server) handleAthenaSeedQueryStatus(w http.ResponseWriter, r *http.Request) {
	athenaQueryProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleAthenaClearQueryStatus handles DELETE /v1/athena/query-status: ?queryExecutionId=… removes
// one seed and its countdown, and no query removes every one.
func (s *Server) handleAthenaClearQueryStatus(w http.ResponseWriter, r *http.Request) {
	athenaQueryProgressions.serveClear(w, r, s.state, s.logger)
}
