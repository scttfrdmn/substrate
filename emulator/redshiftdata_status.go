package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// redshiftDataStatusCtrlNamespace holds the statement-status seeds, their per-statement observation
// counters, and the instant each statement was first observed settled. It is separate from
// [redshiftDataCtrlNamespace], where the result seeds live, so clearing one cannot clear the other.
const redshiftDataStatusCtrlNamespace = "redshift-data-status-ctrl"

// redshiftDataStatuses is StatusString's published enumeration on API_DescribeStatement.
var redshiftDataStatuses = []string{"SUBMITTED", "PICKED", "STARTED", "FINISHED", "ABORTED", "FAILED"}

// redshiftDataTransientStatuses are the three statuses a statement passes through before it settles.
var redshiftDataTransientStatuses = []string{"SUBMITTED", "PICKED", "STARTED"}

// redshiftDataStatementSeed is the body of POST /v1/redshift-data/status (#1155, #1163): how many
// observations of a statement report a transient status, and what it settles to. docs/services.md,
// "How a progression is seeded", states the rules every progression shares.
//
// The pre-#1163 body, {"status","errorMessage"}, still parses. With no statementId it is the "*"
// wildcard, and it is now read when a statement is described rather than frozen onto the statement
// when it is executed, so it governs statements that already exist too.
type redshiftDataStatementSeed struct {
	// StatementID is the statement the seed governs; empty or "*" governs every statement.
	StatementID string `json:"statementId"`

	// PendingObservations is how many describes report TransientStatus before the statement settles.
	PendingObservations int `json:"pendingObservations"`

	// TransientStatus is what the countdown reports: SUBMITTED, PICKED or STARTED. Empty reports
	// STARTED.
	TransientStatus string `json:"transientStatus,omitempty"`

	// Status is the status the statement settles to. Empty settles to its recorded FINISHED.
	Status string `json:"status"`

	// ErrorMessage is reported as Error alongside a settled FAILED, the member API_DescribeStatement
	// publishes for it.
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// UnmarshalJSON accepts the statuses in any case, as the pre-#1163 handler did, and stores them
// upper-cased, the only form the page publishes.
func (s *redshiftDataStatementSeed) UnmarshalJSON(data []byte) error {
	type plain redshiftDataStatementSeed
	var v plain
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("redshift-data status seed: %w", err)
	}
	v.Status = strings.ToUpper(strings.TrimSpace(v.Status))
	v.TransientStatus = strings.ToUpper(strings.TrimSpace(v.TransientStatus))
	*s = redshiftDataStatementSeed(v)
	return nil
}

func (s redshiftDataStatementSeed) progressionID() string        { return s.StatementID }
func (s redshiftDataStatementSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression accepts all six published statuses (#1163; the pre-#1163 handler refused
// SUBMITTED and PICKED), a transient status only from the three a statement passes through, and an
// error message only alongside FAILED.
func (s redshiftDataStatementSeed) validateProgression() error {
	if s.Status == "" && s.PendingObservations == 0 {
		return fmt.Errorf("status is required when pendingObservations is 0")
	}
	if err := progressionStates("statement", redshiftDataStatuses, s.Status); err != nil {
		return err
	}
	if err := progressionStates("transient statement", redshiftDataTransientStatuses, s.TransientStatus); err != nil {
		return err
	}
	if s.ErrorMessage != "" && s.Status != "FAILED" {
		return fmt.Errorf("errorMessage applies only to status FAILED, got %q", s.Status)
	}
	return nil
}

// redshiftDataStatementProgressions is the statement-status progression. It holds configuration only.
var redshiftDataStatementProgressions = newProgression[redshiftDataStatementSeed](redshiftDataStatusCtrlNamespace, "statementId", "pendingObservations")

// redshiftDataSettledKey is where the instant a statement was first observed settled is kept.
func redshiftDataSettledKey(id string) string { return "settled:" + id }

// redshiftDataView is what one observation of a statement reports.
type redshiftDataView struct {
	status    string
	errorMsg  string
	updatedAt time.Time
}

// statementView reports what the next observation of stmt sees. observe spends that observation, as
// DescribeStatement does; a false observe peeks, as GetStatementResult's precondition does, so asking
// for a result never moves a statement along.
//
// UpdatedAt is the simulated instant the statement was first observed in its settled status after a
// countdown, recorded once, so a statement that ran for three describes reports an UpdatedAt later
// than its CreatedAt (#1163). An unseeded statement is created settled and never changes, so its
// UpdatedAt stays its CreatedAt.
func (p *RedshiftDataPlugin) statementView(stmt *RedshiftDataStatement, observe bool) (redshiftDataView, error) {
	view := redshiftDataView{status: stmt.Status, errorMsg: stmt.Error, updatedAt: stmt.CreatedAt}
	ctx := context.Background()
	var (
		seed *redshiftDataStatementSeed
		seen int
		err  error
	)
	if observe {
		seed, seen, err = redshiftDataStatementProgressions.observe(ctx, p.state, &p.seedMu, stmt.ID)
	} else {
		seed, seen, err = redshiftDataStatementProgressions.peek(ctx, p.state, stmt.ID)
	}
	if err != nil {
		return view, fmt.Errorf("redshift-data statement %s: %w", stmt.ID, err)
	}
	if seed == nil {
		return view, nil
	}
	status, terminal := countdownState(seen, seed.PendingObservations, seed.TransientStatus, seed.Status, "STARTED", stmt.Status)
	view.status = status
	view.errorMsg = ""
	if terminal && status == "FAILED" {
		view.errorMsg = seed.ErrorMessage
	}
	if !terminal || seed.PendingObservations == 0 {
		return view, nil
	}
	settled, err := p.settledAt(ctx, stmt.ID, observe)
	if err != nil {
		return view, err
	}
	if !settled.IsZero() {
		view.updatedAt = settled
	}
	return view, nil
}

// settledAt returns the instant stmt was first observed settled, recording now as that instant on
// the first settled observation. A peek records nothing.
func (p *RedshiftDataPlugin) settledAt(ctx context.Context, id string, record bool) (time.Time, error) {
	data, err := p.state.Get(ctx, redshiftDataStatusCtrlNamespace, redshiftDataSettledKey(id))
	if err != nil {
		return time.Time{}, fmt.Errorf("redshift-data settled get: %w", err)
	}
	if data != nil {
		var at time.Time
		if err := json.Unmarshal(data, &at); err != nil {
			return time.Time{}, fmt.Errorf("redshift-data settled unmarshal: %w", err)
		}
		return at, nil
	}
	if !record {
		return time.Time{}, nil
	}
	now := p.tc.Now()
	enc, err := json.Marshal(now)
	if err != nil {
		return time.Time{}, fmt.Errorf("redshift-data settled marshal: %w", err)
	}
	if err := p.state.Put(ctx, redshiftDataStatusCtrlNamespace, redshiftDataSettledKey(id), enc); err != nil {
		return time.Time{}, fmt.Errorf("redshift-data settled put: %w", err)
	}
	return now, nil
}
