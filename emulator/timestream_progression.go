package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// A Timestream table's seeded status progression (#1196).
//
// API_Table publishes TableStatus as `ACTIVE | DELETING | RESTORING`. #1196's table listed CREATING
// among the unreachable values; the page does not publish it, so a table is created ACTIVE, as
// substrate always answered, and that is unchanged. What was unreachable is DELETING — DeleteTable
// erased the table at once — and RESTORING.
//
// The table kind is the shared progression in emulator/progression.go, keyed by "database/table" or
// "*", spent by DescribeTable and by each table ListTables reports:
//
//   - A seed may hold a live table at RESTORING for pendingObservations observations before it
//     reports ACTIVE again. Unseeded, or seeded with no state, a live table reads ACTIVE.
//   - Under a seed with observations to spend, DeleteTable keeps the table, marked DELETING, and
//     restarts its countdown: the next pendingObservations observations report DELETING, and the one
//     after finishes the delete, answering ResourceNotFoundException. Unseeded, DeleteTable removes
//     the table at once, as before. A delete of a table already DELETING answers the same empty body
//     and does not restart the window.
//
// The table publishes no failure state, so none is seedable.

// timestreamTableStatuses is API_Table's TableStatus Valid Values.
var timestreamTableStatuses = []string{"ACTIVE", "DELETING", "RESTORING"}

// timestreamTableDeleting is the TableStatus a table reports through a seeded deleting window.
const timestreamTableDeleting = "DELETING"

// timestreamTableStatusSeed is the body of POST /v1/timestream-write/table-status.
type timestreamTableStatusSeed struct {
	// DatabaseName and TableName name the table the seed targets; both empty means every table.
	DatabaseName string `json:"databaseName"`
	TableName    string `json:"tableName"`
	// PendingObservations is how many observations report State — or DELETING, after a delete —
	// before the table reports ACTIVE, or before a deleted table is gone.
	PendingObservations int `json:"pendingObservations"`
	// State is the transient status of a live table: RESTORING, or empty for none.
	State string `json:"state"`
}

// timestreamTableProgressions is the table kind's seeded countdown.
var timestreamTableProgressions = newProgression[timestreamTableStatusSeed]("timestream-table-ctrl", "table", "pendingObservations")

// timestreamTableProgressionID is a table's progression ID, "database/table".
func timestreamTableProgressionID(db, table string) string { return db + "/" + table }

// progressionID implements [progressionSeed].
func (seed timestreamTableStatusSeed) progressionID() string {
	if seed.DatabaseName == "" && seed.TableName == "" {
		return "*"
	}
	return timestreamTableProgressionID(seed.DatabaseName, seed.TableName)
}

// progressionObservations implements [progressionSeed].
func (seed timestreamTableStatusSeed) progressionObservations() int {
	return seed.PendingObservations
}

// validateProgression implements [progressionSeed]: a seed names both a database and a table or
// neither, and a live table's transient status may only be RESTORING.
func (seed timestreamTableStatusSeed) validateProgression() error {
	if (seed.DatabaseName == "") != (seed.TableName == "") {
		return errors.New("databaseName and tableName name one table together; give both, or neither for every table")
	}
	if err := progressionStates("table", timestreamTableStatuses, seed.State); err != nil {
		return err
	}
	if seed.State != "" && seed.State != "RESTORING" {
		return fmt.Errorf("state %q is not a transient status of a live table, want RESTORING", seed.State)
	}
	return nil
}

// observeTable reports one observation of tbl, spending it. gone is true when the observation ended a
// deleting window, which completes the delete.
func (p *TimestreamPlugin) observeTable(acct, region string, tbl TimestreamTable) (observed TimestreamTable, gone bool, err error) {
	goCtx := context.Background()
	id := timestreamTableProgressionID(tbl.DatabaseName, tbl.TableName)
	seed, seen, err := timestreamTableProgressions.observe(goCtx, p.state, &p.seedMu, id)
	if err != nil {
		return tbl, false, fmt.Errorf("timestream observeTable: %w", err)
	}
	if tbl.TableStatus == timestreamTableDeleting {
		if seed != nil && seen < seed.PendingObservations {
			return tbl, false, nil
		}
		if err := p.finishTableDelete(acct, region, tbl.DatabaseName, tbl.TableName); err != nil {
			return tbl, false, err
		}
		return tbl, true, nil
	}
	if seed == nil || seed.State == "" {
		return tbl, false, nil
	}
	tbl.TableStatus, _ = countdownState(seen, seed.PendingObservations, seed.State, "", seed.State, tbl.TableStatus)
	return tbl, false, nil
}

// beginTableDelete marks tbl DELETING and restarts its countdown when a seed with observations to
// spend governs it, reporting true; otherwise it reports false and the caller removes it at once.
func (p *TimestreamPlugin) beginTableDelete(acct, region string, tbl TimestreamTable) (bool, error) {
	goCtx := context.Background()
	id := timestreamTableProgressionID(tbl.DatabaseName, tbl.TableName)
	seed, err := timestreamTableProgressions.resolve(goCtx, p.state, id)
	if err != nil {
		return false, fmt.Errorf("timestream beginTableDelete: %w", err)
	}
	if seed == nil || seed.PendingObservations == 0 {
		return false, nil
	}
	tbl.TableStatus = timestreamTableDeleting
	if err := p.putTable(acct, region, tbl); err != nil {
		return false, err
	}
	if err := timestreamTableProgressions.reset(goCtx, p.state, id); err != nil {
		return false, fmt.Errorf("timestream beginTableDelete: %w", err)
	}
	return true, nil
}

// putTable stores tbl's record.
func (p *TimestreamPlugin) putTable(acct, region string, tbl TimestreamTable) error {
	data, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("marshal timestream table: %w", err)
	}
	if err := p.state.Put(context.Background(), timestreamNamespace, timestreamTableKey(acct, region, tbl.DatabaseName, tbl.TableName), data); err != nil {
		return fmt.Errorf("put timestream table: %w", err)
	}
	return nil
}

// finishTableDelete removes a table's record and its index entry.
func (p *TimestreamPlugin) finishTableDelete(acct, region, db, table string) error {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, timestreamNamespace, timestreamTableKey(acct, region, db, table)); err != nil {
		return fmt.Errorf("delete timestream table: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, db), table)
	return nil
}

// handleTimestreamSeedTableStatus handles POST /v1/timestream-write/table-status. Body:
// {"databaseName","tableName","pendingObservations","state"}.
func (s *Server) handleTimestreamSeedTableStatus(w http.ResponseWriter, r *http.Request) {
	timestreamTableProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleTimestreamClearTableStatus handles DELETE /v1/timestream-write/table-status; with
// ?table=database/table it removes that seed, without it every one.
func (s *Server) handleTimestreamClearTableStatus(w http.ResponseWriter, r *http.Request) {
	timestreamTableProgressions.serveClear(w, r, s.state, s.logger)
}
