package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// handleRedshiftDataSeedResult handles POST /v1/redshift-data/results.
// It seeds a GetStatementResult response for a given SQL pattern (or wildcard "*").
// Body: {"sql": "SELECT ...", "result": {"ColumnMetadata": [...], "Records": [[...]]}}
// The "sql" field defaults to "*" (wildcard) if omitted or empty.
func (s *Server) handleRedshiftDataSeedResult(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SQL    string              `json:"sql"`
		Result *RedshiftDataResult `json:"result"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Result == nil {
		http.Error(w, `{"error":"result is required"}`, http.StatusBadRequest)
		return
	}
	sql := body.SQL
	if sql == "" {
		sql = "*"
	}
	data, err := json.Marshal(body.Result)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), redshiftDataCtrlNamespace, redshiftDataCtrlResultKey(sql), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "sql": sql})
}

// handleRedshiftDataClearResults handles DELETE /v1/redshift-data/results.
// With ?sql=... it removes the seeded result for that specific SQL pattern.
// Without a query param it removes all seeded results.
func (s *Server) handleRedshiftDataClearResults(w http.ResponseWriter, r *http.Request) {
	sql := r.URL.Query().Get("sql")
	if sql != "" {
		if err := s.state.Delete(r.Context(), redshiftDataCtrlNamespace, redshiftDataCtrlResultKey(sql)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	// Clear all seeded results.
	keys, err := s.state.List(r.Context(), redshiftDataCtrlNamespace, "result:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), redshiftDataCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}

// handleRedshiftDataSetStatus handles POST /v1/redshift-data/status.
// It sets the default Status (and optional ErrorMessage) applied to new ExecuteStatement calls.
// Body: {"status": "FAILED", "errorMessage": "query timed out"}
// Valid statuses: FINISHED, FAILED, ABORTED, STARTED.
func (s *Server) handleRedshiftDataSetStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Status       string `json:"status"`
		ErrorMessage string `json:"errorMessage"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	status := strings.ToUpper(strings.TrimSpace(body.Status))
	switch status {
	case "FINISHED", "FAILED", "ABORTED", "STARTED":
	default:
		http.Error(w, `{"error":"status must be one of FINISHED, FAILED, ABORTED, STARTED"}`, http.StatusBadRequest)
		return
	}
	if err := s.state.Put(r.Context(), redshiftDataCtrlNamespace, redshiftDataCtrlStatusKey, []byte(status)); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), redshiftDataCtrlNamespace, redshiftDataCtrlErrorKey, []byte(body.ErrorMessage)); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"status": status, "errorMessage": body.ErrorMessage})
}
