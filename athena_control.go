package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleAthenaSeedResult handles POST /v1/athena/results.
// It seeds a GetQueryResults response for a given SQL pattern (or wildcard "*").
// Body: {"sql": "SELECT ...", "result": {"Rows": [...], "ColumnInfo": [...]}}
// The "sql" field defaults to "*" (wildcard) if omitted or empty.
func (s *Server) handleAthenaSeedResult(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SQL    string           `json:"sql"`
		Result *AthenaResultSet `json:"result"`
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
	if err := s.state.Put(r.Context(), athenaCtrlNamespace, "result:"+sql, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "sql": sql})
}

// handleAthenaClearResults handles DELETE /v1/athena/results.
// With ?sql=... it removes the seeded result for that specific SQL pattern.
// Without a query param it removes all seeded results.
func (s *Server) handleAthenaClearResults(w http.ResponseWriter, r *http.Request) {
	sql := r.URL.Query().Get("sql")
	if sql != "" {
		if err := s.state.Delete(r.Context(), athenaCtrlNamespace, "result:"+sql); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	// Clear all seeded results.
	keys, err := s.state.List(r.Context(), athenaCtrlNamespace, "result:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), athenaCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
