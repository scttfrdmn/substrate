package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleTimestreamSeedResult handles POST /v1/timestream-query/results.
// It seeds a Query response for a given query string (or wildcard "*").
// Body: {"queryString": "SELECT ...", "result": {"Rows": [...], "ColumnInfo": [...]}}
// The "queryString" field defaults to "*" (wildcard) if omitted or empty.
func (s *Server) handleTimestreamSeedResult(w http.ResponseWriter, r *http.Request) {
	var body struct {
		QueryString string                 `json:"queryString"`
		Result      *TimestreamQueryResult `json:"result"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Result == nil {
		http.Error(w, `{"error":"result is required"}`, http.StatusBadRequest)
		return
	}
	qs := body.QueryString
	if qs == "" {
		qs = "*"
	}
	data, err := json.Marshal(body.Result)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), timestreamCtrlNamespace, timestreamCtrlResultKey(qs), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "queryString": qs})
}

// handleTimestreamClearResults handles DELETE /v1/timestream-query/results.
// With ?queryString=... it removes the seeded result for that specific query.
// Without a query param it removes all seeded results.
func (s *Server) handleTimestreamClearResults(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query().Get("queryString")
	if qs != "" {
		if err := s.state.Delete(r.Context(), timestreamCtrlNamespace, timestreamCtrlResultKey(qs)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	// Clear all seeded results.
	keys, err := s.state.List(r.Context(), timestreamCtrlNamespace, "result:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), timestreamCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
