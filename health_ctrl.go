package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleHealthSeedEvents handles POST /v1/health/events.
// It seeds Health events for DescribeEvents to return.
// Body: {"events": [{"arn":"...", "service":"...", ...}]}.
func (s *Server) handleHealthSeedEvents(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Events []HealthEvent `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Events == nil {
		http.Error(w, `{"error":"events array is required"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(body.Events)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), healthNamespace, "events", data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "count": len(body.Events)})
}

// handleHealthClearEvents handles DELETE /v1/health/events.
// It clears all seeded Health events.
func (s *Server) handleHealthClearEvents(w http.ResponseWriter, r *http.Request) {
	_ = s.state.Delete(r.Context(), healthNamespace, "events")
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
