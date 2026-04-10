package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleBedrockRuntimeSeedResponse handles POST /v1/bedrock-runtime/responses.
// It seeds an InvokeModel response for a given model ID (or wildcard "*").
// Body: {"modelId": "anthropic.claude-...", "body": {...}}
// The "modelId" field defaults to "*" (wildcard) if omitted or empty.
func (s *Server) handleBedrockRuntimeSeedResponse(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ModelID      string          `json:"modelId"`
		ResponseBody json.RawMessage `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.ResponseBody == nil {
		http.Error(w, `{"error":"body is required"}`, http.StatusBadRequest)
		return
	}
	modelID := body.ModelID
	if modelID == "" {
		modelID = "*"
	}
	if err := s.state.Put(r.Context(), bedrockRuntimeCtrlNamespace, bedrockRuntimeCtrlResponseKey(modelID), body.ResponseBody); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "modelId": modelID})
}

// handleBedrockRuntimeClearResponses handles DELETE /v1/bedrock-runtime/responses.
// With ?modelId=... it removes the seeded response for that specific model.
// Without a query param it removes all seeded responses.
func (s *Server) handleBedrockRuntimeClearResponses(w http.ResponseWriter, r *http.Request) {
	modelID := r.URL.Query().Get("modelId")
	if modelID != "" {
		if err := s.state.Delete(r.Context(), bedrockRuntimeCtrlNamespace, bedrockRuntimeCtrlResponseKey(modelID)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	// Clear all seeded responses.
	keys, err := s.state.List(r.Context(), bedrockRuntimeCtrlNamespace, "response:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), bedrockRuntimeCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
