package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleFaultSetRules handles POST /v1/fault/rules.
// It decodes a FaultConfig from the request body and replaces the server's
// current fault injection configuration. Returns 501 when the server was
// started without a FaultController.
//
// Request body:
//
//	{"enabled": true, "rules": [{"service":"s3","operation":"GetObject",
//	  "fault_type":"error","error_code":"NoSuchKey","http_status":404,
//	  "times":2}]}
//
// Posting rules replaces the whole configuration, which resets every rule's
// fired count. See [FaultRule] for the matcher fields and for why "times"
// defaults to one rather than to unlimited.
func (s *Server) handleFaultSetRules(w http.ResponseWriter, r *http.Request) {
	if s.opts.Fault == nil {
		http.Error(w, `{"error":"fault injection not enabled on this server"}`, http.StatusNotImplemented)
		return
	}
	var cfg FaultConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	s.opts.Fault.UpdateConfig(cfg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleFaultClearRules handles DELETE /v1/fault/rules.
// It disables fault injection and removes all active rules. Returns 501 when
// the server was started without a FaultController.
func (s *Server) handleFaultClearRules(w http.ResponseWriter, r *http.Request) {
	if s.opts.Fault == nil {
		http.Error(w, `{"error":"fault injection not enabled on this server"}`, http.StatusNotImplemented)
		return
	}
	s.opts.Fault.UpdateConfig(FaultConfig{Enabled: false})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleFaultGetRules handles GET /v1/fault/rules.
// It returns the current FaultConfig as JSON, each rule carrying the "fired"
// count of requests it has injected a fault into. Returns 501 when the server
// was started without a FaultController.
//
// The count is the assertion that separates "my retry worked" from "the rule
// matched nothing": a rule that never fired produces the same passing test as a
// consumer's retry handling the failure.
func (s *Server) handleFaultGetRules(w http.ResponseWriter, r *http.Request) {
	if s.opts.Fault == nil {
		http.Error(w, `{"error":"fault injection not enabled on this server"}`, http.StatusNotImplemented)
		return
	}
	cfg := s.opts.Fault.GetConfig()
	body, err := json.Marshal(cfg)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body) // nosemgrep
}
