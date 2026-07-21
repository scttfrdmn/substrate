package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// ssmCtrlNamespace is the state namespace for SSM control-plane (seed) data.
const ssmCtrlNamespace = "ssm-ctrl"

// ssmSeededInvocation is a seeded Run Command outcome. Substrate never executes
// the command; a seed lets a test set the observable result that
// GetCommandInvocation returns, keyed by document name (with an optional
// command-parameter substring match) or the "*" wildcard.
type ssmSeededInvocation struct {
	// DocumentName the seed applies to, or "*" for any document.
	DocumentName string `json:"documentName"`

	// ParamMatch, if set, requires the SendCommand parameters to contain this
	// substring (matched against the flattened Parameters values) — lets a test
	// distinguish, e.g., "systemctl status spored" from other RunShellScript calls.
	ParamMatch string `json:"paramMatch"`

	// Status is the resolved invocation status ("Success", "Failed", ...).
	Status string `json:"status"`

	// Stdout / Stderr are the resolved StandardOutputContent / StandardErrorContent.
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`

	// ExitCode is the resolved ResponseCode.
	ExitCode int `json:"exitCode"`
}

// ssmCtrlKey returns the state key for a seeded invocation. Document-scoped seeds
// use "invocation:{doc}"; the wildcard uses "invocation:*".
func ssmCtrlKey(documentName string) string {
	if documentName == "" {
		documentName = "*"
	}
	return "invocation:" + documentName
}

// resolveSeededInvocation returns the seeded outcome for a command, if any,
// matching by exact document name first then the "*" wildcard, and honoring an
// optional ParamMatch substring against the flattened parameter values. It
// returns (nil, nil) when no seed applies, so callers fall back to the nominal
// Success/empty result.
func (p *SSMPlugin) resolveSeededInvocation(documentName string, params map[string][]string) (*ssmSeededInvocation, error) {
	goCtx := context.Background()
	flat := flattenSSMParams(params)
	for _, key := range []string{ssmCtrlKey(documentName), ssmCtrlKey("*")} {
		data, err := p.state.Get(goCtx, ssmCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("ssm resolveSeededInvocation get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed ssmSeededInvocation
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("ssm resolveSeededInvocation unmarshal: %w", err)
		}
		if seed.ParamMatch != "" && !strings.Contains(flat, seed.ParamMatch) {
			continue
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// flattenSSMParams joins all SendCommand parameter values into a single string
// for substring matching.
func flattenSSMParams(params map[string][]string) string {
	var b strings.Builder
	for _, vals := range params {
		for _, v := range vals {
			b.WriteString(v)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// handleSSMSeedCommandInvocation handles POST /v1/ssm/command-invocation. It
// seeds the outcome GetCommandInvocation returns for commands matching the given
// document name (default "*") and optional parameter substring. Substrate does
// not execute the command; this sets the observable result.
// Body: {"documentName","paramMatch","status","stdout","stderr","exitCode"}.
func (s *Server) handleSSMSeedCommandInvocation(w http.ResponseWriter, r *http.Request) {
	var seed ssmSeededInvocation
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Status == "" {
		http.Error(w, `{"error":"status is required"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), ssmCtrlNamespace, ssmCtrlKey(seed.DocumentName), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "documentName": ssmCtrlKey(seed.DocumentName)})
}

// handleSSMClearCommandInvocation handles DELETE /v1/ssm/command-invocation.
// With ?documentName=... it removes that seed; without it removes all.
func (s *Server) handleSSMClearCommandInvocation(w http.ResponseWriter, r *http.Request) {
	if doc := r.URL.Query().Get("documentName"); doc != "" {
		if err := s.state.Delete(r.Context(), ssmCtrlNamespace, ssmCtrlKey(doc)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), ssmCtrlNamespace, "invocation:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), ssmCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
