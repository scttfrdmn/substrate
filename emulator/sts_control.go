package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// handleSTSSeedFederationOutcome handles POST /v1/sts/federation-outcome.
//
// It makes AssumeRoleWithWebIdentity and AssumeRoleWithSAML answer an identity-provider outcome
// substrate cannot produce on its own, because it verifies no signature and contacts no provider:
// InvalidIdentityToken (a signature or thumbprint that does not verify), IDPRejectedClaim, or —
// for web identity only, the one operation that documents it — IDPCommunicationError.
//
// Body: {"provider": "<provider ARN>", "error": "IDPRejectedClaim", "message": "...", "count": 2}.
// "provider" defaults to "*". "count" is how many assumes the seed answers before it clears
// itself, so a retry loop can be shown failing and then succeeding; 0 or absent holds until
// cleared. The claim checks (issuer, audience, expiry) run before the seed for the issuer and after
// it for the rest, so a seed applies only to a token naming a registered provider.
func (s *Server) handleSTSSeedFederationOutcome(w http.ResponseWriter, r *http.Request) {
	var seed STSFederationSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if _, ok := stsFederationOutcomes[seed.Error]; !ok {
		names := make([]string, 0, len(stsFederationOutcomes))
		for name := range stsFederationOutcomes {
			names = append(names, name)
		}
		sort.Strings(names)
		http.Error(w, fmt.Sprintf(`{"error":"error must be one of %s"}`, strings.Join(names, ", ")),
			http.StatusBadRequest)
		return
	}
	if seed.Count < 0 {
		http.Error(w, `{"error":"count must not be negative"}`, http.StatusBadRequest)
		return
	}
	if seed.Provider == "" {
		seed.Provider = "*"
	}
	raw, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), stsCtrlNamespace, stsCtrlFederationOutcomeKey(seed.Provider), raw); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "provider": seed.Provider, "error": seed.Error})
}

// handleSTSClearFederationOutcome handles DELETE /v1/sts/federation-outcome. With ?provider=... it
// removes that seed; without, every seeded federation outcome.
func (s *Server) handleSTSClearFederationOutcome(w http.ResponseWriter, r *http.Request) {
	if err := clearCtrlSeeds(r.Context(), s.state, stsCtrlNamespace, "federation_outcome:",
		r.URL.Query().Get("provider")); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
