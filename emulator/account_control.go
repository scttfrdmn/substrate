package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// accountCtrlNamespace is the state namespace for Account Management
// control-plane (seed) data. It is separate from the account namespace so a seed
// is not mistaken for an account's real opt state during replay or a state dump.
const accountCtrlNamespace = "account-ctrl"

// accountCtrlRegionOptStatusKey returns the state key for a seeded Region opt
// status. Region-scoped seeds use "region_opt_status:{region}"; the wildcard uses
// "region_opt_status:*".
func accountCtrlRegionOptStatusKey(region string) string {
	if region == "" {
		region = "*"
	}
	return "region_opt_status:" + region
}

// accountSeededRegionOptStatus is a pinned Region opt status.
//
// Substrate does not enable anything, so the seed sets what an observation
// reports. It is the only way to reach a status a sequence of API calls cannot
// produce: an in-flight ENABLING resolves on first observation, so a test that
// needs a Region *stuck* mid-flight — to exercise a waiter's timeout, or the
// ConflictException an opposite opt gets while one is in progress — has to pin it.
type accountSeededRegionOptStatus struct {
	// RegionName the seed applies to, or "*" for any Region.
	RegionName string `json:"regionName"`

	// Status is the RegionOptStatus every observation reports.
	Status string `json:"status"`
}

// seededRegionOptStatus returns the pinned status for a Region, if any, matching
// the exact Region first and then the "*" wildcard. It reports found=false when
// no seed applies, so the caller falls back to the stored record.
func (p *AccountPlugin) seededRegionOptStatus(ctx context.Context, region string) (status string, found bool, err error) {
	for _, key := range []string{accountCtrlRegionOptStatusKey(region), accountCtrlRegionOptStatusKey("*")} {
		data, getErr := p.state.Get(ctx, accountCtrlNamespace, key)
		if getErr != nil {
			return "", false, fmt.Errorf("account seededRegionOptStatus %s: %w", key, getErr)
		}
		if data == nil {
			continue
		}
		var seed accountSeededRegionOptStatus
		if err := json.Unmarshal(data, &seed); err != nil {
			return "", false, fmt.Errorf("account seededRegionOptStatus %s unmarshal: %w", key, err)
		}
		if seed.Status != "" {
			return seed.Status, true, nil
		}
	}
	return "", false, nil
}

// handleAccountSeedRegionOptStatus handles POST /v1/account/region-opt-status.
// It pins the status every observation of a Region reports, for a given Region
// code (default "*"), which is how a test reaches a Region held in ENABLING or
// DISABLING — a status an unseeded emulator resolves away on first observation.
// Body: {"regionName":"af-south-1","status":"ENABLING"}.
func (s *Server) handleAccountSeedRegionOptStatus(w http.ResponseWriter, r *http.Request) {
	var seed accountSeededRegionOptStatus
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Status == "" {
		http.Error(w, `{"error":"status is required"}`, http.StatusBadRequest)
		return
	}
	// A status outside the enum would be reported by GetRegionOptStatus as a value
	// no SDK enum member matches, so the caller's switch would fall through to its
	// default and the test would pass without exercising anything.
	if !slices.Contains(accountRegionOptStatuses, seed.Status) {
		writeJSONErrorDebug(w, http.StatusBadRequest, "status %q is not a RegionOptStatus", seed.Status)
		return
	}
	// A default Region is always ENABLED_BY_DEFAULT, and a seed naming one would be
	// silently ignored: regionStatus answers for a default Region before it ever
	// consults a seed. Refusing is the difference between a test that fails and one
	// that passes while asserting nothing.
	if seed.RegionName != "" && seed.RegionName != "*" {
		isDefault, isOptIn := accountRegionKind(seed.RegionName)
		if isDefault {
			http.Error(w, fmt.Sprintf(`{"error":"%s is enabled by default; its status cannot be seeded"}`, seed.RegionName), http.StatusBadRequest)
			return
		}
		if !isOptIn {
			writeJSONErrorDebug(w, http.StatusBadRequest, "%q is not an AWS Region code substrate knows",
				seed.RegionName)
			return
		}
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), accountCtrlNamespace, accountCtrlRegionOptStatusKey(seed.RegionName), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{
		"ok":         true,
		"regionName": accountCtrlRegionOptStatusKey(seed.RegionName),
		"status":     seed.Status,
	})
}

// handleAccountClearRegionOptStatus handles DELETE /v1/account/region-opt-status.
// With ?regionName=... it removes that seed; without it removes all of them,
// restoring each Region to its stored opt record.
func (s *Server) handleAccountClearRegionOptStatus(w http.ResponseWriter, r *http.Request) {
	if region := r.URL.Query().Get("regionName"); region != "" {
		if err := s.state.Delete(r.Context(), accountCtrlNamespace, accountCtrlRegionOptStatusKey(region)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), accountCtrlNamespace, "region_opt_status:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), accountCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
