package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ec2FleetCtrlNamespace is the state namespace for EC2 Fleet control-plane
// (seed) data.
const ec2FleetCtrlNamespace = "ec2-fleet-ctrl"

// ec2FleetShortfall is a seeded CreateFleet fulfillment outcome. Substrate never
// runs a real capacity broker; a seed lets a test set how much of the requested
// target capacity a fleet actually fulfills and which error the shortfall reports,
// keyed by launch-template name/ID or the "*" wildcard.
//
// Partial fulfillment is the observation callers most often get wrong — an
// instant fleet that asks for 12 and receives 8 still looks like a successful
// call, and TotalTargetCapacity echoes the request, not the result. Seeding it
// makes that path instant and reproducible (#387).
type ec2FleetShortfall struct {
	// LaunchTemplate is the launch-template name or ID the seed applies to, or
	// "*" for any template.
	LaunchTemplate string `json:"launchTemplate"`

	// Fulfill is the number of instances the fleet actually launches. It is
	// clamped to the requested TotalTargetCapacity. Zero means fulfill nothing,
	// so the fleet returns only errors.
	Fulfill int `json:"fulfill"`

	// ErrorCode is the code reported for the unfulfilled capacity, e.g.
	// "InsufficientInstanceCapacity" or "InsufficientFreeAddressesInSubnet".
	// Defaults to "InsufficientInstanceCapacity" when empty.
	ErrorCode string `json:"errorCode"`

	// ErrorMessage is the human-readable message for the shortfall. A default
	// derived from ErrorCode is used when empty.
	ErrorMessage string `json:"errorMessage"`

	// Lifecycle is the lifecycle reported for the unfulfilled capacity
	// ("spot" or "on-demand"). Defaults to the fleet's default target capacity
	// type when empty.
	Lifecycle string `json:"lifecycle"`
}

// ec2FleetCtrlKey returns the state key for a seeded shortfall. Template-scoped
// seeds use "shortfall:{template}"; the wildcard uses "shortfall:*".
func ec2FleetCtrlKey(launchTemplate string) string {
	if launchTemplate == "" {
		launchTemplate = "*"
	}
	return "shortfall:" + launchTemplate
}

// resolveFleetShortfall returns the seeded shortfall for a fleet request, if any,
// matching by exact launch-template ID, then name, then the "*" wildcard. It
// returns (nil, nil) when no seed applies, so callers fall back to fulfilling the
// full requested capacity.
func (p *EC2Plugin) resolveFleetShortfall(ltID, ltName string) (*ec2FleetShortfall, error) {
	goCtx := context.Background()
	candidates := make([]string, 0, 3)
	if ltID != "" {
		candidates = append(candidates, ec2FleetCtrlKey(ltID))
	}
	if ltName != "" {
		candidates = append(candidates, ec2FleetCtrlKey(ltName))
	}
	candidates = append(candidates, ec2FleetCtrlKey("*"))

	for _, key := range candidates {
		data, err := p.state.Get(goCtx, ec2FleetCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("ec2 resolveFleetShortfall get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed ec2FleetShortfall
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("ec2 resolveFleetShortfall unmarshal: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// handleEC2SeedFleetShortfall handles POST /v1/ec2/fleet-shortfall. It seeds how
// much of a CreateFleet request's target capacity is fulfilled and which error
// the shortfall reports, for fleets matching the given launch template (default
// "*"). Substrate does not model real capacity; this sets the observable result.
// Body: {"launchTemplate","fulfill","errorCode","errorMessage","lifecycle"}.
func (s *Server) handleEC2SeedFleetShortfall(w http.ResponseWriter, r *http.Request) {
	var seed ec2FleetShortfall
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Fulfill < 0 {
		http.Error(w, `{"error":"fulfill must be >= 0"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), ec2FleetCtrlNamespace, ec2FleetCtrlKey(seed.LaunchTemplate), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "launchTemplate": ec2FleetCtrlKey(seed.LaunchTemplate)})
}

// handleEC2ClearFleetShortfall handles DELETE /v1/ec2/fleet-shortfall. With
// ?launchTemplate=... it removes that seed; without it removes all.
func (s *Server) handleEC2ClearFleetShortfall(w http.ResponseWriter, r *http.Request) {
	if lt := r.URL.Query().Get("launchTemplate"); lt != "" {
		if err := s.state.Delete(r.Context(), ec2FleetCtrlNamespace, ec2FleetCtrlKey(lt)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), ec2FleetCtrlNamespace, "shortfall:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), ec2FleetCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
