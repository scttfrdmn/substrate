package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// Spot placement score seeding (#892).
//
// GetSpotPlacementScores answers a recommendation AWS computes from live Spot capacity, and
// substrate models no capacity broker — so the interesting observations (a Region that scores
// badly, one zone lower than its siblings, one instance type scarcer than another) cannot be
// derived from anything substrate knows. They are seeded instead, per the rule that a
// deterministic emulator produces different results by being seeded rather than by behaving
// nondeterministically.
//
// The score is what a consumer's own logic branches on — "sample the free score, and only pay for
// a fulfillment probe where it looks promising" — so the branch a consumer most needs to test is
// the low-score one, and without a seed that branch is unreachable.

// ec2SPSCtrlNamespace is the state namespace for spot-placement-score control-plane (seed) data.
//
// It is a namespace of its own rather than a key prefix inside [ec2Namespace], following
// [ec2FleetCtrlNamespace] and ec2SnapCtrlNamespace: seed data is not a resource, and mixing it
// into the resource namespace would put it in front of every prefix scan that walks EC2 state.
const ec2SPSCtrlNamespace = "ec2-sps-ctrl"

// ec2SpotPlacementScoreSeed is a seeded placement score.
//
// A seed is scoped to one Availability Zone, one Region, or every Region, and to one instance type
// or every instance type — so a test can say "us-east-1 scores 2", "use1-az3 scores 2" or
// "inf2.48xlarge scores 1 everywhere" without enumerating the rest.
type ec2SpotPlacementScoreSeed struct {
	// Region is the Region code the seed applies to, e.g. "us-east-1". Empty, together with an
	// empty AvailabilityZoneID, means every Region.
	Region string `json:"region,omitempty"`

	// AvailabilityZoneID is the AZ **ID** the seed applies to, e.g. "use1-az1" — the identifier
	// GetSpotPlacementScores itself reports, not the AZ name "us-east-1a". A zone-scoped seed is
	// observable only when a request passes SingleAvailabilityZone=true, because a Region-scored
	// answer names no zone.
	AvailabilityZoneID string `json:"availabilityZoneId,omitempty"`

	// InstanceType is the instance type the seed applies to, e.g. "inf2.48xlarge". Empty means
	// every instance type, including a request that names none.
	InstanceType string `json:"instanceType,omitempty"`

	// Score is the score reported for the scope, 1 to 10 — the scale AWS publishes for the
	// operation. It is required, so a seed cannot silently mean "score zero".
	Score int `json:"score"`
}

// ec2SPSCtrlKey returns the state key for a seeded score. The scope is an AZ ID, a Region code, or
// "*" for every Region; the instance type is a type name or "*" for every type.
func ec2SPSCtrlKey(scope, instanceType string) string {
	if scope == "" {
		scope = "*"
	}
	if instanceType == "" {
		instanceType = "*"
	}
	return "score:" + scope + "/" + instanceType
}

// ec2SPSSeedScope reports the scope half of a seed's key: the AZ ID when the seed names one,
// otherwise the Region, otherwise the wildcard.
//
// The two are not combined into one composite scope, because a zone-scoped seed already fixes its
// Region — an AZ ID is unique across Regions, which is exactly why AWS reports the ID here rather
// than the zone name. The seeding endpoint refuses a seed naming both, so this never has to pick
// between disagreeing halves.
func ec2SPSSeedScope(seed *ec2SpotPlacementScoreSeed) string {
	if seed.AvailabilityZoneID != "" {
		return seed.AvailabilityZoneID
	}
	return seed.Region
}

// resolveSpotPlacementScore reports the score for one scored scope, applying the most specific
// seed that matches and falling back to defaultScore when none does.
//
// Scopes are tried most specific first — zone, then Region, then the wildcard — and the first
// scope carrying any seed at all decides the answer. A Region-wide seed therefore does not
// override a zone-scoped one, which is the direction a caller expects: seeding one bad zone in an
// otherwise nominal Region is the whole point of the zone scope.
//
// Within one scope a request naming several instance types takes the **lowest** seeded score, and
// only types that actually carry a seed are considered. Both halves of that follow from what a
// seed is for: including unseeded types at the default would let the default mask a seed, and
// taking the maximum would let a nominal type mask the scarce one a test seeded. It is also the
// reading closest to AWS's own, whose score describes fulfilling the whole request rather than its
// easiest member — a request is no easier to place than its scarcest instance type.
//
// A score of zero cannot occur in a stored seed, because the seeding endpoint refuses anything
// outside 1 to 10; zero is therefore usable as the "nothing matched at this scope" sentinel.
func (p *EC2Plugin) resolveSpotPlacementScore(region, zoneID string, instanceTypes []string, defaultScore int) (int, error) {
	scopes := make([]string, 0, 3)
	if zoneID != "" {
		scopes = append(scopes, zoneID)
	}
	if region != "" {
		scopes = append(scopes, region)
	}
	scopes = append(scopes, "*")

	types := make([]string, 0, len(instanceTypes)+1)
	types = append(types, instanceTypes...)
	types = append(types, "*")

	for _, scope := range scopes {
		lowest := 0
		for _, instanceType := range types {
			seed, err := p.spotPlacementScoreSeed(ec2SPSCtrlKey(scope, instanceType))
			if err != nil {
				return 0, err
			}
			if seed == nil {
				continue
			}
			if lowest == 0 || seed.Score < lowest {
				lowest = seed.Score
			}
		}
		if lowest != 0 {
			return lowest, nil
		}
	}
	return defaultScore, nil
}

// spotPlacementScoreSeed reads one seed by key, reporting (nil, nil) when none is stored.
func (p *EC2Plugin) spotPlacementScoreSeed(key string) (*ec2SpotPlacementScoreSeed, error) {
	data, err := p.state.Get(context.Background(), ec2SPSCtrlNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 spotPlacementScoreSeed get: %w", err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
	}
	var seed ec2SpotPlacementScoreSeed
	if err := json.Unmarshal(data, &seed); err != nil {
		return nil, fmt.Errorf("ec2 spotPlacementScoreSeed unmarshal: %w", err)
	}
	return &seed, nil
}

// handleEC2SeedSpotPlacementScore handles POST /v1/ec2/spot-placement-scores. It seeds the score
// GetSpotPlacementScores reports for a Region, an Availability Zone, or every Region, optionally
// narrowed to one instance type. Substrate models no Spot capacity; this sets the observable
// result. Body: {"region","availabilityZoneId","instanceType","score"}.
func (s *Server) handleEC2SeedSpotPlacementScore(w http.ResponseWriter, r *http.Request) {
	var seed ec2SpotPlacementScoreSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Score < ec2SPSMinScore || seed.Score > ec2SPSMaxScore {
		http.Error(w, fmt.Sprintf(`{"error":"score must be between %d and %d"}`,
			ec2SPSMinScore, ec2SPSMaxScore), http.StatusBadRequest)
		return
	}
	// Refused rather than resolved by precedence: a seed naming both a Region and a zone ID in
	// another Region has no reading that is not a guess, and an AZ ID already fixes its Region.
	if seed.Region != "" && seed.AvailabilityZoneID != "" {
		http.Error(w, `{"error":"specify region or availabilityZoneId, not both"}`,
			http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := ec2SPSCtrlKey(ec2SPSSeedScope(&seed), seed.InstanceType)
	if err := s.state.Put(r.Context(), ec2SPSCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "scope": key, "score": seed.Score})
}

// handleEC2ClearSpotPlacementScore handles DELETE /v1/ec2/spot-placement-scores. With any of
// ?region=, ?availabilityZoneId= or ?instanceType= it removes that one seed; with none it removes
// all of them.
func (s *Server) handleEC2ClearSpotPlacementScore(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	region := query.Get("region")
	zoneID := query.Get("availabilityZoneId")
	instanceType := query.Get("instanceType")
	if region != "" || zoneID != "" || instanceType != "" {
		scope := zoneID
		if scope == "" {
			scope = region
		}
		if err := s.state.Delete(r.Context(), ec2SPSCtrlNamespace,
			ec2SPSCtrlKey(scope, instanceType)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), ec2SPSCtrlNamespace, "score:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, key := range keys {
		if err := s.state.Delete(r.Context(), ec2SPSCtrlNamespace, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "cleared": strconv.Itoa(len(keys))})
}
