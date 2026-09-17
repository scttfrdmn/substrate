package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Capacity Reservation outcome seeding (#891).
//
// The reason a consumer reserves capacity at all is that capacity is finite, so the
// observations worth testing are the ones where the request does not succeed: not enough
// capacity in the cell, an On-Demand quota already spent, a Region or tenancy that cannot
// serve the instance type, a throttled caller. Substrate models no capacity broker and no
// quota ledger, so none of those can be derived from anything it knows — they are seeded,
// per the rule that a deterministic emulator produces different results by being seeded
// rather than by behaving nondeterministically.
//
// A capacity failure has **two** documented observable shapes, which is the fact that
// shapes this seed. Either the call fails outright with a code from EC2's error tables, or
// it succeeds and the reservation reports a non-nominal state — AWS's own prose for the
// failed state is "A request can fail due to request parameters that are not valid,
// capacity constraints, or instance limit constraints. You can view a failed request for
// 60 minutes." Which of the two AWS produces for a given cell is not documented, so a seed
// selects one rather than substrate choosing on the caller's behalf; a seed naming both is
// refused.
//
// The seed is scoped to a (instance type, Availability Zone) pair, because that pair is
// what a consumer treats as one capacity cell, and either half may be the wildcard so a
// test can say "g6.xlarge is short everywhere" or "us-east-1a is short for everything"
// without enumerating the rest.

// ec2CRCtrlNamespace is the state namespace for Capacity Reservation control-plane (seed)
// data.
//
// A namespace of its own rather than a key prefix inside [ec2Namespace], following
// [ec2FleetCtrlNamespace] and [ec2SPSCtrlNamespace]: seed data is not a resource, and
// mixing it into the resource namespace would put it in front of every prefix scan that
// walks EC2 state — including the one [ec2TagScanTargets] now performs over "cr:".
const ec2CRCtrlNamespace = "ec2-cr-ctrl"

// ec2CRSeedableStates are the states a seed may put a new reservation into.
//
// Five of the state member's thirteen values. The other eight belong to a future-dated
// reservation or a Capacity Block — assessing, scheduled, payment-pending, payment-failed,
// delayed, unsupported, cancelling — or, in the case of unavailable, appear in the member's
// Valid Values line with no prose description anywhere on the page. Substrate models
// neither future-dated reservations nor Capacity Blocks, so seeding a state that only those
// reach would publish an observation nothing else in the emulator is consistent with. The
// eight are recorded as unmodelled in docs/services.md rather than silently absent.
var ec2CRSeedableStates = []string{"active", "pending", "failed", "expired", "cancelled"}

// ec2CRSeedableErrors are the failure codes a seed may make CreateCapacityReservation
// answer, each with the HTTP status substrate answers it with.
//
// The five codes are the ones EC2's error tables publish for this operation, and two of
// them name Capacity Reservations outright: InsufficientInstanceCapacity ("This error can
// occur if you launch a new instance, restart a stopped instance, create a new Capacity
// Reservation, or modify an existing Capacity Reservation") and InstanceLimitExceeded
// ("This error can occur if you are launching an instance or if you are creating a Capacity
// Reservation. Capacity Reservations count towards your On-Demand Instance limits").
//
// **Two of the five are server errors, and substrate answers the documented class rather
// than the intuitive one.** errors-overview.html puts InsufficientInstanceCapacity and
// RequestLimitExceeded in its *server* error table, whose preamble says such errors "are
// accompanied by a 500-series HTTP response code", while the other three are in the client
// table, "accompanied by a 400-series HTTP response code". A consumer reads
// InsufficientInstanceCapacity as a capacity signal and would expect a 400; answering one
// would let retry logic pass a test here that it fails against AWS, which is the divergence
// direction that matters. The same page contradicts itself once, writing the throttle code
// as "Client.RequestLimitExceeded" in prose while listing RequestLimitExceeded in the
// server table, and CommonErrors.html does not list it at all — the server table is
// followed because it is the one place the code appears in a table at all.
//
// The exact numeric status is **UNVERIFIED**: neither page assigns a number, only a class,
// so 500 is substrate's reading of "500-series" and 400 of "400-series". A code outside
// this table is refused by the seeding endpoint rather than defaulted, because the status
// is the half substrate cannot derive — adding a code means deciding its class here.
var ec2CRSeedableErrors = map[string]int{
	"InsufficientInstanceCapacity": http.StatusInternalServerError,
	"RequestLimitExceeded":         http.StatusInternalServerError,
	"InstanceLimitExceeded":        http.StatusBadRequest,
	"VcpuLimitExceeded":            http.StatusBadRequest,
	"Unsupported":                  http.StatusBadRequest,
}

// ec2CRDefaultErrorMessages are the messages substrate answers a seeded code with when the
// seed supplies none, each AWS's own words for that code from errors-overview.html.
var ec2CRDefaultErrorMessages = map[string]string{
	"InsufficientInstanceCapacity": "There is not enough capacity to fulfill your request. " +
		"Try again at a later time, try in a different Availability Zone, or request a smaller " +
		"Capacity Reservation.",
	"RequestLimitExceeded":  "Request limit exceeded.",
	"InstanceLimitExceeded": "The requested quantity exceeds your On-Demand Instance quota.",
	"VcpuLimitExceeded":     "The requested quantity exceeds your vCPU-based On-Demand Instance quota.",
	"Unsupported":           "The requested configuration is currently not supported.",
}

// ec2CRSeedableErrorList renders [ec2CRSeedableErrors]' codes in a stable order, for the
// refusal the seeding endpoint answers an unknown code with.
//
// Sorted rather than ranged, because a map range would order the five differently on each call
// and a refusal message that changes on its own is exactly what a caller diffing two runs must
// not see.
func ec2CRSeedableErrorList() string {
	codes := make([]string, 0, len(ec2CRSeedableErrors))
	for code := range ec2CRSeedableErrors {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return strings.Join(codes, ", ")
}

// ec2CapacityReservationOutcome is a seeded CreateCapacityReservation outcome for one
// capacity cell.
//
// Exactly one of ErrorCode and State is set; the seeding endpoint refuses a seed carrying
// both, because the two are the alternative shapes of one answer rather than two
// independent knobs. See the file preamble for why AWS documents both.
type ec2CapacityReservationOutcome struct {
	// InstanceType is the instance type the seed applies to, e.g. "g6.xlarge". Empty means
	// every instance type.
	InstanceType string `json:"instanceType,omitempty"`

	// AvailabilityZone is the zone *name* the seed applies to, e.g. "us-east-1a" — the
	// spelling CreateCapacityReservation takes and the reservation reports. Empty means
	// every zone, including a request that names none.
	AvailabilityZone string `json:"availabilityZone,omitempty"`

	// ErrorCode is the code CreateCapacityReservation fails with, one of
	// [ec2CRSeedableErrors]. Empty means the call succeeds.
	ErrorCode string `json:"errorCode,omitempty"`

	// ErrorMessage is the message accompanying ErrorCode. Empty falls back to AWS's own
	// words for the code, from [ec2CRDefaultErrorMessages].
	ErrorMessage string `json:"errorMessage,omitempty"`

	// State is the state the created reservation reports, one of [ec2CRSeedableStates].
	// Empty means the nominal state, which is active.
	State string `json:"state,omitempty"`
}

// ec2CRCtrlKey returns the state key for a seeded outcome. Either half may be "*", which is
// what an empty instance type or zone becomes.
func ec2CRCtrlKey(instanceType, availabilityZone string) string {
	if instanceType == "" {
		instanceType = "*"
	}
	if availabilityZone == "" {
		availabilityZone = "*"
	}
	return "outcome:" + instanceType + "/" + availabilityZone
}

// resolveCapacityReservationOutcome returns the seeded outcome for one create request, or
// (nil, nil) when no seed applies.
//
// Four candidate keys, most specific first: the exact cell, then the type in any zone, then
// any type in the zone, then the wildcard. Type-before-zone is substrate's ordering and it
// is the one a caller expects — a seed naming an instance type is a statement about that
// type's scarcity, which is the narrower claim of the two, where a zone-wide seed is about
// the zone as a whole.
func (p *EC2Plugin) resolveCapacityReservationOutcome(instanceType, availabilityZone string) (*ec2CapacityReservationOutcome, error) {
	keys := []string{
		ec2CRCtrlKey(instanceType, availabilityZone),
		ec2CRCtrlKey(instanceType, ""),
		ec2CRCtrlKey("", availabilityZone),
		ec2CRCtrlKey("", ""),
	}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		data, err := p.state.Get(context.Background(), ec2CRCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("ec2 resolveCapacityReservationOutcome get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed ec2CapacityReservationOutcome
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("ec2 resolveCapacityReservationOutcome unmarshal: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// ec2CRSeededError builds the refusal a seeded outcome names, with the status its code's
// error table implies. See [ec2CRSeedableErrors].
func ec2CRSeededError(seed *ec2CapacityReservationOutcome) *AWSError {
	message := seed.ErrorMessage
	if message == "" {
		message = ec2CRDefaultErrorMessages[seed.ErrorCode]
	}
	return &AWSError{
		Code:       seed.ErrorCode,
		Message:    message,
		HTTPStatus: ec2CRSeedableErrors[seed.ErrorCode],
	}
}

// handleEC2SeedCapacityReservationOutcome handles POST /v1/ec2/capacity-reservation-outcomes.
// It seeds what CreateCapacityReservation answers for one capacity cell — a failure code, or
// a non-nominal state on an otherwise successful create. Substrate models no capacity broker
// and no On-Demand quota; this sets the observable result.
// Body: {"instanceType","availabilityZone","errorCode","errorMessage","state"}.
func (s *Server) handleEC2SeedCapacityReservationOutcome(w http.ResponseWriter, r *http.Request) {
	var seed ec2CapacityReservationOutcome
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.ErrorCode != "" && seed.State != "" {
		http.Error(w, `{"error":"specify errorCode or state, not both"}`, http.StatusBadRequest)
		return
	}
	if seed.ErrorCode == "" && seed.State == "" {
		http.Error(w, `{"error":"one of errorCode or state is required"}`, http.StatusBadRequest)
		return
	}
	if seed.ErrorCode != "" {
		if _, ok := ec2CRSeedableErrors[seed.ErrorCode]; !ok {
			http.Error(w, fmt.Sprintf(`{"error":"errorCode must be one of %s"}`,
				ec2CRSeedableErrorList()), http.StatusBadRequest)
			return
		}
	}
	if seed.State != "" && !containsStr(ec2CRSeedableStates, seed.State) {
		http.Error(w, fmt.Sprintf(`{"error":"state must be one of %s"}`,
			strings.Join(ec2CRSeedableStates, ", ")), http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := ec2CRCtrlKey(seed.InstanceType, seed.AvailabilityZone)
	if err := s.state.Put(r.Context(), ec2CRCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "cell": key})
}

// handleEC2ClearCapacityReservationOutcome handles DELETE
// /v1/ec2/capacity-reservation-outcomes. With ?instanceType= and/or ?availabilityZone= it
// removes that one cell's seed; with neither it removes all of them.
func (s *Server) handleEC2ClearCapacityReservationOutcome(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	instanceType := query.Get("instanceType")
	zone := query.Get("availabilityZone")
	if instanceType != "" || zone != "" {
		if err := s.state.Delete(r.Context(), ec2CRCtrlNamespace,
			ec2CRCtrlKey(instanceType, zone)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), ec2CRCtrlNamespace, "outcome:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, key := range keys {
		if err := s.state.Delete(r.Context(), ec2CRCtrlNamespace, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "cleared": strconv.Itoa(len(keys))})
}
