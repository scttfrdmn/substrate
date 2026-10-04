package emulator

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// ec2NatCtrlNamespace is the state namespace for EC2 NAT gateway control-plane (seed) data. It is
// the NAT gateway kind's own, so a prefix-sweeping DELETE of these seeds cannot clear a snapshot's
// or an instance's (see [progression]).
const ec2NatCtrlNamespace = "ec2-nat-ctrl"

// ec2NatGatewayProgression is a seeded progression of a NAT gateway's state (#1188), on the shared
// [progression] helper and in the shape of [ec2SnapshotProgression].
//
// A NAT gateway is one of the slowest resources in a VPC to come up, which is why every CDK,
// CloudFormation and Terraform run waits on it; API_CreateNatGateway's two published sample
// responses both close `<state>pending</state>`. Substrate's record settles `available` at once, so
// a consumer's wait loop exited on its first iteration. Seeding is what makes the transition
// observable.
//
// # Why the default count is zero
//
// An unseeded NAT gateway reads exactly as it always has — CreateNatGateway and every
// DescribeNatGateways answer `available` — so every existing fixture and recorded event log
// replays byte-identically, which #1188 requires. A NAT gateway created under a "*" seed with
// pendingObservations above zero is born `pending`, as the samples show, and its create response
// says so. That is the snapshot rule (#715) unchanged: the seed governs what an observation
// *reports* and never rewrites the record, which holds the settled state.
//
// # Both transitions are modeled
//
// One seed governs the creating transition (state, default `pending`, then finalState, default
// `available`) and the deleting one: DeleteNatGateway restarts the countdown
// ([progression.reset], the instance variant's rule), and the same pendingObservations then report
// `deleting` before `deleted`. The deleting transition's two states are fixed, because the page
// publishes no other path out of a delete; a seed's state and finalState describe creation only.
type ec2NatGatewayProgression struct {
	// NatGatewayID is the NAT gateway the seed applies to, or "*" for any.
	NatGatewayID string `json:"natGatewayId"`

	// PendingObservations is the number of DescribeNatGateways observations that report State
	// (or `deleting`, after a delete) before the gateway reaches FinalState (or `deleted`). Zero
	// means the gateway is in its final state from the first observation.
	PendingObservations int `json:"pendingObservations"`

	// State is what a creating gateway reports while the countdown runs. Defaults to "pending".
	State string `json:"state"`

	// FinalState is what it reports once the countdown is exhausted. Defaults to "available";
	// "failed" is the seedable failure outcome.
	FinalState string `json:"finalState"`

	// FailureCode is rendered as failureCode when the gateway reports `failed`. It must be one of
	// the six codes API_NatGateway publishes ([ec2NatFailureMessages]); it defaults to
	// InternalError, because the page documents a failed gateway as always carrying one ("Check
	// the failureCode and failureMessage fields for the reason").
	FailureCode string `json:"failureCode"`

	// FailureMessage is rendered as failureMessage beside FailureCode. It defaults to the message
	// the page publishes for the code, with the gateway's own identifiers substituted where
	// substrate holds them.
	FailureMessage string `json:"failureMessage"`
}

// ec2NatGatewayStates is NatGateway.state's Valid Values, as API_NatGateway publishes them.
var ec2NatGatewayStates = []string{"pending", "failed", "available", "deleting", "deleted"}

// ec2NatFailureMessages maps each failureCode API_NatGateway publishes to the failureMessage it
// publishes for it, verbatim, with the placeholder identifiers {alloc}, {vpc} and {subnet} that
// [ec2NatGatewayProgression.failureMessage] fills in. InternalError's network interface is one
// substrate does not model, so its placeholder is left as the page prints it.
var ec2NatFailureMessages = map[string]string{
	"InsufficientFreeAddressesInSubnet": "Subnet has insufficient free addresses to create this NAT gateway",
	"Gateway.NotAttached":               "Network {vpc} has no Internet gateway attached",
	"InvalidAllocationID.NotFound":      "Elastic IP address {alloc} could not be associated with this NAT gateway",
	"Resource.AlreadyAssociated":        "Elastic IP address {alloc} is already associated",
	"InternalError":                     "Network interface eni-xxxxxxxx, created and used internally by this NAT gateway is in an invalid state. Please try again.",
	"InvalidSubnetID.NotFound":          "The specified subnet {subnet} does not exist or could not be found.",
}

// ec2NatGatewayProgressions is the NAT gateway kind's seeded countdown, in [ec2NatCtrlNamespace].
// Its counters are per gateway even under the "*" wildcard (#582).
var ec2NatGatewayProgressions = newProgression[ec2NatGatewayProgression](ec2NatCtrlNamespace, "natGatewayId", "pendingObservations")

// progressionID implements [progressionSeed].
func (seed ec2NatGatewayProgression) progressionID() string { return seed.NatGatewayID }

// progressionObservations implements [progressionSeed].
func (seed ec2NatGatewayProgression) progressionObservations() int {
	return seed.PendingObservations
}

// validateProgression implements [progressionSeed]: both states are published values, and a
// failure code or message is accepted only with a `failed` final state and a published code.
func (seed ec2NatGatewayProgression) validateProgression() error {
	if err := progressionStates("NAT gateway", ec2NatGatewayStates, seed.State, seed.FinalState); err != nil {
		return err
	}
	if seed.FailureCode == "" && seed.FailureMessage == "" {
		return nil
	}
	if seed.FinalState != "failed" {
		return fmt.Errorf("failureCode and failureMessage apply only to finalState %q", "failed")
	}
	if seed.FailureCode != "" {
		if _, ok := ec2NatFailureMessages[seed.FailureCode]; !ok {
			codes := make([]string, 0, len(ec2NatFailureMessages))
			for code := range ec2NatFailureMessages {
				codes = append(codes, code)
			}
			slices.Sort(codes)
			return fmt.Errorf("unknown NAT gateway failureCode %q; published codes are %s", seed.FailureCode, strings.Join(codes, ", "))
		}
	}
	return nil
}

// ec2NatObservation is what one observation of a NAT gateway reports.
type ec2NatObservation struct {
	State          string
	FailureCode    string
	FailureMessage string
}

// observation returns what the seen-th observation of a seeded gateway reports, counting from
// zero. deleted says whether the record has been deleted, which selects the deleting transition.
func (seed ec2NatGatewayProgression) observation(gw EC2NATGateway, seen int) ec2NatObservation {
	total := seed.PendingObservations
	if gw.State == "deleted" {
		state, _ := countdownState(seen, total, "", "", "deleting", "deleted")
		return ec2NatObservation{State: state}
	}
	state, _ := countdownState(seen, total, seed.State, seed.FinalState, "pending", gw.State)
	if state != "failed" {
		return ec2NatObservation{State: state}
	}
	code := seed.FailureCode
	if code == "" {
		code = "InternalError"
	}
	message := seed.FailureMessage
	if message == "" {
		message = strings.NewReplacer(
			"{alloc}", ec2OrPlaceholder(gw.AllocationID, "eipalloc-xxxxxxxx"),
			"{vpc}", ec2OrPlaceholder(gw.VPCID, "vpc-xxxxxxxx"),
			"{subnet}", ec2OrPlaceholder(gw.SubnetID, "subnet-xxxxxxxx"),
		).Replace(ec2NatFailureMessages[code])
	}
	return ec2NatObservation{State: state, FailureCode: code, FailureMessage: message}
}

// ec2OrPlaceholder returns id, or the page's own placeholder when substrate holds none — a private
// NAT gateway has no allocation ID to name.
func ec2OrPlaceholder(id, placeholder string) string {
	if id == "" {
		return placeholder
	}
	return id
}

// peekNatGatewayState reports what the gateway would observe right now without spending an
// observation. CreateNatGateway and a ClientToken retry use it: neither is a poll, so neither
// counts against the budget a test means for DescribeNatGateways.
func (p *EC2Plugin) peekNatGatewayState(gw EC2NATGateway) (ec2NatObservation, error) {
	seed, seen, err := ec2NatGatewayProgressions.peek(context.Background(), p.state, gw.NatGatewayID)
	if err != nil {
		return ec2NatObservation{}, fmt.Errorf("ec2 peekNatGatewayState: %w", err)
	}
	if seed == nil {
		return ec2NatObservation{State: gw.State}, nil
	}
	return seed.observation(gw, seen), nil
}

// observeNatGatewayState reports what this DescribeNatGateways observation of the gateway sees,
// and spends one observation. It takes [EC2Plugin.seedMu], for the reason
// [EC2Plugin.observeSnapshotStatus] records.
func (p *EC2Plugin) observeNatGatewayState(gw EC2NATGateway) (ec2NatObservation, error) {
	seed, seen, err := ec2NatGatewayProgressions.observe(context.Background(), p.state, &p.seedMu, gw.NatGatewayID)
	if err != nil {
		return ec2NatObservation{}, fmt.Errorf("ec2 observeNatGatewayState: %w", err)
	}
	if seed == nil {
		return ec2NatObservation{State: gw.State}, nil
	}
	return seed.observation(gw, seen), nil
}

// handleEC2SeedNatGatewayState handles POST /v1/ec2/nat-gateway-state. Body:
// {"natGatewayId","pendingObservations","state","finalState","failureCode","failureMessage"}.
// Seeding resets the countdown of every gateway the seed governs.
func (s *Server) handleEC2SeedNatGatewayState(w http.ResponseWriter, r *http.Request) {
	ec2NatGatewayProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleEC2ClearNatGatewayState handles DELETE /v1/ec2/nat-gateway-state. With ?natGatewayId=...
// it removes that seed; without it removes all, and the countdowns they governed.
func (s *Server) handleEC2ClearNatGatewayState(w http.ResponseWriter, r *http.Request) {
	ec2NatGatewayProgressions.serveClear(w, r, s.state, s.logger)
}
