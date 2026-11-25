package emulator

// EC2 state changes another service makes on its own behalf (#1619).
//
// AWS Fault Injection Service stops, starts, terminates and interrupts instances during an
// experiment. None of those is a caller's StopInstances or TerminateInstances: routing one would
// record a request the caller never sent, and an interruption is not an API call at all. So the
// changes go through these methods, which no request reaches, as an Auto Scaling group's
// terminations go through [EC2Plugin.terminateForAutoScaling].
//
// Termination reuses [EC2Plugin.terminateInstanceIDs], so volume settlement and the Spot
// request's settlement happen exactly as for a caller's TerminateInstances. Stop and start mirror
// the record writes of StopInstances and StartInstances, which have no per-instance helper to
// share.
//
// # State reasons
//
// Instance carries stateReason and reason (StateTransitionReason), "The reason for the most
// recent state transition" (API_Instance). An internal change records both; a caller's stop,
// start or terminate clears them, so a describe never reports the reason for a transition that
// is no longer the most recent. Terminating an instance that is already terminated is no
// transition, so it changes nothing: the reasons, TerminatedTime, the observation countdown and
// the Spot request stay as the termination that happened settled them, and the response reports
// the state a describe would report now. The messages are API_StateReason's own, which publishes
// each message prefixed by its code. The transition reason's "Service initiated (YYYY-MM-DD
// HH:MM:SS GMT)" form is observed behavior (DescribeInstances of a reclaimed Spot host,
// ejc3/aws#489); its "User initiated" counterpart is the same form for an API call.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// The StateReason codes an internal transition records, from API_StateReason.
const (
	ec2ReasonUserInitiatedShutdown   = "Client.UserInitiatedShutdown"
	ec2ReasonSpotInstanceTermination = "Server.SpotInstanceTermination"
	ec2ReasonSpotInstanceShutdown    = "Server.SpotInstanceShutdown"
)

// ec2StateReasonMessages are API_StateReason's messages for the codes above, verbatim.
var ec2StateReasonMessages = map[string]string{
	ec2ReasonUserInitiatedShutdown: "Client.UserInitiatedShutdown: The instance was shut down using the Amazon EC2 API.",
	ec2ReasonSpotInstanceTermination: "Server.SpotInstanceTermination: The instance was terminated because the number of " +
		"Spot requests with a maximum price equal to or higher than the Spot price exceeded available capacity or " +
		"because of an increase in the Spot price.",
	ec2ReasonSpotInstanceShutdown: "Server.SpotInstanceShutdown: The instance was stopped because the number of Spot " +
		"requests with a maximum price equal to or higher than the Spot price exceeded available capacity or because " +
		"of an increase in the Spot price.",
}

// The initiators a transition reason names.
const (
	ec2InitiatorUser    = "User initiated"
	ec2InitiatorService = "Service initiated"
)

// The Spot request status codes an experiment's interruption records, from the FIS Spot
// interruption tutorial: "terminate - The status changes to instance-terminated-by-experiment"
// and "stop - The status changes to marked-for-stop-by-experiment and then
// instance-stopped-by-experiment". Their messages, in [ec2SpotStatusMessages], are substrate's own.
const (
	ec2SpotStatusTerminatedByExperiment = "instance-terminated-by-experiment"
	ec2SpotStatusStoppedByExperiment    = "instance-stopped-by-experiment"
)

// EC2StateReason is an instance's stateReason: the code and message of its most recent
// state transition.
type EC2StateReason struct {
	// Code is the reason code, e.g. Server.SpotInstanceTermination.
	Code string `json:"code"`
	// Message is the reason's message.
	Message string `json:"message"`
}

// ec2StateReasonItem is Instance's stateReason element.
type ec2StateReasonItem struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

// ec2StateReasonItemFor renders an instance's stateReason, nil when it records none.
func ec2StateReasonItemFor(inst EC2Instance) *ec2StateReasonItem {
	if inst.StateReason == nil {
		return nil
	}
	return &ec2StateReasonItem{Code: inst.StateReason.Code, Message: inst.StateReason.Message}
}

// ec2ClearStateReason drops the reasons a caller's own transition supersedes.
func ec2ClearStateReason(inst *EC2Instance) {
	inst.StateReason = nil
	inst.StateTransitionReason = ""
}

// ec2InternalTransition says why an internal transition happened and what it records.
type ec2InternalTransition struct {
	// reasonCode is the StateReason code the instance records.
	reasonCode string
	// initiator begins the instance's StateTransitionReason.
	initiator string
	// spotStatus is the status a terminated Spot Instance's request records; empty records
	// the status a caller's termination does.
	spotStatus string
	// at is when the transition happened, which its StateTransitionReason names; zero names
	// the clock's reading when it is applied.
	at time.Time
}

// apply records t on inst at now, or at t.at when it is set. A nil t clears the reasons, which
// is what a caller's own transition does.
func (t *ec2InternalTransition) apply(inst *EC2Instance, now time.Time) {
	if t == nil {
		ec2ClearStateReason(inst)
		return
	}
	if !t.at.IsZero() {
		now = t.at
	}
	inst.StateReason = &EC2StateReason{Code: t.reasonCode, Message: ec2StateReasonMessages[t.reasonCode]}
	inst.StateTransitionReason = fmt.Sprintf("%s (%s GMT)", t.initiator, now.UTC().Format("2006-01-02 15:04:05"))
}

// when is the transition's time: t.at when it is set, otherwise now, the clock's reading. A
// nil t is a caller's own transition, which happens now.
func (t *ec2InternalTransition) when(now time.Time) time.Time {
	if t == nil || t.at.IsZero() {
		return now
	}
	return t.at
}

// spotTerminationStatus is the status a terminated Spot Instance's request records under t.
func (t *ec2InternalTransition) spotTerminationStatus() string {
	if t == nil || t.spotStatus == "" {
		return ec2SpotStatusTerminatedByUser
	}
	return t.spotStatus
}

// peekInstanceState returns the state an observation of inst would report now — the seeded
// transient state while its countdown runs, its settled state otherwise — without spending an
// observation, as [EC2Plugin.observeInstanceState] does when a describe reports it. A seed-store
// failure reports the settled state, as an observation does.
func (p *EC2Plugin) peekInstanceState(inst EC2Instance) EC2InstanceState {
	p.seedMu.Lock()
	defer p.seedMu.Unlock()
	seed, seen, err := ec2InstanceProgressions.peek(context.Background(), p.state, inst.InstanceID)
	if err != nil || seed == nil || seed.TransientObservations <= 0 || seen >= seed.TransientObservations {
		return inst.State
	}
	if transient, ok := ec2TransientStateFor(inst.State); ok {
		return transient
	}
	return inst.State
}

// ec2InstanceKey is the state key of one instance record.
func ec2InstanceKey(accountID, region, id string) string {
	return "instance:" + accountID + "/" + region + "/" + id
}

// loadInstanceForInternal reads one instance, nil when absent.
func (p *EC2Plugin) loadInstanceForInternal(accountID, region, id string) (*EC2Instance, error) {
	data, err := p.state.Get(context.Background(), ec2Namespace, ec2InstanceKey(accountID, region, id))
	if err != nil {
		return nil, fmt.Errorf("ec2 internal get %s: %w", id, err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // (nil, nil) is "no such instance", which callers handle.
	}
	var inst EC2Instance
	if err := json.Unmarshal(data, &inst); err != nil {
		return nil, fmt.Errorf("ec2 internal unmarshal %s: %w", id, err)
	}
	return &inst, nil
}

// instancesInRegion returns every instance record in an account and Region, sorted by instance
// ID, which is the order an experiment's target selection reads them in.
func (p *EC2Plugin) instancesInRegion(accountID, region string) ([]EC2Instance, error) {
	keys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+accountID+"/"+region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 internal list instances: %w", err)
	}
	out := make([]EC2Instance, 0, len(keys))
	for _, key := range keys {
		inst, err := p.loadInstanceForInternal(accountID, region, strings.TrimPrefix(key, "instance:"+accountID+"/"+region+"/"))
		if err != nil {
			return nil, err
		}
		if inst != nil {
			out = append(out, *inst)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

// terminateInternally terminates ids through [EC2Plugin.terminateInstanceIDs], recording why.
// honorProtection says whether termination protection refuses the termination, as it does for
// an API call made on the caller's behalf and does not for a Spot interruption.
func (p *EC2Plugin) terminateInternally(reqCtx *RequestContext, ids []string, honorProtection bool, why *ec2InternalTransition) error {
	_, err := p.terminateInstanceIDsBecause(reqCtx, ids, honorProtection, why)
	return err
}

// stopInternally stops ids, recording why, as StopInstances writes the record: stopped,
// with the instance's observation countdown restarted so a seeded progression reports
// `stopping` from here. A terminated instance cannot be stopped and is refused with
// StopInstances' own answer. Every ID is resolved before any is stopped.
func (p *EC2Plugin) stopInternally(reqCtx *RequestContext, ids []string, why *ec2InternalTransition) error {
	return p.transitionInternally(reqCtx, ids, "stopped", EC2InstanceState{Code: 80, Name: ec2StateStopped}, why)
}

// startInternally starts ids as StartInstances writes the record, clearing the reasons the stop
// recorded, since the start is now the most recent transition.
func (p *EC2Plugin) startInternally(reqCtx *RequestContext, ids []string) error {
	return p.transitionInternally(reqCtx, ids, "started", EC2InstanceState{Code: 16, Name: ec2StateRunning}, nil)
}

// transitionInternally settles ids into to, recording why.
func (p *EC2Plugin) transitionInternally(reqCtx *RequestContext, ids []string, verb string, to EC2InstanceState, why *ec2InternalTransition) error {
	instances := make([]EC2Instance, 0, len(ids))
	for _, id := range ids {
		inst, err := p.loadInstanceForInternal(reqCtx.AccountID, reqCtx.Region, id)
		if err != nil {
			return err
		}
		if lookupErr := ec2RequireResource(ec2InstanceIDKind, id, inst != nil); lookupErr != nil {
			return lookupErr
		}
		if inst.State.Name == ec2StateTerminated {
			return ec2CannotTransitionTerminated(verb)
		}
		instances = append(instances, *inst)
	}
	now := p.tc.Now()
	for _, inst := range instances {
		inst.State = to
		why.apply(&inst, now)
		if err := p.putEC2Record(context.Background(), ec2InstanceKey(reqCtx.AccountID, reqCtx.Region, inst.InstanceID), inst); err != nil {
			return fmt.Errorf("ec2 internal %s: %w", verb, err)
		}
		if err := p.resetInstanceObservations(inst.InstanceID); err != nil {
			return err
		}
	}
	return nil
}

// interruptSpotInstances interrupts Spot Instances as EC2 does when it reclaims capacity:
// "Two minutes after the interruption time, the Spot Instances are terminated or stopped,
// depending on their interruption behavior" (FIS actions reference,
// aws:ec2:send-spot-instance-interruptions). An instance whose request names stop or hibernate
// is stopped with Server.SpotInstanceShutdown and its request records
// instance-stopped-by-experiment; every other instance is terminated with
// Server.SpotInstanceTermination, and its request records instance-terminated-by-experiment and
// is settled as a terminated Spot Instance's request is. Hibernation is modeled as a stop.
// An instance already terminated, or already stopped for a stop, is left as it is. at is when
// the interruption happened, which the transition reasons and request statuses record.
func (p *EC2Plugin) interruptSpotInstances(reqCtx *RequestContext, ids []string, at time.Time) error {
	var terminate, stop []string
	stopRequests := map[string]string{}
	for _, id := range ids {
		inst, err := p.loadInstanceForInternal(reqCtx.AccountID, reqCtx.Region, id)
		if err != nil {
			return err
		}
		if inst == nil || inst.State.Name == ec2StateTerminated {
			continue
		}
		behavior := ""
		if inst.SpotInstanceRequestID != "" {
			r, err := p.getSpotRequest(reqCtx, inst.SpotInstanceRequestID)
			if err != nil {
				return err
			}
			if r != nil {
				behavior = r.InstanceInterruptionBehavior
			}
		}
		if behavior == "stop" || behavior == "hibernate" {
			if inst.State.Name != ec2StateStopped {
				stop = append(stop, id)
				stopRequests[id] = inst.SpotInstanceRequestID
			}
			continue
		}
		terminate = append(terminate, id)
	}
	if len(terminate) > 0 {
		if err := p.terminateInternally(reqCtx, terminate, false, &ec2InternalTransition{
			reasonCode: ec2ReasonSpotInstanceTermination, initiator: ec2InitiatorService,
			spotStatus: ec2SpotStatusTerminatedByExperiment, at: at,
		}); err != nil {
			return err
		}
	}
	if len(stop) == 0 {
		return nil
	}
	if err := p.stopInternally(reqCtx, stop, &ec2InternalTransition{
		reasonCode: ec2ReasonSpotInstanceShutdown, initiator: ec2InitiatorService, at: at,
	}); err != nil {
		return err
	}
	now := at.UTC().Format(time.RFC3339)
	for _, id := range stop {
		if stopRequests[id] == "" {
			continue
		}
		r, err := p.getSpotRequest(reqCtx, stopRequests[id])
		if err != nil {
			return err
		}
		if r == nil {
			continue
		}
		r.setStatus(ec2SpotStatusStoppedByExperiment, now)
		if err := p.putSpotRequest(*r); err != nil {
			return err
		}
	}
	return nil
}
