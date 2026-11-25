package emulator

// EC2 state changes another service makes on its own behalf (#1619).
//
// AWS Fault Injection Service stops, starts, terminates and interrupts instances during an
// experiment. None of those is a caller's StopInstances or TerminateInstances: routing one would
// record a request the caller never sent, and an interruption is not an API call at all. So the
// changes go through these methods, which no request reaches, as an Auto Scaling group's
// terminations go through [EC2Plugin.terminateForAutoScaling].
//
// Termination goes through [EC2Plugin.terminateInstanceIDsBecause], the body a caller's
// TerminateInstances runs, so everything a termination settles is settled the same way: an
// instance with legacy interfaces has them materialized as records first, its interfaces are
// deleted or detached and their addresses released, its volumes deleted or released, and its Spot
// request settled. Stop and start mirror the record writes of StopInstances and StartInstances,
// which have no per-instance helper to share, under the same instMu.
//
// # State reasons
//
// Instance carries stateReason and reason (StateTransitionReason), "The reason for the most
// recent state transition" (API_Instance). An internal change records both; a caller's stop,
// start or terminate clears them, so a describe never reports the reason for a transition that
// is no longer the most recent. Terminating an instance that is already terminated is no
// transition: the reasons, TerminatedTime and the observation countdown stay as the termination
// that happened wrote them, only settlement that termination left unfinished is repeated (see
// [EC2Plugin.resettleTerminated]), and the response reports the state a describe would report
// now. The messages are API_StateReason's own, which publishes
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

// terminateInternally terminates ids through [EC2Plugin.terminateInstanceIDsBecause], recording
// why. honorProtection says whether termination protection refuses the termination, as it does
// for an API call made on the caller's behalf and does not for a Spot interruption.
func (p *EC2Plugin) terminateInternally(reqCtx *RequestContext, ids []string, honorProtection bool, why *ec2InternalTransition) error {
	_, err := p.terminateInstanceIDsBecause(reqCtx, ids, honorProtection, why)
	return err
}

// stopInternally stops ids, recording why, as StopInstances writes the record: stopped, with the
// instance's observation countdown restarted so a seeded progression reports `stopping` from
// here. It returns the instances it stopped; see [EC2Plugin.transitionInternally] for the ones
// it passes over.
func (p *EC2Plugin) stopInternally(reqCtx *RequestContext, ids []string, why *ec2InternalTransition) ([]string, error) {
	return p.transitionInternally(reqCtx, ids, EC2InstanceState{Code: 80, Name: ec2StateStopped}, why)
}

// startInternally starts ids as StartInstances writes the record, clearing the reasons the stop
// recorded, since the start is now the most recent transition.
func (p *EC2Plugin) startInternally(reqCtx *RequestContext, ids []string) ([]string, error) {
	return p.transitionInternally(reqCtx, ids, EC2InstanceState{Code: 16, Name: ec2StateRunning}, nil)
}

// transitionInternally settles ids into to, recording why, and returns the ones it changed.
//
// It holds instMu, the lock every termination holds, and reads each record under it, so an
// instance a concurrent termination ended is seen ended and never written back. An instance that
// is gone, terminated, or already in the target state is passed over rather than refused: no
// transition is due, and a stop rewriting a stopped instance's reasons, or a later restart
// starting an instance a caller had stopped, would be a change nothing asked for.
func (p *EC2Plugin) transitionInternally(reqCtx *RequestContext, ids []string, to EC2InstanceState, why *ec2InternalTransition) ([]string, error) {
	p.instMu.Lock()
	defer p.instMu.Unlock()
	now := p.tc.Now()
	var changed []string
	for _, id := range ids {
		inst, err := p.loadInstanceForInternal(reqCtx.AccountID, reqCtx.Region, id)
		if err != nil {
			return changed, err
		}
		if inst == nil || inst.State.Name == ec2StateTerminated || inst.State.Name == to.Name {
			continue
		}
		inst.State = to
		why.apply(inst, now)
		if err := p.putEC2Record(context.Background(), ec2InstanceKey(reqCtx.AccountID, reqCtx.Region, id), *inst); err != nil {
			return changed, fmt.Errorf("ec2 internal transition to %s: %w", to.Name, err)
		}
		if err := p.resetInstanceObservations(id); err != nil {
			return changed, err
		}
		changed = append(changed, id)
	}
	return changed, nil
}

// ec2SpotStateDisabled is the Spot request state of a request whose instance is stopped: the
// User Guide's State changes for a Spot request lists instance-stopped-by-experiment (like
// instance-stopped-by-user) as "disabled | stopped".
const ec2SpotStateDisabled = "disabled"

// interruptSpotInstances interrupts Spot Instances as EC2 does when it reclaims capacity:
// "Two minutes after the interruption time, the Spot Instances are terminated or stopped,
// depending on their interruption behavior" (FIS actions reference,
// aws:ec2:send-spot-instance-interruptions). An instance whose request names stop or hibernate
// is stopped with Server.SpotInstanceShutdown and its request records
// instance-stopped-by-experiment and the disabled state; every other instance is terminated with
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
			stop = append(stop, id)
			stopRequests[id] = inst.SpotInstanceRequestID
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
	stopped, err := p.stopInternally(reqCtx, stop, &ec2InternalTransition{
		reasonCode: ec2ReasonSpotInstanceShutdown, initiator: ec2InitiatorService, at: at,
	})
	if err != nil {
		return err
	}
	now := at.UTC().Format(time.RFC3339)
	for _, id := range stopped {
		if stopRequests[id] == "" {
			continue
		}
		r, err := p.getSpotRequest(reqCtx, stopRequests[id])
		if err != nil {
			return err
		}
		if r == nil || r.InstanceID != id || r.State != ec2SpotStateActive {
			continue
		}
		r.State = ec2SpotStateDisabled
		r.setStatus(ec2SpotStatusStoppedByExperiment, now)
		if err := p.putSpotRequest(*r); err != nil {
			return err
		}
	}
	return nil
}

// onSpotInstanceStarted returns a disabled request to active when its stopped instance starts:
// "fulfilled | active | stopped → running" (State changes for a Spot request, Fulfilled). The
// same page has a restarted instance-stopped-by-user request pass through pending-evaluation
// first; substrate models no evaluation delay, as for every fulfillment. A request in any other
// state, or one now fulfilled by another instance, is left as it is.
func (p *EC2Plugin) onSpotInstanceStarted(reqCtx *RequestContext, inst EC2Instance) error {
	if inst.SpotInstanceRequestID == "" {
		return nil
	}
	r, err := p.getSpotRequest(reqCtx, inst.SpotInstanceRequestID)
	if err != nil || r == nil || r.InstanceID != inst.InstanceID || r.State != ec2SpotStateDisabled {
		return err
	}
	r.State = ec2SpotStateActive
	r.setStatus(ec2SpotStatusFulfilled, p.tc.Now().UTC().Format(time.RFC3339))
	return p.putSpotRequest(*r)
}

// resettleTerminated repeats, for an instance already terminated, the settlement its termination
// may not have finished — a store failure part-way through leaves the instance written terminated
// and the rest undone — so a retried TerminateInstances repairs it, as it always has for volumes.
// Each step is idempotent and changes nothing a finished settlement left:
//
//   - interfaces still attached to the instance are deleted or detached and their addresses
//     released ([EC2Plugin.settleInstanceInterfaces]); with none attached, nothing is read
//     further, so an instance whose interfaces were never materialized draws no identifier;
//   - its volumes are deleted or released;
//   - its Spot request is settled only while it still shows this instance as fulfilling it
//     (active, or cancelled with the instance running), with the status the termination's own
//     reason implies and at TerminatedTime, so instance-terminated-by-experiment survives a later
//     caller's TerminateInstances.
func (p *EC2Plugin) resettleTerminated(reqCtx *RequestContext, inst EC2Instance) error {
	all, err := p.ec2NetworkInterfaces(context.Background(), reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return err
	}
	for _, rec := range all {
		if rec.attached() && rec.Attachment.InstanceID == inst.InstanceID {
			if err := p.settleInstanceInterfaces(reqCtx, inst); err != nil {
				return err
			}
			break
		}
	}
	if err := p.ec2DeleteInstanceVolumes(reqCtx.AccountID, reqCtx.Region, inst.InstanceID); err != nil {
		return err
	}
	if inst.SpotInstanceRequestID == "" {
		return nil
	}
	r, err := p.getSpotRequest(reqCtx, inst.SpotInstanceRequestID)
	if err != nil || r == nil || r.InstanceID != inst.InstanceID {
		return err
	}
	unsettled := r.State == ec2SpotStateActive ||
		(r.State == ec2SpotStateCancelled && r.StatusCode == ec2SpotStatusCanceledRunning)
	if !unsettled {
		return nil
	}
	why := (*ec2InternalTransition)(nil)
	if inst.StateReason != nil && inst.StateReason.Code == ec2ReasonSpotInstanceTermination {
		why = &ec2InternalTransition{spotStatus: ec2SpotStatusTerminatedByExperiment}
	}
	at := p.tc.Now()
	if t, parseErr := time.Parse(time.RFC3339, inst.TerminatedTime); parseErr == nil {
		at = t
	}
	return p.onSpotInstanceTerminated(reqCtx, inst, why.spotTerminationStatus(), at)
}
