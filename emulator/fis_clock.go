package emulator

// An experiment's progress on the simulated clock (#1619).
//
// # The timeline
//
// StartExperiment resolves the targets and answers initiating. At the next observation the
// experiment fails if a target resolved to nothing ("Target resolution returned empty set.",
// the reason AWS reports, as observed in aws-samples/fis-template-library#66 and #68) unless its
// template's emptyTargetResolutionMode is skip, in which case the actions on that target are
// skipped. Otherwise it is running, and its actions run in startAfter order: an action with no
// startAfter starts at the experiment's start time, and one with startAfter starts when the last
// of the actions it names ends. Every time is computed from the experiment's start and the
// actions' durations, never from when a request happened to observe it, so a GetExperiment
// reports the same action start and end times however often it is polled. An experiment
// completes when every action has, at the latest action's end time.
//
// The actions substrate models:
//
//   - aws:ec2:stop-instances stops its instances at the action's start, passing over one already
//     stopped. With startInstancesAfterDuration it starts the ones it stopped again that long
//     afterwards, which is when the action completes; one terminated meanwhile fails the action, unless
//     completeIfInstancesTerminated is true (FIS actions reference). Without it the action
//     completes at once.
//   - aws:ec2:terminate-instances terminates its instances at the action's start, honoring
//     termination protection, since FIS runs TerminateInstances on the caller's behalf.
//   - aws:ec2:reboot-instances completes at once and changes nothing: a reboot is not
//     observable through DescribeInstances.
//   - aws:ec2:send-spot-instance-interruptions interrupts its Spot Instances
//     durationBeforeInterruption after the action starts, which is when the action completes
//     (see [EC2Plugin.interruptSpotInstances]). The interruption notice and the rebalance
//     recommendation are delivered through instance metadata and EventBridge, which substrate
//     does not model (#1722).
//   - Every other known action changes nothing and completes after its duration parameter, or
//     at once when it takes none.
//
// # Stopping, stop conditions and seeds
//
// StopExperiment answers stopping; at the next observation the experiment is stopped, its
// running actions stopped and its pending ones cancelled. Substrate does not roll an action
// back on a stop: instances a stop-instances action stopped stay stopped. A running experiment
// with an aws:cloudwatch:alarm stop condition reads the alarm's state on every observation, and
// stops when the alarm is in ALARM. A seeded outcome (fis_control.go) ends the experiment at its
// next observation, with the seeded status and reason.
//
// # Replay
//
// [FISPlugin.RunDue] runs before every top-level request, live and on replay, at the same step
// and with the clock pinned to the same reading (see clock_driven.go). Its effects are a
// function of the experiment records, the EC2 state and that clock reading, so a replay of the
// recorded requests re-derives them. It makes no dispatch: the EC2 changes go through EC2's
// internal path, and the alarm read is a read routed without being recorded, so there is no
// dispatch for a replay to skip. Any identifier an effect mints (a persistent Spot request's
// replacement instance) is drawn from a request ID derived from the triggering request, the
// experiment and the action, as [clockDrivenRequestID] derives a dispatch's.
//
// The reasons substrate gives where AWS publishes none — running, stopped, a stop condition, a
// failed action — are substrate's own.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The reasons substrate gives where no source publishes AWS's.
const (
	fisReasonRunning        = "Experiment is running."
	fisReasonStopped        = "Experiment stopped."
	fisReasonStopCondition  = "Experiment stopped by a stop condition."
	fisReasonEmptyTargets   = "Target resolution returned empty set."
	fisReasonActionStopped  = "Action was stopped."
	fisReasonActionCanceled = "Action was cancelled."
	fisReasonActionSkipped  = "Action was skipped."
	fisReasonSeededFailure  = "Experiment failed."
)

// fisActiveEntry is one experiment in the active set: when it is next due, whether it must be
// observed on every request regardless because a stop condition reads an alarm, and the
// template it was started from, which a seed may name.
type fisActiveEntry struct {
	Due        time.Time `json:"due"`
	Watch      bool      `json:"watch,omitempty"`
	TemplateID string    `json:"template_id,omitempty"`
}

// RunDue advances every active experiment that is due at the simulated clock's current reading:
// one whose next action start or end has come, one with an alarm stop condition, and one a seed
// names. It implements [ClockDrivenPlugin]. A request with no experiment running reads one key.
func (p *FISPlugin) RunDue(trigger *RequestContext, _ InternalDispatch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	active, err := p.loadActiveEntries()
	if err != nil || len(active) == 0 {
		return err
	}
	seeded, err := p.seededIDs()
	if err != nil {
		return err
	}
	now := p.tc.Now().UTC()
	var errs []error
	for _, key := range fisSortedKeys(active) {
		entry := active[key]
		id := key[strings.LastIndex(key, "/")+1:]
		if entry.Due.After(now) && !entry.Watch && !seeded[id] && !seeded[entry.TemplateID] && !seeded["*"] {
			continue
		}
		if err := p.advanceKey(trigger, key, now); err != nil {
			p.logger.Warn("fis: experiment's due work failed", "experiment", key, "err", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// advanceKey advances the experiment at key and records when it is next due.
func (p *FISPlugin) advanceKey(trigger *RequestContext, key string, now time.Time) error {
	var e FISExperiment
	found, err := p.loadJSON(key, &e)
	if err != nil {
		return err
	}
	if !found || fisTerminal(e.State.Status) {
		return p.setActive(key, nil)
	}
	before, err := json.Marshal(&e)
	if err != nil {
		return err
	}
	run := &fisRun{p: p, trigger: trigger, e: &e, now: now}
	if err := run.advance(); err != nil {
		return err
	}
	after, err := json.Marshal(&e)
	if err != nil {
		return err
	}
	if string(after) != string(before) {
		if err := p.putJSON(key, &e); err != nil {
			return err
		}
	}
	if fisTerminal(e.State.Status) {
		return p.setActive(key, nil)
	}
	next := run.nextDue()
	return p.setActive(key, &fisActiveEntry{Due: next, Watch: e.State.Status == fisStatusRunning && run.watched(), TemplateID: e.TemplateID})
}

// fisRun is one experiment's advance in one RunDue.
type fisRun struct {
	p       *FISPlugin
	trigger *RequestContext
	e       *FISExperiment
	now     time.Time
}

// advance applies everything due by r.now.
func (r *fisRun) advance() error {
	e := r.e
	seed, err := r.p.resolveSeededResult(e)
	if err != nil {
		return err
	}
	if seed != nil {
		r.finishSeeded(seed)
		return nil
	}
	if e.State.Status == fisStatusStopping {
		at := r.now
		if e.StopRequested != nil {
			at = *e.StopRequested
		}
		r.settle(fisStatusStopped, e.StopReason, at)
		return nil
	}
	if e.State.Status == fisStatusInitiating || e.State.Status == fisStatusPending {
		if !r.initiate() {
			return nil
		}
	}
	if r.watched() {
		alarmed, failure := r.stopConditionAlarmed()
		switch {
		case failure != "":
			r.settle(fisStatusFailed, failure, r.now)
			return nil
		case alarmed:
			r.settle(fisStatusStopped, fisReasonStopCondition, r.now)
			return nil
		}
	}
	return r.runTimeline()
}

// initiate settles target resolution and moves the experiment to running, reporting false
// when it ended instead.
func (r *fisRun) initiate() bool {
	e := r.e
	if e.ExperimentOptions.ActionsMode == "skip-all" {
		for _, name := range e.actionNames() {
			e.Actions[name].State = FISActionState{Status: fisStatusSkipped, Reason: fisReasonActionSkipped}
		}
		r.end(fisStatusCompleted, fisReasonCompleted, e.StartTime)
		return false
	}
	empty := map[string]bool{}
	for _, name := range fisSortedKeys(e.Targets) {
		if len(e.Resolved[name]) == 0 {
			if e.ExperimentOptions.EmptyTargetResolutionMode != "skip" {
				r.settle(fisStatusFailed, fisReasonEmptyTargets, e.StartTime)
				return false
			}
			empty[name] = true
		}
	}
	for _, name := range e.actionNames() {
		a := e.Actions[name]
		for _, target := range a.Targets {
			if empty[target] {
				a.State = FISActionState{Status: fisStatusSkipped, Reason: fisReasonActionSkipped}
			}
		}
	}
	e.State = FISActionState{Status: fisStatusRunning, Reason: fisReasonRunning}
	return true
}

// readyAt returns when a pending action may start, and false while one of the actions it
// starts after has not ended.
func (r *fisRun) readyAt(a *FISExperimentAction) (time.Time, bool) {
	at := r.e.StartTime
	for _, dep := range a.StartAfter {
		d := r.e.Actions[dep]
		if d == nil {
			continue
		}
		if !fisActionTerminal(d.State.Status) {
			return time.Time{}, false
		}
		if d.EndTime != nil && d.EndTime.After(at) {
			at = *d.EndTime
		}
	}
	return at, true
}

// nextEvent returns the earliest pending start or running end, and the action it belongs to;
// ties go to the first action name.
func (r *fisRun) nextEvent() (string, time.Time, bool) {
	var bestName string
	var best time.Time
	found := false
	for _, name := range r.e.actionNames() {
		a := r.e.Actions[name]
		var at time.Time
		switch a.State.Status {
		case fisStatusPending:
			t, ok := r.readyAt(a)
			if !ok {
				continue
			}
			at = t
		case fisStatusRunning:
			if a.DueAt == nil {
				continue
			}
			at = *a.DueAt
		default:
			continue
		}
		if !found || at.Before(best) {
			bestName, best, found = name, at, true
		}
	}
	return bestName, best, found
}

// runTimeline applies every action start and end due by r.now, in time order.
func (r *fisRun) runTimeline() error {
	for {
		name, at, ok := r.nextEvent()
		if !ok || at.After(r.now) {
			break
		}
		a := r.e.Actions[name]
		var failure string
		if a.State.Status == fisStatusPending {
			failure = r.startAction(name, a, at)
		} else {
			failure = r.finishAction(name, a, at)
		}
		if failure != "" {
			a.State = FISActionState{Status: fisStatusFailed, Reason: failure}
			a.EndTime = fisTimePtr(at)
			a.DueAt = nil
			r.settle(fisStatusFailed, "Action "+name+" failed: "+failure, at)
			return nil
		}
	}
	end := r.e.StartTime
	for _, name := range r.e.actionNames() {
		a := r.e.Actions[name]
		if !fisActionTerminal(a.State.Status) {
			return nil
		}
		if a.EndTime != nil && a.EndTime.After(end) {
			end = *a.EndTime
		}
	}
	r.end(fisStatusCompleted, fisReasonCompleted, end)
	return nil
}

// fisTimePtr returns a pointer to a copy of t.
func fisTimePtr(t time.Time) *time.Time { return &t }

// reqCtx derives the context the action's EC2 changes run under, from the triggering request,
// the experiment and the action, so a replay derives the same request ID and mints the same IDs.
func (r *fisRun) reqCtx(action, step string) *RequestContext {
	id := clockDrivenRequestID(r.trigger.RequestID, "fis/"+r.e.ARN+"/"+action+"/"+step)
	return &RequestContext{
		RequestID: id, AccountID: r.e.AccountID, Region: r.e.Region, Timestamp: r.now,
		IDs: NewIDMint(id), Metadata: map[string]interface{}{"stream_id": streamIDFromContext(r.trigger)},
	}
}

// targetIDs returns the resources the action's target resolved to.
func (r *fisRun) targetIDs(a *FISExperimentAction) []string {
	for _, target := range a.Targets {
		return r.e.Resolved[target]
	}
	return nil
}

// liveTargetIDs returns the EC2 plugin and the action's resolved instances that have not ended
// since the experiment resolved them. An instance terminated, or shutting down, by the time a
// delayed stop or terminate action starts is passed over, as target resolution passes it over
// (fis_targets.go), rather than failing the action with StopInstances' refusal for an instance
// that has been permanently deleted. That reading is substrate's.
func (r *fisRun) liveTargetIDs(a *FISExperimentAction) (*EC2Plugin, []string, error) {
	ec2, err := r.p.ec2()
	if err != nil {
		return nil, nil, err
	}
	var ids []string
	for _, id := range r.targetIDs(a) {
		inst, err := ec2.loadInstanceForInternal(r.e.AccountID, r.e.Region, id)
		if err != nil {
			return nil, nil, err
		}
		if inst == nil {
			continue
		}
		inst.State = ec2.peekInstanceState(*inst)
		if fisEndedInstance(*inst) {
			continue
		}
		ids = append(ids, id)
	}
	return ec2, ids, nil
}

// failureText renders an EC2 refusal as an action's failure reason.
func fisFailureText(err error) string {
	var awsErr *AWSError
	if errors.As(err, &awsErr) {
		return awsErr.Message
	}
	return err.Error()
}

// startAction starts an action at at and applies its start effect, returning a failure reason
// or "".
func (r *fisRun) startAction(name string, a *FISExperimentAction, at time.Time) string {
	a.State = FISActionState{Status: fisStatusRunning, Reason: ""}
	a.StartTime = fisTimePtr(at)
	due := at
	userInitiated := &ec2InternalTransition{reasonCode: ec2ReasonUserInitiatedShutdown, initiator: ec2InitiatorUser, at: at}
	switch a.ActionID {
	case fisActionStopInstances:
		ec2, ids, err := r.liveTargetIDs(a)
		if err == nil && len(ids) > 0 {
			a.Stopped, err = ec2.stopInternally(r.reqCtx(name, "stop"), ids, userInitiated)
		}
		if err != nil {
			return fisFailureText(err)
		}
		if d, ok := parseFISDuration(a.Parameters["startInstancesAfterDuration"]); ok {
			due = at.Add(d)
		}
	case fisActionTerminateInstances:
		ec2, ids, err := r.liveTargetIDs(a)
		if err == nil && len(ids) > 0 {
			err = ec2.terminateInternally(r.reqCtx(name, "terminate"), ids, true, userInitiated)
		}
		if err != nil {
			return fisFailureText(err)
		}
	case fisActionSpotInterruptions:
		if d, ok := parseFISDuration(a.Parameters["durationBeforeInterruption"]); ok {
			due = at.Add(d)
		}
	default:
		if d, ok := parseFISDuration(a.Parameters["duration"]); ok {
			due = at.Add(d)
		}
	}
	a.DueAt = fisTimePtr(due)
	if !due.After(at) {
		return r.finishAction(name, a, at)
	}
	return ""
}

// finishAction applies an action's end effect at at and completes it, returning a failure
// reason or "".
func (r *fisRun) finishAction(name string, a *FISExperimentAction, at time.Time) string {
	switch a.ActionID {
	case fisActionStopInstances:
		if _, timed := a.Parameters["startInstancesAfterDuration"]; timed {
			if failure := r.restartInstances(name, a); failure != "" {
				return failure
			}
		}
	case fisActionSpotInterruptions:
		ec2, err := r.p.ec2()
		if err == nil {
			err = ec2.interruptSpotInstances(r.reqCtx(name, "interrupt"), r.targetIDs(a), at)
		}
		if err != nil {
			return fisFailureText(err)
		}
	}
	a.State = FISActionState{Status: fisStatusCompleted, Reason: fisReasonActionCompleted}
	a.EndTime = fisTimePtr(at)
	a.DueAt = nil
	return ""
}

// restartInstances starts the instances a stop-instances action itself stopped — not one that
// was already stopped when the action started, which a caller may have stopped for its own
// reasons. An instance terminated meanwhile fails the action unless completeIfInstancesTerminated
// is true.
func (r *fisRun) restartInstances(name string, a *FISExperimentAction) string {
	ec2, err := r.p.ec2()
	if err != nil {
		return fisFailureText(err)
	}
	skipTerminated := strings.EqualFold(a.Parameters["completeIfInstancesTerminated"], "true")
	var ids []string
	for _, id := range a.Stopped {
		inst, err := ec2.loadInstanceForInternal(r.e.AccountID, r.e.Region, id)
		if err != nil {
			return fisFailureText(err)
		}
		if inst == nil || inst.State.Name == ec2StateTerminated {
			if skipTerminated {
				continue
			}
			return "Instance " + id + " was terminated and cannot be started."
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return ""
	}
	if _, err := ec2.startInternally(r.reqCtx(name, "start"), ids); err != nil {
		return fisFailureText(err)
	}
	return ""
}

// end sets the experiment's final state.
func (r *fisRun) end(status, reason string, at time.Time) {
	r.e.State = FISActionState{Status: status, Reason: reason}
	r.e.EndTime = fisTimePtr(at)
}

// settle ends the experiment with status, stopping its running actions and cancelling its
// pending ones.
func (r *fisRun) settle(status, reason string, at time.Time) {
	for _, name := range r.e.actionNames() {
		a := r.e.Actions[name]
		switch a.State.Status {
		case fisStatusRunning:
			a.State = FISActionState{Status: fisStatusStopped, Reason: fisReasonActionStopped}
			a.EndTime = fisTimePtr(at)
		case fisStatusPending, fisStatusInitiating:
			a.State = FISActionState{Status: fisStatusCancelled, Reason: fisReasonActionCanceled}
		}
		a.DueAt = nil
	}
	r.end(status, reason, at)
}

// finishSeeded ends the experiment with a seeded outcome at r.now. A seeded completion completes
// every action that has not ended; a seeded failure fails the running ones and cancels the
// pending ones.
func (r *fisRun) finishSeeded(seed *fisSeededResult) {
	if seed.Status == fisStatusCompleted {
		for _, name := range r.e.actionNames() {
			a := r.e.Actions[name]
			if fisActionTerminal(a.State.Status) {
				continue
			}
			if a.StartTime == nil {
				a.StartTime = fisTimePtr(r.now)
			}
			a.State = FISActionState{Status: fisStatusCompleted, Reason: fisReasonActionCompleted}
			a.EndTime = fisTimePtr(r.now)
			a.DueAt = nil
		}
		reason := seed.Reason
		if reason == "" {
			reason = fisReasonCompleted
		}
		r.end(fisStatusCompleted, reason, r.now)
		return
	}
	reason := seed.Reason
	if reason == "" {
		reason = fisReasonSeededFailure
	}
	for _, name := range r.e.actionNames() {
		a := r.e.Actions[name]
		if a.State.Status == fisStatusRunning {
			a.State = FISActionState{Status: fisStatusFailed, Reason: reason}
			a.EndTime = fisTimePtr(r.now)
			a.DueAt = nil
		}
	}
	r.settle(fisStatusFailed, reason, r.now)
}

// nextDue is when the experiment next has something to do: its earliest pending start or
// running end, or now when nothing is scheduled.
func (r *fisRun) nextDue() time.Time {
	if _, at, ok := r.nextEvent(); ok {
		return at
	}
	return r.now
}

// watched reports whether the experiment has an alarm stop condition to read.
func (r *fisRun) watched() bool {
	for _, c := range r.e.StopConditions {
		if c.Source == "aws:cloudwatch:alarm" {
			return true
		}
	}
	return false
}

// fisAlarmStatePattern finds the StateValue of the alarm a DescribeAlarms answer carries.
var fisAlarmStatePattern = regexp.MustCompile(`<StateValue>([A-Z_]+)</StateValue>`)

// stopConditionAlarmed reports whether any alarm stop condition's alarm is in ALARM, and a
// failure reason when an alarm cannot be read at all. The alarm is read through CloudWatch's own
// DescribeAlarms, routed without being recorded: it is a read, and a replay re-derives it at the
// same step. It is read in the experiment's account and Region, which validation requires the
// alarm's ARN to name; an ARN naming any other is skipped.
//
// An alarm that does not exist is not in ALARM — DescribeAlarms answers it with an empty list —
// and neither is one CloudWatch refuses with a client error, which is logged: "If a stop
// condition is triggered during an experiment, AWS FIS stops the experiment" (Stop conditions
// for AWS FIS), and such an alarm has triggered nothing. A read substrate itself cannot make — no
// CloudWatch plugin registered, a state-store failure, a 5xx — fails the experiment with a
// reason instead: the guard it was started with cannot be evaluated, and returning the error
// would stall every later observation of it. Both readings are substrate's.
func (r *fisRun) stopConditionAlarmed() (bool, string) {
	for i, c := range r.e.StopConditions {
		if c.Source != "aws:cloudwatch:alarm" {
			continue
		}
		ref, ok := parseFISAlarmARN(c.Value)
		if !ok || ref.accountID != r.e.AccountID || ref.region != r.e.Region {
			continue
		}
		unreadable := "The stop condition alarm " + c.Value + " could not be read"
		if r.p.registry == nil {
			return false, unreadable + ": CloudWatch is not available."
		}
		resp, err := r.p.registry.RouteRequest(r.reqCtx("stop-condition", strconv.Itoa(i)), &AWSRequest{
			Service: monitoringNamespace, Operation: "DescribeAlarms", Headers: map[string]string{},
			Params: map[string]string{"Action": "DescribeAlarms", "AlarmNames.member.1": ref.name},
		})
		var awsErr *AWSError
		switch {
		case err != nil && errors.As(err, &awsErr) && awsErr.HTTPStatus < http.StatusInternalServerError:
			r.p.logger.Warn("fis: stop-condition alarm refused; treated as not in ALARM",
				"experiment", r.e.ID, "alarm", c.Value, "err", err)
			continue
		case err != nil:
			return false, unreadable + ": " + fisFailureText(err)
		case resp == nil:
			return false, unreadable + "."
		}
		if m := fisAlarmStatePattern.FindSubmatch(resp.Body); m != nil && string(m[1]) == "ALARM" {
			return true, ""
		}
	}
	return false, ""
}

// loadActiveEntries reads the active set.
func (p *FISPlugin) loadActiveEntries() (map[string]fisActiveEntry, error) {
	active := map[string]fisActiveEntry{}
	if _, err := p.loadJSON(fisActiveKey, &active); err != nil {
		return nil, err
	}
	return active, nil
}

// setActive records the active-set entry for the experiment at key, removing it when entry is
// nil. An unchanged entry writes nothing, so observing an experiment that has nothing due
// writes no state.
func (p *FISPlugin) setActive(key string, entry *fisActiveEntry) error {
	active, err := p.loadActiveEntries()
	if err != nil {
		return err
	}
	prior, had := active[key]
	switch {
	case entry == nil && !had:
		return nil
	case entry == nil:
		delete(active, key)
	case had && prior.Due.Equal(entry.Due) && prior.Watch == entry.Watch && prior.TemplateID == entry.TemplateID:
		return nil
	default:
		active[key] = fisActiveEntry{Due: entry.Due.UTC(), Watch: entry.Watch, TemplateID: entry.TemplateID}
	}
	if len(active) == 0 {
		if err := p.state.Delete(context.Background(), fisNamespace, fisActiveKey); err != nil {
			return fmt.Errorf("fis clear active set: %w", err)
		}
		return nil
	}
	return p.putJSON(fisActiveKey, active)
}
