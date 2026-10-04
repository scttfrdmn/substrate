package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// A seeded instance-state progression, and the published transient states (#514).
//
// Every state change substrate applies reached its terminal state in the same request, so
// `pending`, `stopping` and `shutting-down` were states [EC2InstanceState]'s own doc comment
// enumerated and no code path could produce. The loop callers actually write around this API —
// run/start/stop, then poll DescribeInstances until the state settles, which is what the CLI's
// `instance-running` waiter, Terraform's `aws_instance` and CDK's own custom resources all do —
// exited on its first iteration and never exercised the path it exists for.
//
// Two halves, and only the second is seeded.
//
// # The immediate response reports the transient state unconditionally
//
// Both operation pages' sample responses publish it: `API_StartInstances` shows `currentState`
// 0/`pending` against `previousState` 80/`stopped`, and `API_StopInstances` shows `currentState`
// 64/`stopping` against `previousState` 16/`running`. Substrate reported the settled state in
// both, which is a published-response divergence that has nothing to do with seeding — so it is
// corrected for every caller rather than behind a seed. A test that does not seed sees the
// transient state once, in the operation's own response, and the settled state on its first
// describe, which is AWS with an instantaneous transition.
//
// # The transient state is derived, not chosen
//
// Unlike a snapshot's state — a free choice from a five-value enumeration, which is why
// [ec2SnapshotProgression] carries `state` and `finalState` members — the state on the way to a
// target is fixed by the published lifecycle. `ec2-instance-lifecycle.html` states each one:
// `pending` is "preparing to enter the running state … when it is launched or when it is started
// after being in the stopped state", `stopping` is "preparing to be stopped", `shutting-down` is
// "preparing to be terminated". So [ec2InstanceProgression] carries a count and nothing else, and
// [ec2TransientStateFor] is the published map rather than a seedable field.
//
// # What a seed governs
//
// A seed governs what an observation *reports*; it never rewrites the instance record. That is
// [ec2SnapshotProgression]'s rule and it is load-bearing here in a way it is not there: because
// the stored state is always the settled one, a `StartInstances` that follows a `StopInstances`
// still sees `stopped` and succeeds, so seeding a progression cannot break a caller's own
// sequence. It is also why no operation can ever observe a transient state, which is what makes
// "an operation illegal from a transient state" vacuous by construction rather than a refusal
// substrate had to invent a citation for — neither `API_StartInstances` nor `API_StopInstances`
// publishes any state precondition at all (both Errors sections are empty).
//
// # Why observations and not a duration
//
// The same reason [ec2SnapshotProgression] gives: [TimeController.Now] advances with wall time,
// so a duration seed would make every "still pending" assertion depend on how long the rest of
// the test took. A count of observations is exactly reproducible.
//
// Reproducible across a replay too, and that took a second mechanism. A seed is written through
// the control plane rather than as an AWS request, and [ReplayEngine.Replay] resets the whole
// [StateManager] first, so a replay of a seeded stream used to answer the *unseeded* sequence.
// [Server.recordControlPlaneWrites] records the write as an event of its own and the replay
// re-issues it in position, which restores the countdown from zero rather than carrying over the
// position the recording reached (#1140). A replay handed no control-plane handler still answers
// the unseeded sequence, and that is worth having: it is the sharpest available test of the rule
// that a seed governs what an observation reports and never rewrites the instance record — see
// TestEC2_InstanceState_AReplayReproducesTheRecordsOwnStates.

// ec2InstStateNamespace is the state namespace for EC2 instance-state control-plane (seed) data.
//
// A namespace of its own rather than a second key space inside [ec2SnapCtrlNamespace], so
// `DELETE /v1/ec2/snapshot-status` with no query cannot sweep an instance seed along with the
// snapshot ones — it lists by prefix within a namespace, and both progressions use "status:".
const ec2InstStateNamespace = "ec2-inst-state-ctrl"

// ec2InstanceProgressions is the instance kind's seeded countdown, in [ec2InstStateNamespace].
//
// The counters are keyed per instance even under the "*" wildcard, which is what keeps one
// DescribeInstances call over five instances from burning five observations off a single shared
// countdown — the hazard #582 records for the SQS seed. See [progression].
var ec2InstanceProgressions = newProgression[ec2InstanceProgression](ec2InstStateNamespace, "instanceId", "transientObservations")

// ec2InstanceProgression is a seeded transient → settled instance-state progression.
//
// One member, for the reason the file comment gives: the transient state is derived from the
// target by the published lifecycle, so there is nothing else for a seed to say.
type ec2InstanceProgression struct {
	// InstanceID is the instance the seed applies to, or "*" for any instance.
	//
	// An instance launched *after* a "*" seed is in place is born reporting `pending`, which is
	// what makes the launch → poll → running sequence testable end to end. An ID-scoped seed
	// necessarily names an instance that already exists, since the ID does not exist before the
	// launch.
	InstanceID string `json:"instanceId"`

	// TransientObservations is the number of observations that report the transient state before
	// the instance reports the state its record already holds. Zero — the default, and what every
	// unseeded instance has — means the transition is instantaneous, which is the behavior the
	// whole existing suite pins.
	TransientObservations int `json:"transientObservations"`
}

// progressionID implements [progressionSeed].
func (seed ec2InstanceProgression) progressionID() string { return seed.InstanceID }

// progressionObservations implements [progressionSeed].
func (seed ec2InstanceProgression) progressionObservations() int { return seed.TransientObservations }

// validateProgression implements [progressionSeed]. An instance seed names no state — the
// transient state is derived from the record by [ec2TransientStateFor] — so there is nothing to
// refuse beyond the negative count the handler already refuses.
func (seed ec2InstanceProgression) validateProgression() error { return nil }

// ec2TransientStateFor returns the state AWS publishes on the way to a settled one, and false
// for a settled state no published transition leads through.
//
// `running` maps to `pending` from both of its published entrances — "when it is launched or when
// it is started after being in the stopped state" — so one entry covers `RunInstances` and
// `StartInstances` alike. A state that is itself transient returns false: it is not a target, and
// a record holding one would mean the "a seed never rewrites the record" rule had been broken.
func ec2TransientStateFor(settled EC2InstanceState) (EC2InstanceState, bool) {
	switch settled.Name {
	case ec2StateRunning:
		return EC2InstanceState{Code: 0, Name: "pending"}, true
	case ec2StateStopped:
		return EC2InstanceState{Code: 64, Name: "stopping"}, true
	case ec2StateTerminated:
		return EC2InstanceState{Code: 32, Name: "shutting-down"}, true
	default:
		return EC2InstanceState{}, false
	}
}

// The three settled states substrate's write sites produce, named because each is spelled at a
// write site, at a transition and in a filter, and a typo in any one of those is silent.
const (
	ec2StateRunning    = "running"
	ec2StateStopped    = "stopped"
	ec2StateTerminated = "terminated"
)

// ec2ReportedTransition returns the state a state-changing operation's own response reports as
// the instance's `currentState`, given the state it just settled the record to.
//
// Unconditional — not seed-dependent — because both operation pages' sample responses publish the
// transient state there: `API_StartInstances` shows `currentState` 0/`pending` beside
// `previousState` 80/`stopped`, and `API_StopInstances` shows 64/`stopping` beside 16/`running`.
// A settled state with no published transition into it is returned unchanged, so this is safe to
// apply at any write site.
func ec2ReportedTransition(settled EC2InstanceState) EC2InstanceState {
	if transient, ok := ec2TransientStateFor(settled); ok {
		return transient
	}
	return settled
}

// observeInstanceState reports the state this observation of the instance sees and advances its
// countdown by one.
//
// It takes [EC2Plugin.seedMu] for the reason [EC2Plugin.observeSnapshotStatus] records: advancing
// the counter is a read-modify-write and [StateManager] offers no compare-and-swap, so two
// concurrent describes could both read a count of 1, both write 2, and consume one observation
// twice — making "the third poll is running" flake. The guarantee is process-local, which covers
// substrate's single-process topology.
//
// The instance's own state is returned unchanged on any state error rather than propagated,
// which is the one divergence from the snapshot precedent and is deliberate: this sits inside two
// describe loops that already `continue` past an unreadable record, and a seed-store failure must
// not turn a describe of a hundred instances into an error about none of them. An unseeded
// instance — every instance in every existing fixture — takes the first return and touches no
// state at all.
func (p *EC2Plugin) observeInstanceState(inst EC2Instance) EC2InstanceState {
	p.seedMu.Lock()
	defer p.seedMu.Unlock()

	ctx := context.Background()
	seed, err := ec2InstanceProgressions.resolve(ctx, p.state, inst.InstanceID)
	if err != nil || seed == nil || seed.TransientObservations <= 0 {
		return inst.State
	}
	transient, ok := ec2TransientStateFor(inst.State)
	if !ok {
		return inst.State
	}
	seen, err := ec2InstanceProgressions.observations(ctx, p.state, inst.InstanceID)
	if err != nil {
		return inst.State
	}
	if seen >= seed.TransientObservations {
		return inst.State
	}

	// Advanced only while it can still change an answer, per [progression]: past the end of the
	// countdown a further Put would rewrite state on every describe of a settled instance for no
	// observable difference, and every one of those writes lands in the event log a replay has to
	// walk.
	if err := ec2InstanceProgressions.advance(ctx, p.state, inst.InstanceID, seen); err != nil {
		return inst.State
	}
	return transient
}

// resetInstanceObservations restarts one instance's countdown, and is called by every operation
// that changes an instance's state.
//
// Without it the countdown would run from the moment the seed was written rather than from the
// transition, so a test that launched an instance, let two describes settle it, and then stopped
// it would see the stop as instantaneous — the opposite of what a progression is for. It is also
// what makes a seed reusable across several transitions of one instance, which is the sequence a
// stop/start waiter test walks.
//
// The error is returned rather than swallowed because its callers already handle one; a failure
// here would silently report a settled state for a transition the caller asked to be observable.
func (p *EC2Plugin) resetInstanceObservations(instanceID string) error {
	if err := ec2InstanceProgressions.reset(context.Background(), p.state, instanceID); err != nil {
		return fmt.Errorf("ec2 resetInstanceObservations: %w", err)
	}
	return nil
}

// ec2CannotTransitionTerminated returns the refusal for starting or stopping an instance that has
// been terminated.
//
// The code is published in EC2's client-error table for specific actions, whose description is
// the general rule this is an instance of: "The instance is in an incorrect state for the
// requested action." The *condition* is published on `ec2-instance-lifecycle.html`, whose state
// table says a `terminated` instance "has been permanently deleted and cannot be started" — so
// the start refusal rests on that sentence directly. The stop refusal rests on "permanently
// deleted" alone: no page found states a stop precondition, and refusing is still the reading
// substrate takes rather than letting a stop resurrect a deleted instance into `stopped`, from
// which it would then start. Neither operation page publishes any error of its own, so the
// message text is substrate's; the verb is interpolated because it is the one fact a caller
// cannot recover from the code.
func ec2CannotTransitionTerminated(verb string) *AWSError {
	return &AWSError{
		Code: "IncorrectInstanceState",
		Message: "The instance is in an incorrect state for the requested action: an instance in the '" +
			ec2StateTerminated + "' state has been permanently deleted and cannot be " + verb,
		HTTPStatus: http.StatusBadRequest,
	}
}

// handleEC2SeedInstanceState handles POST /v1/ec2/instance-state. It seeds how many observations
// report the published transient state before an instance reports the state its record holds, for
// instances matching the given ID (default "*"). Body: {"instanceId","transientObservations"}.
//
// Seeding resets the countdown for every instance the seed governs, so a test that seeds twice
// gets two full progressions rather than the remainder of the first.
func (s *Server) handleEC2SeedInstanceState(w http.ResponseWriter, r *http.Request) {
	ec2InstanceProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleEC2ClearInstanceState handles DELETE /v1/ec2/instance-state. With ?instanceId=... it
// removes that seed; without it removes all. Either way it also clears the countdown position of
// the instances the seed governed, so a later seed starts from the beginning.
func (s *Server) handleEC2ClearInstanceState(w http.ResponseWriter, r *http.Request) {
	ec2InstanceProgressions.serveClear(w, r, s.state, s.logger)
}
