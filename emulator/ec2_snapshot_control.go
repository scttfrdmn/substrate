package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// ec2SnapCtrlNamespace is the state namespace for EC2 snapshot control-plane (seed) data.
const ec2SnapCtrlNamespace = "ec2-snap-ctrl"

// ec2SnapshotProgression is a seeded pending → terminal snapshot progression (#715).
//
// A snapshot in substrate is born completed, so the one loop callers actually write around
// this API — poll DescribeSnapshots until status is completed, which is what CDK's custom
// resources, Terraform's aws_ebs_snapshot and the CLI's own snapshot-completed waiter all do —
// exits on its first iteration and never exercises the path it exists for. A consumer whose
// polling is broken, or which treats error as retryable forever, passes against substrate and
// fails against AWS. Seeding is what makes the transition observable.
//
// # Why the progression is counted in observations rather than measured as a duration
//
// [TimeController.Now] advances with wall time from its baseline, so a duration seed would
// expire partway through a test and make every "still pending" assertion depend on how long
// the rest of the test took — which no test here may be. A count of observations is exactly
// reproducible, and it is the same choice [sqsConsistencySeed] records for the same reason.
// It is also the more useful unit: "the next two polls see pending" is what a test of a poll
// loop actually wants to say.
//
// # What a seed governs
//
// A seed governs what an observation *reports*; it never rewrites the snapshot record. So
// clearing the seed — or POST /v1/state/reset, which clears the whole namespace — makes every
// snapshot read completed again, and an unseeded snapshot is untouched.
type ec2SnapshotProgression struct {
	// SnapshotID is the snapshot the seed applies to, or "*" for any snapshot.
	//
	// A snapshot created *after* a "*" seed is in place is born in State, which is what
	// makes the create → poll → completed sequence testable end to end. An ID-scoped seed
	// necessarily names a snapshot that already exists, since the ID does not exist before
	// the create.
	SnapshotID string `json:"snapshotId"`

	// PendingObservations is the number of DescribeSnapshots observations that report State
	// before the snapshot reaches FinalState. Zero means the snapshot is in FinalState from
	// the start, which is how error is seeded as an immediate terminal outcome.
	PendingObservations int `json:"pendingObservations"`

	// State is what the snapshot reports while the countdown runs. Defaults to "pending".
	State string `json:"state"`

	// FinalState is what it reports once the countdown is exhausted. Defaults to
	// "completed".
	FinalState string `json:"finalState"`

	// StateMessage is the statusMessage rendered alongside a terminal state other than
	// completed.
	//
	// It is optional and has no default, because AWS's own member is narrow: "if a snapshot
	// copy operation fails (for example, if the proper AWS KMS permissions are not obtained)
	// this field displays error state details to help you diagnose why the error occurred",
	// and "this parameter is only returned by DescribeSnapshots". So substrate renders it
	// only where AWS does, and only when a seed supplies the text — inventing a diagnostic
	// would be indistinguishable to a caller from an observation.
	StateMessage string `json:"stateMessage"`
}

// ec2SnapshotStates is the snapshot-state enumeration, which AWS publishes identically as the
// Valid Values of Snapshot.status and of SnapshotInfo.state.
//
// A seed naming anything else is refused at the endpoint rather than stored, because a state
// outside the enumeration is one no SDK can map and no consumer can branch on — the seed would
// look accepted and produce a response the caller's own model rejects.
//
// Two of the five are seedable but not otherwise reachable, and that is deliberate. AWS's
// DescribeSnapshots *filter* documents only three values ("status - The status of the snapshot
// (pending | completed | error)") where the member documents five, because recoverable means
// the snapshot is in the Recycle Bin — which real DescribeSnapshots does not return at all,
// ListSnapshotsInRecycleBin does. Substrate models neither the Recycle Bin nor that operation,
// so the two states exist here only as something a seed can make a consumer's branch see. The
// member's enumeration is the one followed, per #671: what the API model states.
var ec2SnapshotStates = []string{"pending", "completed", "error", "recoverable", "recovering"}

// ec2SnapCtrlKeyPrefix is the key prefix for every progression seed, used to clear all.
const ec2SnapCtrlKeyPrefix = "status:"

// ec2SnapObservedPrefix is the key prefix for the per-snapshot observation counters.
const ec2SnapObservedPrefix = "observed:"

// ec2SnapCtrlKey returns the state key for a progression seed. Snapshot-scoped seeds use
// "status:{id}"; the wildcard uses "status:*".
func ec2SnapCtrlKey(snapshotID string) string {
	if snapshotID == "" {
		snapshotID = "*"
	}
	return ec2SnapCtrlKeyPrefix + snapshotID
}

// ec2SnapObservedKey returns the state key holding how many observations one snapshot has
// already had.
//
// The count is keyed by snapshot ID even when the seed that governs it is the "*" wildcard,
// which is what keeps one DescribeSnapshots call over five snapshots from burning five
// observations off a single shared countdown. That is the hazard #582 records for the SQS
// seed, met here by separating the specification (shared, read-only) from the progress
// through it (per snapshot).
func ec2SnapObservedKey(snapshotID string) string {
	return ec2SnapObservedPrefix + snapshotID
}

// ec2SnapshotObserved is the per-snapshot countdown position.
type ec2SnapshotObserved struct {
	// Observations is how many observations the snapshot has already had.
	Observations int `json:"observations"`
}

// ec2SnapshotObservation is what one observation of a snapshot reports: the state and
// progress to render, and a statusMessage when the seed supplies one for a terminal state
// other than completed.
type ec2SnapshotObservation struct {
	State        string
	Progress     string
	StateMessage string
}

// ec2UnseededObservation is what a snapshot with no seed against it reports: the record's own
// state — "completed", for every snapshot substrate writes — at 100%.
//
// It is the default the whole of the existing suite pins, and keeping it a named function is
// what makes "no seed changes nothing" checkable in one place rather than inferred from two
// early returns.
func ec2UnseededObservation(snap EC2Snapshot) ec2SnapshotObservation {
	return ec2SnapshotObservation{State: snap.State, Progress: "100%"}
}

// resolveSnapshotProgression returns the seeded progression for one snapshot, matching the
// exact ID first and then the "*" wildcard, or (nil, nil) when none applies.
func (p *EC2Plugin) resolveSnapshotProgression(snapshotID string) (*ec2SnapshotProgression, error) {
	goCtx := context.Background()
	for _, key := range []string{ec2SnapCtrlKey(snapshotID), ec2SnapCtrlKey("*")} {
		data, err := p.state.Get(goCtx, ec2SnapCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("ec2 resolveSnapshotProgression get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed ec2SnapshotProgression
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("ec2 resolveSnapshotProgression unmarshal: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// peekSnapshotStatus reports what the snapshot would observe right now, without advancing its
// countdown.
//
// Its callers need the state without consuming an observation, because none of them is a
// poll: CreateSnapshot and CreateSnapshots, which report the state a snapshot is born in;
// CreateVolume, which refuses a snapshot that is not usable yet; and the block-device-mapping
// rule that refuses the same snapshot in a launch or a RegisterImage (#732), which is also the
// one caller that can reach here more than once per request — one mapping each. Counting any
// of those against a budget the caller meant to spend on polling would make
// "pendingObservations: 2" mean two polls in one test and one in another, and in the mapping
// case would make it depend on how many volumes the launch declared.
//
// DeleteSnapshot needs neither peek nor observation: AWS permits deleting a snapshot in
// progress, so its answer does not depend on the state at all — see [EC2Plugin.deleteSnapshot].
func (p *EC2Plugin) peekSnapshotStatus(snap EC2Snapshot) (ec2SnapshotObservation, error) {
	seed, err := p.resolveSnapshotProgression(snap.SnapshotID)
	if err != nil {
		return ec2SnapshotObservation{}, err
	}
	if seed == nil {
		return ec2UnseededObservation(snap), nil
	}
	seen, err := p.snapshotObservations(snap.SnapshotID)
	if err != nil {
		return ec2SnapshotObservation{}, err
	}
	return seed.observation(seen), nil
}

// observeSnapshotStatus reports what this observation of the snapshot sees and advances its
// countdown by one.
//
// It takes one dedicated mutex rather than one per snapshot ID. Advancing the counter is a
// read-modify-write and [StateManager] offers no compare-and-swap — Get/Put/Delete/List only,
// with [MemoryStateManager] last-write-wins — so two concurrent describes of the same snapshot
// could both read a count of 1, both write 2, and consume one observation twice, making a test
// that asserts "the third poll completes" flake. One lock means there is no lock order to get
// wrong, and seed consumption is a harness-frequency operation, so it costs nothing.
// [SQSPlugin.consumeQueueMiss] and [S3Plugin.consumeConditionalConflict] hold the same shape.
//
// The guarantee is bounded the way those are: it is process-local, which covers substrate's
// single-process topology and would not hold across two emulator processes sharing one state
// backend.
func (p *EC2Plugin) observeSnapshotStatus(snap EC2Snapshot) (ec2SnapshotObservation, error) {
	p.seedMu.Lock()
	defer p.seedMu.Unlock()

	seed, err := p.resolveSnapshotProgression(snap.SnapshotID)
	if err != nil {
		return ec2SnapshotObservation{}, err
	}
	if seed == nil {
		return ec2UnseededObservation(snap), nil
	}
	seen, err := p.snapshotObservations(snap.SnapshotID)
	if err != nil {
		return ec2SnapshotObservation{}, err
	}
	observation := seed.observation(seen)

	// The counter is advanced only while it can still change an answer. Past the end of the
	// countdown the snapshot is terminal, so a further Put would rewrite state on every
	// describe of a completed snapshot for no observable difference — and every one of those
	// writes lands in the event log a replay has to walk.
	if seen < seed.PendingObservations {
		data, marshalErr := json.Marshal(ec2SnapshotObserved{Observations: seen + 1})
		if marshalErr != nil {
			return ec2SnapshotObservation{}, fmt.Errorf("ec2 observeSnapshotStatus marshal: %w", marshalErr)
		}
		if putErr := p.state.Put(context.Background(), ec2SnapCtrlNamespace,
			ec2SnapObservedKey(snap.SnapshotID), data); putErr != nil {
			return ec2SnapshotObservation{}, fmt.Errorf("ec2 observeSnapshotStatus put: %w", putErr)
		}
	}
	return observation, nil
}

// snapshotObservations returns how many observations one snapshot has already had, zero when
// it has had none.
func (p *EC2Plugin) snapshotObservations(snapshotID string) (int, error) {
	data, err := p.state.Get(context.Background(), ec2SnapCtrlNamespace, ec2SnapObservedKey(snapshotID))
	if err != nil {
		return 0, fmt.Errorf("ec2 snapshotObservations get: %w", err)
	}
	if data == nil {
		return 0, nil
	}
	var observed ec2SnapshotObserved
	if err := json.Unmarshal(data, &observed); err != nil {
		return 0, fmt.Errorf("ec2 snapshotObservations unmarshal: %w", err)
	}
	return observed.Observations, nil
}

// observation returns what the seen-th observation of a seeded snapshot reports, counting from
// zero.
//
// # The progress ramp
//
// progress is the fraction of the countdown already spent, so a seed of four pending
// observations reports 0%, 25%, 50% and 75% and then 100%. A single value held for the whole
// pending phase would satisfy "below 100%", but a caller's poll loop very often logs or asserts
// on a rising percentage, and a ramp is what lets that be tested.
//
// AWS documents the member only as "the progress of the snapshot, as a percentage", and the
// trailing % comes from its filter text ("for example, 80%") and from its own examples rather
// than from the member's description. So the *schedule* is substrate's. It is deterministic,
// which is the property that matters here; a consumer must no more assert on an exact
// intermediate value than it may against AWS, whose CreateSnapshot sample response implausibly
// shows a freshly created snapshot at 60%.
//
// progress reaches 100% when the countdown is exhausted **whatever the terminal state is**,
// rather than freezing below it for a state other than completed. AWS's one observable
// data point says so: its restore-snapshot-from-recycle-bin example shows "Progress": "100%"
// beside "State": "recovering". Progress therefore measures how far the progression ran, not
// whether it succeeded — which is exactly the distinction a consumer that polls progress
// instead of status gets wrong, and now a test can catch that.
func (seed ec2SnapshotProgression) observation(seen int) ec2SnapshotObservation {
	pendingState := seed.State
	if pendingState == "" {
		pendingState = "pending"
	}
	finalState := seed.FinalState
	if finalState == "" {
		finalState = "completed"
	}

	if total := seed.PendingObservations; seen < total {
		return ec2SnapshotObservation{
			State:    pendingState,
			Progress: fmt.Sprintf("%d%%", seen*100/total),
		}
	}
	return ec2SnapshotObservation{
		State:        finalState,
		Progress:     "100%",
		StateMessage: seed.StateMessage,
	}
}

// handleEC2SeedSnapshotStatus handles POST /v1/ec2/snapshot-status. It seeds how many
// DescribeSnapshots observations report a non-terminal state before the snapshot reaches its
// final one, for snapshots matching the given ID (default "*"). Body:
// {"snapshotId","pendingObservations","state","finalState","stateMessage"}.
//
// Seeding resets the countdown for every snapshot the seed governs, so a test that seeds twice
// gets two full progressions rather than the remainder of the first.
func (s *Server) handleEC2SeedSnapshotStatus(w http.ResponseWriter, r *http.Request) {
	var seed ec2SnapshotProgression
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.PendingObservations < 0 {
		http.Error(w, `{"error":"pendingObservations must be >= 0"}`, http.StatusBadRequest)
		return
	}
	for _, state := range []string{seed.State, seed.FinalState} {
		if state != "" && !slices.Contains(ec2SnapshotStates, state) {
			http.Error(w, fmt.Sprintf(`{"error":"unknown snapshot state %q, want one of %v"}`,
				state, ec2SnapshotStates), http.StatusBadRequest)
			return
		}
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), ec2SnapCtrlNamespace, ec2SnapCtrlKey(seed.SnapshotID), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.clearSnapshotObservations(r, seed.SnapshotID); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "snapshotId": ec2SnapCtrlKey(seed.SnapshotID)})
}

// handleEC2ClearSnapshotStatus handles DELETE /v1/ec2/snapshot-status. With ?snapshotId=... it
// removes that seed; without it removes all. Either way it also clears the countdown position
// of the snapshots the seed governed, so a later seed starts from the beginning.
func (s *Server) handleEC2ClearSnapshotStatus(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("snapshotId"); id != "" {
		if err := s.state.Delete(r.Context(), ec2SnapCtrlNamespace, ec2SnapCtrlKey(id)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if err := s.clearSnapshotObservations(r, id); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	for _, prefix := range []string{ec2SnapCtrlKeyPrefix, ec2SnapObservedPrefix} {
		keys, err := s.state.List(r.Context(), ec2SnapCtrlNamespace, prefix)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		for _, k := range keys {
			if err := s.state.Delete(r.Context(), ec2SnapCtrlNamespace, k); err != nil {
				http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
				return
			}
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}

// clearSnapshotObservations resets the countdown position of the snapshots a seed governs: one
// snapshot for an ID-scoped seed, every snapshot for the "*" wildcard.
//
// The wildcard case has to sweep the prefix rather than delete one key, because the counters
// are per snapshot by design — see [ec2SnapObservedKey] — so there is no single key holding a
// wildcard seed's progress.
func (s *Server) clearSnapshotObservations(r *http.Request, snapshotID string) error {
	if snapshotID != "" && snapshotID != "*" {
		if err := s.state.Delete(r.Context(), ec2SnapCtrlNamespace, ec2SnapObservedKey(snapshotID)); err != nil {
			return fmt.Errorf("ec2 clear snapshot observations %s: %w", snapshotID, err)
		}
		return nil
	}
	keys, err := s.state.List(r.Context(), ec2SnapCtrlNamespace, ec2SnapObservedPrefix)
	if err != nil {
		return fmt.Errorf("ec2 list snapshot observations: %w", err)
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), ec2SnapCtrlNamespace, k); err != nil {
			return fmt.Errorf("ec2 clear snapshot observations %s: %w", k, err)
		}
	}
	return nil
}
