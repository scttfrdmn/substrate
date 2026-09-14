package emulator

import (
	"context"
	"fmt"
	"math/rand/v2" // nosemgrep
	"sync"
	"time"
)

// ReplayEngine replays recorded [Event] streams deterministically. It is the
// engine behind Substrate's time-travel debugging: load a recorded session,
// jump to any event, step forward or backward, and inspect service state at
// every point in time.
type ReplayEngine struct {
	mu             sync.RWMutex
	eventStore     *EventStore
	stateManager   StateManager
	timeController *TimeController
	registry       *PluginRegistry
	config         ReplayConfig
	logger         Logger

	// currentReplay is the in-progress replay session, if any.
	currentReplay *ActiveReplay

	// rng is the seeded random source for deterministic replay.
	// It is nil when config.RandomSeed is zero (unseeded).
	rng *rand.Rand
}

// ReplayConfig controls the behavior of a [ReplayEngine].
type ReplayConfig struct {
	// SpeedMultiplier scales event timing during replay.
	// 1.0 replays at real time; 0 replays instantly.
	SpeedMultiplier float64

	// StopOnError halts replay when an event fails.
	StopOnError bool

	// ValidateState enables before/after state hash comparison on each event.
	ValidateState bool

	// UseSnapshots fast-forwards replay using the nearest stored snapshot.
	UseSnapshots bool

	// RandomSeed is the RNG seed used for deterministic replay.
	// Zero disables seeded randomness.
	RandomSeed int64
}

// ActiveReplay represents a replay session in progress.
type ActiveReplay struct {
	// ID uniquely identifies this replay run.
	ID string

	// StreamID is the event stream being replayed.
	StreamID string

	// StartTime is when this replay session began.
	StartTime time.Time

	// Events is the ordered list of events to replay.
	Events []*Event

	// Position is the index of the next event to process.
	Position int

	// Paused is true when the replay is suspended at a breakpoint.
	Paused bool

	// Breakpoints maps event positions to true; replay pauses at each.
	Breakpoints map[int]bool

	// StateSnapshots maps event positions to serialized state, used by
	// [ReplayEngine.StepBackward] and [ReplayEngine.JumpToEvent].
	StateSnapshots map[int][]byte

	// Results accumulates outcome metrics.
	Results *ReplayResults
}

// ReplayResults holds the outcome of a completed replay run.
//
// The three event counters partition the stream: every event a replay reaches is
// counted in exactly one of SuccessEvents, FailedEvents and SkippedEvents, and a
// run that reached the end of the stream satisfies
// SuccessEvents + FailedEvents + SkippedEvents == TotalEvents. That was not true
// before #833: a skipped event incremented SkippedEvents *and* SuccessEvents,
// because the skip returned no error and the driver read a nil error as a
// success. So a stream recorded without bodies — the default, see
// [EventStoreConfig.IncludeBodies] — reported every one of its events as a
// successful replay while executing none of them.
//
// SuccessEvents means "re-executed without returning an error", **not** "matched
// the recording". A successful event may still have diverged; whether the replay
// reproduced the run is [ReplayResults.Differences] together with StateValid.
type ReplayResults struct {
	// TotalEvents is the number of events in the stream.
	TotalEvents int

	// SuccessEvents is the number of events that were re-executed and returned
	// no error. It does not imply the replay matched the recording; see
	// Differences and StateValid.
	SuccessEvents int

	// FailedEvents is the number of events that produced an error during replay.
	FailedEvents int

	// SkippedEvents is the number of events that could not be re-executed
	// because the recorded event carries no request. An event stream recorded
	// without [EventStoreConfig.IncludeBodies] carries no request on any event,
	// so every event is skipped and nothing is verified.
	SkippedEvents int

	// Duration is the wall-clock time taken for the replay run.
	Duration time.Duration

	// Differences lists divergences between original and replayed responses.
	Differences []*EventDifference

	// StateValid reports whether all state hash checks passed.
	StateValid bool

	// StateErrors contains descriptions of any state hash mismatches.
	StateErrors []string
}

// EventDifference records a single divergence between an original event and
// its replay counterpart.
type EventDifference struct {
	// EventID is the ID of the diverging event.
	EventID string

	// Sequence is the event's position in the stream.
	Sequence int64

	// Field names the response or state field that diverged.
	Field string

	// Expected is the value recorded in the original event.
	Expected interface{}

	// Actual is the value produced during replay.
	Actual interface{}

	// Significance is "minor", "major", or "critical".
	Significance string
}

// NewReplayEngine creates a ReplayEngine wired to the given dependencies.
func NewReplayEngine(
	eventStore *EventStore,
	stateManager StateManager,
	timeController *TimeController,
	registry *PluginRegistry,
	config ReplayConfig,
	logger Logger,
) *ReplayEngine {
	return &ReplayEngine{
		eventStore:     eventStore,
		stateManager:   stateManager,
		timeController: timeController,
		registry:       registry,
		config:         config,
		logger:         logger,
	}
}

// Replay replays all events in streamID and returns an outcome report.
// Events are re-executed through the plugin registry; responses are compared
// with the originals and differences are recorded in [ReplayResults].
func (r *ReplayEngine) Replay(ctx context.Context, streamID string) (*ReplayResults, error) {
	r.mu.Lock()

	events, err := r.eventStore.GetStream(ctx, streamID)
	if err != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("get stream: %w", err)
	}

	if len(events) == 0 {
		r.mu.Unlock()
		return nil, fmt.Errorf("no events in stream: %s", streamID)
	}

	replay := &ActiveReplay{
		ID:             generateReplayID(),
		StreamID:       streamID,
		StartTime:      time.Now(),
		Events:         events,
		Position:       0,
		Breakpoints:    make(map[int]bool),
		StateSnapshots: make(map[int][]byte),
		Results: &ReplayResults{
			TotalEvents: len(events),
			Differences: make([]*EventDifference, 0),
			StateErrors: make([]string, 0),
			StateValid:  true,
		},
	}

	r.currentReplay = replay
	r.mu.Unlock()

	r.logger.Info("starting replay",
		"replay_id", replay.ID,
		"stream_id", streamID,
		"events", len(events),
	)

	if r.config.UseSnapshots {
		if err := r.loadFromSnapshot(ctx, streamID); err != nil {
			r.logger.Warn("snapshot load failed, starting from empty state", "error", err)
		}
	} else {
		if err := r.resetState(ctx); err != nil {
			return nil, fmt.Errorf("reset state: %w", err)
		}
	}

	if r.config.RandomSeed != 0 {
		r.rng = rand.New(rand.NewPCG(uint64(r.config.RandomSeed), 0)) //nolint:gosec
	}

	start := time.Now()

	for replay.Position < len(replay.Events) {
		if replay.Paused {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if replay.Breakpoints[replay.Position] {
			replay.Paused = true
			r.logger.Info("hit breakpoint", "position", replay.Position)
			continue
		}

		event := replay.Events[replay.Position]

		// executed is what distinguishes a success from a skip. Both return a nil
		// error, so a driver that reads only the error counts a skipped event as a
		// successful replay — which is what #833 fixed here.
		executed, err := r.replayEvent(ctx, event, replay)
		if err != nil {
			r.logger.Error("event replay failed",
				"event_id", event.ID,
				"position", replay.Position,
				"error", err,
			)
			replay.Results.FailedEvents++
			if r.config.StopOnError {
				break
			}
		} else if executed {
			replay.Results.SuccessEvents++
		}

		replay.Position++

		if r.config.SpeedMultiplier > 0 && replay.Position < len(replay.Events) {
			original := replay.Events[replay.Position].Timestamp.Sub(event.Timestamp)
			time.Sleep(time.Duration(float64(original) * r.config.SpeedMultiplier))
		}
	}

	replay.Results.Duration = time.Since(start)

	// skipped is reported because it is the number that tells a caller whether the
	// replay verified anything: total=N success=N reads as a clean run, and
	// total=N skipped=N is the same stream with no request bodies recorded.
	r.logger.Info("replay complete",
		"replay_id", replay.ID,
		"total", replay.Results.TotalEvents,
		"success", replay.Results.SuccessEvents,
		"failed", replay.Results.FailedEvents,
		"skipped", replay.Results.SkippedEvents,
		"differences", len(replay.Results.Differences),
		"duration", replay.Results.Duration,
	)

	return replay.Results, nil
}

// replayEvent re-executes a single event through the plugin registry and
// compares the result against the original.
//
// The bool reports whether the event was actually re-executed. It is separate
// from the error because a skip is neither a success nor a failure, and a caller
// that reads only the error cannot tell the two apart: both return nil. That is
// how a stream recorded without request bodies reported every event as a
// successful replay while executing none of them (#833).
func (r *ReplayEngine) replayEvent(ctx context.Context, event *Event, replay *ActiveReplay) (bool, error) {
	if event.Request == nil {
		replay.Results.SkippedEvents++
		return false, nil
	}

	if r.timeController != nil {
		r.timeController.SetTime(event.Timestamp)
	}

	reqCtx := &RequestContext{
		RequestID: event.ID,
		AccountID: event.AccountID,
		Region:    event.Region,
		Timestamp: event.Timestamp,
		Metadata: map[string]interface{}{
			"stream_id": event.StreamID,
			"replay_id": replay.ID,
			"replaying": true,
		},
	}

	if r.config.ValidateState && event.StateHashBefore != "" {
		if actual := r.computeStateHash(ctx); actual != event.StateHashBefore {
			replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
				EventID:      event.ID,
				Sequence:     event.Sequence,
				Field:        "state_hash_before",
				Expected:     event.StateHashBefore,
				Actual:       actual,
				Significance: "critical",
			})
			replay.Results.StateValid = false
		}
	}

	resp, err := r.registry.RouteRequest(reqCtx, event.Request)
	if err != nil {
		if event.Error == "" {
			replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
				EventID:      event.ID,
				Sequence:     event.Sequence,
				Field:        "error",
				Expected:     nil,
				Actual:       err.Error(),
				Significance: "critical",
			})
		} else if err.Error() != event.Error {
			replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
				EventID:      event.ID,
				Sequence:     event.Sequence,
				Field:        "error_message",
				Expected:     event.Error,
				Actual:       err.Error(),
				Significance: "major",
			})
		}
		return true, err
	}

	// A recorded refusal that replays as a success. Nothing caught this before
	// #833: the error comparison above runs only when the *replay* errored, and
	// the status comparison below cannot see it either, because a pre-plugin
	// refusal records with a nil response (server.go:801, :813, :829, :849) and so
	// event.Response is nil for exactly the events that were refused. A recorded
	// 403 replaying as a 200 was reported as no difference at all — and, before
	// the counter fix, as a success.
	//
	// Critical rather than major, and matching the reverse case above: a replay
	// that grants what the recording denied is the divergence most likely to make
	// a passing test meaningless, since it is the one that lets a request through.
	if event.Error != "" {
		replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
			EventID:      event.ID,
			Sequence:     event.Sequence,
			Field:        "error",
			Expected:     event.Error,
			Actual:       nil,
			Significance: "critical",
		})
	}

	if event.Response != nil && resp != nil && resp.StatusCode != event.Response.StatusCode {
		replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
			EventID:      event.ID,
			Sequence:     event.Sequence,
			Field:        "status_code",
			Expected:     event.Response.StatusCode,
			Actual:       resp.StatusCode,
			Significance: "major",
		})
	}
	// TODO(#817): deep-compare response bodies.

	if r.config.ValidateState && event.StateHashAfter != "" {
		if actual := r.computeStateHash(ctx); actual != event.StateHashAfter {
			replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
				EventID:      event.ID,
				Sequence:     event.Sequence,
				Field:        "state_hash_after",
				Expected:     event.StateHashAfter,
				Actual:       actual,
				Significance: "critical",
			})
			replay.Results.StateValid = false
		}
	}

	return true, nil
}

// StepForward re-executes the next event and advances the position.
// Returns the event that was replayed.
//
// It records differences but does not maintain [ReplayResults]' success and
// failure counters, which belong to a whole-stream [ReplayEngine.Replay]; the
// counter partition documented on ReplayResults holds for that run, not for a
// hand-stepped session.
func (r *ReplayEngine) StepForward(ctx context.Context) (*Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay == nil {
		return nil, fmt.Errorf("no active replay")
	}

	if r.currentReplay.Position >= len(r.currentReplay.Events) {
		return nil, fmt.Errorf("end of replay")
	}

	event := r.currentReplay.Events[r.currentReplay.Position]

	if _, err := r.replayEvent(ctx, event, r.currentReplay); err != nil {
		return nil, err
	}

	r.currentReplay.Position++

	return event, nil
}

// StepBackward restores state to the previous event position.
// A stored [ActiveReplay.StateSnapshots] entry is required for the target
// position; otherwise an error is returned.
func (r *ReplayEngine) StepBackward(ctx context.Context) (*Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay == nil {
		return nil, fmt.Errorf("no active replay")
	}

	if r.currentReplay.Position == 0 {
		return nil, fmt.Errorf("already at beginning of replay")
	}

	r.currentReplay.Position--

	snapshot, ok := r.currentReplay.StateSnapshots[r.currentReplay.Position]
	if !ok {
		return nil, fmt.Errorf("no state snapshot at position %d; replay from beginning to build snapshots", r.currentReplay.Position)
	}

	if err := r.restoreState(ctx, snapshot); err != nil {
		return nil, fmt.Errorf("restore state: %w", err)
	}

	return r.currentReplay.Events[r.currentReplay.Position], nil
}

// JumpToEvent repositions the replay to sequence. If a snapshot exists before
// sequence it is used to fast-forward; otherwise replay restarts from zero.
func (r *ReplayEngine) JumpToEvent(ctx context.Context, sequence int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay == nil {
		return fmt.Errorf("no active replay")
	}

	targetPos := int(sequence)
	if targetPos < 0 || targetPos >= len(r.currentReplay.Events) {
		return fmt.Errorf("sequence %d out of range [0, %d)", sequence, len(r.currentReplay.Events))
	}

	// Find the latest snapshot that precedes targetPos.
	nearestSnapshot := -1
	for pos := range r.currentReplay.StateSnapshots {
		if pos <= targetPos && pos > nearestSnapshot {
			nearestSnapshot = pos
		}
	}

	if nearestSnapshot >= 0 {
		if err := r.restoreState(ctx, r.currentReplay.StateSnapshots[nearestSnapshot]); err != nil {
			return fmt.Errorf("restore state from snapshot: %w", err)
		}
		r.currentReplay.Position = nearestSnapshot
	} else {
		if err := r.resetState(ctx); err != nil {
			return fmt.Errorf("reset state: %w", err)
		}
		r.currentReplay.Position = 0
	}

	for r.currentReplay.Position < targetPos {
		event := r.currentReplay.Events[r.currentReplay.Position]
		if _, err := r.replayEvent(ctx, event, r.currentReplay); err != nil {
			return fmt.Errorf("replay failed at position %d: %w", r.currentReplay.Position, err)
		}
		r.currentReplay.Position++
	}

	return nil
}

// SetBreakpoint registers position as a breakpoint. Replay pauses before
// executing the event at that position.
func (r *ReplayEngine) SetBreakpoint(position int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay != nil {
		r.currentReplay.Breakpoints[position] = true
	}
}

// ClearBreakpoint removes the breakpoint at position.
func (r *ReplayEngine) ClearBreakpoint(position int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay != nil {
		delete(r.currentReplay.Breakpoints, position)
	}
}

// Pause suspends the current replay. It is a no-op when no replay is active.
func (r *ReplayEngine) Pause() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay != nil {
		r.currentReplay.Paused = true
	}
}

// Resume continues a paused replay. It is a no-op when no replay is active.
func (r *ReplayEngine) Resume() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay != nil {
		r.currentReplay.Paused = false
	}
}

// InspectState returns all key-value pairs in namespace at the current replay
// position.
func (r *ReplayEngine) InspectState(ctx context.Context, namespace string) (map[string][]byte, error) {
	keys, err := r.stateManager.List(ctx, namespace, "")
	if err != nil {
		return nil, err
	}

	state := make(map[string][]byte, len(keys))
	for _, key := range keys {
		val, err := r.stateManager.Get(ctx, namespace, key)
		if err == nil {
			state[key] = val
		}
	}

	return state, nil
}

// GetCurrentEvent returns the event at the current replay position without
// advancing the position.
func (r *ReplayEngine) GetCurrentEvent() (*Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.currentReplay == nil {
		return nil, fmt.Errorf("no active replay")
	}

	if r.currentReplay.Position >= len(r.currentReplay.Events) {
		return nil, fmt.Errorf("past end of replay")
	}

	return r.currentReplay.Events[r.currentReplay.Position], nil
}

// RecordingSession tracks a named test-recording session.
type RecordingSession struct {
	// StreamID is the event stream created for this session.
	StreamID string

	// StartTime is when recording began.
	StartTime time.Time

	// EventStore is the store receiving events for this session.
	EventStore *EventStore
}

// StartRecording begins a new recording session named name.
// All events tagged with the returned [RecordingSession.StreamID] are
// grouped together for later replay.
func (r *ReplayEngine) StartRecording(_ context.Context, name string) (*RecordingSession, error) {
	streamID := fmt.Sprintf("recording-%s-%d", name, time.Now().UnixNano())

	session := &RecordingSession{
		StreamID:   streamID,
		StartTime:  time.Now(),
		EventStore: r.eventStore,
	}

	r.logger.Info("recording started", "stream_id", streamID, "name", name)

	return session, nil
}

// StopRecording ends session and returns the number of recorded events.
func (r *ReplayEngine) StopRecording(ctx context.Context, session *RecordingSession) (int, error) {
	events, err := r.eventStore.GetStream(ctx, session.StreamID)
	if err != nil {
		return 0, err
	}

	r.logger.Info("recording stopped",
		"stream_id", session.StreamID,
		"events", len(events),
		"duration", time.Since(session.StartTime),
	)

	return len(events), nil
}

// generateReplayID produces a unique replay run identifier.
func generateReplayID() string {
	return fmt.Sprintf("replay-%d", time.Now().UnixNano())
}

// resetState clears all emulator state in preparation for a fresh replay.
// When the state manager implements [SnapshotableStateManager] its Reset method
// is called; otherwise this is a no-op.
func (r *ReplayEngine) resetState(ctx context.Context) error {
	if r.stateManager == nil {
		return nil
	}
	if ss, ok := r.stateManager.(SnapshotableStateManager); ok {
		return ss.Reset(ctx)
	}
	return nil
}

// loadFromSnapshot restores state from the most recent snapshot for streamID.
func (r *ReplayEngine) loadFromSnapshot(ctx context.Context, streamID string) error {
	snapshot, err := r.eventStore.GetLatestSnapshot(ctx, streamID)
	if err != nil {
		return err
	}
	return r.restoreState(ctx, snapshot.State)
}

// restoreState deserializes state bytes back into the state manager.
// When the state manager implements [SnapshotableStateManager] its Restore
// method is called; otherwise this is a no-op.
func (r *ReplayEngine) restoreState(ctx context.Context, data []byte) error {
	if r.stateManager == nil {
		return nil
	}
	if ss, ok := r.stateManager.(SnapshotableStateManager); ok {
		return ss.Restore(ctx, data)
	}
	return nil
}

// computeStateHash returns a SHA-256 hash of the current state manager contents.
// Returns an empty string when no state manager is set or it does not implement
// [SnapshotableStateManager].
func (r *ReplayEngine) computeStateHash(ctx context.Context) string {
	return stateSnapshotHash(ctx, r.stateManager)
}

// stateSnapshotHash returns a SHA-256 hash of sm's entire contents, or an empty
// string when sm is nil, does not implement [SnapshotableStateManager], or cannot
// be snapshotted.
//
// It is one function because a recorded hash and a replayed hash have to be
// produced the same way to be comparable at all: [Server] calls it to fill
// [Event.StateHashBefore] and [Event.StateHashAfter], and [ReplayEngine] calls it
// to compare against them. Two implementations of "hash the state" would be two
// answers, and the comparison would report a difference for every event.
//
// The hash is deterministic for [MemoryStateManager]: Snapshot marshals a map,
// and encoding/json sorts map keys. It is *not* invariant across a recording and
// its replay for a stream containing a create, because substrate mints most
// identifiers from crypto/rand and the minted value ends up in the state key or
// the record — see #856. A reported mismatch there is a real divergence, not a
// defect in this function.
//
// An empty string means "not available", and both call sites treat it as "do not
// compare" rather than as a hash of empty state — a nil state manager and an
// empty one must not look alike.
func stateSnapshotHash(ctx context.Context, sm StateManager) string {
	if sm == nil {
		return ""
	}
	ss, ok := sm.(SnapshotableStateManager)
	if !ok {
		return ""
	}
	data, err := ss.Snapshot(ctx)
	if err != nil {
		return ""
	}
	return hashBytes(data)
}

// RandFloat64 returns a pseudo-random float64 in [0, 1).
// When a seed was configured via [ReplayConfig.RandomSeed] the result is
// deterministic; otherwise it falls back to the global random source.
func (r *ReplayEngine) RandFloat64() float64 {
	if r.rng != nil {
		return r.rng.Float64()
	}
	return rand.Float64()
}

// RandInt64 returns a non-negative pseudo-random int64.
// When a seed was configured via [ReplayConfig.RandomSeed] the result is
// deterministic; otherwise it falls back to the global random source.
func (r *ReplayEngine) RandInt64() int64 {
	if r.rng != nil {
		return r.rng.Int64()
	}
	return rand.Int64()
}
