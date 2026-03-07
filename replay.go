package substrate

import (
	"context"
	"fmt"
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
}

// ReplayConfig controls the behaviour of a [ReplayEngine].
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

	// StateSnapshots maps event positions to serialised state, used by
	// [ReplayEngine.StepBackward] and [ReplayEngine.JumpToEvent].
	StateSnapshots map[int][]byte

	// Results accumulates outcome metrics.
	Results *ReplayResults
}

// ReplayResults holds the outcome of a completed replay run.
type ReplayResults struct {
	// TotalEvents is the number of events in the stream.
	TotalEvents int

	// SuccessEvents is the number of events that replayed without error.
	SuccessEvents int

	// FailedEvents is the number of events that produced an error during replay.
	FailedEvents int

	// SkippedEvents is the number of events skipped (e.g., missing request body).
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

	// TODO(#6): seed the RNG for deterministic replay when config.RandomSeed != 0.

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

		if err := r.replayEvent(ctx, event, replay); err != nil {
			r.logger.Error("event replay failed",
				"event_id", event.ID,
				"position", replay.Position,
				"error", err,
			)
			replay.Results.FailedEvents++
			if r.config.StopOnError {
				break
			}
		} else {
			replay.Results.SuccessEvents++
		}

		replay.Position++

		if r.config.SpeedMultiplier > 0 && replay.Position < len(replay.Events) {
			original := replay.Events[replay.Position].Timestamp.Sub(event.Timestamp)
			time.Sleep(time.Duration(float64(original) * r.config.SpeedMultiplier))
		}
	}

	replay.Results.Duration = time.Since(start)

	r.logger.Info("replay complete",
		"replay_id", replay.ID,
		"total", replay.Results.TotalEvents,
		"success", replay.Results.SuccessEvents,
		"failed", replay.Results.FailedEvents,
		"duration", replay.Results.Duration,
	)

	return replay.Results, nil
}

// replayEvent re-executes a single event through the plugin registry and
// compares the result against the original.
func (r *ReplayEngine) replayEvent(ctx context.Context, event *Event, replay *ActiveReplay) error {
	if event.Request == nil {
		replay.Results.SkippedEvents++
		return nil
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
		return err
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
	// TODO(#6): deep-compare response bodies.

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

	return nil
}

// StepForward re-executes the next event and advances the position.
// Returns the event that was replayed.
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

	if err := r.replayEvent(ctx, event, r.currentReplay); err != nil {
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
		if err := r.replayEvent(ctx, event, r.currentReplay); err != nil {
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
func (r *ReplayEngine) StartRecording(ctx context.Context, name string) (*RecordingSession, error) {
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
func (r *ReplayEngine) resetState(_ context.Context) error {
	// TODO(#5): clear all state manager namespaces.
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

// restoreState deserialises state bytes back into the state manager.
func (r *ReplayEngine) restoreState(_ context.Context, _ []byte) error {
	// TODO(#5): deserialise state into the state manager.
	return nil
}

// computeStateHash returns a SHA-256 hash of the current state manager contents.
func (r *ReplayEngine) computeStateHash(_ context.Context) string {
	// TODO(#5): implement deterministic state hashing.
	return ""
}
