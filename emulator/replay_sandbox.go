package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ReplaySandbox is a fresh emulator built to replay a recorded stream into, so a
// replay never resets or writes to the state of the emulator that recorded it.
//
// It registers the default plugins from the recording server's configuration over its
// own [MemoryStateManager], clock and registry, and wires the engine with a control
// plane over that same emulator, so a recorded seed is re-applied (#1140). All four
// pre-plugin controllers are built from that configuration, so authorization, quota,
// consistency and faults are re-decided as the recording decided them (#833, #1601).
// Its plugins record nothing: they are given a disabled store.
//
// This is the path behind GET /v1/debug/events/{seq}/state, `substrate debug state-at`,
// [TestServer.Replay] and [DebugSession].
type ReplaySandbox struct {
	state    *MemoryStateManager
	registry *PluginRegistry
	engine   *ReplayEngine

	// stateAtMu serializes [ReplaySandbox.StateAt], which moves the engine's one replay
	// and then reads the state there.
	stateAtMu sync.Mutex
}

// ReplaySandboxOption configures [NewReplaySandbox].
type ReplaySandboxOption func(*replaySandboxSettings)

// replaySandboxSettings collects what a [ReplaySandboxOption] sets.
type replaySandboxSettings struct {
	fault  *FaultController
	replay *ReplayConfig

	// fromServer is set by [withSandboxGatesOf]: the gates are the recording server's,
	// not its configuration file's.
	fromServer     bool
	quotaCfg       QuotaConfig
	consistencyCfg ConsistencyConfig
}

// WithSandboxFaults re-decides faults from a copy of f's armed rules and seed, rather
// than from the configuration's fault section. Pass the recording server's controller
// when its rules were armed over HTTP. f itself is not advanced.
func WithSandboxFaults(f *FaultController) ReplaySandboxOption {
	return func(s *replaySandboxSettings) { s.fault = f.cloneArmed() }
}

// withSandboxGatesOf re-decides quota, consistency and faults as the controllers in o
// did, rather than as the configuration describes them. A server built in process,
// [TestServer] among them, can run without a controller its configuration enables, and a
// nil controller here is a disabled one.
func withSandboxGatesOf(o ServerOptions) ReplaySandboxOption {
	return func(s *replaySandboxSettings) {
		s.fromServer = true
		s.fault = o.Fault.cloneArmed()
		s.quotaCfg = o.Quota.config()
		s.consistencyCfg = o.Consistency.config()
	}
}

// WithSandboxReplayConfig sets the engine's [ReplayConfig]. Without it the sandbox
// uses the configuration's replay section.
func WithSandboxReplayConfig(rc ReplayConfig) ReplaySandboxOption {
	return func(s *replaySandboxSettings) { s.replay = &rc }
}

// NewReplaySandbox builds a sandbox that replays streams from store. cfg is the
// configuration the recording server ran with; nil means [DefaultConfig]. Its quotas,
// consistency and fault sections decide how those gates are re-decided, so a sandbox
// for a server that ran without one of those controllers must be given a configuration
// that turns it off; [TestServer.ReplaySandbox] does this for a test server. The Docker
// Lambda and RDS executors are always off in a sandbox, because a replay must not start
// containers.
func NewReplaySandbox(
	ctx context.Context,
	cfg *Config,
	store *EventStore,
	logger Logger,
	opts ...ReplaySandboxOption,
) (*ReplaySandbox, error) {
	var settings replaySandboxSettings
	for _, opt := range opts {
		opt(&settings)
	}

	sandboxCfg := DefaultConfig()
	if cfg != nil {
		copied := *cfg
		sandboxCfg = &copied
	}
	sandboxCfg.Lambda.DockerEnabled = false
	sandboxCfg.RDS.Engine = "stub"

	state := NewMemoryStateManager()
	tc := NewTimeController(time.Time{})
	registry := NewPluginRegistry()
	auth := NewAuthController(state, logger, WithAuthTimeController(tc))
	pluginStore := NewEventStore(EventStoreConfig{Enabled: false})

	if err := RegisterDefaultPlugins(ctx, registry, state, tc, logger, pluginStore, sandboxCfg,
		WithPluginAuth(auth)); err != nil {
		return nil, fmt.Errorf("replay sandbox: register plugins: %w", err)
	}

	fault := settings.fault
	if fault == nil {
		if settings.fromServer {
			fault = NewFaultController(FaultConfig{}, 0)
		} else {
			fault = NewFaultController(sandboxCfg.Fault.ToFaultConfig(), sandboxCfg.Fault.Seed)
		}
	}
	replayCfg := sandboxCfg.Replay.ToReplayConfig()
	if settings.replay != nil {
		replayCfg = *settings.replay
	}

	// Quota and consistency are re-decided on replay, from the recording's configuration,
	// as `substrate replay` builds them.
	quotaCfg, consistencyCfg := settings.quotaCfg, settings.consistencyCfg
	if !settings.fromServer {
		quotaCfg = sandboxCfg.Quotas.ToQuotaConfig()
		var err error
		if consistencyCfg, err = sandboxCfg.Consistency.ToConsistencyConfig(); err != nil {
			return nil, fmt.Errorf("replay sandbox: consistency config: %w", err)
		}
	}
	quota := NewQuotaController(quotaCfg, tc)
	consistency, err := NewConsistencyController(consistencyCfg, tc)
	if err != nil {
		return nil, fmt.Errorf("replay sandbox: consistency controller: %w", err)
	}
	pipeline := ReplayPipeline{Auth: auth, Quota: quota, Consistency: consistency, Fault: fault}

	controlPlane := NewServer(*sandboxCfg, registry, pluginStore, state, tc, logger,
		ServerOptions{Auth: auth, Quota: quota, Consistency: consistency, Fault: fault})

	return &ReplaySandbox{
		state:    state,
		registry: registry,
		engine: NewReplayEngine(store, state, tc, registry, replayCfg, logger,
			WithControlPlaneHandler(controlPlane),
			WithReplayPipeline(pipeline)),
	}, nil
}

// Engine returns the sandbox's [ReplayEngine], for [ReplayEngine.Replay],
// [ReplayEngine.JumpToEvent], [ReplayEngine.StepBackward] and the rest.
func (s *ReplaySandbox) Engine() *ReplayEngine { return s.engine }

// StateManager returns the sandbox's own state, which the engine replays into.
func (s *ReplaySandbox) StateManager() StateManager { return s.state }

// Registry returns the sandbox's plugin registry, for reading what a plugin keeps
// outside the state, such as an S3 object's bytes, at the replay's position.
func (s *ReplaySandbox) Registry() *PluginRegistry { return s.registry }

// StateAt is the state a recorded stream reached before one event, and what replaying
// to it found.
type StateAt struct {
	// StreamID is the stream that was replayed.
	StreamID string `json:"stream"`

	// Sequence is the [Event.Sequence] the state was taken before.
	Sequence int64 `json:"sequence"`

	// EventsApplied is how many of the stream's events ran before Sequence.
	EventsApplied int `json:"events_applied"`

	// State is the [MemoryStateManager] snapshot: namespace, then key, then the
	// base64-encoded value.
	State json.RawMessage `json:"state"`

	// Differences lists where replaying those events diverged from the recording. An
	// empty list means the state is the one the live emulator had.
	Differences []*EventDifference `json:"differences"`

	// FailedEvents is how many of those events returned an error when replayed. A
	// recorded error that replays as the same error is counted here and is not a
	// difference.
	FailedEvents int `json:"failed_events"`

	// Skipped counts the events that were not re-executed, by reason.
	Skipped map[ReplaySkipReason]int `json:"skipped,omitempty"`
}

// StateAt replays streamID up to, and not including, the first event whose
// [Event.Sequence] is sequence or greater, and returns the state there. Sequence 0 is
// the state before any event; a sequence past the stream's last event is its final
// state.
//
// Successive calls on one sandbox move the same replay, so asking for a later point
// replays only the events in between. Results are reported once per event however often
// it is re-run. StateAt is safe for concurrent use: calls run one at a time. Driving
// [ReplaySandbox.Engine] from another goroutine during a call is not.
func (s *ReplaySandbox) StateAt(ctx context.Context, streamID string, sequence int64) (*StateAt, error) {
	// One StateAt at a time: the state is read at the position this call moved to, and a
	// concurrent call moving the same replay would hand back the other call's state.
	s.stateAtMu.Lock()
	defer s.stateAtMu.Unlock()

	replay, err := s.activate(ctx, streamID)
	if err != nil {
		return nil, err
	}
	target := positionOfSequence(replay.Events, sequence)
	if err := s.engine.JumpToEvent(ctx, target); err != nil {
		return nil, err
	}
	snapshot, err := s.state.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot replayed state: %w", err)
	}

	at := &StateAt{
		StreamID:      streamID,
		Sequence:      sequence,
		EventsApplied: target,
		State:         snapshot,
		Differences:   make([]*EventDifference, 0),
	}
	// Read under the engine's lock, which its own navigation methods hold while they
	// write these, so a caller driving Engine directly cannot tear them.
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	for _, d := range replay.Results.Differences {
		if d.Sequence < sequence {
			copied := *d
			at.Differences = append(at.Differences, &copied)
		}
	}
	for _, outcome := range replay.outcomes[:target] {
		switch {
		case outcome.skipped != "":
			if at.Skipped == nil {
				at.Skipped = make(map[ReplaySkipReason]int)
			}
			at.Skipped[outcome.skipped]++
		case outcome.failed:
			at.FailedEvents++
		}
	}
	return at, nil
}

// activate makes streamID the engine's active replay, loading it at its start unless it
// is already active and up to date. A stream that has gained events since it was loaded,
// as a sandbox held across live traffic sees, is loaded again: the cached event list
// would answer a later sequence with the state at its own last event.
func (s *ReplaySandbox) activate(ctx context.Context, streamID string) (*ActiveReplay, error) {
	events, err := s.engine.eventStore.GetStream(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("get stream %q: %w", streamID, err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no events in stream %q", streamID)
	}
	s.engine.mu.RLock()
	current := s.engine.currentReplay
	s.engine.mu.RUnlock()
	if current != nil && current.StreamID == streamID && len(current.Events) == len(events) &&
		current.Events[len(current.Events)-1].Sequence == events[len(events)-1].Sequence {
		return current, nil
	}
	return s.engine.begin(ctx, events)
}
