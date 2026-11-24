package emulator

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
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

	// pipeline holds the pre-plugin controllers a replayed request passes through.
	// Zero-valued unless [WithReplayPipeline] was given, in which case a replay
	// reaches step 5 directly — which is how a recorded 403 replayed as a 200
	// (#833).
	pipeline ReplayPipeline

	// controlPlane is the handler a recorded control-plane write is re-issued
	// against. Nil unless [WithControlPlaneHandler] was given, in which case a
	// recorded seed is skipped and the replay answers the unseeded sequence (#1140).
	controlPlane http.Handler

	// currentReplay is the in-progress replay session, if any.
	currentReplay *ActiveReplay
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

	// StateSnapshots maps a stream position to the serialized [StateManager] contents
	// before the event at that position ran. Once a replay has moved backward, the
	// engine takes one every [EventStoreConfig.SnapshotInterval] events it replays, and
	// [ReplayEngine.JumpToEvent] restores the nearest one at or before its target
	// instead of replaying from the start.
	// They are an optimisation only: a jump with no usable snapshot replays from the
	// beginning and reaches the same state.
	StateSnapshots map[int][]byte

	// Results accumulates outcome metrics.
	Results *ReplayResults

	// pluginSnapshots holds, per position in StateSnapshots, the state plugins keep
	// outside the StateManager (see [replaySnapshotter]). A position is restorable
	// only when it has both.
	pluginSnapshots map[int]map[string]any

	// reached is one past the furthest position whose differences are in Results. An
	// event re-run below it, by a jump or a step after one, is compared into a scratch
	// result, so a difference is reported once however often the event is re-run.
	reached int

	// snapshotting is set by the first backward jump. A replay that only moves forward
	// never restores a snapshot, and a snapshot copies every S3 payload, so none is
	// taken before a caller has shown it navigates backward.
	snapshotting bool

	// parents holds the request ids of the stream's replayable top-level requests. An
	// internal dispatch whose [Event.ParentRequestID] is among them is reproduced by
	// re-running its parent and is skipped; see [isInternalDispatchEvent].
	parents map[string]bool

	// skipInternal is true when the stream holds a top-level request. It decides an
	// internal dispatch recorded without a parent, before #1601.
	skipInternal bool

	// outcomes records, per position, how the event's first replay ended, so a partial
	// replay can report its counts (see [ReplaySandbox.StateAt]).
	outcomes []replayOutcome

	// lastSkip is the reason the event being replayed was skipped, or empty.
	lastSkip ReplaySkipReason
}

// replayOutcome is how one event's replay ended.
type replayOutcome struct {
	skipped ReplaySkipReason
	failed  bool
}

// ReplaySkipReason says why a replay did not re-execute an event.
type ReplaySkipReason string

const (
	// SkipNoRequest is an event recorded without its request, because
	// event_store.include_bodies was off. Nothing about it was verified.
	SkipNoRequest ReplaySkipReason = "no recorded request"

	// SkipBodyOverCap is an event whose request body was over
	// event_store.max_body_bytes and was not stored. Nothing about it was verified.
	SkipBodyOverCap ReplaySkipReason = "request body over max_body_bytes"

	// SkipNoControlPlane is a recorded control-plane write replayed by an engine built
	// without [WithControlPlaneHandler]. The seed was not re-applied.
	SkipNoControlPlane ReplaySkipReason = "control-plane write without a handler"

	// SkipClockDriven is a recorded dispatch of clock-driven work. Replaying the request
	// that triggered it runs the same work again.
	SkipClockDriven ReplaySkipReason = "clock-driven dispatch"

	// SkipInternalDispatch is a CloudFormation or API Gateway internal dispatch whose
	// parent request is in the replayed events. Replaying the parent runs the same
	// dispatch again.
	SkipInternalDispatch ReplaySkipReason = "internal dispatch"

	// SkipUnattributedDispatch is an internal dispatch recorded without its parent's
	// request id, before #1601, in a stream that holds a top-level request. It is
	// skipped because the parent is probably there and would apply it twice, but
	// nothing shows that it is, so the skip is unverified.
	SkipUnattributedDispatch ReplaySkipReason = "internal dispatch with no recorded parent"
)

// Unverified reports whether an event skipped for this reason leaves part of the
// recording unchecked. The clock-driven and internal-dispatch skips do not: their
// parent request reproduces them.
func (r ReplaySkipReason) Unverified() bool {
	return r == SkipNoRequest || r == SkipBodyOverCap || r == SkipNoControlPlane ||
		r == SkipUnattributedDispatch
}

// skip counts event as skipped for reason.
func (a *ActiveReplay) skip(reason ReplaySkipReason) {
	a.lastSkip = reason
	a.Results.SkippedEvents++
	if a.Results.SkipReasons == nil {
		a.Results.SkipReasons = make(map[ReplaySkipReason]int)
	}
	a.Results.SkipReasons[reason]++
}

// Request-ID prefixes of the internal dispatches a parent request makes in process.
const (
	cfnInternalRequestIDPrefix   = "req-cfn-"
	apigwInternalRequestIDPrefix = "req-apigw-"
)

// isInternalDispatchEvent reports whether event records a request the CloudFormation
// deployer or the API Gateway proxy dispatched on a parent request's behalf. Its
// request ID is derived from the parent's, so re-running the parent re-runs it.
func isInternalDispatchEvent(event *Event) bool {
	return strings.HasPrefix(event.RequestID, cfnInternalRequestIDPrefix) ||
		strings.HasPrefix(event.RequestID, apigwInternalRequestIDPrefix)
}

// isTopLevelEvent reports whether ev records a request that arrived from outside,
// rather than a dispatch made in process or a control-plane write.
func isTopLevelEvent(ev *Event) bool {
	return !isInternalDispatchEvent(ev) && !isClockDrivenDispatchEvent(ev) && !isControlPlaneEvent(ev)
}

// replayableParents returns the request ids of the top-level requests in events that a
// replay re-executes, which are the parents whose internal dispatches it reproduces. A
// stream recorded by [Client.Deploy] holds none: its dispatches are replayed by
// executing them. It also reports whether events holds any top-level request, which
// decides a dispatch recorded without a parent.
func replayableParents(events []*Event) (map[string]bool, bool) {
	parents := make(map[string]bool)
	topLevel := false
	for _, ev := range events {
		if !isTopLevelEvent(ev) {
			continue
		}
		topLevel = true
		if ev.Request != nil && ev.RequestBodyOmitted == nil && ev.RequestID != "" {
			parents[ev.RequestID] = true
		}
	}
	return parents, topLevel
}

// internalDispatchSkip returns why event, an internal dispatch, is not executed, or
// false when it must be: its parent is not re-run, so nothing else reproduces it.
func (a *ActiveReplay) internalDispatchSkip(event *Event) (ReplaySkipReason, bool) {
	if event.ParentRequestID != "" {
		return SkipInternalDispatch, a.parents[event.ParentRequestID]
	}
	return SkipUnattributedDispatch, a.skipInternal
}

// ReplayResults holds the outcome of a completed replay run.
//
// The three event counters partition the stream: every event a replay reaches is
// counted in exactly one of SuccessEvents, FailedEvents and SkippedEvents, and a
// run that reached the end of the stream satisfies
// SuccessEvents + FailedEvents + SkippedEvents == TotalEvents. That was not true
// before #833: a skipped event incremented SkippedEvents *and* SuccessEvents,
// because the skip returned no error and the driver read a nil error as a
// success. So a stream recorded without bodies — the default before #1599, see
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

	// SkippedEvents is the number of events that were not re-executed. SkipReasons
	// says why; a reason whose [ReplaySkipReason.Unverified] is true means part of the
	// recording was not checked.
	SkippedEvents int

	// SkipReasons counts SkippedEvents by reason.
	SkipReasons map[ReplaySkipReason]int

	// Duration is the wall-clock time taken for the replay run.
	Duration time.Duration

	// Differences lists divergences between original and replayed responses:
	// the status code, the error, the state hash before and after, and every
	// difference inside the response body, each located by its path within that
	// body (see [bodyDifferenceField]). One event can contribute several.
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
	EventID string `json:"event_id"`

	// Sequence is the event's position in the stream.
	Sequence int64 `json:"sequence"`

	// Operation is the AWS API operation the diverging event invoked.
	//
	// Recorded so that a difference names the call that produced it without the
	// reader having to look the sequence number back up in the stream. EventID
	// carries the operation too, but only as one field of a minted composite
	// (see generateEventID), which is not something a report should be parsing
	// (#817).
	Operation string `json:"operation"`

	// Field names the response or state field that diverged. A body comparison
	// reports "response_body" followed by the path within the body; see
	// [bodyDifferenceField].
	Field string `json:"field"`

	// Expected is the value recorded in the original event.
	Expected interface{} `json:"expected"`

	// Actual is the value produced during replay.
	Actual interface{} `json:"actual"`

	// Significance is "minor", "major", or "critical".
	Significance string `json:"significance"`
}

// ReplayPipeline carries the pre-plugin controllers a replayed request passes
// through, mirroring the four [ServerOptions] fields of the same names so that one
// wiring can hand both a server and a replay of that server's recording the same
// controllers.
//
// Which of the four re-decides a replayed request and which is merely consulted is
// documented on [ReplayEngine.replayEvent]. Any field may be nil, in which case that
// step is skipped — but a nil is a decision to skip, not an absence of one: a replay
// given no auth controller cannot reproduce a recorded 403 through the pipeline, and
// a replay given no fault controller re-executes the request an injected fault failed.
type ReplayPipeline struct {
	// Auth authorizes a replayed request against the principal the event records.
	Auth *AuthController

	// Quota is consulted and exempts every replayed request; see
	// [QuotaController.CheckQuota].
	Quota *QuotaController

	// Consistency is consulted and exempts every replayed request; see
	// [ConsistencyController.CheckRead].
	Consistency *ConsistencyController

	// Fault re-injects from the armed rules, with the controller rewound to its
	// armed state at the start of the replay.
	Fault *FaultController
}

// ReplayEngineOption configures a [ReplayEngine] at construction.
type ReplayEngineOption func(*ReplayEngine)

// WithReplayPipeline gives the engine the controllers a replayed request passes
// through before it reaches a plugin.
//
// It is an option rather than a seventh positional parameter for the same reason
// [NewEventStore] takes options: four controllers, all of them optional, none of them
// meaningful to a caller replaying a stream recorded without them.
func WithReplayPipeline(pipeline ReplayPipeline) ReplayEngineOption {
	return func(r *ReplayEngine) { r.pipeline = pipeline }
}

// WithControlPlaneHandler gives the engine the handler a recorded control-plane
// write is re-issued against, which is how a replay reproduces the seeds the
// recording ran under (#1140).
//
// Pass the [Server] whose stream is being replayed, or a server built over the same
// registry, state manager and clock; [Server.ServeHTTP] needs no [Server.Start].
// Without it a control-plane event is **skipped**, which is reported in
// [ReplayResults.SkippedEvents] and leaves the replay answering the unseeded
// sequence — the behavior every replay had before #1140, now visible in the counters
// instead of silent.
//
// A replay writes no events, so the engine marks the request it synthesizes and
// [Server.recordControlPlaneWrites] declines to record it again.
func WithControlPlaneHandler(handler http.Handler) ReplayEngineOption {
	return func(r *ReplayEngine) { r.controlPlane = handler }
}

// NewReplayEngine creates a ReplayEngine wired to the given dependencies.
func NewReplayEngine(
	eventStore *EventStore,
	stateManager StateManager,
	timeController *TimeController,
	registry *PluginRegistry,
	config ReplayConfig,
	logger Logger,
	opts ...ReplayEngineOption,
) *ReplayEngine {
	r := &ReplayEngine{
		eventStore:     eventStore,
		stateManager:   stateManager,
		timeController: timeController,
		registry:       registry,
		config:         config,
		logger:         logger,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Replay replays all events in streamID and returns an outcome report.
// Events are re-executed through the plugin registry; responses are compared
// with the originals and differences are recorded in [ReplayResults].
func (r *ReplayEngine) Replay(ctx context.Context, streamID string) (*ReplayResults, error) {
	events, err := r.eventStore.GetStream(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no events in stream: %s", streamID)
	}
	return r.ReplayEvents(ctx, events)
}

// ReplayEvents replays events, in order, as one stream. It is [ReplayEngine.Replay]
// for events that are not in the engine's store, such as the ones a generated
// regression fixture embeds (see [GenerateTestFixture]).
func (r *ReplayEngine) ReplayEvents(ctx context.Context, events []*Event) (*ReplayResults, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("no events to replay")
	}
	replay, err := r.begin(ctx, events)
	if err != nil {
		return nil, err
	}
	return r.run(ctx, replay), nil
}

// newReplayResults returns an empty result for a stream of total events.
func newReplayResults(total int) *ReplayResults {
	return &ReplayResults{
		TotalEvents: total,
		Differences: make([]*EventDifference, 0),
		StateErrors: make([]string, 0),
		StateValid:  true,
	}
}

// begin makes events the engine's active replay, positioned before the first event,
// and brings the state to the start of the run.
func (r *ReplayEngine) begin(ctx context.Context, events []*Event) (*ActiveReplay, error) {
	parents, topLevel := replayableParents(events)
	replay := &ActiveReplay{
		ID:              generateReplayID(),
		StreamID:        events[0].StreamID,
		StartTime:       time.Now(),
		Events:          events,
		Breakpoints:     make(map[int]bool),
		StateSnapshots:  make(map[int][]byte),
		pluginSnapshots: make(map[int]map[string]any),
		Results:         newReplayResults(len(events)),
		parents:         parents,
		skipInternal:    topLevel,
		outcomes:        make([]replayOutcome, len(events)),
	}

	r.logger.Info("starting replay",
		"replay_id", replay.ID,
		"stream_id", replay.StreamID,
		"events", len(events),
	)

	if err := r.startOfRun(ctx, replay); err != nil {
		return nil, err
	}
	// Published only once the run can start, so a failed begin leaves no active replay
	// that navigation could move.
	r.mu.Lock()
	r.currentReplay = replay
	r.mu.Unlock()
	return replay, nil
}

// startOfRun brings the emulator to the state a run starts from. A replay begins here,
// and a backward jump with no snapshot to restore returns here.
//
// The state is always reset first: the state manager, what plugins keep on themselves,
// and the pre-plugin controllers. Under [ReplayConfig.UseSnapshots] the stream's stored
// snapshot is then restored on top. A stream with no stored snapshot starts from the
// reset state; a stored snapshot that cannot be restored is an error, because the replay
// would otherwise run from a state that is neither the snapshot nor empty.
func (r *ReplayEngine) startOfRun(ctx context.Context, replay *ActiveReplay) error {
	if err := r.resetState(ctx); err != nil {
		return fmt.Errorf("reset state: %w", err)
	}
	if r.config.UseSnapshots {
		if err := r.loadFromSnapshot(ctx, replay.StreamID); err != nil {
			return fmt.Errorf("load stored snapshot for stream %s: %w", replay.StreamID, err)
		}
	}
	return nil
}

// run replays replay from its position to the end and returns its results.
func (r *ReplayEngine) run(ctx context.Context, replay *ActiveReplay) *ReplayResults {
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
		executed, err := r.advance(ctx, replay)
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

	return replay.Results
}

// advance re-executes the event at the replay's position without moving the position,
// taking a snapshot first when one is due.
//
// An event below [ActiveReplay.reached] has been compared already, so its comparison
// goes to a scratch result and is dropped: re-running it on the way to a jump target
// must not report its differences a second time.
func (r *ReplayEngine) advance(ctx context.Context, replay *ActiveReplay) (bool, error) {
	pos := replay.Position
	r.maybeSnapshot(ctx, replay)
	first := pos >= replay.reached
	if first {
		replay.reached = pos + 1
	} else {
		kept := replay.Results
		replay.Results = newReplayResults(kept.TotalEvents)
		defer func() { replay.Results = kept }()
	}
	replay.lastSkip = ""
	executed, err := r.replayEvent(ctx, replay.Events[pos], replay)
	if first {
		replay.outcomes[pos] = replayOutcome{skipped: replay.lastSkip, failed: err != nil}
	}
	return executed, err
}

// replayEvent re-executes a single event through the pre-plugin pipeline and the
// plugin registry, and compares the result against the original.
//
// The bool reports whether the event was actually re-executed. It is separate
// from the error because a skip is neither a success nor a failure, and a caller
// that reads only the error cannot tell the two apart: both return nil. That is
// how a stream recorded without request bodies reported every event as a
// successful replay while executing none of them (#833).
//
// # What a replay re-executes, and what it does not
//
// A refusal before plugin dispatch is part of what a recorded run means, so four of
// [Server.handleAWSRequest]'s steps run here too, through the same [prePluginGates]
// the live path uses. The other five read the live *http.Request or the replay's own
// configuration and are skipped. An internal dispatch (see [isInternalDispatchEvent])
// passes only step 2, as it did live. Each decision, in pipeline order:
//
//   - **Step 1.1, the rpc-v2-cbor/target conflict** — skipped. It is a property of the
//     HTTP request's headers and path, and no event carries either.
//   - **Step 1.4, the region allow-list** — skipped. It is a property of the
//     configuration the *replay* was started with rather than of the recorded run, so
//     refusing here would report a divergence caused by a config file.
//   - **Step 1.5, credential resolution** — replaced. The recorded access key resolves
//     to nothing after a replay, because [IAMPlugin] mints a different one; see
//     [replayPrincipal] for why the recorded principal ARN is the input instead, and
//     what that leaves for #856.
//   - **Step 1.6, SigV4 verification** — skipped, and not implementable. The canonical
//     request cannot be reconstructed from an event: the raw query string is never
//     recorded, and the body is replaced with a re-encoding of the parsed form for
//     iam/sts/sqs/sns (see [Server.handleAWSRequest] step 1). A replay also
//     re-executes a request whose signature was verified once already.
//   - **Step 1.65, presigned-URL expiry** — skipped, same class: it is a statement
//     about the wall-clock moment a URL was signed, which the recording settled.
//   - **Step 2, authorization** — re-decided, against the principal restored from
//     [Event.Principal].
//   - **Step 3, quota** — re-decided, from a controller rewound at the start of the
//     replay, at the recorded clock reading, so a recorded throttle replays as the same
//     throttle (#1601). It exempted a replay before #1601, which replayed a throttled
//     write as a success.
//   - **Step 4, consistency** — re-decided the same way. A replayed write opens its
//     propagation window (step 7, which a replay runs too) and a replayed read inside one
//     is refused.
//   - **Step 4.5, fault injection** — re-decided from the armed rules, with the
//     controller rewound at the start of the replay; see
//     [FaultController.rewindForReplay]. A latency rule's delay is *not* slept: the
//     recorded [Event.Duration] is what the live run took, and no replay may consume
//     wall-clock time.
//
// A replay writes no events, so a refusal here is compared against the recording
// rather than recorded, and it counts in [ReplayResults.FailedEvents] — which means
// "returned an error", not "diverged", exactly as it does for a recorded plugin error.
//
// # What is compared once the request has been re-executed
//
// Four comparisons, each recording its own [EventDifference]: the state hash before
// the event and after it (when [ReplayConfig.ValidateState] is set and the recording
// carries one), the error, the status code, and the response body — the last through
// [ReplayEngine.recordBodyDifferences], which names the path within the body that
// diverged (#817). Response *headers* are not compared; a difference in one is not
// something any plugin can produce independently of the body or the status today,
// and comparing them would report the Content-Length of a body already reported.
func (r *ReplayEngine) replayEvent(ctx context.Context, event *Event, replay *ActiveReplay) (bool, error) {
	if event.RequestBodyOmitted != nil {
		replay.skip(SkipBodyOverCap)
		r.chargeSkippedGates(event)
		return false, nil
	}
	if event.Request == nil {
		replay.skip(SkipNoRequest)
		r.chargeSkippedGates(event)
		return false, nil
	}

	// A recorded dispatch of clock-driven work is reproduced by replaying the request that
	// triggered it, which runs the same work at the same clock reading. Executing the
	// recorded copy as well would apply each dispatch twice (#1292).
	if isClockDrivenDispatchEvent(event) {
		replay.skip(SkipClockDriven)
		return false, nil
	}

	// The same holds for a CloudFormation or API Gateway internal dispatch whose parent
	// request is replayed too: re-running the parent re-runs it, with the same derived
	// request ID. One whose parent is not in the replayed events, such as a stack's own
	// stream or a [Client.Deploy] stream, is executed.
	if isInternalDispatchEvent(event) {
		if reason, skip := replay.internalDispatchSkip(event); skip {
			replay.skip(reason)
			return false, nil
		}
	}

	// The clock is frozen *and then* set, in that order, and restored when the event
	// is done. SetTime alone sets a baseline that advances with wall time, so a
	// handler rendering from Now() saw event.Timestamp plus however long the replay
	// dispatch took — reproducing the recorded value only when the two reads landed
	// in the same second, and reporting a one-second difference when they straddled a
	// boundary. That made a replayed timestamp nearly reproducible rather than
	// reproducible, and made any test asserting one a wall-clock-dependent test
	// (#1217). Frozen, every read during this event returns event.Timestamp exactly.
	//
	// Restoring rather than leaving it frozen matters because the controller is the
	// server's: a replay that ran on a live emulator would otherwise stop the clock
	// for every request after it. A clock that was already frozen when the replay
	// reached this event is left frozen, so a caller that stopped the clock on purpose
	// — which is the one way to make a live run's timestamps exact too — does not have
	// it started again by a replay.
	if r.timeController != nil {
		wasFrozen := r.timeController.Frozen()
		r.timeController.Freeze()
		r.timeController.SetTime(event.Timestamp)
		if !wasFrozen {
			defer r.timeController.Unfreeze()
		}
	}

	// A control-plane write is not an AWS request: it passes no pre-plugin gate, reaches
	// no plugin, and is re-applied by re-issuing the HTTP request that made it. It is
	// dispatched after the freeze above so a seed that stores a timestamp stores the
	// recorded one.
	if isControlPlaneEvent(event) {
		return r.replayControlPlaneEvent(ctx, event, replay)
	}

	requestID := replayRequestID(event)
	reqCtx := &RequestContext{
		RequestID: requestID,
		AccountID: event.AccountID,
		Region:    event.Region,
		Timestamp: event.Timestamp,
		// Seeded from the recorded request id, which is what makes a replayed create mint
		// the identifier the recording minted (#856). A stream recorded before the event
		// carried a request id falls back to the event id, per replayRequestID, and then
		// mints identifiers of its own — the original values were never written down.
		IDs: NewIDMint(requestID),
		// The caller the recording was served for. Without it every authorization
		// door — the pipeline's step 2 and each plugin's own p.authorize — sees a nil
		// principal and leaves the request unenforced, so a replay authorized nothing
		// at all (#833).
		Principal: replayPrincipal(ctx, r.stateManager, event),
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
				Operation:    event.Operation,
				Field:        "state_hash_before",
				Expected:     event.StateHashBefore,
				Actual:       actual,
				Significance: "critical",
			})
			replay.Results.StateValid = false
			replay.Results.StateErrors = append(replay.Results.StateErrors,
				stateHashError(event, "state_hash_before", event.StateHashBefore, actual))
		}
	}

	// Steps 2 through 4.5, through the same gates the live server runs. A refusal
	// stops here: the request must not reach the plugin, because the recorded run's
	// plugin never saw it either and routing it would apply a state change the
	// recording does not contain.
	//
	// An internal dispatch passes authorization only: live, [StackDeployer.dispatch]
	// authorizes and the API Gateway proxy's Lambda invocation checks nothing, and
	// neither consults quota, consistency or faults, so replaying them through those
	// gates would refuse what the live run never asked.
	gates := r.pipeline.gates()
	internal := isInternalDispatchEvent(event)
	if internal {
		gates.quota, gates.consistency, gates.fault = nil, nil, nil
	}
	if gate := gates.check(reqCtx, event.Request); gate.err != nil {
		r.recordErrorDifference(replay, event, gate.err)
		return true, gate.err
	}

	// The clock-driven work the live path ran at this step, before this request (#1292).
	// The clock is pinned to the recorded timestamp, so the same work is due, and it derives
	// the same request IDs from this request's. It is not recorded: a replay writes no events.
	if err := r.registry.RunClockDriven(reqCtx, r.registry.RouteRequest); err != nil {
		r.recordErrorDifference(replay, event, err)
		return true, err
	}

	resp, err := r.registry.RouteRequest(reqCtx, event.Request)
	if err != nil {
		r.recordErrorDifference(replay, event, err)
		return true, err
	}

	// Step 7, as the live server runs it: a successful write opens the propagation window
	// a later replayed read is checked against.
	if !internal && r.pipeline.Consistency != nil && isMutating(event.Request.Operation) {
		r.pipeline.Consistency.RecordWrite(reqCtx, event.Request)
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
			Operation:    event.Operation,
			Field:        "error",
			Expected:     event.Error,
			Actual:       nil,
			Significance: "critical",
		})
	}

	if event.Response != nil && resp != nil {
		if resp.StatusCode != event.Response.StatusCode {
			replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
				EventID:      event.ID,
				Sequence:     event.Sequence,
				Operation:    event.Operation,
				Field:        "status_code",
				Expected:     event.Response.StatusCode,
				Actual:       resp.StatusCode,
				Significance: "major",
			})
		}
		if event.ResponseBodyOmitted != nil {
			r.recordDigestDifference(replay, event, resp)
		} else {
			r.recordBodyDifferences(replay, event, resp)
		}
	}

	if r.config.ValidateState && event.StateHashAfter != "" {
		if actual := r.computeStateHash(ctx); actual != event.StateHashAfter {
			replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
				EventID:      event.ID,
				Sequence:     event.Sequence,
				Operation:    event.Operation,
				Field:        "state_hash_after",
				Expected:     event.StateHashAfter,
				Actual:       actual,
				Significance: "critical",
			})
			replay.Results.StateValid = false
			replay.Results.StateErrors = append(replay.Results.StateErrors,
				stateHashError(event, "state_hash_after", event.StateHashAfter, actual))
		}
	}

	return true, nil
}

// recordErrorDifference compares an error a replay produced against the one the
// recording carries, and records the divergence when they disagree.
//
// One helper for both errors a replay can produce — a pre-plugin refusal and a
// plugin's own error — because the comparison is the same question in both cases and
// two copies of it could answer differently for one stream.
func (r *ReplayEngine) recordErrorDifference(replay *ActiveReplay, event *Event, err error) {
	if event.Error == "" {
		replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
			EventID:      event.ID,
			Sequence:     event.Sequence,
			Operation:    event.Operation,
			Field:        "error",
			Expected:     nil,
			Actual:       err.Error(),
			Significance: "critical",
		})
		return
	}
	if err.Error() != event.Error {
		replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
			EventID:      event.ID,
			Sequence:     event.Sequence,
			Operation:    event.Operation,
			Field:        "error_message",
			Expected:     event.Error,
			Actual:       err.Error(),
			Significance: "major",
		})
	}
}

// recordBodyDifferences compares the replayed response body against the recorded
// one and records each divergence with the path it was found at.
//
// Every body divergence is "major", not "critical". The critical band is reserved
// for a replay that reached a different *outcome* than the recording — a different
// state, or a refusal that became a success — because those make a passing test
// meaningless. A body difference is a divergence in what the recording reported,
// which is what this comparison exists to surface, and marking it critical would
// leave no band above it for the outcome divergences that are strictly worse.
//
// The comparison is unconditional rather than gated behind a [ReplayConfig] flag.
// A flag defaulting to off would recreate exactly the hole #833 closed — a replay
// that verifies less than the reader assumes and says nothing about it — and a
// flag defaulting to on is a flag nobody sets. Note that it runs whether or not
// [ReplayConfig.ValidateState] is set, because a state hash cannot see a body at
// all: the divergence a read-only operation can produce is invisible to it.
//
// See [responseBodyDifferences] for the normalisation policy, and in particular
// for why an identifier minted during the recording is reported rather than masked
// (#856).
func (r *ReplayEngine) recordBodyDifferences(replay *ActiveReplay, event *Event, resp *AWSResponse) {
	for _, diff := range responseBodyDifferences(event.Response, resp) {
		replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
			EventID:      event.ID,
			Sequence:     event.Sequence,
			Operation:    event.Operation,
			Field:        bodyDifferenceField(diff.path),
			Expected:     diff.expected,
			Actual:       diff.actual,
			Significance: "major",
		})
	}
}

// chargeSkippedGates applies to the quota and consistency controllers what a top-level
// event the replay cannot re-execute did to them live: it took a quota token and, as a
// write that reached its plugin, opened a propagation window. Without it every gate
// decision after a skipped event diverges from the recording.
//
// It is best effort. An event recorded without its request is charged under its
// service and operation alone, so a rule or resource key that needs the path or the
// parameters is not reached. An event refused for access was refused before the quota
// gate and takes no token.
func (r *ReplayEngine) chargeSkippedGates(event *Event) {
	if !isTopLevelEvent(event) || (r.pipeline.Quota == nil && r.pipeline.Consistency == nil) {
		return
	}
	req := event.Request
	if req == nil {
		req = &AWSRequest{Service: event.Service, Operation: event.Operation}
	}
	if r.timeController != nil {
		wasFrozen := r.timeController.Frozen()
		r.timeController.Freeze()
		r.timeController.SetTime(event.Timestamp)
		if !wasFrozen {
			defer r.timeController.Unfreeze()
		}
	}
	reqCtx := &RequestContext{
		RequestID: replayRequestID(event),
		AccountID: event.AccountID,
		Region:    event.Region,
		Timestamp: event.Timestamp,
		Metadata:  map[string]interface{}{"stream_id": event.StreamID, "replaying": true},
	}
	if r.pipeline.Quota != nil && !isAccessRefusal(event.ErrorCode) {
		// The token is what is being charged; whether it was granted is the recording's
		// answer, and the event is not re-decided.
		_ = r.pipeline.Quota.CheckQuota(reqCtx, req)
	}
	if r.pipeline.Consistency != nil && event.Error == "" && isMutating(req.Operation) {
		r.pipeline.Consistency.RecordWrite(reqCtx, req)
	}
}

// isAccessRefusal reports whether code is an authorization refusal, which the pipeline
// answers before it reaches the quota gate.
func isAccessRefusal(code string) bool {
	return code == "AccessDenied" || code == "AccessDeniedException"
}

// recordDigestDifference compares a replayed body against the digest recorded in place
// of a body that was over [EventStoreConfig.MaxBodyBytes]. Only byte equality can be
// checked: the normalisation [responseBodyDifferences] applies (JSON member order, XML
// whitespace) is not, so two bodies an SDK decodes identically can still differ here. A
// mismatch is one whole-body difference.
func (r *ReplayEngine) recordDigestDifference(replay *ActiveReplay, event *Event, resp *AWSResponse) {
	got := newBodyDigest(resp.Body)
	if *got == *event.ResponseBodyOmitted {
		return
	}
	replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
		EventID:      event.ID,
		Sequence:     event.Sequence,
		Operation:    event.Operation,
		Field:        bodyDifferenceField(""),
		Expected:     fmt.Sprintf("%d bytes, sha256 %s", event.ResponseBodyOmitted.Length, event.ResponseBodyOmitted.SHA256),
		Actual:       fmt.Sprintf("%d bytes, sha256 %s", got.Length, got.SHA256),
		Significance: "major",
	})
}

// replayPrincipal rebuilds the principal a recorded request was served for, or nil
// when the event names none — an unsigned request, which most recorded requests are,
// and which was unenforced live for the same reason it is unenforced here.
//
// The input is the recorded caller ARN rather than the recorded access key, and that
// is what makes replaying an authorization decision possible at all. Every consumer of
// a principal in the authorization path resolves the *ARN*
// ([AuthController.CheckAccess] and each plugin's own door both call
// [resolveIAMEntity]), and an ARN survives a replay where a key does not: an IAM user
// ARN carries the name and path CreateUser was given, and an assumed-role ARN carries
// the role name and session name AssumeRole was given, while both access key IDs are
// minted. Re-resolving the recorded key would find nothing in replayed state and
// leave the request unenforced, which is why this was recorded twice on #833 as
// blocked on #856 and is not.
//
// UserName is derived only for a user ARN whose record exists in replayed state. A
// registry hit with no IAM entity behind it synthesizes …:user/<access-key-id> (see
// [Server.handleAWSRequest] step 1.5), and deriving a name from that ARN would publish
// a credential ID as aws:username in a policy comparison (#745).
//
// UserID is deliberately left empty, so a replayed request publishes no aws:userid at
// all. An IAM user's AIDA… and an assumed role's <role-id>:<session-name> are minted,
// are not recoverable from the ARN, and the replay's own value would be a *different*
// identifier rather than the recorded one — absent is the shape [Principal.UserID]
// documents for a caller substrate has no ID for, and the answer #745 chose over a
// substituted-but-wrong one. It is the only part of authorization that #856 still owes.
func replayPrincipal(ctx context.Context, state StateManager, event *Event) *Principal {
	if state == nil || event.Principal == "" {
		return nil
	}
	principal := &Principal{
		ARN: event.Principal,
		// Derived rather than recorded, following [cfnPrincipalType]: nothing
		// evaluates Type, and a stored one could disagree with the ARN beside it.
		Type: cfnPrincipalType(event.Principal),
		// Read from the replayed entity's own record, which is where the live path
		// reads them from too — so a TagUser the stream replays is reflected here.
		Tags: iamPrincipalTags(ctx, state, event.Principal),
	}
	if entityType, nameWithPath := parsePrincipalARN(event.Principal); entityType == "user" {
		if name := iamFriendlyName(nameWithPath); name != "" {
			if _, exists, err := resolveIAMEntity(ctx, state, event.Principal); err == nil && exists {
				principal.UserName = name
			}
		}
	}
	return principal
}

// stateHashError describes one state hash mismatch for [ReplayResults.StateErrors].
//
// StateErrors was documented to carry "descriptions of any state hash mismatches"
// and nothing ever appended to it: a mismatch set StateValid to false and recorded
// an [EventDifference], leaving the slice empty on every run. That was invisible
// while [ReplayConfig.ValidateState] could not be switched on from the CLI at all;
// making it configurable (#880) would otherwise have shipped a summary reporting
// "MISMATCH (0 error(s))" for a real divergence, and printing no line for any of
// them.
func stateHashError(event *Event, field, recorded, replayed string) string {
	return fmt.Sprintf("event %s (seq %d) %s: recorded %s, replayed %s",
		event.ID, event.Sequence, field, recorded, replayed)
}

// replayRequestID is the request id a replayed handler is dispatched under: the
// one the recording was served with, so a success body rendering ctx.RequestID
// reproduces the recorded bytes rather than inventing a value (#866).
//
// It falls back to the event id for a stream recorded before [Event.RequestID]
// existed. That is not a reproduction of the original run — nothing can be, since
// the value was never written down — but it is what such a stream replayed to
// before, so an older stream keeps replaying exactly as it did instead of carrying
// an empty RequestId into every body.
func replayRequestID(event *Event) string {
	if event.RequestID != "" {
		return event.RequestID
	}
	return event.ID
}

// StepForward re-executes the event at the current position, advances the position
// past it, and returns it.
//
// The error is the event's own replay error. The position advances either way, because
// a recorded failure is part of the stream and stepping past it is how a session reaches
// the events after it; whether the error matches the recording is in
// [ReplayResults.Differences]. It returns a nil event only when there is no active replay
// or the replay is at its end.
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
	_, err := r.advance(ctx, r.currentReplay)
	r.currentReplay.Position++
	return event, err
}

// StepBackward moves the replay back one event, so the event before the current
// position is the next to run, and returns that event.
//
// It is [ReplayEngine.JumpToEvent] to the previous position. State cannot be run
// backwards, so the engine restores the nearest snapshot at or before that position, or
// resets, and replays forward to it.
func (r *ReplayEngine) StepBackward(ctx context.Context) (*Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay == nil {
		return nil, fmt.Errorf("no active replay")
	}

	if r.currentReplay.Position == 0 {
		return nil, fmt.Errorf("already at beginning of replay")
	}

	target := r.currentReplay.Position - 1
	if err := r.jump(ctx, r.currentReplay, target); err != nil {
		return nil, err
	}
	return r.currentReplay.Events[target], nil
}

// JumpToEvent moves the active replay to position, an index into the stream: the state
// becomes the state before the event at that position ran, so 0 is the start of the run
// and len(stream) is its end.
//
// A forward jump replays from the current position. A backward jump restores the
// nearest snapshot at or before position (see [ActiveReplay.StateSnapshots]), or resets
// the state when there is none, and replays forward from there. An event's replay error
// does not stop the jump; it is compared with the recording like any other result, and
// an event re-run by a jump does not report its differences again.
//
// Use [ReplayEngine.JumpToSequence] to name the target by [Event.Sequence] instead.
func (r *ReplayEngine) JumpToEvent(ctx context.Context, position int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay == nil {
		return fmt.Errorf("no active replay")
	}
	return r.jump(ctx, r.currentReplay, position)
}

// JumpToSequence moves the active replay to just before the first event whose
// [Event.Sequence] is sequence or greater. A sequence past the stream's last event moves
// it to the end. Sequences are numbered across the whole store, which is why this is
// separate from [ReplayEngine.JumpToEvent].
func (r *ReplayEngine) JumpToSequence(ctx context.Context, sequence int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentReplay == nil {
		return fmt.Errorf("no active replay")
	}
	return r.jump(ctx, r.currentReplay, positionOfSequence(r.currentReplay.Events, sequence))
}

// positionOfSequence returns the index of the first event in events whose sequence is
// at least sequence, or len(events) when there is none. events is in sequence order.
func positionOfSequence(events []*Event, sequence int64) int {
	return sort.Search(len(events), func(i int) bool { return events[i].Sequence >= sequence })
}

// jump implements [ReplayEngine.JumpToEvent]. The caller holds r.mu.
func (r *ReplayEngine) jump(ctx context.Context, replay *ActiveReplay, target int) error {
	if target < 0 || target > len(replay.Events) {
		return fmt.Errorf("position %d out of range [0, %d]", target, len(replay.Events))
	}

	if target < replay.Position {
		replay.snapshotting = true
		if pos, ok := r.nearestSnapshot(replay, target); ok {
			if err := r.restoreSnapshot(ctx, replay, pos); err != nil {
				return err
			}
			replay.Position = pos
		} else {
			if err := r.startOfRun(ctx, replay); err != nil {
				return err
			}
			replay.Position = 0
		}
	}

	for replay.Position < target {
		if _, err := r.advance(ctx, replay); err != nil {
			r.logger.Debug("event replay returned an error during a jump",
				"position", replay.Position, "error", err)
		}
		replay.Position++
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
// position. A failed read is returned rather than leaving the key out, so a partial
// map is never mistaken for the state.
func (r *ReplayEngine) InspectState(ctx context.Context, namespace string) (map[string][]byte, error) {
	if r.stateManager == nil {
		return nil, fmt.Errorf("replay engine has no state manager")
	}
	keys, err := r.stateManager.List(ctx, namespace, "")
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", namespace, err)
	}

	state := make(map[string][]byte, len(keys))
	for _, key := range keys {
		val, err := r.stateManager.Get(ctx, namespace, key)
		if err != nil {
			return nil, fmt.Errorf("read %s/%s: %w", namespace, key, err)
		}
		state[key] = val
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
//
// Nothing tags a request that arrives over HTTP with this stream: every such request
// is recorded in [DefaultStreamID]. Only an in-process dispatch that sets
// RequestContext.Metadata["stream_id"] to the session's StreamID joins it. To replay a
// test server's traffic, replay [DefaultStreamID] or call [TestServer.Replay].
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

// resetState returns the emulator to a run's starting point in preparation for a
// fresh replay. That is two things, not one: the [StateManager]'s contents, and
// whatever mutable state the plugins keep on themselves.
//
// The second half was missing until #886. Most of what a plugin mutates lives in
// the state manager, but a few plugins hold their own — the S3 version-ID counter,
// the SES v2 message counter, and (until #1166 moved it onto the request mint) the
// HealthOmics random source — and nothing reset
// those. So replaying one stream twice in a single process minted different
// identifiers each time from identical recorded events, which is the property the
// event log exists to rule out.
//
// It also releases what a plugin started rather than minted: S3's object payloads
// (#902) and Lambda's event-source-mapping pollers (#903). A replay needs those gone
// for the same reason it needs the counters rewound — a poller left over from the
// previous replay polls the queue this one recreates.
//
// The [FaultController] is rewound here too, for the same reason and with the same
// consequence if it is not: its per-rule fired counts and PRNG positions are state a
// run advances, and a rule that has spent its bound refuses nothing (#833).
//
// The state manager is cleared first and the plugins second, because
// [ResettablePlugin.ResetForRun] is allowed to write a plugin's start-of-run state
// into the state manager; wiping the store afterwards would erase exactly what the
// plugin had just re-established.
//
// A missing state manager, a state manager that does not implement
// [SnapshotableStateManager], and a nil registry are each skipped rather than
// treated as errors, matching what an engine constructed without them could do
// before.
func (r *ReplayEngine) resetState(ctx context.Context) error {
	if r.stateManager != nil {
		if ss, ok := r.stateManager.(SnapshotableStateManager); ok {
			if err := ss.Reset(ctx); err != nil {
				return fmt.Errorf("reset state manager: %w", err)
			}
		}
	}
	// A controller's own mutable state, for the same reason the plugins' is reset
	// below: a rule that fired during the recording has spent its Times bound, and a
	// replay that started from those counts would take the unfaulted path through
	// every request the recording failed (#833).
	if r.pipeline.Fault != nil {
		r.pipeline.Fault.rewindForReplay()
	}
	// Token buckets and propagation windows are spent and opened the same way (#1601).
	if r.pipeline.Quota != nil {
		r.pipeline.Quota.rewindForReplay()
	}
	if r.pipeline.Consistency != nil {
		r.pipeline.Consistency.rewindForReplay()
	}
	if r.registry == nil {
		return nil
	}
	if err := r.registry.ResetPlugins(ctx); err != nil {
		return fmt.Errorf("reset plugins: %w", err)
	}
	return nil
}

// loadFromSnapshot restores state from the most recent snapshot for streamID.
//
// A stream with no stored snapshot is not an error: there is nothing to restore, and the
// run starts from the state the caller reset.
func (r *ReplayEngine) loadFromSnapshot(ctx context.Context, streamID string) error {
	snapshot, err := r.eventStore.GetLatestSnapshot(ctx, streamID)
	if err != nil {
		r.logger.Debug("no stored snapshot; starting from reset state", "stream_id", streamID)
		return nil //nolint:nilerr // GetLatestSnapshot's only error is that none exists.
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
