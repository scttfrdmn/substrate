package substrate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// EventStore records all AWS requests as immutable events. It is the
// foundation of Substrate's deterministic reproducibility: because every
// request is stored as an event, test sessions can be replayed byte-for-byte
// with identical results.
type EventStore struct {
	mu         sync.RWMutex
	events     []*Event
	snapshots  map[string]*Snapshot
	streams    map[string][]*Event // stream_id → events
	config     EventStoreConfig
	serializer Serializer
}

// EventStoreConfig controls the behaviour of an [EventStore].
type EventStoreConfig struct {
	// Enabled gates event recording. When false, RecordEvent is a no-op.
	Enabled bool

	// SnapshotInterval creates a snapshot automatically every N events.
	// Zero disables automatic snapshots.
	SnapshotInterval int

	// MaxEventsInMemory is the maximum number of events held in memory before
	// the store flushes to the configured backend. Zero means unlimited.
	MaxEventsInMemory int

	// Backend selects the storage driver: "memory", "sqlite", or "file".
	Backend string

	// PersistPath is the filesystem path used by the "sqlite" and "file"
	// backends.
	PersistPath string

	// IncludeBodies instructs the store to capture raw request and response
	// bodies inside each event. Disable to reduce storage when bodies are large.
	IncludeBodies bool

	// IncludeStateHashes enables before/after state hashing on each event,
	// which [ReplayEngine] uses to validate determinism.
	IncludeStateHashes bool
}

// Event represents a single AWS request/response pair captured by [EventStore].
// Events are immutable once recorded.
type Event struct {
	// ID uniquely identifies this event within the store.
	ID string `json:"id"`

	// Sequence is the monotonically increasing position in the global log.
	Sequence int64 `json:"sequence"`

	// Timestamp is the time at which the request was processed.
	Timestamp time.Time `json:"timestamp"`

	// StreamID groups related events (e.g., a single test session).
	StreamID string `json:"stream_id"`

	// AccountID is the AWS account associated with the request.
	AccountID string `json:"account_id"`

	// Region is the AWS region targeted by the request.
	Region string `json:"region"`

	// Principal is the ARN of the caller.
	Principal string `json:"principal,omitempty"`

	// Service is the AWS service name.
	Service string `json:"service"`

	// Operation is the AWS API operation name.
	Operation string `json:"operation"`

	// Request is the raw AWS request, populated only when [EventStoreConfig.IncludeBodies] is true.
	Request *AWSRequest `json:"request,omitempty"`

	// Response is the raw AWS response, populated only when [EventStoreConfig.IncludeBodies] is true.
	Response *AWSResponse `json:"response,omitempty"`

	// StateHashBefore is a SHA-256 hash of service state before this request.
	StateHashBefore string `json:"state_hash_before,omitempty"`

	// StateHashAfter is a SHA-256 hash of service state after this request.
	StateHashAfter string `json:"state_hash_after,omitempty"`

	// Error is the string representation of any error returned by the handler.
	Error string `json:"error,omitempty"`

	// ErrorCode is the structured AWS error code when the handler returned an [*AWSError].
	ErrorCode string `json:"error_code,omitempty"`

	// Duration is the wall-clock time taken to process the request.
	Duration time.Duration `json:"duration"`

	// Cost is the estimated AWS cost for this operation in USD.
	Cost float64 `json:"cost,omitempty"`

	// Metadata holds arbitrary key-value data attached to the event.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Snapshot captures emulator state at a specific event sequence position,
// enabling the [ReplayEngine] to fast-forward instead of replaying from the
// beginning.
type Snapshot struct {
	// ID uniquely identifies this snapshot.
	ID string `json:"id"`

	// Timestamp is when the snapshot was taken.
	Timestamp time.Time `json:"timestamp"`

	// Sequence is the event sequence number at which the snapshot was taken.
	Sequence int64 `json:"sequence"`

	// StreamID is the event stream this snapshot belongs to.
	StreamID string `json:"stream_id"`

	// State is the serialised emulator state at Sequence.
	State []byte `json:"state"`

	// StateHash is the SHA-256 hash of State, used for integrity verification.
	StateHash string `json:"state_hash"`

	// FromEvent and ToEvent delimit the range of events covered.
	FromEvent int64 `json:"from_event"`
	ToEvent   int64 `json:"to_event"`
}

// NewEventStore creates a new EventStore with the given configuration.
func NewEventStore(config EventStoreConfig) *EventStore {
	return &EventStore{
		events:     make([]*Event, 0),
		snapshots:  make(map[string]*Snapshot),
		streams:    make(map[string][]*Event),
		config:     config,
		serializer: &JSONSerializer{},
	}
}

// RecordEvent appends event to the store, assigning it a sequence number and
// generating an ID if one was not provided. It is a no-op when
// [EventStoreConfig.Enabled] is false.
func (e *EventStore) RecordEvent(ctx context.Context, event *Event) error {
	if !e.config.Enabled {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	event.Sequence = int64(len(e.events))

	if event.ID == "" {
		event.ID = generateEventID(event)
	}

	e.events = append(e.events, event)

	if event.StreamID != "" {
		e.streams[event.StreamID] = append(e.streams[event.StreamID], event)
	}

	// TODO(#3): trigger async snapshot when SnapshotInterval is reached.

	return nil
}

// RecordRequest is a convenience wrapper that builds an [Event] from a
// completed AWS request/response cycle and records it.
func (e *EventStore) RecordRequest(
	goctx context.Context,
	reqCtx *RequestContext,
	req *AWSRequest,
	resp *AWSResponse,
	duration time.Duration,
	cost float64,
	err error,
) error {
	event := &Event{
		Timestamp: time.Now(),
		StreamID:  streamIDFromContext(reqCtx),
		AccountID: reqCtx.AccountID,
		Region:    reqCtx.Region,
		Service:   serviceFromRequest(req),
		Operation: req.Operation,
		Duration:  duration,
		Cost:      cost,
	}

	if reqCtx.Principal != nil {
		event.Principal = reqCtx.Principal.ARN
	}

	if e.config.IncludeBodies {
		event.Request = req
		event.Response = resp
	}

	if err != nil {
		event.Error = err.Error()
		if asErr, ok := err.(*AWSError); ok {
			event.ErrorCode = asErr.Code
		}
	}

	return e.RecordEvent(goctx, event)
}

// GetEvent retrieves a single event by ID.
// Returns an error if the event does not exist.
func (e *EventStore) GetEvent(ctx context.Context, id string) (*Event, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, event := range e.events {
		if event.ID == id {
			return event, nil
		}
	}

	return nil, fmt.Errorf("event not found: %s", id)
}

// GetEvents returns all events matching filter, in sequence order.
func (e *EventStore) GetEvents(ctx context.Context, filter EventFilter) ([]*Event, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var results []*Event
	for _, event := range e.events {
		if matchesFilter(event, filter) {
			results = append(results, event)
		}
	}

	return results, nil
}

// GetStream returns all events belonging to streamID, in sequence order.
func (e *EventStore) GetStream(ctx context.Context, streamID string) ([]*Event, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if events, ok := e.streams[streamID]; ok {
		return events, nil
	}

	return []*Event{}, nil
}

// EventFilter constrains the events returned by [EventStore.GetEvents].
// Zero values for each field are treated as "no constraint".
type EventFilter struct {
	// StreamID matches events belonging to this stream.
	StreamID string

	// Service matches events for this AWS service.
	Service string

	// Operation matches events for this API operation.
	Operation string

	// AccountID matches events for this AWS account.
	AccountID string

	// Region matches events for this AWS region.
	Region string

	// StartTime, if non-zero, excludes events before this time.
	StartTime time.Time

	// EndTime, if non-zero, excludes events after this time.
	EndTime time.Time

	// MinSequence, if positive, excludes events with lower sequence numbers.
	MinSequence int64

	// MaxSequence, if positive, excludes events with higher sequence numbers.
	MaxSequence int64

	// HasError, if non-nil, filters to events that have (true) or lack (false)
	// an error.
	HasError *bool
}

// CreateSnapshot serialises state into a [Snapshot] and stores it.
func (e *EventStore) CreateSnapshot(ctx context.Context, streamID string, state StateManager) (*Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	stateBytes, err := e.serializeState(ctx, state)
	if err != nil {
		return nil, fmt.Errorf("serialize state: %w", err)
	}

	snapshot := &Snapshot{
		ID:        generateSnapshotID(),
		Timestamp: time.Now(),
		Sequence:  int64(len(e.events)),
		StreamID:  streamID,
		State:     stateBytes,
		StateHash: hashBytes(stateBytes),
		FromEvent: 0,
		ToEvent:   int64(len(e.events)),
	}

	e.snapshots[snapshot.ID] = snapshot

	return snapshot, nil
}

// GetSnapshot retrieves a snapshot by ID.
func (e *EventStore) GetSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if snapshot, ok := e.snapshots[id]; ok {
		return snapshot, nil
	}

	return nil, fmt.Errorf("snapshot not found: %s", id)
}

// GetLatestSnapshot returns the most recent snapshot for streamID.
// Returns an error if no snapshot exists for the stream.
func (e *EventStore) GetLatestSnapshot(ctx context.Context, streamID string) (*Snapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var latest *Snapshot
	for _, snapshot := range e.snapshots {
		if snapshot.StreamID == streamID {
			if latest == nil || snapshot.Sequence > latest.Sequence {
				latest = snapshot
			}
		}
	}

	if latest == nil {
		return nil, fmt.Errorf("no snapshot found for stream: %s", streamID)
	}

	return latest, nil
}

// ReplayIterator iterates over a filtered sequence of events for replay.
type ReplayIterator struct {
	events   []*Event
	position int
}

// NewReplayIterator creates an iterator over events matching filter.
func (e *EventStore) NewReplayIterator(ctx context.Context, filter EventFilter) (*ReplayIterator, error) {
	events, err := e.GetEvents(ctx, filter)
	if err != nil {
		return nil, err
	}

	return &ReplayIterator{events: events}, nil
}

// Next returns the next event and true, or nil and false when exhausted.
func (i *ReplayIterator) Next() (*Event, bool) {
	if i.position >= len(i.events) {
		return nil, false
	}

	event := i.events[i.position]
	i.position++

	return event, true
}

// HasNext reports whether more events remain.
func (i *ReplayIterator) HasNext() bool {
	return i.position < len(i.events)
}

// Reset positions the iterator back to the first event.
func (i *ReplayIterator) Reset() {
	i.position = 0
}

// Flush persists in-memory events to the configured backend.
// It is a no-op for the "memory" backend.
func (e *EventStore) Flush(ctx context.Context) error {
	if e.config.Backend == "memory" {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// TODO(#3): implement SQLite and file persistence.
	_, err := json.Marshal(e.events)
	if err != nil {
		return fmt.Errorf("marshal events: %w", err)
	}

	return nil
}

// Load restores events from the configured backend.
// It is a no-op for the "memory" backend.
func (e *EventStore) Load(ctx context.Context) error {
	if e.config.Backend == "memory" {
		return nil
	}

	// TODO(#3): implement SQLite and file loading.
	return nil
}

// EventStoreStats summarises the contents and cost of an [EventStore].
type EventStoreStats struct {
	// TotalEvents is the number of recorded events.
	TotalEvents int64

	// TotalStreams is the number of distinct stream IDs.
	TotalStreams int64

	// TotalSnapshots is the number of stored snapshots.
	TotalSnapshots int64

	// OldestEvent is the timestamp of the first recorded event.
	OldestEvent time.Time

	// NewestEvent is the timestamp of the most recently recorded event.
	NewestEvent time.Time

	// TotalDuration is the sum of all event durations.
	TotalDuration time.Duration

	// TotalCost is the sum of all event costs in USD.
	TotalCost float64

	// ErrorCount is the number of events that recorded an error.
	ErrorCount int64

	// SuccessCount is the number of events that succeeded.
	SuccessCount int64

	// ServiceBreakdown maps service name to event count.
	ServiceBreakdown map[string]int64
}

// GetStats returns aggregate statistics for all recorded events.
func (e *EventStore) GetStats(ctx context.Context) *EventStoreStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := &EventStoreStats{
		TotalEvents:      int64(len(e.events)),
		TotalStreams:     int64(len(e.streams)),
		TotalSnapshots:   int64(len(e.snapshots)),
		ServiceBreakdown: make(map[string]int64),
	}

	if len(e.events) > 0 {
		stats.OldestEvent = e.events[0].Timestamp
		stats.NewestEvent = e.events[len(e.events)-1].Timestamp
	}

	for _, event := range e.events {
		stats.TotalDuration += event.Duration
		stats.TotalCost += event.Cost

		if event.Error != "" {
			stats.ErrorCount++
		} else {
			stats.SuccessCount++
		}

		stats.ServiceBreakdown[event.Service]++
	}

	return stats
}

// Serializer converts [Event] values to and from bytes.
type Serializer interface {
	// Serialize encodes event into bytes.
	Serialize(event *Event) ([]byte, error)

	// Deserialize decodes bytes into an event.
	Deserialize(data []byte) (*Event, error)
}

// JSONSerializer implements [Serializer] using standard JSON encoding.
type JSONSerializer struct{}

// Serialize encodes event as JSON.
func (s *JSONSerializer) Serialize(event *Event) ([]byte, error) {
	return json.Marshal(event)
}

// Deserialize decodes JSON data into an Event.
func (s *JSONSerializer) Deserialize(data []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// generateEventID produces a unique event ID from service, operation, and time.
func generateEventID(event *Event) string {
	return fmt.Sprintf("%s-%s-%d", event.Service, event.Operation, time.Now().UnixNano())
}

// generateSnapshotID produces a unique snapshot ID.
func generateSnapshotID() string {
	return fmt.Sprintf("snapshot-%d", time.Now().UnixNano())
}

// hashBytes returns the hex-encoded SHA-256 hash of data.
func hashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

// serviceFromRequest extracts the service name from a request.
// It prefers AWSRequest.Service and falls back to the X-Amz-Target header.
func serviceFromRequest(req *AWSRequest) string {
	if req.Service != "" {
		return req.Service
	}
	// X-Amz-Target format: "DynamoDB_20120810.GetItem"
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if i := strings.IndexByte(target, '_'); i > 0 {
			return strings.ToLower(target[:i])
		}
	}
	return "unknown"
}

// streamIDFromContext extracts the stream_id from the request context metadata.
func streamIDFromContext(ctx *RequestContext) string {
	if ctx.Metadata != nil {
		if id, ok := ctx.Metadata["stream_id"].(string); ok {
			return id
		}
	}
	return "default"
}

// matchesFilter reports whether event satisfies all constraints in filter.
func matchesFilter(event *Event, filter EventFilter) bool {
	if filter.StreamID != "" && event.StreamID != filter.StreamID {
		return false
	}
	if filter.Service != "" && event.Service != filter.Service {
		return false
	}
	if filter.Operation != "" && event.Operation != filter.Operation {
		return false
	}
	if filter.AccountID != "" && event.AccountID != filter.AccountID {
		return false
	}
	if filter.Region != "" && event.Region != filter.Region {
		return false
	}
	if !filter.StartTime.IsZero() && event.Timestamp.Before(filter.StartTime) {
		return false
	}
	if !filter.EndTime.IsZero() && event.Timestamp.After(filter.EndTime) {
		return false
	}
	if filter.MinSequence > 0 && event.Sequence < filter.MinSequence {
		return false
	}
	if filter.MaxSequence > 0 && event.Sequence > filter.MaxSequence {
		return false
	}
	if filter.HasError != nil {
		hasError := event.Error != ""
		if *filter.HasError != hasError {
			return false
		}
	}
	return true
}

// serializeState serialises the current state into bytes for snapshotting.
func (e *EventStore) serializeState(_ context.Context, _ StateManager) ([]byte, error) {
	// TODO(#5): implement full state serialisation.
	return []byte("{}"), nil
}
