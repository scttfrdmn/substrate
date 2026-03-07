package substrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate"
)

func TestNewEventStore(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	require.NotNil(t, store)

	stats := store.GetStats(context.Background())
	assert.Equal(t, int64(0), stats.TotalEvents)
}

func TestEventStore_RecordEvent(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	event := &substrate.Event{
		StreamID:  "stream-1",
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Now(),
	}

	require.NoError(t, store.RecordEvent(ctx, event))

	// ID and sequence should be assigned.
	assert.NotEmpty(t, event.ID)
	assert.Equal(t, int64(0), event.Sequence)
}

func TestEventStore_RecordEvent_Disabled(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: false, Backend: "memory"})
	ctx := context.Background()

	event := &substrate.Event{Service: "s3", Operation: "PutObject", Timestamp: time.Now()}
	require.NoError(t, store.RecordEvent(ctx, event))

	stats := store.GetStats(ctx)
	assert.Equal(t, int64(0), stats.TotalEvents, "disabled store must not record events")
}

func TestEventStore_GetStream(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	const streamID = "test-stream"

	for i := range 5 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  streamID,
			Service:   "s3",
			Operation: "PutObject",
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
		}))
	}
	// Extra event on a different stream.
	require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
		StreamID:  "other-stream",
		Service:   "iam",
		Operation: "CreateUser",
		Timestamp: time.Now(),
	}))

	events, err := store.GetStream(ctx, streamID)
	require.NoError(t, err)
	assert.Len(t, events, 5)

	for _, e := range events {
		assert.Equal(t, streamID, e.StreamID)
	}
}

func TestEventStore_GetStream_Unknown(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	events, err := store.GetStream(context.Background(), "no-such-stream")
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestEventStore_GetEvent(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	event := &substrate.Event{
		StreamID:  "s",
		Service:   "dynamodb",
		Operation: "GetItem",
		Timestamp: time.Now(),
	}
	require.NoError(t, store.RecordEvent(ctx, event))

	got, err := store.GetEvent(ctx, event.ID)
	require.NoError(t, err)
	assert.Equal(t, event.ID, got.ID)
	assert.Equal(t, "GetItem", got.Operation)
}

func TestEventStore_GetEvent_NotFound(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	_, err := store.GetEvent(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestEventStore_GetEvents_Filter(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for _, svc := range []string{"s3", "s3", "iam", "dynamodb"} {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  "s",
			Service:   svc,
			Operation: "Op",
			Timestamp: time.Now(),
		}))
	}

	events, err := store.GetEvents(ctx, substrate.EventFilter{Service: "s3"})
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestEventStore_GetStats(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	hasErr := true
	events := []*substrate.Event{
		{StreamID: "a", Service: "s3", Operation: "Put", Cost: 0.005, Timestamp: time.Now()},
		{StreamID: "a", Service: "s3", Operation: "Get", Cost: 0.001, Error: "boom", Timestamp: time.Now()},
		{StreamID: "b", Service: "iam", Operation: "CreateUser", Timestamp: time.Now()},
	}
	for _, e := range events {
		require.NoError(t, store.RecordEvent(ctx, e))
	}

	stats := store.GetStats(ctx)
	assert.Equal(t, int64(3), stats.TotalEvents)
	assert.Equal(t, int64(2), stats.TotalStreams)
	assert.Equal(t, int64(1), stats.ErrorCount)
	assert.Equal(t, int64(2), stats.SuccessCount)
	assert.InDelta(t, 0.006, stats.TotalCost, 1e-9)
	assert.Equal(t, int64(2), stats.ServiceBreakdown["s3"])
	assert.Equal(t, int64(1), stats.ServiceBreakdown["iam"])

	_ = hasErr
}

func TestEventStore_ReplayIterator(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for i := range 3 {
		require.NoError(t, store.RecordEvent(ctx, &substrate.Event{
			StreamID:  "s",
			Service:   "s3",
			Operation: "Put",
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
		}))
	}

	iter, err := store.NewReplayIterator(ctx, substrate.EventFilter{StreamID: "s"})
	require.NoError(t, err)

	count := 0
	for iter.HasNext() {
		e, ok := iter.Next()
		require.True(t, ok)
		require.NotNil(t, e)
		count++
	}
	assert.Equal(t, 3, count)

	// After exhaustion, Next returns false.
	_, ok := iter.Next()
	assert.False(t, ok)

	// Reset restarts iteration.
	iter.Reset()
	assert.True(t, iter.HasNext())
}

func TestEventStore_Flush_Memory(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	assert.NoError(t, store.Flush(context.Background()))
}

func TestEventStore_Load_Memory(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	assert.NoError(t, store.Load(context.Background()))
}

func TestJSONSerializer(t *testing.T) {
	s := &substrate.JSONSerializer{}

	original := &substrate.Event{
		ID:        "evt-1",
		Sequence:  42,
		Service:   "s3",
		Operation: "PutObject",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Cost:      0.005,
	}

	data, err := s.Serialize(original)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := s.Deserialize(data)
	require.NoError(t, err)
	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Sequence, decoded.Sequence)
	assert.Equal(t, original.Service, decoded.Service)
	assert.Equal(t, original.Cost, decoded.Cost)
}

func TestJSONSerializer_InvalidJSON(t *testing.T) {
	s := &substrate.JSONSerializer{}
	_, err := s.Deserialize([]byte("not json"))
	assert.Error(t, err)
}
