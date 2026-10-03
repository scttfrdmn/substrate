package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for KinesisStream (#756), and
// the published form of GetRecords' ApproximateArrivalTimestamp (#1338).
//
// KinesisStream declares AccountID, Region and CreatedAt under wire-visible `json` tags, and EverTagged
// as `ever_tagged`, because the record is what MemoryStateManager snapshots and a replay reads back.
// None reaches a body: DescribeStream and DescribeStreamSummary answer buildStreamDescription and
// buildStreamDescriptionSummary (emulator/kinesis_stream_shapes.go), which build their maps member by
// member, and every other operation answers a map or a list of names. So Kinesis was already
// projected in code, and what it lacked was this file. CreatedAt does reach the wire, as the
// published StreamCreationTimestamp, which is not a member the walk below looks for.
//
// The shard iterator is the one place a record is marshaled into a response: kinesisIterator is
// base64-encoded into the ShardIterator and NextShardIterator strings, which the walk sees as string
// values, not members. That is kinesisIterator's own citation, not this file's.

// kinesisWireClock is the instant every fixture below starts the simulated clock at.
var kinesisWireClock = time.Unix(1700000000, 0).UTC()

// kinesisWireAccount and kinesisWireRegion scope every state key the plugin writes.
const (
	kinesisWireAccount = "123456789012"
	kinesisWireRegion  = "us-east-1"
)

// kinesisBookkeepingMembers are the members KinesisStream declares and no Kinesis shape publishes.
// `ever_tagged` is listed in its own right because a fold does not reach a snake_case spelling.
var kinesisBookkeepingMembers = []string{"AccountID", "Region", "CreatedAt", "EverTagged", "ever_tagged"}

// setupKinesisWirePlugin returns the Kinesis plugin, a request context and the state manager behind
// it, on a clock that does not advance, so a rendered date can be asserted exactly.
//
// Freeze then SetTime, in that order, which is the ordering TimeController.Freeze documents: Freeze
// alone stops the clock where it currently reads, and that drift would be visible in a rendered date.
func setupKinesisWirePlugin(t *testing.T) (*emulator.KinesisPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(kinesisWireClock)
	tc.Freeze()
	tc.SetTime(kinesisWireClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.KinesisPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.KinesisPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: kinesisWireAccount,
		Region:    kinesisWireRegion,
		RequestID: "req-kinesis-wire",
		IDs:       emulator.NewIDMint("req-kinesis-wire"),
	}, state
}

// kinesisWire issues one operation and returns the raw response body, failing on anything but 200.
func kinesisWire(t *testing.T, p *emulator.KinesisPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, kinesisRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// kinesisWireRecord returns the stream record as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func kinesisWireRecord(t *testing.T, state emulator.StateManager, name string) map[string]json.RawMessage {
	t.Helper()
	key := "stream:" + kinesisWireAccount + "/" + kinesisWireRegion + "/" + name
	data, err := state.Get(t.Context(), "kinesis", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

func TestKinesisWire_StreamResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupKinesisWirePlugin(t)

	const name = "wire-stream"
	stream := map[string]any{"StreamName": name}
	created := kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": name, "ShardCount": 2})

	record := kinesisWireRecord(t, state, name)
	for _, member := range []string{"AccountID", "Region", "CreatedAt"} {
		require.NotEmptyf(t, record[member], "the stream must persist %s before an absence assertion on it means anything", member)
		require.NotEqualf(t, `""`, string(record[member]), "the stream persists an empty %s", member)
	}

	// KinesisStream's fourth bookkeeping member is `ever_tagged,omitempty`, set only by a tag write
	// (#938). Until it is set, every response is missing it for free — the vacuous assertion #1304
	// shipped on EFS — so the stream is tagged and the flag read back before any response is walked.
	tagged := kinesisWire(t, p, ctx, "AddTagsToStream", map[string]any{"StreamName": name, "Tags": map[string]string{"team": "wire"}})
	require.JSONEq(t, "true", string(kinesisWireRecord(t, state, name)["ever_tagged"]),
		"the stream must persist ever_tagged before an absence assertion on it means anything")

	// A record to read back, and the iterator to read it through.
	put := kinesisWire(t, p, ctx, "PutRecord", map[string]any{"StreamName": name, "PartitionKey": "k", "Data": "d2lyZQ=="})
	var putOut struct {
		ShardID string `json:"ShardId"`
	}
	require.NoError(t, json.Unmarshal(put, &putOut), "decode PutRecord: %s", put)
	iterBody := kinesisWire(t, p, ctx, "GetShardIterator", map[string]any{
		"StreamName": name, "ShardId": putOut.ShardID, "ShardIteratorType": "TRIM_HORIZON",
	})
	var iter struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(iterBody, &iter), "decode GetShardIterator: %s", iterBody)
	records := kinesisWire(t, p, ctx, "GetRecords", map[string]any{"ShardIterator": iter.ShardIterator})

	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "CreateStream", held: created, anchor: "{}"},
		{op: "AddTagsToStream", held: tagged, anchor: "{}"},
		{op: "DescribeStream", body: stream, anchor: `"StreamName":"` + name + `"`},
		{op: "DescribeStreamSummary", body: stream, anchor: `"StreamName":"` + name + `"`},
		{op: "ListStreams", anchor: `"` + name + `"`},
		{op: "ListTagsForStream", body: stream, anchor: `"Key":"team"`},
		{op: "PutRecord", held: put, anchor: `"SequenceNumber"`},
		{op: "PutRecords", body: map[string]any{"StreamName": name, "Records": []map[string]string{{"PartitionKey": "k", "Data": "d2lyZQ=="}}},
			anchor: `"SequenceNumber"`},
		{op: "GetShardIterator", held: iterBody, anchor: `"ShardIterator"`},
		{op: "GetRecords", held: records, anchor: `"PartitionKey":"k"`},
		{op: "EnableEnhancedMonitoring", body: map[string]any{"StreamName": name, "ShardLevelMetrics": []string{"IncomingBytes"}},
			anchor: `"StreamName":"` + name + `"`},
		{op: "DisableEnhancedMonitoring", body: map[string]any{"StreamName": name, "ShardLevelMetrics": []string{"IncomingBytes"}},
			anchor: `"StreamName":"` + name + `"`},
		{op: "RemoveTagsFromStream", body: map[string]any{"StreamName": name, "TagKeys": []string{"team"}}, anchor: "{}"},
		{op: "UpdateShardCount", body: map[string]any{"StreamName": name, "TargetShardCount": 4, "ScalingType": "UNIFORM_SCALING"},
			anchor: `"TargetShardCount":4`},
		// Last: it removes the record every case above reads.
		{op: "DeleteStream", body: stream, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = kinesisWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, kinesisBookkeepingMembers)
		})
	}

	// After the walk, so a regression is reported as a bookkeeping leak first. API_GetRecords
	// publishes ApproximateArrivalTimestamp as a number — epoch seconds with millisecond precision —
	// and the clock is frozen, so the rendering is exact (#1338).
	require.Contains(t, string(records), `"ApproximateArrivalTimestamp":1700000000.000`,
		"GetRecords answers ApproximateArrivalTimestamp as epoch seconds: %s", records)
	require.NotContains(t, string(records), `"ApproximateArrivalTimestamp":"`,
		"GetRecords rendered ApproximateArrivalTimestamp as a string, which awsJson1_1 does not publish: %s", records)
}

// TestKinesisWire_ShardOperationsCarryNoBookkeepingMember drives the two shard-topology operations on
// a stream of their own, because each replaces shards the main test's iterator reads.
func TestKinesisWire_ShardOperationsCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)

	const name = "wire-shards"
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": name, "ShardCount": 2})
	described := kinesisWire(t, p, ctx, "DescribeStream", map[string]any{"StreamName": name})
	var desc struct {
		StreamDescription struct {
			Shards []struct {
				ShardID      string `json:"ShardId"`
				HashKeyRange struct {
					StartingHashKey string `json:"StartingHashKey"`
					EndingHashKey   string `json:"EndingHashKey"`
				} `json:"HashKeyRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(described, &desc), "decode DescribeStream: %s", described)
	require.Len(t, desc.StreamDescription.Shards, 2, "a two-shard stream: %s", described)
	first, second := desc.StreamDescription.Shards[0], desc.StreamDescription.Shards[1]

	// Merge the two shards, then split the shard that is still open afterwards: splitting a shard the
	// merge has just closed would be a sequence AWS refuses, whatever substrate does with it. Substrate
	// widens the surviving shard in place rather than minting a child, so "open" is the test, not
	// "has a parent".
	merged := kinesisWire(t, p, ctx, "MergeShards", map[string]any{
		"StreamName": name, "ShardToMerge": first.ShardID, "AdjacentShardToMerge": second.ShardID,
	})
	t.Run("MergeShards", func(t *testing.T) { wireAssertNoMemberJSON(t, "MergeShards", merged, kinesisBookkeepingMembers) })

	after := kinesisWire(t, p, ctx, "DescribeStream", map[string]any{"StreamName": name})
	var afterDesc struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					EndingSequenceNumber string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(after, &afterDesc), "decode DescribeStream: %s", after)
	child := ""
	for _, shard := range afterDesc.StreamDescription.Shards {
		if shard.SequenceNumberRange.EndingSequenceNumber == "" {
			child = shard.ShardID
		}
	}
	require.NotEmpty(t, child, "MergeShards must leave an open shard: %s", after)

	split := kinesisWire(t, p, ctx, "SplitShard", map[string]any{"StreamName": name, "ShardToSplit": child, "NewStartingHashKey": "1"})
	t.Run("SplitShard", func(t *testing.T) { wireAssertNoMemberJSON(t, "SplitShard", split, kinesisBookkeepingMembers) })
}
