package emulator_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// MergeShards and SplitShard act on the shards they name, ListStreams pages, and DeleteStream returns
// its store errors (#1399). Every assertion reads the raw DescribeStream body, so a parent or a hash
// range dropped on the way through is visible rather than decoded away.

// kinesisReshardShard is one element of DescribeStream's Shards, as published by API_Shard.
type kinesisReshardShard struct {
	ShardID               string `json:"ShardId"`
	ParentShardID         string `json:"ParentShardId"`
	AdjacentParentShardID string `json:"AdjacentParentShardId"`
	HashKeyRange          struct {
		StartingHashKey string `json:"StartingHashKey"`
		EndingHashKey   string `json:"EndingHashKey"`
	} `json:"HashKeyRange"`
	SequenceNumberRange struct {
		StartingSequenceNumber string `json:"StartingSequenceNumber"`
		EndingSequenceNumber   string `json:"EndingSequenceNumber"`
	} `json:"SequenceNumberRange"`
}

// kinesisReshardShards describes the stream and returns its shards by ID.
func kinesisReshardShards(t *testing.T, p *emulator.KinesisPlugin, ctx *emulator.RequestContext, name string) map[string]kinesisReshardShard {
	t.Helper()
	raw := kinesisWire(t, p, ctx, "DescribeStream", map[string]any{"StreamName": name})
	var out struct {
		StreamDescription struct {
			Shards []kinesisReshardShard `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode DescribeStream: %s", raw)
	shards := make(map[string]kinesisReshardShard, len(out.StreamDescription.Shards))
	for _, s := range out.StreamDescription.Shards {
		shards[s.ShardID] = s
	}
	return shards
}

// kinesisReshardOpenCount reads DescribeStreamSummary's OpenShardCount.
func kinesisReshardOpenCount(t *testing.T, p *emulator.KinesisPlugin, ctx *emulator.RequestContext, name string) int {
	t.Helper()
	raw := kinesisWire(t, p, ctx, "DescribeStreamSummary", map[string]any{"StreamName": name})
	var out struct {
		StreamDescriptionSummary struct {
			OpenShardCount int `json:"OpenShardCount"`
		} `json:"StreamDescriptionSummary"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode DescribeStreamSummary: %s", raw)
	return out.StreamDescriptionSummary.OpenShardCount
}

// kinesisMaxHashKey is 2^128-1, the top of a stream's hash key space.
func kinesisMaxHashKey() string {
	return new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1)).String()
}

// kinesisHalfHashKey is 2^127, the split key API_SplitShard's own sample would use for an even split.
func kinesisHalfHashKey() string { return new(big.Int).Lsh(big.NewInt(1), 127).String() }

func TestKinesisResharding_CreateDividesTheWholeHashKeySpace(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "space", "ShardCount": 2})

	shards := kinesisReshardShards(t, p, ctx, "space")
	require.Len(t, shards, 2)
	first, second := shards["shardId-000000000000"], shards["shardId-000000000001"]
	assert.Equal(t, "0", first.HashKeyRange.StartingHashKey)
	assert.Equal(t, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1)).String(), first.HashKeyRange.EndingHashKey)
	assert.Equal(t, kinesisHalfHashKey(), second.HashKeyRange.StartingHashKey)
	assert.Equal(t, kinesisMaxHashKey(), second.HashKeyRange.EndingHashKey, "the last shard reaches 2^128-1")
}

func TestKinesisResharding_MergeClosesBothParentsAndOpensOneChild(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "merge", "ShardCount": 2})
	kinesisWire(t, p, ctx, "MergeShards", map[string]any{
		"StreamName": "merge", "ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000001",
	})

	shards := kinesisReshardShards(t, p, ctx, "merge")
	require.Len(t, shards, 3, "both parents stay listed, closed, beside the child: %v", shards)
	for _, id := range []string{"shardId-000000000000", "shardId-000000000001"} {
		assert.NotEmptyf(t, shards[id].SequenceNumberRange.EndingSequenceNumber, "parent %s must be closed", id)
	}
	child := shards["shardId-000000000002"]
	assert.Empty(t, child.SequenceNumberRange.EndingSequenceNumber, "the child is open")
	assert.Equal(t, "shardId-000000000000", child.ParentShardID, "ParentShardId is ShardToMerge")
	assert.Equal(t, "shardId-000000000001", child.AdjacentParentShardID, "AdjacentParentShardId is AdjacentShardToMerge")
	assert.Equal(t, "0", child.HashKeyRange.StartingHashKey, "the child covers the union of both ranges")
	assert.Equal(t, kinesisMaxHashKey(), child.HashKeyRange.EndingHashKey)
	assert.Equal(t, 1, kinesisReshardOpenCount(t, p, ctx, "merge"))
}

func TestKinesisResharding_SplitClosesTheParentAndOpensTwoChildren(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "split", "ShardCount": 1})
	kinesisWire(t, p, ctx, "SplitShard", map[string]any{
		"StreamName": "split", "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": kinesisHalfHashKey(),
	})

	shards := kinesisReshardShards(t, p, ctx, "split")
	require.Len(t, shards, 3, "%v", shards)
	assert.NotEmpty(t, shards["shardId-000000000000"].SequenceNumberRange.EndingSequenceNumber, "the parent is closed")
	lower, upper := shards["shardId-000000000001"], shards["shardId-000000000002"]
	for _, c := range []kinesisReshardShard{lower, upper} {
		assert.Equal(t, "shardId-000000000000", c.ParentShardID, "%s's parent", c.ShardID)
		assert.Empty(t, c.AdjacentParentShardID, "a split child has one parent")
		assert.Empty(t, c.SequenceNumberRange.EndingSequenceNumber, "%s is open", c.ShardID)
	}
	half := new(big.Int).Lsh(big.NewInt(1), 127)
	assert.Equal(t, "0", lower.HashKeyRange.StartingHashKey)
	assert.Equal(t, new(big.Int).Sub(half, big.NewInt(1)).String(), lower.HashKeyRange.EndingHashKey,
		"the keys below NewStartingHashKey go to one child")
	assert.Equal(t, half.String(), upper.HashKeyRange.StartingHashKey, "NewStartingHashKey and above go to the other")
	assert.Equal(t, kinesisMaxHashKey(), upper.HashKeyRange.EndingHashKey)
	assert.Equal(t, 2, kinesisReshardOpenCount(t, p, ctx, "split"))
}

func TestKinesisResharding_ARecordLandsOnAnOpenShard(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 2})
	kinesisWire(t, p, ctx, "MergeShards", map[string]any{
		"StreamName": "route", "ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000001",
	})

	put := kinesisWire(t, p, ctx, "PutRecord", map[string]any{"StreamName": "route", "PartitionKey": "k", "Data": "d2lyZQ=="})
	assert.Contains(t, string(put), `"ShardId":"shardId-000000000002"`, "a closed parent takes no more records: %s", put)
	puts := kinesisWire(t, p, ctx, "PutRecords", map[string]any{"StreamName": "route", "Records": []map[string]any{
		{"PartitionKey": "a", "Data": "d2lyZQ=="}, {"PartitionKey": "b", "Data": "d2lyZQ=="},
	}})
	assert.NotContains(t, string(puts), "shardId-000000000000", "PutRecords spreads over open shards only: %s", puts)
	assert.NotContains(t, string(puts), "shardId-000000000001", "PutRecords spreads over open shards only: %s", puts)
}

func TestKinesisResharding_UpdateShardCountContinuesTheShardIDs(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "uniform", "ShardCount": 1})
	kinesisWire(t, p, ctx, "SplitShard", map[string]any{
		"StreamName": "uniform", "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": kinesisHalfHashKey(),
	})
	kinesisWire(t, p, ctx, "UpdateShardCount", map[string]any{"StreamName": "uniform", "TargetShardCount": 4, "ScalingType": "UNIFORM_SCALING"})

	shards := kinesisReshardShards(t, p, ctx, "uniform")
	require.Len(t, shards, 7, "three shards from the split, four new: %v", shards)
	for i := 0; i <= 2; i++ {
		assert.NotEmptyf(t, shards[fmt.Sprintf("shardId-%012d", i)].SequenceNumberRange.EndingSequenceNumber, "shard %d is closed", i)
	}
	for i := 3; i <= 6; i++ {
		assert.Emptyf(t, shards[fmt.Sprintf("shardId-%012d", i)].SequenceNumberRange.EndingSequenceNumber, "shard %d is open", i)
	}
	assert.Equal(t, 4, kinesisReshardOpenCount(t, p, ctx, "uniform"))
}

func TestKinesisResharding_RefusesWhatThePagesRefuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		shards int
		prep   func(t *testing.T, p *emulator.KinesisPlugin, ctx *emulator.RequestContext)
		op     string
		body   map[string]any
		code   string
	}{
		{"merge of non-adjacent shards", 3, nil, "MergeShards",
			map[string]any{"ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000002"}, "InvalidArgumentException"},
		{"merge of one shard with itself", 2, nil, "MergeShards",
			map[string]any{"ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000000"}, "InvalidArgumentException"},
		{"merge naming a shard the stream does not hold", 2, nil, "MergeShards",
			map[string]any{"ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000009"}, "ResourceNotFoundException"},
		{"merge without AdjacentShardToMerge", 2, nil, "MergeShards",
			map[string]any{"ShardToMerge": "shardId-000000000000"}, "InvalidArgumentException"},
		{"split of a closed shard", 1, func(t *testing.T, p *emulator.KinesisPlugin, ctx *emulator.RequestContext) {
			kinesisWire(t, p, ctx, "SplitShard", map[string]any{"StreamName": "r", "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": kinesisHalfHashKey()})
		}, "SplitShard",
			map[string]any{"ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "10"}, "InvalidArgumentException"},
		{"split at the shard's own starting key", 1, nil, "SplitShard",
			map[string]any{"ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "0"}, "InvalidArgumentException"},
		{"split above the shard's range", 2, nil, "SplitShard",
			map[string]any{"ShardToSplit": "shardId-000000000000", "NewStartingHashKey": kinesisMaxHashKey()}, "InvalidArgumentException"},
		{"split key outside the published pattern", 1, nil, "SplitShard",
			map[string]any{"ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "010"}, "InvalidArgumentException"},
		{"split of a shard the stream does not hold", 1, nil, "SplitShard",
			map[string]any{"ShardToSplit": "shardId-000000000007", "NewStartingHashKey": "10"}, "ResourceNotFoundException"},
		{"split without NewStartingHashKey", 1, nil, "SplitShard",
			map[string]any{"ShardToSplit": "shardId-000000000000"}, "InvalidArgumentException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx, _ := setupKinesisWirePlugin(t)
			kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "r", "ShardCount": tc.shards})
			if tc.prep != nil {
				tc.prep(t, p, ctx)
			}
			before := kinesisReshardShards(t, p, ctx, "r")

			tc.body["StreamName"] = "r"
			_, err := p.HandleRequest(ctx, kinesisRequest(t, tc.op, tc.body))
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%s must be refused, got %v", tc.name, err)
			assert.Equal(t, tc.code, awsErr.Code, "%s: %s", tc.name, awsErr.Message)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus, "%s", tc.name)
			assert.Equal(t, before, kinesisReshardShards(t, p, ctx, "r"), "a refused %s must change no shard", tc.op)
		})
	}
}

func TestKinesisResharding_ListStreamsPages(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	for _, name := range []string{"c", "a", "e", "b", "d"} {
		kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": name, "ShardCount": 1})
	}
	list := func(body map[string]any) ([]string, bool) {
		t.Helper()
		raw := kinesisWire(t, p, ctx, "ListStreams", body)
		var out struct {
			StreamNames    []string `json:"StreamNames"`
			HasMoreStreams bool     `json:"HasMoreStreams"`
		}
		require.NoError(t, json.Unmarshal(raw, &out), "decode ListStreams: %s", raw)
		return out.StreamNames, out.HasMoreStreams
	}

	names, more := list(map[string]any{"Limit": 2})
	assert.Equal(t, []string{"a", "b"}, names)
	assert.True(t, more)
	names, more = list(map[string]any{"Limit": 2, "ExclusiveStartStreamName": "b"})
	assert.Equal(t, []string{"c", "d"}, names, "the page starts after the named stream")
	assert.True(t, more)
	names, more = list(map[string]any{"Limit": 2, "ExclusiveStartStreamName": "d"})
	assert.Equal(t, []string{"e"}, names)
	assert.False(t, more, "the last page reports no more")
	names, more = list(map[string]any{})
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, names, "the default limit is 100")
	assert.False(t, more)

	for _, limit := range []int{-1, 10001} {
		_, err := p.HandleRequest(ctx, kinesisRequest(t, "ListStreams", map[string]any{"Limit": limit}))
		var awsErr *emulator.AWSError
		require.Truef(t, errors.As(err, &awsErr), "Limit %d must be refused", limit)
		assert.Equal(t, "InvalidArgumentException", awsErr.Code)
		assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
	}
}

func TestKinesisResharding_ListStreamsCapsALimitAboveOneHundred(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	for i := range 101 {
		kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": fmt.Sprintf("s%03d", i), "ShardCount": 1})
	}
	raw := kinesisWire(t, p, ctx, "ListStreams", map[string]any{"Limit": 500})
	var out struct {
		StreamNames    []string `json:"StreamNames"`
		HasMoreStreams bool     `json:"HasMoreStreams"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Len(t, out.StreamNames, 100, `"at most 100 results are returned"`)
	assert.True(t, out.HasMoreStreams)
}

// A store fault deleting a shard's records is an error, not a stream reported deleted with its
// records left behind.
func TestKinesisResharding_DeleteStreamReturnsItsStoreErrors(t *testing.T) {
	t.Parallel()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	tc := emulator.NewTimeController(time.Unix(1700000000, 0).UTC())
	tc.Freeze()
	p := &emulator.KinesisPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State: fault, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-ks-fault", IDs: emulator.NewIDMint("req-ks-fault")}
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "gone", "ShardCount": 1})

	fault.failDelete = "record:"
	_, err := p.HandleRequest(ctx, kinesisRequest(t, "DeleteStream", map[string]any{"StreamName": "gone"}))
	require.Error(t, err, "a failed record delete must fail the DeleteStream")
	var awsErr *emulator.AWSError
	assert.False(t, errors.As(err, &awsErr), "a store fault is not a published refusal: %v", err)
}
