package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A record is routed to the open shard whose hash key range contains MD5(PartitionKey), or its
// ExplicitHashKey (#1409). The hash keys below are precomputed outside Go:
// int(hashlib.md5(key.encode()).hexdigest(), 16). A four-shard stream divides 0..2^128-1 into four
// ranges 2^126 wide, so a key's shard is its hash key's top two bits.

// kinesisRoutingCases are partition keys with their precomputed MD5 hash keys and four-shard shard.
var kinesisRoutingCases = []struct {
	key   string
	md5   string
	shard string
}{
	{"alpha", "58606826522850340945736240254012750329", "shardId-000000000000"},
	{"delta", "132573211365730055574155209150136284695", "shardId-000000000001"},
	{"charlie", "254503635870842867121759496893308434743", "shardId-000000000002"},
	{"bravo", "337097949882550486299015321206604644431", "shardId-000000000003"},
}

func TestKinesisRouting_PutRecordAnswersTheShardThePartitionKeyHashesInto(t *testing.T) {
	t.Parallel()
	for _, tc := range kinesisRoutingCases {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			p, ctx, _ := setupKinesisWirePlugin(t)
			kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 4})

			raw := kinesisWire(t, p, ctx, "PutRecord", map[string]any{"StreamName": "route", "PartitionKey": tc.key, "Data": "ZA=="})
			var out struct {
				ShardID string `json:"ShardId"`
			}
			require.NoError(t, json.Unmarshal(raw, &out), "decode PutRecord: %s", raw)
			assert.Equal(t, tc.shard, out.ShardID, "MD5(%q) = %s", tc.key, tc.md5)
			assert.Equal(t, []string{tc.key}, kinesisRoutingShardKeys(t, p, ctx, "route", tc.shard),
				"the record is stored on the shard the response names")
		})
	}
}

func TestKinesisRouting_ExplicitHashKeyOverridesThePartitionKey(t *testing.T) {
	t.Parallel()
	for _, tc := range kinesisRoutingCases {
		t.Run(tc.shard, func(t *testing.T) {
			t.Parallel()
			p, ctx, _ := setupKinesisWirePlugin(t)
			kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 4})

			// "alpha" hashes into shard 0; the explicit key, another case's MD5, decides instead.
			raw := kinesisWire(t, p, ctx, "PutRecord", map[string]any{
				"StreamName": "route", "PartitionKey": "alpha", "ExplicitHashKey": tc.md5, "Data": "ZA==",
			})
			assert.Contains(t, string(raw), `"ShardId":"`+tc.shard+`"`, "ExplicitHashKey %s: %s", tc.md5, raw)
		})
	}
}

func TestKinesisRouting_PutRecordsAnswersEachRecordsOwnShard(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 4})

	records := make([]map[string]any, 0, len(kinesisRoutingCases)+1)
	for _, tc := range kinesisRoutingCases {
		records = append(records, map[string]any{"PartitionKey": tc.key, "Data": "ZA=="})
	}
	records = append(records, map[string]any{"PartitionKey": "bravo", "ExplicitHashKey": "0", "Data": "ZA=="})
	raw := kinesisWire(t, p, ctx, "PutRecords", map[string]any{"StreamName": "route", "Records": records})
	var out struct {
		Records []struct {
			ShardID string `json:"ShardId"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode PutRecords: %s", raw)
	require.Len(t, out.Records, len(records), "one result per record: %s", raw)
	for i, tc := range kinesisRoutingCases {
		assert.Equal(t, tc.shard, out.Records[i].ShardID, "record %d, MD5(%q) = %s", i, tc.key, tc.md5)
	}
	assert.Equal(t, "shardId-000000000000", out.Records[len(records)-1].ShardID, "ExplicitHashKey 0 is shard 0's")
}

func TestKinesisRouting_ARecordFollowsTheChildThatHoldsItsHashKey(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupKinesisWirePlugin(t)
	kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 1})
	kinesisWire(t, p, ctx, "SplitShard", map[string]any{
		"StreamName": "route", "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": kinesisHalfHashKey(),
	})

	// The split leaves shard 1 holding 0..2^127-1 and shard 2 holding 2^127..2^128-1.
	for key, want := range map[string]string{"alpha": "shardId-000000000001", "bravo": "shardId-000000000002"} {
		raw := kinesisWire(t, p, ctx, "PutRecord", map[string]any{"StreamName": "route", "PartitionKey": key, "Data": "ZA=="})
		assert.Contains(t, string(raw), `"ShardId":"`+want+`"`, "%q after the split: %s", key, raw)
	}
}

func TestKinesisRouting_RefusesAnExplicitHashKeyNoShardCanHold(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		key  string
	}{
		{"leading zero", "01"},
		{"not decimal", "0x10"},
		{"negative", "-1"},
		{"forty digits", "1000000000000000000000000000000000000000"},
		{"2^128, one past the largest hash key", "340282366920938463463374607431768211456"},
	} {
		for _, op := range []string{"PutRecord", "PutRecords"} {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				p, ctx, _ := setupKinesisWirePlugin(t)
				kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 2})

				record := map[string]any{"PartitionKey": "alpha", "ExplicitHashKey": tc.key, "Data": "ZA=="}
				body := map[string]any{"StreamName": "route"}
				if op == "PutRecord" {
					for k, v := range record {
						body[k] = v
					}
				} else {
					body["Records"] = []map[string]any{{"PartitionKey": "bravo", "Data": "ZA=="}, record}
				}
				_, err := p.HandleRequest(ctx, kinesisRequest(t, op, body))
				var awsErr *emulator.AWSError
				require.Truef(t, errors.As(err, &awsErr), "ExplicitHashKey %q must be refused, got %v", tc.key, err)
				assert.Equal(t, "InvalidArgumentException", awsErr.Code, "%s", awsErr.Message)
				assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
				for _, shard := range []string{"shardId-000000000000", "shardId-000000000001"} {
					assert.Empty(t, kinesisRoutingShardKeys(t, p, ctx, "route", shard), "a refused %s stores no record", op)
				}
			})
		}
	}
}

func TestKinesisRouting_TheSameRecordsLandOnTheSameShardsInEveryRun(t *testing.T) {
	t.Parallel()
	run := func() []byte {
		p, ctx, _ := setupKinesisWirePlugin(t)
		kinesisWire(t, p, ctx, "CreateStream", map[string]any{"StreamName": "route", "ShardCount": 3})
		records := make([]map[string]any, 0, 8)
		for _, key := range []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"} {
			records = append(records, map[string]any{"PartitionKey": key, "Data": "ZA=="})
		}
		raw := kinesisWire(t, p, ctx, "PutRecords", map[string]any{"StreamName": "route", "Records": records})
		var out struct {
			Records []struct {
				ShardID string `json:"ShardId"`
			} `json:"Records"`
		}
		require.NoError(t, json.Unmarshal(raw, &out), "decode PutRecords: %s", raw)
		shards, err := json.Marshal(out.Records)
		require.NoError(t, err)
		return shards
	}
	assert.Equal(t, run(), run(), "routing depends only on the keys")
}

// kinesisRoutingShardKeys reads a shard from TRIM_HORIZON and returns its records' partition keys.
func kinesisRoutingShardKeys(t *testing.T, p *emulator.KinesisPlugin, ctx *emulator.RequestContext, stream, shard string) []string {
	t.Helper()
	raw := kinesisWire(t, p, ctx, "GetShardIterator", map[string]any{
		"StreamName": stream, "ShardId": shard, "ShardIteratorType": "TRIM_HORIZON",
	})
	var it struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(raw, &it), "decode GetShardIterator: %s", raw)
	raw = kinesisWire(t, p, ctx, "GetRecords", map[string]any{"ShardIterator": it.ShardIterator})
	var out struct {
		Records []struct {
			PartitionKey string `json:"PartitionKey"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode GetRecords: %s", raw)
	keys := []string{}
	for _, r := range out.Records {
		keys = append(keys, r.PartitionKey)
	}
	return keys
}
