package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1613: ListShards.

// ksTestARN is the ARN of a stream the default test credential owns.
func ksTestARN(name string) string { return kinesisStreamARNFor("123456789012", "us-east-1", name) }

// ksListShards issues one ListShards and returns the shard IDs and the NextToken.
func ksListShards(t *testing.T, ts *emulator.TestServer, body map[string]any) ([]string, string) {
	t.Helper()
	out := ksOK(t, ts, "ListShards", body)
	var resp struct {
		Shards []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(out, &resp), "%s", out)
	ids := make([]string, 0, len(resp.Shards))
	for _, s := range resp.Shards {
		ids = append(ids, s.ShardID)
	}
	return ids, resp.NextToken
}

// ksRefusal issues an operation and requires a 400 with the given code.
func ksRefusal(t *testing.T, ts *emulator.TestServer, op string, body map[string]any, code string) {
	t.Helper()
	status, out := ksCall(t, ts, op, body)
	assert.Equal(t, http.StatusBadRequest, status, "%s: %s", op, out)
	assert.Contains(t, string(out), code, "%s", op)
}

func TestKinesisListShards_PagesByTokenAndStartsAfterAShard(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "ls", "ShardCount": 5})

	all, next := ksListShards(t, ts, map[string]any{"StreamName": "ls"})
	assert.Len(t, all, 5)
	assert.Empty(t, next, "one page holds the stream, so no token")

	// A token walk returns every shard once, and the follow-up pages name the stream by token alone.
	var walked []string
	page, token := ksListShards(t, ts, map[string]any{"StreamARN": ksTestARN("ls"), "MaxResults": 2})
	walked = append(walked, page...)
	for token != "" {
		page, token = ksListShards(t, ts, map[string]any{"NextToken": token, "MaxResults": 2})
		walked = append(walked, page...)
	}
	assert.Equal(t, all, walked)

	after, _ := ksListShards(t, ts, map[string]any{"StreamName": "ls", "ExclusiveStartShardId": all[2]})
	assert.Equal(t, all[3:], after)
	afterFilter, _ := ksListShards(t, ts, map[string]any{
		"StreamName": "ls", "ShardFilter": map[string]any{"Type": "AFTER_SHARD_ID", "ShardId": all[0]},
	})
	assert.Equal(t, all[1:], afterFilter)
}

func TestKinesisListShards_Refusals(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "lr", "ShardCount": 3})
	_, token := ksListShards(t, ts, map[string]any{"StreamName": "lr", "MaxResults": 1})
	require.NotEmpty(t, token)

	for name, body := range map[string]map[string]any{
		"NextToken with StreamName":              {"NextToken": token, "StreamName": "lr"},
		"NextToken with ExclusiveStartShardId":   {"NextToken": token, "ExclusiveStartShardId": "shardId-000000000000"},
		"NextToken with StreamCreationTimestamp": {"NextToken": token, "StreamCreationTimestamp": 1},
		"NextToken with another StreamARN":       {"NextToken": token, "StreamARN": ksTestARN("other")},
		"NextToken with another filter":          {"NextToken": token, "ShardFilter": map[string]any{"Type": "AT_LATEST"}},
		"an unissued NextToken":                  {"NextToken": "bm90LWEtdG9rZW4="},
		"MaxResults 0":                           {"StreamName": "lr", "MaxResults": 0},
		"MaxResults 10001":                       {"StreamName": "lr", "MaxResults": 10001},
		"no stream":                              {},
		"a filter with no type":                  {"StreamName": "lr", "ShardFilter": map[string]any{}},
		"an unknown filter type":                 {"StreamName": "lr", "ShardFilter": map[string]any{"Type": "SOMETIME"}},
		"AFTER_SHARD_ID with no ShardId":         {"StreamName": "lr", "ShardFilter": map[string]any{"Type": "AFTER_SHARD_ID"}},
		"AT_TIMESTAMP with no Timestamp":         {"StreamName": "lr", "ShardFilter": map[string]any{"Type": "AT_TIMESTAMP"}},
		"AT_LATEST with a ShardId":               {"StreamName": "lr", "ShardFilter": map[string]any{"Type": "AT_LATEST", "ShardId": "x"}},
	} {
		t.Run(name, func(t *testing.T) { ksRefusal(t, ts, "ListShards", body, "InvalidArgumentException") })
	}
	ksRefusal(t, ts, "ListShards", map[string]any{"StreamName": "missing"}, "ResourceNotFoundException")
	ksRefusal(t, ts, "ListShards", map[string]any{"StreamName": "lr", "StreamCreationTimestamp": 1},
		"ResourceNotFoundException")

	// The same token is accepted with the filter it was issued for, and with the stream's own ARN.
	ids, _ := ksListShards(t, ts, map[string]any{"NextToken": token, "StreamARN": ksTestARN("lr")})
	assert.Equal(t, []string{"shardId-000000000001", "shardId-000000000002"}, ids)
}

func TestKinesisListShards_ShardFilterReadsTheReshardHistory(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTime()
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ts.SetTime(start)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "lf", "ShardCount": 2})

	ts.SetTime(start.Add(time.Hour))
	ksOK(t, ts, "UpdateShardCount", map[string]any{"StreamName": "lf", "TargetShardCount": 4, "ScalingType": "UNIFORM_SCALING"})
	parents := []string{"shardId-000000000000", "shardId-000000000001"}
	children := []string{"shardId-000000000002", "shardId-000000000003", "shardId-000000000004", "shardId-000000000005"}

	ts.SetTime(start.Add(2 * time.Hour))
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "lf-ts"})
	created := float64(start.Add(2 * time.Hour).Unix())
	ksOK(t, ts, "ListShards", map[string]any{"StreamName": "lf-ts", "StreamCreationTimestamp": created})

	for _, tc := range []struct {
		name   string
		filter map[string]any
		want   []string
	}{
		{"no filter lists the retention period", nil, append(append([]string{}, parents...), children...)},
		{"AT_LATEST lists the open shards", map[string]any{"Type": "AT_LATEST"}, children},
		{"AT_TRIM_HORIZON lists what was open at the trim horizon", map[string]any{"Type": "AT_TRIM_HORIZON"}, parents},
		{"AT_TIMESTAMP before the reshard", map[string]any{
			"Type": "AT_TIMESTAMP", "Timestamp": float64(start.Add(30 * time.Minute).Unix()),
		}, parents},
		{"AT_TIMESTAMP after the reshard", map[string]any{
			"Type": "AT_TIMESTAMP", "Timestamp": float64(start.Add(90 * time.Minute).Unix()),
		}, children},
		{"FROM_TIMESTAMP after the reshard drops the closed parents", map[string]any{
			"Type": "FROM_TIMESTAMP", "Timestamp": float64(start.Add(90 * time.Minute).Unix()),
		}, children},
	} {
		body := map[string]any{"StreamName": "lf"}
		if tc.filter != nil {
			body["ShardFilter"] = tc.filter
		}
		ids, _ := ksListShards(t, ts, body)
		assert.Equal(t, tc.want, ids, tc.name)
	}

	// Once the parents' end falls outside the 24-hour retention period, the default listing drops
	// them while DescribeStream still reports them.
	ts.SetTime(start.Add(26 * time.Hour))
	ids, _ := ksListShards(t, ts, map[string]any{"StreamName": "lf"})
	assert.Equal(t, children, ids)
	ids, _ = ksListShards(t, ts, map[string]any{"StreamName": "lf", "ShardFilter": map[string]any{"Type": "AT_TRIM_HORIZON"}})
	assert.Equal(t, children, ids)
	out := ksOK(t, ts, "DescribeStream", map[string]any{"StreamName": "lf"})
	assert.Contains(t, string(out), parents[0])
}
