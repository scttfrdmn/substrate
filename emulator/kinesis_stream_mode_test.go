package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Kinesis capacity mode (#1118) and PutRecord/PutRecords' published limits (#1418).

// kinesisModeOf reports the StreamModeDetails.StreamMode both describes answer for one stream,
// requiring them to agree.
func kinesisModeOf(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	ref := map[string]any{"StreamName": name}
	modes := make([]string, 0, 2)
	for op, member := range map[string]string{
		"DescribeStream":        "StreamDescription",
		"DescribeStreamSummary": "StreamDescriptionSummary",
	} {
		shape := kinesisShapesDescribeOf(t, ts, op, member, ref)
		details, ok := shape["StreamModeDetails"].(map[string]any)
		require.Truef(t, ok, "%s carries a StreamModeDetails object: %v", op, shape)
		mode, ok := details["StreamMode"].(string)
		require.Truef(t, ok, "%s StreamModeDetails.StreamMode is a string: %v", op, details)
		modes = append(modes, mode)
	}
	require.Equal(t, modes[0], modes[1], "the two describes report one mode")
	return modes[0]
}

// kinesisShapesDescribeOf posts one describe for ref and returns the shape it wraps.
func kinesisShapesDescribeOf(t *testing.T, ts *emulator.TestServer, op, member string, ref map[string]any) map[string]any {
	t.Helper()
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, op, ref)
	var doc map[string]map[string]any
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &doc), "decode %s: %s", op, raw)
	shape, ok := doc[member]
	require.Truef(t, ok, "%s answers no %s: %s", op, member, raw)
	return shape
}

func TestKinesisMode_CreateStreamRecordsTheMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		body       map[string]any
		wantMode   string
		wantShards float64
	}{
		{
			name:       "an omitted mode is PROVISIONED",
			body:       map[string]any{"ShardCount": 2},
			wantMode:   "PROVISIONED",
			wantShards: 2,
		},
		{
			name:       "an explicit PROVISIONED keeps the requested shard count",
			body:       map[string]any{"ShardCount": 3, "StreamModeDetails": map[string]any{"StreamMode": "PROVISIONED"}},
			wantMode:   "PROVISIONED",
			wantShards: 3,
		},
		{
			name:       "ON_DEMAND starts with four shards",
			body:       map[string]any{"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"}},
			wantMode:   "ON_DEMAND",
			wantShards: 4,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			body := map[string]any{"StreamName": "modes"}
			for k, v := range tc.body {
				body[k] = v
			}
			kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "CreateStream", body)

			assert.Equal(t, tc.wantMode, kinesisModeOf(t, ts, "modes"))
			summary := kinesisShapesDescribeOf(t, ts, "DescribeStreamSummary", "StreamDescriptionSummary",
				map[string]any{"StreamName": "modes"})
			assert.Equal(t, tc.wantShards, summary["OpenShardCount"])
			assert.Equal(t, float64(1024), summary["MaxRecordSizeInKiB"], "an omitted MaxRecordSizeInKiB reads as 1 MiB")
		})
	}
}

func TestKinesisMode_CreateStreamRefusesAnUnusableMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		body map[string]any
	}{
		{"an unknown StreamMode", map[string]any{"StreamModeDetails": map[string]any{"StreamMode": "SERVERLESS"}}},
		{"a StreamModeDetails with no StreamMode", map[string]any{"StreamModeDetails": map[string]any{}}},
		{"ShardCount alongside ON_DEMAND", map[string]any{
			"ShardCount": 2, "StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
		}},
		{"a MaxRecordSizeInKiB below 1024", map[string]any{"MaxRecordSizeInKiB": 1023}},
		{"a MaxRecordSizeInKiB above 10240", map[string]any{"MaxRecordSizeInKiB": 10241}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			body := map[string]any{"StreamName": "refused"}
			for k, v := range tc.body {
				body[k] = v
			}
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "CreateStream", body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "InvalidArgumentException", code, raw)

			status, code, _ = kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "DescribeStreamSummary",
				map[string]any{"StreamName": "refused"})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ResourceNotFoundException", code, "a refused create leaves no stream behind")
		})
	}
}

// kinesisReshardBodies are the three reshards a provisioned stream accepts and an on-demand one
// refuses, each addressed at the first two of four uniform shards.
func kinesisReshardBodies(name string) map[string]map[string]any {
	return map[string]map[string]any{
		"UpdateShardCount": {"StreamName": name, "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"},
		"MergeShards": {
			"StreamName": name, "ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000001",
		},
		"SplitShard": {"StreamName": name, "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "1"},
	}
}

func TestKinesisMode_AReshardOfAnOnDemandStreamIsValidationException(t *testing.T) {
	t.Parallel()
	for op, body := range kinesisReshardBodies("od") {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "CreateStream", map[string]any{
				"StreamName": "od", "StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			})
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op, body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "ValidationException", code, raw)

			// Switched to PROVISIONED, the same reshard is accepted.
			kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "UpdateStreamMode", map[string]any{
				"StreamARN":         kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "od"),
				"StreamModeDetails": map[string]any{"StreamMode": "PROVISIONED"},
			})
			kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, op, body)
		})
	}
}

func TestKinesisMode_UpdateStreamModeSwitchesTheMode(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, "switch", 2)
	arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "switch")

	require.Equal(t, "PROVISIONED", kinesisModeOf(t, ts, "switch"))
	// The second ON_DEMAND is a switch to the mode the stream already has, which changes nothing.
	for _, want := range []string{"ON_DEMAND", "ON_DEMAND", "PROVISIONED"} {
		raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "UpdateStreamMode", map[string]any{
			"StreamARN": arn, "StreamModeDetails": map[string]any{"StreamMode": want},
		})
		assert.Equal(t, "{}", strings.TrimSpace(raw), "UpdateStreamMode answers an empty body")
		assert.Equal(t, want, kinesisModeOf(t, ts, "switch"))
	}
	summary := kinesisShapesDescribeOf(t, ts, "DescribeStreamSummary", "StreamDescriptionSummary",
		map[string]any{"StreamName": "switch"})
	assert.Equal(t, float64(2), summary["OpenShardCount"], "a switch keeps the stream's shards")
}

func TestKinesisMode_UpdateStreamModeRefusals(t *testing.T) {
	t.Parallel()
	arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "present")
	cases := []struct {
		name     string
		body     map[string]any
		wantCode string
	}{
		{"no StreamARN", map[string]any{
			"StreamName": "present", "StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
		}, "InvalidArgumentException"},
		{"a malformed StreamARN", map[string]any{
			"StreamARN": "arn:aws:sqs:us-east-1:123456789012:q", "StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
		}, "InvalidArgumentException"},
		{"no StreamModeDetails", map[string]any{"StreamARN": arn}, "InvalidArgumentException"},
		{"an unknown StreamMode", map[string]any{
			"StreamARN": arn, "StreamModeDetails": map[string]any{"StreamMode": "on-demand"},
		}, "InvalidArgumentException"},
		{"an absent stream", map[string]any{
			"StreamARN":         kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "absent"),
			"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
		}, "ResourceNotFoundException"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, "present", 1)
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "UpdateStreamMode", tc.body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, tc.wantCode, code, raw)
		})
	}
}

func TestKinesisLimits_PutRecordRefusesAPartitionKeyOutOfRange(t *testing.T) {
	t.Parallel()
	data := base64.StdEncoding.EncodeToString([]byte("x"))
	cases := []struct {
		name   string
		key    any
		status int
	}{
		{"an absent key", nil, http.StatusBadRequest},
		{"an empty key", "", http.StatusBadRequest},
		{"one character", "k", http.StatusOK},
		{"256 characters", strings.Repeat("k", 256), http.StatusOK},
		{"256 multi-byte characters", strings.Repeat("é", 256), http.StatusOK},
		{"257 characters", strings.Repeat("k", 257), http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, "keys", 1)
			body := map[string]any{"StreamName": "keys", "Data": data}
			if tc.key != nil {
				body["PartitionKey"] = tc.key
			}
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "PutRecord", body)
			assert.Equal(t, tc.status, status, raw)
			if tc.status != http.StatusOK {
				assert.Equal(t, "InvalidArgumentException", code, raw)
			}
		})
	}
}

// kinesisLimitRecords builds n PutRecords entries of size bytes of data each.
func kinesisLimitRecords(n, size int) []map[string]any {
	data := base64.StdEncoding.EncodeToString(make([]byte, size))
	records := make([]map[string]any, n)
	for i := range records {
		records[i] = map[string]any{"Data": data, "PartitionKey": "k"}
	}
	return records
}

func TestKinesisLimits_PutRecordsChecksItsPublishedLimits(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	cases := []struct {
		name    string
		records []map[string]any
		maxKiB  int
		status  int
	}{
		{"no records", []map[string]any{}, 0, http.StatusBadRequest},
		{"500 records", kinesisLimitRecords(500, 1), 0, http.StatusOK},
		{"501 records", kinesisLimitRecords(501, 1), 0, http.StatusBadRequest},
		{"an entry with no partition key", []map[string]any{{"Data": "eA=="}}, 0, http.StatusBadRequest},
		// The partition key counts toward the record size, so 1 MiB of data plus a one-character key
		// is one byte over the default limit.
		{"a record of exactly 1 MiB with its key", kinesisLimitRecords(1, mib-1), 0, http.StatusOK},
		{"a record one byte over 1 MiB", kinesisLimitRecords(1, mib), 0, http.StatusBadRequest},
		{"a 2 MiB record on a stream created for 2 MiB records", kinesisLimitRecords(1, 2*mib-1), 2048, http.StatusOK},
		{"ten records just under 1 MiB, inside 10 MiB", kinesisLimitRecords(10, mib-1), 0, http.StatusOK},
		{"eleven records just under 1 MiB, over 10 MiB", kinesisLimitRecords(11, mib-1), 0, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			create := map[string]any{"StreamName": "limits", "ShardCount": 1}
			if tc.maxKiB != 0 {
				create["MaxRecordSizeInKiB"] = tc.maxKiB
			}
			kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "CreateStream", create)
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "PutRecords",
				map[string]any{"StreamName": "limits", "Records": tc.records})
			assert.Equal(t, tc.status, status, kinesisLimitsAbbrev(raw))
			if tc.status != http.StatusOK {
				assert.Equal(t, "InvalidArgumentException", code, kinesisLimitsAbbrev(raw))
			}
		})
	}
}

// TestKinesisLimits_TheRequestsOwnLimitsComeBeforeTheStream asserts the order: a request that breaks
// a limit it alone decides is refused for that limit even when its stream does not exist.
func TestKinesisLimits_TheRequestsOwnLimitsComeBeforeTheStream(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "PutRecords",
		map[string]any{"StreamName": "absent", "Records": kinesisLimitRecords(501, 1)})
	assert.Equal(t, http.StatusBadRequest, status, kinesisLimitsAbbrev(raw))
	assert.Equal(t, "InvalidArgumentException", code, kinesisLimitsAbbrev(raw))

	status, code, raw = kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "PutRecord",
		map[string]any{"StreamName": "absent", "Data": "eA=="})
	assert.Equal(t, http.StatusBadRequest, status, raw)
	assert.Equal(t, "InvalidArgumentException", code, raw)
}

// TestKinesisMode_CloudFormationForwardsTheStreamsCapacityProperties: AWS::Kinesis::Stream's
// ShardCount, StreamModeDetails and MaxRecordSizeInKiB reach CreateStream, where the deploy used to
// send ShardCount 1 and nothing else.
func TestKinesisMode_CloudFormationForwardsTheStreamsCapacityProperties(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, props       string
		wantMode          string
		wantShards, wantK float64
	}{
		{"nothing declared", `{"Name":"cfn-plain"}`, "PROVISIONED", 1, 1024},
		{"a shard count", `{"Name":"cfn-shards","ShardCount":3}`, "PROVISIONED", 3, 1024},
		{"on demand", `{"Name":"cfn-od","StreamModeDetails":{"StreamMode":"ON_DEMAND"},"MaxRecordSizeInKiB":2048}`,
			"ON_DEMAND", 4, 2048},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, registry := newFullTestDeployerWithRegistry(t)
			result, err := d.Deploy(t.Context(),
				`{"Resources":{"S":{"Type":"AWS::Kinesis::Stream","Properties":`+tc.props+`}}}`, "kinesis-cfn", nil)
			require.NoError(t, err)
			require.Len(t, result.Resources, 1)
			require.Empty(t, result.Resources[0].Error)

			var name struct {
				Name string `json:"Name"`
			}
			require.NoError(t, json.Unmarshal([]byte(tc.props), &name))
			resp, err := registry.RouteRequest(&emulator.RequestContext{
				RequestID: "test-request", AccountID: "123456789012", Region: "us-east-1",
			}, kinesisRequest(t, "DescribeStreamSummary", map[string]any{"StreamName": name.Name}))
			require.NoError(t, err)
			var out struct {
				Summary struct {
					OpenShardCount     float64 `json:"OpenShardCount"`
					MaxRecordSizeInKiB float64 `json:"MaxRecordSizeInKiB"`
					StreamModeDetails  struct {
						StreamMode string `json:"StreamMode"`
					} `json:"StreamModeDetails"`
				} `json:"StreamDescriptionSummary"`
			}
			require.NoError(t, json.Unmarshal(resp.Body, &out), "%s", resp.Body)
			assert.Equal(t, tc.wantMode, out.Summary.StreamModeDetails.StreamMode)
			assert.Equal(t, tc.wantShards, out.Summary.OpenShardCount)
			assert.Equal(t, tc.wantK, out.Summary.MaxRecordSizeInKiB)
		})
	}
}

// kinesisLimitsAbbrev shortens a response body for an assertion message, since a 500-record
// success answers 500 results.
func kinesisLimitsAbbrev(s string) string {
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}
