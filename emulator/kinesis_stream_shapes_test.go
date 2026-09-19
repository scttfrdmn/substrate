package emulator_test

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The two describe shapes and UpdateShardCount's published bounds — #1076.
//
// Three findings, and the first is the only one a real client cannot work around:
//
//  1. **EnhancedMonitoring was a flat array of names** where API_StreamDescription and
//     API_StreamDescriptionSummary both publish an array of EnhancedMetrics *objects*. An SDK
//     decoding into []types.EnhancedMetrics gets a json.UnmarshalTypeError, so the test that
//     matters is the one that decodes through the published shape rather than one that reads a
//     member out of a map — a map assertion passes against either shape.
//  2. **One builder served both operations** and emitted the union of their members, so
//     DescribeStream answered an OpenShardCount its page does not publish and
//     DescribeStreamSummary answered Shards and HasMoreShards its page does not. The assertion is
//     on the whole *key set* of each body, because a "contains what it should" test cannot see a
//     member that should not be there — and TestKinesisPlugin_CreateAndDescribeStream, which read
//     OpenShardCount out of a StreamDescription, is how the union survived #999.
//  3. **UpdateShardCount read neither of the two members it decoded.** ScalingType is Required: Yes
//     with one valid value and TargetShardCount has a published minimum and a published
//     double/half pair, and all four were accepted as sent.
//
// Requests go over the wire through the #966 helpers rather than through HandleRequest, so the
// bodies under assertion are the ones a caller receives.

// kinesisShapesStream is the stream every case here creates.
const kinesisShapesStream = "shapes"

// kinesisShapesServer starts a server holding one four-shard stream.
//
// Four shards rather than one or two so that the double and half bounds both have room: against 4,
// a target of 8 is the ceiling, 2 is the floor, and 9 and 1 are the refusals.
func kinesisShapesServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisShapesStream, 4)
	return ts
}

// kinesisShapesDescribe posts one describe operation and returns the shape it wraps.
func kinesisShapesDescribe(t *testing.T, ts *emulator.TestServer, op, member string) map[string]any {
	t.Helper()
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, op,
		map[string]any{"StreamName": kinesisShapesStream})

	var doc map[string]json.RawMessage
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &doc), "decode %s: %s", op, raw)
	require.Containsf(t, doc, member, "%s answers no %s: %s", op, member, raw)

	var shape map[string]any
	require.NoErrorf(t, json.Unmarshal(doc[member], &shape), "decode %s.%s: %s", op, member, raw)
	return shape
}

// kinesisShapesKeys reports a body's member names sorted, so a set comparison reads in one line.
func kinesisShapesKeys(shape map[string]any) []string {
	keys := make([]string, 0, len(shape))
	for k := range shape {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestKinesisShapes_TheTwoDescribesAnswerDisjointMembers is finding 2.
//
// The three members that differ are asserted individually as well as through the key set, because
// the key set says "these eight" and the individual assertions say which of them is the finding.
func TestKinesisShapes_TheTwoDescribesAnswerDisjointMembers(t *testing.T) {
	t.Parallel()
	ts := kinesisShapesServer(t)

	description := kinesisShapesDescribe(t, ts, "DescribeStream", "StreamDescription")
	assert.Equal(t, []string{
		"EnhancedMonitoring", "HasMoreShards", "RetentionPeriodHours", "Shards",
		"StreamARN", "StreamCreationTimestamp", "StreamName", "StreamStatus",
	}, kinesisShapesKeys(description),
		"StreamDescription publishes eight Required: Yes members and substrate answers those")
	assert.NotContains(t, description, "OpenShardCount",
		"OpenShardCount is a StreamDescriptionSummary member; API_StreamDescription publishes it nowhere")

	summary := kinesisShapesDescribe(t, ts, "DescribeStreamSummary", "StreamDescriptionSummary")
	assert.Equal(t, []string{
		"EnhancedMonitoring", "OpenShardCount", "RetentionPeriodHours",
		"StreamARN", "StreamCreationTimestamp", "StreamName", "StreamStatus",
	}, kinesisShapesKeys(summary),
		"StreamDescriptionSummary publishes seven Required: Yes members and substrate answers those")
	assert.NotContains(t, summary, "Shards",
		"Shards is a StreamDescription member; the summary publishes a count and no shard list")
	assert.NotContains(t, summary, "HasMoreShards",
		"HasMoreShards pages the shard list the summary does not carry")

	// The counts still agree, so the split did not change what either operation reports about the
	// stream — only which members it reports it through.
	assert.Equal(t, float64(4), summary["OpenShardCount"])
	shards, ok := description["Shards"].([]any)
	require.True(t, ok, "Shards is an array: %v", description["Shards"])
	assert.Len(t, shards, 4)
}

// TestKinesisShapes_EnhancedMonitoringDecodesThroughItsPublishedType is finding 1, asserted the one
// way that can fail against the old shape: by decoding into the published type.
func TestKinesisShapes_EnhancedMonitoringDecodesThroughItsPublishedType(t *testing.T) {
	t.Parallel()
	ts := kinesisShapesServer(t)

	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "EnableEnhancedMonitoring",
		map[string]any{
			"StreamName":        kinesisShapesStream,
			"ShardLevelMetrics": []string{"OutgoingBytes", "IncomingBytes"},
		})

	for _, tc := range []struct{ op, member string }{
		{op: "DescribeStream", member: "StreamDescription"},
		{op: "DescribeStreamSummary", member: "StreamDescriptionSummary"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, tc.op,
				map[string]any{"StreamName": kinesisShapesStream})

			// This is the shape an SDK generates from API_EnhancedMetrics. Against the flat
			// []string substrate answered until #1076 it fails with json.UnmarshalTypeError,
			// which is why a real client could not read the member at all.
			var doc map[string]struct {
				EnhancedMonitoring []struct {
					ShardLevelMetrics []string `json:"ShardLevelMetrics"`
				} `json:"EnhancedMonitoring"`
			}
			require.NoErrorf(t, json.Unmarshal([]byte(raw), &doc),
				"decode %s through the published EnhancedMetrics shape: %s", tc.op, raw)

			metrics := doc[tc.member].EnhancedMonitoring
			require.Lenf(t, metrics, 1, "one EnhancedMetrics object carries the whole set: %s", raw)

			// The order is the pages' own bullet order and not the order the caller sent, so two
			// replays of one request render byte-identical bodies (#999).
			assert.Equal(t, []string{"IncomingBytes", "OutgoingBytes"}, metrics[0].ShardLevelMetrics)
		})
	}
}

// TestKinesisShapes_AStreamWithNothingEnhancedStillCarriesTheMember records the reading #1076 left
// open.
//
// EnhancedMonitoring is Required: Yes on both shapes, so it cannot be dropped; and
// API_EnhancedMetrics publishes "Array Members: Minimum number of 1 item" on ShardLevelMetrics, so
// [{"ShardLevelMetrics": []}] is a shape the model does not permit. An empty outer array is the only
// rendering that satisfies both, and it is [] rather than null per #938.
func TestKinesisShapes_AStreamWithNothingEnhancedStillCarriesTheMember(t *testing.T) {
	t.Parallel()
	ts := kinesisShapesServer(t)

	for _, tc := range []struct{ op, member string }{
		{op: "DescribeStream", member: "StreamDescription"},
		{op: "DescribeStreamSummary", member: "StreamDescriptionSummary"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, tc.op,
				map[string]any{"StreamName": kinesisShapesStream})

			// Asserted on the raw document as well as the decoded value, because an absent member
			// and an empty array decode into the same nil slice.
			assert.Contains(t, raw, `"EnhancedMonitoring":[]`,
				"the member is present and empty, not omitted and not null")

			shape := kinesisShapesDescribe(t, ts, tc.op, tc.member)
			require.Contains(t, shape, "EnhancedMonitoring")
			assert.Empty(t, shape["EnhancedMonitoring"])
			assert.NotNil(t, shape["EnhancedMonitoring"], "[] and not null")
		})
	}
}

// TestKinesisShapes_TheALLWildcardIsExpandedInADescribeToo extends #999's expansion to the two
// describes, which read the stored names through the same set/render pair.
func TestKinesisShapes_TheALLWildcardIsExpandedInADescribeToo(t *testing.T) {
	t.Parallel()
	ts := kinesisShapesServer(t)

	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "EnableEnhancedMonitoring",
		map[string]any{
			"StreamName":        kinesisShapesStream,
			"ShardLevelMetrics": []string{"ALL"},
		})

	summary := kinesisShapesDescribe(t, ts, "DescribeStreamSummary", "StreamDescriptionSummary")
	objects, ok := summary["EnhancedMonitoring"].([]any)
	require.Truef(t, ok, "EnhancedMonitoring is an array of objects: %v", summary["EnhancedMonitoring"])
	require.Len(t, objects, 1)

	object, ok := objects[0].(map[string]any)
	require.Truef(t, ok, "each entry is an EnhancedMetrics object: %v", objects[0])
	assert.Len(t, object["ShardLevelMetrics"], 7,
		"ALL means the seven metrics and never appears in a response")
}

// TestKinesisShapes_UpdateShardCountChecksItsPublishedShape is finding 3.
//
// Every refusal is InvalidArgumentException/400. ValidationException is published on the page but
// its gloss is capacity-mode-specific, so it is not the code for a bad parameter — the reading
// kinesis_errors.go has carried since #950 and which #1118 will give its one site.
func TestKinesisShapes_UpdateShardCountChecksItsPublishedShape(t *testing.T) {
	t.Parallel()
	ts := kinesisShapesServer(t)

	for _, tc := range []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "an absent ScalingType",
			body: map[string]any{"TargetShardCount": 8},
			want: "ScalingType is required",
		},
		{
			name: "a ScalingType outside the one-value enum",
			body: map[string]any{"TargetShardCount": 8, "ScalingType": "uniform_scaling"},
			want: "is not a valid ScalingType",
		},
		{
			name: "an absent TargetShardCount",
			body: map[string]any{"ScalingType": "UNIFORM_SCALING"},
			want: "TargetShardCount is required",
		},
		{
			name: "a TargetShardCount below the published minimum",
			body: map[string]any{"TargetShardCount": 0, "ScalingType": "UNIFORM_SCALING"},
			want: "must be at least 1",
		},
		{
			name: "a TargetShardCount above the published ceiling",
			body: map[string]any{"TargetShardCount": 10001, "ScalingType": "UNIFORM_SCALING"},
			want: "may not exceed 10000 shards",
		},
		{
			name: "more than double the current count",
			body: map[string]any{"TargetShardCount": 9, "ScalingType": "UNIFORM_SCALING"},
			want: "more than double",
		},
		{
			name: "less than half the current count",
			body: map[string]any{"TargetShardCount": 1, "ScalingType": "UNIFORM_SCALING"},
			want: "less than half",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"StreamName": kinesisShapesStream}
			for k, v := range tc.body {
				body[k] = v
			}
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion,
				"UpdateShardCount", body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "InvalidArgumentException", code, raw)
			assert.Contains(t, raw, tc.want, "the message names what is wrong with the request")
		})
	}
}

// TestKinesisShapes_TheBoundsAreInclusive pins the edges, because an off-by-one here refuses a
// request AWS accepts — the more damaging direction of the same defect.
func TestKinesisShapes_TheBoundsAreInclusive(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		target int
	}{
		{name: "exactly double", target: 8},
		{name: "exactly half", target: 2},
		{name: "unchanged", target: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh server per case: the bounds are stated against the *current* count, so a
			// shared stream would make each case's bound depend on the one before it.
			ts := kinesisShapesServer(t)
			raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "UpdateShardCount",
				map[string]any{
					"StreamName":       kinesisShapesStream,
					"TargetShardCount": tc.target,
					"ScalingType":      "UNIFORM_SCALING",
				})

			var out struct {
				CurrentShardCount int `json:"CurrentShardCount"`
				TargetShardCount  int `json:"TargetShardCount"`
			}
			require.NoErrorf(t, json.Unmarshal([]byte(raw), &out), "decode: %s", raw)
			assert.Equal(t, 4, out.CurrentShardCount, "CurrentShardCount is the count before the reshard")
			assert.Equal(t, tc.target, out.TargetShardCount)

			summary := kinesisShapesDescribe(t, ts, "DescribeStreamSummary", "StreamDescriptionSummary")
			assert.Equal(t, float64(tc.target), summary["OpenShardCount"], "the reshard reached the record")
		})
	}
}

// TestKinesisShapes_AnAbsentStreamIsRefusedBeforeItsShapeIsChecked records the precedence, which is
// forced rather than chosen: the double and half bounds are stated against the stream's current
// shard count, so they cannot be evaluated until the record is loaded.
func TestKinesisShapes_AnAbsentStreamIsRefusedBeforeItsShapeIsChecked(t *testing.T) {
	t.Parallel()
	ts := kinesisShapesServer(t)

	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion,
		"UpdateShardCount", map[string]any{
			"StreamName":       "no-such-stream",
			"TargetShardCount": 0,
			"ScalingType":      "",
		})
	assert.Equal(t, http.StatusBadRequest, status, raw)
	assert.Equal(t, "ResourceNotFoundException", code,
		"the stream is resolved first, so a bad shape on an absent stream reports the absence")
}
