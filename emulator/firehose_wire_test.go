package emulator_test

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestFirehoseWire_DeliveryStreamResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for FirehoseDeliveryStream (#756), and pins the
// published form of its CreateTimestamp (#1305).
//
// FirehoseDeliveryStream declares AccountId and Region under wire-visible `json` tags, neither under
// omitempty, so a stream that exists holds both and no absence below is vacuous.
// DescribeDeliveryStream used to answer the record whole; it answers emulator/firehose_wire.go's
// projection now. Every routed operation is driven.
func TestFirehoseWire_DeliveryStreamResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.FirehosePlugin{}
	ctx, state := wireSetup(t, p, "req-firehose-wire")
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "firehose", "Firehose_20150804", op, body)
	}

	stream := map[string]any{"DeliveryStreamName": "wire-stream"}
	created := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "wire-stream", "Tags": map[string]string{"team": "wire"}})
	wireRequireHeld(t, state, "firehose", "stream:123456789012/us-east-1/wire-stream", "AccountId", "Region")
	described := call("DescribeDeliveryStream", stream)

	wireRunJSON(t, []string{"AccountID", "Region"}, []wireCase{
		{op: "CreateDeliveryStream", held: created, anchor: `"DeliveryStreamARN"`},
		{op: "DescribeDeliveryStream", held: described, anchor: `"DeliveryStreamName":"wire-stream"`},
		{op: "ListDeliveryStreams", call: func() []byte { return call("ListDeliveryStreams", nil) }, anchor: "wire-stream"},
		{op: "PutRecord", call: func() []byte {
			return call("PutRecord", map[string]any{"DeliveryStreamName": "wire-stream", "Record": map[string]string{"Data": "aGVsbG8="}})
		}, anchor: `"RecordId"`},
		{op: "PutRecordBatch", call: func() []byte {
			return call("PutRecordBatch", map[string]any{"DeliveryStreamName": "wire-stream", "Records": []map[string]string{{"Data": "aGVsbG8="}}})
		}, anchor: `"RequestResponses"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteDeliveryStream", call: func() []byte { return call("DeleteDeliveryStream", stream) }, anchor: "{}"},
	})

	// After the walk. API_DeliveryStreamDescription publishes CreateTimestamp as a Timestamp, a number
	// under awsJson1_1 (#1305), and publishes no Tags: a stream's tags are ListTagsForDeliveryStream's.
	var out struct {
		DeliveryStreamDescription map[string]json.RawMessage `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(described, &out), "decode DescribeDeliveryStream: %s", described)
	var stamp float64
	require.NoErrorf(t, json.Unmarshal(out.DeliveryStreamDescription["CreateTimestamp"], &stamp),
		"CreateTimestamp must be epoch seconds, a JSON number: %s", described)
	require.Positive(t, stamp, "CreateTimestamp: %s", described)
	require.NotContains(t, slices.Sorted(maps.Keys(out.DeliveryStreamDescription)), "Tags",
		"API_DeliveryStreamDescription publishes no Tags: %s", described)
}
