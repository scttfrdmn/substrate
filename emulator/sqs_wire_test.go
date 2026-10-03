package emulator_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestSQSWire_QueueResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for SQSQueue (#756).
//
// SQSQueue's one bookkeeping member is EverTagged, as `ever_tagged,omitempty`, set only by a tag write
// (#938), so the queue is tagged and the flag read back before any response is walked (#1304). Every
// operation that answers the queue is driven through the JSON protocol.
func TestSQSWire_QueueResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.SQSPlugin{}
	ctx, state := wireSetup(t, p, "req-sqs-wire")
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "sqs", "AmazonSQS", op, body)
	}

	created := call("CreateQueue", map[string]any{"QueueName": "wire-queue"})
	var out struct {
		QueueURL string `json:"QueueUrl"`
	}
	require.NoError(t, json.Unmarshal(created, &out), "decode CreateQueue: %s", created)
	queue := map[string]any{"QueueUrl": out.QueueURL}

	tagged := call("TagQueue", map[string]any{"QueueUrl": out.QueueURL, "Tags": map[string]string{"team": "wire"}})
	require.JSONEq(t, "true", string(wireRecordByPrefix(t, state, "sqs", "queue:")["ever_tagged"]),
		"the queue must persist ever_tagged before an absence assertion on it means anything")

	wireRunJSON(t, []string{"EverTagged", "ever_tagged"}, []wireCase{
		{op: "CreateQueue", held: created, anchor: "wire-queue"},
		{op: "TagQueue", held: tagged, anchor: ""},
		{op: "GetQueueUrl", call: func() []byte { return call("GetQueueUrl", map[string]any{"QueueName": "wire-queue"}) }, anchor: "wire-queue"},
		{op: "ListQueues", call: func() []byte { return call("ListQueues", nil) }, anchor: "wire-queue"},
		{op: "GetQueueAttributes", call: func() []byte {
			return call("GetQueueAttributes", map[string]any{"QueueUrl": out.QueueURL, "AttributeNames": []string{"All"}})
		}, anchor: `"QueueArn"`},
		{op: "SetQueueAttributes", call: func() []byte {
			return call("SetQueueAttributes", map[string]any{"QueueUrl": out.QueueURL, "Attributes": map[string]string{"VisibilityTimeout": "60"}})
		}, anchor: ""},
		{op: "ListQueueTags", call: func() []byte { return call("ListQueueTags", queue) }, anchor: `"team"`},
		{op: "UntagQueue", call: func() []byte {
			return call("UntagQueue", map[string]any{"QueueUrl": out.QueueURL, "TagKeys": []string{"team"}})
		}, anchor: ""},
		// Last: it removes the record every case above reads.
		{op: "DeleteQueue", call: func() []byte { return call("DeleteQueue", queue) }, anchor: ""},
	})
}
