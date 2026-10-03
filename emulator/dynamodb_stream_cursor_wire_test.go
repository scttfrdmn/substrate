package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestDynamoDBStreamsWire_CursorReachesResponsesOnlyAsAToken is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for DynamoDBStreamCursor (#756).
//
// DynamoDBStreamCursor does reach an AWS body, unlike the records in
// scripts/wire-bookkeeping-internal.txt: GetShardIterator and GetRecords marshal it, base64-encode it
// and answer it as the opaque ShardIterator and NextShardIterator strings. A real shard iterator is
// opaque too, so carrying the account and Region inside the token is faithful, and it is what stops
// an iterator from being redeemed against another account's or Region's table (#943). What must hold
// is that the cursor is never rendered as an object. So each token is decoded first, proving the
// account and Region are really in it, and then each document is walked for either member at any
// depth and the tokens are required to be strings.
func TestDynamoDBStreamsWire_CursorReachesResponsesOnlyAsAToken(t *testing.T) {
	t.Parallel()
	p := &emulator.DynamoDBPlugin{}
	ctx, _ := wireSetup(t, p, "req-ddbstreams-wire")
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "dynamodb", "DynamoDB_20120810", op, body)
	}

	created := call("CreateTable", map[string]any{
		"TableName":            "wire-table",
		"AttributeDefinitions": []map[string]any{{"AttributeName": "pk", "AttributeType": "S"}},
		"KeySchema":            []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
		"BillingMode":          "PAY_PER_REQUEST",
		"StreamSpecification":  map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
	})
	var table struct {
		TableDescription struct {
			LatestStreamArn string `json:"LatestStreamArn"`
		} `json:"TableDescription"`
	}
	require.NoError(t, json.Unmarshal(created, &table), "decode CreateTable: %s", created)
	streamArn := table.TableDescription.LatestStreamArn
	require.NotEmpty(t, streamArn, "CreateTable must report a stream ARN: %s", created)
	call("PutItem", map[string]any{"TableName": "wire-table", "Item": map[string]any{"pk": map[string]any{"S": "k1"}}})

	described := call("DescribeStream", map[string]any{"StreamArn": streamArn})
	var stream struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(described, &stream), "decode DescribeStream: %s", described)
	require.NotEmpty(t, stream.StreamDescription.Shards, "DescribeStream: %s", described)

	iterBody := call("GetShardIterator", map[string]any{
		"StreamArn": streamArn, "ShardId": stream.StreamDescription.Shards[0].ShardID, "ShardIteratorType": "TRIM_HORIZON",
	})
	iterator := ddbStreamsWireToken(t, "GetShardIterator", iterBody, "ShardIterator")
	records := call("GetRecords", map[string]any{"ShardIterator": iterator})
	ddbStreamsWireToken(t, "GetRecords", records, "NextShardIterator")

	members := []string{"AccountID", "Region"}
	for op, body := range map[string][]byte{"GetShardIterator": iterBody, "GetRecords": records} {
		t.Run(op, func(t *testing.T) {
			wireAssertNoMemberJSON(t, op, body, members)
		})
	}
}

// ddbStreamsWireToken requires that member is a JSON string in body, decodes it as a cursor, and
// requires the cursor to carry the account and Region, so the walk's absence means the cursor stayed
// inside the token rather than that it held nothing. It returns the token.
func ddbStreamsWireToken(t *testing.T, op string, body []byte, member string) string {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc), "decode %s: %s", op, body)
	var token string
	require.NoErrorf(t, json.Unmarshal(doc[member], &token), "%s must answer %s as an opaque string: %s", op, member, body)
	raw, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "%s's %s is base64", op, member)
	var cursor map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &cursor), "decode %s's cursor: %s", op, raw)
	require.JSONEqf(t, `"123456789012"`, string(cursor["accountId"]), "%s's cursor must carry accountId: %s", op, raw)
	require.JSONEqf(t, `"us-east-1"`, string(cursor["region"]), "%s's cursor must carry region: %s", op, raw)
	return token
}
