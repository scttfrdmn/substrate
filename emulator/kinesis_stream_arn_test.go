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

// Kinesis stream addressing by ARN — #966.
//
// Fifteen of the seventeen operations substrate models publish StreamARN alongside StreamName, both
// Required: No, under a Note that is byte-identical on every one of their pages: "you must use either
// the StreamARN or the StreamName parameter, or both. It is recommended that you use the StreamARN
// input parameter." Substrate decoded only StreamName, so the recommended form — the one an SDK sends
// when its client was built from an ARN, and the only form a CloudFormation Ref or a Lambda
// event-source mapping carries — was refused at every one of them.
//
// Three things this file has to show, and the third is the one a "does the ARN work" test misses:
//
//  1. **The ARN is decoded.** An ARN-only request succeeds where it used to be refused.
//  2. **The ARN is not merely decoded but resolved.** An ARN naming another account's or another
//     Region's stream must reach *that* stream — so the assertion is not that such a request
//     succeeds but that it reports the target's own state, and that a foreign ARN naming nothing
//     is refused rather than quietly serving the caller's same-named stream. That direction is the
//     damaging one, and DeleteStream is where it destroys data.
//  3. **Neither member is authoritative by default.** A request carrying both that disagrees is
//     refused, which is substrate's reading (no AWS page states it), and a request carrying neither
//     is refused under a code Kinesis publishes rather than the InvalidParameterException it does not.
//
// Every ARN under test is either taken from the state a create wrote, by way of
// [kinesisStreamARNFor] composing exactly what CreateStream mints, or composed *deliberately* to name
// something that does not exist — and the two cases are kept visibly apart, per #765. Statuses are
// asserted alongside codes throughout, per #923, and every code is read off the raw body rather than
// a decoded struct.
//
// Requests go through [scanScopeSignedTarget] rather than a plain signed helper because the Region
// travels in the Host and in the credential scope, and a cross-Region assertion needs both to agree
// with the endpoint being addressed.

const (
	// kinesisARNTargetPrefix is the X-Amz-Target prefix Kinesis's JSON-1.1 protocol carries.
	kinesisARNTargetPrefix = "Kinesis_20131202"

	// kinesisARNOtherAccount is the second registered account, so a cross-account ARN names an
	// account that exists rather than one the emulator would reject outright.
	kinesisARNOtherAccount = "210987654321"

	kinesisARNEastRegion = "us-east-1"
	kinesisARNWestRegion = "us-west-2"

	// kinesisARNStream is the stream name every case creates. One name shared by every account and
	// Region in the file is deliberate: a resolution that fell back to the caller's own account or
	// Region would find a stream and answer 200, which is exactly the defect, so the name must
	// collide for the tests to be able to catch it.
	kinesisARNStream = "orders"
)

// kinesisStreamARNFor composes the ARN CreateStream mints for a stream.
//
// Spelled out here rather than read back from a response because CreateStream's own response body is
// empty — it publishes no StreamARN at all — so there is nothing to take one from until a
// DescribeStreamSummary has succeeded, and the ARN is what the ARN-only calls need in order to make
// that first successful call.
func kinesisStreamARNFor(account, region, name string) string {
	return "arn:aws:kinesis:" + region + ":" + account + ":stream/" + name
}

// kinesisARNServer starts a server with both accounts registered.
func kinesisARNServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount, kinesisARNOtherAccount))
}

// kinesisARNCall posts one Kinesis operation and returns the status, the error code (empty on
// success) and the raw body.
//
// The account and Region are parameters rather than constants because half the assertions here turn
// on the caller being somewhere other than where the ARN points.
func kinesisARNCall(t *testing.T, ts *emulator.TestServer, account, region, op string,
	body map[string]any,
) (int, string, string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoErrorf(t, err, "marshal %s body", op)

	raw, status := scanScopeSignedTarget(t, ts, account,
		"kinesis."+region+".amazonaws.com", "kinesis", region, kinesisARNTargetPrefix, op, data)

	if status == http.StatusOK {
		return status, "", string(raw)
	}
	var doc map[string]any
	require.NoErrorf(t, json.Unmarshal(raw, &doc), "decode error body: %s", raw)
	code, _ := doc["__type"].(string)
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}
	return status, code, string(raw)
}

// kinesisARNOK posts one operation and requires it to succeed, returning the raw body.
func kinesisARNOK(t *testing.T, ts *emulator.TestServer, account, region, op string,
	body map[string]any,
) string {
	t.Helper()
	status, code, raw := kinesisARNCall(t, ts, account, region, op, body)
	require.Equalf(t, http.StatusOK, status, "%s in %s/%s answered %s: %s", op, account, region, code, raw)
	return raw
}

// kinesisARNCreate creates one stream, by name, in the given account and Region.
func kinesisARNCreate(t *testing.T, ts *emulator.TestServer, account, region, name string, shards int) {
	t.Helper()
	kinesisARNOK(t, ts, account, region, "CreateStream",
		map[string]any{"StreamName": name, "ShardCount": shards})
}

// kinesisARNMember reads one top-level member out of a response body as a string.
func kinesisARNMember(t *testing.T, raw, member string) string {
	t.Helper()
	var doc map[string]any
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &doc), "decode body: %s", raw)
	value, ok := doc[member]
	require.Truef(t, ok, "no %q member in %s", member, raw)
	text, ok := value.(string)
	require.Truef(t, ok, "%q is not a string in %s", member, raw)
	return text
}

// kinesisARNSummaryMember reads one member out of a DescribeStreamSummary response's
// StreamDescriptionSummary.
func kinesisARNSummaryMember(t *testing.T, raw, member string) any {
	t.Helper()
	var doc struct {
		Summary map[string]any `json:"StreamDescriptionSummary"`
	}
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &doc), "decode summary: %s", raw)
	value, ok := doc.Summary[member]
	require.Truef(t, ok, "no %q in StreamDescriptionSummary: %s", member, raw)
	return value
}

// kinesisARNOp is one operation and the members it requires beyond the stream reference.
type kinesisARNOp struct {
	// op is the operation name.
	op string

	// extra is every member the operation requires other than StreamName and StreamARN.
	extra map[string]any
}

// kinesisARNOps is the fourteen operations that publish both StreamName and StreamARN and name their
// stream directly.
//
// GetRecords is the fifteenth that publishes StreamARN and is absent here because it is the one page
// that publishes **no StreamName** — its stream comes from ShardIterator — so it cannot take part in a
// name-only or a disagreement case. It has its own tests below. CreateStream and ListStreams publish
// no StreamARN at all, and
// [TestKinesisARN_CreateStreamAndListStreamsPublishNoStreamARN] records that.
func kinesisARNOps() []kinesisARNOp {
	return []kinesisARNOp{
		{op: "DeleteStream"},
		{op: "DescribeStream"},
		{op: "DescribeStreamSummary"},
		{op: "ListTagsForStream"},
		{op: "UpdateShardCount", extra: map[string]any{
			"TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING",
		}},
		{op: "PutRecord", extra: map[string]any{
			"Data": base64.StdEncoding.EncodeToString([]byte("payload")), "PartitionKey": "pk",
		}},
		{op: "PutRecords", extra: map[string]any{
			"Records": []map[string]any{{
				"Data": base64.StdEncoding.EncodeToString([]byte("payload")), "PartitionKey": "pk",
			}},
		}},
		{op: "GetShardIterator", extra: map[string]any{
			"ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON",
		}},
		{op: "MergeShards", extra: map[string]any{
			"ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000001",
		}},
		{op: "SplitShard", extra: map[string]any{
			"ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "500",
		}},
		{op: "AddTagsToStream", extra: map[string]any{"Tags": map[string]string{"env": "prod"}}},
		{op: "RemoveTagsFromStream", extra: map[string]any{"TagKeys": []string{"env"}}},
		{op: "EnableEnhancedMonitoring", extra: map[string]any{
			"ShardLevelMetrics": []string{"IncomingBytes"},
		}},
		{op: "DisableEnhancedMonitoring", extra: map[string]any{
			"ShardLevelMetrics": []string{"IncomingBytes"},
		}},
	}
}

// kinesisARNBody builds one operation's request body from a stream reference and its extra members.
func kinesisARNBody(op kinesisARNOp, ref map[string]any) map[string]any {
	body := make(map[string]any, len(op.extra)+len(ref))
	for k, v := range op.extra {
		body[k] = v
	}
	for k, v := range ref {
		body[k] = v
	}
	return body
}

// TestKinesisARN_AnARNOnlyRequestAddressesTheStream is the criterion the issue leads with: on every
// operation that publishes StreamARN, a request carrying only the ARN succeeds.
//
// Each case gets its own server because several of these operations mutate or remove the stream, and
// a shared one would make the table order load-bearing.
func TestKinesisARN_AnARNOnlyRequestAddressesTheStream(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream)
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{"StreamARN": arn}))
			assert.Equalf(t, http.StatusOK, status,
				"%s carrying only StreamARN answered %s: %s", op.op, code, raw)
		})
	}
}

// TestKinesisARN_ANameOnlyRequestStillAddressesTheStream is the control for the test above: the form
// that always worked still does, so an ARN that reaches the stream cannot be a resolver that ignores
// its input and finds one stream whatever it is handed.
func TestKinesisARN_ANameOnlyRequestStillAddressesTheStream(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{"StreamName": kinesisARNStream}))
			assert.Equalf(t, http.StatusOK, status,
				"%s carrying only StreamName answered %s: %s", op.op, code, raw)
		})
	}
}

// TestKinesisARN_BothMembersAgreeingIsAccepted covers the "or both" half of the Note, which is the
// form an SDK sends when a caller supplied a name and the client held an ARN.
func TestKinesisARN_BothMembersAgreeingIsAccepted(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream)
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{"StreamName": kinesisARNStream, "StreamARN": arn}))
			assert.Equalf(t, http.StatusOK, status,
				"%s carrying both members answered %s: %s", op.op, code, raw)
		})
	}
}

// TestKinesisARN_AForeignARNNamingNothingIsARefusalNotTheCallersOwn is the assertion that
// distinguishes a decoded ARN from a resolved one.
//
// A stream named "orders" exists in the caller's own account and Region. Every call below hands an
// ARN naming "orders" in a *different* account, where nothing has been created. Before #966 the ARN
// was ignored and the caller's own stream answered; a resolver that read the name out of the ARN and
// the account out of the request context would do the same. Only a resolver that takes the account
// from the ARN can answer ResourceNotFoundException here.
func TestKinesisARN_AForeignARNNamingNothingIsARefusalNotTheCallersOwn(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream)
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{"StreamARN": foreign}))
			assert.Equalf(t, "ResourceNotFoundException", code,
				"%s reached the caller's own stream through a %s ARN: %s", op.op, kinesisARNOtherAccount, raw)
			assert.Equalf(t, http.StatusBadRequest, status,
				"%s: every Kinesis error is published at 400: %s", op.op, raw)
		})
	}
}

// TestKinesisARN_ACrossRegionARNNamingNothingIsARefusal is the Region half of the test above. Region
// and account are separate segments and a resolver can get one right and the other wrong.
func TestKinesisARN_ACrossRegionARNNamingNothingIsARefusal(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			foreign := kinesisStreamARNFor(taggingTestAccount, kinesisARNWestRegion, kinesisARNStream)
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{"StreamARN": foreign}))
			assert.Equalf(t, "ResourceNotFoundException", code,
				"%s reached the caller's own Region's stream through a %s ARN: %s",
				op.op, kinesisARNWestRegion, raw)
			assert.Equalf(t, http.StatusBadRequest, status, "%s: %s", op.op, raw)
		})
	}
}

// TestKinesisARN_ACrossAccountARNReportsTheTargetsOwnState is the positive direction: an ARN naming
// another account's stream reaches that stream and reports *its* shape, not a same-named local one's.
//
// The two streams are created with different shard counts, so the response distinguishes them. An
// assertion that the call merely succeeded would pass against the old behavior.
func TestKinesisARN_ACrossAccountARNReportsTheTargetsOwnState(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream, 4)

	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream)
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "DescribeStreamSummary",
		map[string]any{"StreamARN": foreign})

	assert.Equalf(t, float64(4), kinesisARNSummaryMember(t, raw, "OpenShardCount"),
		"the caller's own one-shard stream answered instead of the ARN's four-shard one: %s", raw)
	assert.Equalf(t, foreign, kinesisARNSummaryMember(t, raw, "StreamARN"),
		"the reported ARN is not the one that was asked for: %s", raw)
}

// TestKinesisARN_ACrossRegionARNReportsTheTargetsOwnState is the Region half of the test above.
func TestKinesisARN_ACrossRegionARNReportsTheTargetsOwnState(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNWestRegion, kinesisARNStream, 3)

	west := kinesisStreamARNFor(taggingTestAccount, kinesisARNWestRegion, kinesisARNStream)
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "DescribeStreamSummary",
		map[string]any{"StreamARN": west})

	assert.Equalf(t, float64(3), kinesisARNSummaryMember(t, raw, "OpenShardCount"),
		"the caller's own Region's stream answered instead of the ARN's: %s", raw)
	assert.Equalf(t, west, kinesisARNSummaryMember(t, raw, "StreamARN"),
		"the reported ARN is not the one that was asked for: %s", raw)
}

// TestKinesisARN_DeleteRemovesTheARNsStreamAndOnlyThat is the damaging direction, and the reason the
// resolution is worth a release note.
//
// Before #966 a DeleteStream carrying a cross-account ARN was refused outright, so no data was lost;
// but the same resolution serves every operation, and the arrangement that would have made the delete
// reach the caller's own stream is the one this test rules out. It also checks the two indexes the
// delete touches: the target's ListStreams no longer names the stream, and the caller's still does.
func TestKinesisARN_DeleteRemovesTheARNsStreamAndOnlyThat(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream)
	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "DeleteStream",
		map[string]any{"StreamARN": foreign})

	status, code, raw := kinesisARNCall(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion,
		"DescribeStreamSummary", map[string]any{"StreamName": kinesisARNStream})
	assert.Equal(t, "ResourceNotFoundException", code, "the ARN's own stream survived the delete")
	assert.Equal(t, http.StatusBadRequest, status, raw)

	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "DescribeStreamSummary",
		map[string]any{"StreamName": kinesisARNStream})

	// The index the delete removed from must be the target's, so ListStreams answers differently in
	// each account. A delete that removed the caller's index entry would leave the caller's stream
	// readable by name but invisible to ListStreams — the split #826 warns about.
	assert.NotContains(t,
		kinesisARNOK(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, "ListStreams", map[string]any{}),
		kinesisARNStream, "the deleted stream is still in the target account's index")
	assert.Contains(t,
		kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListStreams", map[string]any{}),
		kinesisARNStream, "the caller's own stream left the caller's index")
}

// TestKinesisARN_AMutationThroughAForeignARNWritesToTheTargetsRecord shows that the write path is
// resolved as well as the read path: saveStream derives its key from the record rather than from the
// request context, so a cross-account UpdateShardCount changes the target's stream and leaves the
// caller's own untouched. Reading state back through *each account's own name* is what proves it —
// a copy written into the caller's namespace would show up as the caller's shard count changing.
func TestKinesisARN_AMutationThroughAForeignARNWritesToTheTargetsRecord(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	// The target was 6 against a stream of 1 until #1076 taught UpdateShardCount the published
	// "no more than double your current shard count" bound, which refuses it. 2 is double 1, so it
	// is the largest target this test can use, and the property it proves — which record the write
	// lands in — does not depend on how far the count moves, only that it moves.
	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNEastRegion, kinesisARNStream)
	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "UpdateShardCount", map[string]any{
		"StreamARN": foreign, "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING",
	})

	target := kinesisARNOK(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, "DescribeStreamSummary",
		map[string]any{"StreamName": kinesisARNStream})
	assert.Equalf(t, float64(2), kinesisARNSummaryMember(t, target, "OpenShardCount"),
		"the update did not reach the ARN's stream: %s", target)

	caller := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "DescribeStreamSummary",
		map[string]any{"StreamName": kinesisARNStream})
	assert.Equalf(t, float64(1), kinesisARNSummaryMember(t, caller, "OpenShardCount"),
		"the update also rewrote the caller's own stream: %s", caller)
}

// TestKinesisARN_ATagWrittenThroughAnARNIsReadBackThroughTheName closes the loop the tag operations
// are for: AddTagsToStream by ARN and ListTagsForStream by name reach one record.
func TestKinesisARN_ATagWrittenThroughAnARNIsReadBackThroughTheName(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNWestRegion, kinesisARNStream, 1)
	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNWestRegion, kinesisARNStream)

	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream", map[string]any{
		"StreamARN": foreign, "Tags": map[string]string{"env": "prod"},
	})

	byName := kinesisARNOK(t, ts, kinesisARNOtherAccount, kinesisARNWestRegion, "ListTagsForStream",
		map[string]any{"StreamName": kinesisARNStream})
	assert.Containsf(t, byName, `"env"`, "the tag did not land on the ARN's stream: %s", byName)

	byARN := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListTagsForStream",
		map[string]any{"StreamARN": foreign})
	assert.Equalf(t, byName, byARN, "the same stream answered differently by name and by ARN")
}

// TestKinesisARN_ARecordPutThroughAnARNIsReadThroughTheName shows the record keys follow the ARN too:
// appendRecord and getShardIterator both derive their account and Region from the resolved target, so
// a record produced cross-Region is consumed from that Region's shard.
func TestKinesisARN_ARecordPutThroughAnARNIsReadThroughTheName(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNWestRegion, kinesisARNStream, 1)
	west := kinesisStreamARNFor(taggingTestAccount, kinesisARNWestRegion, kinesisARNStream)
	payload := base64.StdEncoding.EncodeToString([]byte("west-payload"))

	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "PutRecord", map[string]any{
		"StreamARN": west, "Data": payload, "PartitionKey": "pk",
	})

	iterRaw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNWestRegion, "GetShardIterator",
		map[string]any{
			"StreamName": kinesisARNStream, "ShardId": "shardId-000000000000",
			"ShardIteratorType": "TRIM_HORIZON",
		})
	records := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNWestRegion, "GetRecords",
		map[string]any{"ShardIterator": kinesisARNMember(t, iterRaw, "ShardIterator")})
	assert.Containsf(t, records, payload,
		"the record was written somewhere other than the ARN's shard: %s", records)
}

// TestKinesisARN_AMalformedARNIsInvalidArgumentAt400 walks every published constraint the ARN pattern
// states, at every operation.
//
// The pattern is arn:aws.*:kinesis:.*:\d{12}:stream/\S+, and the shapes below violate exactly one
// piece of it each. InvalidArgumentException/400 is published on all fifteen operations and its
// description — "A specified parameter exceeds its restrictions, is not supported, or can't be used"
// — is this case.
func TestKinesisARN_AMalformedARNIsInvalidArgumentAt400(t *testing.T) {
	t.Parallel()

	malformed := map[string]string{
		"a bare name":            kinesisARNStream,
		"no arn prefix":          "aws:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":stream/orders",
		"another partition":      "arn:xyz:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":stream/orders",
		"another service":        "arn:aws:sqs:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":stream/orders",
		"no resource portion":    "arn:aws:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount,
		"empty resource":         "arn:aws:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":",
		"no stream prefix":       "arn:aws:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":orders",
		"empty stream name":      "arn:aws:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":stream/",
		"eleven-digit account":   "arn:aws:kinesis:" + kinesisARNEastRegion + ":12345678901:stream/orders",
		"non-numeric account":    "arn:aws:kinesis:" + kinesisARNEastRegion + ":acctacctacct:stream/orders",
		"empty account":          "arn:aws:kinesis:" + kinesisARNEastRegion + "::stream/orders",
		"a consumer arn":         kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream) + "/consumer/reader",
		"whitespace in the name": "arn:aws:kinesis:" + kinesisARNEastRegion + ":" + taggingTestAccount + ":stream/or ders",
	}

	for name, arn := range malformed {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			for _, op := range kinesisARNOps() {
				status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
					kinesisARNBody(op, map[string]any{"StreamARN": arn}))
				assert.Equalf(t, "InvalidArgumentException", code, "%s on %q: %s", op.op, arn, raw)
				assert.Equalf(t, http.StatusBadRequest, status, "%s on %q: %s", op.op, arn, raw)
			}
		})
	}
}

// TestKinesisARN_AnEmptyRegionResolvesRatherThanBeingRefused records the one shape the published
// pattern admits that cannot name anything.
//
// The pattern's Region segment is ".*", which matches the empty string, so an ARN with no Region is
// well-formed. It is not refused for its shape — that would claim AWS rejects an ARN its own pattern
// accepts — but resolves to a key nothing is written at and reports the stream absent. That is the
// decision #912 recorded for an express execution ARN, applied here.
func TestKinesisARN_AnEmptyRegionResolvesRatherThanBeingRefused(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	noRegion := "arn:aws:kinesis::" + taggingTestAccount + ":stream/" + kinesisARNStream
	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion,
		"DescribeStreamSummary", map[string]any{"StreamARN": noRegion})
	assert.Equalf(t, "ResourceNotFoundException", code,
		"a Region-less ARN was refused for its shape, or reached the caller's own stream: %s", raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)
}

// TestKinesisARN_TwoMembersNamingDifferentStreamsIsRefused is substrate's reading, stated as such:
// no Kinesis page says what happens when StreamARN and StreamName disagree.
//
// Refusing is the choice, because serving either of them is how a caller's bug stays hidden — the same
// judgement that made a well-formed ARN of the wrong resource type a refusal in #910 and #912. Both
// streams exist, so neither answer would be a not-found and the test cannot pass by accident.
func TestKinesisARN_TwoMembersNamingDifferentStreamsIsRefused(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, "invoices", 2)

			arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "invoices")
			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{"StreamName": kinesisARNStream, "StreamARN": arn}))
			assert.Equalf(t, "InvalidArgumentException", code,
				"%s served one of two disagreeing members: %s", op.op, raw)
			assert.Equalf(t, http.StatusBadRequest, status, "%s: %s", op.op, raw)
		})
	}
}

// TestKinesisARN_AnARNInAnotherAccountIsNotADisagreementWithTheName records the boundary of the rule
// above: only the *name* segments are compared.
//
// An ARN whose account or Region differ from the caller's while its name agrees with StreamName is
// the case the whole change exists for, not a contradiction, so it resolves to the ARN's stream. A
// comparison that included the account would have refused it and undone the fix.
func TestKinesisARN_AnARNInAnotherAccountIsNotADisagreementWithTheName(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNWestRegion, kinesisARNStream, 5)

	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNWestRegion, kinesisARNStream)
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "DescribeStreamSummary",
		map[string]any{"StreamName": kinesisARNStream, "StreamARN": foreign})
	assert.Equalf(t, float64(5), kinesisARNSummaryMember(t, raw, "OpenShardCount"),
		"the name won over the ARN: %s", raw)
}

// TestKinesisARN_NeitherMemberIsRefusedUnderAPublishedCode covers the last of the Note's cases.
//
// "You must use either the StreamARN or the StreamName parameter, or both" makes a request carrying
// neither invalid. This is the guard that used to answer InvalidParameterException — a code Kinesis
// publishes nowhere at all, so no SDK models it — and the code moves with the guard's meaning.
func TestKinesisARN_NeitherMemberIsRefusedUnderAPublishedCode(t *testing.T) {
	t.Parallel()

	for _, op := range kinesisARNOps() {
		t.Run(op.op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op.op,
				kinesisARNBody(op, map[string]any{}))
			assert.Equalf(t, "InvalidArgumentException", code, "%s: %s", op.op, raw)
			assert.Equalf(t, http.StatusBadRequest, status, "%s: %s", op.op, raw)
		})
	}
}

// TestKinesisARN_StreamIdIsNotDecoded holds the line #966 draws around the member a sweep would have
// wired up by mistake.
//
// All fifteen pages publish StreamId as "Not Implemented. Reserved for future use." A request naming
// its stream *only* by StreamId therefore names it not at all, and must be refused rather than
// resolved — including when the value is a well-formed one for its published pattern
// [a-z0-9]{20}-[a-z0-9]{3}.
func TestKinesisARN_StreamIdIsNotDecoded(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion,
		"DescribeStreamSummary", map[string]any{"StreamId": "abcdefghij0123456789-xyz"})
	assert.Equalf(t, "InvalidArgumentException", code,
		"StreamId was treated as a way to name a stream: %s", raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)
}

// TestKinesisARN_CreateStreamAndListStreamsPublishNoStreamARN records the two operations the audit
// found do *not* publish it, which is the finding the issue asks for by name.
//
// CreateStream is the service's one operation-wide "Required: Yes" StreamName and publishes no
// StreamARN because it is minting one; ListStreams names no single stream at all and publishes
// neither member. So an ARN-only CreateStream stays a refusal, and it keeps the code the rest of the
// file's body guards answer, because that guard is #950's rather than this issue's.
func TestKinesisARN_CreateStreamAndListStreamsPublishNoStreamARN(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream)
	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "CreateStream",
		map[string]any{"StreamARN": arn, "ShardCount": 1})
	assert.Equalf(t, http.StatusBadRequest, status, "CreateStream accepted a StreamARN: %s", raw)
	assert.NotEmpty(t, code, "CreateStream answered no error code: %s", raw)

	// ListStreams takes neither member, so an ARN on it is simply ignored rather than resolved: it
	// lists the caller's own Region and account, which is all it can do.
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	listed := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListStreams",
		map[string]any{"StreamARN": arn})
	assert.Contains(t, listed, kinesisARNStream, listed)
}

// TestKinesisARN_ADuplicateCreateAnswersResourceInUseAt400 is the second status correction #966
// carries. API_CreateStream publishes ResourceInUseException at 400 and substrate answered 409.
func TestKinesisARN_ADuplicateCreateAnswersResourceInUseAt400(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "CreateStream",
		map[string]any{"StreamName": kinesisARNStream, "ShardCount": 1})
	assert.Equal(t, "ResourceInUseException", code, raw)
	assert.Equalf(t, http.StatusBadRequest, status,
		"every Kinesis error is published at 400, including this one: %s", raw)
}

// TestKinesisARN_GetRecordsChecksAStreamARNAgainstItsIterator covers the fifteenth operation, the one
// that publishes StreamARN and no StreamName.
//
// Its stream comes from ShardIterator, which is Required: Yes and already carries the account and
// Region the stream was resolved in. A supplied StreamARN is therefore redundant, and the question is
// what to do when it is not: an ARN agreeing with the iterator is accepted, and one naming a different
// stream is refused rather than ignored, because ignoring it would serve records from a stream the
// request did not name.
func TestKinesisARN_GetRecordsChecksAStreamARNAgainstItsIterator(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, "invoices", 1)

	payload := base64.StdEncoding.EncodeToString([]byte("ordered"))
	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "PutRecord", map[string]any{
		"StreamName": kinesisARNStream, "Data": payload, "PartitionKey": "pk",
	})
	iterRaw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "GetShardIterator",
		map[string]any{
			"StreamName": kinesisARNStream, "ShardId": "shardId-000000000000",
			"ShardIteratorType": "TRIM_HORIZON",
		})
	iter := kinesisARNMember(t, iterRaw, "ShardIterator")

	agreeing := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream)
	records := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "GetRecords",
		map[string]any{"ShardIterator": iter, "StreamARN": agreeing})
	assert.Containsf(t, records, payload, "an agreeing StreamARN was refused: %s", records)

	disagreeing := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "invoices")
	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "GetRecords",
		map[string]any{"ShardIterator": iter, "StreamARN": disagreeing})
	assert.Equalf(t, "InvalidArgumentException", code,
		"GetRecords served the iterator's stream under a StreamARN naming another: %s", raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)

	malformed := "arn:aws:kinesis:" + kinesisARNEastRegion + ":1:stream/" + kinesisARNStream
	status, code, raw = kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "GetRecords",
		map[string]any{"ShardIterator": iter, "StreamARN": malformed})
	assert.Equal(t, "InvalidArgumentException", code, raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)
}

// TestKinesisARN_TheTaggingAPIAndKinesisResolveOneARN is the cross-readability criterion #765
// established, and the reason the tagging API's kinesis arm now goes through the same parser.
//
// A tag written by the Resource Groups Tagging API against a stream ARN must be the tag
// ListTagsForStream reports for that stream, and the reverse. Two independent parses of one ARN is
// exactly how the two drift apart — the arrangement #826 warns about.
func TestKinesisARN_TheTaggingAPIAndKinesisResolveOneARN(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	arn := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream)

	tagged := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamARN": arn, "Tags": map[string]string{"owner": "billing"}})
	require.NotNil(t, tagged)

	var resources struct {
		ResourceTagMappingList []struct {
			ResourceARN string `json:"ResourceARN"`
			Tags        []struct {
				Key   string `json:"Key"`
				Value string `json:"Value"`
			} `json:"Tags"`
		} `json:"ResourceTagMappingList"`
	}
	scanScopeJSON(t, ts, taggingTestAccount, "tagging", kinesisARNEastRegion,
		"ResourceGroupsTaggingAPI_20170126", "GetResources",
		map[string]any{"ResourceTypeFilters": []string{"kinesis"}}, &resources)

	found := false
	for _, mapping := range resources.ResourceTagMappingList {
		if mapping.ResourceARN != arn {
			continue
		}
		found = true
		require.Len(t, mapping.Tags, 1, "the tagging API read a different record than AddTagsToStream wrote")
		assert.Equal(t, "owner", mapping.Tags[0].Key)
		assert.Equal(t, "billing", mapping.Tags[0].Value)
	}
	assert.Truef(t, found, "the tagging API did not report the stream at %s: %+v", arn, resources)
}
