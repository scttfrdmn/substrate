package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateStream's published Tags member, and the two refusals it brings with it (#1087).
//
// The refusal assertions each check twice: the code and status, and then that no stream was left
// behind. A create that refuses after writing its record is the worse failure of the two, because the
// caller's retry then hits ResourceInUseException for a stream it was told it had not created.

// kinesisCreateTagsTarget is Kinesis's JSON door.
var kinesisCreateTagsTarget = signedRequestTarget{
	host:        "kinesis.us-east-1.amazonaws.com",
	target:      "Kinesis_20131202",
	signingName: "kinesis",
}

// kinesisCreateTagsCall posts one Kinesis operation and returns the status and any error code.
func kinesisCreateTagsCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any, out any) (int, string) {
	t.Helper()
	return decodeAWSResponse(t,
		signedRequest(t, ts, kinesisCreateTagsTarget, taggingTestAccount, op, body), out)
}

// kinesisCreateTagsList reads a stream's tags back through ListTagsForStream.
func kinesisCreateTagsList(t *testing.T, ts *emulator.TestServer, stream string) map[string]string {
	t.Helper()
	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	status, errCode := kinesisCreateTagsCall(t, ts, "ListTagsForStream",
		map[string]any{"StreamName": stream}, &out)
	require.Emptyf(t, errCode, "ListTagsForStream %s", stream)
	require.Equalf(t, http.StatusOK, status, "ListTagsForStream %s", stream)

	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// kinesisCreateTagsExists reports whether DescribeStreamSummary can see the stream.
func kinesisCreateTagsExists(t *testing.T, ts *emulator.TestServer, stream string) bool {
	t.Helper()
	status, errCode := kinesisCreateTagsCall(t, ts, "DescribeStreamSummary",
		map[string]any{"StreamName": stream}, nil)
	if errCode == "" && status == http.StatusOK {
		return true
	}
	require.Equalf(t, "ResourceNotFoundException", errCode,
		"DescribeStreamSummary %s answered an unexpected code", stream)
	return false
}

// TestKinesisCreateStream_TagsAreStoredAndReadableBothWays asserts the round trip CreateStream's own
// lede describes: "You can add tags to the stream when making a CreateStream request by setting the
// Tags parameter."
//
// Read back through ListTagsForStream and through GetResources, per #765 — the tagging API reaches the
// record through a different scanner, and before #1087 the stream appeared there with no tags at all.
func TestKinesisCreateStream_TagsAreStoredAndReadableBothWays(t *testing.T) {
	ts := arnGuardServer(t)

	status, errCode := kinesisCreateTagsCall(t, ts, "CreateStream", map[string]any{
		"StreamName": "create-tags-stream",
		"ShardCount": 1,
		"Tags":       map[string]string{"env": "staging", "owner": "platform"},
	}, nil)
	require.Empty(t, errCode, "CreateStream with tags")
	require.Equal(t, http.StatusOK, status, "CreateStream with tags")

	assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"},
		kinesisCreateTagsList(t, ts, "create-tags-stream"),
		"ListTagsForStream reports what CreateStream was given")

	var found bool
	for _, rm := range getResourcesMappings(t, ts, "kinesis") {
		if !strings.HasSuffix(rm.ResourceARN, ":stream/create-tags-stream") {
			continue
		}
		found = true
		reported := make(map[string]string, len(rm.Tags))
		for _, tag := range rm.Tags {
			reported[tag.Key] = tag.Value
		}
		assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"}, reported,
			"GetResources reports the create-time tags")
	}
	assert.True(t, found, "GetResources reports the stream at all")
}

// TestKinesisCreateStream_AnAbsentTagsMemberIsNotRefused is the nil-map trap.
//
// kinesisValidateTagMap refuses a nil map with "Tags is required", which is right for
// AddTagsToStream, where the member is Required: Yes. On CreateStream it is Required: No, so calling
// the helper unconditionally would refuse every CreateStream request that omits tags — which is most
// of them, including every other fixture in this package. An empty map is distinct again and is
// accepted as the no-op the helper already reads it as.
func TestKinesisCreateStream_AnAbsentTagsMemberIsNotRefused(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
	}{
		{name: "no Tags member", body: map[string]any{"StreamName": "no-tags", "ShardCount": 1}},
		{name: "explicit null", body: map[string]any{"StreamName": "null-tags", "Tags": nil}},
		{name: "empty map", body: map[string]any{"StreamName": "empty-tags", "Tags": map[string]string{}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)
			status, errCode := kinesisCreateTagsCall(t, ts, "CreateStream", tc.body, nil)
			require.Empty(t, errCode, "CreateStream")
			require.Equal(t, http.StatusOK, status, "CreateStream")

			name, _ := tc.body["StreamName"].(string)
			assert.Empty(t, kinesisCreateTagsList(t, ts, name), "the stream carries no tags")
		})
	}
}

// TestKinesisCreateStream_TagRefusalsLeaveNoStream covers both published bounds and the ordering that
// makes them safe.
//
// The two numbers are the pair kinesis_tag_quota.go's preamble dissects, published on this page in one
// member entry: prose "A set of up to 50 key-value pairs" over "Map Entries: Maximum number of 200
// items". Over 200 in one request is the request's own shape and answers InvalidArgumentException;
// over 50 on the stream is the resource quota and answers LimitExceededException — the second being
// substrate's reading of a code the page publishes under a different attribution, which the preamble
// records.
//
// Both run before the state read, so neither leaves a stream behind. That is asserted rather than
// assumed: a refusal after the Put would make the caller's retry hit ResourceInUseException.
func TestKinesisCreateStream_TagRefusalsLeaveNoStream(t *testing.T) {
	tests := []struct {
		name  string
		count int
		code  string
	}{
		{name: "201 entries in one request", count: 201, code: "InvalidArgumentException"},
		{name: "51 tags on the stream", count: 51, code: "LimitExceededException"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)

			tags := make(map[string]string, tc.count)
			for i := range tc.count {
				tags[fmt.Sprintf("key-%03d", i)] = fmt.Sprintf("value-%03d", i)
			}
			status, errCode := kinesisCreateTagsCall(t, ts, "CreateStream", map[string]any{
				"StreamName": "refused-stream",
				"ShardCount": 1,
				"Tags":       tags,
			}, nil)
			assert.Equal(t, tc.code, errCode, "the published code")
			assert.Equal(t, http.StatusBadRequest, status, "every Kinesis error is 400")

			assert.False(t, kinesisCreateTagsExists(t, ts, "refused-stream"),
				"a refused create leaves no stream behind")
		})
	}
}

// TestKinesisCreateStream_FiftyTagsIsAccepted pins the boundary from the other side, so the quota
// check cannot drift into an off-by-one that refuses the fiftieth tag the page explicitly allows.
func TestKinesisCreateStream_FiftyTagsIsAccepted(t *testing.T) {
	ts := arnGuardServer(t)

	tags := make(map[string]string, kinesisTagQuotaForTest)
	for i := range kinesisTagQuotaForTest {
		tags[fmt.Sprintf("key-%03d", i)] = fmt.Sprintf("value-%03d", i)
	}
	status, errCode := kinesisCreateTagsCall(t, ts, "CreateStream", map[string]any{
		"StreamName": "fifty-tags",
		"Tags":       tags,
	}, nil)
	require.Empty(t, errCode, "fifty tags is at the quota, not over it")
	require.Equal(t, http.StatusOK, status, "fifty tags is at the quota, not over it")

	assert.Len(t, kinesisCreateTagsList(t, ts, "fifty-tags"), kinesisTagQuotaForTest,
		"all fifty are stored")
}

// kinesisTagQuotaForTest is the published per-stream tag quota, spelled here because the constant it
// mirrors is unexported. A divergence would show up as this test failing, which is the point.
const kinesisTagQuotaForTest = 50

// TestKinesisCreateStream_AnEmptyTagValueIsValid pins the asymmetry the published lengths state: a key
// is 1–128 characters and a value 0–256, because "a tag consists of a required key and an optional
// value". An empty value is therefore a tag, not a malformed one.
func TestKinesisCreateStream_AnEmptyTagValueIsValid(t *testing.T) {
	ts := arnGuardServer(t)

	status, errCode := kinesisCreateTagsCall(t, ts, "CreateStream", map[string]any{
		"StreamName": "empty-value",
		"Tags":       map[string]string{"marker": ""},
	}, nil)
	require.Empty(t, errCode, "an empty value is valid")
	require.Equal(t, http.StatusOK, status, "an empty value is valid")

	assert.Equal(t, map[string]string{"marker": ""}, kinesisCreateTagsList(t, ts, "empty-value"),
		"and is stored as one")
}
