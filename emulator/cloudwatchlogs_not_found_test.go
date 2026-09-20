package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cwLogsSeedGroupWithEvent creates a log group, a stream in it and one event, all through published
// operations (#765), so the fixture the refusals below are asserted against is one a caller could
// have built rather than records written straight into the store.
func cwLogsSeedGroupWithEvent(t *testing.T, srv *emulator.Server, group, stream, message string) {
	t.Helper()
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": group})
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateLogGroup %s", group)
	resp = cwLogsRequest(t, srv, "CreateLogStream", map[string]any{"logGroupName": group, "logStreamName": stream})
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateLogStream %s", stream)
	resp = cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName":  group,
		"logStreamName": stream,
		"logEvents":     []map[string]any{{"timestamp": 1700000000000, "message": message}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, "PutLogEvents %s", stream)
}

// TestCWLogs_ReadsRefuseAnAbsentLogGroup is #1224's first criterion: the three reads that publish
// ResourceNotFoundException answer it, at the 400 their pages publish, for a group with no record.
//
// Before this, each answered HTTP 200 with an empty listing, so "this group is empty" and "this group
// was never created" were the same response.
func TestCWLogs_ReadsRefuseAnAbsentLogGroup(t *testing.T) {
	srv := newCWLogsTestServer(t)

	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"DescribeLogStreams", map[string]any{"logGroupName": "/never/created"}},
		{"GetLogEvents", map[string]any{"logGroupName": "/never/created", "logStreamName": "stream"}},
		{"FilterLogEvents", map[string]any{"logGroupName": "/never/created"}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, tc.op, tc.body)
			raw := cwLogsReadBody(t, resp)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"%s publishes ResourceNotFoundException at 400, not 404 and not an empty 200", tc.op)
			assert.Equal(t, "ResourceNotFoundException", cwLogsErrorTypeFrom(t, raw))
			assert.Contains(t, string(raw), "/never/created", "the message names the group")
		})
	}
}

// TestCWLogs_GetLogEventsRefusesAnAbsentStream covers the second criterion: a stream that has no record
// under a group that does exist, which is PutLogEvents' existing distinction applied to the read.
func TestCWLogs_GetLogEventsRefusesAnAbsentStream(t *testing.T) {
	srv := newCWLogsTestServer(t)
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": "/exists"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = cwLogsRequest(t, srv, "GetLogEvents", map[string]any{
		"logGroupName": "/exists", "logStreamName": "/no/such/stream",
	})
	raw := cwLogsReadBody(t, resp)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ResourceNotFoundException", cwLogsErrorTypeFrom(t, raw))
	assert.Contains(t, string(raw), "log stream", "the message says which of the two resources is missing")
	assert.Contains(t, string(raw), "/no/such/stream")

	// And the group is reported as the group when both are absent, which is why requireLogGroup runs
	// first: a caller told the stream is missing would create it and be refused again.
	resp = cwLogsRequest(t, srv, "GetLogEvents", map[string]any{
		"logGroupName": "/absent", "logStreamName": "/no/such/stream",
	})
	raw = cwLogsReadBody(t, resp)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(raw), "log group")
	assert.Contains(t, string(raw), "/absent")
}

// TestCWLogs_DescribeLogGroupsStaysAnEmpty200 is the third criterion, and the reason this is a
// three-site change: API_DescribeLogGroups publishes no ResourceNotFoundException at all, so a prefix
// matching nothing is a legitimately empty listing and a refusal there would be invented.
func TestCWLogs_DescribeLogGroupsStaysAnEmpty200(t *testing.T) {
	srv := newCWLogsTestServer(t)

	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{"logGroupNamePrefix": "/matches/nothing"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"logGroups":[]}`, string(cwLogsReadBody(t, resp)))
}

// TestCWLogs_EveryNotFoundAnswers400 is the fourth criterion: the four operations that already refused
// an absent resource did so at 404, and each of their pages publishes 400. One code cannot keep two
// statuses in one service, so they moved with the three reads rather than being left as a split.
func TestCWLogs_EveryNotFoundAnswers400(t *testing.T) {
	srv := newCWLogsTestServer(t)
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": "/exists"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
	}{
		{"DeleteLogGroup", "DeleteLogGroup", map[string]any{"logGroupName": "/absent"}},
		{"CreateLogStream", "CreateLogStream", map[string]any{"logGroupName": "/absent", "logStreamName": "s"}},
		{"DeleteLogStream", "DeleteLogStream", map[string]any{"logGroupName": "/exists", "logStreamName": "/absent"}},
		{"PutLogEvents", "PutLogEvents", map[string]any{
			"logGroupName": "/exists", "logStreamName": "/absent",
			"logEvents": []map[string]any{{"timestamp": 1, "message": "m"}},
		}},
		{"PutRetentionPolicy", "PutRetentionPolicy", map[string]any{"logGroupName": "/absent", "retentionInDays": 7}},
		{"DescribeLogStreams", "DescribeLogStreams", map[string]any{"logGroupName": "/absent"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"every CloudWatch Logs page publishes ResourceNotFoundException at 400")
			assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp))
		})
	}
}

// TestCWLogs_NotFoundTakesPrecedenceOverAnUnissuableToken is the fifth criterion. The decision, argued
// from SNS ListSubscriptionsByTopic (#926) in cloudwatchlogs_not_found.go, is that the resource the
// request addresses is resolved first: a token is a continuation of a listing over that resource, and
// there is no listing to continue when the resource does not exist.
func TestCWLogs_NotFoundTakesPrecedenceOverAnUnissuableToken(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedGroupWithEvent(t, srv, "/exists", "stream-1", "hello")

	t.Run("the group wins over the token", func(t *testing.T) {
		for _, tc := range []struct {
			op   string
			body map[string]any
		}{
			{"DescribeLogStreams", map[string]any{"logGroupName": "/absent", "nextToken": "not-a-token"}},
			{"GetLogEvents", map[string]any{"logGroupName": "/absent", "logStreamName": "s", "nextToken": "not-a-token"}},
			{"FilterLogEvents", map[string]any{"logGroupName": "/absent", "nextToken": "not-a-token"}},
		} {
			t.Run(tc.op, func(t *testing.T) {
				resp := cwLogsRequest(t, srv, tc.op, tc.body)
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp),
					"both refusals are 400, so the code is the only thing that shows which one won")
			})
		}
	})

	t.Run("the token is still refused when the resource exists", func(t *testing.T) {
		// #1086's rule, unchanged: a token no previous call returned is refused rather than answering
		// page one. Only the order against the not-found moved.
		for _, tc := range []struct {
			op   string
			body map[string]any
		}{
			{"DescribeLogStreams", map[string]any{"logGroupName": "/exists", "nextToken": "not-a-token"}},
			{"GetLogEvents", map[string]any{"logGroupName": "/exists", "logStreamName": "stream-1", "nextToken": "not-a-token"}},
			{"FilterLogEvents", map[string]any{"logGroupName": "/exists", "nextToken": "not-a-token"}},
		} {
			t.Run(tc.op, func(t *testing.T) {
				resp := cwLogsRequest(t, srv, tc.op, tc.body)
				raw := cwLogsReadBody(t, resp)
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, "InvalidParameterException", cwLogsErrorTypeFrom(t, raw))
				assert.Contains(t, string(raw), "nextToken")
			})
		}
	})

	t.Run("an absent required member still wins over both", func(t *testing.T) {
		// A request naming no group names no resource to resolve, so the not-found has nothing to be
		// about and the required-member refusal keeps the front of the sequence.
		resp := cwLogsRequest(t, srv, "DescribeLogStreams", map[string]any{"nextToken": "not-a-token"})
		raw := cwLogsReadBody(t, resp)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "InvalidParameterException", cwLogsErrorTypeFrom(t, raw))
		assert.Contains(t, string(raw), "logGroupName")
	})
}

// TestCWLogs_ExistingGroupsStillRead is the other half of the refusal: the three reads still answer
// what they answered for a group that does exist, including the empty listing that is now only reachable
// when the group is real.
func TestCWLogs_ExistingGroupsStillRead(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedGroupWithEvent(t, srv, "/exists", "stream-1", "ERROR: disk full")

	resp := cwLogsRequest(t, srv, "DescribeLogStreams", map[string]any{"logGroupName": "/exists"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(cwLogsReadBody(t, resp)), "stream-1")

	resp = cwLogsRequest(t, srv, "GetLogEvents", map[string]any{"logGroupName": "/exists", "logStreamName": "stream-1"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(cwLogsReadBody(t, resp)), "ERROR: disk full")

	resp = cwLogsRequest(t, srv, "FilterLogEvents", map[string]any{"logGroupName": "/exists", "filterPattern": "ERROR"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(cwLogsReadBody(t, resp)), "ERROR: disk full")

	// An empty stream under a real group: a 200 with no events, which is the answer the refusal above
	// used to hide.
	resp = cwLogsRequest(t, srv, "CreateLogStream", map[string]any{"logGroupName": "/exists", "logStreamName": "quiet"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = cwLogsRequest(t, srv, "GetLogEvents", map[string]any{"logGroupName": "/exists", "logStreamName": "quiet"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"events":[]}`, string(cwLogsReadBody(t, resp)))
}
