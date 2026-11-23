package emulator_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// runTwiceJourney is a small sequential journey: a VPC, a KMS key, an IAM user, an S3 bucket
// and listings of each, which is enough to mint identifiers and render timestamps.
func runTwiceJourney(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	c := &detClient{t: t, ts: ts}
	c.query("ec2.us-east-1.amazonaws.com", "2016-11-15", map[string]string{"Action": "CreateVpc", "CidrBlock": "10.50.0.0/16"})
	c.query("ec2.us-east-1.amazonaws.com", "2016-11-15", map[string]string{"Action": "DescribeVpcs"})
	c.json("kms.us-east-1.amazonaws.com", "TrentService.CreateKey", map[string]any{"Description": "twice"})
	c.json("kms.us-east-1.amazonaws.com", "TrentService.ListKeys", map[string]any{})
	c.query("iam.amazonaws.com", "2010-05-08", map[string]string{"Action": "CreateUser", "UserName": "twice"})
	c.s3("CreateBucket", http.MethodPut, "/twice-bucket", nil)
	c.s3("ListBuckets", http.MethodGet, "/", nil)
}

// RunTwice passes a journey whose two runs agree.
func TestRunTwice_DeterministicJourneyPasses(t *testing.T) {
	t.Parallel()
	emulator.RunTwice(t, detEpoch, runTwiceJourney)
}

// Two runs with unseeded request IDs mint different identifiers, and the comparison reports it:
// the check is not vacuous.
func TestCompareRecordedRuns_ReportsUnseededRuns(t *testing.T) {
	t.Parallel()
	record := func() []*emulator.Event {
		ts := emulator.StartTestServer(t, emulator.WithoutSeededIDs(), emulator.WithDeterministicRun(detEpoch))
		runTwiceJourney(t, ts)
		events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{})
		require.NoError(t, err)
		return events
	}
	diffs := emulator.CompareRecordedRuns(record(), record())
	require.NotEmpty(t, diffs)
	var fields []string
	for _, d := range diffs {
		fields = append(fields, d.Field)
	}
	joined := strings.Join(fields, " ")
	assert.Contains(t, joined, "request_id")
	assert.Contains(t, joined, "response_body", "the minted VPC ID differs in the body")
}

// TestCompareRecordedRuns covers each kind of difference on hand-built events.
func TestCompareRecordedRuns(t *testing.T) {
	t.Parallel()
	ev := func(seq int64, op string, status int, body, date string) *emulator.Event {
		return &emulator.Event{
			Sequence: seq, Service: "sqs", Operation: op, RequestID: "req-1", StatusCode: status,
			Response: &emulator.AWSResponse{StatusCode: status, Body: []byte(body),
				Headers: map[string]string{"Date": date, "Content-Type": "application/json"}},
		}
	}
	tests := []struct {
		name          string
		first, second []*emulator.Event
		wantFields    []string
	}{
		{
			name:  "identical runs, Date aside",
			first: []*emulator.Event{ev(1, "ListQueues", 200, `{"QueueUrls":["a","b"]}`, "Mon")},
			second: []*emulator.Event{
				ev(1, "ListQueues", 200, `{"QueueUrls":["a","b"]}`, "Tue"),
			},
		},
		{
			name:       "JSON member order is not a difference, element order is",
			first:      []*emulator.Event{ev(1, "ListQueues", 200, `{"x":1,"QueueUrls":["a","b"]}`, "")},
			second:     []*emulator.Event{ev(1, "ListQueues", 200, `{"QueueUrls":["b","a"],"x":1}`, "")},
			wantFields: []string{"response_body/QueueUrls/0", "response_body/QueueUrls/1"},
		},
		{
			name:       "the second run stops early",
			first:      []*emulator.Event{ev(1, "ListQueues", 200, `{}`, ""), ev(2, "ListQueues", 200, `{}`, "")},
			second:     []*emulator.Event{ev(1, "ListQueues", 200, `{}`, "")},
			wantFields: []string{"count", "missing_event"},
		},
		{
			name:       "the second run makes an extra call",
			first:      []*emulator.Event{ev(1, "ListQueues", 200, `{}`, "")},
			second:     []*emulator.Event{ev(1, "ListQueues", 200, `{}`, ""), ev(2, "GetQueueUrl", 200, `{}`, "")},
			wantFields: []string{"count", "extra_event"},
		},
		{
			name: "the first misaligned operation is reported once and the walk stops",
			first: []*emulator.Event{
				ev(1, "ListQueues", 200, `{}`, ""), ev(2, "GetQueueUrl", 200, `{"a":1}`, ""),
				ev(3, "ListQueues", 200, `{"a":1}`, ""),
			},
			second: []*emulator.Event{
				ev(1, "ListQueues", 200, `{}`, ""), ev(2, "ListQueues", 200, `{"b":2}`, ""),
				ev(3, "GetQueueUrl", 200, `{"b":2}`, ""), ev(4, "ListQueues", 200, `{"b":2}`, ""),
			},
			wantFields: []string{"count", "operation"},
		},
		{
			name:       "a different status",
			first:      []*emulator.Event{ev(1, "ListQueues", 200, `{}`, "")},
			second:     []*emulator.Event{ev(1, "ListQueues", 400, `{}`, "")},
			wantFields: []string{"status_code"},
		},
		{
			name:  "paired by sequence, not by slice position",
			first: []*emulator.Event{ev(2, "GetQueueUrl", 200, `{}`, ""), ev(1, "ListQueues", 200, `{}`, "")},
			second: []*emulator.Event{
				ev(1, "ListQueues", 200, `{}`, ""), ev(2, "GetQueueUrl", 200, `{}`, ""),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			diffs := emulator.CompareRecordedRuns(tc.first, tc.second)
			var got []string
			for _, d := range diffs {
				got = append(got, d.Field)
				assert.NotEmpty(t, d.String())
			}
			assert.Equal(t, tc.wantFields, got)
		})
	}

	t.Run("request IDs, errors and headers", func(t *testing.T) {
		a := ev(1, "ListQueues", 200, `{}`, "")
		b := ev(1, "ListQueues", 200, `{}`, "")
		b.RequestID, b.ParentRequestID, b.Error = "req-2", "req-0", "boom"
		b.Response.Headers["Content-Type"] = "text/plain"
		diffs := emulator.CompareRecordedRuns([]*emulator.Event{a}, []*emulator.Event{b})
		var got []string
		for _, d := range diffs {
			got = append(got, d.Field)
		}
		assert.Equal(t, []string{"request_id", "parent_request_id", "error", "header/Content-Type"}, got)
		assert.Equal(t, "#1 sqs.ListQueues request_id: req-1 != req-2", diffs[0].String())
	})

	t.Run("timestamps, costs and state hashes", func(t *testing.T) {
		a := ev(1, "ListQueues", 200, `{}`, "")
		b := ev(1, "ListQueues", 200, `{}`, "")
		a.Timestamp, b.Timestamp = detEpoch, detEpoch.Add(time.Second)
		a.Cost, b.Cost = 0.0004, 0.0008
		a.StateHashAfter, b.StateHashAfter = "aaa", "bbb"
		var got []string
		for _, d := range emulator.CompareRecordedRuns([]*emulator.Event{a}, []*emulator.Event{b}) {
			got = append(got, d.Field)
		}
		assert.Equal(t, []string{"timestamp", "cost", "state_hash_after"}, got)

		b.StateHashAfter = ""
		b.Timestamp, b.Cost = a.Timestamp, a.Cost
		assert.Empty(t, emulator.CompareRecordedRuns([]*emulator.Event{a}, []*emulator.Event{b}),
			"a run recorded without state hashes has none to compare")
	})

	t.Run("an omitted body compares by digest", func(t *testing.T) {
		a := ev(1, "ListQueues", 200, ``, "")
		b := ev(1, "ListQueues", 200, ``, "")
		a.ResponseBodyOmitted = &emulator.BodyDigest{Length: 1}
		b.ResponseBodyOmitted = &emulator.BodyDigest{Length: 2}
		diffs := emulator.CompareRecordedRuns([]*emulator.Event{a}, []*emulator.Event{b})
		require.Len(t, diffs, 1)
		assert.Equal(t, "response_body_omitted", diffs[0].Field)
	})

	t.Run("a nil response compares as empty", func(t *testing.T) {
		a := &emulator.Event{Sequence: 1, Service: "s3", Operation: "HeadBucket", StatusCode: 404}
		b := &emulator.Event{Sequence: 1, Service: "s3", Operation: "HeadBucket", StatusCode: 404}
		assert.Empty(t, emulator.CompareRecordedRuns([]*emulator.Event{a}, []*emulator.Event{b}))
		count := emulator.RunDifference{Field: "count", Expected: 2, Actual: 1}
		assert.Equal(t, "count: 2 != 1", count.String())
	})
}

// RunThenReplay passes a journey whose replay into a second fresh server reproduces it.
func TestRunThenReplay_DeterministicJourneyPasses(t *testing.T) {
	t.Parallel()
	emulator.RunThenReplay(t, detEpoch, runTwiceJourney)
}
