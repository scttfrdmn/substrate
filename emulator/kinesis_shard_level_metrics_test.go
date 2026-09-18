package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Shard-level metrics, and the StreamARN the three responses owed — #999.
//
// Four separate defects sat in the two enhanced-monitoring handlers and in UpdateShardCount, and they
// are tested apart because three of them are invisible while the fourth is present:
//
//  1. **StreamARN was omitted** from all three responses although every one of the three pages
//     publishes it. #966 taught fifteen operations to *read* a StreamARN; no response gained one.
//  2. **CurrentShardLevelMetrics reported the after-state.** Both handlers rendered the same slice
//     for Current and Desired, after the write, where the page says Current is "the current state of
//     the metrics that are in the enhanced state before the operation."
//  3. **disableEnhancedMonitoring filtered its stored slice in place** — kept :=
//     stream.EnhancedMonitoring[:0] aliased the backing array — so the before-state was destroyed as
//     the filter ran. This is why (2) could not have been fixed by swapping two renders: the slice a
//     reordered render would read has already been clobbered. The test that catches it is the one
//     disabling *some* of a larger set, because an aliased filter and a correct one agree when
//     everything is removed.
//  4. **ShardLevelMetrics was never checked**, although its shape publishes Required: Yes, a 1–7 item
//     range and an eight-value enum.
//
// Every response is read off the raw body rather than a decoded struct, both because the key set
// itself is under assertion and because [] and null are the same value once decoded into a []string —
// and the distinction is the point of one of these tests.
//
// Requests reuse kinesis_stream_arn_test.go's signed helpers, so the account and Region travel in the
// Host and the credential scope. That matters for the last test here: a StreamARN in a response must
// name the stream the request *resolved to*, which for an ARN-only request is not the caller's own
// account and Region.

// kinesisMetricsPageOrder is the seven metric names in the order both API_EnableEnhancedMonitoring and
// API_DisableEnhancedMonitoring bullet them.
//
// Spelled out here rather than taken from the emulator so the test pins the order rather than agreeing
// with whatever the implementation happens to produce. Neither page states a *response* ordering, so
// this order is substrate's reading and needs pinning somewhere.
//
//nolint:gochecknoglobals // the published order, shared by every case in this file
var kinesisMetricsPageOrder = []string{
	"IncomingBytes",
	"IncomingRecords",
	"OutgoingBytes",
	"OutgoingRecords",
	"WriteProvisionedThroughputExceeded",
	"ReadProvisionedThroughputExceeded",
	"IteratorAgeMilliseconds",
}

// kinesisMetricsBody decodes an enhanced-monitoring response's four members, keeping the two arrays as
// raw JSON so a test can tell [] from null.
type kinesisMetricsBody struct {
	StreamName string          `json:"StreamName"`
	StreamARN  string          `json:"StreamARN"`
	Current    json.RawMessage `json:"CurrentShardLevelMetrics"`
	Desired    json.RawMessage `json:"DesiredShardLevelMetrics"`
}

// kinesisMetricsDecode decodes one enhanced-monitoring response body.
func kinesisMetricsDecode(t *testing.T, raw string) kinesisMetricsBody {
	t.Helper()
	var body kinesisMetricsBody
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &body), "decode body: %s", raw)
	return body
}

// kinesisMetricsList decodes one of the two arrays into a slice.
func kinesisMetricsList(t *testing.T, member json.RawMessage) []string {
	t.Helper()
	var list []string
	require.NoErrorf(t, json.Unmarshal(member, &list), "decode metric array: %s", member)
	return list
}

// kinesisMetricsCall posts one enhanced-monitoring operation for the caller's own stream, by name.
func kinesisMetricsCall(t *testing.T, ts *emulator.TestServer, op string, metrics []string) kinesisMetricsBody {
	t.Helper()
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, op, map[string]any{
		"StreamName":        kinesisARNStream,
		"ShardLevelMetrics": metrics,
	})
	return kinesisMetricsDecode(t, raw)
}

// TestKinesisMetrics_TheThreeResponsesReportTheStreamARN is the criterion the issue leads with.
//
// UpdateShardCount is in the table with the two monitoring operations because its omission is the same
// one: the member is published, the renderer exists (kinesisStreamARN), and nothing called it.
func TestKinesisMetrics_TheThreeResponsesReportTheStreamARN(t *testing.T) {
	t.Parallel()

	cases := map[string]map[string]any{
		"EnableEnhancedMonitoring": {
			"ShardLevelMetrics": []string{"IncomingBytes"},
		},
		"DisableEnhancedMonitoring": {
			"ShardLevelMetrics": []string{"IncomingBytes"},
		},
		"UpdateShardCount": {
			"TargetShardCount": 4,
			"ScalingType":      "UNIFORM_SCALING",
		},
	}

	for op, extra := range cases {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 2)

			body := map[string]any{"StreamName": kinesisARNStream}
			for k, v := range extra {
				body[k] = v
			}
			raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, op, body)

			want := kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisARNStream)
			assert.Equalf(t, want, kinesisARNMember(t, raw, "StreamARN"),
				"%s reported the wrong StreamARN: %s", op, raw)
			assert.Equalf(t, kinesisARNStream, kinesisARNMember(t, raw, "StreamName"),
				"%s reported the wrong StreamName: %s", op, raw)
		})
	}
}

// TestKinesisMetrics_CurrentIsTheSetBeforeTheOperation asserts the published before/after split on
// both operations, and is the test that catches the in-place filter.
//
// The disable removes one of two metrics rather than all of them, deliberately: an aliased filter and a
// correct one produce the same answer when the result is empty, so a test that disables everything
// passes against the bug.
func TestKinesisMetrics_CurrentIsTheSetBeforeTheOperation(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	first := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", []string{"IncomingBytes"})
	assert.Empty(t, kinesisMetricsList(t, first.Current),
		"the first enable on a fresh stream has an empty before-state")
	assert.Equal(t, []string{"IncomingBytes"}, kinesisMetricsList(t, first.Desired))

	second := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", []string{"OutgoingBytes"})
	assert.Equal(t, []string{"IncomingBytes"}, kinesisMetricsList(t, second.Current),
		"Current must report the set before this enable, not after it")
	assert.Equal(t, []string{"IncomingBytes", "OutgoingBytes"}, kinesisMetricsList(t, second.Desired))

	third := kinesisMetricsCall(t, ts, "DisableEnhancedMonitoring", []string{"IncomingBytes"})
	assert.Equal(t, []string{"IncomingBytes", "OutgoingBytes"}, kinesisMetricsList(t, third.Current),
		"Current must survive the filter that computes the after-state")
	assert.Equal(t, []string{"OutgoingBytes"}, kinesisMetricsList(t, third.Desired))
}

// TestKinesisMetrics_AnEmptySetIsAnEmptyArrayNotNull pins the shape both Sample Responses show.
//
// Both members publish "Minimum number of 1 item", yet Enable's sample carries
// "CurrentShardLevelMetrics": [] and Disable's carries "DesiredShardLevelMetrics": []. The samples are
// the authority on what a caller must handle, so the minimum is not a response guarantee — and a Go
// nil slice would have marshaled as null, which is the #938 defect class.
func TestKinesisMetrics_AnEmptySetIsAnEmptyArrayNotNull(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	enabled := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", []string{"IncomingBytes"})
	assert.JSONEq(t, `[]`, string(enabled.Current),
		"the before-state of a fresh stream is [], the shape Enable's own sample shows")

	cleared := kinesisMetricsCall(t, ts, "DisableEnhancedMonitoring", []string{"IncomingBytes"})
	assert.JSONEq(t, `[]`, string(cleared.Desired),
		"the after-state of a stream with nothing left enhanced is [], per Disable's own sample")
}

// TestKinesisMetrics_ALLExpandsIntoTheSevenMetrics records the reading that resolves the page's
// eight-value enum against its seven-item maximum.
//
// "The value "ALL" enables every metric", composed with DesiredShardLevelMetrics' own description —
// "the list of all the metrics that would be in the enhanced state after the operation" — names the
// seven, not the wildcard. The second half of the test is why it matters rather than being a cosmetic
// choice: against a stored ["ALL"], disabling one metric would remove nothing.
func TestKinesisMetrics_ALLExpandsIntoTheSevenMetrics(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	all := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", []string{"ALL"})
	assert.Equal(t, kinesisMetricsPageOrder, kinesisMetricsList(t, all.Desired),
		"ALL enables every metric, so the after-state names all seven")
	assert.NotContains(t, kinesisMetricsList(t, all.Desired), "ALL",
		"the wildcard itself is not one of the metrics in the enhanced state")

	one := kinesisMetricsCall(t, ts, "DisableEnhancedMonitoring", []string{"IncomingBytes"})
	assert.Equal(t, kinesisMetricsPageOrder, kinesisMetricsList(t, one.Current))
	assert.Equal(t, kinesisMetricsPageOrder[1:], kinesisMetricsList(t, one.Desired),
		"disabling one metric after an ALL leaves the other six")
}

// TestKinesisMetrics_DisableALLClearsEveryMetric is the wildcard's other direction, which the Disable
// page states in its own words: the value ALL disables every metric.
func TestKinesisMetrics_DisableALLClearsEveryMetric(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring",
		[]string{"IncomingBytes", "OutgoingRecords", "IteratorAgeMilliseconds"})
	cleared := kinesisMetricsCall(t, ts, "DisableEnhancedMonitoring", []string{"ALL"})

	assert.Equal(t, []string{"IncomingBytes", "OutgoingRecords", "IteratorAgeMilliseconds"},
		kinesisMetricsList(t, cleared.Current))
	assert.JSONEq(t, `[]`, string(cleared.Desired))
}

// TestKinesisMetrics_TheResponseOrderIsThePagesNotTheCallers pins the ordering reading.
//
// A set has no order and neither page states one, so substrate renders the page's own bullet order.
// The metrics go in reversed so an implementation echoing the request would fail here — which matters
// for replay: two runs of the same recorded request must render the same body.
func TestKinesisMetrics_TheResponseOrderIsThePagesNotTheCallers(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

	reversed := make([]string, 0, len(kinesisMetricsPageOrder))
	for i := len(kinesisMetricsPageOrder) - 1; i >= 0; i-- {
		reversed = append(reversed, kinesisMetricsPageOrder[i])
	}

	body := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", reversed)
	assert.Equal(t, kinesisMetricsPageOrder, kinesisMetricsList(t, body.Desired),
		"the response orders the metrics as the page bullets them, not as the caller sent them")
}

// TestKinesisMetrics_RefusesAMemberItsShapeForbids covers the four constraints ShardLevelMetrics
// publishes, on both operations.
//
// Every case asserts the status alongside the code, per #923, and reads the code off the raw body. The
// eight-item case is the one that resolves the page's own contradiction: the enum has eight entries and
// the array admits seven, so naming every metric *and* ALL cannot satisfy both — substrate keeps the
// bound AWS states, and a caller wanting all seven sends ALL alone.
func TestKinesisMetrics_RefusesAMemberItsShapeForbids(t *testing.T) {
	t.Parallel()

	everyEnumValue := append(append([]string{}, kinesisMetricsPageOrder...), "ALL")

	cases := map[string]any{
		"an empty array, where the shape publishes a minimum of 1 item": []string{},
		"eight items, where the shape publishes a maximum of 7":         everyEnumValue,
		"a metric name that is not in the enum":                         []string{"IncomingPackets"},
		"a valid metric in the wrong case":                              []string{"incomingbytes"},
		"one bad metric among several good ones": []string{
			"IncomingBytes", "OutgoingBytes", "NotAMetric",
		},
	}

	for _, op := range []string{"EnableEnhancedMonitoring", "DisableEnhancedMonitoring"} {
		for name, metrics := range cases {
			t.Run(op+"/"+name, func(t *testing.T) {
				t.Parallel()
				ts := kinesisARNServer(t)
				kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

				status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op,
					map[string]any{"StreamName": kinesisARNStream, "ShardLevelMetrics": metrics})
				assert.Equalf(t, http.StatusBadRequest, status, "%s with %s: %s", op, name, raw)
				assert.Equalf(t, "InvalidArgumentException", code, "%s with %s: %s", op, name, raw)

				// A refused request writes nothing (#965): the stream's set is still empty, which the
				// next enable's own before-state reports.
				after := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", []string{"IncomingBytes"})
				assert.Emptyf(t, kinesisMetricsList(t, after.Current),
					"%s with %s changed the stream's metric set: %s", op, name, raw)
			})
		}
	}
}

// TestKinesisMetrics_AnAbsentMemberIsRefused is separate from the constraint table because an absent
// member and an empty array are different JSON and the page treats them differently: absent fails
// Required: Yes, where [] fails the minimum. Both land on the same code, so the distinction lives in
// the message rather than in the answer a caller matches on.
func TestKinesisMetrics_AnAbsentMemberIsRefused(t *testing.T) {
	t.Parallel()

	for _, op := range []string{"EnableEnhancedMonitoring", "DisableEnhancedMonitoring"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := kinesisARNServer(t)
			kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)

			status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, op,
				map[string]any{"StreamName": kinesisARNStream})
			assert.Equalf(t, http.StatusBadRequest, status, "%s with no ShardLevelMetrics: %s", op, raw)
			assert.Equalf(t, "InvalidArgumentException", code, "%s with no ShardLevelMetrics: %s", op, raw)
			assert.Containsf(t, raw, "ShardLevelMetrics is required",
				"%s must name the member it is missing: %s", op, raw)
		})
	}
}

// TestKinesisMetrics_TheStreamARNNamesTheResolvedTarget is the direction a "does the member appear"
// test misses.
//
// The ARN in a response has to name the stream the request addressed, which for an ARN-only request is
// the ARN's own account and Region — not the caller's. Rendering it from the stored record would have
// happened to pass here; rendering it from the request context would not. The stream exists under the
// same name in both accounts, so a resolution that fell back to the caller's own would find one and
// answer 200 with the wrong ARN.
func TestKinesisMetrics_TheStreamARNNamesTheResolvedTarget(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisARNStream, 1)
	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNWestRegion, kinesisARNStream, 1)

	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNWestRegion, kinesisARNStream)
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "EnableEnhancedMonitoring",
		map[string]any{"StreamARN": foreign, "ShardLevelMetrics": []string{"IncomingBytes"}})

	assert.Equal(t, foreign, kinesisARNMember(t, raw, "StreamARN"),
		"the response must name the stream the ARN resolved to, not the caller's own")

	// And the caller's own stream is untouched, so the 200 above was not served from it.
	own := kinesisMetricsCall(t, ts, "EnableEnhancedMonitoring", []string{"OutgoingBytes"})
	assert.Empty(t, kinesisMetricsList(t, own.Current),
		"the cross-account enable must not have written to the caller's same-named stream")
}
