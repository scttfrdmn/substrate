package emulator_test

// Response-body comparison during replay verification (#817).
//
// Until this existed, replay verification compared a recorded outcome at the
// granularity of the status code and the error string, and two runs that agreed on
// 200 and differed on every value inside the document were reported as matching.
//
// Two halves here. The table drives the comparison on pairs of bodies, because the
// normalisation policy is a claim about specific documents — a JSON object whose
// members are reordered, an XML body whose indentation changed, a collection whose
// order changed — and a recording cannot be made to contain most of those on demand.
// The end-to-end tests then drive it through a real record-and-replay over the wire,
// because a comparison the engine never reaches is the defect #833 found arriving in
// a new place.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

func TestResponseBodyComparison(t *testing.T) {
	absent := emulator.ReplayBodyMemberAbsentForTest()

	tests := []struct {
		name     string
		recorded string
		replayed string
		want     []emulator.ReplayBodyDifference
	}{
		{
			name:     "two empty bodies agree",
			recorded: "",
			replayed: "",
		},
		{
			name:     "identical json agrees",
			recorded: `{"TableName":"orders","ItemCount":2}`,
			replayed: `{"TableName":"orders","ItemCount":2}`,
		},
		{
			// Normalisation 1: an object is a map, so two orderings are one document.
			name:     "json member order is normalised",
			recorded: `{"TableName":"orders","ItemCount":2}`,
			replayed: `{"ItemCount":2,"TableName":"orders"}`,
		},
		{
			name:     "a differing json scalar names its member",
			recorded: `{"Table":{"TableName":"orders","TableStatus":"ACTIVE"}}`,
			replayed: `{"Table":{"TableName":"orders","TableStatus":"CREATING"}}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Table/TableStatus", Expected: "ACTIVE", Actual: "CREATING"},
			},
		},
		{
			name:     "a differing json array element names its index",
			recorded: `{"Items":[{"id":"a"},{"id":"b"}]}`,
			replayed: `{"Items":[{"id":"a"},{"id":"c"}]}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Items/1/id", Expected: "b", Actual: "c"},
			},
		},
		{
			// Collection order is compared, not normalised: this is the shape of the
			// defect #864 fixed, and sorting either side would hide it.
			name:     "a reordered json array is a difference",
			recorded: `{"Names":["alpha","beta"]}`,
			replayed: `{"Names":["beta","alpha"]}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Names/0", Expected: "alpha", Actual: "beta"},
				{Field: "response_body/Names/1", Expected: "beta", Actual: "alpha"},
			},
		},
		{
			name:     "an element the replay did not produce is absent, not nil",
			recorded: `{"Names":["alpha","beta"]}`,
			replayed: `{"Names":["alpha"]}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Names/1", Expected: "beta", Actual: absent},
			},
		},
		{
			name:     "an element only the replay produced is reported",
			recorded: `{"Names":["alpha"]}`,
			replayed: `{"Names":["alpha","beta"]}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Names/1", Expected: absent, Actual: "beta"},
			},
		},
		{
			// The distinction bodyMemberAbsent exists for: a member carrying null and
			// a member that is not there are different divergences.
			name:     "a null member and a missing member are different differences",
			recorded: `{"A":null,"B":null}`,
			replayed: `{"A":null,"B":"set"}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/B", Expected: nil, Actual: "set"},
			},
		},
		{
			name:     "a member only the recording has is absent on the replay side",
			recorded: `{"A":1,"B":2}`,
			replayed: `{"A":1}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/B", Expected: "2", Actual: absent},
			},
		},
		{
			name:     "a value that changed kind is one difference at its own path",
			recorded: `{"Tags":{"env":"dev"}}`,
			replayed: `{"Tags":["env"]}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Tags", Expected: `{"env":"dev"}`, Actual: `["env"]`},
			},
		},
		{
			// UseNumber keeps the literal, so a body that stopped rendering a
			// float as a float is a difference rather than a normalisation.
			name:     "a number literal is compared as written",
			recorded: `{"Size":1.0}`,
			replayed: `{"Size":1}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Size", Expected: "1.0", Actual: "1"},
			},
		},
		{
			name:     "a member name containing a slash is escaped per RFC 6901",
			recorded: `{"a/b":1,"c~d":2}`,
			replayed: `{"a/b":9,"c~d":9}`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/a~1b", Expected: "1", Actual: "9"},
				{Field: "response_body/c~0d", Expected: "2", Actual: "9"},
			},
		},
		{
			// Normalisation 2: indentation between elements is not content.
			name:     "xml indentation is normalised",
			recorded: "<Result><Name>orders</Name></Result>",
			replayed: "<Result>\n  <Name>orders</Name>\n</Result>\n",
		},
		{
			name:     "a differing xml leaf names its element",
			recorded: "<Result><Bucket><Name>a</Name></Bucket></Result>",
			replayed: "<Result><Bucket><Name>b</Name></Bucket></Result>",
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Result/Bucket/Name", Expected: "a", Actual: "b"},
			},
		},
		{
			// A repeated sibling carries a one-based XPath index, so the third of
			// three names the third.
			name: "a repeated xml sibling carries its index",
			recorded: "<Buckets><Bucket><Name>a</Name></Bucket>" +
				"<Bucket><Name>b</Name></Bucket><Bucket><Name>c</Name></Bucket></Buckets>",
			replayed: "<Buckets><Bucket><Name>a</Name></Bucket>" +
				"<Bucket><Name>b</Name></Bucket><Bucket><Name>z</Name></Bucket></Buckets>",
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Buckets/Bucket[3]/Name", Expected: "c", Actual: "z"},
			},
		},
		{
			name:     "a differing xml attribute carries an at sign",
			recorded: `<Result><Item key="a">1</Item></Result>`,
			replayed: `<Result><Item key="b">1</Item></Result>`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Result/Item/@key", Expected: "a", Actual: "b"},
			},
		},
		{
			name:     "an attribute the replay dropped is absent",
			recorded: `<Result><Item key="a">1</Item></Result>`,
			replayed: `<Result><Item>1</Item></Result>`,
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Result/Item/@key", Expected: "a", Actual: absent},
			},
		},
		{
			name:     "an element the replay dropped is rendered and absent",
			recorded: "<Result><Name>a</Name><Region>us-east-1</Region></Result>",
			replayed: "<Result><Name>a</Name></Result>",
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Result/Region", Expected: "<Region>us-east-1</Region>", Actual: absent},
			},
		},
		{
			name:     "a child that changed name is one difference, not a subtree walk",
			recorded: "<Result><Name>a</Name></Result>",
			replayed: "<Result><Value>a</Value></Result>",
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Result/Name", Expected: "Name", Actual: "Value"},
			},
		},
		{
			name:     "a differing root element is one difference",
			recorded: "<Result><Name>a</Name></Result>",
			replayed: "<Other><Name>a</Name></Other>",
			want: []emulator.ReplayBodyDifference{
				{Field: "response_body/Result", Expected: "Result", Actual: "Other"},
			},
		},
		{
			// Once, at the element that declares it — not once per element the
			// decoder resolved it onto.
			name:     "a changed default namespace is reported where it is declared",
			recorded: `<Result xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>a</Name></Result>`,
			replayed: `<Result xmlns="http://example.invalid/"><Name>a</Name></Result>`,
			want: []emulator.ReplayBodyDifference{
				{
					Field:    "response_body/Result/@xmlns",
					Expected: "http://s3.amazonaws.com/doc/2006-03-01/",
					Actual:   "http://example.invalid/",
				},
			},
		},
		{
			// Normalisation 3, and the case that keeps a CBOR or object payload from
			// being reported as a parse failure.
			name:     "a body that parses as neither format is compared as bytes",
			recorded: "\x83\x01\x02\x03",
			replayed: "\x83\x01\x02\x04",
			want: []emulator.ReplayBodyDifference{
				{
					Field:    "response_body",
					Expected: `4 bytes: "\x83\x01\x02\x03"`,
					Actual:   `4 bytes: "\x83\x01\x02\x04"`,
				},
			},
		},
		{
			name:     "an empty body against a rendered one is reported whole",
			recorded: "",
			replayed: "<Result><Name>a</Name></Result>",
			want: []emulator.ReplayBodyDifference{
				{
					Field:    "response_body",
					Expected: "empty body",
					Actual:   `31 bytes: "<Result><Name>a</Name></Result>"`,
				},
			},
		},
		{
			name:     "two formats against each other are compared as bytes",
			recorded: `{"Name":"a"}`,
			replayed: "<Result><Name>a</Name></Result>",
			want: []emulator.ReplayBodyDifference{
				{
					Field:    "response_body",
					Expected: `12 bytes: "{\"Name\":\"a\"}"`,
					Actual:   `31 bytes: "<Result><Name>a</Name></Result>"`,
				},
			},
		},
		{
			// A wrong sniff costs precision, never correctness: the comparison still
			// answers whether the two bodies agree.
			name:     "malformed json falls back to a byte comparison",
			recorded: `{"Name":"a"}`,
			replayed: `{"Name":"a"`,
			want: []emulator.ReplayBodyDifference{
				{
					Field:    "response_body",
					Expected: `12 bytes: "{\"Name\":\"a\"}"`,
					Actual:   `11 bytes: "{\"Name\":\"a\""`,
				},
			},
		},
		{
			name:     "json with trailing content is not one document",
			recorded: `{"Name":"a"}`,
			replayed: `{"Name":"a"}{"Name":"b"}`,
			want: []emulator.ReplayBodyDifference{
				{
					Field:    "response_body",
					Expected: `12 bytes: "{\"Name\":\"a\"}"`,
					Actual:   `24 bytes: "{\"Name\":\"a\"}{\"Name\":\"b\"}"`,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := emulator.CompareResponseBodiesForTest([]byte(tc.recorded), []byte(tc.replayed))
			if len(tc.want) == 0 {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestResponseBodyComparison_StopsAtTheCapAndSaysSo asserts the bound is not
// silent. A difference near the root of a large document produces one difference per
// leaf, and a truncated list that looked complete would report a body as diverging
// in twenty places when it diverges in five hundred.
func TestResponseBodyComparison_StopsAtTheCapAndSaysSo(t *testing.T) {
	limit := emulator.ReplayBodyDiffLimitForTest()

	recorded := map[string]string{}
	replayed := map[string]string{}
	for i := range limit * 3 {
		key := fmt.Sprintf("member-%03d", i)
		recorded[key] = "recorded"
		replayed[key] = "replayed"
	}
	recordedBody, err := json.Marshal(recorded)
	require.NoError(t, err)
	replayedBody, err := json.Marshal(replayed)
	require.NoError(t, err)

	got := emulator.CompareResponseBodiesForTest(recordedBody, replayedBody)

	require.Len(t, got, limit+1, "the cap plus the marker that says the walk stopped")
	assert.Equal(t, emulator.ReplayBodyDiffTruncatedForTest(), got[limit].Actual)
	for _, diff := range got[:limit] {
		assert.Equal(t, "recorded", diff.Expected)
		assert.Equal(t, "replayed", diff.Actual)
	}
}

// TestResponseBodyComparison_MemberUnionIsDeterministic asserts the difference list
// itself does not depend on Go's map iteration order.
//
// It matters more here than it looks: the defect this comparison exists to detect is
// a body rendered in map order (#864), and a comparison whose own output was
// unstable could not be asserted on at all — the report would differ between two
// runs of one replay.
func TestResponseBodyComparison_MemberUnionIsDeterministic(t *testing.T) {
	recorded := []byte(`{"z":1,"m":1,"a":1,"q":1,"b":1,"y":1,"c":1,"x":1}`)
	replayed := []byte(`{"z":2,"m":2,"a":2,"q":2,"b":2,"y":2,"c":2,"x":2}`)

	first := emulator.CompareResponseBodiesForTest(recorded, replayed)
	require.Len(t, first, 8)
	for range 20 {
		assert.Equal(t, first, emulator.CompareResponseBodiesForTest(recorded, replayed))
	}

	fields := make([]string, 0, len(first))
	for _, diff := range first {
		fields = append(fields, diff.Field)
	}
	assert.Equal(t, []string{
		"response_body/a", "response_body/b", "response_body/c", "response_body/m",
		"response_body/q", "response_body/x", "response_body/y", "response_body/z",
	}, fields, "members are reported in sorted order")
}

// TestReplayBodyDiff_ARecordedListingReplaysByteIdentically is the passing half of
// #817's fourth criterion: a recorded run whose bodies carry no minted value replays
// with no difference at all.
//
// ListBuckets is the operation to prove it on, because its body carries three things
// a replay has to get right for the comparison to be usable: the bucket names, their
// creation dates — rendered from the simulated clock, which replayEvent freezes at the
// recorded event's timestamp — and the request id, which is reproducible only because
// the event records it (#866). Any one of the three going wrong would report a
// difference here.
//
// The word is "freezes" rather than "sets" because this comment said "sets" and that
// was the defect: SetTime alone sets a baseline the clock then advances from, so the
// creation dates were reproduced only to within the wall-clock latency of the replay
// path, and this test failed roughly once per (4 × latency / 1s) runs — including on a
// documentation-only PR, which is where it was found (#1217).
func TestReplayBodyDiff_ARecordedListingReplaysByteIdentically(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	for _, bucket := range []string{"diff-alpha", "diff-beta", "diff-gamma"} {
		replayPutBucket(t, ts, bucket)
	}
	listed := replayListBuckets(t, ts)
	require.Contains(t, listed, "diff-gamma", "the recording must contain the listing")

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	require.Zero(t, results.SkippedEvents)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
}

// TestReplayBodyDiff_APerturbedRecordedBodyIsReportedWithItsPath is the failing half:
// one byte range of one recorded body is changed, and the replay reports it at the
// path it sits at, naming the operation and the sequence.
//
// The perturbation stands in for the class of regression the comparison exists to
// catch — a plugin that starts rendering a different value into a body it used to get
// right — which cannot be produced on demand from a correct emulator. Everything
// else about the replay is unchanged, so a difference reported here is a difference
// the comparison found rather than one the replay caused.
func TestReplayBodyDiff_APerturbedRecordedBodyIsReportedWithItsPath(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	for _, bucket := range []string{"perturb-alpha", "perturb-beta"} {
		replayPutBucket(t, ts, bucket)
	}
	require.Contains(t, replayListBuckets(t, ts), "perturb-beta")

	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)

	var listing *emulator.Event
	for _, ev := range events {
		if ev.Operation == "ListBuckets" {
			listing = ev
		}
	}
	require.NotNil(t, listing, "the stream records no listing")
	require.NotNil(t, listing.Response, "the listing carries no recorded response")

	// The second bucket in the listing, because #864 made the order lexicographic and
	// an index in the reported path is only meaningful if the position is stable.
	perturbed := bytes.Replace(listing.Response.Body,
		[]byte("perturb-beta"), []byte("perturb-zeta"), 1)
	require.NotEqual(t, listing.Response.Body, perturbed, "the recorded body did not name the bucket")
	listing.Response.Body = perturbed

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	bodyDiffs := make([]*emulator.EventDifference, 0, len(results.Differences))
	for _, diff := range results.Differences {
		if strings.HasPrefix(diff.Field, "response_body") {
			bodyDiffs = append(bodyDiffs, diff)
		}
	}
	require.Len(t, bodyDiffs, 1, "%s", replayDifferenceSummary(results))

	diff := bodyDiffs[0]
	assert.Equal(t, "response_body/ListAllMyBucketsResult/Buckets/Bucket[2]/Name", diff.Field,
		"the path must name the position within the listing, not just the body")
	assert.Equal(t, "perturb-zeta", diff.Expected)
	assert.Equal(t, "perturb-beta", diff.Actual)
	assert.Equal(t, "ListBuckets", diff.Operation,
		"a difference names the operation, so a reader need not look the sequence back up")
	assert.Equal(t, listing.Sequence, diff.Sequence)
	assert.Equal(t, "major", diff.Significance,
		"the critical band is for a divergence in outcome, not in a reported value")
	assert.Empty(t, replayDifferencesOn(results, "status_code"),
		"the perturbation is inside the body; the status agreed")
}

// TestReplayBodyDiff_TheComparisonRunsWithoutStateValidation asserts the body
// comparison is not gated on [emulator.ReplayConfig.ValidateState].
//
// The two answer different questions, and a read is where they come apart: a
// GetBucketVersioning or a ListBuckets that rendered the wrong value writes nothing,
// so the state hash before and after are both untouched and a hash comparison — even
// when it is switched on — reports nothing. Gating the body comparison on state
// validation would leave every read-only operation unverified, which is most of a
// recorded run.
func TestReplayBodyDiff_TheComparisonRunsWithoutStateValidation(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	replayPutBucket(t, ts, "unvalidated-state")
	require.Contains(t, replayListBuckets(t, ts), "unvalidated-state")

	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)
	perturbations := 0
	for _, ev := range events {
		if ev.Operation != "ListBuckets" {
			continue
		}
		require.NotNil(t, ev.Response)
		ev.Response.Body = bytes.Replace(ev.Response.Body,
			[]byte("unvalidated-state"), []byte("something-else--"), 1)
		perturbations++
	}
	require.Equal(t, 1, perturbations, "exactly one listing was recorded")

	// ReplayConfig's zero value leaves ValidateState false, so no hash is compared.
	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Empty(t, replayDifferencesOn(results, "state_hash_before"))
	assert.Empty(t, replayDifferencesOn(results, "state_hash_after"))
	assert.True(t, results.StateValid, "nothing was validated, so nothing invalidated it")

	require.Len(t, results.Differences, 1, "%s", replayDifferenceSummary(results))
	assert.Equal(t, "response_body/ListAllMyBucketsResult/Buckets/Bucket/Name",
		results.Differences[0].Field)
}

// replayListBuckets issues ListBuckets over the wire and returns the body.
func replayListBuckets(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/", nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

// replayDifferenceSummary renders every difference on one line each, so a failing
// assertion on the count says which differences were found rather than printing a
// slice of pointers.
func replayDifferenceSummary(results *emulator.ReplayResults) string {
	if len(results.Differences) == 0 {
		return "no differences"
	}
	var sb strings.Builder
	for _, diff := range results.Differences {
		fmt.Fprintf(&sb, "\n  seq=%d op=%s %s: %v -> %v (%s)",
			diff.Sequence, diff.Operation, diff.Field, diff.Expected, diff.Actual, diff.Significance)
	}
	return sb.String()
}
