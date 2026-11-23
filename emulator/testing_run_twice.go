package emulator

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

// Deterministic-run conformance (#1628).
//
// Deterministic-run mode (#1598) promises that two fresh servers given the same calls
// answer byte-identical bodies. Its acceptance test covers a handful of operations
// that were already deterministic; the rest of the surface is checked by running a
// whole journey twice and diffing what the two servers recorded. [RunTwice] is that
// check, and [CompareRecordedRuns] is the comparison it makes, exported so a caller
// holding two event logs some other way can make it too.

// runTwiceDiffLimit caps the differences [RunTwice] reports, for the reason
// [replayBodyDiffLimit] caps them per body: one divergence early in a run shifts
// every event after it, and a thousand reports of that one fact hide it.
const runTwiceDiffLimit = 40

// RunDifference is one way two recorded runs disagree.
type RunDifference struct {
	// Sequence is the position of the pair of events in the first run's log, 1-based,
	// or 0 for a difference about the runs as a whole (their event counts).
	Sequence int

	// Operation names the first run's event as "service.Operation", empty for a
	// difference about the runs as a whole.
	Operation string

	// Field is what differs: "count", "operation", "request_id",
	// "parent_request_id", "status_code", "error", "header/" followed by the header
	// name, or a "response_body" path as [ReplayDifferences] reports it.
	Field string

	// Expected is the first run's value, Actual the second's.
	Expected, Actual interface{}
}

// String renders the difference on one line.
func (d RunDifference) String() string {
	if d.Sequence == 0 {
		return fmt.Sprintf("%s: %v != %v", d.Field, d.Expected, d.Actual)
	}
	return fmt.Sprintf("#%d %s %s: %v != %v", d.Sequence, d.Operation, d.Field, d.Expected, d.Actual)
}

// runTwiceIgnoredHeaders are the response headers [CompareRecordedRuns] does not
// compare. Date is written from the wall clock by the HTTP server, which is one of
// the exceptions deterministic-run mode documents.
var runTwiceIgnoredHeaders = map[string]bool{"date": true}

// CompareRecordedRuns pairs two runs' recorded events by position and reports every
// way they disagree: the number of events, and for each pair the service and
// operation, the request ID and parent request ID, the status code, the error, the
// response headers other than Date, and the response body under the normalisation
// policy [ReplayDifferences] applies.
//
// The events are paired by position in sequence order, so the comparison is only
// meaningful for runs whose requests did not overlap: requests a client sends
// concurrently draw their request IDs in scheduler order, which deterministic-run
// mode documents as an exception.
func CompareRecordedRuns(first, second []*Event) []RunDifference {
	first, second = sortedBySequence(first), sortedBySequence(second)
	var out []RunDifference
	if len(first) != len(second) {
		out = append(out, RunDifference{Field: "count", Expected: len(first), Actual: len(second)})
	}
	for i := 0; i < min(len(first), len(second)); i++ {
		out = append(out, compareRecordedEvents(i+1, first[i], second[i])...)
	}
	return out
}

// sortedBySequence returns a copy of events in ascending sequence order.
func sortedBySequence(events []*Event) []*Event {
	out := make([]*Event, len(events))
	copy(out, events)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out
}

// compareRecordedEvents reports how one pair of events disagrees.
func compareRecordedEvents(pos int, a, b *Event) []RunDifference {
	op := a.Service + "." + a.Operation
	var out []RunDifference
	add := func(field string, expected, actual interface{}) {
		out = append(out, RunDifference{Sequence: pos, Operation: op, Field: field, Expected: expected, Actual: actual})
	}
	if other := b.Service + "." + b.Operation; other != op {
		// Two different operations at one position: everything after this is the
		// comparison of unrelated responses, so this is the only difference worth
		// reporting for the pair.
		add("operation", op, other)
		return out
	}
	if a.RequestID != b.RequestID {
		add("request_id", a.RequestID, b.RequestID)
	}
	if a.ParentRequestID != b.ParentRequestID {
		add("parent_request_id", a.ParentRequestID, b.ParentRequestID)
	}
	if a.StatusCode != b.StatusCode {
		add("status_code", a.StatusCode, b.StatusCode)
	}
	if a.Error != b.Error {
		add("error", a.Error, b.Error)
	}
	for _, name := range headerUnion(a.Response, b.Response) {
		av, bv := responseHeader(a.Response, name), responseHeader(b.Response, name)
		if av != bv {
			add("header/"+name, av, bv)
		}
	}
	if a.ResponseBodyOmitted != nil || b.ResponseBodyOmitted != nil {
		if fmt.Sprint(a.ResponseBodyOmitted) != fmt.Sprint(b.ResponseBodyOmitted) {
			add("response_body_omitted", fmt.Sprint(a.ResponseBodyOmitted), fmt.Sprint(b.ResponseBodyOmitted))
		}
		return out
	}
	for _, d := range ReplayDifferences(bodyOnly(a.Response), bodyOnly(b.Response)) {
		add(d.Field, d.Expected, d.Actual)
	}
	return out
}

// bodyOnly is resp with its status code cleared, so [ReplayDifferences] reports the
// body alone; the status was compared from [Event.StatusCode], which an error
// answer records too.
func bodyOnly(resp *AWSResponse) *AWSResponse {
	if resp == nil {
		return nil
	}
	return &AWSResponse{StatusCode: http.StatusOK, Body: resp.Body}
}

// headerUnion returns the canonical name of every header either response carries,
// sorted, less [runTwiceIgnoredHeaders].
func headerUnion(a, b *AWSResponse) []string {
	seen := map[string]struct{}{}
	for _, resp := range []*AWSResponse{a, b} {
		if resp == nil {
			continue
		}
		for name := range resp.Headers {
			canonical := http.CanonicalHeaderKey(name)
			if !runTwiceIgnoredHeaders[strings.ToLower(canonical)] {
				seen[canonical] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// responseHeader reads a header case-insensitively, "" when resp is nil or lacks it.
func responseHeader(resp *AWSResponse, canonical string) string {
	if resp == nil {
		return ""
	}
	for name, v := range resp.Headers {
		if http.CanonicalHeaderKey(name) == canonical {
			return v
		}
	}
	return ""
}

// RunTwice runs journey against two fresh test servers, each started with
// [WithDeterministicRun] at epoch and with opts, and fails t unless the two servers
// recorded the same events: the same count, the same operations in the same order,
// and identical request IDs, statuses, errors, headers other than Date, and response
// bodies. See [CompareRecordedRuns] for the comparison.
//
// Each run is a subtest, "first" and "second", so a journey that fails says which
// run it failed in; a failed first run skips the second and the comparison. The
// journey must issue its requests one at a time: concurrent requests draw their
// request IDs in scheduler order, which deterministic-run mode does not promise to
// reproduce.
func RunTwice(t *testing.T, epoch time.Time, journey func(t *testing.T, ts *TestServer), opts ...TestServerOption) {
	t.Helper()
	serverOpts := append([]TestServerOption{WithDeterministicRun(epoch)}, opts...)
	var runs [2][]*Event
	for i, name := range []string{"first", "second"} {
		ok := t.Run(name, func(t *testing.T) {
			ts := StartTestServer(t, serverOpts...)
			journey(t, ts)
			events, err := ts.Store().GetEvents(t.Context(), EventFilter{})
			if err != nil {
				t.Fatalf("RunTwice: read the %s run's events: %v", name, err)
			}
			runs[i] = events
		})
		if !ok {
			t.Fatalf("RunTwice: the %s run failed, so the runs were not compared", name)
		}
	}
	diffs := CompareRecordedRuns(runs[0], runs[1])
	for i, d := range diffs {
		if i == runTwiceDiffLimit {
			t.Errorf("RunTwice: %d more differences not shown", len(diffs)-i)
			break
		}
		t.Errorf("RunTwice: %s", d)
	}
}

// RunThenReplay is [RunTwice] for a journey whose caller does not send the same requests in
// the same order on every run. It runs journey once, against a fresh server started with
// [WithDeterministicRun] at epoch and opts, and then replays what that server recorded, in
// recorded order, into a second fresh server started the same way. It fails t on every
// difference the replay reports and on every event the replay could not check, as
// [AssertReplayMatches] does.
//
// The Terraform journey needs it. Terraform walks independent resources in whichever order its
// scheduler picks, even at -parallelism=1, and its waiters poll on the wall clock, so two runs of
// one configuration issue the same calls in different orders and numbers. Deterministic-run mode
// promises identical answers only to identical sequences, so [RunTwice] would report the tool's
// scheduling. Replaying the first run gives the second server the identical sequence, which
// isolates what the comparison is for: a response that differs because substrate, not the
// caller, did something different.
func RunThenReplay(t *testing.T, epoch time.Time, journey func(t *testing.T, ts *TestServer), opts ...TestServerOption) {
	t.Helper()
	serverOpts := append([]TestServerOption{WithDeterministicRun(epoch)}, opts...)
	var events []*Event
	if !t.Run("run", func(t *testing.T) {
		ts := StartTestServer(t, serverOpts...)
		journey(t, ts)
		recorded, err := ts.Store().GetEvents(t.Context(), EventFilter{})
		if err != nil {
			t.Fatalf("RunThenReplay: read the run's events: %v", err)
		}
		events = recorded
	}) {
		t.Fatalf("RunThenReplay: the run failed, so it was not replayed")
	}
	t.Run("replay", func(t *testing.T) {
		ts := StartTestServer(t, serverOpts...)
		AssertReplayMatches(t, ts.ReplayEvents(t, events))
	})
}
