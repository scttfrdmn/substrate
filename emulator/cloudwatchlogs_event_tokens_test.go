package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Wire tests for #1223: GetLogEvents' pair of directional tokens, and the termination rule that is
// stated in terms of the pair.
//
// Everything below goes through CreateLogGroup / CreateLogStream / PutLogEvents / GetLogEvents. That
// matters more than usual here: the defect was that a caller following the published rule could not
// terminate, and "the caller" is whatever reads the wire, so a test that called the bounds helper
// directly would assert the arithmetic and not the thing that was broken.

// cwLogsEventPage is one GetLogEvents answer. The tokens are plain strings rather than pointers because
// the published Length Constraints are minimum 1 on both, so an empty one is a defect and not a
// distinguishable state worth modeling in the fixture.
type cwLogsEventPage struct {
	Events []struct {
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	} `json:"events"`
	NextForwardToken  string `json:"nextForwardToken"`
	NextBackwardToken string `json:"nextBackwardToken"`
}

// messages reduces a page to the messages it reported, in order.
func (p cwLogsEventPage) messages() []string {
	out := make([]string, len(p.Events))
	for i, ev := range p.Events {
		out[i] = ev.Message
	}
	return out
}

// cwLogsSeedStream creates a group and stream and puts count events in it, messaged "event-0" upward in
// ingestion order.
func cwLogsSeedStream(t *testing.T, srv *emulator.Server, group, stream string, count int) {
	t.Helper()
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": group})
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateLogGroup %s", group)
	resp = cwLogsRequest(t, srv, "CreateLogStream", map[string]any{"logGroupName": group, "logStreamName": stream})
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateLogStream %s", stream)

	events := make([]map[string]any, 0, count)
	for i := range count {
		events = append(events, map[string]any{
			"timestamp": 1700000000000 + int64(i)*1000,
			"message":   fmt.Sprintf("event-%d", i),
		})
	}
	if count == 0 {
		return
	}
	resp = cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName":  group,
		"logStreamName": stream,
		"logEvents":     events,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, "PutLogEvents %s", stream)
}

// cwLogsGetEvents calls GetLogEvents and decodes the page, requiring a 200.
func cwLogsGetEvents(t *testing.T, srv *emulator.Server, body map[string]any) cwLogsEventPage {
	t.Helper()
	resp := cwLogsRequest(t, srv, "GetLogEvents", body)
	raw := cwLogsReadBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GetLogEvents: %s", raw)
	var page cwLogsEventPage
	require.NoError(t, json.Unmarshal(raw, &page), "decode GetLogEvents: %s", raw)
	require.NotEmpty(t, page.NextForwardToken, "nextForwardToken is published never null: %s", raw)
	require.NotEmpty(t, page.NextBackwardToken, "nextBackwardToken is published not null: %s", raw)
	return page
}

// TestCWLogsGetLogEventsWalksForwardToTheDocumentedTermination is #1223's central assertion: a loop
// written from the page — "stop when the returned token equals the one I sent" — terminates, and reports
// every event once on the way.
//
// Before this, nextForwardToken was emitted only when a further page existed and nextBackwardToken never,
// so the comparison the page documents had nothing to compare against: a caller either stopped on its
// first call, having sent an empty token and been returned an empty one, or spun.
func TestCWLogsGetLogEventsWalksForwardToTheDocumentedTermination(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/walk", "forward", 5)

	var seen []string
	sent := ""
	for range 10 {
		body := map[string]any{
			"logGroupName": "/walk", "logStreamName": "forward",
			"limit": 2, "startFromHead": true,
		}
		if sent != "" {
			body["nextToken"] = sent
		}
		page := cwLogsGetEvents(t, srv, body)
		if sent != "" && page.NextForwardToken == sent {
			// The published termination condition, and the only one this page offers: "As long as the
			// nextBackwardToken or nextForwardToken returned is NOT equal to the nextToken that you passed
			// into the API call, there might be more log events available."
			assert.Empty(t, page.messages(), "the terminating page is past the end of the stream")
			break
		}
		seen = append(seen, page.messages()...)
		sent = page.NextForwardToken
	}

	assert.Equal(t, []string{"event-0", "event-1", "event-2", "event-3", "event-4"}, seen,
		"every event once, in ingestion order, and the loop terminated rather than being cut off")
}

// TestCWLogsGetLogEventsWalksBackwardToTheHead is the same walk in the other direction, which is the
// direction the pair exists to express and the one substrate did not report at all.
func TestCWLogsGetLogEventsWalksBackwardToTheHead(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/walk", "backward", 5)

	// No startFromHead and no token: the published default is false, so the first page is the tail.
	first := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/walk", "logStreamName": "backward", "limit": 2,
	})
	require.Equal(t, []string{"event-3", "event-4"}, first.messages(),
		"startFromHead defaults to false, so the latest events are returned first")

	seen := first.messages()
	sent := first.NextBackwardToken
	for range 10 {
		page := cwLogsGetEvents(t, srv, map[string]any{
			"logGroupName": "/walk", "logStreamName": "backward",
			"limit": 2, "nextToken": sent,
		})
		if page.NextBackwardToken == sent {
			assert.Empty(t, page.messages(), "the terminating page is before the head of the stream")
			break
		}
		seen = append(page.messages(), seen...)
		sent = page.NextBackwardToken
	}

	assert.Equal(t, []string{"event-0", "event-1", "event-2", "event-3", "event-4"}, seen,
		"a backward walk reaches the head and terminates there")
}

// TestCWLogsGetLogEventsTokensAreDirectional asserts the two tokens are distinguishable, which is what
// stops a backward token being silently read as a forward offset — the same class of wrong answer #1086
// refused a foreign token for, and the reason the prefix is part of the wire shape rather than an
// internal detail.
//
// The prefixes are the ones AWS's own example responses publish: `b/31132629…` and `f/31132629…`.
func TestCWLogsGetLogEventsTokensAreDirectional(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/directional", "stream", 4)

	page := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/directional", "logStreamName": "stream",
		"limit": 2, "startFromHead": true,
	})
	require.Equal(t, []string{"event-0", "event-1"}, page.messages())
	assert.True(t, strings.HasPrefix(page.NextForwardToken, "f/"), "forward token: %s", page.NextForwardToken)
	assert.True(t, strings.HasPrefix(page.NextBackwardToken, "b/"), "backward token: %s", page.NextBackwardToken)
	assert.NotEqual(t, page.NextForwardToken, page.NextBackwardToken,
		"the two tokens name different positions, so they cannot be the same string")

	// The forward token reads on toward the tail; the backward token from the same response reads back
	// toward the head, which for a first page means there is nothing before it.
	onward := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/directional", "logStreamName": "stream",
		"limit": 2, "nextToken": page.NextForwardToken,
	})
	assert.Equal(t, []string{"event-2", "event-3"}, onward.messages())

	back := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/directional", "logStreamName": "stream",
		"limit": 2, "nextToken": page.NextBackwardToken,
	})
	assert.Empty(t, back.messages(), "the first page's backward token names the head")
	assert.Equal(t, page.NextBackwardToken, back.NextBackwardToken,
		"and returns the same token, which is how the page says the end is reported")
}

// TestCWLogsGetLogEventsRefusesATokenWithoutADirection asserts the refusal a caller reaches by bringing
// a token from one of the other three Logs paginators, which all encode a bare offset.
//
// All four shared that wire shape before #1223, so DescribeLogGroups' token indexed into this stream and
// answered a well-formed page of the wrong thing. The prefix is what makes it refusable, and
// #1086's InvalidParameterException/400 is what it is refused with — the only 400-class code this page
// publishes for a bad parameter.
func TestCWLogsGetLogEventsRefusesATokenWithoutADirection(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/undirected", "stream", 3)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"a bare offset, as the other three Logs paginators issue", "MQ=="},
		{"a prefix with nothing after it", "f/"},
		{"a backward prefix with nothing after it", "b/"},
		{"a prefix on something that is not base64", "f/!!nope!!"},
		{"a prefix on base64 that is not an offset", "f/aGVsbG8="},
		{"the wrong prefix letter", "x/MQ=="},
		{"a prefix in the middle", "MQ==f/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, "GetLogEvents", map[string]any{
				"logGroupName": "/undirected", "logStreamName": "stream", "nextToken": tc.token,
			})
			raw := string(cwLogsReadBody(t, resp))
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, raw)
			assert.Contains(t, raw, "InvalidParameterException", raw)
			assert.Contains(t, raw, "GetLogEvents", "the message names the operation")
			assert.NotContains(t, raw, "event-0", "a refused token must not be answered with a page")
		})
	}
}

// TestCWLogsGetLogEventsStartFromHeadDecidesOnlyTheFirstPage pins what the parameter is read for, which
// is the divergence #1223's fourth criterion asked to be resolved one way or the other.
//
// It decides where a walk that presents no token begins. Once a token is present the token's own
// direction decides, so a forward token resumes forward whether or not startFromHead was sent —
// substrate does not enforce the page's *"you must specify true for startFromHead"*, because no Logs
// page publishes a code for violating it and inventing one would refuse a request AWS does not document
// refusing.
func TestCWLogsGetLogEventsStartFromHeadDecidesOnlyTheFirstPage(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/direction", "stream", 4)

	head := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/direction", "logStreamName": "stream", "limit": 2, "startFromHead": true,
	})
	assert.Equal(t, []string{"event-0", "event-1"}, head.messages(), "startFromHead true reads the head")

	tail := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/direction", "logStreamName": "stream", "limit": 2, "startFromHead": false,
	})
	assert.Equal(t, []string{"event-2", "event-3"}, tail.messages(), "startFromHead false reads the tail")

	absent := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/direction", "logStreamName": "stream", "limit": 2,
	})
	assert.Equal(t, tail.messages(), absent.messages(), "and absent means false, which is the published default")

	// The forward token is honored without startFromHead, which the page says a caller "must" send. The
	// requirement is not enforced, so the token decides.
	resumed := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/direction", "logStreamName": "stream",
		"limit": 2, "nextToken": head.NextForwardToken,
	})
	assert.Equal(t, []string{"event-2", "event-3"}, resumed.messages())
}

// TestCWLogsGetLogEventsReportsBothTokensForAnEmptyStream covers the case where the published "never
// null" rule is the whole of what there is to report: a stream with nothing in it still answers both
// tokens, and either one presented back comes out equal, so a walk terminates on its second call rather
// than having no token to compare.
func TestCWLogsGetLogEventsReportsBothTokensForAnEmptyStream(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/quiet", "stream", 0)

	page := cwLogsGetEvents(t, srv, map[string]any{"logGroupName": "/quiet", "logStreamName": "stream"})
	assert.Empty(t, page.messages())

	forward := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/quiet", "logStreamName": "stream", "nextToken": page.NextForwardToken,
	})
	assert.Equal(t, page.NextForwardToken, forward.NextForwardToken, "the forward token names the same position")

	backward := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/quiet", "logStreamName": "stream", "nextToken": page.NextBackwardToken,
	})
	assert.Equal(t, page.NextBackwardToken, backward.NextBackwardToken, "and so does the backward one")
}

// TestCWLogsGetLogEventsIssuesAForwardTokenOnAFullFinalPage is the difference between this cursor and the
// other three, written down as an assertion.
//
// [pageByOffsetToken] omits the token on a final page, which is right where its absence means "done".
// Here *equality* means done, so a full final page still carries a forward token and presenting it costs
// one round trip to an empty page. AWS describes exactly that when it says *"Partially full or empty
// pages don't necessarily mean that pagination is finished."* — the empty page is not the anomaly.
func TestCWLogsGetLogEventsIssuesAForwardTokenOnAFullFinalPage(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/exact", "stream", 4)

	page := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/exact", "logStreamName": "stream", "limit": 4, "startFromHead": true,
	})
	require.Len(t, page.Events, 4, "the page is exactly full")

	past := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/exact", "logStreamName": "stream",
		"limit": 4, "nextToken": page.NextForwardToken,
	})
	assert.Empty(t, past.messages(), "one round trip to an empty page")
	assert.Equal(t, page.NextForwardToken, past.NextForwardToken, "which reports itself as the end")
}

// TestCWLogsGetLogEventsClampsATokenPastATrimmedStream asserts the clamp rather than a refusal, for the
// reason decodeOffsetPaginationToken records: a token past the end is one substrate issued over a stream
// that has since gone away, and an empty final page is the honest answer where an error would break a
// walk whose events were deleted mid-loop.
func TestCWLogsGetLogEventsClampsATokenPastATrimmedStream(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsSeedStream(t, srv, "/trimmed", "stream", 6)

	page := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/trimmed", "logStreamName": "stream", "limit": 4, "startFromHead": true,
	})
	require.Len(t, page.Events, 4)

	// The stream goes away under the walk, and is recreated empty.
	resp := cwLogsRequest(t, srv, "DeleteLogStream", map[string]any{
		"logGroupName": "/trimmed", "logStreamName": "stream",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = cwLogsRequest(t, srv, "CreateLogStream", map[string]any{
		"logGroupName": "/trimmed", "logStreamName": "stream",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	past := cwLogsGetEvents(t, srv, map[string]any{
		"logGroupName": "/trimmed", "logStreamName": "stream",
		"limit": 4, "nextToken": page.NextForwardToken,
	})
	assert.Empty(t, past.messages(), "clamped to a final empty page rather than refused")
	assert.Equal(t, "f/MA==", past.NextForwardToken, "and the clamp is reported, so the walk terminates")
}
