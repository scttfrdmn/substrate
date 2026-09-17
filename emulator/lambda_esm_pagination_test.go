package emulator_test

// ListEventSourceMappings pages (#917).
//
// Both pagination parameters are published on the operation's URI — "GET
// /2015-03-31/event-source-mappings?EventSourceArn={EventSourceArn}&FunctionName={FunctionName}&
// Marker={Marker}&MaxItems={MaxItems}" — and substrate read neither, answering the whole listing
// with no NextMarker whatever a caller sent. A paging loop therefore terminated on its first
// response against substrate and first ran for real against an account holding more mappings than
// one page.
//
// Everything below goes through the operation's own wire protocol, and every mapping is created
// through CreateEventSourceMapping rather than written to state, per #765's rule that state written
// directly cannot prove a value is observable. The event sources are Kinesis stream ARNs
// throughout: an SQS ARN starts a poller goroutine (lambda_plugin.go, createEventSourceMapping),
// which a listing test has no use for.
//
// Two of MaxItems' three published bounds are asserted separately, because they are separate rules
// rather than one range: 1..10000 is what the parameter accepts, and 100 is what a response may
// carry "even if you set the number higher".

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// esmKinesisARN is the event source ARN a mapping is created against.
func esmKinesisARN(name string) string {
	return "arn:aws:kinesis:us-east-1:123456789012:stream/" + name
}

// esmCreate creates one event source mapping and returns its UUID.
func esmCreate(t *testing.T, srv *emulator.Server, functionName, eventSourceARN string) string {
	t.Helper()
	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings", map[string]any{
		"FunctionName":   functionName,
		"EventSourceArn": eventSourceARN,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created struct {
		UUID string `json:"UUID"`
	}
	decodeLambdaJSON(t, resp, &created)
	require.NotEmpty(t, created.UUID)
	return created.UUID
}

// esmListPage is one ListEventSourceMappings response, decoded.
type esmListPage struct {
	EventSourceMappings []struct {
		UUID           string `json:"UUID"`
		FunctionArn    string `json:"FunctionArn"`
		EventSourceArn string `json:"EventSourceArn"`
	} `json:"EventSourceMappings"`
	NextMarker string `json:"NextMarker"`
}

// uuids returns the page's mapping UUIDs in the order the response carries them.
func (p esmListPage) uuids() []string {
	out := make([]string, 0, len(p.EventSourceMappings))
	for _, esm := range p.EventSourceMappings {
		out = append(out, esm.UUID)
	}
	return out
}

// esmListRaw calls ListEventSourceMappings and returns the status and the raw body.
//
// The query is built with url.Values so a Marker containing "+" or "=" survives the round trip: a
// token written into the path by hand would arrive as a space, and the test would then be asserting
// against a token it corrupted itself rather than the one substrate issued.
func esmListRaw(t *testing.T, srv *emulator.Server, query url.Values) (int, string) {
	t.Helper()
	path := "/2015-03-31/event-source-mappings"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	resp := lambdaRequest(t, srv, http.MethodGet, path, nil)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// esmList calls ListEventSourceMappings and requires a 200, returning the decoded page.
func esmList(t *testing.T, srv *emulator.Server, query url.Values) esmListPage {
	t.Helper()
	status, raw := esmListRaw(t, srv, query)
	require.Equal(t, http.StatusOK, status, raw)
	var page esmListPage
	require.NoError(t, json.Unmarshal([]byte(raw), &page), raw)
	return page
}

// esmQuery builds a query string from alternating key/value pairs, omitting an empty value so a
// test can leave a parameter absent rather than sending it empty — the two are different requests.
func esmQuery(pairs ...string) url.Values {
	query := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			query.Set(pairs[i], pairs[i+1])
		}
	}
	return query
}

// esmErrorCode reads the error code out of a REST-JSON error body.
func esmErrorCode(t *testing.T, raw string) string {
	t.Helper()
	var shape struct {
		Code string `json:"Code"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &shape), raw)
	return shape.Code
}

// TestLambdaESMPagination_AbsentMaxItemsPagesAtThePublishedCap asserts the bound that is not the
// parameter's range: with no MaxItems at all a response carries at most 100 mappings and a
// NextMarker, because the page states the cap against every response rather than against a
// particular request.
//
// 101 mappings is the smallest listing that can tell the cap from "everything", which is what
// substrate answered before #917.
func TestLambdaESMPagination_AbsentMaxItemsPagesAtThePublishedCap(t *testing.T) {
	srv := newLambdaTestServer(t)
	const total = 101
	for i := 0; i < total; i++ {
		esmCreate(t, srv, "capped-fn", esmKinesisARN("stream-"+strconv.Itoa(i)))
	}

	page1 := esmList(t, srv, esmQuery())
	assert.Len(t, page1.EventSourceMappings, 100)
	require.NotEmpty(t, page1.NextMarker, "a listing longer than the cap must carry a NextMarker")

	page2 := esmList(t, srv, esmQuery("Marker", page1.NextMarker))
	assert.Len(t, page2.EventSourceMappings, total-100)
	assert.Empty(t, page2.NextMarker, "the last page carries no NextMarker")
}

// TestLambdaESMPagination_WalkReportsEveryMappingExactlyOnce is the assertion the operation exists
// to satisfy: a caller looping until NextMarker comes back empty sees the whole listing, in the
// same order and with no repeat and no omission, compared element for element against the
// unpaginated answer.
func TestLambdaESMPagination_WalkReportsEveryMappingExactlyOnce(t *testing.T) {
	srv := newLambdaTestServer(t)
	const total = 7
	for i := 0; i < total; i++ {
		esmCreate(t, srv, "walked-fn", esmKinesisARN("stream-"+strconv.Itoa(i)))
	}

	whole := esmList(t, srv, esmQuery()).uuids()
	require.Len(t, whole, total)

	var walked []string
	marker := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, total+2, "the walk did not terminate")
		page := esmList(t, srv, esmQuery("MaxItems", "3", "Marker", marker))
		assert.LessOrEqual(t, len(page.EventSourceMappings), 3)
		walked = append(walked, page.uuids()...)
		if page.NextMarker == "" {
			break
		}
		marker = page.NextMarker
	}
	assert.Equal(t, whole, walked)
}

// TestLambdaESMPagination_FullFinalPageCarriesNoMarker covers the exact-multiple case, where a
// token would name a page that does not exist: NextMarker is "returned when the response doesn't
// contain all event source mappings", and a full final page contains the rest of them.
func TestLambdaESMPagination_FullFinalPageCarriesNoMarker(t *testing.T) {
	srv := newLambdaTestServer(t)
	for i := 0; i < 6; i++ {
		esmCreate(t, srv, "exact-fn", esmKinesisARN("stream-"+strconv.Itoa(i)))
	}

	page1 := esmList(t, srv, esmQuery("MaxItems", "3"))
	require.Len(t, page1.EventSourceMappings, 3)
	require.NotEmpty(t, page1.NextMarker)

	page2 := esmList(t, srv, esmQuery("MaxItems", "3", "Marker", page1.NextMarker))
	assert.Len(t, page2.EventSourceMappings, 3)
	assert.Empty(t, page2.NextMarker, "a full final page must not carry a token")
	assert.NotContains(t, page2.uuids(), page1.uuids()[0])
}

// TestLambdaESMPagination_MaxItemsOutsideThePublishedRangeIsRefused asserts the parameter's own
// range, 1..10000, and that a value inside it is applied only up to the per-response cap. A value
// outside the range is refused rather than coerced: coercing would answer a page size the caller
// did not ask for, and nothing in the response would say so.
func TestLambdaESMPagination_MaxItemsOutsideThePublishedRangeIsRefused(t *testing.T) {
	srv := newLambdaTestServer(t)
	for i := 0; i < 3; i++ {
		esmCreate(t, srv, "ranged-fn", esmKinesisARN("stream-"+strconv.Itoa(i)))
	}

	for _, tc := range []struct {
		name     string
		maxItems string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"not an integer", "many"},
		{"one above the published maximum", "10001"},
	} {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			status, raw := esmListRaw(t, srv, esmQuery("MaxItems", tc.maxItems))
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "InvalidParameterValueException", esmErrorCode(t, raw), raw)
		})
	}

	t.Run("the published minimum is accepted", func(t *testing.T) {
		page := esmList(t, srv, esmQuery("MaxItems", "1"))
		assert.Len(t, page.EventSourceMappings, 1)
		assert.NotEmpty(t, page.NextMarker)
	})

	// The top of the range is a valid request that answers at most the cap — the two bounds are
	// different rules, and this is the case that proves the larger one is not read as a page size.
	t.Run("the published maximum is accepted and capped", func(t *testing.T) {
		page := esmList(t, srv, esmQuery("MaxItems", "10000"))
		assert.Len(t, page.EventSourceMappings, 3)
		assert.Empty(t, page.NextMarker)
	})
}

// TestLambdaESMPagination_RefusesAMarkerItDidNotIssue is #915 at this operation: a token substrate
// could not have issued is refused rather than answered with a well-formed page one, which is the
// one wrong answer a paging caller cannot detect.
func TestLambdaESMPagination_RefusesAMarkerItDidNotIssue(t *testing.T) {
	srv := newLambdaTestServer(t)
	for i := 0; i < 3; i++ {
		esmCreate(t, srv, "marked-fn", esmKinesisARN("stream-"+strconv.Itoa(i)))
	}

	// A token the operation issued still resumes the walk. Asserted first, so a refusal that
	// swallowed every token could not pass the rest of this test.
	page1 := esmList(t, srv, esmQuery("MaxItems", "1"))
	require.Len(t, page1.EventSourceMappings, 1)
	issued := page1.NextMarker
	require.NotEmpty(t, issued)

	page2 := esmList(t, srv, esmQuery("MaxItems", "1", "Marker", issued))
	require.Len(t, page2.EventSourceMappings, 1)
	assert.NotEqual(t, page1.uuids(), page2.uuids(), "an issued marker must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := esmListRaw(t, srv, esmQuery("MaxItems", "1", "Marker", tc.token))
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "InvalidParameterValueException", esmErrorCode(t, raw), raw)
			assert.NotContains(t, raw, page1.uuids()[0], "a refused marker must not be answered with page one")
		})
	}

	// A marker past the end is one substrate did issue, over a listing that has since shrunk, so it
	// clamps to a final empty page rather than being refused — the same reading the three other
	// offset sites record. The token is built here rather than issued because no listing substrate
	// can be asked to produce would hand one back.
	t.Run("a marker past the end clamps to an empty page", func(t *testing.T) {
		beyond := base64.StdEncoding.EncodeToString([]byte("99"))
		page := esmList(t, srv, esmQuery("MaxItems", "1", "Marker", beyond))
		assert.Empty(t, page.EventSourceMappings)
		assert.Empty(t, page.NextMarker)
	})

	// The refusal precedes the read of any state, per #887: the answer to a malformed marker must
	// not depend on how many mappings exist. Sealed against reads, the handler still refuses rather
	// than reporting the store's failure as a 500.
	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := newLambdaTestServerWithState(t, sealed)
		status, raw := esmListRaw(t, sealedSrv, esmQuery("Marker", "!!not-base64!!"))
		assert.Equal(t, http.StatusBadRequest, status, raw)
		assert.Equal(t, "InvalidParameterValueException", esmErrorCode(t, raw), raw)
	})

	// So does the refusal of a MaxItems outside the range, for the same reason.
	t.Run("MaxItems is refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := newLambdaTestServerWithState(t, sealed)
		status, raw := esmListRaw(t, sealedSrv, esmQuery("MaxItems", "0"))
		assert.Equal(t, http.StatusBadRequest, status, raw)
		assert.Equal(t, "InvalidParameterValueException", esmErrorCode(t, raw), raw)
	})
}

// TestLambdaESMPagination_FilterAndMaxItemsAreNotExclusive records the decision not to import EC2's
// rule: API_ListEventSourceMappings makes nothing mutually exclusive, so FunctionName and
// EventSourceArn narrow a page rather than forbidding one, and no InvalidParameterCombination is
// invented here.
//
// It also asserts the two code paths agree about order. A filtered listing walks a per-function
// index whose order is creation order, and an unfiltered one scans state keys, so the offset a
// marker names would mean two different positions if the index were not sorted.
func TestLambdaESMPagination_FilterAndMaxItemsAreNotExclusive(t *testing.T) {
	srv := newLambdaTestServer(t)
	const total = 5
	for i := 0; i < total; i++ {
		esmCreate(t, srv, "filtered-fn", esmKinesisARN("stream-"+strconv.Itoa(i)))
	}

	t.Run("FunctionName pages", func(t *testing.T) {
		page := esmList(t, srv, esmQuery("FunctionName", "filtered-fn", "MaxItems", "2"))
		assert.Len(t, page.EventSourceMappings, 2)
		assert.NotEmpty(t, page.NextMarker)
	})

	t.Run("EventSourceArn pages", func(t *testing.T) {
		page := esmList(t, srv, esmQuery("EventSourceArn", esmKinesisARN("stream-0")))
		require.Len(t, page.EventSourceMappings, 1)
		assert.Equal(t, esmKinesisARN("stream-0"), page.EventSourceMappings[0].EventSourceArn)
		assert.Empty(t, page.NextMarker)
	})

	t.Run("both filters and MaxItems together", func(t *testing.T) {
		page := esmList(t, srv, esmQuery(
			"FunctionName", "filtered-fn",
			"EventSourceArn", esmKinesisARN("stream-1"),
			"MaxItems", "1",
		))
		require.Len(t, page.EventSourceMappings, 1)
		assert.Equal(t, esmKinesisARN("stream-1"), page.EventSourceMappings[0].EventSourceArn)
	})

	t.Run("the filtered and unfiltered branches agree on order", func(t *testing.T) {
		filtered := esmList(t, srv, esmQuery("FunctionName", "filtered-fn")).uuids()
		scanned := esmList(t, srv, esmQuery()).uuids()
		require.Len(t, filtered, total)
		assert.Equal(t, scanned, filtered)
	})
}
