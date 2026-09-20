package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The request log an out-of-process consumer counts with — #1237.
//
// A consumer that runs `substrate server` as a separate process cannot reach
// [emulator.EventStore.GetEvents]; objectfs mounts a bucket under a real
// `sudo mount -t objectfs`, so the mount is a different process by construction. Its
// only alternative was to list the bucket before and after and diff the listing, which
// cannot see a PUT that rewrote an object with identical bytes and cannot tell "nothing
// was written" from "a write was attempted and refused".
//
// `GET /v1/debug/events` already carried the data. What it could not do was answer a
// question about one operation, and what it answered about a long run was a count that
// was silently short — the limit trims the *oldest* matching events, so a write early
// in the run fell out of the window and the endpoint gave the same false negative as
// the state diff. Both are asserted here over the wire rather than through the store,
// because the wire is the only surface the consumer has.
//
// Every case runs with `include_bodies` **off**, which is the default and therefore the
// configuration a consumer runs: a proof that only holds with bodies recorded would not
// be a proof for the process that needed it.

// newRequestLogServer starts a server recording events with bodies off, as the default
// configuration does, and returns it with the store behind it.
func newRequestLogServer(t *testing.T) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	cfg.EventStore.Enabled = true
	cfg.EventStore.IncludeBodies = false
	cfg.Server.ReadTimeout = "5s"
	cfg.Server.WriteTimeout = "5s"
	cfg.Server.ShutdownTimeout = "1s"

	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig(), emulator.WithTimeController(tc))
	require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc, logger, store, nil))

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// requestLogPage is one answer from `GET /v1/debug/events`.
type requestLogPage struct {
	// Events is the page, oldest first, trimmed to the limit.
	Events []struct {
		Sequence   int64  `json:"seq"`
		Service    string `json:"service"`
		Operation  string `json:"operation"`
		StatusCode int    `json:"status_code"`
		ErrorCode  string `json:"error_code"`
		Error      string `json:"error"`
	} `json:"events"`

	// Count is the length of Events.
	Count int `json:"count"`

	// Total is how many events matched the filter before the limit was applied,
	// which is the number a counting caller asserts on.
	Total int `json:"total"`

	// Truncated says whether Count is short of Total.
	Truncated bool `json:"truncated"`
}

// readRequestLog fetches one page of the request log through the server's own router.
func readRequestLog(t *testing.T, srv *emulator.Server, query string) requestLogPage {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/debug/events"+query, nil))
	require.Equal(t, http.StatusOK, rec.Code, "%s: %s", query, rec.Body.String())

	var page requestLogPage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page), rec.Body.String())
	return page
}

// TestRequestLog_CountsOneOperationOutOfProcess is #1237's question asked of a real run:
// how many PUTs did this process issue?
//
// The run issues two PutObjects and one GetObject, and each count is read with a single
// request naming the operation. Before the operation filter, the same question needed
// the whole log pulled and filtered by the caller — and a caller that filtered on a
// field it guessed the name of would have counted every event in the log instead.
func TestRequestLog_CountsOneOperationOutOfProcess(t *testing.T) {
	t.Parallel()
	srv := newRequestLogServer(t)

	require.Equal(t, http.StatusOK, s3Request(t, srv, "PUT", "/count-bucket", nil, nil).Code)
	s3Request(t, srv, "PUT", "/count-bucket/one", []byte("a"), nil)
	s3Request(t, srv, "PUT", "/count-bucket/two", []byte("b"), nil)
	s3Request(t, srv, "GET", "/count-bucket/one", nil, nil)

	puts := readRequestLog(t, srv, "?service=s3&operation=PutObject")
	assert.Equal(t, 2, puts.Total, "two objects were written")
	assert.False(t, puts.Truncated)
	for _, ev := range puts.Events {
		assert.Equal(t, "PutObject", ev.Operation)
		assert.Equal(t, "s3", ev.Service)
	}

	gets := readRequestLog(t, srv, "?service=s3&operation=GetObject")
	assert.Equal(t, 1, gets.Total, "one object was read")

	// The assertion the consumer actually wants to make, on an operation the run never
	// issued: zero, from a log that is not empty.
	deletes := readRequestLog(t, srv, "?service=s3&operation=DeleteObject")
	assert.Equal(t, 0, deletes.Total, "nothing was deleted")
	assert.Empty(t, deletes.Events)

	all := readRequestLog(t, srv, "?service=s3")
	assert.Greater(t, all.Total, puts.Total, "the filter narrowed the log rather than returning it whole")
}

// TestRequestLog_OpIsAnAliasForOperation pins the spelling #1237 proposed.
//
// Ignoring an unrecognized spelling would answer "how many PutObjects" with every event
// recorded, so the two names have to agree rather than one of them being silently
// dropped. `operation` is canonical because it is the member's name in each entry.
func TestRequestLog_OpIsAnAliasForOperation(t *testing.T) {
	t.Parallel()
	srv := newRequestLogServer(t)

	s3Request(t, srv, "PUT", "/alias-bucket", nil, nil)
	s3Request(t, srv, "PUT", "/alias-bucket/one", []byte("a"), nil)
	s3Request(t, srv, "GET", "/alias-bucket/one", nil, nil)

	canonical := readRequestLog(t, srv, "?service=s3&operation=PutObject")
	alias := readRequestLog(t, srv, "?service=s3&op=PutObject")
	assert.Equal(t, canonical.Total, alias.Total, "op= and operation= are one filter")
	assert.Equal(t, 1, alias.Total)

	// An operation nobody issued, through the alias: still narrowing, not ignored.
	assert.Equal(t, 0, readRequestLog(t, srv, "?op=DeleteObject").Total)
}

// TestRequestLog_TotalSurvivesTheLimit is the false negative the endpoint used to share
// with the state diff.
//
// `limit` trims the oldest matching events, so a write at the start of a long run is not
// in the page. `count` describes the page and `total` describes the run, and a caller
// asserting "nothing was written" has to be reading the second: with only the first, the
// write is invisible precisely when there was the most activity to hide it.
func TestRequestLog_TotalSurvivesTheLimit(t *testing.T) {
	t.Parallel()
	srv := newRequestLogServer(t)

	s3Request(t, srv, "PUT", "/limit-count-bucket", nil, nil)
	for _, key := range []string{"one", "two", "three", "four", "five"} {
		s3Request(t, srv, "PUT", "/limit-count-bucket/"+key, []byte(key), nil)
	}

	full := readRequestLog(t, srv, "?service=s3&operation=PutObject")
	require.Equal(t, 5, full.Total)
	require.False(t, full.Truncated, "the whole run fits under the default limit")

	page := readRequestLog(t, srv, "?service=s3&operation=PutObject&limit=2")
	assert.Equal(t, 5, page.Total, "the run wrote five objects however short the page is")
	assert.Equal(t, 2, page.Count)
	assert.Len(t, page.Events, 2)
	assert.True(t, page.Truncated, "a caller reading only the page is told it is not the whole run")

	// The page is the newest end of the run, which is what makes `total` the only
	// trustworthy count: the first PUT is the one that fell out.
	require.NotEmpty(t, page.Events)
	assert.Greater(t, page.Events[0].Sequence, full.Events[0].Sequence)
}

// TestRequestLog_ARefusedWriteIsDistinguishableFromAnAcceptedOne is the other half of
// what the state diff could not answer.
//
// A PUT into a bucket that does not exist changes no state, so a before/after listing
// reads exactly like a run that attempted nothing. The entry that tells them apart is the
// status the caller was answered with — and the status is what was missing: it was read
// off the recorded response, which the default configuration does not keep, and S3
// reports a refusal *as* a response rather than as an error, so the refusal left no trace
// in the log at all. Both requests are made here so the assertion is a difference rather
// than a constant either one could satisfy alone.
func TestRequestLog_ARefusedWriteIsDistinguishableFromAnAcceptedOne(t *testing.T) {
	t.Parallel()
	srv := newRequestLogServer(t)

	refused := s3Request(t, srv, "PUT", "/no-such-bucket-here/key", []byte("a"), nil)
	require.NotEqual(t, http.StatusOK, refused.Code, "the write is refused: %s", refused.Body.String())

	s3Request(t, srv, "PUT", "/accepted-bucket", nil, nil)
	accepted := s3Request(t, srv, "PUT", "/accepted-bucket/key", []byte("a"), nil)
	require.Equal(t, http.StatusOK, accepted.Code)

	puts := readRequestLog(t, srv, "?operation=PutObject")
	require.Equal(t, 2, puts.Total, "both attempts are recorded, including the one that wrote nothing")
	require.Len(t, puts.Events, 2)

	// Oldest first, so the refusal is the first entry.
	assert.Equal(t, refused.Code, puts.Events[0].StatusCode,
		"the log records the status the caller was answered with")
	assert.Equal(t, http.StatusOK, puts.Events[1].StatusCode)
	assert.NotEqual(t, puts.Events[0].StatusCode, puts.Events[1].StatusCode,
		"a refused write and an accepted one are two entries, not one shape twice")
}

// TestRequestLog_ARefusalReportedAsAnErrorCarriesItsCode covers the other way a plugin
// refuses: returning an [emulator.AWSError] rather than a response.
//
// Those refusals never reach a response for the status to be read off, so the status has
// to come from the error — [recordedStatusCode] takes it from the same place
// `writeError` does. `error_code` is the refusal's own AWS code, and it was already
// recorded unconditionally; what is asserted here is that the two agree with what the
// caller saw.
func TestRequestLog_ARefusalReportedAsAnErrorCarriesItsCode(t *testing.T) {
	t.Parallel()
	srv := newRequestLogServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://elasticloadbalancing.us-east-1.amazonaws.com/",
		nil)
	req.URL.RawQuery = "Action=DescribeAccountLimits&PageSize=401"
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	refusals := readRequestLog(t, srv, "?operation=DescribeAccountLimits")
	require.Equal(t, 1, refusals.Total)
	assert.Equal(t, http.StatusBadRequest, refusals.Events[0].StatusCode)
	assert.Equal(t, "ValidationError", refusals.Events[0].ErrorCode)
	assert.NotEmpty(t, refusals.Events[0].Error)
}
