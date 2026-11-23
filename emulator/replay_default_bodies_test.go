package emulator_test

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// replayClockStart is the instant the #1599/#1601 tests freeze the clock at, so live and
// replayed state carry identical timestamps.
var replayClockStart = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// replayDo sends one request to ts and returns the status and body.
func replayDo(t *testing.T, ts *emulator.TestServer, method, path, host string, body []byte, header map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = host
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// TestReplay_DefaultTestServerReproducesItsSession is #1599's acceptance: a default
// StartTestServer records bodies, and replaying its stream reproduces the session.
func TestReplay_DefaultTestServerReproducesItsSession(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)

	replayPutBucket(t, ts, "default-bodies")
	replayPutObject(t, ts, "default-bodies", "a.txt", "one")
	status, body := replayDo(t, ts, http.MethodGet, "/default-bodies/a.txt", "s3.us-east-1.amazonaws.com", nil, nil)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "one", string(body))

	results := ts.AssertReplay(t)
	assert.Equal(t, 3, results.TotalEvents)
	assert.Equal(t, 3, results.SuccessEvents, "every event was re-executed")
	assert.Zero(t, results.SkippedEvents)

	// The server's own state is untouched by the replay, which ran in a sandbox.
	status, body = replayDo(t, ts, http.MethodGet, "/default-bodies/a.txt", "s3.us-east-1.amazonaws.com", nil, nil)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "one", string(body))
}

// TestReplay_CredentialHeadersAreNotRecorded pins the redaction rule: the headers,
// parameters and body fields that carry a credential, a signature or the signing time
// never reach an event, and the stream still replays. It covers a SigV4 header, a
// presigned SigV4 query, a SigV2 query, and a SigV2 form body both for a service whose
// recorded body is the form and for one whose recorded body is the form's JSON
// re-encoding.
func TestReplay_CredentialHeadersAreNotRecorded(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	const s3Host = "s3.us-east-1.amazonaws.com"
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	sigV2 := "&AWSAccessKeyId=AKIDSIGV2&Signature=c2lndjI%3D&SignatureMethod=HmacSHA256&SignatureVersion=2&SecurityToken=v2-token"

	replayDo(t, ts, http.MethodPut, "/redacted", s3Host, nil, map[string]string{
		"Authorization": "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260301/us-east-1/s3/aws4_request, " +
			"SignedHeaders=host, Signature=deadbeef",
		"X-Amz-Security-Token": "session-token",
		"X-Amz-Date":           "20260301T090000Z",
		"X-Amz-Meta-Kept":      "yes",
	})
	replayDo(t, ts, http.MethodGet,
		"/redacted?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20260301&X-Amz-Signature=cafe&list-type=2",
		s3Host, nil, nil)
	replayDo(t, ts, http.MethodGet, "/redacted?AWSAccessKeyId=AKIDSIGV2&Signature=c2lndjI%3D&Expires=1900000000",
		s3Host, nil, nil)
	status, body := replayDo(t, ts, http.MethodPost, "/", "sqs.us-east-1.amazonaws.com",
		[]byte("Action=CreateQueue&QueueName=redacted&Version=2012-11-05"+sigV2), form)
	require.Equal(t, http.StatusOK, status, string(body))
	status, body = replayDo(t, ts, http.MethodPost, "/", "ec2.us-east-1.amazonaws.com",
		[]byte("Action=CreateVpc&CidrBlock=10.9.0.0%2F16&Version=2016-11-15"+sigV2), form)
	require.Equal(t, http.StatusOK, status, string(body))

	events, err := ts.Store().GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, events, 5)
	for _, ev := range events {
		require.NotNil(t, ev.Request)
		raw, err := json.Marshal(ev)
		require.NoError(t, err)
		for _, secret := range []string{"AKIDEXAMPLE", "AKIDSIGV2", "session-token", "v2-token", "deadbeef", "cafe", "c2lndjI"} {
			assert.NotContains(t, string(raw), secret, "seq %d recorded a credential", ev.Sequence)
			assert.NotContains(t, string(ev.Request.Body), secret, "seq %d body", ev.Sequence)
		}
		for name := range ev.Request.Headers {
			for _, banned := range []string{"authorization", "x-amz-security-token", "x-amz-date"} {
				assert.NotEqual(t, banned, strings.ToLower(name), "seq %d recorded %s", ev.Sequence, name)
			}
		}
		for _, name := range []string{"X-Amz-Credential", "X-Amz-Signature", "AWSAccessKeyId", "Signature",
			"SignatureMethod", "SignatureVersion", "SecurityToken"} {
			assert.NotContains(t, ev.Request.Params, name, "seq %d", ev.Sequence)
		}
	}
	assert.Equal(t, "yes", events[0].Request.Headers["X-Amz-Meta-Kept"], "other headers are kept")
	assert.Equal(t, "2", events[1].Request.Params["list-type"], "other parameters are kept")
	assert.Equal(t, "1900000000", events[2].Request.Params["Expires"])
	assert.Contains(t, string(events[3].Request.Body), `"QueueName":"redacted"`, "the JSON re-encoding keeps the API members")
	assert.Contains(t, string(events[4].Request.Body), "CidrBlock=10.9.0.0%2F16", "the form keeps the API members")

	ts.AssertReplay(t)
}

// TestReplay_SignatureIsKeptWithoutSigV2 pins that a parameter named Signature is not
// dropped from a request that is not SigV2-signed.
func TestReplay_SignatureIsKeptWithoutSigV2(t *testing.T) {
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, IncludeBodies: true})
	require.NoError(t, store.RecordRequest(t.Context(), &emulator.RequestContext{RequestID: "req-1"},
		&emulator.AWSRequest{Service: "kms", Operation: "Verify", Params: map[string]string{"Signature": "keep"},
			MultiValueParams: map[string][]string{"X-Amz-Signature": {"a", "b"}, "tag": {"x", "y"}}},
		&emulator.AWSResponse{StatusCode: 200}, 0, 0, nil))
	events, err := store.GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	assert.Equal(t, "keep", events[0].Request.Params["Signature"])
	assert.Equal(t, map[string][]string{"tag": {"x", "y"}}, events[0].Request.MultiValueParams)
}

// newCappedServer starts a server whose event store caps recorded bodies at capBytes,
// and returns its URL and store.
func newCappedServer(t *testing.T, capBytes int) (*httptest.Server, *emulator.EventStore, *emulator.Config) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	cfg.EventStore.MaxBodyBytes = capBytes
	cfg.Log.Level = "error"
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(replayClockStart)
	tc.Freeze()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig(), emulator.WithTimeController(tc))
	require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc, logger, store, cfg))
	srv := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(srv.Close)
	return srv, store, cfg
}

// cappedDo sends one S3 request to srv.
func cappedDo(t *testing.T, srv *httptest.Server, method, path string, body []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s", method, path)
}

// TestReplay_BodyCap pins event_store.max_body_bytes: a request body over the cap is
// replaced by its digest and the event is skipped with a reason, and a response body
// over the cap is compared by digest.
func TestReplay_BodyCap(t *testing.T) {
	srv, store, cfg := newCappedServer(t, 64)
	big := bytes.Repeat([]byte("x"), 200)

	cappedDo(t, srv, http.MethodPut, "/capped", nil)
	cappedDo(t, srv, http.MethodPut, "/capped/small", []byte("tiny"))
	cappedDo(t, srv, http.MethodGet, "/", nil) // a ListBuckets answer longer than 64 bytes
	cappedDo(t, srv, http.MethodPut, "/capped/big", big)

	events, err := store.GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, events, 4)

	list := events[2]
	require.NotNil(t, list.ResponseBodyOmitted, "ListBuckets answered more than the cap")
	assert.Nil(t, list.Response.Body)
	assert.Nil(t, list.RequestBodyOmitted)

	put := events[3]
	require.NotNil(t, put.RequestBodyOmitted)
	assert.Equal(t, 200, put.RequestBodyOmitted.Length)
	assert.Len(t, put.RequestBodyOmitted.SHA256, 64)
	assert.Nil(t, put.Request.Body, "the body itself is not stored")

	sandbox, err := emulator.NewReplaySandbox(t.Context(), cfg, store, emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	results, err := sandbox.Engine().Replay(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)

	assert.Empty(t, results.Differences, "the ListBuckets digest matched: %s", replayDifferenceSummary(results))
	assert.Equal(t, map[emulator.ReplaySkipReason]int{emulator.SkipBodyOverCap: 1}, results.SkipReasons)
	assert.True(t, emulator.SkipBodyOverCap.Unverified())
}

// TestReplay_DigestMismatchIsADifference pins the other half of a response digest: a
// replayed body that hashes differently is reported.
func TestReplay_DigestMismatchIsADifference(t *testing.T) {
	srv, store, cfg := newCappedServer(t, 64)
	cappedDo(t, srv, http.MethodPut, "/digest-a", nil)
	cappedDo(t, srv, http.MethodGet, "/", nil)

	events, err := store.GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.NotNil(t, events[1].ResponseBodyOmitted)
	events[1].ResponseBodyOmitted.SHA256 = strings.Repeat("0", 64)

	sandbox, err := emulator.NewReplaySandbox(t.Context(), cfg, store, emulator.NewDefaultLogger(slog.LevelError, false))
	require.NoError(t, err)
	results, err := sandbox.Engine().Replay(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, results.Differences, 1)
	assert.Equal(t, "response_body", results.Differences[0].Field)
}

// TestReplay_BodyConfigDefaults pins the shipped defaults: bodies on, capped at 1 MiB.
func TestReplay_BodyConfigDefaults(t *testing.T) {
	cfg, err := emulator.LoadConfig("")
	require.NoError(t, err)
	assert.True(t, cfg.EventStore.IncludeBodies)
	assert.Equal(t, emulator.DefaultMaxBodyBytes, cfg.EventStore.MaxBodyBytes)
	assert.Equal(t, 1<<20, emulator.DefaultMaxBodyBytes)
	assert.Equal(t, cfg.EventStore.MaxBodyBytes, cfg.EventStore.ToEventStoreConfig().MaxBodyBytes)
}

// TestReplay_NegativeCapRecordsEveryBody pins that a negative cap disables it.
func TestReplay_NegativeCapRecordsEveryBody(t *testing.T) {
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, IncludeBodies: true, MaxBodyBytes: -1})
	big := bytes.Repeat([]byte("y"), emulator.DefaultMaxBodyBytes+1)
	require.NoError(t, store.RecordRequest(t.Context(), &emulator.RequestContext{RequestID: "req-1"},
		&emulator.AWSRequest{Service: "s3", Operation: "PutObject", Body: big},
		&emulator.AWSResponse{StatusCode: 200, Body: big}, 0, 0, nil))
	events, err := store.GetStream(t.Context(), emulator.DefaultStreamID)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Nil(t, events[0].RequestBodyOmitted)
	assert.Len(t, events[0].Request.Body, len(big))
	assert.Len(t, events[0].Response.Body, len(big))
}

// TestReplay_S3MultipartUploadReplays is the roadmap review's multipart finding: the
// UploadId must come from the request's mint, or UploadPart replays as NoSuchUpload.
func TestReplay_S3MultipartUploadReplays(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(replayClockStart)
	const host = "s3.us-east-1.amazonaws.com"

	replayPutBucket(t, ts, "multipart")
	status, body := replayDo(t, ts, http.MethodPost, "/multipart/big.bin?uploads", host, nil, nil)
	require.Equal(t, http.StatusOK, status, string(body))
	uploadID := xmlElement(t, body, "UploadId")

	status, body = replayDo(t, ts, http.MethodPut, "/multipart/big.bin?partNumber=1&uploadId="+uploadID, host,
		[]byte("part-one"), nil)
	require.Equal(t, http.StatusOK, status, string(body))

	complete := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"` +
		md5Hex("part-one") + `"</ETag></Part></CompleteMultipartUpload>`
	status, body = replayDo(t, ts, http.MethodPost, "/multipart/big.bin?uploadId="+uploadID, host, []byte(complete), nil)
	require.Equal(t, http.StatusOK, status, string(body))

	ts.AssertReplay(t)
}

// TestReplay_SQLiteStatusCodeColumn pins that the sqlite backend's status_code column
// holds the event's status, which is recorded whether or not bodies are.
func TestReplay_SQLiteStatusCodeColumn(t *testing.T) {
	dir := t.TempDir()
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true, Backend: "sqlite", PersistPath: dir, DSN: "status.db",
	})
	require.NoError(t, store.RecordRequest(t.Context(), &emulator.RequestContext{RequestID: "req-1"},
		&emulator.AWSRequest{Service: "s3", Operation: "PutObject"}, nil, 0, 0,
		&emulator.AWSError{Code: "AccessDenied", HTTPStatus: http.StatusForbidden}))
	require.NoError(t, store.Flush(t.Context()))
	require.NoError(t, store.Close())

	db, err := sql.Open("sqlite", filepath.Join(dir, "status.db"))
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck
	var status int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT status_code FROM events`).Scan(&status))
	assert.Equal(t, http.StatusForbidden, status)
}

// md5Hex returns the hex MD5 of s, the ETag S3 gives a part.
func md5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec
	return hex.EncodeToString(sum[:])
}
