package emulator_test

// A pagination token substrate never issued is refused, not answered with page one (#915).
//
// Four operations decoded their token and discarded the error, so a token substrate could not have
// issued — one from another operation, a truncated copy, a hand-written string, an offset left over
// from an older recording — selected the start of the listing and the operation answered a
// well-formed **page one**. That is the one wrong answer a paginating caller cannot detect: a loop
// that runs until the token comes back empty is handed the first page again, so it either spins or
// processes the same records twice, and nothing in the response says so. #884 named the shape and
// #887 fixed it for the RDS and ElastiCache markers.
//
// Every assertion here goes through the operation's own wire protocol — the CloudWatch Query
// protocol, Systems Manager's JSON target, S3's REST query string — and every listing is built by
// creating resources through real calls, per #765's rule that state written directly cannot prove a
// value is observable. Each of the four is checked twice over: a hand-written token is refused with
// the code and message that operation publishes, **and** a token the operation itself issued still
// resumes the walk. Without the second half the first would pass against an operation that refused
// every token.
//
// The third assertion at each site is about *ordering*, which is the criterion #887 established and
// the reason this is not just a decode fix: the token is validated before any state is read. It is
// asserted by sealing the state store against reads and requiring the refusal to arrive anyway. A
// handler that read first would answer 500 from the sealed store instead, which is exactly what three
// of the four did — CloudWatch loaded its alarm index and Systems Manager loaded its parameter paths
// before looking at the token. For S3 only the object listing is sealed, because the bucket-existence
// 404 keeps its precedence: the bucket is the resource the request addresses, and AWS publishes
// nothing about which of the two refusals wins.
//
// Provenance of the three codes is per operation and is recorded with each helper:
// [cwInvalidNextToken] and [ssmInvalidNextToken] are published, S3's is substrate's reading.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// tokenRefusalBadTokens are tokens substrate could not have issued, with the reason each one is not
// issuable.
//
// The list is the rule in [decodeOffsetPaginationToken] read back as cases: not base64, base64 of
// something that is not an integer, a negative offset, and the three forms strconv.Atoi accepts but
// the encoder never emits. The last three are the ones a decode that only checked strconv.Atoi's
// error would let through, and they are here because "a token substrate issued" is defined by what
// the encoder produces rather than by what a parser tolerates.
var tokenRefusalBadTokens = []struct {
	name  string
	token string
}{
	{"not base64", "!!not-base64!!"},
	{"base64 of a word", base64.StdEncoding.EncodeToString([]byte("abc"))},
	{"base64 of a space", base64.StdEncoding.EncodeToString([]byte(" "))},
	{"a negative offset", base64.StdEncoding.EncodeToString([]byte("-1"))},
	{"a signed offset the encoder never emits", base64.StdEncoding.EncodeToString([]byte("+1"))},
	{"a zero-padded offset the encoder never emits", base64.StdEncoding.EncodeToString([]byte("01"))},
	{"an offset with trailing space", base64.StdEncoding.EncodeToString([]byte("1 "))},
}

// errTokenRefusalSealed is what a sealed store answers a read with.
var errTokenRefusalSealed = errors.New("state store sealed by the test")

// tokenRefusalSealedState wraps a state manager and can be sealed against reads, so that a handler
// which reads state before validating its pagination token is distinguishable from one that does not.
//
// Sealing is a flag rather than a constructor argument because the resources a test asserts over have
// to be created first, through real wire calls. Get and List are sealed independently: S3's
// bucket-existence check is a Get that keeps its precedence over the token refusal, while the object
// listing it guards is a List that must not be reached.
type tokenRefusalSealedState struct {
	inner     emulator.StateManager
	sealGets  bool
	sealLists bool
}

// Get reads through to the wrapped manager unless gets are sealed.
func (s *tokenRefusalSealedState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if s.sealGets {
		return nil, errTokenRefusalSealed
	}
	return s.inner.Get(ctx, namespace, key)
}

// Put writes through to the wrapped manager.
func (s *tokenRefusalSealedState) Put(ctx context.Context, namespace, key string, value []byte) error {
	return s.inner.Put(ctx, namespace, key, value)
}

// Delete removes through to the wrapped manager.
func (s *tokenRefusalSealedState) Delete(ctx context.Context, namespace, key string) error {
	return s.inner.Delete(ctx, namespace, key)
}

// List reads through to the wrapped manager unless lists are sealed.
func (s *tokenRefusalSealedState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if s.sealLists {
		return nil, errTokenRefusalSealed
	}
	return s.inner.List(ctx, namespace, prefix)
}

// tokenRefusalBetween returns the text between the first open and close tag, for reading a token out
// of a Query-protocol or S3 response body.
//
// The token is read out of the response rather than constructed here, which is the point of the
// round-trip half of each assertion: a token this file built could agree with a wrong encoding.
func tokenRefusalBetween(t *testing.T, body, tag string) string {
	t.Helper()
	_, after, found := strings.Cut(body, "<"+tag+">")
	require.True(t, found, "no <%s> in %s", tag, body)
	value, _, found := strings.Cut(after, "</"+tag+">")
	require.True(t, found, "unterminated <%s> in %s", tag, body)
	return value
}

// --- CloudWatch DescribeAlarms ---

// tokenRefusalCWServer builds a CloudWatch server over a caller-supplied state manager, which
// [newCWAlarmTestServer] does not allow and the sealed-store assertion needs.
func tokenRefusalCWServer(t *testing.T, state emulator.StateManager) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	plugin := &emulator.CloudWatchPlugin{}
	require.NoError(t, plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// tokenRefusalPutAlarm creates one alarm through PutMetricAlarm.
func tokenRefusalPutAlarm(t *testing.T, srv *emulator.Server, name string) {
	t.Helper()
	resp := cwRequest(t, srv, map[string]string{
		"Action":             "PutMetricAlarm",
		"AlarmName":          name,
		"MetricName":         "CPUUtilization",
		"Namespace":          "AWS/EC2",
		"Statistic":          "Average",
		"ComparisonOperator": "GreaterThanThreshold",
		"Threshold":          "80",
		"EvaluationPeriods":  "1",
		"Period":             "60",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "PutMetricAlarm %s", name)
}

// tokenRefusalDescribeAlarms pages DescribeAlarms and returns the status and body.
func tokenRefusalDescribeAlarms(t *testing.T, srv *emulator.Server, maxRecords, nextToken string) (int, string) {
	t.Helper()
	params := map[string]string{"Action": "DescribeAlarms"}
	if maxRecords != "" {
		params["MaxRecords"] = maxRecords
	}
	if nextToken != "" {
		params["NextToken"] = nextToken
	}
	resp := cwRequest(t, srv, params)
	defer resp.Body.Close() //nolint:errcheck
	return resp.StatusCode, cwReadBody(t, resp)
}

// TestPaginationToken_CloudWatchDescribeAlarmsRefusesATokenItDidNotIssue is #915 at the site whose
// token decode discarded both errors and whose alarm index was read first.
func TestPaginationToken_CloudWatchDescribeAlarmsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalCWServer(t, emulator.NewMemoryStateManager())
	for _, name := range []string{"alarm-a", "alarm-b", "alarm-c"} {
		tokenRefusalPutAlarm(t, srv, name)
	}

	// The token the operation issues resumes the walk. Asserted first, so a refusal that swallowed
	// every token could not pass the rest of this test.
	status, page1 := tokenRefusalDescribeAlarms(t, srv, "1", "")
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, "alarm-a")
	assert.NotContains(t, page1, "alarm-b")
	issued := tokenRefusalBetween(t, page1, "NextToken")
	require.NotEmpty(t, issued)

	status, page2 := tokenRefusalDescribeAlarms(t, srv, "1", issued)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "alarm-b", "an issued token must resume after the first page")
	assert.NotContains(t, page2, "alarm-a", "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tokenRefusalDescribeAlarms(t, srv, "1", tc.token)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Contains(t, body, "<Code>InvalidNextToken</Code>", body)
			assert.Contains(t, body, "The next token specified is invalid.", body)
			assert.NotContains(t, body, "alarm-a", "a refused token must not be answered with page one")
		})
	}

	// The refusal precedes the read of the alarm index: sealed against reads, the handler still
	// refuses rather than reporting the store's failure as a 500.
	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalCWServer(t, sealed)
		status, body := tokenRefusalDescribeAlarms(t, sealedSrv, "1", "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Contains(t, body, "<Code>InvalidNextToken</Code>", body)
	})
}

// --- Systems Manager DescribeParameters and GetParametersByPath ---

// tokenRefusalSSMServer builds a Systems Manager server over a caller-supplied state manager.
func tokenRefusalSSMServer(t *testing.T, state emulator.StateManager) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	plugin := &emulator.SSMPlugin{}
	require.NoError(t, plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// tokenRefusalSSMCall posts one Systems Manager operation and returns the status, the raw body and
// the bare error code a refusal carries.
//
// The body is passed as bytes rather than as a Go value so that a token which is not valid base64
// reaches the handler exactly as written.
func tokenRefusalSSMCall(t *testing.T, srv *emulator.Server, op, body string) (int, string, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	r.Host = "ssm.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "AmazonSSM."+op)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/"+
		"us-east-1/ssm/aws4_request, SignedHeaders=host, Signature=fake")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s body", op)

	var errShape struct {
		Type string `json:"__type"`
	}
	code := ""
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr == nil {
		code = awsErrorCode(errShape.Type)
	}
	return resp.StatusCode, string(raw), code
}

// tokenRefusalPutParameter creates one parameter through PutParameter.
func tokenRefusalPutParameter(t *testing.T, srv *emulator.Server, name string) {
	t.Helper()
	status, body, code := tokenRefusalSSMCall(t, srv, "PutParameter",
		fmt.Sprintf(`{"Name":%q,"Value":"v","Type":"String"}`, name))
	require.Empty(t, code, "PutParameter %s: %s", name, body)
	require.Equal(t, http.StatusOK, status, body)
}

// tokenRefusalSSMNextToken reads the NextToken out of a paginated Systems Manager response.
func tokenRefusalSSMNextToken(t *testing.T, body string) string {
	t.Helper()
	var out struct {
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out), "decode %s", body)
	require.NotEmpty(t, out.NextToken, "no NextToken in %s", body)
	return out.NextToken
}

// TestPaginationToken_SSMDescribeParametersRefusesATokenItDidNotIssue is #915 at Systems Manager's
// parameter listing, whose parameter paths were read before the token was looked at.
func TestPaginationToken_SSMDescribeParametersRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalSSMServer(t, emulator.NewMemoryStateManager())
	for _, name := range []string{"/app/one", "/app/two", "/app/three"} {
		tokenRefusalPutParameter(t, srv, name)
	}

	status, page1, code := tokenRefusalSSMCall(t, srv, "DescribeParameters", `{"MaxResults":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	issued := tokenRefusalSSMNextToken(t, page1)

	status, page2, code := tokenRefusalSSMCall(t, srv, "DescribeParameters",
		fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.NotEqual(t, page1, page2, "an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalSSMCall(t, srv, "DescribeParameters",
				fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidNextToken", code, body)
			assert.Contains(t, body, "The specified token isn't valid.", body)
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalSSMServer(t, sealed)
		status, body, code := tokenRefusalSSMCall(t, sealedSrv, "DescribeParameters",
			`{"MaxResults":1,"NextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidNextToken", code, body)
	})
}

// TestPaginationToken_SSMGetParametersByPathRefusesATokenItDidNotIssue is the same defect at the
// operation a caller actually walks a hierarchy with, which is why it is asserted separately rather
// than trusting the shared helper: the two decoded their tokens with two copies of the same block.
func TestPaginationToken_SSMGetParametersByPathRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalSSMServer(t, emulator.NewMemoryStateManager())
	for _, name := range []string{"/app/one", "/app/two", "/app/three"} {
		tokenRefusalPutParameter(t, srv, name)
	}

	status, page1, code := tokenRefusalSSMCall(t, srv, "GetParametersByPath",
		`{"Path":"/app","MaxResults":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	issued := tokenRefusalSSMNextToken(t, page1)

	status, page2, code := tokenRefusalSSMCall(t, srv, "GetParametersByPath",
		fmt.Sprintf(`{"Path":"/app","MaxResults":1,"NextToken":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.NotEqual(t, page1, page2, "an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalSSMCall(t, srv, "GetParametersByPath",
				fmt.Sprintf(`{"Path":"/app","MaxResults":1,"NextToken":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidNextToken", code, body)
			assert.Contains(t, body, "The specified token isn't valid.", body)
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalSSMServer(t, sealed)
		status, body, code := tokenRefusalSSMCall(t, sealedSrv, "GetParametersByPath",
			`{"Path":"/app","MaxResults":1,"NextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidNextToken", code, body)
	})
}

// --- S3 ListObjectsV2 ---

// tokenRefusalListObjectsV2 pages ListObjectsV2 over a bucket and returns the status and body.
//
// The query string is built with url.Values so that a base64 token containing "+" survives the round
// trip: written into a path by hand it would arrive as a space, and the test would then be asserting
// against a token it had corrupted itself rather than the one S3 issued.
func tokenRefusalListObjectsV2(t *testing.T, srv *emulator.Server, bucket, maxKeys, contToken string) (int, string) {
	t.Helper()
	query := url.Values{"list-type": []string{"2"}}
	if maxKeys != "" {
		query.Set("max-keys", maxKeys)
	}
	if contToken != "" {
		query.Set("continuation-token", contToken)
	}
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?"+query.Encode(), nil, nil)
	return w.Code, w.Body.String()
}

// TestPaginationToken_S3ListObjectsV2RefusesATokenItDidNotIssue is #915 at the one site of the four
// whose cursor is a key rather than an offset, so the refusal is about the token being undecodable
// rather than about the offset it names.
func TestPaginationToken_S3ListObjectsV2RefusesATokenItDidNotIssue(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	sealed := &tokenRefusalSealedState{inner: state}
	srv := newS3TestServerWithState(t, sealed)

	w := s3Request(t, srv, http.MethodPut, "/token-bucket", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		w = s3Request(t, srv, http.MethodPut, "/token-bucket/"+key, []byte("body"), nil)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	}

	status, page1 := tokenRefusalListObjectsV2(t, srv, "token-bucket", "1", "")
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, "a.txt")
	issued := tokenRefusalBetween(t, page1, "NextContinuationToken")
	require.NotEmpty(t, issued)

	status, page2 := tokenRefusalListObjectsV2(t, srv, "token-bucket", "1", issued)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "b.txt", "an issued token must resume after the first page")
	assert.NotContains(t, page2, "a.txt", "an issued token must not re-serve page one")

	// Only the not-base64 cases apply here: the cursor is a key, so base64 of any text is a token
	// substrate could have issued and selects the keys sorting after that text — which is what makes
	// resuming after a deleted object work, the same reading parseQueryMarker records.
	status, body := tokenRefusalListObjectsV2(t, srv, "token-bucket", "1", "!!not-base64!!")
	assert.Equal(t, http.StatusBadRequest, status, body)
	assert.Contains(t, body, "<Code>InvalidArgument</Code>", body)
	assert.Contains(t, body, "The continuation token provided is incorrect.", body)
	assert.NotContains(t, body, "a.txt", "a refused token must not be answered with page one")

	// The refusal precedes the object listing. Only List is sealed: the bucket-existence Get keeps its
	// precedence, since the bucket is the resource the request addresses.
	t.Run("refused before the object listing is read", func(t *testing.T) {
		sealed.sealLists = true
		defer func() { sealed.sealLists = false }()
		status, body := tokenRefusalListObjectsV2(t, srv, "token-bucket", "1", "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Contains(t, body, "<Code>InvalidArgument</Code>", body)
	})
}
