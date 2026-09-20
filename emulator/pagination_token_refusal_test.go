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
// nothing about which of the two refusals wins. SNS ListSubscriptionsByTopic is the same exception for
// the same reason, and because both of its reads are Gets it seals one state key rather than a method.
// CloudWatch Logs' three group-addressed reads joined that shape in #1224 and seal a key too — the
// stream-name index at DescribeLogStreams and FilterLogEvents, the events key at GetLogEvents — each
// with a control call proving the sealed key is reached at all.
// Athena is asserted by counting reads instead of sealing, because its index loader turns a store
// failure into an empty index and so a sealed read is indistinguishable from an empty listing — see
// [tokenRefusalCountingState], which is the honest instrument for that shape rather than a seal that
// would pass either way.
//
// Provenance of the three codes is per operation and is recorded with each helper:
// [cwInvalidNextToken] and [ssmInvalidNextToken] are published, S3's is substrate's reading.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
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
//
// sealGetKey seals a single Get, for the case where both reads are Gets and only the second one is
// below the decode. SNS ListSubscriptionsByTopic is that case: it resolves the topic first so #926's
// NotFound keeps precedence, so sealGets would fail the topic lookup and assert nothing about the
// token. The key is obtained from the plugin's own builder ([emulator.SNSSubscriptionIndexKeyForTest])
// rather than spelled here, so the seal cannot drift from the key actually written.
type tokenRefusalSealedState struct {
	inner      emulator.StateManager
	sealGets   bool
	sealLists  bool
	sealGetKey string
}

// Get reads through to the wrapped manager unless gets are sealed.
func (s *tokenRefusalSealedState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if s.sealGets || (s.sealGetKey != "" && key == s.sealGetKey) {
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

// tokenRefusalCountingState wraps a state manager and counts the reads made in one namespace, for the
// case where sealing cannot distinguish the two orderings.
//
// Athena is that case: its index loader (athenaLoadStringIndex) reports a store failure as an empty
// index, so a sealed read answers 200 with an empty page — indistinguishable from an empty listing,
// which means the seal would pass whichever side of the read the decode sat on. Counting asks the
// question directly: a request that was refused must have read nothing. The paired control, a token the
// operation could have issued, must read something, or a zero count would prove only that the test
// never reached a handler.
//
// The count is scoped to a namespace because it is not the only read a request makes: the server
// authorizes every request first, which reads the principal and its policies out of the iam namespace
// before any plugin is dispatched to. Those reads are not the handler's and counting them would make the
// question unanswerable. The namespace comes from the plugin's own constant
// ([emulator.AthenaStateNamespaceForTest]) rather than being spelled here.
//
// No mutex: every assertion here drives one request at a time through httptest, so the count is read
// after the call that wrote it.
type tokenRefusalCountingState struct {
	inner     emulator.StateManager
	namespace string
	reads     int
}

// Get counts the read when it is in the counted namespace and passes it through.
func (s *tokenRefusalCountingState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if namespace == s.namespace {
		s.reads++
	}
	return s.inner.Get(ctx, namespace, key)
}

// Put writes through to the wrapped manager.
func (s *tokenRefusalCountingState) Put(ctx context.Context, namespace, key string, value []byte) error {
	return s.inner.Put(ctx, namespace, key, value)
}

// Delete removes through to the wrapped manager.
func (s *tokenRefusalCountingState) Delete(ctx context.Context, namespace, key string) error {
	return s.inner.Delete(ctx, namespace, key)
}

// List counts the read when it is in the counted namespace and passes it through.
func (s *tokenRefusalCountingState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if namespace == s.namespace {
		s.reads++
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

// --- #1086: the four services whose pages publish a code of their own ---
//
// #915 fixed three operations; the same decode idiom was found at fifteen more, and only four
// services publish anything a refusal can be answered with. Those four are here. The remaining ten
// sites convert under a code that is substrate's reading of a generic code published on the
// operation's *own* page — never one borrowed from a sibling operation, which is the analogy #671's
// binding scope decision forbids — and SNS's three are the first of them, below.
//
// Each test is the shape the three above established: the issued token round-trips first, so a
// handler that refused everything could not pass; then every unissuable form is refused with the
// code that service publishes; then the refusal is required to arrive from a store sealed against
// reads, which is #887's ordering criterion and the half that a decode-only fix would fail.

// tokenRefusalJSONCall posts one JSON-protocol operation and returns the status, the raw body and the
// bare error code a refusal carries.
//
// The body is a string rather than a Go value so that a token which is not valid base64 reaches the
// handler exactly as written — the same reason [tokenRefusalSSMCall] takes one.
func tokenRefusalJSONCall(t *testing.T, srv *emulator.Server, host, target, body string) (int, string, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	r.Host = host
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", target)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/"+
		"us-east-1/service/aws4_request, SignedHeaders=host, Signature=fake")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s body", target)

	var errShape struct {
		Type string `json:"__type"`
	}
	code := ""
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr == nil {
		code = awsErrorCode(errShape.Type)
	}
	return resp.StatusCode, string(raw), code
}

// tokenRefusalServer builds a one-plugin server over a caller-supplied state manager, which the
// per-service test helpers do not allow and the sealed-store assertion needs.
func tokenRefusalServer(t *testing.T, state emulator.StateManager, plugin emulator.Plugin) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	require.NoError(t, plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// tokenRefusalJSONToken reads a named cursor member out of a JSON response.
func tokenRefusalJSONToken(t *testing.T, body, member string) string {
	t.Helper()
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &out), "decode %s", body)
	raw, found := out[member]
	require.True(t, found, "no %s in %s", member, body)
	var token string
	require.NoError(t, json.Unmarshal(raw, &token), "decode %s in %s", member, body)
	require.NotEmpty(t, token)
	return token
}

// --- KMS ListKeys and ListAliases ---

const tokenRefusalKMSHost = "kms.us-east-1.amazonaws.com"

// tokenRefusalKMSCall posts one KMS operation.
func tokenRefusalKMSCall(t *testing.T, srv *emulator.Server, op, body string) (int, string, string) {
	t.Helper()
	return tokenRefusalJSONCall(t, srv, tokenRefusalKMSHost, "TrentService."+op, body)
}

// TestPaginationToken_KMSListKeysRefusesAMarkerItDidNotIssue is #1086 at the first of the two
// operations whose own Errors section publishes InvalidMarkerException.
//
// The keys are created through CreateKey rather than seeded, per #765: a listing built by writing
// state directly would not prove the offset indexes into what a caller can observe.
func TestPaginationToken_KMSListKeysRefusesAMarkerItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.KMSPlugin{})
	for range 3 {
		status, body, code := tokenRefusalKMSCall(t, srv, "CreateKey", `{}`)
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
	}

	status, page1, code := tokenRefusalKMSCall(t, srv, "ListKeys", `{"Limit":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	issued := tokenRefusalJSONToken(t, page1, "NextMarker")

	status, page2, code := tokenRefusalKMSCall(t, srv, "ListKeys",
		fmt.Sprintf(`{"Limit":1,"Marker":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.NotEqual(t, page1, page2, "an issued marker must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalKMSCall(t, srv, "ListKeys",
				fmt.Sprintf(`{"Limit":1,"Marker":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidMarkerException", code, body)
			assert.NotContains(t, body, `"Keys"`, "a refused marker must not be answered with page one")
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.KMSPlugin{})
		status, body, code := tokenRefusalKMSCall(t, sealedSrv, "ListKeys", `{"Marker":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidMarkerException", code, body)
	})
}

// TestPaginationToken_KMSListAliasesRefusesAMarkerItDidNotIssue is the same refusal at the second
// site, asserted separately because the two decoded their markers with two copies of one block —
// which is how they came to be fixed twice and can come to diverge again.
func TestPaginationToken_KMSListAliasesRefusesAMarkerItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.KMSPlugin{})
	status, keyBody, code := tokenRefusalKMSCall(t, srv, "CreateKey", `{}`)
	require.Empty(t, code, keyBody)
	require.Equal(t, http.StatusOK, status, keyBody)
	var created struct {
		KeyMetadata struct {
			KeyID string `json:"KeyId"`
		} `json:"KeyMetadata"`
	}
	require.NoError(t, json.Unmarshal([]byte(keyBody), &created))
	require.NotEmpty(t, created.KeyMetadata.KeyID)

	for _, alias := range []string{"alias/one", "alias/two", "alias/three"} {
		status, body, code := tokenRefusalKMSCall(t, srv, "CreateAlias",
			fmt.Sprintf(`{"AliasName":%q,"TargetKeyId":%q}`, alias, created.KeyMetadata.KeyID))
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
	}

	status, page1, code := tokenRefusalKMSCall(t, srv, "ListAliases", `{"Limit":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	issued := tokenRefusalJSONToken(t, page1, "NextMarker")

	status, page2, code := tokenRefusalKMSCall(t, srv, "ListAliases",
		fmt.Sprintf(`{"Limit":1,"Marker":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.NotEqual(t, page1, page2, "an issued marker must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalKMSCall(t, srv, "ListAliases",
				fmt.Sprintf(`{"Limit":1,"Marker":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidMarkerException", code, body)
			assert.NotContains(t, body, "alias/one", "a refused marker must not be answered with page one")
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.KMSPlugin{})
		status, body, code := tokenRefusalKMSCall(t, sealedSrv, "ListAliases", `{"Marker":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidMarkerException", code, body)
	})
}

// --- Secrets Manager ListSecrets ---

// TestPaginationToken_SecretsManagerListSecretsRefusesATokenItDidNotIssue is #1086 at the one site
// whose page publishes a pagination code *separately* from its parameter code, which is why the
// refusal is InvalidNextTokenException and not InvalidParameterException.
func TestPaginationToken_SecretsManagerListSecretsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.SecretsManagerPlugin{})
	call := func(op, body string) (int, string, string) {
		t.Helper()
		return tokenRefusalJSONCall(t, srv, "secretsmanager.us-east-1.amazonaws.com", "secretsmanager."+op, body)
	}
	for _, name := range []string{"one", "two", "three"} {
		status, body, code := call("CreateSecret", fmt.Sprintf(`{"Name":%q,"SecretString":"s"}`, name))
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
	}

	status, page1, code := call("ListSecrets", `{"MaxResults":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	issued := tokenRefusalJSONToken(t, page1, "NextToken")

	status, page2, code := call("ListSecrets", fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.NotEqual(t, page1, page2, "an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := call("ListSecrets", fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidNextTokenException", code, body)
			assert.NotContains(t, body, "SecretList", "a refused token must not be answered with page one")
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.SecretsManagerPlugin{})
		status, body, code := tokenRefusalJSONCall(t, sealedSrv, "secretsmanager.us-east-1.amazonaws.com",
			"secretsmanager.ListSecrets", `{"NextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidNextTokenException", code, body)
	})
}

// --- EventBridge ListRules ---

// TestPaginationToken_EventBridgeListRulesRefusesATokenItDidNotIssue is #1086 at the site whose code
// comes from prose rather than from an Errors section.
//
// API_ListRules names InvalidToken and its 400 twice, in the NextToken member's own description —
// "Using an expired pagination token results in an HTTP 400 InvalidToken error" — and nowhere in its
// Errors list, which is why #950's sweep of this service missed it and settled for the common-errors
// fallback. The condition AWS names is an expired token, which substrate has none of; see
// [ebInvalidToken] for why an unissuable one is the same observation for a caller.
func TestPaginationToken_EventBridgeListRulesRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.EventBridgePlugin{})
	call := func(srv *emulator.Server, op, body string) (int, string, string) {
		t.Helper()
		return tokenRefusalJSONCall(t, srv, "events.us-east-1.amazonaws.com", "AmazonEventBridge."+op, body)
	}
	for _, name := range []string{"rule-a", "rule-b", "rule-c"} {
		status, body, code := call(srv, "PutRule",
			fmt.Sprintf(`{"Name":%q,"ScheduleExpression":"rate(5 minutes)"}`, name))
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
	}

	status, page1, code := call(srv, "ListRules", `{"Limit":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	issued := tokenRefusalJSONToken(t, page1, "NextToken")

	status, page2, code := call(srv, "ListRules", fmt.Sprintf(`{"Limit":1,"NextToken":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "rule-b", "an issued token must resume after the first page")
	assert.NotContains(t, page2, "rule-a", "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := call(srv, "ListRules", fmt.Sprintf(`{"Limit":1,"NextToken":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidToken", code, body)
			assert.NotContains(t, body, "rule-a", "a refused token must not be answered with page one")
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.EventBridgePlugin{})
		status, body, code := call(sealedSrv, "ListRules", `{"NextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidToken", code, body)
	})
}

// --- SNS ListTopics, ListSubscriptions and ListSubscriptionsByTopic ---

// tokenRefusalSNSHost is the endpoint all three SNS listings are called through.
const tokenRefusalSNSHost = "sns.us-east-1.amazonaws.com"

// tokenRefusalSNSRegion is the Region that host resolves to, needed to rebuild the state key the
// ListSubscriptionsByTopic ordering assertion seals.
const tokenRefusalSNSRegion = "us-east-1"

// tokenRefusalSNSCall posts one SNS Query-protocol action and returns the status, the raw body and the
// bare error code a refusal carries.
//
// The parameters go through url.Values so that a token which is not valid base64 is encoded for the
// wire and arrives at the handler exactly as written, rather than being rejected by the form parser.
func tokenRefusalSNSCall(t *testing.T, srv *emulator.Server, params map[string]string) (int, string, string) {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = tokenRefusalSNSHost
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s body", params["Action"])

	var errDoc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Code    string   `xml:"Error>Code"`
	}
	if unmarshalErr := xml.Unmarshal(raw, &errDoc); unmarshalErr == nil && errDoc.Code != "" {
		return resp.StatusCode, string(raw), errDoc.Code
	}
	return resp.StatusCode, string(raw), ""
}

// tokenRefusalSNSCreateTopic creates one topic and returns its ARN.
func tokenRefusalSNSCreateTopic(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	status, body, code := tokenRefusalSNSCall(t, srv, map[string]string{"Action": "CreateTopic", "Name": name})
	require.Empty(t, code, body)
	require.Equal(t, http.StatusOK, status, body)
	return tokenRefusalBetween(t, body, "TopicArn")
}

// tokenRefusalSNSAccountOf reads the account segment out of an SNS topic ARN.
//
// The account comes from an ARN the server minted rather than from a constant, because the ordering
// assertion has to seal the key the running plugin writes and a guessed account would seal a key
// nothing reads — which would make that assertion pass for the wrong reason.
func tokenRefusalSNSAccountOf(t *testing.T, topicARN string) string {
	t.Helper()
	parts := strings.Split(topicARN, ":")
	require.Len(t, parts, 6, "not a topic ARN: %s", topicARN)
	return parts[4]
}

// TestPaginationToken_SNSListTopicsRefusesATokenItDidNotIssue is #1086 at the first of SNS's three
// offset paginators.
//
// API_ListTopics publishes InvalidParameter/400 in its own Errors section and says nothing at all about
// the token beyond "Token returned by the previous ListTopics request", so the code is the page's and
// the condition is substrate's reading of it — see snsInvalidPaginationToken. The listing is 101 topics
// because the page size is a constant 100 that no request parameter can lower: none of the three
// operations publishes one.
func TestPaginationToken_SNSListTopicsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.SNSPlugin{})
	for i := range 101 {
		tokenRefusalSNSCreateTopic(t, srv, fmt.Sprintf("topic-%03d", i))
	}

	list := func(srv *emulator.Server, token string) (int, string, string) {
		t.Helper()
		params := map[string]string{"Action": "ListTopics"}
		if token != "" {
			params["NextToken"] = token
		}
		return tokenRefusalSNSCall(t, srv, params)
	}

	// The token the operation issues resumes the walk. Asserted first, so a refusal that swallowed every
	// token could not pass the rest of this test.
	status, page1, code := list(srv, "")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, ":topic-000")
	assert.NotContains(t, page1, ":topic-100", "the 101st topic belongs to page two")
	issued := tokenRefusalBetween(t, page1, "NextToken")
	require.NotEmpty(t, issued)

	status, page2, code := list(srv, issued)
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, ":topic-100", "an issued token must resume after the first page")
	assert.NotContains(t, page2, ":topic-000", "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := list(srv, tc.token)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameter", code, body)
			assert.Contains(t, body, "NextToken is not a token returned by a previous ListTopics request", body)
			assert.NotContains(t, body, ":topic-000", "a refused token must not be answered with page one")
		})
	}

	// The refusal precedes the read of the topic index: sealed against reads, the handler still refuses
	// rather than reporting the store's failure as a 500.
	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.SNSPlugin{})
		status, body, code := list(sealedSrv, "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameter", code, body)
	})
}

// tokenRefusalSNSSubscribeMany creates one topic with n subscriptions and returns the topic ARN and the
// ARN of the last subscription, which is the one page two must carry.
//
// The subscriptions are created through Subscribe rather than seeded, per #765: an index written
// directly would not prove the offset indexes into what a caller can observe.
func tokenRefusalSNSSubscribeMany(t *testing.T, srv *emulator.Server, topicName string, n int) (string, string) {
	t.Helper()
	topicARN := tokenRefusalSNSCreateTopic(t, srv, topicName)
	last := ""
	for i := range n {
		status, body, code := tokenRefusalSNSCall(t, srv, map[string]string{
			"Action":   "Subscribe",
			"TopicArn": topicARN,
			"Protocol": "email",
			"Endpoint": fmt.Sprintf("sub-%03d@example.com", i),
		})
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
		last = tokenRefusalBetween(t, body, "SubscriptionArn")
	}
	return topicARN, last
}

// TestPaginationToken_SNSListSubscriptionsRefusesATokenItDidNotIssue is #1086 at the shared subscription
// paginator, reached through the account-wide listing.
//
// The order here is the subscription index's append order rather than a sort, which is what
// pageByOffsetToken's offset relies on; the round-trip half asserts it by requiring the 101st
// subscription — and only it — on page two.
func TestPaginationToken_SNSListSubscriptionsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.SNSPlugin{})
	_, lastSubARN := tokenRefusalSNSSubscribeMany(t, srv, "walked", 101)

	list := func(srv *emulator.Server, token string) (int, string, string) {
		t.Helper()
		params := map[string]string{"Action": "ListSubscriptions"}
		if token != "" {
			params["NextToken"] = token
		}
		return tokenRefusalSNSCall(t, srv, params)
	}

	status, page1, code := list(srv, "")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, "sub-000@example.com")
	assert.NotContains(t, page1, lastSubARN, "the 101st subscription belongs to page two")
	issued := tokenRefusalBetween(t, page1, "NextToken")
	require.NotEmpty(t, issued)

	status, page2, code := list(srv, issued)
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, lastSubARN, "an issued token must resume after the first page")
	assert.NotContains(t, page2, "sub-000@example.com", "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := list(srv, tc.token)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameter", code, body)
			assert.Contains(t, body,
				"NextToken is not a token returned by a previous ListSubscriptions request", body)
			assert.NotContains(t, body, "sub-000@example.com", "a refused token must not be answered with page one")
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.SNSPlugin{})
		status, body, code := list(sealedSrv, "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameter", code, body)
	})
}

// TestPaginationToken_SNSListSubscriptionsByTopicRefusesATokenItDidNotIssue is #1086 at the one of the
// three whose token is not checked first, and the assertion that the exception is the intended one.
//
// #926 gave this operation NotFound/404 for a topic that does not exist, and the topic is the resource
// the request addresses, so that refusal keeps its precedence — the same reading S3's ListObjectsV2
// records for its bucket. The token is still decoded before the subscription index is read, which is the
// property the sealed-key subtest below pins: sealing every Get would fail the topic lookup instead and
// assert nothing about the token.
func TestPaginationToken_SNSListSubscriptionsByTopicRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.SNSPlugin{})
	topicARN, lastSubARN := tokenRefusalSNSSubscribeMany(t, srv, "by-topic", 101)

	list := func(srv *emulator.Server, arn, token string) (int, string, string) {
		t.Helper()
		params := map[string]string{"Action": "ListSubscriptionsByTopic", "TopicArn": arn}
		if token != "" {
			params["NextToken"] = token
		}
		return tokenRefusalSNSCall(t, srv, params)
	}

	status, page1, code := list(srv, topicARN, "")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, "sub-000@example.com")
	assert.NotContains(t, page1, lastSubARN, "the 101st subscription belongs to page two")
	issued := tokenRefusalBetween(t, page1, "NextToken")
	require.NotEmpty(t, issued)

	status, page2, code := list(srv, topicARN, issued)
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, lastSubARN, "an issued token must resume after the first page")
	assert.NotContains(t, page2, "sub-000@example.com", "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := list(srv, topicARN, tc.token)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameter", code, body)
			assert.Contains(t, body,
				"NextToken is not a token returned by a previous ListSubscriptionsByTopic request", body)
			assert.NotContains(t, body, "sub-000@example.com", "a refused token must not be answered with page one")
		})
	}

	// A topic that does not exist is still NotFound, whatever the token says: the two refusals are
	// ordered, and this is the half that fixes which.
	t.Run("an absent topic outranks a bad token", func(t *testing.T) {
		absent := strings.Replace(topicARN, ":by-topic", ":never-created", 1)
		require.NotEqual(t, topicARN, absent)
		status, body, code := list(srv, absent, "!!not-base64!!")
		assert.Equal(t, http.StatusNotFound, status, body)
		assert.Equal(t, "NotFound", code, body)
	})

	// The decode sits above the subscription-index read. Sealing that one key leaves the topic lookup
	// working, so a handler that decoded after it would report the seal as a 500 instead of refusing.
	t.Run("refused before the subscription index is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager()}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.SNSPlugin{})
		arn := tokenRefusalSNSCreateTopic(t, sealedSrv, "sealed-index")
		sealed.sealGetKey = emulator.SNSSubscriptionIndexKeyForTest(
			tokenRefusalSNSAccountOf(t, arn), tokenRefusalSNSRegion, "sealed-index")

		// The control: with a token this operation could have issued, the sealed read is reached and
		// reported. Without it the refusal below could pass because the key is never read at all.
		status, body, _ := list(sealedSrv, arn, base64.StdEncoding.EncodeToString([]byte("0")))
		require.NotEqual(t, http.StatusBadRequest, status, body)

		status, body, code := list(sealedSrv, arn, "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameter", code, body)
	})
}

// --- Athena ListQueryExecutions and ListWorkGroups ---

// tokenRefusalAthenaHost is the endpoint both Athena listings are called through.
const tokenRefusalAthenaHost = "athena.us-east-1.amazonaws.com"

// tokenRefusalAthenaCall posts one Athena operation.
func tokenRefusalAthenaCall(t *testing.T, srv *emulator.Server, op, body string) (int, string, string) {
	t.Helper()
	return tokenRefusalJSONCall(t, srv, tokenRefusalAthenaHost, "AmazonAthena."+op, body)
}

// tokenRefusalAthenaStartQuery starts one query execution and returns its ID.
//
// The queries are started through StartQueryExecution rather than seeded, per #765: an index written
// directly would not prove the offset indexes into what a caller can observe.
func tokenRefusalAthenaStartQuery(t *testing.T, srv *emulator.Server, sql string) string {
	t.Helper()
	status, body, code := tokenRefusalAthenaCall(t, srv, "StartQueryExecution",
		fmt.Sprintf(`{"QueryString":%q}`, sql))
	require.Empty(t, code, body)
	require.Equal(t, http.StatusOK, status, body)
	var out struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	require.NotEmpty(t, out.QueryExecutionID)
	return out.QueryExecutionID
}

// TestPaginationToken_AthenaListQueryExecutionsRefusesATokenItDidNotIssue is #1086 at the first of
// Athena's two offset paginators.
//
// Both pages publish InvalidRequestException/400 in their own Errors sections and describe NextToken as
// "A token generated by the Athena service", which is the closest any site in this class comes to
// publishing the refusal — see athenaInvalidPaginationToken. Two records and MaxResults=1 are enough to
// truncate a page, because unlike SNS both operations publish a MaxResults a caller can lower.
func TestPaginationToken_AthenaListQueryExecutionsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.AthenaPlugin{})
	first := tokenRefusalAthenaStartQuery(t, srv, "SELECT 1")
	second := tokenRefusalAthenaStartQuery(t, srv, "SELECT 2")

	list := func(srv *emulator.Server, token string) (int, string, string) {
		t.Helper()
		if token == "" {
			return tokenRefusalAthenaCall(t, srv, "ListQueryExecutions", `{"MaxResults":1}`)
		}
		return tokenRefusalAthenaCall(t, srv, "ListQueryExecutions",
			fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, token))
	}

	// The token the operation issues resumes the walk. Asserted first, so a refusal that swallowed every
	// token could not pass the rest of this test.
	status, page1, code := list(srv, "")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, first)
	assert.NotContains(t, page1, second, "the second query belongs to page two")
	issued := tokenRefusalJSONToken(t, page1, "NextToken")

	status, page2, code := list(srv, issued)
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, second, "an issued token must resume after the first page")
	assert.NotContains(t, page2, first, "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := list(srv, tc.token)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidRequestException", code, body)
			assert.Contains(t, body,
				"NextToken is not a token returned by a previous ListQueryExecutions request", body)
			assert.NotContains(t, body, first, "a refused token must not be answered with page one")
		})
	}

	// #887's ordering criterion, counted rather than sealed: see [tokenRefusalCountingState] for why a
	// sealed store cannot tell Athena's two orderings apart.
	t.Run("refused before any state is read", func(t *testing.T) {
		counted := &tokenRefusalCountingState{
			inner:     emulator.NewMemoryStateManager(),
			namespace: emulator.AthenaStateNamespaceForTest(),
		}
		countedSrv := tokenRefusalServer(t, counted, &emulator.AthenaPlugin{})

		before := counted.reads
		status, body, code := list(countedSrv, "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidRequestException", code, body)
		assert.Equal(t, before, counted.reads, "a refused token must be refused before any read")

		// The control: a token this operation could have issued reaches the index and reads it. Without it
		// the assertion above could pass because no request ever reaches the handler.
		before = counted.reads
		status, body, _ = list(countedSrv, base64.StdEncoding.EncodeToString([]byte("0")))
		require.Equal(t, http.StatusOK, status, body)
		assert.Greater(t, counted.reads, before, "an accepted token must reach the index")
	})
}

// TestPaginationToken_AthenaListWorkGroupsRefusesATokenItDidNotIssue is the same refusal at the second
// site, asserted separately because the two decoded their tokens with two copies of one block — which is
// how they came to be fixed twice and can come to diverge again.
//
// It also pins the portability the per-operation message exists to make detectable: both listings encode
// an offset identically, so each one's token is issuable under the other and only the operation name in
// the message distinguishes them.
//
// The walk starts at the `primary` workgroup because #1222 prepended it: it exists before any workgroup
// a caller creates, so it holds offset 0 and the two created here follow it. The page-by-page assertions
// below are about the offset advancing and not re-serving, which is unchanged by there being one more
// entry to advance over.
func TestPaginationToken_AthenaListWorkGroupsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.AthenaPlugin{})
	for _, name := range []string{"wg-one", "wg-two"} {
		status, body, code := tokenRefusalAthenaCall(t, srv, "CreateWorkGroup",
			fmt.Sprintf(`{"Name":%q}`, name))
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
	}

	list := func(srv *emulator.Server, token string) (int, string, string) {
		t.Helper()
		if token == "" {
			return tokenRefusalAthenaCall(t, srv, "ListWorkGroups", `{"MaxResults":1}`)
		}
		return tokenRefusalAthenaCall(t, srv, "ListWorkGroups",
			fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, token))
	}

	status, page1, code := list(srv, "")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	assert.Contains(t, page1, "primary", "the workgroup that exists before either created one")
	assert.NotContains(t, page1, "wg-one", "the first created workgroup belongs to page two")
	issued := tokenRefusalJSONToken(t, page1, "NextToken")

	status, page2, code := list(srv, issued)
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "wg-one", "an issued token must resume after the first page")
	assert.NotContains(t, page2, "primary", "an issued token must not re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := list(srv, tc.token)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidRequestException", code, body)
			assert.Contains(t, body,
				"NextToken is not a token returned by a previous ListWorkGroups request", body)
			assert.NotContains(t, body, "wg-one", "a refused token must not be answered with page one")
		})
	}

	// The other listing's token is issuable here, so it is accepted — the message is what tells a caller
	// which listing a token belongs to, and this records that the refusal does not claim otherwise.
	t.Run("a token from the other listing is issuable and is not refused", func(t *testing.T) {
		tokenRefusalAthenaStartQuery(t, srv, "SELECT 1")
		tokenRefusalAthenaStartQuery(t, srv, "SELECT 2")
		status, queries, code := tokenRefusalAthenaCall(t, srv, "ListQueryExecutions", `{"MaxResults":1}`)
		require.Empty(t, code, queries)
		require.Equal(t, http.StatusOK, status, queries)

		status, body, code := list(srv, tokenRefusalJSONToken(t, queries, "NextToken"))
		assert.Equal(t, http.StatusOK, status, body)
		assert.Empty(t, code, body)
	})

	t.Run("refused before any state is read", func(t *testing.T) {
		counted := &tokenRefusalCountingState{
			inner:     emulator.NewMemoryStateManager(),
			namespace: emulator.AthenaStateNamespaceForTest(),
		}
		countedSrv := tokenRefusalServer(t, counted, &emulator.AthenaPlugin{})

		before := counted.reads
		status, body, code := list(countedSrv, "!!not-base64!!")
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidRequestException", code, body)
		assert.Equal(t, before, counted.reads, "a refused token must be refused before any read")

		before = counted.reads
		status, body, _ = list(countedSrv, base64.StdEncoding.EncodeToString([]byte("0")))
		require.Equal(t, http.StatusOK, status, body)
		assert.Greater(t, counted.reads, before, "an accepted token must reach the index")
	})
}

// --- CloudWatch Logs DescribeLogGroups, DescribeLogStreams, GetLogEvents and FilterLogEvents ---

const tokenRefusalCWLogsHost = "logs.us-east-1.amazonaws.com"

// tokenRefusalCWLogsCall posts one CloudWatch Logs operation.
func tokenRefusalCWLogsCall(t *testing.T, srv *emulator.Server, op, body string) (int, string, string) {
	t.Helper()
	return tokenRefusalJSONCall(t, srv, tokenRefusalCWLogsHost, "Logs_20140328."+op, body)
}

// tokenRefusalCWLogsGroup creates one log group through CreateLogGroup.
func tokenRefusalCWLogsGroup(t *testing.T, srv *emulator.Server, name string) {
	t.Helper()
	status, body, code := tokenRefusalCWLogsCall(t, srv, "CreateLogGroup",
		fmt.Sprintf(`{"logGroupName":%q}`, name))
	require.Empty(t, code, body)
	require.Equal(t, http.StatusOK, status, body)
}

// tokenRefusalCWLogsStream creates one log stream through CreateLogStream.
func tokenRefusalCWLogsStream(t *testing.T, srv *emulator.Server, group, stream string) {
	t.Helper()
	status, body, code := tokenRefusalCWLogsCall(t, srv, "CreateLogStream",
		fmt.Sprintf(`{"logGroupName":%q,"logStreamName":%q}`, group, stream))
	require.Empty(t, code, body)
	require.Equal(t, http.StatusOK, status, body)
}

// tokenRefusalCWLogsPutEvents writes n events to one stream, each with a distinct message and
// timestamp so that a page boundary is visible in the body rather than only in the token.
func tokenRefusalCWLogsPutEvents(t *testing.T, srv *emulator.Server, group, stream string, n int) {
	t.Helper()
	for i := range n {
		status, body, code := tokenRefusalCWLogsCall(t, srv, "PutLogEvents",
			fmt.Sprintf(`{"logGroupName":%q,"logStreamName":%q,"logEvents":[{"timestamp":%d,"message":%q}]}`,
				group, stream, 1700000000000+int64(i)*1000, fmt.Sprintf("token-refusal-event-%d", i)))
		require.Empty(t, code, body)
		require.Equal(t, http.StatusOK, status, body)
	}
}

// tokenRefusalCWLogsRegion is the region the host above resolves to, named so the sealed-key helpers
// below cannot spell a different one than the calls they seal.
const tokenRefusalCWLogsRegion = "us-east-1"

// tokenRefusalCWLogsAccountOf reads the account segment out of a log group's ARN.
//
// The account comes from an ARN the server minted rather than from a constant, for the reason
// [tokenRefusalSNSAccountOf] gives: the ordering assertions below have to seal the key the running
// plugin writes, and a guessed account would seal a key nothing reads — which would make the assertion
// pass for the wrong reason.
func tokenRefusalCWLogsAccountOf(t *testing.T, srv *emulator.Server, group string) string {
	t.Helper()
	status, body, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogGroups",
		fmt.Sprintf(`{"logGroupNamePrefix":%q}`, group))
	require.Empty(t, code, body)
	require.Equal(t, http.StatusOK, status, body)
	var out struct {
		LogGroups []struct {
			ARN string `json:"arn"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	require.Len(t, out.LogGroups, 1, body)
	parts := strings.Split(out.LogGroups[0].ARN, ":")
	require.Greater(t, len(parts), 5, "not a log group ARN: %s", out.LogGroups[0].ARN)
	return parts[4]
}

// TestPaginationToken_CWLogsDescribeLogGroupsRefusesATokenItDidNotIssue is #1086 at the first of
// CloudWatch Logs' four sites, the only one of them whose page publishes no ResourceNotFoundException
// and so the only one with nothing ahead of the decode at all.
//
// The groups are created through CreateLogGroup rather than seeded, per #765: a listing built by
// writing state directly would not prove the offset indexes into what a caller can observe.
func TestPaginationToken_CWLogsDescribeLogGroupsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.CloudWatchLogsPlugin{})
	for _, name := range []string{"trg-groups-a", "trg-groups-b", "trg-groups-c"} {
		tokenRefusalCWLogsGroup(t, srv, name)
	}

	status, page1, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogGroups", `{"limit":1}`)
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	require.Contains(t, page1, "trg-groups-a")
	issued := tokenRefusalJSONToken(t, page1, "nextToken")

	status, page2, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogGroups",
		fmt.Sprintf(`{"limit":1,"nextToken":%q}`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "trg-groups-b", "an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogGroups",
				fmt.Sprintf(`{"limit":1,"nextToken":%q}`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameterException", code, body)
			assert.Contains(t, body, "DescribeLogGroups", "the message must name the operation")
			assert.NotContains(t, body, "trg-groups-a", "a refused token must not be answered with page one")
		})
	}

	t.Run("refused before any state is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.CloudWatchLogsPlugin{})
		status, body, code := tokenRefusalCWLogsCall(t, sealedSrv, "DescribeLogGroups",
			`{"nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameterException", code, body)
	})
}

// TestPaginationToken_CWLogsDescribeLogStreamsRefusesATokenItDidNotIssue is the same refusal at the
// second site, asserted separately because the four decoded their tokens with four copies of one
// block — which is how one fix could have left three of them answering page one.
//
// It is also the first of the three sites that require a member, so the refusal sits below that one:
// a request with neither logGroupName nor a valid token is still told about logGroupName.
func TestPaginationToken_CWLogsDescribeLogStreamsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.CloudWatchLogsPlugin{})
	const group = "trg-streams-group"
	tokenRefusalCWLogsGroup(t, srv, group)
	for _, name := range []string{"trg-stream-a", "trg-stream-b", "trg-stream-c"} {
		tokenRefusalCWLogsStream(t, srv, group, name)
	}

	status, page1, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogStreams",
		fmt.Sprintf(`{"logGroupName":%q,"limit":1}`, group))
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	require.Contains(t, page1, "trg-stream-a")
	issued := tokenRefusalJSONToken(t, page1, "nextToken")

	status, page2, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogStreams",
		fmt.Sprintf(`{"logGroupName":%q,"limit":1,"nextToken":%q}`, group, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "trg-stream-b", "an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogStreams",
				fmt.Sprintf(`{"logGroupName":%q,"limit":1,"nextToken":%q}`, group, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameterException", code, body)
			assert.Contains(t, body, "DescribeLogStreams", "the message must name the operation")
			assert.NotContains(t, body, "trg-stream-a", "a refused token must not be answered with page one")
		})
	}

	t.Run("the required member keeps precedence over the token", func(t *testing.T) {
		// Both refusals carry InvalidParameterException, so the message is the only thing that says
		// which one answered — which is why this is asserted rather than left to the reader.
		status, body, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogStreams",
			`{"nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameterException", code, body)
		assert.Contains(t, body, "logGroupName is required", body)
	})

	t.Run("an absent log group outranks a bad token", func(t *testing.T) {
		// #1224's half of the ordering: the group is the resource the request addresses, so its
		// not-found keeps precedence, the same reading SNS ListSubscriptionsByTopic records for its
		// topic. Both refusals are 400, so the code is the only thing that says which one answered.
		status, body, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogStreams",
			`{"logGroupName":"trg-streams-never-created","nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "ResourceNotFoundException", code, body)
	})

	// The decode sits above the stream-index read. Sealing that one key leaves the group lookup
	// working, so a handler that decoded after it would report the seal as a 500 instead of refusing —
	// which sealing every Get can no longer show, since #1224 put the group lookup ahead of the token.
	t.Run("refused before the stream index is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager()}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.CloudWatchLogsPlugin{})
		tokenRefusalCWLogsGroup(t, sealedSrv, "trg-sealed-streams")
		sealed.sealGetKey = emulator.CWLogStreamNamesKeyForTest(
			tokenRefusalCWLogsAccountOf(t, sealedSrv, "trg-sealed-streams"),
			tokenRefusalCWLogsRegion, "trg-sealed-streams")

		// The control: with a token this operation could have issued, the sealed read is reached and
		// reported. Without it the refusal below could pass because the key is never read at all.
		status, body, _ := tokenRefusalCWLogsCall(t, sealedSrv, "DescribeLogStreams",
			fmt.Sprintf(`{"logGroupName":"trg-sealed-streams","nextToken":%q}`,
				base64.StdEncoding.EncodeToString([]byte("0"))))
		require.NotEqual(t, http.StatusBadRequest, status, body)

		status, body, code := tokenRefusalCWLogsCall(t, sealedSrv, "DescribeLogStreams",
			`{"logGroupName":"trg-sealed-streams","nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameterException", code, body)
	})
}

// TestPaginationToken_CWLogsGetLogEventsRefusesATokenItDidNotIssue is the same refusal at the third
// site, which is the one whose published cursor is a *pair* of directional tokens.
//
// The pair itself is asserted in cloudwatchlogs_event_tokens_test.go (#1223); this test is about the
// refusal, so it walks forward with an explicit startFromHead and only needs a token substrate issued
// to be accepted and a token it did not to be refused. The bad-token table is shared with the other
// three sites, which is the point: those tokens carry no direction prefix, so they are exactly the
// foreign tokens this operation stopped sharing a wire shape with.
func TestPaginationToken_CWLogsGetLogEventsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.CloudWatchLogsPlugin{})
	const group, stream = "trg-events-group", "trg-events-stream"
	tokenRefusalCWLogsGroup(t, srv, group)
	tokenRefusalCWLogsStream(t, srv, group, stream)
	tokenRefusalCWLogsPutEvents(t, srv, group, stream, 3)

	get := func(extra string) (int, string, string) {
		return tokenRefusalCWLogsCall(t, srv, "GetLogEvents",
			fmt.Sprintf(`{"logGroupName":%q,"logStreamName":%q,"limit":1,"startFromHead":true%s}`,
				group, stream, extra))
	}

	status, page1, code := get("")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	require.Contains(t, page1, "token-refusal-event-0")
	issued := tokenRefusalJSONToken(t, page1, "nextForwardToken")

	status, page2, code := get(fmt.Sprintf(`,"nextToken":%q`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "token-refusal-event-1",
		"an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := get(fmt.Sprintf(`,"nextToken":%q`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameterException", code, body)
			assert.Contains(t, body, "GetLogEvents", "the message must name the operation")
			assert.NotContains(t, body, "token-refusal-event-0",
				"a refused token must not be answered with page one")
		})
	}

	t.Run("an absent resource outranks a bad token", func(t *testing.T) {
		// This is the site that addresses two resources, so both are asserted, and the group first:
		// a caller told the stream is missing would create it and be refused again (#1224).
		status, body, code := tokenRefusalCWLogsCall(t, srv, "GetLogEvents",
			fmt.Sprintf(`{"logGroupName":%q,"logStreamName":"trg-never-created","nextToken":"!!not-base64!!"}`, group))
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "ResourceNotFoundException", code, body)
		assert.Contains(t, body, "log stream", body)

		status, body, code = tokenRefusalCWLogsCall(t, srv, "GetLogEvents",
			fmt.Sprintf(`{"logGroupName":"trg-never-created","logStreamName":%q,"nextToken":"!!not-base64!!"}`, stream))
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "ResourceNotFoundException", code, body)
		assert.Contains(t, body, "log group", body)
	})

	// The decode sits above the events read, which at this site is a key rather than an index. Sealing
	// that one key leaves both resource lookups working, so a handler that decoded after them would
	// report the seal as a 500 instead of refusing.
	t.Run("refused before the events key is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager()}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.CloudWatchLogsPlugin{})
		tokenRefusalCWLogsGroup(t, sealedSrv, "trg-sealed-events")
		tokenRefusalCWLogsStream(t, sealedSrv, "trg-sealed-events", "trg-sealed-stream")
		sealed.sealGetKey = emulator.CWLogEventsKeyForTest(
			tokenRefusalCWLogsAccountOf(t, sealedSrv, "trg-sealed-events"),
			tokenRefusalCWLogsRegion, "trg-sealed-events", "trg-sealed-stream")

		// The control, as above: the sealed key has to be reachable for the refusal to mean anything. The
		// token carries the forward prefix this operation's tokens carry (#1223), so the bare offset the
		// other three sites use would be refused here for the wrong reason.
		status, body, _ := tokenRefusalCWLogsCall(t, sealedSrv, "GetLogEvents",
			fmt.Sprintf(`{"logGroupName":"trg-sealed-events","logStreamName":"trg-sealed-stream","nextToken":"f/%s"}`,
				base64.StdEncoding.EncodeToString([]byte("0"))))
		require.NotEqual(t, http.StatusBadRequest, status, body)

		status, body, code := tokenRefusalCWLogsCall(t, sealedSrv, "GetLogEvents",
			`{"logGroupName":"trg-sealed-events","logStreamName":"trg-sealed-stream","nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameterException", code, body)
	})
}

// TestPaginationToken_CWLogsFilterLogEventsRefusesATokenItDidNotIssue is the same refusal at the
// fourth site, the one that concatenates every stream in the group before paging.
//
// The token is not passed logStreamNames, so the walk goes through the stream index — which is both
// the path a caller takes by default and the one the sealed-key assertion can see, since the per-stream
// reads below it swallow their errors.
func TestPaginationToken_CWLogsFilterLogEventsRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.CloudWatchLogsPlugin{})
	const group = "trg-filter-group"
	tokenRefusalCWLogsGroup(t, srv, group)
	tokenRefusalCWLogsStream(t, srv, group, "trg-filter-stream")
	tokenRefusalCWLogsPutEvents(t, srv, group, "trg-filter-stream", 3)

	filter := func(extra string) (int, string, string) {
		return tokenRefusalCWLogsCall(t, srv, "FilterLogEvents",
			fmt.Sprintf(`{"logGroupName":%q,"limit":1%s}`, group, extra))
	}

	status, page1, code := filter("")
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	require.Contains(t, page1, "token-refusal-event-0")
	issued := tokenRefusalJSONToken(t, page1, "nextToken")

	status, page2, code := filter(fmt.Sprintf(`,"nextToken":%q`, issued))
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "token-refusal-event-1",
		"an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := filter(fmt.Sprintf(`,"nextToken":%q`, tc.token))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "InvalidParameterException", code, body)
			assert.Contains(t, body, "FilterLogEvents", "the message must name the operation")
			assert.NotContains(t, body, "token-refusal-event-0",
				"a refused token must not be answered with page one")
		})
	}

	t.Run("a token from another Logs listing is issuable and is not refused", func(t *testing.T) {
		// All four encode an offset the same way, so DescribeLogGroups' token decodes cleanly here. The
		// refusal is about issuability, not about provenance, and this pins that it does not over-claim:
		// what the message says is that no *previous call* returned the token, which a test cannot
		// disprove and the implementation does not try to.
		tokenRefusalCWLogsGroup(t, srv, "trg-filter-second-group")
		status, groups, code := tokenRefusalCWLogsCall(t, srv, "DescribeLogGroups", `{"limit":1}`)
		require.Empty(t, code, groups)
		require.Equal(t, http.StatusOK, status, groups)

		status, body, code := filter(fmt.Sprintf(`,"nextToken":%q`, tokenRefusalJSONToken(t, groups, "nextToken")))
		assert.Equal(t, http.StatusOK, status, body)
		assert.Empty(t, code, body)
	})

	t.Run("an absent log group outranks a bad token", func(t *testing.T) {
		// Only the group: logStreamNames is a filter on the search rather than the resource the request
		// addresses, so a name in it with no stream behind it is still not refused (#1224).
		status, body, code := tokenRefusalCWLogsCall(t, srv, "FilterLogEvents",
			`{"logGroupName":"trg-filter-never-created","nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "ResourceNotFoundException", code, body)

		status, body, code = tokenRefusalCWLogsCall(t, srv, "FilterLogEvents",
			fmt.Sprintf(`{"logGroupName":%q,"logStreamNames":["trg-no-such-stream"]}`, group))
		assert.Equal(t, http.StatusOK, status, body)
		assert.Empty(t, code, body)
	})

	// The decode sits above the stream-index read, as at DescribeLogStreams. Sealing that one key leaves
	// the group lookup working, so a handler that decoded after it would report the seal as a 500.
	t.Run("refused before the stream index is read", func(t *testing.T) {
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager()}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.CloudWatchLogsPlugin{})
		tokenRefusalCWLogsGroup(t, sealedSrv, "trg-sealed-filter")
		sealed.sealGetKey = emulator.CWLogStreamNamesKeyForTest(
			tokenRefusalCWLogsAccountOf(t, sealedSrv, "trg-sealed-filter"),
			tokenRefusalCWLogsRegion, "trg-sealed-filter")

		// The control, as above.
		status, body, _ := tokenRefusalCWLogsCall(t, sealedSrv, "FilterLogEvents",
			fmt.Sprintf(`{"logGroupName":"trg-sealed-filter","nextToken":%q}`,
				base64.StdEncoding.EncodeToString([]byte("0"))))
		require.NotEqual(t, http.StatusBadRequest, status, body)

		status, body, code := tokenRefusalCWLogsCall(t, sealedSrv, "FilterLogEvents",
			`{"logGroupName":"trg-sealed-filter","nextToken":"!!not-base64!!"}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "InvalidParameterException", code, body)
	})
}

// --- EventBridge Scheduler ListSchedules ---

const tokenRefusalSchedulerHost = "scheduler.us-east-1.amazonaws.com"

// tokenRefusalSchedulerCall sends one Scheduler REST/JSON request and returns the status, the raw body
// and the bare error code a refusal carries.
//
// It is separate from [tokenRefusalJSONCall] for two reasons that are both the protocol's: Scheduler
// routes on the method and path rather than on X-Amz-Target, and it renders a refusal as
// {"message":…,"Code":…} rather than under __type, so the code has to be read from a different member.
func tokenRefusalSchedulerCall(t *testing.T, srv *emulator.Server, method, path, body string) (int, string, string) {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	r.Host = tokenRefusalSchedulerHost
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/"+
		"us-east-1/scheduler/aws4_request, SignedHeaders=host, Signature=fake")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s %s body", method, path)

	var errShape struct {
		Code string `json:"Code"`
	}
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr != nil {
		errShape.Code = ""
	}
	return resp.StatusCode, string(raw), errShape.Code
}

// tokenRefusalSchedulerList calls ListSchedules with the given query parameters.
//
// The token goes through [url.Values.Encode] rather than being concatenated, because that is what an
// SDK does and because a token containing a byte the query string reserves — "+" for one, which decodes
// to a space — would otherwise reach the handler as something the caller never sent, and the test would
// be asserting about the wrong string.
func tokenRefusalSchedulerList(t *testing.T, srv *emulator.Server, params url.Values) (int, string, string) {
	t.Helper()
	path := "/schedules"
	if len(params) > 0 {
		path += "?" + params.Encode()
	}
	return tokenRefusalSchedulerCall(t, srv, http.MethodGet, path, "")
}

// tokenRefusalSchedulerCreate creates one schedule through CreateSchedule, with every member the page
// marks Required: Yes, so the fixture is built the way a caller builds it (#765).
func tokenRefusalSchedulerCreate(t *testing.T, srv *emulator.Server, name string) {
	t.Helper()
	const body = `{"ScheduleExpression":"rate(1 hour)",` +
		`"Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"arn:aws:iam::123456789012:role/r"},` +
		`"FlexibleTimeWindow":{"Mode":"OFF"}}`
	status, out, code := tokenRefusalSchedulerCall(t, srv, http.MethodPost, "/schedules/"+name, body)
	require.Empty(t, code, out)
	require.Equal(t, http.StatusOK, status, out)
}

// TestPaginationToken_SchedulerListSchedulesRefusesATokenItDidNotIssue is #1086 at EventBridge
// Scheduler's single site.
//
// The code is ValidationException/400, which this operation's own Errors section publishes and which is
// the only refusal the service publishes for an input that fails a constraint — so the message carries
// which input failed, in the shape every other Scheduler refusal uses. The footing for the condition is
// the request parameter's own sentence, *"The token returned by a previous call to retrieve the next set
// of results."*, not an Errors entry: see scheduler_pagination.go.
//
// Unlike the JSON-protocol sites, the token travels in a query string, so the round-trip half is doing
// double duty — it also proves the encoding survives URL encoding, which a body-carried token never has
// to.
func TestPaginationToken_SchedulerListSchedulesRefusesATokenItDidNotIssue(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.SchedulerPlugin{})
	for _, name := range []string{"trs-sched-a", "trs-sched-b", "trs-sched-c"} {
		tokenRefusalSchedulerCreate(t, srv, name)
	}

	status, page1, code := tokenRefusalSchedulerList(t, srv, url.Values{"MaxResults": {"1"}})
	require.Empty(t, code, page1)
	require.Equal(t, http.StatusOK, status, page1)
	require.Contains(t, page1, "trs-sched-a")
	issued := tokenRefusalJSONToken(t, page1, "NextToken")

	status, page2, code := tokenRefusalSchedulerList(t, srv,
		url.Values{"MaxResults": {"1"}, "NextToken": {issued}})
	require.Empty(t, code, page2)
	require.Equal(t, http.StatusOK, status, page2)
	assert.Contains(t, page2, "trs-sched-b", "an issued token must resume rather than re-serve page one")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, body, code := tokenRefusalSchedulerList(t, srv,
				url.Values{"MaxResults": {"1"}, "NextToken": {tc.token}})
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Equal(t, "ValidationException", code, body)
			assert.Contains(t, body, "'nextToken'", "the message must name the member that failed")
			assert.Contains(t, body, "ListSchedules", "the message must name the operation")
			assert.NotContains(t, body, "trs-sched-a", "a refused token must not be answered with page one")
		})
	}

	t.Run("a past-the-end token clamps rather than being refused", func(t *testing.T) {
		// The two are different conditions and only one of them is a caller's mistake: a token substrate
		// issued over a listing that has since shrunk is still a token it issued, so it is answered with a
		// final empty page. Asserted here because [decodeOffsetPaginationToken] accepts it and only
		// [pageByOffsetToken] decides what it means.
		status, body, code := tokenRefusalSchedulerList(t, srv,
			url.Values{"NextToken": {base64.StdEncoding.EncodeToString([]byte("99"))}})
		assert.Equal(t, http.StatusOK, status, body)
		assert.Empty(t, code, body)
		assert.NotContains(t, body, "trs-sched-a", "a past-the-end offset must not reset to page one")
	})

	t.Run("refused before any state is read", func(t *testing.T) {
		// The seal works here because listSchedules' index loader propagates the store error rather than
		// reporting it as an empty index, so a handler that read first would answer 500 rather than an
		// empty 200. Athena needed [tokenRefusalCountingState] for the opposite reason.
		sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true}
		sealedSrv := tokenRefusalServer(t, sealed, &emulator.SchedulerPlugin{})
		status, body, code := tokenRefusalSchedulerList(t, sealedSrv,
			url.Values{"NextToken": {"!!not-base64!!"}})
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Equal(t, "ValidationException", code, body)
	})
}

// --- Batch's three resource describes ---

const tokenRefusalBatchHost = "batch.us-east-1.amazonaws.com"

// tokenRefusalBatchCall sends one Batch REST/JSON request and returns the status, the raw body and
// the bare error code a refusal carries.
//
// Batch renders a refusal as {"Code":…,"Message":…,"message":…} rather than under __type, so
// [tokenRefusalJSONCall] cannot read it; it also routes on the request path rather than on
// X-Amz-Target.
func tokenRefusalBatchCall(t *testing.T, srv *emulator.Server, path, body string) (int, string, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
	r.Host = tokenRefusalBatchHost
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/"+
		"us-east-1/batch/aws4_request, SignedHeaders=host, Signature=fake")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s body", path)

	var errShape struct {
		Code string `json:"Code"`
	}
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr != nil {
		errShape.Code = ""
	}
	return resp.StatusCode, string(raw), errShape.Code
}

// tokenRefusalBatchDescribe is one of the three describes, addressed by the resource member it
// returns.
type tokenRefusalBatchDescribe struct {
	// operation is the wire name the refusal message must carry.
	operation string
	// path is the describe's published request path.
	path string
	// createPath and createBody build one record; %s is the resource's name.
	createPath string
	createBody string
	// member is the response member the records arrive under.
	member string
	// names are the three records the fixture creates, in the order the index sorts them.
	names []string
}

// tokenRefusalBatchDescribes is the three operations sharing one paginator, which is why this is the
// only site in the class where one conversion covers three operations — and why the decode had to move
// out of the shared helper rather than staying in it.
var tokenRefusalBatchDescribes = []tokenRefusalBatchDescribe{
	{
		operation:  "DescribeComputeEnvironments",
		path:       "/v1/describecomputeenvironments",
		createPath: "/v1/createcomputeenvironment",
		createBody: `{"computeEnvironmentName":"%s","type":"MANAGED"}`,
		member:     "computeEnvironments",
		names:      []string{"trb-ce-a", "trb-ce-b", "trb-ce-c"},
	},
	{
		operation:  "DescribeJobQueues",
		path:       "/v1/describejobqueues",
		createPath: "/v1/createjobqueue",
		createBody: `{"jobQueueName":"%s","priority":1,` +
			`"computeEnvironmentOrder":[{"order":1,"computeEnvironment":"trb-ce-a"}]}`,
		member: "jobQueues",
		names:  []string{"trb-q-a", "trb-q-b", "trb-q-c"},
	},
	{
		operation:  "DescribeJobDefinitions",
		path:       "/v1/describejobdefinitions",
		createPath: "/v1/registerjobdefinition",
		createBody: `{"jobDefinitionName":"%s","type":"container"}`,
		member:     "jobDefinitions",
		names:      []string{"trb-jd-a", "trb-jd-b", "trb-jd-c"},
	},
}

// tokenRefusalBatchFixture creates one describe's three records through the real create operation
// (#765) and returns the server.
func tokenRefusalBatchFixture(t *testing.T, d tokenRefusalBatchDescribe) *emulator.Server {
	t.Helper()
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.BatchPlugin{})
	for _, name := range d.names {
		status, out, code := tokenRefusalBatchCall(t, srv, d.createPath, fmt.Sprintf(d.createBody, name))
		require.Empty(t, code, out)
		require.Equal(t, http.StatusOK, status, out)
	}
	return srv
}

// TestPaginationToken_BatchDescribesRefuseATokenTheyDidNotIssue is #1086's last site, and the only one
// where three operations share one paginator.
//
// The code is ClientException/400, which each page publishes in its own Errors section and which is —
// with ServerException/500 — the whole of Batch's published error vocabulary, since Batch publishes no
// common-errors page. The footing for the condition is each page's own description of the parameter:
// *"The nextToken value returned from a previous paginated Describe… request where maxResults was used
// and the results exceeded the value of that parameter."* See batch_pagination.go.
//
// All three are asserted rather than one plus an argument about the shared helper, because the fix had
// to *leave* the shared helper — DescribeJobDefinitions reads state before calling it, so a decode
// there would have been below a state read on exactly one of the three.
func TestPaginationToken_BatchDescribesRefuseATokenTheyDidNotIssue(t *testing.T) {
	for _, d := range tokenRefusalBatchDescribes {
		t.Run(d.operation, func(t *testing.T) {
			srv := tokenRefusalBatchFixture(t, d)

			status, page1, code := tokenRefusalBatchCall(t, srv, d.path, `{"maxResults":1}`)
			require.Empty(t, code, page1)
			require.Equal(t, http.StatusOK, status, page1)
			require.Contains(t, page1, d.names[0])
			issued := tokenRefusalJSONToken(t, page1, "nextToken")

			status, page2, code := tokenRefusalBatchCall(t, srv, d.path,
				fmt.Sprintf(`{"maxResults":1,"nextToken":%q}`, issued))
			require.Empty(t, code, page2)
			require.Equal(t, http.StatusOK, status, page2)
			assert.Contains(t, page2, d.names[1],
				"an issued token must resume rather than re-serve page one")

			for _, tc := range tokenRefusalBadTokens {
				t.Run(tc.name, func(t *testing.T) {
					status, body, code := tokenRefusalBatchCall(t, srv, d.path,
						fmt.Sprintf(`{"maxResults":1,"nextToken":%q}`, tc.token))
					assert.Equal(t, http.StatusBadRequest, status, body)
					assert.Equal(t, "ClientException", code, body)
					assert.Contains(t, body, d.operation, "the message must name the operation")
					assert.NotContains(t, body, d.names[0],
						"a refused token must not be answered with page one")
				})
			}

			t.Run("a past-the-end token clamps rather than being refused", func(t *testing.T) {
				status, body, code := tokenRefusalBatchCall(t, srv, d.path,
					fmt.Sprintf(`{"nextToken":%q}`,
						base64.StdEncoding.EncodeToString([]byte("99"))))
				assert.Equal(t, http.StatusOK, status, body)
				assert.Empty(t, code, body)
				assert.NotContains(t, body, d.names[0], "a past-the-end offset must not reset to page one")
			})

			t.Run("refused before any state is read", func(t *testing.T) {
				// The seal proves the ordering on all three, which is the half the shared helper could
				// not deliver: describeBatchResources only loads an index when no filter is supplied,
				// and DescribeJobDefinitions loads one of its own before it ever calls the helper.
				sealed := &tokenRefusalSealedState{
					inner: emulator.NewMemoryStateManager(), sealGets: true, sealLists: true,
				}
				sealedSrv := tokenRefusalServer(t, sealed, &emulator.BatchPlugin{})
				status, body, code := tokenRefusalBatchCall(t, sealedSrv, d.path,
					`{"nextToken":"!!not-base64!!"}`)
				assert.Equal(t, http.StatusBadRequest, status, body)
				assert.Equal(t, "ClientException", code, body)
			})
		})
	}
}

// TestPaginationToken_BatchTokensAreNotPortableBetweenTheThreeDescribes records the boundary the
// refusal does not cross: the token carries an offset and nothing else, so one operation's token is
// well-formed at another and indexes into a listing the caller never asked for.
//
// Asserted rather than left in a comment because the refusal's name invites the stronger reading, and
// because the message naming the operation would otherwise look like it enforced something.
func TestPaginationToken_BatchTokensAreNotPortableBetweenTheThreeDescribes(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.BatchPlugin{})
	for _, d := range tokenRefusalBatchDescribes {
		for _, name := range d.names {
			status, out, code := tokenRefusalBatchCall(t, srv, d.createPath, fmt.Sprintf(d.createBody, name))
			require.Empty(t, code, out)
			require.Equal(t, http.StatusOK, status, out)
		}
	}

	queues := tokenRefusalBatchDescribes[1]
	status, body, code := tokenRefusalBatchCall(t, srv, queues.path, `{"maxResults":1}`)
	require.Empty(t, code, body)
	require.Equal(t, http.StatusOK, status, body)
	issued := tokenRefusalJSONToken(t, body, "nextToken")

	envs := tokenRefusalBatchDescribes[0]
	status, body, code = tokenRefusalBatchCall(t, srv, envs.path,
		fmt.Sprintf(`{"maxResults":1,"nextToken":%q}`, issued))
	assert.Equal(t, http.StatusOK, status, body)
	assert.Empty(t, code, body)
	assert.Contains(t, body, envs.names[1],
		"a sibling's token decodes cleanly and indexes into this operation's own listing")
}
