package emulator_test

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// s3QuotaController builds a QuotaController whose only rules are the ones given,
// on a clock that does not advance on its own — a token refilled by wall-clock
// elapsed would make every rate assertion below a race.
func s3QuotaController(rules map[string]emulator.RateRule) *emulator.QuotaController {
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return emulator.NewQuotaController(emulator.QuotaConfig{Enabled: true, Rules: rules}, tc)
}

// s3QuotaRequest builds an S3 request the quota gate can classify and locate: a
// verb for the rate class and a path for the bucket and prefix, exactly the two
// fields ParseAWSRequest has populated by the time the gate runs.
func s3QuotaRequest(method, path string) *emulator.AWSRequest {
	return &emulator.AWSRequest{
		Service:    "s3",
		Operation:  "PutObject",
		HTTPMethod: method,
		Path:       path,
	}
}

func TestS3AccountingPrefix(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantPrefix string
	}{
		{
			name: "service root names no bucket",
			path: "/",
		},
		{
			name:       "bucket alone is the bucket root",
			path:       "/photos",
			wantBucket: "photos",
		},
		{
			name:       "bucket with a trailing slash is the bucket root",
			path:       "/photos/",
			wantBucket: "photos",
		},
		{
			name:       "a key with no slash is the bucket root",
			path:       "/photos/cat.jpg",
			wantBucket: "photos",
		},
		{
			name:       "the prefix includes its trailing slash",
			path:       "/logs/2026/09/app.log",
			wantBucket: "logs",
			wantPrefix: "2026/",
		},
		{
			name:       "a directory marker is its own prefix",
			path:       "/logs/2026/",
			wantBucket: "logs",
			wantPrefix: "2026/",
		},
		{
			name:       "only the first slash splits",
			path:       "/logs/a/b",
			wantBucket: "logs",
			wantPrefix: "a/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix := emulator.S3AccountingPrefixForTest(
				s3QuotaRequest(http.MethodPut, tt.path))
			assert.Equal(t, tt.wantBucket, bucket, "bucket")
			assert.Equal(t, tt.wantPrefix, prefix, "prefix")
		})
	}
}

// TestS3PrefixQuota_DistinctPrefixesDoNotShareACeiling is the acceptance criterion
// itself: the same volume of writes is refused against one prefix and allowed when
// spread across two, which is what AWS's own guidance to parallelise across
// prefixes presumes (#818).
func TestS3PrefixQuota_DistinctPrefixesDoNotShareACeiling(t *testing.T) {
	_, writeKey := emulator.S3PrefixRateRuleKeysForTest()
	ctrl := s3QuotaController(map[string]emulator.RateRule{
		writeKey: {Rate: 3, Burst: 3},
	})
	reqCtx := makeQuotaReqCtx()

	for i := 0; i < 3; i++ {
		require.NoError(t,
			ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodPut, "/logs/alpha/f.log")),
			"write %d under the first prefix should pass", i)
	}

	err := ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodPut, "/logs/alpha/f.log"))
	require.Error(t, err, "the fourth write under one prefix must be refused")
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "SlowDown", awsErr.Code)
	assert.Equal(t, http.StatusServiceUnavailable, awsErr.HTTPStatus)
	assert.Contains(t, awsErr.Message, `"alpha/"`,
		"the message should name the prefix that was throttled")

	// The prefix is exhausted; a different prefix in the same bucket has its own
	// ceiling, so the identical volume passes.
	for i := 0; i < 3; i++ {
		require.NoError(t,
			ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodPut, "/logs/beta/f.log")),
			"write %d under the second prefix should pass", i)
	}

	// And so does the same bucket's root, which is a prefix like any other.
	require.NoError(t, ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodPut, "/logs/f.log")))
}

// TestS3PrefixQuota_ReadAndWriteCeilingsAreIndependent covers the other half of the
// split: AWS publishes two figures per prefix, not one, so exhausting writes must
// not refuse a read of the same prefix.
func TestS3PrefixQuota_ReadAndWriteCeilingsAreIndependent(t *testing.T) {
	readKey, writeKey := emulator.S3PrefixRateRuleKeysForTest()
	ctrl := s3QuotaController(map[string]emulator.RateRule{
		writeKey: {Rate: 1, Burst: 1},
		readKey:  {Rate: 1, Burst: 2},
	})
	reqCtx := makeQuotaReqCtx()

	const path = "/logs/alpha/f.log"
	require.NoError(t, ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodPut, path)))
	require.Error(t, ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodPut, path)),
		"the write ceiling is exhausted")

	require.NoError(t, ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodGet, path)),
		"a read counts against the read ceiling, which the writes did not touch")
	require.NoError(t, ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodHead, path)),
		"HEAD is a read too")

	// The two reads shared one ceiling, so the third is over it — which is what
	// makes the first two a real observation of the read rule rather than of a rule
	// nothing enforced.
	err := ctrl.CheckQuota(reqCtx, s3QuotaRequest(http.MethodGet, path))
	require.Error(t, err)
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "SlowDown", awsErr.Code)
}

// TestS3PrefixQuota_OperationRuleWinsOverItsClass pins the resolution order: a
// caller who wants to rate-limit one S3 operation can still say so, and the rule
// they name governs the whole service rather than one prefix, because it is not a
// class rule.
func TestS3PrefixQuota_OperationRuleWinsOverItsClass(t *testing.T) {
	_, writeKey := emulator.S3PrefixRateRuleKeysForTest()
	ctrl := s3QuotaController(map[string]emulator.RateRule{
		writeKey:          {Rate: 100, Burst: 100},
		"s3/PutObject":    {Rate: 1, Burst: 1},
		"s3/DeleteObject": {Rate: 100, Burst: 100},
	})
	reqCtx := makeQuotaReqCtx()

	put := func(path string) *emulator.AWSRequest {
		req := s3QuotaRequest(http.MethodPut, path)
		req.Operation = "PutObject"
		return req
	}

	require.NoError(t, ctrl.CheckQuota(reqCtx, put("/logs/alpha/f.log")))
	err := ctrl.CheckQuota(reqCtx, put("/logs/beta/f.log"))
	require.Error(t, err,
		"the operation rule beat the class rule, and it is not accounted per prefix")
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "SlowDown", awsErr.Code,
		"S3 answers SlowDown whichever rule refused the request")
	assert.Contains(t, awsErr.Message, `"s3/PutObject"`,
		"the message names the rule, not a prefix the refusal was not accounted against")
	assert.NotContains(t, awsErr.Message, "prefix")
}

func TestDefaultQuotaRules_S3CarriesThePublishedPerPrefixCeilings(t *testing.T) {
	rules := emulator.DefaultQuotaRulesForTest()
	readKey, writeKey := emulator.S3PrefixRateRuleKeysForTest()
	wantRead, wantWrite := emulator.S3PrefixRateDefaultsForTest()

	read, ok := rules[readKey]
	require.True(t, ok, "a default rule must exist for %q", readKey)
	assert.Equal(t, wantRead, read.Rate)
	assert.Equal(t, wantRead, read.Burst,
		"AWS publishes no burst allowance, so the depth is one second's worth")

	write, ok := rules[writeKey]
	require.True(t, ok, "a default rule must exist for %q", writeKey)
	assert.Equal(t, wantWrite, write.Rate)
	assert.Equal(t, wantWrite, write.Burst)

	_, ok = rules["s3"]
	assert.False(t, ok,
		"a bucket-wide s3 rule would account every prefix together, which is what #818 removed")
}

// TestS3PrefixQuota_WireBodyIsS3sBareErrorDocument asserts what an SDK sees. The
// code and the status matter more than the message: an S3 error goes back as a bare
// <Error> document rather than the Query protocol's wrapped <ErrorResponse>, and an
// SDK that cannot find the code falls back to reporting the bare HTTP status.
func TestS3PrefixQuota_WireBodyIsS3sBareErrorDocument(t *testing.T) {
	plug := &serverPlugin{
		serviceName: "s3",
		resp: &emulator.AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"ETag": `"d41d8cd98f00b204e9800998ecf8427e"`},
		},
	}

	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	registry.Register(plug)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)

	_, writeKey := emulator.S3PrefixRateRuleKeysForTest()
	quotaCtrl := emulator.NewQuotaController(emulator.QuotaConfig{
		Enabled: true,
		Rules:   map[string]emulator.RateRule{writeKey: {Rate: 1, Burst: 1}},
	}, tc)

	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger,
		emulator.ServerOptions{Quota: quotaCtrl})

	putObject := func(path string) *http.Response {
		r := httptest.NewRequest(http.MethodPut, path, strings.NewReader("payload"))
		r.Host = "s3.amazonaws.com"
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Result()
	}

	resp := putObject("/logs/alpha/f.log")
	require.Equal(t, http.StatusOK, resp.StatusCode, "the first write is within the ceiling")

	resp = putObject("/logs/alpha/g.log")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var doc struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc),
		"the body must be S3's bare <Error> document: %s", body)
	assert.Equal(t, "SlowDown", doc.Code)
	assert.NotEmpty(t, doc.Message)

	// A different prefix is unaffected, over the wire as in the gate.
	resp = putObject("/logs/beta/f.log")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
