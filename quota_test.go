package substrate_test

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// makeQuotaReqCtx returns a minimal RequestContext for quota tests.
func makeQuotaReqCtx() *substrate.RequestContext {
	return &substrate.RequestContext{
		RequestID: "test-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{},
	}
}

func TestQuotaController_Disabled_AllowsUnlimited(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	ctrl := substrate.NewQuotaController(substrate.QuotaConfig{Enabled: false}, tc)

	req := &substrate.AWSRequest{Service: "iam", Operation: "CreateUser"}
	reqCtx := makeQuotaReqCtx()

	for i := 0; i < 1000; i++ {
		require.NoError(t, ctrl.CheckQuota(reqCtx, req), "request %d should not be throttled", i)
	}
}

func TestQuotaController_BurstExhausted_Returns429(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"iam": {Rate: 1, Burst: 5},
		},
	}
	ctrl := substrate.NewQuotaController(cfg, tc)

	req := &substrate.AWSRequest{Service: "iam", Operation: "ListUsers"}
	reqCtx := makeQuotaReqCtx()

	// First 5 should pass (burst=5).
	for i := 0; i < 5; i++ {
		require.NoError(t, ctrl.CheckQuota(reqCtx, req), "request %d should pass", i)
	}

	// 6th must be throttled.
	err := ctrl.CheckQuota(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "ThrottlingException", awsErr.Code)
	assert.Equal(t, http.StatusTooManyRequests, awsErr.HTTPStatus)
}

func TestQuotaController_TokenRefill_AllowsAfterAdvance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"iam": {Rate: 1, Burst: 1},
		},
	}
	ctrl := substrate.NewQuotaController(cfg, tc)

	req := &substrate.AWSRequest{Service: "iam", Operation: "ListUsers"}
	reqCtx := makeQuotaReqCtx()

	// Consume the single token.
	require.NoError(t, ctrl.CheckQuota(reqCtx, req))

	// Next request should be throttled.
	require.Error(t, ctrl.CheckQuota(reqCtx, req))

	// Advance clock by 2 seconds → 2 tokens refilled (capped at burst=1).
	tc.SetTime(start.Add(2 * time.Second))

	// Now should pass.
	require.NoError(t, ctrl.CheckQuota(reqCtx, req))
}

func TestQuotaController_OperationKeyTakesPrecedence(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"iam":            {Rate: 100, Burst: 100},
			"iam/CreateUser": {Rate: 1, Burst: 1},
		},
	}
	ctrl := substrate.NewQuotaController(cfg, tc)

	reqCtx := makeQuotaReqCtx()

	// iam/CreateUser uses the operation-level rule (burst=1).
	createReq := &substrate.AWSRequest{Service: "iam", Operation: "CreateUser"}
	require.NoError(t, ctrl.CheckQuota(reqCtx, createReq))
	require.Error(t, ctrl.CheckQuota(reqCtx, createReq), "second CreateUser should be throttled")

	// iam/ListUsers falls back to the service rule (burst=100).
	listReq := &substrate.AWSRequest{Service: "iam", Operation: "ListUsers"}
	for i := 0; i < 10; i++ {
		require.NoError(t, ctrl.CheckQuota(reqCtx, listReq), "ListUsers request %d should pass", i)
	}
}

func TestQuotaController_ReplayingContext_NeverThrottled(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"iam": {Rate: 1, Burst: 1},
		},
	}
	ctrl := substrate.NewQuotaController(cfg, tc)

	req := &substrate.AWSRequest{Service: "iam", Operation: "ListUsers"}
	reqCtx := &substrate.RequestContext{
		RequestID: "replay-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{"replaying": true},
	}

	// Even beyond burst, replaying requests always pass.
	for i := 0; i < 100; i++ {
		require.NoError(t, ctrl.CheckQuota(reqCtx, req), "replaying request %d should never be throttled", i)
	}
}

func TestQuotaController_UnknownService_Allowed(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"iam": {Rate: 1, Burst: 1},
		},
	}
	ctrl := substrate.NewQuotaController(cfg, tc)

	req := &substrate.AWSRequest{Service: "s3", Operation: "GetObject"}
	reqCtx := makeQuotaReqCtx()

	// s3 has no rule configured — should always pass.
	for i := 0; i < 50; i++ {
		require.NoError(t, ctrl.CheckQuota(reqCtx, req))
	}
}

func TestQuotaController_Race(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"iam": {Rate: 10000, Burst: 10000},
		},
	}
	ctrl := substrate.NewQuotaController(cfg, tc)

	req := &substrate.AWSRequest{Service: "iam", Operation: "ListUsers"}

	var wg sync.WaitGroup
	var throttled atomic.Int64

	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqCtx := makeQuotaReqCtx()
			for i := 0; i < 10; i++ {
				if err := ctrl.CheckQuota(reqCtx, req); err != nil {
					throttled.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	// With burst=10000 and only 500 total requests, none should be throttled.
	assert.Equal(t, int64(0), throttled.Load())
}

func TestQuotaController_Integration_Returns429XMLBody(t *testing.T) {
	plug := &serverPlugin{
		serviceName: "iam",
		resp: &substrate.AWSResponse{
			StatusCode: http.StatusOK,
			Body:       []byte(`<CreateUserResponse/>`),
		},
	}

	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	registry.Register(plug)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)

	quotaCtrl := substrate.NewQuotaController(substrate.QuotaConfig{
		Enabled: true,
		Rules:   map[string]substrate.RateRule{"iam/CreateUser": {Rate: 1, Burst: 3}},
	}, tc)

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
		substrate.ServerOptions{Quota: quotaCtrl})

	sendCreateUser := func() *http.Response {
		r := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Action=CreateUser&UserName=alice&Version=2010-05-08"))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Result()
	}

	// First 3 succeed (burst=3).
	for i := 0; i < 3; i++ {
		resp := sendCreateUser()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "request %d should pass", i)
	}

	// 4th must return 429 with ThrottlingException XML.
	resp := sendCreateUser()
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &errResp))
	assert.Equal(t, "ThrottlingException", errResp.Error.Code)
}

func TestQuotaController_UpdateConfig(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"s3": {Rate: 10, Burst: 10},
		},
	}
	qc := substrate.NewQuotaController(cfg, tc)

	// Replace config with higher-rate rules.
	newCfg := substrate.QuotaConfig{
		Enabled: true,
		Rules: map[string]substrate.RateRule{
			"s3":       {Rate: 1000, Burst: 1000},
			"dynamodb": {Rate: 500, Burst: 500},
		},
	}
	qc.UpdateConfig(newCfg)
	// After update with high burst, the first request should pass.
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := makeQuotaReqCtx()
	err := qc.CheckQuota(reqCtx, req)
	require.NoError(t, err, "first request after UpdateConfig should pass")
}
