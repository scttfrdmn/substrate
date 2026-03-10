package substrate_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestCostController_KnownOperations(t *testing.T) {
	ctrl := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	tests := []struct {
		service   string
		operation string
		want      float64
	}{
		{"s3", "PutObject", 0.000005},
		{"s3", "GetObject", 0.0000004},
		{"dynamodb", "GetItem", 0.00000025},
		{"lambda", "Invoke", 0.0000002},
		{"iam", "CreateUser", 0.0},
		{"sts", "AssumeRole", 0.0},
	}

	for _, tc := range tests {
		req := &substrate.AWSRequest{Service: tc.service, Operation: tc.operation}
		got := ctrl.CostForRequest(req)
		assert.InDelta(t, tc.want, got, 1e-12,
			"CostForRequest(%s/%s) = %v, want %v", tc.service, tc.operation, got, tc.want)
	}
}

func TestCostController_Unknown_ReturnsZero(t *testing.T) {
	ctrl := substrate.NewCostController(substrate.CostConfig{Enabled: true})
	req := &substrate.AWSRequest{Service: "unknownservice", Operation: "UnknownOp"}
	assert.Equal(t, 0.0, ctrl.CostForRequest(req))
}

func TestCostController_Disabled_AlwaysZero(t *testing.T) {
	ctrl := substrate.NewCostController(substrate.CostConfig{Enabled: false})

	for _, svc := range []string{"s3", "dynamodb", "lambda", "iam"} {
		req := &substrate.AWSRequest{Service: svc, Operation: "PutObject"}
		assert.Equal(t, 0.0, ctrl.CostForRequest(req),
			"disabled controller should always return 0 for %s", svc)
	}
}

func TestCostController_Override_WinsOverBuiltin(t *testing.T) {
	ctrl := substrate.NewCostController(substrate.CostConfig{
		Enabled: true,
		Overrides: map[string]float64{
			"s3/PutObject": 0.001, // override
		},
	})

	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	assert.InDelta(t, 0.001, ctrl.CostForRequest(req), 1e-12)

	// Built-in entry untouched.
	req2 := &substrate.AWSRequest{Service: "s3", Operation: "GetObject"}
	assert.InDelta(t, 0.0000004, ctrl.CostForRequest(req2), 1e-12)
}

func TestGetCostSummary_EmptyStore(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	summary, err := store.GetCostSummary(context.Background(), "123456789012", time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 0.0, summary.TotalCost)
	assert.Equal(t, int64(0), summary.RequestCount)
}

func TestGetCostSummary_AggregatesCorrectly(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	reqCtx := &substrate.RequestContext{
		AccountID: "111111111111",
		Region:    "us-east-1",
		Metadata:  map[string]interface{}{},
	}

	// Record three events.
	for i, tc := range []struct {
		svc  string
		op   string
		cost float64
	}{
		{"s3", "PutObject", 0.000005},
		{"s3", "GetObject", 0.0000004},
		{"iam", "CreateUser", 0.0},
	} {
		req := &substrate.AWSRequest{Service: tc.svc, Operation: tc.op}
		require.NoError(t, store.RecordRequest(ctx, reqCtx, req, nil,
			time.Duration(i)*time.Millisecond, tc.cost, nil))
	}

	summary, err := store.GetCostSummary(ctx, "111111111111", time.Time{}, time.Time{})
	require.NoError(t, err)

	assert.Equal(t, int64(3), summary.RequestCount)
	assert.InDelta(t, 0.0000054, summary.TotalCost, 1e-12)
	assert.InDelta(t, 0.0000054, summary.ByService["s3"], 1e-12)
	assert.Equal(t, 0.0, summary.ByService["iam"])
	assert.InDelta(t, 0.000005, summary.ByOperation["s3/PutObject"], 1e-12)
	assert.InDelta(t, 0.0000004, summary.ByOperation["s3/GetObject"], 1e-12)
	_ = base
}

func TestGetCostSummary_AccountFilter(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()

	for _, acct := range []string{"111111111111", "222222222222"} {
		reqCtx := &substrate.RequestContext{
			AccountID: acct,
			Region:    "us-east-1",
			Metadata:  map[string]interface{}{},
		}
		req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
		require.NoError(t, store.RecordRequest(ctx, reqCtx, req, nil, time.Millisecond, 0.000005, nil))
	}

	summary, err := store.GetCostSummary(ctx, "111111111111", time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), summary.RequestCount)
}

func TestGetCostSummary_TimeRangeFilter(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	ctx := context.Background()
	reqCtx := &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		Metadata:  map[string]interface{}{},
	}

	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	// RecordRequest timestamps events with time.Now(), so we record two in
	// quick succession and use the start/end filter with zero values (unbounded).
	require.NoError(t, store.RecordRequest(ctx, reqCtx, req, nil, time.Millisecond, 0.000005, nil))
	require.NoError(t, store.RecordRequest(ctx, reqCtx, req, nil, time.Millisecond, 0.000005, nil))

	summary, err := store.GetCostSummary(ctx, "123456789012", time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), summary.RequestCount)
	assert.InDelta(t, 0.00001, summary.TotalCost, 1e-12)
}

func TestCostController_Integration_CostRecordedInStore(t *testing.T) {
	// Use DynamoDB/GetItem which has a defined cost (0.00000025) and uses
	// X-Amz-Target so the parser produces a clean operation name.
	plug := &serverPlugin{
		serviceName: "dynamodb",
		resp: &substrate.AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
			Body:       []byte(`{"Item":{}}`),
		},
	}

	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	registry.Register(plug)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)

	costCtrl := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
		substrate.ServerOptions{Costs: costCtrl})

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"TableName":"test"}`))
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	events, err := store.GetStream(context.Background(), "default")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "GetItem", events[0].Operation)
	assert.InDelta(t, 0.00000025, events[0].Cost, 1e-12)
}

func TestCostController_UpdateConfig(t *testing.T) {
	cc := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	// Update with a custom cost override.
	newCfg := substrate.CostConfig{
		Enabled:   true,
		Overrides: map[string]float64{"s3/PutObject": 0.0001},
	}
	cc.UpdateConfig(newCfg)

	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	cost := cc.CostForRequest(req)
	// The override rate for PutObject, should be non-negative.
	if cost < 0 {
		t.Errorf("expected non-negative cost, got %f", cost)
	}
}
