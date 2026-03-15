package substrate_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func newCETestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.CEPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"event_store": store,
		},
	}); err != nil {
		t.Fatalf("initialize ce plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func ceRequest(t *testing.T, ts *httptest.Server, op string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal ce request: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSInsightsIndexService."+op)
	req.Host = "ce.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ce request %s: %v", op, err)
	}
	return resp
}

func TestCE_GetCostAndUsage(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "GetCostAndUsage", map[string]interface{}{
		"TimePeriod":  map[string]string{"Start": "2026-01-01", "End": "2026-02-01"},
		"Granularity": "MONTHLY",
		"Metrics":     []string{"UnblendedCost"},
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["ResultsByTime"]; !ok {
		t.Error("expected ResultsByTime in response")
	}
	if _, ok := out["DimensionValueAttributes"]; !ok {
		t.Error("expected DimensionValueAttributes in response")
	}
}

func TestCE_GetCostForecast(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "GetCostForecast", map[string]interface{}{
		"TimePeriod":  map[string]string{"Start": "2026-02-01", "End": "2026-03-01"},
		"Metric":      "UNBLENDED_COST",
		"Granularity": "MONTHLY",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["Total"]; !ok {
		t.Error("expected Total in response")
	}
	if _, ok := out["ForecastResultsByTime"]; !ok {
		t.Error("expected ForecastResultsByTime in response")
	}

	total, ok := out["Total"].(map[string]interface{})
	if !ok {
		t.Fatal("Total is not an object")
	}
	if _, ok := total["Amount"]; !ok {
		t.Error("expected Amount in Total")
	}
	if unit, _ := total["Unit"].(string); unit != "USD" {
		t.Errorf("expected Unit=USD, got %q", unit)
	}
}

func TestCE_GetDimensionValues(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "GetDimensionValues", map[string]interface{}{
		"TimePeriod": map[string]string{"Start": "2026-01-01", "End": "2026-02-01"},
		"Dimension":  "SERVICE",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["DimensionValues"]; !ok {
		t.Error("expected DimensionValues in response")
	}
}

func TestCE_UnsupportedOperation(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "UnknownOp", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}
