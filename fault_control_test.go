package substrate_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func newFaultTestServer(t *testing.T) (*httptest.Server, *substrate.FaultController) {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	fc := substrate.NewFaultController(substrate.FaultConfig{Enabled: false}, 42)
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
		substrate.ServerOptions{Fault: fc})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, fc
}

func TestFaultControl_SetGetClearRules(t *testing.T) {
	ts, _ := newFaultTestServer(t)

	// GET before any rules — should be disabled with empty rules.
	resp, err := http.Get(ts.URL + "/v1/fault/rules")
	if err != nil {
		t.Fatalf("GET /v1/fault/rules: %v", err)
	}
	var cfg substrate.FaultConfig
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &cfg); err != nil {
		t.Fatalf("unmarshal GET response: %v", err)
	}
	if cfg.Enabled {
		t.Error("want enabled=false before any POST")
	}
	if len(cfg.Rules) != 0 {
		t.Errorf("want 0 rules, got %d", len(cfg.Rules))
	}

	// POST new rules.
	payload := substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{
				Service:     "s3",
				Operation:   "GetObject",
				FaultType:   "error",
				ErrorCode:   "NoSuchKey",
				HTTPStatus:  404,
				ErrorMsg:    "The specified key does not exist.",
				Probability: 1.0,
			},
		},
	}
	postBody, _ := json.Marshal(payload)
	postResp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(postBody))
	if err != nil {
		t.Fatalf("POST /v1/fault/rules: %v", err)
	}
	_, _ = io.ReadAll(postResp.Body)
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 from POST, got %d", postResp.StatusCode)
	}

	// GET again — should reflect the new config.
	resp2, err := http.Get(ts.URL + "/v1/fault/rules")
	if err != nil {
		t.Fatalf("second GET: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	var cfg2 substrate.FaultConfig
	if err := json.Unmarshal(body2, &cfg2); err != nil {
		t.Fatalf("unmarshal second GET: %v", err)
	}
	if !cfg2.Enabled {
		t.Error("want enabled=true after POST")
	}
	if len(cfg2.Rules) != 1 {
		t.Fatalf("want 1 rule, got %d", len(cfg2.Rules))
	}
	if cfg2.Rules[0].ErrorCode != "NoSuchKey" {
		t.Errorf("want ErrorCode=NoSuchKey, got %q", cfg2.Rules[0].ErrorCode)
	}

	// DELETE — should clear rules and disable.
	delReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/fault/rules", nil)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE /v1/fault/rules: %v", err)
	}
	_, _ = io.ReadAll(delResp.Body)
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 from DELETE, got %d", delResp.StatusCode)
	}

	// GET after DELETE — should be disabled again.
	resp3, err := http.Get(ts.URL + "/v1/fault/rules")
	if err != nil {
		t.Fatalf("GET after DELETE: %v", err)
	}
	body3, _ := io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
	var cfg3 substrate.FaultConfig
	if err := json.Unmarshal(body3, &cfg3); err != nil {
		t.Fatalf("unmarshal GET after DELETE: %v", err)
	}
	if cfg3.Enabled {
		t.Error("want enabled=false after DELETE")
	}
}

func TestFaultControl_NilController_Returns501(t *testing.T) {
	// Build a server WITHOUT a FaultController.
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		req, _ := http.NewRequest(method, ts.URL+"/v1/fault/rules", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s /v1/fault/rules: %v", method, err)
		}
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotImplemented {
			t.Errorf("%s: want 501, got %d", method, resp.StatusCode)
		}
	}
}

func TestFaultControl_StateResetClearsFaultRules(t *testing.T) {
	ts, fc := newFaultTestServer(t)

	// Enable fault injection via the API.
	payload := substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 1.0},
		},
	}
	postBody, _ := json.Marshal(payload)
	postResp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(postBody))
	if err != nil {
		t.Fatalf("POST rules: %v", err)
	}
	_, _ = io.ReadAll(postResp.Body)
	_ = postResp.Body.Close()

	if !fc.GetConfig().Enabled {
		t.Fatal("want fault enabled after POST")
	}

	// Call state reset.
	resetResp, err := http.Post(ts.URL+"/v1/state/reset", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /v1/state/reset: %v", err)
	}
	_, _ = io.ReadAll(resetResp.Body)
	_ = resetResp.Body.Close()

	// Fault config should now be cleared.
	if fc.GetConfig().Enabled {
		t.Error("want fault disabled after state reset")
	}
	if len(fc.GetConfig().Rules) != 0 {
		t.Errorf("want 0 rules after state reset, got %d", len(fc.GetConfig().Rules))
	}
}

func TestFaultControl_InvalidBody_Returns400(t *testing.T) {
	ts, _ := newFaultTestServer(t)
	resp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader([]byte(`not-json`)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400 for bad JSON, got %d", resp.StatusCode)
	}
}
