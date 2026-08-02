package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

func newFaultTestServer(t *testing.T) (*httptest.Server, *emulator.FaultController) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	fc := emulator.NewFaultController(emulator.FaultConfig{Enabled: false}, 42)
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger,
		emulator.ServerOptions{Fault: fc})
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
	var cfg emulator.FaultConfig
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
	payload := emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
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
	var cfg2 emulator.FaultConfig
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
	var cfg3 emulator.FaultConfig
	if err := json.Unmarshal(body3, &cfg3); err != nil {
		t.Fatalf("unmarshal GET after DELETE: %v", err)
	}
	if cfg3.Enabled {
		t.Error("want enabled=false after DELETE")
	}
}

// TestFaultControl_ReportsFiredCount covers the #480 companion finding over the
// control plane. A rule that matches nothing produces exactly the same passing test
// as a consumer's retry working, so a fixture that arms a fault and then sees success
// has proven nothing without this count. It rides on the existing GET, since that
// handler already marshals the whole FaultConfig.
func TestFaultControl_ReportsFiredCount(t *testing.T) {
	ts, fc := newFaultTestServer(t)

	payload := emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
			{Service: "s3", Operation: "PutObject", FaultType: "error", ErrorCode: "SlowDown", Times: 2},
			// A second rule that no request below touches, so its count stays zero and
			// the report is per-rule rather than a single total.
			{Service: "dynamodb", Operation: "PutItem", FaultType: "error", ErrorCode: "InternalError", Times: -1},
		},
	}
	postBody, _ := json.Marshal(payload)
	postResp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(postBody))
	if err != nil {
		t.Fatalf("POST rules: %v", err)
	}
	_, _ = io.ReadAll(postResp.Body)
	_ = postResp.Body.Close()

	// A freshly armed rule reports zero, which is what makes a non-zero count later
	// an assertion rather than an artifact of the rule having been armed.
	if got := faultRuleCounts(t, ts); got[0] != 0 || got[1] != 0 {
		t.Fatalf("fired counts before any request = %v, want [0 0]", got)
	}

	req := &emulator.AWSRequest{Service: "s3", Operation: "PutObject"}
	for i := 0; i < 3; i++ {
		_, _ = fc.InjectFault(&emulator.RequestContext{}, req)
	}

	got := faultRuleCounts(t, ts)
	if got[0] != 2 {
		t.Errorf("s3 rule fired = %d, want 2 (its Times bound)", got[0])
	}
	if got[1] != 0 {
		t.Errorf("dynamodb rule fired = %d, want 0 — it matched nothing", got[1])
	}

	// Re-arming replaces the configuration, so the counts start over. A fixture that
	// arms the same rule between phases needs its full budget, not a spent one.
	rearm, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(postBody))
	if err != nil {
		t.Fatalf("re-POST rules: %v", err)
	}
	_, _ = io.ReadAll(rearm.Body)
	_ = rearm.Body.Close()
	if got := faultRuleCounts(t, ts); got[0] != 0 {
		t.Errorf("s3 rule fired = %d after re-arming, want 0", got[0])
	}
}

// faultRuleCounts GETs the control plane's rule list and returns each rule's fired
// count, in the order the rules were armed.
func faultRuleCounts(t *testing.T, ts *httptest.Server) []int {
	t.Helper()
	resp, err := http.Get(ts.URL + "/v1/fault/rules")
	if err != nil {
		t.Fatalf("GET /v1/fault/rules: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/fault/rules: status %d: %s", resp.StatusCode, body)
	}
	var cfg emulator.FaultConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		t.Fatalf("unmarshal GET response: %v", err)
	}
	counts := make([]int, 0, len(cfg.Rules))
	for _, rule := range cfg.Rules {
		counts = append(counts, rule.Fired)
	}
	return counts
}

// TestFaultControl_RoundTripsTheWireMatchers asserts the three matchers #480 added
// survive the control plane's JSON encoding. They are useless if a rule armed over
// HTTP loses them, and the field tags are the only thing that carries them.
func TestFaultControl_RoundTripsTheWireMatchers(t *testing.T) {
	ts, _ := newFaultTestServer(t)

	payload := emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:      "s3",
			Operation:    "CompleteMultipartUpload",
			PathSuffix:   ".parquet",
			QueryKey:     "uploadId",
			HeaderPrefix: "x-amz-server-side-encryption",
			FaultType:    "error",
			ErrorCode:    "InternalError",
			Times:        3,
		}},
	}
	postBody, _ := json.Marshal(payload)
	postResp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(postBody))
	if err != nil {
		t.Fatalf("POST rules: %v", err)
	}
	_, _ = io.ReadAll(postResp.Body)
	_ = postResp.Body.Close()

	resp, err := http.Get(ts.URL + "/v1/fault/rules")
	if err != nil {
		t.Fatalf("GET rules: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var cfg emulator.FaultConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(cfg.Rules) != 1 {
		t.Fatalf("want 1 rule, got %d", len(cfg.Rules))
	}
	got := cfg.Rules[0]
	if got.PathSuffix != ".parquet" {
		t.Errorf("PathSuffix = %q, want .parquet", got.PathSuffix)
	}
	if got.QueryKey != "uploadId" {
		t.Errorf("QueryKey = %q, want uploadId", got.QueryKey)
	}
	if got.HeaderPrefix != "x-amz-server-side-encryption" {
		t.Errorf("HeaderPrefix = %q, want x-amz-server-side-encryption", got.HeaderPrefix)
	}
	if got.Times != 3 {
		t.Errorf("Times = %d, want 3", got.Times)
	}
}

func TestFaultControl_NilController_Returns501(t *testing.T) {
	// Build a server WITHOUT a FaultController.
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)
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
	payload := emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{
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
