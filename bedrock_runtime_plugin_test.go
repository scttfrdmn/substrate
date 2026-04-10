package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// bedrockRuntimeTestSetup holds state and server for Bedrock Runtime tests.
type bedrockRuntimeTestSetup struct {
	server *httptest.Server
	state  *substrate.MemoryStateManager
}

// newBedrockRuntimeTestServer builds a minimal server with BedrockRuntimePlugin registered,
// returning both the server and the underlying state manager for blocklist seeding.
func newBedrockRuntimeTestServer(t *testing.T) bedrockRuntimeTestSetup {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.BedrockRuntimePlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize bedrock-runtime plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return bedrockRuntimeTestSetup{server: ts, state: state}
}

func brRequest(t *testing.T, ts *httptest.Server, guardrailID, version string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal br body: %v", err)
	}
	path := "/guardrail/" + guardrailID + "/version/" + version + "/apply"
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build br request: %v", err)
	}
	req.Host = "bedrock-runtime.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do br request: %v", err)
	}
	return resp
}

func brBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read br body: %v", err)
	}
	return b
}

func brPayload(source, text string) map[string]interface{} {
	return map[string]interface{}{
		"source": source,
		"content": []map[string]interface{}{
			{"text": map[string]string{"text": text}},
		},
	}
}

// brInvokeRequest sends a Bedrock Runtime InvokeModel request.
func brInvokeRequest(t *testing.T, ts *httptest.Server, modelID string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal invoke body: %v", err)
	}
	path := "/model/" + modelID + "/invoke"
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build invoke request: %v", err)
	}
	req.Host = "bedrock-runtime.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do invoke request: %v", err)
	}
	return resp
}

// TestBedrockRuntimePlugin_PassThrough verifies that content passes through with action NONE.
func TestBedrockRuntimePlugin_PassThrough(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	resp := brRequest(t, setup.server, "guardrail-001", "DRAFT", brPayload("INPUT", "Hello world"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, brBody(t, resp))
	}
	var result struct {
		Action  string              `json:"action"`
		Outputs []map[string]string `json:"outputs"`
	}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Action != "NONE" {
		t.Errorf("expected NONE, got %q", result.Action)
	}
	if len(result.Outputs) != 1 || result.Outputs[0]["text"] != "Hello world" {
		t.Errorf("expected echoed text, got %+v", result.Outputs)
	}
}

// TestBedrockRuntimePlugin_BlocklistIntervened verifies blocked content triggers GUARDRAIL_INTERVENED.
func TestBedrockRuntimePlugin_BlocklistIntervened(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	// Seed the blocklist with "forbidden" term.
	blocklistKey := "guardrail:000000000000/guardrail-block/blocklist"
	blocklist, _ := json.Marshal([]string{"forbidden"})
	if err := setup.state.Put(context.Background(), "bedrock-runtime", blocklistKey, blocklist); err != nil {
		t.Fatalf("seed blocklist: %v", err)
	}

	resp := brRequest(t, setup.server, "guardrail-block", "1", brPayload("INPUT", "This contains forbidden content"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Action != "GUARDRAIL_INTERVENED" {
		t.Errorf("expected GUARDRAIL_INTERVENED, got %q", result.Action)
	}
}

// TestBedrockRuntimePlugin_BlocklistNonMatching verifies non-matching content passes through.
func TestBedrockRuntimePlugin_BlocklistNonMatching(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	// Seed blocklist with term that won't match.
	blocklistKey := "guardrail:000000000000/guardrail-nm/blocklist"
	blocklist, _ := json.Marshal([]string{"badword"})
	if err := setup.state.Put(context.Background(), "bedrock-runtime", blocklistKey, blocklist); err != nil {
		t.Fatalf("seed blocklist: %v", err)
	}

	resp := brRequest(t, setup.server, "guardrail-nm", "1", brPayload("OUTPUT", "This is fine content"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Action != "NONE" {
		t.Errorf("expected NONE, got %q", result.Action)
	}
}

// TestBedrockRuntimePlugin_SourceOutput verifies source=OUTPUT also works.
func TestBedrockRuntimePlugin_SourceOutput(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	resp := brRequest(t, setup.server, "guardrail-out", "DRAFT", brPayload("OUTPUT", "assistant response here"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Action  string              `json:"action"`
		Outputs []map[string]string `json:"outputs"`
	}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Action != "NONE" {
		t.Errorf("expected NONE, got %q", result.Action)
	}
	if len(result.Outputs) < 1 || result.Outputs[0]["text"] != "assistant response here" {
		t.Errorf("expected echoed output text, got %+v", result.Outputs)
	}
}

// TestBedrockRuntimePlugin_InvokeModel_DefaultResponse verifies InvokeModel returns a
// canned response when no seeded response is configured.
func TestBedrockRuntimePlugin_InvokeModel_DefaultResponse(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	resp := brInvokeRequest(t, setup.server, "anthropic.claude-sonnet-4-20250514-v1:0", map[string]any{
		"messages": []map[string]string{{"role": "user", "content": "Hello"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["type"] != "message" {
		t.Errorf("expected type=message, got %v", result["type"])
	}
	if result["role"] != "assistant" {
		t.Errorf("expected role=assistant, got %v", result["role"])
	}
	if result["model"] != "anthropic.claude-sonnet-4-20250514-v1:0" {
		t.Errorf("expected model echoed back, got %v", result["model"])
	}
	content, _ := result["content"].([]interface{})
	if len(content) < 1 {
		t.Fatal("expected at least 1 content item")
	}
	item, _ := content[0].(map[string]interface{})
	if item["text"] == "" {
		t.Error("expected non-empty text in content")
	}
}

// TestBedrockRuntimePlugin_InvokeModel_SeededResponse verifies that seeded responses
// are returned and can be cleared.
func TestBedrockRuntimePlugin_InvokeModel_SeededResponse(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	modelID := "anthropic.claude-sonnet-4-20250514-v1:0"

	// Seed a response for the specific model.
	seedPayload, _ := json.Marshal(map[string]any{
		"modelId": modelID,
		"body":    map[string]string{"completion": "SELECT * FROM table"},
	})
	seedReq, _ := http.NewRequest(http.MethodPost, setup.server.URL+"/v1/bedrock-runtime/responses", bytes.NewReader(seedPayload))
	seedReq.Header.Set("Content-Type", "application/json")
	seedResp, err := http.DefaultClient.Do(seedReq)
	if err != nil {
		t.Fatalf("seed POST: %v", err)
	}
	_, _ = io.ReadAll(seedResp.Body)
	_ = seedResp.Body.Close()
	if seedResp.StatusCode != http.StatusOK {
		t.Fatalf("seed POST: got %d", seedResp.StatusCode)
	}

	// InvokeModel should return the seeded response.
	resp := brInvokeRequest(t, setup.server, modelID, map[string]any{})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("invoke: got %d", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["completion"] != "SELECT * FROM table" {
		t.Errorf("expected seeded completion, got %v", result)
	}

	// Clear all seeded responses.
	delReq, _ := http.NewRequest(http.MethodDelete, setup.server.URL+"/v1/bedrock-runtime/responses", nil)
	delResp, _ := http.DefaultClient.Do(delReq)
	_, _ = io.ReadAll(delResp.Body)
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE responses: got %d", delResp.StatusCode)
	}

	// InvokeModel should return default canned response.
	resp2 := brInvokeRequest(t, setup.server, modelID, map[string]any{})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("invoke after clear: got %d", resp2.StatusCode)
	}
	var result2 map[string]interface{}
	if err := json.Unmarshal(brBody(t, resp2), &result2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result2["type"] != "message" {
		t.Errorf("expected default response type=message after clear, got %v", result2["type"])
	}
}

// TestBedrockRuntimePlugin_InvokeModel_WildcardSeed verifies wildcard seeding and
// that exact model matches take priority over wildcard.
func TestBedrockRuntimePlugin_InvokeModel_WildcardSeed(t *testing.T) {
	setup := newBedrockRuntimeTestServer(t)

	// Seed a wildcard response.
	seedWildcard, _ := json.Marshal(map[string]any{
		"modelId": "*",
		"body":    map[string]string{"completion": "wildcard-response"},
	})
	seedReq, _ := http.NewRequest(http.MethodPost, setup.server.URL+"/v1/bedrock-runtime/responses", bytes.NewReader(seedWildcard))
	seedReq.Header.Set("Content-Type", "application/json")
	seedResp, _ := http.DefaultClient.Do(seedReq)
	_, _ = io.ReadAll(seedResp.Body)
	_ = seedResp.Body.Close()

	// Any model should get the wildcard response.
	resp := brInvokeRequest(t, setup.server, "some-model-id", map[string]any{})
	var result map[string]interface{}
	if err := json.Unmarshal(brBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["completion"] != "wildcard-response" {
		t.Errorf("expected wildcard-response, got %v", result)
	}

	// Seed an exact model response — should take priority.
	seedExact, _ := json.Marshal(map[string]any{
		"modelId": "exact-model",
		"body":    map[string]string{"completion": "exact-response"},
	})
	exactReq, _ := http.NewRequest(http.MethodPost, setup.server.URL+"/v1/bedrock-runtime/responses", bytes.NewReader(seedExact))
	exactReq.Header.Set("Content-Type", "application/json")
	exactResp, _ := http.DefaultClient.Do(exactReq)
	_, _ = io.ReadAll(exactResp.Body)
	_ = exactResp.Body.Close()

	// Exact model should get exact response.
	resp2 := brInvokeRequest(t, setup.server, "exact-model", map[string]any{})
	var result2 map[string]interface{}
	if err := json.Unmarshal(brBody(t, resp2), &result2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result2["completion"] != "exact-response" {
		t.Errorf("expected exact-response, got %v", result2)
	}

	// Different model should still get wildcard.
	resp3 := brInvokeRequest(t, setup.server, "other-model", map[string]any{})
	var result3 map[string]interface{}
	if err := json.Unmarshal(brBody(t, resp3), &result3); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result3["completion"] != "wildcard-response" {
		t.Errorf("expected wildcard-response for other model, got %v", result3)
	}
}
