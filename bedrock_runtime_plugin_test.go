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
