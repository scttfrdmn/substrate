package substrate_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newOmicsTestServer builds a minimal server with the OmicsPlugin registered.
func newOmicsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.OmicsPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize omics plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func omicsRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal omics body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build omics request: %v", err)
	}
	req.Host = "omics.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do omics request: %v", err)
	}
	return resp
}

func omicsBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read omics body: %v", err)
	}
	return b
}

// TestOmicsPlugin_StartGetCancelRun verifies full run lifecycle.
func TestOmicsPlugin_StartGetCancelRun(t *testing.T) {
	ts := newOmicsTestServer(t)

	// StartRun
	resp := omicsRequest(t, ts, http.MethodPost, "/run", map[string]string{
		"workflowId":   "wf-12345",
		"workflowType": "PRIVATE",
		"name":         "my-run",
		"roleArn":      "arn:aws:iam::000000000000:role/OmicsRole",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("startRun: expected 201, got %d; body: %s", resp.StatusCode, omicsBody(t, resp))
	}
	var startResult struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(omicsBody(t, resp), &startResult); err != nil {
		t.Fatalf("decode startRun: %v", err)
	}
	if startResult.ID == "" {
		t.Fatal("expected non-empty id")
	}
	runID := startResult.ID

	// GetRun
	resp2 := omicsRequest(t, ts, http.MethodGet, "/run/"+runID, nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("getRun: expected 200, got %d", resp2.StatusCode)
	}
	var getRun struct {
		Id     string `json:"id"`
		Status string `json:"status"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal(omicsBody(t, resp2), &getRun); err != nil {
		t.Fatalf("decode getRun: %v", err)
	}
	if getRun.Id != runID {
		t.Errorf("expected %q, got %q", runID, getRun.Id)
	}
	if getRun.Status != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %q", getRun.Status)
	}
	if getRun.Name != "my-run" {
		t.Errorf("expected my-run, got %q", getRun.Name)
	}

	// CancelRun
	resp3 := omicsRequest(t, ts, http.MethodDelete, "/run/"+runID, nil)
	if resp3.StatusCode != http.StatusNoContent {
		t.Fatalf("cancelRun: expected 204, got %d", resp3.StatusCode)
	}
	omicsBody(t, resp3)

	// GetRun — status should be CANCELED
	resp4 := omicsRequest(t, ts, http.MethodGet, "/run/"+runID, nil)
	var afterCancel struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(omicsBody(t, resp4), &afterCancel); err != nil {
		t.Fatalf("decode after-cancel: %v", err)
	}
	if afterCancel.Status != "CANCELED" {
		t.Errorf("expected CANCELED, got %q", afterCancel.Status)
	}
}

// TestOmicsPlugin_ListRuns verifies that started runs appear in the list.
func TestOmicsPlugin_ListRuns(t *testing.T) {
	ts := newOmicsTestServer(t)

	for i := 0; i < 3; i++ {
		r := omicsRequest(t, ts, http.MethodPost, "/run", map[string]string{
			"workflowId": "wf-1",
			"roleArn":    "arn:aws:iam::000000000000:role/R",
		})
		omicsBody(t, r)
	}

	resp := omicsRequest(t, ts, http.MethodGet, "/run", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listRuns: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Items []struct {
			Id string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(omicsBody(t, resp), &result); err != nil {
		t.Fatalf("decode listRuns: %v", err)
	}
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(result.Items))
	}
}

// TestOmicsPlugin_Error_RunNotFound verifies 404 for unknown run IDs.
func TestOmicsPlugin_Error_RunNotFound(t *testing.T) {
	ts := newOmicsTestServer(t)

	resp := omicsRequest(t, ts, http.MethodGet, "/run/9999999999", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	omicsBody(t, resp)
}
