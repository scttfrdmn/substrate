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

// newAthenaTestServer builds a minimal server with the AthenaPlugin registered.
func newAthenaTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.AthenaPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize athena plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func athenaRequest(t *testing.T, ts *httptest.Server, operation string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal athena body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build athena request: %v", err)
	}
	req.Host = "athena.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonAthena."+operation)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do athena request: %v", err)
	}
	return resp
}

func athenaBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read athena body: %v", err)
	}
	return b
}

// TestAthenaPlugin_StartQueryExecution verifies that a query is started and returns a QueryExecutionId.
func TestAthenaPlugin_StartQueryExecution(t *testing.T) {
	ts := newAthenaTestServer(t)

	resp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT * FROM my_table",
		"WorkGroup":   "primary",
		"ResultConfiguration": map[string]string{
			"OutputLocation": "s3://my-bucket/results/",
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, athenaBody(t, resp))
	}
	var result struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(athenaBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.QueryExecutionId == "" {
		t.Error("expected non-empty QueryExecutionId")
	}
}

// TestAthenaPlugin_GetQueryExecution_Succeeded verifies a started query immediately reports SUCCEEDED.
func TestAthenaPlugin_GetQueryExecution_Succeeded(t *testing.T) {
	ts := newAthenaTestServer(t)

	// Start a query.
	startResp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT 1",
	})
	var started struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(athenaBody(t, startResp), &started); err != nil {
		t.Fatalf("decode start: %v", err)
	}

	// Get its execution status.
	resp := athenaRequest(t, ts, "GetQueryExecution", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, athenaBody(t, resp))
	}
	var result struct {
		QueryExecution struct {
			QueryExecutionId string `json:"QueryExecutionId"`
			Query            string `json:"Query"`
			Status           struct {
				State string `json:"State"`
			} `json:"Status"`
		} `json:"QueryExecution"`
	}
	if err := json.Unmarshal(athenaBody(t, resp), &result); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if result.QueryExecution.QueryExecutionId != started.QueryExecutionId {
		t.Errorf("id mismatch: got %q", result.QueryExecution.QueryExecutionId)
	}
	if result.QueryExecution.Status.State != "SUCCEEDED" {
		t.Errorf("expected SUCCEEDED, got %q", result.QueryExecution.Status.State)
	}
}

// TestAthenaPlugin_GetQueryResults_Empty verifies GetQueryResults returns an empty result set.
func TestAthenaPlugin_GetQueryResults_Empty(t *testing.T) {
	ts := newAthenaTestServer(t)

	// Start a query.
	startResp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT * FROM t",
	})
	var started struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(athenaBody(t, startResp), &started); err != nil {
		t.Fatalf("decode start: %v", err)
	}

	// Fetch results.
	resp := athenaRequest(t, ts, "GetQueryResults", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, athenaBody(t, resp))
	}
	var result struct {
		ResultSet struct {
			Rows []interface{} `json:"Rows"`
		} `json:"ResultSet"`
	}
	if err := json.Unmarshal(athenaBody(t, resp), &result); err != nil {
		t.Fatalf("decode results: %v", err)
	}
	if len(result.ResultSet.Rows) != 0 {
		t.Errorf("expected empty rows, got %d", len(result.ResultSet.Rows))
	}
}

// TestAthenaPlugin_StopQueryExecution_Cancelled verifies StopQueryExecution transitions state to CANCELLED.
func TestAthenaPlugin_StopQueryExecution_Cancelled(t *testing.T) {
	ts := newAthenaTestServer(t)

	// Start a query.
	startResp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT * FROM big_table",
	})
	var started struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(athenaBody(t, startResp), &started); err != nil {
		t.Fatalf("decode start: %v", err)
	}

	// Stop it.
	stopResp := athenaRequest(t, ts, "StopQueryExecution", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	})
	if stopResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", stopResp.StatusCode, athenaBody(t, stopResp))
	}

	// Verify state is CANCELLED.
	getResp := athenaRequest(t, ts, "GetQueryExecution", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	})
	var result struct {
		QueryExecution struct {
			Status struct {
				State string `json:"State"`
			} `json:"Status"`
		} `json:"QueryExecution"`
	}
	if err := json.Unmarshal(athenaBody(t, getResp), &result); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if result.QueryExecution.Status.State != "CANCELLED" {
		t.Errorf("expected CANCELLED, got %q", result.QueryExecution.Status.State)
	}
}

// TestAthenaPlugin_GetQueryExecution_NotFound verifies a 400 error for an unknown query ID.
func TestAthenaPlugin_GetQueryExecution_NotFound(t *testing.T) {
	ts := newAthenaTestServer(t)

	resp := athenaRequest(t, ts, "GetQueryExecution", map[string]string{
		"QueryExecutionId": "00000000-0000-0000-0000-000000000000",
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", resp.StatusCode, athenaBody(t, resp))
	}
}

// TestAthenaPlugin_StartQueryExecution_DefaultWorkGroup verifies default workgroup is "primary".
func TestAthenaPlugin_StartQueryExecution_DefaultWorkGroup(t *testing.T) {
	ts := newAthenaTestServer(t)

	startResp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT 1",
	})
	var started struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(athenaBody(t, startResp), &started); err != nil {
		t.Fatalf("decode: %v", err)
	}

	getResp := athenaRequest(t, ts, "GetQueryExecution", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	})
	var result struct {
		QueryExecution struct {
			WorkGroup string `json:"WorkGroup"`
		} `json:"QueryExecution"`
	}
	if err := json.Unmarshal(athenaBody(t, getResp), &result); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if result.QueryExecution.WorkGroup != "primary" {
		t.Errorf("expected primary, got %q", result.QueryExecution.WorkGroup)
	}
}
