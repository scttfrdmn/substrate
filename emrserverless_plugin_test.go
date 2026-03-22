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

// newEMRServerlessTestServer builds a minimal server with the EMRServerlessPlugin registered.
func newEMRServerlessTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.EMRServerlessPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize emrserverless plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func emrRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal emr body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build emr request: %v", err)
	}
	req.Host = "emrserverless.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do emr request: %v", err)
	}
	return resp
}

func emrBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read emr body: %v", err)
	}
	return b
}

// TestEMRServerlessPlugin_CreateGetDeleteApp verifies application lifecycle.
func TestEMRServerlessPlugin_CreateGetDeleteApp(t *testing.T) {
	ts := newEMRServerlessTestServer(t)

	// CreateApplication
	resp := emrRequest(t, ts, http.MethodPost, "/applications", map[string]string{
		"name":         "my-spark-app",
		"type":         "SPARK",
		"releaseLabel": "emr-6.9.0",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("createApplication: expected 200, got %d; body: %s", resp.StatusCode, emrBody(t, resp))
	}
	var createResult struct {
		ApplicationId string `json:"applicationId"`
		Arn           string `json:"arn"`
	}
	if err := json.Unmarshal(emrBody(t, resp), &createResult); err != nil {
		t.Fatalf("decode createApplication: %v", err)
	}
	if createResult.ApplicationId == "" {
		t.Fatal("expected non-empty applicationId")
	}
	appID := createResult.ApplicationId

	// GetApplication
	resp2 := emrRequest(t, ts, http.MethodGet, "/applications/"+appID, nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("getApplication: expected 200, got %d", resp2.StatusCode)
	}
	var getResult struct {
		Application struct {
			ApplicationId string `json:"applicationId"`
			State         string `json:"state"`
		} `json:"application"`
	}
	if err := json.Unmarshal(emrBody(t, resp2), &getResult); err != nil {
		t.Fatalf("decode getApplication: %v", err)
	}
	if getResult.Application.ApplicationId != appID {
		t.Errorf("expected %q, got %q", appID, getResult.Application.ApplicationId)
	}
	if getResult.Application.State != "CREATED" {
		t.Errorf("expected CREATED, got %q", getResult.Application.State)
	}

	// DeleteApplication
	resp3 := emrRequest(t, ts, http.MethodDelete, "/applications/"+appID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("deleteApplication: expected 200, got %d", resp3.StatusCode)
	}
	emrBody(t, resp3)

	// GetApplication should now 404
	resp4 := emrRequest(t, ts, http.MethodGet, "/applications/"+appID, nil)
	if resp4.StatusCode != http.StatusNotFound {
		t.Fatalf("getApplication after delete: expected 404, got %d", resp4.StatusCode)
	}
	emrBody(t, resp4)
}

// TestEMRServerlessPlugin_StartGetCancelJobRun verifies job run lifecycle.
func TestEMRServerlessPlugin_StartGetCancelJobRun(t *testing.T) {
	ts := newEMRServerlessTestServer(t)

	// Create application first
	resp := emrRequest(t, ts, http.MethodPost, "/applications", map[string]string{
		"name": "app1",
		"type": "SPARK",
	})
	var cr struct {
		ApplicationId string `json:"applicationId"`
	}
	if err := json.Unmarshal(emrBody(t, resp), &cr); err != nil {
		t.Fatalf("decode createApplication: %v", err)
	}
	appID := cr.ApplicationId

	// StartJobRun
	resp2 := emrRequest(t, ts, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]string{
		"name": "my-run",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("startJobRun: expected 200, got %d; body: %s", resp2.StatusCode, emrBody(t, resp2))
	}
	var startResult struct {
		JobRunId string `json:"jobRunId"`
	}
	if err := json.Unmarshal(emrBody(t, resp2), &startResult); err != nil {
		t.Fatalf("decode startJobRun: %v", err)
	}
	if startResult.JobRunId == "" {
		t.Fatal("expected non-empty jobRunId")
	}
	runID := startResult.JobRunId

	// GetJobRun
	resp3 := emrRequest(t, ts, http.MethodGet, "/applications/"+appID+"/jobruns/"+runID, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("getJobRun: expected 200, got %d", resp3.StatusCode)
	}
	var getResult struct {
		JobRun struct {
			State string `json:"state"`
		} `json:"jobRun"`
	}
	if err := json.Unmarshal(emrBody(t, resp3), &getResult); err != nil {
		t.Fatalf("decode getJobRun: %v", err)
	}
	if getResult.JobRun.State != "SUCCESS" {
		t.Errorf("expected SUCCESS, got %q", getResult.JobRun.State)
	}

	// CancelJobRun
	resp4 := emrRequest(t, ts, http.MethodDelete, "/applications/"+appID+"/jobruns/"+runID, nil)
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("cancelJobRun: expected 200, got %d", resp4.StatusCode)
	}
	emrBody(t, resp4)
}

// TestEMRServerlessPlugin_ListJobRuns verifies job run listing per application.
func TestEMRServerlessPlugin_ListJobRuns(t *testing.T) {
	ts := newEMRServerlessTestServer(t)

	// Create application
	resp := emrRequest(t, ts, http.MethodPost, "/applications", map[string]string{
		"name": "app2",
		"type": "HIVE",
	})
	var cr struct {
		ApplicationId string `json:"applicationId"`
	}
	if err := json.Unmarshal(emrBody(t, resp), &cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	appID := cr.ApplicationId

	// Start 3 runs
	for i := 0; i < 3; i++ {
		r := emrRequest(t, ts, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]string{"name": "run"})
		emrBody(t, r)
	}

	// ListJobRuns
	resp2 := emrRequest(t, ts, http.MethodGet, "/applications/"+appID+"/jobruns", nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("listJobRuns: expected 200, got %d", resp2.StatusCode)
	}
	var result struct {
		JobRuns []struct {
			JobRunId string `json:"jobRunId"`
		} `json:"jobRuns"`
	}
	if err := json.Unmarshal(emrBody(t, resp2), &result); err != nil {
		t.Fatalf("decode listJobRuns: %v", err)
	}
	if len(result.JobRuns) != 3 {
		t.Fatalf("expected 3 job runs, got %d", len(result.JobRuns))
	}
}
