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

// newBatchTestServer builds a minimal server with the BatchPlugin registered.
func newBatchTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.BatchPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize batch plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func batchRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal batch body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build batch request: %v", err)
	}
	req.Host = "batch.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do batch request: %v", err)
	}
	return resp
}

func batchBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read batch body: %v", err)
	}
	return b
}

// TestBatchPlugin_SubmitDescribeTerminateJob verifies full job lifecycle.
func TestBatchPlugin_SubmitDescribeTerminateJob(t *testing.T) {
	ts := newBatchTestServer(t)

	// SubmitJob
	resp := batchRequest(t, ts, http.MethodPost, "/v1/submitjob", map[string]string{
		"jobName":       "test-job",
		"jobQueue":      "arn:aws:batch:us-east-1:000000000000:job-queue/q1",
		"jobDefinition": "arn:aws:batch:us-east-1:000000000000:job-definition/jd1:1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submitJob: expected 200, got %d; body: %s", resp.StatusCode, batchBody(t, resp))
	}
	var submitResult struct {
		JobID   string `json:"jobId"`
		JobName string `json:"jobName"`
	}
	if err := json.Unmarshal(batchBody(t, resp), &submitResult); err != nil {
		t.Fatalf("decode submitJob: %v", err)
	}
	if submitResult.JobID == "" {
		t.Fatal("expected non-empty jobId")
	}
	if submitResult.JobName != "test-job" {
		t.Errorf("expected test-job, got %q", submitResult.JobName)
	}
	jobID := submitResult.JobID

	// DescribeJobs
	resp2 := batchRequest(t, ts, http.MethodPost, "/v1/jobs", map[string]interface{}{"jobs": []string{jobID}})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("describeJobs: expected 200, got %d", resp2.StatusCode)
	}
	var descResult struct {
		Jobs []struct {
			JobID  string `json:"jobId"`
			Status string `json:"status"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(batchBody(t, resp2), &descResult); err != nil {
		t.Fatalf("decode describeJobs: %v", err)
	}
	if len(descResult.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(descResult.Jobs))
	}
	if descResult.Jobs[0].Status != "SUCCEEDED" {
		t.Errorf("expected SUCCEEDED, got %q", descResult.Jobs[0].Status)
	}

	// TerminateJob
	resp3 := batchRequest(t, ts, http.MethodDelete, "/v1/jobs/"+jobID, map[string]string{"reason": "test done"})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("terminateJob: expected 200, got %d", resp3.StatusCode)
	}
	batchBody(t, resp3)

	// Verify FAILED status after termination
	resp4 := batchRequest(t, ts, http.MethodPost, "/v1/jobs", map[string]interface{}{"jobs": []string{jobID}})
	var afterTerminate struct {
		Jobs []struct {
			Status string `json:"status"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(batchBody(t, resp4), &afterTerminate); err != nil {
		t.Fatalf("decode after-terminate: %v", err)
	}
	if len(afterTerminate.Jobs) != 1 || afterTerminate.Jobs[0].Status != "FAILED" {
		t.Errorf("expected FAILED after terminate, got %+v", afterTerminate.Jobs)
	}
}

// TestBatchPlugin_ListJobs verifies that all submitted jobs appear in the list.
func TestBatchPlugin_ListJobs(t *testing.T) {
	ts := newBatchTestServer(t)

	for i := 0; i < 3; i++ {
		resp := batchRequest(t, ts, http.MethodPost, "/v1/submitjob", map[string]string{
			"jobName":       "job",
			"jobQueue":      "q1",
			"jobDefinition": "jd1",
		})
		batchBody(t, resp)
	}

	resp := batchRequest(t, ts, http.MethodGet, "/v1/jobs", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listJobs: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		JobSummaryList []struct {
			JobID string `json:"jobId"`
		} `json:"jobSummaryList"`
	}
	if err := json.Unmarshal(batchBody(t, resp), &result); err != nil {
		t.Fatalf("decode listJobs: %v", err)
	}
	if len(result.JobSummaryList) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(result.JobSummaryList))
	}
}

// TestBatchPlugin_CreateComputeEnvQueueDefinition verifies prerequisite resource creation.
func TestBatchPlugin_CreateComputeEnvQueueDefinition(t *testing.T) {
	ts := newBatchTestServer(t)

	// CreateComputeEnvironment
	resp := batchRequest(t, ts, http.MethodPost, "/v1/computeenvironments", map[string]string{
		"computeEnvironmentName": "ce1",
		"type":                   "MANAGED",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("createComputeEnv: expected 200, got %d", resp.StatusCode)
	}
	batchBody(t, resp)

	// CreateJobQueue
	resp2 := batchRequest(t, ts, http.MethodPost, "/v1/jobqueues", map[string]string{
		"jobQueueName": "q1",
		"state":        "ENABLED",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("createJobQueue: expected 200, got %d", resp2.StatusCode)
	}
	batchBody(t, resp2)

	// RegisterJobDefinition
	resp3 := batchRequest(t, ts, http.MethodPost, "/v1/jobdefinitions", map[string]string{
		"jobDefinitionName": "jd1",
		"type":              "container",
	})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("registerJobDef: expected 200, got %d", resp3.StatusCode)
	}
	var jd struct {
		Revision int `json:"revision"`
	}
	if err := json.Unmarshal(batchBody(t, resp3), &jd); err != nil {
		t.Fatalf("decode registerJobDef: %v", err)
	}
	if jd.Revision != 1 {
		t.Errorf("expected revision 1, got %d", jd.Revision)
	}
}
