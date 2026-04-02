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

// newSageMakerTestServer builds a minimal server with the SageMakerPlugin registered.
func newSageMakerTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.SageMakerPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize sagemaker plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func sagemakerRequest(t *testing.T, ts *httptest.Server, operation string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal sagemaker body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build sagemaker request: %v", err)
	}
	req.Host = "sagemaker.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SageMaker."+operation)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do sagemaker request: %v", err)
	}
	return resp
}

func sagemakerBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read sagemaker body: %v", err)
	}
	return b
}

// TestSageMakerPlugin_CreateDescribeApp verifies Studio app create and describe.
func TestSageMakerPlugin_CreateDescribeApp(t *testing.T) {
	ts := newSageMakerTestServer(t)

	// CreateApp
	resp := sagemakerRequest(t, ts, "CreateApp", map[string]string{
		"AppName":         "my-app",
		"AppType":         "JupyterServer",
		"DomainId":        "d-abc123",
		"UserProfileName": "user1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("createApp: expected 200, got %d; body: %s", resp.StatusCode, sagemakerBody(t, resp))
	}
	var createResult struct {
		AppArn string `json:"AppArn"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp), &createResult); err != nil {
		t.Fatalf("decode createApp: %v", err)
	}
	if createResult.AppArn == "" {
		t.Fatal("expected non-empty AppArn")
	}

	// DescribeApp
	resp2 := sagemakerRequest(t, ts, "DescribeApp", map[string]string{
		"AppName":         "my-app",
		"AppType":         "JupyterServer",
		"DomainId":        "d-abc123",
		"UserProfileName": "user1",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("describeApp: expected 200, got %d", resp2.StatusCode)
	}
	var descResult struct {
		AppName string `json:"AppName"`
		Status  string `json:"Status"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp2), &descResult); err != nil {
		t.Fatalf("decode describeApp: %v", err)
	}
	if descResult.AppName != "my-app" {
		t.Errorf("expected my-app, got %q", descResult.AppName)
	}
	if descResult.Status != "InService" {
		t.Errorf("expected InService, got %q", descResult.Status)
	}
}

// TestSageMakerPlugin_TrainingJob_CreateDescribeStop verifies training job lifecycle.
func TestSageMakerPlugin_TrainingJob_CreateDescribeStop(t *testing.T) {
	ts := newSageMakerTestServer(t)

	// CreateTrainingJob
	resp := sagemakerRequest(t, ts, "CreateTrainingJob", map[string]string{
		"TrainingJobName": "my-training-job",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("createTrainingJob: expected 200, got %d; body: %s", resp.StatusCode, sagemakerBody(t, resp))
	}
	var createResult struct {
		TrainingJobArn string `json:"TrainingJobArn"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp), &createResult); err != nil {
		t.Fatalf("decode createTrainingJob: %v", err)
	}
	if createResult.TrainingJobArn == "" {
		t.Fatal("expected non-empty TrainingJobArn")
	}

	// DescribeTrainingJob — should be Completed
	resp2 := sagemakerRequest(t, ts, "DescribeTrainingJob", map[string]string{
		"TrainingJobName": "my-training-job",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("describeTrainingJob: expected 200, got %d", resp2.StatusCode)
	}
	var descResult struct {
		TrainingJobStatus string  `json:"TrainingJobStatus"`
		CreationTime      float64 `json:"CreationTime"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp2), &descResult); err != nil {
		t.Fatalf("decode describeTrainingJob: %v", err)
	}
	if descResult.TrainingJobStatus != "Completed" {
		t.Errorf("expected Completed, got %q", descResult.TrainingJobStatus)
	}
	if descResult.CreationTime == 0 {
		t.Error("expected non-zero CreationTime")
	}

	// StopTrainingJob
	resp3 := sagemakerRequest(t, ts, "StopTrainingJob", map[string]string{
		"TrainingJobName": "my-training-job",
	})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("stopTrainingJob: expected 200, got %d", resp3.StatusCode)
	}
	sagemakerBody(t, resp3)

	// Verify Stopped status
	resp4 := sagemakerRequest(t, ts, "DescribeTrainingJob", map[string]string{
		"TrainingJobName": "my-training-job",
	})
	var afterStop struct {
		TrainingJobStatus string `json:"TrainingJobStatus"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp4), &afterStop); err != nil {
		t.Fatalf("decode after-stop: %v", err)
	}
	if afterStop.TrainingJobStatus != "Stopped" {
		t.Errorf("expected Stopped, got %q", afterStop.TrainingJobStatus)
	}
}

// TestSageMakerPlugin_ListDomains_ListApps verifies domain and app listing.
func TestSageMakerPlugin_ListDomains_ListApps(t *testing.T) {
	ts := newSageMakerTestServer(t)

	// ListDomains returns empty list
	resp := sagemakerRequest(t, ts, "ListDomains", map[string]interface{}{})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listDomains: expected 200, got %d", resp.StatusCode)
	}
	var domainsResult struct {
		Domains []interface{} `json:"Domains"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp), &domainsResult); err != nil {
		t.Fatalf("decode listDomains: %v", err)
	}
	if len(domainsResult.Domains) != 0 {
		t.Errorf("expected empty domains, got %d", len(domainsResult.Domains))
	}

	// Create two apps, list with filter
	for _, name := range []string{"app-a", "app-b"} {
		r := sagemakerRequest(t, ts, "CreateApp", map[string]string{
			"AppName":         name,
			"AppType":         "KernelGateway",
			"DomainId":        "d-domain1",
			"UserProfileName": "user1",
		})
		sagemakerBody(t, r)
	}

	// ListApps — all
	resp2 := sagemakerRequest(t, ts, "ListApps", map[string]interface{}{})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("listApps: expected 200, got %d", resp2.StatusCode)
	}
	var appsResult struct {
		Apps []struct {
			AppName string `json:"AppName"`
		} `json:"Apps"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp2), &appsResult); err != nil {
		t.Fatalf("decode listApps: %v", err)
	}
	if len(appsResult.Apps) != 2 {
		t.Fatalf("expected 2 apps, got %d", len(appsResult.Apps))
	}
}

// TestSageMakerPlugin_CreatePresignedDomainUrl verifies stub URL is returned.
func TestSageMakerPlugin_CreatePresignedDomainUrl(t *testing.T) {
	ts := newSageMakerTestServer(t)

	resp := sagemakerRequest(t, ts, "CreatePresignedDomainUrl", map[string]string{
		"DomainId":        "d-abc",
		"UserProfileName": "user1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("createPresignedDomainUrl: expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		AuthorizedUrl string `json:"AuthorizedUrl"`
	}
	if err := json.Unmarshal(sagemakerBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.AuthorizedUrl == "" {
		t.Error("expected non-empty AuthorizedUrl")
	}
}

// TestSageMakerPlugin_DeleteApp verifies DeleteApp removes the app.
func TestSageMakerPlugin_DeleteApp(t *testing.T) {
	ts := newSageMakerTestServer(t)

	// Create an app first.
	createResp := sagemakerRequest(t, ts, "CreateApp", map[string]string{
		"AppName":         "test-app",
		"AppType":         "KernelGateway",
		"DomainId":        "d-del1",
		"UserProfileName": "user-del",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("createApp: expected 200, got %d", createResp.StatusCode)
	}
	sagemakerBody(t, createResp)

	// Delete the app.
	delResp := sagemakerRequest(t, ts, "DeleteApp", map[string]string{
		"AppName":         "test-app",
		"AppType":         "KernelGateway",
		"DomainId":        "d-del1",
		"UserProfileName": "user-del",
	})
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("deleteApp: expected 200, got %d", delResp.StatusCode)
	}
	sagemakerBody(t, delResp)

	// Describe after delete → not found.
	descResp := sagemakerRequest(t, ts, "DescribeApp", map[string]string{
		"AppName":         "test-app",
		"AppType":         "KernelGateway",
		"DomainId":        "d-del1",
		"UserProfileName": "user-del",
	})
	if descResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("describeApp after delete: expected 400, got %d", descResp.StatusCode)
	}
	sagemakerBody(t, descResp)
}

// TestSageMakerPlugin_ListTrainingJobs verifies listing after creation.
func TestSageMakerPlugin_ListTrainingJobs(t *testing.T) {
	ts := newSageMakerTestServer(t)

	// Create two training jobs.
	for _, name := range []string{"job-list-1", "job-list-2"} {
		r := sagemakerRequest(t, ts, "CreateTrainingJob", map[string]interface{}{
			"TrainingJobName": name,
			"AlgorithmSpecification": map[string]string{
				"TrainingInputMode": "File",
				"TrainingImage":     "123456789012.dkr.ecr.us-east-1.amazonaws.com/algo:latest",
			},
			"OutputDataConfig": map[string]string{"S3OutputPath": "s3://bucket/output"},
			"ResourceConfig": map[string]interface{}{
				"InstanceType":  "ml.m5.large",
				"InstanceCount": 1,
				"VolumeSizeInGB": 10,
			},
			"RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
			"StoppingCondition": map[string]int{"MaxRuntimeInSeconds": 3600},
		})
		if r.StatusCode != http.StatusOK {
			t.Fatalf("createTrainingJob %s: expected 200, got %d", name, r.StatusCode)
		}
		sagemakerBody(t, r)
	}

	// List training jobs.
	listResp := sagemakerRequest(t, ts, "ListTrainingJobs", map[string]interface{}{})
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("listTrainingJobs: expected 200, got %d", listResp.StatusCode)
	}
	var result struct {
		TrainingJobSummaries []struct {
			TrainingJobName string `json:"TrainingJobName"`
		} `json:"TrainingJobSummaries"`
	}
	if err := json.Unmarshal(sagemakerBody(t, listResp), &result); err != nil {
		t.Fatalf("decode listTrainingJobs: %v", err)
	}
	if len(result.TrainingJobSummaries) != 2 {
		t.Fatalf("expected 2 training jobs, got %d", len(result.TrainingJobSummaries))
	}
}
