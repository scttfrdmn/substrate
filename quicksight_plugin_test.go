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

// newQuickSightTestServer builds a minimal server with the QuickSightPlugin registered.
func newQuickSightTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.QuickSightPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize quicksight plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func qsRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal qs body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build qs request: %v", err)
	}
	req.Host = "quicksight.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do qs request: %v", err)
	}
	return resp
}

func qsBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read qs body: %v", err)
	}
	return b
}

const qsAccountID = "123456789012"

// TestQuickSightPlugin_CreateDataSource_ReturnsARN verifies CreateDataSource returns ARN and CREATION_SUCCESSFUL.
func TestQuickSightPlugin_CreateDataSource_ReturnsARN(t *testing.T) {
	ts := newQuickSightTestServer(t)

	resp := qsRequest(t, ts, http.MethodPost,
		"/accounts/"+qsAccountID+"/datasources",
		map[string]string{
			"DataSourceId": "ds-001",
			"Name":         "My S3 Source",
			"Type":         "S3",
		})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", resp.StatusCode, qsBody(t, resp))
	}
	var result struct {
		DataSourceId   string `json:"DataSourceId"`
		Arn            string `json:"Arn"`
		CreationStatus string `json:"CreationStatus"`
	}
	if err := json.Unmarshal(qsBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.DataSourceId != "ds-001" {
		t.Errorf("expected ds-001, got %q", result.DataSourceId)
	}
	if result.Arn == "" {
		t.Error("expected non-empty Arn")
	}
	if result.CreationStatus != "CREATION_SUCCESSFUL" {
		t.Errorf("expected CREATION_SUCCESSFUL, got %q", result.CreationStatus)
	}
}

// TestQuickSightPlugin_DescribeDataSource_Existing returns stored resource.
func TestQuickSightPlugin_DescribeDataSource_Existing(t *testing.T) {
	ts := newQuickSightTestServer(t)

	// Create first
	r := qsRequest(t, ts, http.MethodPost, "/accounts/"+qsAccountID+"/datasources",
		map[string]string{"DataSourceId": "ds-002", "Name": "Src2", "Type": "ATHENA"})
	qsBody(t, r)

	// Describe
	resp := qsRequest(t, ts, http.MethodGet, "/accounts/"+qsAccountID+"/datasources/ds-002", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, qsBody(t, resp))
	}
	var result struct {
		DataSource struct {
			DataSourceId string `json:"DataSourceId"`
			Status       string `json:"Status"`
		} `json:"DataSource"`
	}
	if err := json.Unmarshal(qsBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.DataSource.DataSourceId != "ds-002" {
		t.Errorf("expected ds-002, got %q", result.DataSource.DataSourceId)
	}
	if result.DataSource.Status != "CREATION_SUCCESSFUL" {
		t.Errorf("expected CREATION_SUCCESSFUL, got %q", result.DataSource.Status)
	}
}

// TestQuickSightPlugin_DescribeDataSource_Missing returns 404.
func TestQuickSightPlugin_DescribeDataSource_Missing(t *testing.T) {
	ts := newQuickSightTestServer(t)

	resp := qsRequest(t, ts, http.MethodGet, "/accounts/"+qsAccountID+"/datasources/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	qsBody(t, resp)
}

// TestQuickSightPlugin_CreateDataSet returns ARN and IngestionId.
func TestQuickSightPlugin_CreateDataSet(t *testing.T) {
	ts := newQuickSightTestServer(t)

	resp := qsRequest(t, ts, http.MethodPost,
		"/accounts/"+qsAccountID+"/datasets",
		map[string]string{
			"DataSetId": "set-001",
			"Name":      "My Dataset",
		})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", resp.StatusCode, qsBody(t, resp))
	}
	var result struct {
		DataSetId   string `json:"DataSetId"`
		Arn         string `json:"Arn"`
		IngestionId string `json:"IngestionId"`
	}
	if err := json.Unmarshal(qsBody(t, resp), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.DataSetId != "set-001" {
		t.Errorf("expected set-001, got %q", result.DataSetId)
	}
	if result.IngestionId == "" {
		t.Error("expected non-empty IngestionId")
	}
}

// TestQuickSightPlugin_DescribeIngestion_Existing returns COMPLETED status.
func TestQuickSightPlugin_DescribeIngestion_Existing(t *testing.T) {
	ts := newQuickSightTestServer(t)

	// CreateDataSet
	r := qsRequest(t, ts, http.MethodPost, "/accounts/"+qsAccountID+"/datasets",
		map[string]string{"DataSetId": "set-002", "Name": "DS2"})
	var cr struct {
		IngestionId string `json:"IngestionId"`
	}
	if err := json.Unmarshal(qsBody(t, r), &cr); err != nil {
		t.Fatalf("decode createDataSet: %v", err)
	}

	// DescribeIngestion
	resp := qsRequest(t, ts, http.MethodGet,
		"/accounts/"+qsAccountID+"/datasets/set-002/ingestions/"+cr.IngestionId, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, qsBody(t, resp))
	}
	var result struct {
		Ingestion struct {
			IngestionId     string `json:"IngestionId"`
			IngestionStatus string `json:"IngestionStatus"`
			RowInfo         struct {
				RowsIngested int `json:"RowsIngested"`
			} `json:"RowInfo"`
		} `json:"Ingestion"`
	}
	if err := json.Unmarshal(qsBody(t, resp), &result); err != nil {
		t.Fatalf("decode describeIngestion: %v", err)
	}
	if result.Ingestion.IngestionStatus != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %q", result.Ingestion.IngestionStatus)
	}
	if result.Ingestion.RowInfo.RowsIngested != 1000 {
		t.Errorf("expected 1000 rows, got %d", result.Ingestion.RowInfo.RowsIngested)
	}
}

// TestQuickSightPlugin_DescribeIngestion_MissingDataSet returns 404.
func TestQuickSightPlugin_DescribeIngestion_MissingDataSet(t *testing.T) {
	ts := newQuickSightTestServer(t)

	resp := qsRequest(t, ts, http.MethodGet,
		"/accounts/"+qsAccountID+"/datasets/no-such-set/ingestions/ing-001", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	qsBody(t, resp)
}
