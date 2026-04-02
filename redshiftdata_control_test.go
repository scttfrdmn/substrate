package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// setupRedshiftDataWithSharedState creates a plugin and a server that share the same
// StateManager so that HTTP control-plane writes are visible to the plugin.
func setupRedshiftDataWithSharedState(t *testing.T) (*substrate.RedshiftDataPlugin, *substrate.Server, substrate.StateManager) {
	t.Helper()

	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)

	p := &substrate.RedshiftDataPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("RedshiftDataPlugin.Initialize: %v", err)
	}

	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	registry.Register(p)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	return p, srv, state
}

// --- Plugin-level state tests (direct state manipulation) -----------------

func TestRedshiftDataPlugin_StateSeededResult(t *testing.T) {
	p, _, state := setupRedshiftDataWithSharedState(t)
	ctx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	strVal := "hello"
	result := &substrate.RedshiftDataResult{
		ColumnMetadata: []substrate.RedshiftDataColumnMetadata{
			{Name: "greeting", TypeName: "varchar"},
		},
		Records: [][]substrate.RedshiftDataField{
			{{StringValue: &strVal}},
		},
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if err := state.Put(context.Background(), "redshift-data-ctrl", "result:SELECT greeting FROM greetings", data); err != nil {
		t.Fatalf("state.Put: %v", err)
	}

	// Execute a statement with the exact SQL.
	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"WorkgroupName": "wg",
		"Database":      "db",
		"Sql":           "SELECT greeting FROM greetings",
	}))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("ExecuteStatement: err=%v status=%d", err, resp.StatusCode)
	}
	var execResult struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(resp.Body, &execResult)

	resp, err = p.HandleRequest(ctx, redshiftDataRequest(t, "GetStatementResult", map[string]any{
		"Id": execResult.ID,
	}))
	if err != nil {
		t.Fatalf("GetStatementResult: %v", err)
	}
	var getResult struct {
		TotalNumRows int `json:"TotalNumRows"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if getResult.TotalNumRows != 1 {
		t.Errorf("want TotalNumRows=1, got %d", getResult.TotalNumRows)
	}
}

func TestRedshiftDataPlugin_StateWildcardResult(t *testing.T) {
	p, _, state := setupRedshiftDataWithSharedState(t)
	ctx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	strVal := "wild"
	result := &substrate.RedshiftDataResult{
		ColumnMetadata: []substrate.RedshiftDataColumnMetadata{
			{Name: "v", TypeName: "varchar"},
		},
		Records: [][]substrate.RedshiftDataField{
			{{StringValue: &strVal}},
		},
	}
	data, _ := json.Marshal(result)
	if err := state.Put(context.Background(), "redshift-data-ctrl", "result:*", data); err != nil {
		t.Fatalf("state.Put: %v", err)
	}

	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"WorkgroupName": "wg",
		"Database":      "db",
		"Sql":           "SELECT anything FROM anywhere",
	}))
	if err != nil {
		t.Fatalf("ExecuteStatement: %v", err)
	}
	var execResult struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(resp.Body, &execResult)

	resp, err = p.HandleRequest(ctx, redshiftDataRequest(t, "GetStatementResult", map[string]any{
		"Id": execResult.ID,
	}))
	if err != nil {
		t.Fatalf("GetStatementResult: %v", err)
	}
	var getResult struct {
		TotalNumRows int `json:"TotalNumRows"`
	}
	_ = json.Unmarshal(resp.Body, &getResult)
	if getResult.TotalNumRows != 1 {
		t.Errorf("want TotalNumRows=1 from wildcard, got %d", getResult.TotalNumRows)
	}
}

func TestRedshiftDataPlugin_StateStatusFailed(t *testing.T) {
	p, _, state := setupRedshiftDataWithSharedState(t)
	ctx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	if err := state.Put(context.Background(), "redshift-data-ctrl", "status", []byte("FAILED")); err != nil {
		t.Fatalf("state.Put status: %v", err)
	}
	if err := state.Put(context.Background(), "redshift-data-ctrl", "error_message", []byte("query timed out")); err != nil {
		t.Fatalf("state.Put error_message: %v", err)
	}

	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"WorkgroupName": "wg",
		"Database":      "db",
		"Sql":           "SELECT 1",
	}))
	if err != nil {
		t.Fatalf("ExecuteStatement: %v", err)
	}
	var execResult struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(resp.Body, &execResult)

	resp, err = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{
		"Id": execResult.ID,
	}))
	if err != nil {
		t.Fatalf("DescribeStatement: %v", err)
	}
	var descResult struct {
		Status string `json:"Status"`
		Error  string `json:"Error"`
	}
	if err := json.Unmarshal(resp.Body, &descResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if descResult.Status != "FAILED" {
		t.Errorf("want Status=FAILED, got %q", descResult.Status)
	}
	if descResult.Error != "query timed out" {
		t.Errorf("want Error=%q, got %q", "query timed out", descResult.Error)
	}
}

func TestRedshiftDataPlugin_StateStatusClearedReturnsFinished(t *testing.T) {
	p, _, state := setupRedshiftDataWithSharedState(t)
	ctx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	// Seed FAILED.
	if err := state.Put(context.Background(), "redshift-data-ctrl", "status", []byte("FAILED")); err != nil {
		t.Fatalf("state.Put status: %v", err)
	}

	// Execute one statement — should be FAILED.
	resp, _ := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 1", "WorkgroupName": "wg", "Database": "db",
	}))
	var r1 struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(resp.Body, &r1)
	resp, _ = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{"Id": r1.ID}))
	var d1 struct {
		Status string `json:"Status"`
	}
	_ = json.Unmarshal(resp.Body, &d1)
	if d1.Status != "FAILED" {
		t.Fatalf("want FAILED, got %q", d1.Status)
	}

	// Clear status override.
	if err := state.Delete(context.Background(), "redshift-data-ctrl", "status"); err != nil {
		t.Fatalf("state.Delete status: %v", err)
	}

	// Execute another statement — should now be FINISHED.
	resp, _ = p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 2", "WorkgroupName": "wg", "Database": "db",
	}))
	var r2 struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(resp.Body, &r2)
	resp, _ = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{"Id": r2.ID}))
	var d2 struct {
		Status string `json:"Status"`
	}
	_ = json.Unmarshal(resp.Body, &d2)
	if d2.Status != "FINISHED" {
		t.Errorf("want FINISHED after clearing override, got %q", d2.Status)
	}
}

func TestRedshiftDataPlugin_InMemoryTakesPrecedenceOverState(t *testing.T) {
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)

	specificSQL := "SELECT specific FROM table"
	specificStr := "in-memory"
	wildcardStr := "from-state"

	p := &substrate.RedshiftDataPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"results": map[string]*substrate.RedshiftDataResult{
				specificSQL: {
					ColumnMetadata: []substrate.RedshiftDataColumnMetadata{{Name: "v", TypeName: "varchar"}},
					Records:        [][]substrate.RedshiftDataField{{{StringValue: &specificStr}}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	// Seed wildcard result in state.
	wildcardResult := &substrate.RedshiftDataResult{
		ColumnMetadata: []substrate.RedshiftDataColumnMetadata{{Name: "v", TypeName: "varchar"}},
		Records:        [][]substrate.RedshiftDataField{{{StringValue: &wildcardStr}}},
	}
	data, _ := json.Marshal(wildcardResult)
	_ = state.Put(context.Background(), "redshift-data-ctrl", "result:*", data)

	ctx := &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	// Execute with the specific SQL → in-memory result should win.
	resp, _ := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql": specificSQL, "WorkgroupName": "wg", "Database": "db",
	}))
	var execResult struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(resp.Body, &execResult)

	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "GetStatementResult", map[string]any{
		"Id": execResult.ID,
	}))
	if err != nil {
		t.Fatalf("GetStatementResult: %v", err)
	}
	var getResult struct {
		Records [][]map[string]interface{} `json:"Records"`
	}
	_ = json.Unmarshal(resp.Body, &getResult)
	if len(getResult.Records) != 1 {
		t.Fatalf("want 1 row, got %d", len(getResult.Records))
	}
	if got, ok := getResult.Records[0][0]["stringValue"].(string); !ok || got != "in-memory" {
		t.Errorf("want in-memory value, got %v", getResult.Records[0][0])
	}
}

// --- HTTP handler tests ---------------------------------------------------

func TestHandleRedshiftDataSeedResult(t *testing.T) {
	_, srv, state := setupRedshiftDataWithSharedState(t)

	body := `{"sql":"SELECT foo FROM bar","result":{"ColumnMetadata":[{"name":"foo","typeName":"varchar"}],"Records":[]}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/results", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body)
	}

	// Verify state was written.
	data, err := state.Get(context.Background(), "redshift-data-ctrl", "result:SELECT foo FROM bar")
	if err != nil {
		t.Fatalf("state.Get: %v", err)
	}
	if data == nil {
		t.Fatal("expected state entry to be written")
	}
}

func TestHandleRedshiftDataSeedResult_DefaultsToWildcard(t *testing.T) {
	_, srv, state := setupRedshiftDataWithSharedState(t)

	// Omit sql field — should default to "*".
	body := `{"result":{"ColumnMetadata":[],"Records":[]}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/results", strings.NewReader(body))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body)
	}
	data, _ := state.Get(context.Background(), "redshift-data-ctrl", "result:*")
	if data == nil {
		t.Fatal("expected wildcard state entry to be written")
	}
}

func TestHandleRedshiftDataClearResults(t *testing.T) {
	_, srv, state := setupRedshiftDataWithSharedState(t)

	// Seed two results directly.
	dummy, _ := json.Marshal(&substrate.RedshiftDataResult{})
	_ = state.Put(context.Background(), "redshift-data-ctrl", "result:SELECT 1", dummy)
	_ = state.Put(context.Background(), "redshift-data-ctrl", "result:SELECT 2", dummy)

	// DELETE without sql param clears all.
	req := httptest.NewRequest(http.MethodDelete, "/v1/redshift-data/results", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body)
	}
	for _, sql := range []string{"SELECT 1", "SELECT 2"} {
		d, _ := state.Get(context.Background(), "redshift-data-ctrl", "result:"+sql)
		if d != nil {
			t.Errorf("expected result for %q to be cleared", sql)
		}
	}
}

func TestHandleRedshiftDataClearResults_SpecificSQL(t *testing.T) {
	_, srv, state := setupRedshiftDataWithSharedState(t)

	dummy, _ := json.Marshal(&substrate.RedshiftDataResult{})
	_ = state.Put(context.Background(), "redshift-data-ctrl", "result:SELECT 1", dummy)
	_ = state.Put(context.Background(), "redshift-data-ctrl", "result:SELECT 2", dummy)

	req := httptest.NewRequest(http.MethodDelete, "/v1/redshift-data/results?sql=SELECT+1", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body)
	}
	d1, _ := state.Get(context.Background(), "redshift-data-ctrl", "result:SELECT 1")
	if d1 != nil {
		t.Error("expected result:SELECT 1 to be cleared")
	}
	d2, _ := state.Get(context.Background(), "redshift-data-ctrl", "result:SELECT 2")
	if d2 == nil {
		t.Error("expected result:SELECT 2 to remain")
	}
}

func TestHandleRedshiftDataSetStatus(t *testing.T) {
	_, srv, state := setupRedshiftDataWithSharedState(t)

	body := `{"status":"FAILED","errorMessage":"simulated error"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body)
	}

	// Verify state persisted.
	statusData, _ := state.Get(context.Background(), "redshift-data-ctrl", "status")
	if string(statusData) != "FAILED" {
		t.Errorf("want status=FAILED, got %q", string(statusData))
	}
	errData, _ := state.Get(context.Background(), "redshift-data-ctrl", "error_message")
	if string(errData) != "simulated error" {
		t.Errorf("want error_message=%q, got %q", "simulated error", string(errData))
	}
}

func TestHandleRedshiftDataSetStatus_InvalidStatus(t *testing.T) {
	_, srv, _ := setupRedshiftDataWithSharedState(t)

	body := `{"status":"INVALID"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/status", strings.NewReader(body))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
}

// --- End-to-end: handler sets status, plugin reads it --------------------

func TestHandleRedshiftDataStatus_EndToEnd(t *testing.T) {
	_, srv, _ := setupRedshiftDataWithSharedState(t)

	// Set FAILED status via HTTP.
	statusBody := `{"status":"FAILED","errorMessage":"e2e error"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/status", strings.NewReader(statusBody))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST /v1/redshift-data/status: %d %s", rr.Code, rr.Body)
	}

	// Execute a statement via the same server.
	execBody, _ := json.Marshal(map[string]any{
		"WorkgroupName": "wg",
		"Database":      "db",
		"Sql":           "SELECT 1",
	})
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(execBody)))
	req.Header.Set("X-Amz-Target", "RedshiftData_20191217.ExecuteStatement")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement: %d %s", rr.Code, rr.Body)
	}
	var execResult struct {
		ID string `json:"Id"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &execResult)

	// DescribeStatement — expect FAILED.
	descBody, _ := json.Marshal(map[string]any{"Id": execResult.ID})
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(descBody)))
	req.Header.Set("X-Amz-Target", "RedshiftData_20191217.DescribeStatement")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("DescribeStatement: %d %s", rr.Code, rr.Body)
	}
	var descResult struct {
		Status string `json:"Status"`
		Error  string `json:"Error"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &descResult); err != nil {
		t.Fatalf("unmarshal DescribeStatement: %v", err)
	}
	if descResult.Status != "FAILED" {
		t.Errorf("want Status=FAILED, got %q", descResult.Status)
	}
	if descResult.Error != "e2e error" {
		t.Errorf("want Error=%q, got %q", "e2e error", descResult.Error)
	}
}
