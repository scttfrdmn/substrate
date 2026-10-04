package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// setupRedshiftDataWithSharedState creates a plugin and a server that share the same
// StateManager so that HTTP control-plane writes are visible to the plugin.
func setupRedshiftDataWithSharedState(t *testing.T) (*emulator.RedshiftDataPlugin, *emulator.Server, emulator.StateManager) {
	t.Helper()

	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(slog.LevelError, false)

	p := &emulator.RedshiftDataPlugin{}
	if err := p.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("RedshiftDataPlugin.Initialize: %v", err)
	}

	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	registry.Register(p)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)

	return p, srv, state
}

// --- Plugin-level state tests (direct state manipulation) -----------------

func TestRedshiftDataPlugin_StateSeededResult(t *testing.T) {
	p, _, state := setupRedshiftDataWithSharedState(t)
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	strVal := "hello"
	result := &emulator.RedshiftDataResult{
		ColumnMetadata: []emulator.RedshiftDataColumnMetadata{
			{Name: "greeting", TypeName: "varchar"},
		},
		Records: [][]emulator.RedshiftDataField{
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
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	strVal := "wild"
	result := &emulator.RedshiftDataResult{
		ColumnMetadata: []emulator.RedshiftDataColumnMetadata{
			{Name: "v", TypeName: "varchar"},
		},
		Records: [][]emulator.RedshiftDataField{
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

// rdSeedStatus posts a statement-status seed through the control plane and requires a 200.
func rdSeedStatus(t *testing.T, srv *emulator.Server, body string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/status", strings.NewReader(body))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST /v1/redshift-data/status %s: %d %s", body, rr.Code, rr.Body)
	}
}

// rdExecuteAndDescribe executes sql and returns the first describe's Status and Error.
func rdExecuteAndDescribe(t *testing.T, p *emulator.RedshiftDataPlugin, ctx *emulator.RequestContext, sql string) (status, errMsg string) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"WorkgroupName": "wg", "Database": "db", "Sql": sql,
	}))
	if err != nil {
		t.Fatalf("ExecuteStatement: %v", err)
	}
	var exec struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(resp.Body, &exec); err != nil {
		t.Fatalf("decode ExecuteStatement: %v", err)
	}
	resp, err = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{"Id": exec.ID}))
	if err != nil {
		t.Fatalf("DescribeStatement: %v", err)
	}
	var desc struct {
		Status string `json:"Status"`
		Error  string `json:"Error"`
	}
	if err := json.Unmarshal(resp.Body, &desc); err != nil {
		t.Fatalf("decode DescribeStatement: %v", err)
	}
	return desc.Status, desc.Error
}

// The pre-#1163 body — no statementId — is the "*" wildcard, read at describe time.
func TestRedshiftDataPlugin_StateStatusFailed(t *testing.T) {
	p, srv, _ := setupRedshiftDataWithSharedState(t)
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	rdSeedStatus(t, srv, `{"status":"FAILED","errorMessage":"query timed out"}`)
	status, errMsg := rdExecuteAndDescribe(t, p, ctx, "SELECT 1")
	if status != "FAILED" {
		t.Errorf("want Status=FAILED, got %q", status)
	}
	if errMsg != "query timed out" {
		t.Errorf("want Error=%q, got %q", "query timed out", errMsg)
	}
}

// DELETE /v1/redshift-data/status clears the seed (#1163); there was no way to before.
func TestRedshiftDataPlugin_StateStatusClearedReturnsFinished(t *testing.T) {
	p, srv, _ := setupRedshiftDataWithSharedState(t)
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	rdSeedStatus(t, srv, `{"status":"FAILED"}`)
	if status, _ := rdExecuteAndDescribe(t, p, ctx, "SELECT 1"); status != "FAILED" {
		t.Fatalf("want FAILED, got %q", status)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/redshift-data/status", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("DELETE /v1/redshift-data/status: %d %s", rr.Code, rr.Body)
	}
	if status, _ := rdExecuteAndDescribe(t, p, ctx, "SELECT 2"); status != "FINISHED" {
		t.Errorf("want FINISHED after clearing the seed, got %q", status)
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
	dummy, _ := json.Marshal(&emulator.RedshiftDataResult{})
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

	dummy, _ := json.Marshal(&emulator.RedshiftDataResult{})
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

// The seed endpoint answers the key it stored, a status in any case is accepted upper-cased as the
// pre-#1163 handler did, and the describe that follows reports it.
func TestHandleRedshiftDataSetStatus(t *testing.T) {
	p, srv, _ := setupRedshiftDataWithSharedState(t)
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	req := httptest.NewRequest(http.MethodPost, "/v1/redshift-data/status", strings.NewReader(`{"status":"failed","errorMessage":"simulated error"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body)
	}
	if !strings.Contains(rr.Body.String(), `"statementId":"status:*"`) {
		t.Errorf("want the wildcard key echoed, got %s", rr.Body)
	}
	status, errMsg := rdExecuteAndDescribe(t, p, ctx, "SELECT 1")
	if status != "FAILED" || errMsg != "simulated error" {
		t.Errorf("want FAILED/simulated error, got %q/%q", status, errMsg)
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
