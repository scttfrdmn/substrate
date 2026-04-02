package substrate_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestAthenaPlugin_StopQueryExecution_Canceled verifies StopQueryExecution transitions state to CANCELED.
func TestAthenaPlugin_StopQueryExecution_Canceled(t *testing.T) {
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

	// Verify state is CANCELED.
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
	if result.QueryExecution.Status.State != "CANCELED" {
		t.Errorf("expected CANCELED, got %q", result.QueryExecution.Status.State)
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

// --- ListQueryExecutions --------------------------------------------------

// TestAthenaPlugin_ListQueryExecutions verifies that started queries appear in the list.
func TestAthenaPlugin_ListQueryExecutions(t *testing.T) {
	ts := newAthenaTestServer(t)

	// Start 3 queries.
	var ids []string
	for _, sql := range []string{"SELECT 1", "SELECT 2", "SELECT 3"} {
		resp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
			"QueryString": sql,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var started struct {
			QueryExecutionId string `json:"QueryExecutionId"`
		}
		require.NoError(t, json.Unmarshal(athenaBody(t, resp), &started))
		ids = append(ids, started.QueryExecutionId)
	}

	// List query executions.
	listResp := athenaRequest(t, ts, "ListQueryExecutions", map[string]interface{}{})
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	var result struct {
		QueryExecutionIds []string `json:"QueryExecutionIds"`
	}
	require.NoError(t, json.Unmarshal(athenaBody(t, listResp), &result))
	assert.Len(t, result.QueryExecutionIds, 3)
	for _, id := range ids {
		assert.Contains(t, result.QueryExecutionIds, id)
	}
}

// --- Workgroup CRUD -------------------------------------------------------

// TestAthenaPlugin_Workgroup_CreateGetDelete verifies create/get/delete lifecycle.
func TestAthenaPlugin_Workgroup_CreateGetDelete(t *testing.T) {
	ts := newAthenaTestServer(t)

	// Create workgroup.
	createResp := athenaRequest(t, ts, "CreateWorkGroup", map[string]interface{}{
		"Name":        "analytics",
		"Description": "analytics workgroup",
	})
	require.Equal(t, http.StatusOK, createResp.StatusCode)
	_ = athenaBody(t, createResp)

	// Get it.
	getResp := athenaRequest(t, ts, "GetWorkGroup", map[string]string{"WorkGroup": "analytics"})
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var got struct {
		WorkGroup struct {
			Name  string `json:"Name"`
			State string `json:"State"`
		} `json:"WorkGroup"`
	}
	require.NoError(t, json.Unmarshal(athenaBody(t, getResp), &got))
	assert.Equal(t, "analytics", got.WorkGroup.Name)
	assert.Equal(t, "ENABLED", got.WorkGroup.State)

	// Delete it.
	delResp := athenaRequest(t, ts, "DeleteWorkGroup", map[string]string{"WorkGroup": "analytics"})
	require.Equal(t, http.StatusOK, delResp.StatusCode)
	_ = athenaBody(t, delResp)

	// Get again — should 400.
	get2Resp := athenaRequest(t, ts, "GetWorkGroup", map[string]string{"WorkGroup": "analytics"})
	assert.Equal(t, http.StatusBadRequest, get2Resp.StatusCode)
	_ = athenaBody(t, get2Resp)
}

// TestAthenaPlugin_ListWorkGroups verifies that created workgroups appear in the list.
func TestAthenaPlugin_ListWorkGroups(t *testing.T) {
	ts := newAthenaTestServer(t)

	for _, name := range []string{"wg-a", "wg-b"} {
		resp := athenaRequest(t, ts, "CreateWorkGroup", map[string]interface{}{"Name": name})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_ = athenaBody(t, resp)
	}

	listResp := athenaRequest(t, ts, "ListWorkGroups", map[string]interface{}{})
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	var result struct {
		WorkGroups []struct {
			Name string `json:"Name"`
		} `json:"WorkGroups"`
	}
	require.NoError(t, json.Unmarshal(athenaBody(t, listResp), &result))
	names := make([]string, len(result.WorkGroups))
	for i, wg := range result.WorkGroups {
		names[i] = wg.Name
	}
	assert.Contains(t, names, "wg-a")
	assert.Contains(t, names, "wg-b")
}

// TestAthenaPlugin_GetWorkGroup_Primary_AutoExists verifies "primary" is always available.
func TestAthenaPlugin_GetWorkGroup_Primary_AutoExists(t *testing.T) {
	ts := newAthenaTestServer(t)

	resp := athenaRequest(t, ts, "GetWorkGroup", map[string]string{"WorkGroup": "primary"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		WorkGroup struct {
			Name  string `json:"Name"`
			State string `json:"State"`
		} `json:"WorkGroup"`
	}
	require.NoError(t, json.Unmarshal(athenaBody(t, resp), &result))
	assert.Equal(t, "primary", result.WorkGroup.Name)
	assert.Equal(t, "ENABLED", result.WorkGroup.State)
}

// --- Seeded results -------------------------------------------------------

// TestAthenaPlugin_GetQueryResults_Seeded verifies seeded rows are returned.
func TestAthenaPlugin_GetQueryResults_Seeded(t *testing.T) {
	// Build server with seeded state via the in-process state manager.
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)

	// Pre-seed a result into athena-ctrl namespace.
	seedData := `{"Rows":[{"Data":[{"VarCharValue":"alice"}]}],"ColumnInfo":[{"Name":"name","Type":"varchar"}]}`
	require.NoError(t, state.Put(t.Context(), "athena-ctrl", "result:SELECT name FROM users", []byte(seedData)))

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	p := &substrate.AthenaPlugin{}
	require.NoError(t, p.Initialize(t.Context(), substrate.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Start a query with matching SQL.
	startResp, err := http.DefaultClient.Do(newAthenaHTTPRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT name FROM users",
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, startResp.StatusCode)
	var started struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	b, _ := io.ReadAll(startResp.Body)
	startResp.Body.Close()
	require.NoError(t, json.Unmarshal(b, &started))

	// GetQueryResults — should return seeded rows.
	getResp, err := http.DefaultClient.Do(newAthenaHTTPRequest(t, ts, "GetQueryResults", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var result struct {
		ResultSet struct {
			Rows []struct {
				Data []struct {
					VarCharValue string `json:"VarCharValue"`
				} `json:"Data"`
			} `json:"Rows"`
		} `json:"ResultSet"`
	}
	b2, _ := io.ReadAll(getResp.Body)
	getResp.Body.Close()
	require.NoError(t, json.Unmarshal(b2, &result))
	require.Len(t, result.ResultSet.Rows, 1)
	assert.Equal(t, "alice", result.ResultSet.Rows[0].Data[0].VarCharValue)
}

// newAthenaHTTPRequest builds an *http.Request for use with http.DefaultClient.
func newAthenaHTTPRequest(t *testing.T, ts *httptest.Server, operation string, body interface{}) *http.Request {
	t.Helper()
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = "athena.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonAthena."+operation)
	return req
}

// TestAthena_ControlPlane_SeedAndClear verifies the /v1/athena/results endpoint.
func TestAthena_ControlPlane_SeedAndClear(t *testing.T) {
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	p := &substrate.AthenaPlugin{}
	require.NoError(t, p.Initialize(t.Context(), substrate.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Seed a wildcard result via control plane.
	seedBody := `{"sql":"*","result":{"Rows":[{"Data":[{"VarCharValue":"42"}]}],"ColumnInfo":[{"Name":"n","Type":"bigint"}]}}`
	seedResp, err := http.Post(ts.URL+"/v1/athena/results", "application/json", strings.NewReader(seedBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, seedResp.StatusCode)
	seedResp.Body.Close()

	// Start a query and get results — should return seeded rows.
	startResp, err := http.DefaultClient.Do(newAthenaHTTPRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT count(*) FROM t",
	}))
	require.NoError(t, err)
	var started struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	b, _ := io.ReadAll(startResp.Body)
	startResp.Body.Close()
	require.NoError(t, json.Unmarshal(b, &started))

	getResp, err := http.DefaultClient.Do(newAthenaHTTPRequest(t, ts, "GetQueryResults", map[string]string{
		"QueryExecutionId": started.QueryExecutionId,
	}))
	require.NoError(t, err)
	b2, _ := io.ReadAll(getResp.Body)
	getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	assert.Contains(t, string(b2), "42")

	// Delete seeded results.
	delReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/athena/results", nil)
	delResp, err := http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, delResp.StatusCode)
	delResp.Body.Close()

	// Start another query; GetQueryResults — should now return empty rows.
	startResp2, err := http.DefaultClient.Do(newAthenaHTTPRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT 1",
	}))
	require.NoError(t, err)
	var started2 struct {
		QueryExecutionId string `json:"QueryExecutionId"`
	}
	b3, _ := io.ReadAll(startResp2.Body)
	startResp2.Body.Close()
	require.NoError(t, json.Unmarshal(b3, &started2))

	getResp2, err := http.DefaultClient.Do(newAthenaHTTPRequest(t, ts, "GetQueryResults", map[string]string{
		"QueryExecutionId": started2.QueryExecutionId,
	}))
	require.NoError(t, err)
	var result2 struct {
		ResultSet struct {
			Rows []interface{} `json:"Rows"`
		} `json:"ResultSet"`
	}
	b4, _ := io.ReadAll(getResp2.Body)
	getResp2.Body.Close()
	require.NoError(t, json.Unmarshal(b4, &result2))
	assert.Empty(t, result2.ResultSet.Rows)
}
