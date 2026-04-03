package substrate_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newTimestreamTestServer builds a minimal server with the Timestream plugin registered.
func newTimestreamTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)

	p := &substrate.TimestreamPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize timestream plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// tsRequest sends a Timestream JSON-target request and returns the response.
func tsRequest(t *testing.T, ts *httptest.Server, operation string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal timestream request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build timestream request: %v", err)
	}
	req.Host = "timestream.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "Timestream_20181101."+operation)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do timestream request: %v", err)
	}
	return resp
}

// tsBody reads and closes the response body, returning parsed JSON.
func tsBody(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read timestream response: %v", err)
	}
	var result map[string]interface{}
	if err2 := json.Unmarshal(b, &result); err2 != nil {
		t.Fatalf("unmarshal timestream response: %v\nbody: %s", err2, b)
	}
	return result
}

// TestTimestream_DatabaseCRUD covers the full database lifecycle.
func TestTimestream_DatabaseCRUD(t *testing.T) {
	ts := newTimestreamTestServer(t)

	// CreateDatabase.
	resp := tsRequest(t, ts, "CreateDatabase", map[string]any{"DatabaseName": "metrics"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDatabase: got %d", resp.StatusCode)
	}
	body := tsBody(t, resp)
	dbMap, ok := body["Database"].(map[string]interface{})
	if !ok {
		t.Fatal("expected Database in CreateDatabase response")
	}
	if dbMap["DatabaseName"] != "metrics" {
		t.Errorf("want DatabaseName=metrics, got %v", dbMap["DatabaseName"])
	}
	if dbMap["Arn"] == "" {
		t.Error("want non-empty Arn")
	}

	// Duplicate create → 409.
	resp2 := tsRequest(t, ts, "CreateDatabase", map[string]any{"DatabaseName": "metrics"})
	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("duplicate CreateDatabase: want 409, got %d", resp2.StatusCode)
	}
	_, _ = io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	// DescribeDatabase.
	resp3 := tsRequest(t, ts, "DescribeDatabase", map[string]any{"DatabaseName": "metrics"})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DescribeDatabase: got %d", resp3.StatusCode)
	}
	body3 := tsBody(t, resp3)
	dbMap3, _ := body3["Database"].(map[string]interface{})
	if dbMap3["DatabaseName"] != "metrics" {
		t.Errorf("DescribeDatabase: want DatabaseName=metrics, got %v", dbMap3["DatabaseName"])
	}

	// ListDatabases.
	resp4 := tsRequest(t, ts, "ListDatabases", map[string]any{})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("ListDatabases: got %d", resp4.StatusCode)
	}
	body4 := tsBody(t, resp4)
	dbs, _ := body4["Databases"].([]interface{})
	if len(dbs) != 1 {
		t.Fatalf("want 1 database, got %d", len(dbs))
	}

	// DeleteDatabase.
	resp5 := tsRequest(t, ts, "DeleteDatabase", map[string]any{"DatabaseName": "metrics"})
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("DeleteDatabase: got %d", resp5.StatusCode)
	}
	_, _ = io.ReadAll(resp5.Body)
	_ = resp5.Body.Close()

	// DescribeDatabase after delete → 404.
	resp6 := tsRequest(t, ts, "DescribeDatabase", map[string]any{"DatabaseName": "metrics"})
	if resp6.StatusCode != http.StatusNotFound {
		t.Errorf("want 404 after delete, got %d", resp6.StatusCode)
	}
	_, _ = io.ReadAll(resp6.Body)
	_ = resp6.Body.Close()
}

// TestTimestream_TableCRUD covers the full table lifecycle.
func TestTimestream_TableCRUD(t *testing.T) {
	ts := newTimestreamTestServer(t)

	// Set up parent database.
	resp := tsRequest(t, ts, "CreateDatabase", map[string]any{"DatabaseName": "mydb"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateDatabase: got %d", resp.StatusCode)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	// CreateTable.
	resp2 := tsRequest(t, ts, "CreateTable", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "events",
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("CreateTable: got %d", resp2.StatusCode)
	}
	body2 := tsBody(t, resp2)
	tblMap, ok := body2["Table"].(map[string]interface{})
	if !ok {
		t.Fatal("expected Table in CreateTable response")
	}
	if tblMap["TableStatus"] != "ACTIVE" {
		t.Errorf("want TableStatus=ACTIVE, got %v", tblMap["TableStatus"])
	}
	if tblMap["Arn"] == "" {
		t.Error("want non-empty Arn")
	}

	// Duplicate table → 409.
	resp3 := tsRequest(t, ts, "CreateTable", map[string]any{"DatabaseName": "mydb", "TableName": "events"})
	if resp3.StatusCode != http.StatusConflict {
		t.Errorf("duplicate CreateTable: want 409, got %d", resp3.StatusCode)
	}
	_, _ = io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()

	// DescribeTable.
	resp4 := tsRequest(t, ts, "DescribeTable", map[string]any{"DatabaseName": "mydb", "TableName": "events"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("DescribeTable: got %d", resp4.StatusCode)
	}
	body4 := tsBody(t, resp4)
	tblMap4, _ := body4["Table"].(map[string]interface{})
	if tblMap4["TableName"] != "events" {
		t.Errorf("want TableName=events, got %v", tblMap4["TableName"])
	}

	// ListTables.
	resp5 := tsRequest(t, ts, "ListTables", map[string]any{"DatabaseName": "mydb"})
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("ListTables: got %d", resp5.StatusCode)
	}
	body5 := tsBody(t, resp5)
	tables, _ := body5["Tables"].([]interface{})
	if len(tables) != 1 {
		t.Fatalf("want 1 table, got %d", len(tables))
	}

	// DeleteTable.
	resp6 := tsRequest(t, ts, "DeleteTable", map[string]any{"DatabaseName": "mydb", "TableName": "events"})
	if resp6.StatusCode != http.StatusOK {
		t.Fatalf("DeleteTable: got %d", resp6.StatusCode)
	}
	_, _ = io.ReadAll(resp6.Body)
	_ = resp6.Body.Close()

	// ListTables after delete → empty.
	resp7 := tsRequest(t, ts, "ListTables", map[string]any{"DatabaseName": "mydb"})
	body7 := tsBody(t, resp7)
	tables7, _ := body7["Tables"].([]interface{})
	if len(tables7) != 0 {
		t.Errorf("want 0 tables after delete, got %d", len(tables7))
	}
}

// TestTimestream_WriteAndQuery covers WriteRecords, Query seeding, DescribeEndpoints,
// and CancelQuery.
func TestTimestream_WriteAndQuery(t *testing.T) {
	ts := newTimestreamTestServer(t)

	// Setup: create database and table.
	for _, op := range []struct {
		op   string
		body map[string]any
	}{
		{"CreateDatabase", map[string]any{"DatabaseName": "tsdb"}},
		{"CreateTable", map[string]any{"DatabaseName": "tsdb", "TableName": "metrics"}},
	} {
		r := tsRequest(t, ts, op.op, op.body)
		if r.StatusCode != http.StatusOK {
			t.Fatalf("%s: got %d", op.op, r.StatusCode)
		}
		_, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}

	// WriteRecords — 3 records.
	records := make([]map[string]any, 3)
	for i := range records {
		records[i] = map[string]any{"MeasureName": "cpu", "MeasureValue": "50"}
	}
	resp := tsRequest(t, ts, "WriteRecords", map[string]any{
		"DatabaseName": "tsdb",
		"TableName":    "metrics",
		"Records":      records,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("WriteRecords: got %d", resp.StatusCode)
	}
	body := tsBody(t, resp)
	ingested, _ := body["RecordsIngested"].(map[string]interface{})
	if ingested["Total"].(float64) != 3 {
		t.Errorf("want Total=3, got %v", ingested["Total"])
	}

	// WriteRecords to nonexistent table → 404.
	resp2 := tsRequest(t, ts, "WriteRecords", map[string]any{
		"DatabaseName": "tsdb",
		"TableName":    "no-such-table",
		"Records":      records,
	})
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("WriteRecords missing table: want 404, got %d", resp2.StatusCode)
	}
	_, _ = io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	// DescribeEndpoints.
	resp3 := tsRequest(t, ts, "DescribeEndpoints", map[string]any{})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DescribeEndpoints: got %d", resp3.StatusCode)
	}
	body3 := tsBody(t, resp3)
	endpoints, _ := body3["Endpoints"].([]interface{})
	if len(endpoints) != 1 {
		t.Fatalf("want 1 endpoint, got %d", len(endpoints))
	}
	ep, _ := endpoints[0].(map[string]interface{})
	if ep["Address"] == "" {
		t.Error("want non-empty endpoint Address")
	}

	// Seed a query result via the control-plane endpoint.
	seedPayload := map[string]any{
		"queryString": "SELECT * FROM tsdb.metrics",
		"result": map[string]any{
			"Rows": []map[string]any{
				{"Data": []map[string]any{{"ScalarValue": "42"}}},
			},
			"ColumnInfo": []map[string]any{
				{"Name": "cpu", "Type": map[string]any{"ScalarType": "BIGINT"}},
			},
		},
	}
	seedBytes, _ := json.Marshal(seedPayload)
	seedReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/timestream-query/results", bytes.NewReader(seedBytes))
	seedReq.Header.Set("Content-Type", "application/json")
	seedResp, err := http.DefaultClient.Do(seedReq)
	if err != nil {
		t.Fatalf("seed POST: %v", err)
	}
	_, _ = io.ReadAll(seedResp.Body)
	_ = seedResp.Body.Close()
	if seedResp.StatusCode != http.StatusOK {
		t.Fatalf("seed POST: got %d", seedResp.StatusCode)
	}

	// Query with the seeded query string → 1 row.
	resp4 := tsRequest(t, ts, "Query", map[string]any{"QueryString": "SELECT * FROM tsdb.metrics"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("Query: got %d", resp4.StatusCode)
	}
	body4 := tsBody(t, resp4)
	rows, _ := body4["Rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("want 1 row from seeded query, got %d", len(rows))
	}

	// Seed a wildcard result.
	wildcardPayload := map[string]any{
		"queryString": "*",
		"result": map[string]any{
			"Rows": []map[string]any{
				{"Data": []map[string]any{{"ScalarValue": "wildcard"}}},
			},
			"ColumnInfo": []map[string]any{},
		},
	}
	wcBytes, _ := json.Marshal(wildcardPayload)
	wcReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/timestream-query/results", bytes.NewReader(wcBytes))
	wcReq.Header.Set("Content-Type", "application/json")
	wcResp, _ := http.DefaultClient.Do(wcReq)
	_, _ = io.ReadAll(wcResp.Body)
	_ = wcResp.Body.Close()

	// Query with unknown SQL → wildcard row returned.
	resp5 := tsRequest(t, ts, "Query", map[string]any{"QueryString": "SELECT 1"})
	body5 := tsBody(t, resp5)
	rows5, _ := body5["Rows"].([]interface{})
	if len(rows5) != 1 {
		t.Errorf("want 1 wildcard row, got %d", len(rows5))
	}

	// DELETE all seeded results.
	delReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/timestream-query/results", nil)
	delResp, _ := http.DefaultClient.Do(delReq)
	_, _ = io.ReadAll(delResp.Body)
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE results: got %d", delResp.StatusCode)
	}

	// Query after clear → empty rows.
	resp6 := tsRequest(t, ts, "Query", map[string]any{"QueryString": "SELECT 1"})
	body6 := tsBody(t, resp6)
	rows6, _ := body6["Rows"].([]interface{})
	if len(rows6) != 0 {
		t.Errorf("want 0 rows after clear, got %d", len(rows6))
	}

	// CancelQuery → 200.
	resp7 := tsRequest(t, ts, "CancelQuery", map[string]any{"QueryId": "qid-1"})
	if resp7.StatusCode != http.StatusOK {
		t.Errorf("CancelQuery: want 200, got %d", resp7.StatusCode)
	}
	_, _ = io.ReadAll(resp7.Body)
	_ = resp7.Body.Close()
}
