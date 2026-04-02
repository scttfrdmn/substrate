package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupRedshiftDataPlugin(t *testing.T) (*substrate.RedshiftDataPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.RedshiftDataPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("RedshiftDataPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-rddata-1",
	}
}

func redshiftDataRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal redshift-data request: %v", err)
		}
	}
	return &substrate.AWSRequest{
		Service:   "redshift-data",
		Operation: op,
		Path:      "/",
		Headers:   map[string]string{},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestRedshiftDataPlugin_ExecuteDescribeGet(t *testing.T) {
	p, ctx := setupRedshiftDataPlugin(t)

	// ExecuteStatement.
	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"WorkgroupName": "my-workgroup",
		"Database":      "mydb",
		"Sql":           "SELECT table_name FROM information_schema.tables",
	}))
	if err != nil {
		t.Fatalf("ExecuteStatement: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	var execResult struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(resp.Body, &execResult); err != nil {
		t.Fatalf("unmarshal execute: %v", err)
	}
	if execResult.ID == "" {
		t.Fatal("want non-empty statement Id")
	}
	stmtID := execResult.ID

	// DescribeStatement.
	resp, err = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{
		"Id": stmtID,
	}))
	if err != nil {
		t.Fatalf("DescribeStatement: %v", err)
	}
	var descResult struct {
		ID          string `json:"Id"`
		Status      string `json:"Status"`
		QueryString string `json:"QueryString"`
	}
	if err := json.Unmarshal(resp.Body, &descResult); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if descResult.Status != "FINISHED" {
		t.Errorf("want Status=FINISHED, got %q", descResult.Status)
	}
	if descResult.ID != stmtID {
		t.Errorf("want Id=%s, got %s", stmtID, descResult.ID)
	}

	// GetStatementResult — no pre-seeded data, expect empty result.
	resp, err = p.HandleRequest(ctx, redshiftDataRequest(t, "GetStatementResult", map[string]any{
		"Id": stmtID,
	}))
	if err != nil {
		t.Fatalf("GetStatementResult: %v", err)
	}
	var getResult struct {
		ColumnMetadata []struct{}   `json:"ColumnMetadata"`
		Records        [][]struct{} `json:"Records"`
		TotalNumRows   int          `json:"TotalNumRows"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get result: %v", err)
	}
	if getResult.TotalNumRows != 0 {
		t.Errorf("want TotalNumRows=0, got %d", getResult.TotalNumRows)
	}
}

func TestRedshiftDataPlugin_SeededResults(t *testing.T) {
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.RedshiftDataPlugin{}

	strVal1 := "public"
	strVal2 := "my_table"
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{
			"time_controller": tc,
			"results": map[string]*substrate.RedshiftDataResult{
				"*": {
					ColumnMetadata: []substrate.RedshiftDataColumnMetadata{
						{Name: "table_schema", TypeName: "varchar"},
						{Name: "table_name", TypeName: "varchar"},
					},
					Records: [][]substrate.RedshiftDataField{
						{
							{StringValue: &strVal1},
							{StringValue: &strVal2},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ctx := &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-rddata-2",
	}

	resp, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"WorkgroupName": "my-workgroup",
		"Database":      "mydb",
		"Sql":           "SELECT table_schema, table_name FROM information_schema.tables",
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
		ColumnMetadata []struct {
			Name string `json:"name"`
		} `json:"ColumnMetadata"`
		Records      [][]map[string]interface{} `json:"Records"`
		TotalNumRows int                        `json:"TotalNumRows"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get result: %v", err)
	}
	if getResult.TotalNumRows != 1 {
		t.Errorf("want TotalNumRows=1, got %d", getResult.TotalNumRows)
	}
	if len(getResult.ColumnMetadata) != 2 {
		t.Errorf("want 2 columns, got %d", len(getResult.ColumnMetadata))
	}
	if len(getResult.ColumnMetadata) > 0 && getResult.ColumnMetadata[0].Name != "table_schema" {
		t.Errorf("want column name=table_schema, got %q", getResult.ColumnMetadata[0].Name)
	}
}

func TestRedshiftDataPlugin_Errors(t *testing.T) {
	p, ctx := setupRedshiftDataPlugin(t)

	// ExecuteStatement missing SQL.
	_, err := p.HandleRequest(ctx, redshiftDataRequest(t, "ExecuteStatement", map[string]any{}))
	if err == nil {
		t.Fatal("want error for missing Sql")
	}

	// DescribeStatement missing Id.
	_, err = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{}))
	if err == nil {
		t.Fatal("want error for missing Id")
	}

	// DescribeStatement not found.
	_, err = p.HandleRequest(ctx, redshiftDataRequest(t, "DescribeStatement", map[string]any{
		"Id": "00000000-0000-0000-0000-000000000000",
	}))
	if err == nil {
		t.Fatal("want error for nonexistent statement")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}

	// Unsupported operation.
	_, err = p.HandleRequest(ctx, redshiftDataRequest(t, "UnknownOp", nil))
	if err == nil {
		t.Fatal("want error for unsupported operation")
	}
}
