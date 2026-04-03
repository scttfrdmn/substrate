package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TimestreamPlugin emulates the Amazon Timestream Write and Query services.
// It supports database and table lifecycle management, record ingestion counting,
// and seeded query result responses for deterministic testing.
type TimestreamPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "timestream".
func (p *TimestreamPlugin) Name() string { return timestreamNamespace }

// Initialize sets up the TimestreamPlugin with the provided configuration.
func (p *TimestreamPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for TimestreamPlugin.
func (p *TimestreamPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Timestream JSON-target request to the appropriate handler.
func (p *TimestreamPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	// Write service — database operations.
	case "CreateDatabase":
		return p.createDatabase(ctx, req)
	case "DescribeDatabase":
		return p.describeDatabase(ctx, req)
	case "DeleteDatabase":
		return p.deleteDatabase(ctx, req)
	case "ListDatabases":
		return p.listDatabases(ctx, req)
	// Write service — table operations.
	case "CreateTable":
		return p.createTable(ctx, req)
	case "DescribeTable":
		return p.describeTable(ctx, req)
	case "DeleteTable":
		return p.deleteTable(ctx, req)
	case "ListTables":
		return p.listTables(ctx, req)
	// Write service — record ingestion.
	case "WriteRecords":
		return p.writeRecords(ctx, req)
	// Query service.
	case "DescribeEndpoints":
		return p.describeEndpoints(ctx, req)
	case "Query":
		return p.query(ctx, req)
	case "CancelQuery":
		return p.cancelQuery(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "TimestreamPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// timestreamJSONResponse builds a successful AWSResponse with a JSON body.
func timestreamJSONResponse(status int, body any) (*AWSResponse, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("timestream marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
		Body:       data,
	}, nil
}

// --- Database operations ---

func (p *TimestreamPlugin) createDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName is required", HTTPStatus: http.StatusBadRequest}
	}
	acct, region := reqCtx.AccountID, reqCtx.Region
	goCtx := context.Background()

	existing, _ := p.state.Get(goCtx, timestreamNamespace, timestreamDBKey(acct, region, input.DatabaseName))
	if existing != nil {
		return nil, &AWSError{Code: "ConflictException", Message: "Database already exists: " + input.DatabaseName, HTTPStatus: http.StatusConflict}
	}

	now := p.tc.Now().UTC().Format(time.RFC3339)
	db := TimestreamDatabase{
		DatabaseName:    input.DatabaseName,
		Arn:             fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s", region, acct, input.DatabaseName),
		TableCount:      0,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	data, _ := json.Marshal(db)
	if err := p.state.Put(goCtx, timestreamNamespace, timestreamDBKey(acct, region, input.DatabaseName), data); err != nil {
		return nil, fmt.Errorf("put timestream database: %w", err)
	}
	updateStringIndex(goCtx, p.state, timestreamNamespace, timestreamDBNamesKey(acct, region), input.DatabaseName)
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Database": db})
}

func (p *TimestreamPlugin) describeDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName is required", HTTPStatus: http.StatusBadRequest}
	}
	db, err := p.loadDatabase(reqCtx.AccountID, reqCtx.Region, input.DatabaseName)
	if err != nil {
		return nil, err
	}
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Database": db})
}

func (p *TimestreamPlugin) deleteDatabase(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadDatabase(reqCtx.AccountID, reqCtx.Region, input.DatabaseName); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	if err := p.state.Delete(goCtx, timestreamNamespace, timestreamDBKey(acct, region, input.DatabaseName)); err != nil {
		return nil, fmt.Errorf("delete timestream database: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, timestreamNamespace, timestreamDBNamesKey(acct, region), input.DatabaseName)
	return timestreamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *TimestreamPlugin) listDatabases(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	names, _ := loadStringIndex(goCtx, p.state, timestreamNamespace, timestreamDBNamesKey(acct, region))
	dbs := make([]TimestreamDatabase, 0, len(names))
	for _, name := range names {
		raw, err := p.state.Get(goCtx, timestreamNamespace, timestreamDBKey(acct, region, name))
		if err != nil || raw == nil {
			continue
		}
		var db TimestreamDatabase
		if err2 := json.Unmarshal(raw, &db); err2 == nil {
			dbs = append(dbs, db)
		}
	}
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Databases": dbs})
}

// --- Table operations ---

func (p *TimestreamPlugin) createTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName        string                        `json:"DatabaseName"`
		TableName           string                        `json:"TableName"`
		RetentionProperties TimestreamRetentionProperties `json:"RetentionProperties"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" || input.TableName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName and TableName are required", HTTPStatus: http.StatusBadRequest}
	}
	acct, region := reqCtx.AccountID, reqCtx.Region
	goCtx := context.Background()

	if _, err := p.loadDatabase(acct, region, input.DatabaseName); err != nil {
		return nil, err
	}
	existing, _ := p.state.Get(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, input.TableName))
	if existing != nil {
		return nil, &AWSError{Code: "ConflictException", Message: "Table already exists: " + input.TableName, HTTPStatus: http.StatusConflict}
	}

	retention := input.RetentionProperties
	if retention.MemoryStoreRetentionPeriodInHours == 0 {
		retention.MemoryStoreRetentionPeriodInHours = 24
	}
	if retention.MagneticStoreRetentionPeriodInDays == 0 {
		retention.MagneticStoreRetentionPeriodInDays = 7
	}
	now := p.tc.Now().UTC().Format(time.RFC3339)
	tbl := TimestreamTable{
		DatabaseName:        input.DatabaseName,
		TableName:           input.TableName,
		Arn:                 fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s/table/%s", region, acct, input.DatabaseName, input.TableName),
		TableStatus:         "ACTIVE",
		CreationTime:        now,
		LastUpdatedTime:     now,
		RetentionProperties: retention,
	}
	data, _ := json.Marshal(tbl)
	if err := p.state.Put(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, input.TableName), data); err != nil {
		return nil, fmt.Errorf("put timestream table: %w", err)
	}
	updateStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, input.DatabaseName), input.TableName)
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Table": tbl})
}

func (p *TimestreamPlugin) describeTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		TableName    string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" || input.TableName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName and TableName are required", HTTPStatus: http.StatusBadRequest}
	}
	tbl, err := p.loadTable(reqCtx.AccountID, reqCtx.Region, input.DatabaseName, input.TableName)
	if err != nil {
		return nil, err
	}
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Table": tbl})
}

func (p *TimestreamPlugin) deleteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		TableName    string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" || input.TableName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName and TableName are required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadTable(reqCtx.AccountID, reqCtx.Region, input.DatabaseName, input.TableName); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	if err := p.state.Delete(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, input.TableName)); err != nil {
		return nil, fmt.Errorf("delete timestream table: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, input.DatabaseName), input.TableName)
	return timestreamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *TimestreamPlugin) listTables(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
	}
	_ = json.Unmarshal(req.Body, &input)
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	names, _ := loadStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, input.DatabaseName))
	tables := make([]TimestreamTable, 0, len(names))
	for _, name := range names {
		raw, err := p.state.Get(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, name))
		if err != nil || raw == nil {
			continue
		}
		var tbl TimestreamTable
		if err2 := json.Unmarshal(raw, &tbl); err2 == nil {
			tables = append(tables, tbl)
		}
	}
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Tables": tables})
}

// --- Record ingestion ---

func (p *TimestreamPlugin) writeRecords(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string           `json:"DatabaseName"`
		TableName    string           `json:"TableName"`
		Records      []map[string]any `json:"Records"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.DatabaseName == "" || input.TableName == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "DatabaseName and TableName are required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadDatabase(reqCtx.AccountID, reqCtx.Region, input.DatabaseName); err != nil {
		return nil, err
	}
	if _, err := p.loadTable(reqCtx.AccountID, reqCtx.Region, input.DatabaseName, input.TableName); err != nil {
		return nil, err
	}
	n := int64(len(input.Records))
	return timestreamJSONResponse(http.StatusOK, map[string]any{
		"RecordsIngested": map[string]any{
			"Total":         n,
			"MemoryStore":   n,
			"MagneticStore": int64(0),
		},
	})
}

// --- Query operations ---

func (p *TimestreamPlugin) describeEndpoints(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	return timestreamJSONResponse(http.StatusOK, map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              "timestream." + reqCtx.Region + ".amazonaws.com",
				"CachePeriodInMinutes": int64(1),
			},
		},
	})
}

func (p *TimestreamPlugin) query(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		QueryString string `json:"QueryString"`
	}
	_ = json.Unmarshal(req.Body, &input)

	result := p.lookupQueryResult(input.QueryString)

	rows := result.Rows
	if rows == nil {
		rows = []TimestreamRow{}
	}
	cols := result.ColumnInfo
	if cols == nil {
		cols = []TimestreamColumnInfo{}
	}

	return timestreamJSONResponse(http.StatusOK, map[string]any{
		"QueryId":    randomHex(16),
		"Rows":       rows,
		"ColumnInfo": cols,
		"NextToken":  "",
	})
}

func (p *TimestreamPlugin) cancelQuery(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	return timestreamJSONResponse(http.StatusOK, map[string]any{})
}

// lookupQueryResult returns the seeded result for the given query string,
// falling back to the wildcard "*" seed, and finally an empty result.
func (p *TimestreamPlugin) lookupQueryResult(qs string) TimestreamQueryResult {
	goCtx := context.Background()
	for _, key := range []string{qs, "*"} {
		raw, err := p.state.Get(goCtx, timestreamCtrlNamespace, timestreamCtrlResultKey(key))
		if err != nil || raw == nil {
			continue
		}
		var result TimestreamQueryResult
		if err2 := json.Unmarshal(raw, &result); err2 == nil {
			return result
		}
	}
	return TimestreamQueryResult{}
}

// --- Load helpers ---

func (p *TimestreamPlugin) loadDatabase(acct, region, name string) (TimestreamDatabase, error) {
	raw, err := p.state.Get(context.Background(), timestreamNamespace, timestreamDBKey(acct, region, name))
	if err != nil || raw == nil {
		return TimestreamDatabase{}, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "Database not found: " + name,
			HTTPStatus: http.StatusNotFound,
		}
	}
	var db TimestreamDatabase
	if err2 := json.Unmarshal(raw, &db); err2 != nil {
		return TimestreamDatabase{}, fmt.Errorf("unmarshal timestream database: %w", err2)
	}
	return db, nil
}

func (p *TimestreamPlugin) loadTable(acct, region, dbName, tableName string) (TimestreamTable, error) {
	raw, err := p.state.Get(context.Background(), timestreamNamespace, timestreamTableKey(acct, region, dbName, tableName))
	if err != nil || raw == nil {
		return TimestreamTable{}, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("Table not found: %s/%s", dbName, tableName),
			HTTPStatus: http.StatusNotFound,
		}
	}
	var tbl TimestreamTable
	if err2 := json.Unmarshal(raw, &tbl); err2 != nil {
		return TimestreamTable{}, fmt.Errorf("unmarshal timestream table: %w", err2)
	}
	return tbl, nil
}
