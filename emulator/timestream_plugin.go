package emulator

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
//
// An operation arriving on a Timestream endpoint that does not serve it is refused first; see
// emulator/timestream_query.go.
func (p *TimestreamPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if err := checkTimestreamEndpoint(req); err != nil {
		return nil, err
	}
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
		return nil, unknownActionError(p.Name(), req.Operation)
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

	now := p.tc.Now().UTC()
	db := TimestreamDatabase{
		DatabaseName:    input.DatabaseName,
		Arn:             fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s", region, acct, input.DatabaseName),
		TableCount:      0,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	data, err := json.Marshal(db)
	if err != nil {
		return nil, fmt.Errorf("marshal timestream database: %w", err)
	}
	if err := p.state.Put(goCtx, timestreamNamespace, timestreamDBKey(acct, region, input.DatabaseName), data); err != nil {
		return nil, fmt.Errorf("put timestream database: %w", err)
	}
	updateStringIndex(goCtx, p.state, timestreamNamespace, timestreamDBNamesKey(acct, region), input.DatabaseName)
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Database": timestreamDatabaseToWire(db)})
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
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Database": timestreamDatabaseToWire(db)})
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
	dbs := make([]timestreamDatabaseOut, 0, len(names))
	for _, name := range names {
		raw, err := p.state.Get(goCtx, timestreamNamespace, timestreamDBKey(acct, region, name))
		if err != nil || raw == nil {
			continue
		}
		var db TimestreamDatabase
		if err2 := json.Unmarshal(raw, &db); err2 == nil {
			dbs = append(dbs, timestreamDatabaseToWire(db))
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
	now := p.tc.Now().UTC()
	tbl := TimestreamTable{
		DatabaseName:        input.DatabaseName,
		TableName:           input.TableName,
		Arn:                 fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s/table/%s", region, acct, input.DatabaseName, input.TableName),
		TableStatus:         "ACTIVE",
		CreationTime:        now,
		LastUpdatedTime:     now,
		RetentionProperties: retention,
	}
	data, err := json.Marshal(tbl)
	if err != nil {
		return nil, fmt.Errorf("marshal timestream table: %w", err)
	}
	if err := p.state.Put(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, input.TableName), data); err != nil {
		return nil, fmt.Errorf("put timestream table: %w", err)
	}
	updateStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, input.DatabaseName), input.TableName)
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Table": timestreamTableToWire(tbl)})
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
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Table": timestreamTableToWire(tbl)})
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
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, timestreamInvalidBody()
		}
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	names, _ := loadStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, input.DatabaseName))
	tables := make([]timestreamTableOut, 0, len(names))
	for _, name := range names {
		raw, err := p.state.Get(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, name))
		if err != nil || raw == nil {
			continue
		}
		var tbl TimestreamTable
		if err2 := json.Unmarshal(raw, &tbl); err2 == nil {
			tables = append(tables, timestreamTableToWire(tbl))
		}
	}
	return timestreamJSONResponse(http.StatusOK, map[string]any{"Tables": tables})
}

// --- Record ingestion ---

func (p *TimestreamPlugin) writeRecords(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName     string                   `json:"DatabaseName"`
		TableName        string                   `json:"TableName"`
		CommonAttributes timestreamStoredRecord   `json:"CommonAttributes"`
		Records          []timestreamStoredRecord `json:"Records"`
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
	// Store records for later query retrieval, each with the request's CommonAttributes merged in, so
	// a SELECT * answers what the write meant rather than what one record spelled out (#1209).
	goCtx := context.Background()
	recordsKey := timestreamRecordsKey(reqCtx.AccountID, reqCtx.Region, input.DatabaseName, input.TableName)
	var existing []timestreamStoredRecord
	data, err := p.state.Get(goCtx, timestreamNamespace, recordsKey)
	if err != nil {
		return nil, fmt.Errorf("timestream writeRecords read records: %w", err)
	}
	if data != nil {
		if err := json.Unmarshal(data, &existing); err != nil {
			return nil, fmt.Errorf("timestream writeRecords decode records: %w", err)
		}
	}
	for _, rec := range input.Records {
		existing = append(existing, mergeTimestreamCommon(input.CommonAttributes, rec))
	}
	out, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("timestream writeRecords marshal records: %w", err)
	}
	if err := p.state.Put(goCtx, timestreamNamespace, recordsKey, out); err != nil {
		return nil, fmt.Errorf("timestream writeRecords put records: %w", err)
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

// describeEndpoints answers the cell address of the API the request's host names; see
// timestreamEndpointAddress.
func (p *TimestreamPlugin) describeEndpoints(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	address := timestreamEndpointAddress(classifyTimestreamHost(req.Headers["Host"]), reqCtx.Region)
	return timestreamJSONResponse(http.StatusOK, map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              address,
				"CachePeriodInMinutes": int64(1),
			},
		},
	})
}

// query answers a seeded result, or `SELECT * FROM db.table` reconstructed from stored records, and
// refuses any other unseeded query; see emulator/timestream_query.go.
func (p *TimestreamPlugin) query(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		QueryString string `json:"QueryString"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, timestreamInvalidBody()
		}
	}

	result, seeded, err := p.seededQueryResult(input.QueryString)
	if err != nil {
		return nil, err
	}
	if !seeded {
		db, table, ok := timestreamUnseededSelectAll(input.QueryString)
		if !ok {
			return nil, timestreamUnevaluated(input.QueryString)
		}
		if result, err = p.timestreamSelectAll(reqCtx, db, table); err != nil {
			return nil, err
		}
	}

	rows := result.Rows
	if rows == nil {
		rows = []TimestreamRow{}
	}
	cols := result.ColumnInfo
	if cols == nil {
		cols = []TimestreamColumnInfo{}
	}
	status := result.QueryStatus
	if status == nil {
		derived, err := timestreamQueryStatus(rows)
		if err != nil {
			return nil, err
		}
		status = &derived
	}

	// NextToken is omitted: every result is answered whole, and API_query_Query's NextToken has a
	// minimum length of 1, so the empty string answered before #1209 was never a value it publishes.
	return timestreamJSONResponse(http.StatusOK, map[string]any{
		"QueryId":     timestreamQueryID(reqCtx.IDs),
		"Rows":        rows,
		"ColumnInfo":  cols,
		"QueryStatus": status,
	})
}

func (p *TimestreamPlugin) cancelQuery(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	return timestreamJSONResponse(http.StatusOK, map[string]any{})
}

// timestreamQueryID mints a `Query` response's QueryId from m — 32 lowercase hex characters, which is
// what the shared crypto/rand helper produced at the site this replaces and what API_query_Query
// permits: `QueryId` is 1–64 characters matching `[a-zA-Z0-9]+`, so hex is inside the published
// alphabet where a UUID's hyphens would not be.
//
// It is the loosest case in this family, because substrate's Query is synchronous and its QueryId
// reaches nothing: `CancelQuery` ignores the ID it is given and answers an empty body, and there is no
// asynchronous read path to address. So deriving it does not repair a broken follow-on call the way
// Athena's and Redshift Data's do — it removes the last fresh draw from the response body, which is
// what makes a recorded Query replay with zero differences at all.
func timestreamQueryID(m *IDMint) string {
	return m.Hex(16)
}

// seededQueryResult returns the result seeded for the query string, falling back to the wildcard
// "*" seed, and reports whether either was found.
func (p *TimestreamPlugin) seededQueryResult(qs string) (TimestreamQueryResult, bool, error) {
	goCtx := context.Background()
	for _, key := range []string{qs, "*"} {
		raw, err := p.state.Get(goCtx, timestreamCtrlNamespace, timestreamCtrlResultKey(key))
		if err != nil {
			return TimestreamQueryResult{}, false, fmt.Errorf("timestream query read seed: %w", err)
		}
		if raw == nil {
			continue
		}
		var result TimestreamQueryResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return TimestreamQueryResult{}, false, fmt.Errorf("timestream query decode seed: %w", err)
		}
		return result, true, nil
	}
	return TimestreamQueryResult{}, false, nil
}

// --- Load helpers ---

func (p *TimestreamPlugin) loadDatabase(acct, region, name string) (TimestreamDatabase, error) {
	raw, err := p.state.Get(context.Background(), timestreamNamespace, timestreamDBKey(acct, region, name))
	if err != nil {
		return TimestreamDatabase{}, fmt.Errorf("timestream load database: %w", err)
	}
	if raw == nil {
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
	if err != nil {
		return TimestreamTable{}, fmt.Errorf("timestream load table: %w", err)
	}
	if raw == nil {
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
