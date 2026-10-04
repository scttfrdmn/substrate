package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
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

	existing, err := p.state.Get(goCtx, timestreamNamespace, timestreamDBKey(acct, region, input.DatabaseName))
	if err != nil {
		return nil, fmt.Errorf("timestream createDatabase read: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ConflictException", Message: "Database already exists: " + input.DatabaseName, HTTPStatus: http.StatusBadRequest}
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

// listDatabases answers the account's databases in name order, paged by MaxResults and NextToken;
// see emulator/timestream_pagination.go.
func (p *TimestreamPlugin) listDatabases(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults *int   `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, timestreamInvalidBody()
		}
	}
	offset, pageSize, err := timestreamListPage(input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}
	acct, region := reqCtx.AccountID, reqCtx.Region
	names, err := loadStringIndex(context.Background(), p.state, timestreamNamespace, timestreamDBNamesKey(acct, region))
	if err != nil {
		return nil, fmt.Errorf("timestream listDatabases index: %w", err)
	}
	// The offset is meaningful only over a stable order, so the names are sorted first.
	sort.Strings(names)
	page, next := pageByOffsetToken(names, offset, pageSize)
	dbs := make([]timestreamDatabaseOut, 0, len(page))
	for _, name := range page {
		db, err := p.loadDatabase(acct, region, name)
		if err != nil {
			var awsErr *AWSError
			if errors.As(err, &awsErr) {
				continue // indexed but deleted between the index read and this one
			}
			return nil, err
		}
		dbs = append(dbs, timestreamDatabaseToWire(db))
	}
	out := map[string]any{"Databases": dbs}
	if next != "" {
		out["NextToken"] = next
	}
	return timestreamJSONResponse(http.StatusOK, out)
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
	existing, err := p.state.Get(goCtx, timestreamNamespace, timestreamTableKey(acct, region, input.DatabaseName, input.TableName))
	if err != nil {
		return nil, fmt.Errorf("timestream createTable read: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ConflictException", Message: "Table already exists: " + input.TableName, HTTPStatus: http.StatusBadRequest}
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

// listTables answers tables in (database, table) name order, paged by MaxResults and NextToken.
//
// `DatabaseName` is "Required: No" on API_ListTables. Given, it narrows the listing to that database,
// and a database that does not exist is ResourceNotFoundException, which the page publishes. Absent,
// every database's tables are listed; before #1195 an absent DatabaseName listed the tables of a
// database named "", which is none.
func (p *TimestreamPlugin) listTables(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DatabaseName string `json:"DatabaseName"`
		MaxResults   *int   `json:"MaxResults"`
		NextToken    string `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, timestreamInvalidBody()
		}
	}
	offset, pageSize, err := timestreamListPage(input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	var dbNames []string
	if input.DatabaseName != "" {
		if _, err := p.loadDatabase(acct, region, input.DatabaseName); err != nil {
			return nil, err
		}
		dbNames = []string{input.DatabaseName}
	} else if dbNames, err = loadStringIndex(goCtx, p.state, timestreamNamespace, timestreamDBNamesKey(acct, region)); err != nil {
		return nil, fmt.Errorf("timestream listTables database index: %w", err)
	}
	sort.Strings(dbNames)
	type tableRef struct{ db, table string }
	var refs []tableRef
	for _, db := range dbNames {
		names, err := loadStringIndex(goCtx, p.state, timestreamNamespace, timestreamTableNamesKey(acct, region, db))
		if err != nil {
			return nil, fmt.Errorf("timestream listTables table index: %w", err)
		}
		sort.Strings(names)
		for _, n := range names {
			refs = append(refs, tableRef{db, n})
		}
	}
	page, next := pageByOffsetToken(refs, offset, pageSize)
	tables := make([]timestreamTableOut, 0, len(page))
	for _, ref := range page {
		tbl, err := p.loadTable(acct, region, ref.db, ref.table)
		if err != nil {
			var awsErr *AWSError
			if errors.As(err, &awsErr) {
				continue // indexed but deleted between the index read and this one
			}
			return nil, err
		}
		tables = append(tables, timestreamTableToWire(tbl))
	}
	out := map[string]any{"Tables": tables}
	if next != "" {
		out["NextToken"] = next
	}
	return timestreamJSONResponse(http.StatusOK, out)
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
	// API_WriteRecords: Records is "Required: Yes", "Minimum number of 1 item. Maximum number of 100
	// items". A batch outside that is a malformed request, ValidationException; RejectedRecordsException
	// is the page's code for records rejected on their content, which substrate does not evaluate.
	if n := len(input.Records); n < 1 || n > timestreamRecordsMax {
		return nil, timestreamValidation(fmt.Sprintf("Records must contain 1 to %d records, got %d", timestreamRecordsMax, n))
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
// refuses any other unseeded query (emulator/timestream_query.go). MaxRows and NextToken page the
// result, and every query is recorded so CancelQuery can find it (emulator/timestream_pagination.go).
func (p *TimestreamPlugin) query(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input timestreamQueryInput
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, timestreamInvalidBody()
		}
	}
	if err := input.validate(); err != nil {
		return nil, err
	}

	if input.NextToken != "" {
		id, offset, ok := decodeTimestreamQueryToken(input.NextToken)
		if !ok {
			return nil, timestreamValidation("Invalid pagination token")
		}
		q, err := p.loadIssuedQuery(reqCtx, id)
		if err != nil {
			return nil, err
		}
		if q == nil || q.QueryString != input.QueryString {
			return nil, timestreamValidation("Invalid pagination token")
		}
		if q.Cancelled {
			return nil, &AWSError{Code: "ConflictException", Message: "Unable to poll results for a cancelled query.", HTTPStatus: http.StatusBadRequest}
		}
		if q.Completed {
			return nil, timestreamValidation("Invalid pagination token")
		}
		return p.queryPage(reqCtx, q, offset)
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
	q := &timestreamIssuedQuery{
		QueryID:     timestreamQueryID(reqCtx.IDs),
		QueryString: input.QueryString,
		Status:      result.QueryStatus,
	}
	// "The initial run of Query with a MaxRows value specified will return the result set of the query
	// in two cases: … The number of rows in the result set is less than the value of maxRows. Otherwise,
	// the initial invocation of Query only returns a NextToken" (API_query_Query).
	if input.MaxRows != nil && len(rows) >= *input.MaxRows {
		q.ColumnInfo, q.Rows, q.PageSize = result.ColumnInfo, rows, *input.MaxRows
		return p.queryResponse(reqCtx, q, []TimestreamRow{}, result.ColumnInfo, encodeTimestreamQueryToken(q.QueryID, 0))
	}
	q.Completed = true
	return p.queryResponse(reqCtx, q, rows, result.ColumnInfo, "")
}

// timestreamQueryID mints a `Query` response's QueryId from m — 32 lowercase hex characters, which is
// what the shared crypto/rand helper produced at the site this replaces and what API_query_Query
// permits: `QueryId` is 1–64 characters matching `[a-zA-Z0-9]+`, so hex is inside the published
// alphabet where a UUID's hyphens would not be.
//
// Deriving it is what makes a recorded Query replay with zero differences, and since #1206 the ID is
// also what CancelQuery and a NextToken address, so a replayed cancellation finds the query its
// recording ran.
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
			HTTPStatus: http.StatusBadRequest,
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
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var tbl TimestreamTable
	if err2 := json.Unmarshal(raw, &tbl); err2 != nil {
		return TimestreamTable{}, fmt.Errorf("unmarshal timestream table: %w", err2)
	}
	return tbl, nil
}
