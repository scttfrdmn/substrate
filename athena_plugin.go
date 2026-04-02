package substrate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// athenaNamespace is the state namespace for Amazon Athena.
const athenaNamespace = "athena"

// athenaCtrlNamespace is the state namespace for Athena HTTP control-plane seeds.
const athenaCtrlNamespace = "athena-ctrl"

// AthenaResultSet holds seeded query result data for GetQueryResults.
// It is populated via POST /v1/athena/results and returned by GetQueryResults.
type AthenaResultSet struct {
	// Rows contains the result rows returned to the caller.
	Rows []AthenaResultRow `json:"Rows"`
	// ColumnInfo describes the columns in the result set.
	ColumnInfo []AthenaColumnInfo `json:"ColumnInfo"`
}

// AthenaResultRow is a single result row in an Athena result set.
type AthenaResultRow struct {
	// Data holds the cell values for this row.
	Data []AthenaValue `json:"Data"`
}

// AthenaValue is a single cell value in an Athena result row.
type AthenaValue struct {
	// VarCharValue is the string representation of the cell value.
	VarCharValue string `json:"VarCharValue"`
}

// AthenaColumnInfo describes a single column in an Athena result set.
type AthenaColumnInfo struct {
	// Name is the column name.
	Name string `json:"Name"`
	// Type is the column data type.
	Type string `json:"Type"`
}

// AthenaWorkGroup holds persisted state for an Athena workgroup.
type AthenaWorkGroup struct {
	// Name is the workgroup name.
	Name string `json:"Name"`
	// State is "ENABLED" or "DISABLED".
	State string `json:"State"`
	// Description is an optional description.
	Description string `json:"Description"`
}

// AthenaPlugin emulates the Amazon Athena service.
// It handles the async query lifecycle (StartQueryExecution, GetQueryExecution,
// GetQueryResults, StopQueryExecution) using the Athena JSON-target protocol
// (X-Amz-Target: AmazonAthena.{Op}).
//
// Queries immediately transition to SUCCEEDED (deterministic), so the polling
// loop in callers like clAWS exits on the first GetQueryExecution call.
type AthenaPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "athena".
func (p *AthenaPlugin) Name() string { return athenaNamespace }

// Initialize sets up the AthenaPlugin with the provided configuration.
func (p *AthenaPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for AthenaPlugin.
func (p *AthenaPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an Athena JSON-target request to the appropriate handler.
func (p *AthenaPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "StartQueryExecution":
		return p.startQueryExecution(ctx, req)
	case "GetQueryExecution":
		return p.getQueryExecution(ctx, req)
	case "GetQueryResults":
		return p.getQueryResults(ctx, req)
	case "StopQueryExecution":
		return p.stopQueryExecution(ctx, req)
	case "ListQueryExecutions":
		return p.listQueryExecutions(ctx, req)
	case "CreateWorkGroup":
		return p.createWorkGroup(ctx, req)
	case "GetWorkGroup":
		return p.getWorkGroup(ctx, req)
	case "DeleteWorkGroup":
		return p.deleteWorkGroup(ctx, req)
	case "ListWorkGroups":
		return p.listWorkGroups(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "AthenaPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// AthenaQuery holds persisted state for an Athena query execution.
type AthenaQuery struct {
	// QueryExecutionID is the unique identifier for the query execution.
	QueryExecutionID string `json:"QueryExecutionId"`

	// Query is the SQL query string.
	Query string `json:"Query"`

	// WorkGroup is the workgroup used for the query.
	WorkGroup string `json:"WorkGroup"`

	// OutputLocation is the S3 output location for results.
	OutputLocation string `json:"OutputLocation"`

	// State is the query state (SUCCEEDED, CANCELED, FAILED).
	State string `json:"State"`

	// SubmissionDateTime is the epoch-seconds timestamp of query submission.
	SubmissionDateTime float64 `json:"SubmissionDateTime"`

	// CompletionDateTime is the epoch-seconds timestamp of query completion.
	CompletionDateTime float64 `json:"CompletionDateTime"`

	// AccountID is the AWS account that owns the query.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the query runs.
	Region string `json:"Region"`
}

func (p *AthenaPlugin) startQueryExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		QueryString         string `json:"QueryString"`
		WorkGroup           string `json:"WorkGroup"`
		ResultConfiguration *struct {
			OutputLocation string `json:"OutputLocation"`
		} `json:"ResultConfiguration"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	qID := generateAthenaQueryID()
	now := float64(p.tc.Now().UnixNano()) / 1e9
	outputLoc := ""
	if body.ResultConfiguration != nil {
		outputLoc = body.ResultConfiguration.OutputLocation
	}
	wg := body.WorkGroup
	if wg == "" {
		wg = "primary"
	}

	q := AthenaQuery{
		QueryExecutionID:   qID,
		Query:              body.QueryString,
		WorkGroup:          wg,
		OutputLocation:     outputLoc,
		State:              "SUCCEEDED",
		SubmissionDateTime: now,
		CompletionDateTime: now,
		AccountID:          ctx.AccountID,
		Region:             ctx.Region,
	}

	goCtx := context.Background()
	key := "query:" + ctx.AccountID + "/" + ctx.Region + "/" + qID
	data, err := json.Marshal(q)
	if err != nil {
		return nil, fmt.Errorf("startQueryExecution: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, athenaNamespace, key, data); err != nil {
		return nil, fmt.Errorf("startQueryExecution: put: %w", err)
	}

	// Append to global query ID index for ListQueryExecutions.
	if err := athenaAppendStringIndex(goCtx, p.state, "query_ids:"+ctx.AccountID+"/"+ctx.Region, qID); err != nil {
		return nil, fmt.Errorf("startQueryExecution: update index: %w", err)
	}

	return athenaJSONResponse(http.StatusOK, map[string]string{"QueryExecutionId": qID})
}

func (p *AthenaPlugin) getQueryExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.QueryExecutionID == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "QueryExecutionId is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := "query:" + ctx.AccountID + "/" + ctx.Region + "/" + body.QueryExecutionID
	data, err := p.state.Get(goCtx, athenaNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{
			Code:       "InvalidRequestException",
			Message:    "Query execution " + body.QueryExecutionID + " not found",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var q AthenaQuery
	if err := json.Unmarshal(data, &q); err != nil {
		return nil, fmt.Errorf("getQueryExecution: unmarshal: %w", err)
	}

	return athenaJSONResponse(http.StatusOK, map[string]interface{}{
		"QueryExecution": map[string]interface{}{
			"QueryExecutionId": q.QueryExecutionID,
			"Query":            q.Query,
			"WorkGroup":        q.WorkGroup,
			"Status": map[string]interface{}{
				"State":              q.State,
				"SubmissionDateTime": q.SubmissionDateTime,
				"CompletionDateTime": q.CompletionDateTime,
			},
			"ResultConfiguration": map[string]string{
				"OutputLocation": q.OutputLocation,
			},
		},
	})
}

func (p *AthenaPlugin) getQueryResults(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.QueryExecutionID == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "QueryExecutionId is required", HTTPStatus: http.StatusBadRequest}
	}

	// Verify the query exists and load its SQL for result lookup.
	goCtx := context.Background()
	key := "query:" + ctx.AccountID + "/" + ctx.Region + "/" + body.QueryExecutionID
	data, err := p.state.Get(goCtx, athenaNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{
			Code:       "InvalidRequestException",
			Message:    "Query execution " + body.QueryExecutionID + " not found",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var q AthenaQuery
	if err := json.Unmarshal(data, &q); err != nil {
		return nil, fmt.Errorf("getQueryResults: unmarshal: %w", err)
	}

	// Look up seeded result by SQL (exact match, then wildcard "*").
	rs := p.lookupAthenaResult(q.Query)
	return athenaJSONResponse(http.StatusOK, map[string]interface{}{
		"ResultSet": map[string]interface{}{
			"Rows":              rs.Rows,
			"ResultSetMetadata": map[string]interface{}{"ColumnInfo": rs.ColumnInfo},
		},
		"UpdateCount": 0,
	})
}

func (p *AthenaPlugin) stopQueryExecution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.QueryExecutionID == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "QueryExecutionId is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := "query:" + ctx.AccountID + "/" + ctx.Region + "/" + body.QueryExecutionID
	data, err := p.state.Get(goCtx, athenaNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{
			Code:       "InvalidRequestException",
			Message:    "Query execution " + body.QueryExecutionID + " not found",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var q AthenaQuery
	if err := json.Unmarshal(data, &q); err != nil {
		return nil, fmt.Errorf("stopQueryExecution: unmarshal: %w", err)
	}
	q.State = "CANCELED"
	updated, _ := json.Marshal(q)
	if err := p.state.Put(goCtx, athenaNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("stopQueryExecution: put: %w", err)
	}
	return athenaJSONResponse(http.StatusOK, map[string]interface{}{})
}

// listQueryExecutions returns IDs of all query executions, optionally filtered by workgroup.
func (p *AthenaPlugin) listQueryExecutions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkGroup  string `json:"WorkGroup"`
		MaxResults int    `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(req.Body, &body)

	goCtx := context.Background()
	idsKey := "query_ids:" + ctx.AccountID + "/" + ctx.Region
	ids := athenaLoadStringIndex(goCtx, p.state, idsKey)

	// Filter by workgroup if requested.
	if body.WorkGroup != "" {
		var filtered []string
		for _, id := range ids {
			qKey := "query:" + ctx.AccountID + "/" + ctx.Region + "/" + id
			d, _ := p.state.Get(goCtx, athenaNamespace, qKey)
			if d == nil {
				continue
			}
			var q AthenaQuery
			if err := json.Unmarshal(d, &q); err != nil {
				continue
			}
			if q.WorkGroup == body.WorkGroup {
				filtered = append(filtered, id)
			}
		}
		ids = filtered
	}

	maxResults := body.MaxResults
	if maxResults <= 0 {
		maxResults = 50
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(body.NextToken); err == nil {
			if n, err := strconv.Atoi(string(decoded)); err == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(ids) {
		offset = len(ids)
	}
	page := ids[offset:]
	var nextToken string
	if len(page) > maxResults {
		page = page[:maxResults]
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + maxResults)))
	}
	if page == nil {
		page = []string{}
	}
	resp := map[string]interface{}{"QueryExecutionIds": page}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}
	return athenaJSONResponse(http.StatusOK, resp)
}

// createWorkGroup creates a new Athena workgroup.
func (p *AthenaPlugin) createWorkGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.Name == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	wgKey := "workgroup:" + ctx.AccountID + "/" + ctx.Region + "/" + body.Name
	existing, _ := p.state.Get(goCtx, athenaNamespace, wgKey)
	if existing != nil {
		return nil, &AWSError{
			Code:       "InvalidRequestException",
			Message:    "WorkGroup " + body.Name + " already exists",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	wg := AthenaWorkGroup{Name: body.Name, State: "ENABLED", Description: body.Description}
	data, err := json.Marshal(wg)
	if err != nil {
		return nil, fmt.Errorf("createWorkGroup: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, athenaNamespace, wgKey, data); err != nil {
		return nil, fmt.Errorf("createWorkGroup: put: %w", err)
	}
	namesKey := "workgroup_names:" + ctx.AccountID + "/" + ctx.Region
	if err := athenaAppendStringIndex(goCtx, p.state, namesKey, body.Name); err != nil {
		return nil, fmt.Errorf("createWorkGroup: update index: %w", err)
	}
	return athenaJSONResponse(http.StatusOK, map[string]interface{}{})
}

// getWorkGroup returns an Athena workgroup. The "primary" workgroup auto-exists.
func (p *AthenaPlugin) getWorkGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkGroup string `json:"WorkGroup"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.WorkGroup == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "WorkGroup is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	wgKey := "workgroup:" + ctx.AccountID + "/" + ctx.Region + "/" + body.WorkGroup
	data, _ := p.state.Get(goCtx, athenaNamespace, wgKey)

	var wg AthenaWorkGroup
	if data == nil {
		// "primary" always exists as the default workgroup.
		if body.WorkGroup != "primary" {
			return nil, &AWSError{
				Code:       "InvalidRequestException",
				Message:    "WorkGroup " + body.WorkGroup + " not found",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		wg = AthenaWorkGroup{Name: "primary", State: "ENABLED", Description: "Primary workgroup"}
	} else {
		if err := json.Unmarshal(data, &wg); err != nil {
			return nil, fmt.Errorf("getWorkGroup: unmarshal: %w", err)
		}
	}

	return athenaJSONResponse(http.StatusOK, map[string]interface{}{
		"WorkGroup": map[string]interface{}{
			"Name":        wg.Name,
			"State":       wg.State,
			"Description": wg.Description,
		},
	})
}

// deleteWorkGroup deletes an Athena workgroup.
func (p *AthenaPlugin) deleteWorkGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkGroup string `json:"WorkGroup"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.WorkGroup == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "WorkGroup is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	wgKey := "workgroup:" + ctx.AccountID + "/" + ctx.Region + "/" + body.WorkGroup
	existing, _ := p.state.Get(goCtx, athenaNamespace, wgKey)
	if existing == nil {
		return nil, &AWSError{
			Code:       "InvalidRequestException",
			Message:    "WorkGroup " + body.WorkGroup + " not found",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err := p.state.Delete(goCtx, athenaNamespace, wgKey); err != nil {
		return nil, fmt.Errorf("deleteWorkGroup: delete: %w", err)
	}
	namesKey := "workgroup_names:" + ctx.AccountID + "/" + ctx.Region
	athenaRemoveStringIndex(goCtx, p.state, namesKey, body.WorkGroup)
	return athenaJSONResponse(http.StatusOK, map[string]interface{}{})
}

// listWorkGroups returns all Athena workgroups.
func (p *AthenaPlugin) listWorkGroups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		MaxResults int    `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(req.Body, &body)

	goCtx := context.Background()
	namesKey := "workgroup_names:" + ctx.AccountID + "/" + ctx.Region
	names := athenaLoadStringIndex(goCtx, p.state, namesKey)

	maxResults := body.MaxResults
	if maxResults <= 0 {
		maxResults = 50
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(body.NextToken); err == nil {
			if n, err := strconv.Atoi(string(decoded)); err == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var nextToken string
	if len(page) > maxResults {
		page = page[:maxResults]
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + maxResults)))
	}

	wgs := make([]map[string]interface{}, 0, len(page))
	for _, name := range page {
		wgKey := "workgroup:" + ctx.AccountID + "/" + ctx.Region + "/" + name
		d, _ := p.state.Get(goCtx, athenaNamespace, wgKey)
		if d == nil {
			continue
		}
		var wg AthenaWorkGroup
		if err := json.Unmarshal(d, &wg); err != nil {
			continue
		}
		wgs = append(wgs, map[string]interface{}{
			"Name":  wg.Name,
			"State": wg.State,
		})
	}
	resp := map[string]interface{}{"WorkGroups": wgs}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}
	return athenaJSONResponse(http.StatusOK, resp)
}

// lookupAthenaResult returns a seeded AthenaResultSet for the given SQL query.
// It checks the state by exact SQL match, then by the "*" wildcard, then returns an empty set.
func (p *AthenaPlugin) lookupAthenaResult(sql string) *AthenaResultSet {
	if r := p.loadAthenaStateResult(sql); r != nil {
		return r
	}
	if r := p.loadAthenaStateResult("*"); r != nil {
		return r
	}
	return &AthenaResultSet{Rows: []AthenaResultRow{}, ColumnInfo: []AthenaColumnInfo{}}
}

// loadAthenaStateResult loads a seeded AthenaResultSet from the HTTP control-plane state.
func (p *AthenaPlugin) loadAthenaStateResult(sql string) *AthenaResultSet {
	data, err := p.state.Get(context.Background(), athenaCtrlNamespace, "result:"+sql)
	if err != nil || data == nil {
		return nil
	}
	var rs AthenaResultSet
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil
	}
	return &rs
}

// athenaAppendStringIndex loads a JSON []string index from state, appends value, and re-stores it.
func athenaAppendStringIndex(ctx context.Context, state StateManager, key, value string) error {
	existing := athenaLoadStringIndex(ctx, state, key)
	existing = append(existing, value)
	data, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("athenaAppendStringIndex: marshal: %w", err)
	}
	return state.Put(ctx, athenaNamespace, key, data)
}

// athenaRemoveStringIndex loads a JSON []string index, removes value, and re-stores it.
func athenaRemoveStringIndex(ctx context.Context, state StateManager, key, value string) {
	existing := athenaLoadStringIndex(ctx, state, key)
	filtered := existing[:0]
	for _, v := range existing {
		if v != value {
			filtered = append(filtered, v)
		}
	}
	data, _ := json.Marshal(filtered)
	_ = state.Put(ctx, athenaNamespace, key, data)
}

// athenaLoadStringIndex loads a JSON []string from state, returning nil slice on missing/error.
func athenaLoadStringIndex(ctx context.Context, state StateManager, key string) []string {
	data, err := state.Get(ctx, athenaNamespace, key)
	if err != nil || data == nil {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil
	}
	return ids
}

// generateAthenaQueryID generates a UUID-formatted query execution ID.
func generateAthenaQueryID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// athenaJSONResponse serializes v to JSON and returns an AWSResponse.
func athenaJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("athena json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
