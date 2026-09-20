package emulator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
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
		return nil, unknownActionError(p.Name(), req.Operation)
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
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, athenaInvalidBody()
		}
	}

	// Before the query index is read, so the refusal does not depend on what the store holds — see
	// athena_pagination.go for the code's provenance and the ordering argument (#1086).
	offset, tokenOK := decodeOffsetPaginationToken(body.NextToken)
	if !tokenOK {
		return nil, athenaInvalidPaginationToken("ListQueryExecutions")
	}

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
		maxResults = athenaListDefaultPageSize
	}

	// The index is appended to in submission order, which is stable across calls and is what
	// [pageByOffsetToken]'s offset relies on. The workgroup filter above preserves that order, so the
	// offset counts the records this request can see rather than every query in the account.
	page, nextToken := pageByOffsetToken(ids, offset, maxResults)
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

// athenaPrimaryWorkGroupName is the workgroup Athena gives every account, and the one
// StartQueryExecution attributes a query to when the request names none.
//
// It is not substrate's convention. API_DeleteWorkGroup's own description states "The primary
// workgroup cannot be deleted", and the Athena user guide's Manage workgroups page repeats it verbatim
// in its delete row — so the name is published by the API reference, not merely by the guide.
const athenaPrimaryWorkGroupName = "primary"

// athenaPrimaryWorkGroup is the record substrate synthesizes for the workgroup that exists without
// having been created.
//
// One producer, because until #1222 there were nearly two: getWorkGroup synthesized this record while
// listWorkGroups read only the workgroup-names index, which CreateWorkGroup alone appends to. So
// GetWorkGroup reported an ENABLED primary, ListWorkGroups reported an empty list, and
// ListQueryExecutions reported every unqualified query under a workgroup the listing said did not
// exist — three answers that cannot all be true of one account. A second copy of the literal would
// have let them drift apart again.
//
// State is ENABLED because the workgroup is usable: a query naming no workgroup is attributed to it and
// runs, and API_WorkGroupSummary publishes only ENABLED and DISABLED, so a usable workgroup has exactly
// one of the two values available. Description is substrate's own placeholder — AWS publishes no
// description for the primary workgroup, and a real one has none — and is recorded as such in
// docs/services.md rather than presented as a read.
func athenaPrimaryWorkGroup() AthenaWorkGroup {
	return AthenaWorkGroup{
		Name:        athenaPrimaryWorkGroupName,
		State:       "ENABLED",
		Description: "Primary workgroup",
	}
}

// loadWorkGroup returns the workgroup a name identifies, synthesizing the primary one when no record
// was ever written for it, or nil when the name is neither.
//
// The single reader behind getWorkGroup and listWorkGroups, so the two cannot disagree about whether a
// workgroup exists or about what it says — which is the whole of #1222. A caller that has created a
// workgroup of its own named `primary` gets that record instead, because the stored one is read first:
// CreateWorkGroup writes the key unconditionally, and answering the synthesized record over a real one
// would discard a write.
func (p *AthenaPlugin) loadWorkGroup(goCtx context.Context, ctx *RequestContext, name string) (*AthenaWorkGroup, error) {
	wgKey := "workgroup:" + ctx.AccountID + "/" + ctx.Region + "/" + name
	data, err := p.state.Get(goCtx, athenaNamespace, wgKey)
	if err != nil {
		return nil, fmt.Errorf("athena loadWorkGroup get: %w", err)
	}
	if data == nil {
		if name != athenaPrimaryWorkGroupName {
			return nil, nil
		}
		wg := athenaPrimaryWorkGroup()
		return &wg, nil
	}
	var wg AthenaWorkGroup
	if err := json.Unmarshal(data, &wg); err != nil {
		return nil, fmt.Errorf("athena loadWorkGroup unmarshal: %w", err)
	}
	return &wg, nil
}

// getWorkGroup returns an Athena workgroup. The "primary" workgroup auto-exists.
func (p *AthenaPlugin) getWorkGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkGroup string `json:"WorkGroup"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.WorkGroup == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "WorkGroup is required", HTTPStatus: http.StatusBadRequest}
	}

	wg, err := p.loadWorkGroup(context.Background(), ctx, body.WorkGroup)
	if err != nil {
		return nil, err
	}
	if wg == nil {
		return nil, athenaNoSuchWorkGroup(body.WorkGroup)
	}

	return athenaJSONResponse(http.StatusOK, map[string]interface{}{
		"WorkGroup": athenaWorkGroupMembers(wg),
	})
}

// athenaWorkGroupMembers renders the members API_WorkGroup and API_WorkGroupSummary have in common.
//
// One renderer for both, so GetWorkGroup and ListWorkGroups cannot describe one workgroup two ways —
// the same reason loadWorkGroup is one reader (#1222). The two shapes are not identical on the wire
// (API_WorkGroup adds Configuration, API_WorkGroupSummary adds EngineVersion and
// IdentityCenterApplicationArn, neither of which substrate carries), but every member substrate does
// carry appears on both pages, so nothing here is published by only one of them.
//
// Description is omitted when empty rather than sent as "": both pages give it "Required: No" with a
// minimum length of 0, and a workgroup created without one has no description rather than an empty
// one. Before #1222 GetWorkGroup sent the empty string and ListWorkGroups sent no Description member
// at all, so a caller comparing the two readers' answers found them different for a workgroup neither
// reader was wrong about.
func athenaWorkGroupMembers(wg *AthenaWorkGroup) map[string]interface{} {
	members := map[string]interface{}{
		"Name":  wg.Name,
		"State": wg.State,
	}
	if wg.Description != "" {
		members["Description"] = wg.Description
	}
	return members
}

// athenaNoSuchWorkGroup reports that no workgroup of that name exists.
//
// InvalidRequestException/400 is the only 400-class error API_GetWorkGroup and API_DeleteWorkGroup
// publish — "Indicates that something is wrong with the input to the request. For example, a required
// parameter may be missing or out of range" — so an absent workgroup has no more specific code to be
// answered with. The message text is substrate's; the reference publishes codes and not messages.
func athenaNoSuchWorkGroup(name string) *AWSError {
	return &AWSError{
		Code:       "InvalidRequestException",
		Message:    "WorkGroup " + name + " not found",
		HTTPStatus: http.StatusBadRequest,
	}
}

// deleteWorkGroup deletes an Athena workgroup, refusing the primary one.
//
// API_DeleteWorkGroup's description states "The primary workgroup cannot be deleted", and the user
// guide's Manage workgroups page repeats it verbatim, so the refusal is a read rather than an
// inference. Before #1222 the answer was "WorkGroup primary not found" — wrong under either reading of
// the page, since GetWorkGroup reported the same workgroup as existing in the same breath.
//
// The check precedes the record load, because the primary workgroup has no record and an
// existence-first order would reach the not-found arm and answer the wrong reason. It is deliberately
// on the *name* and not on the absence of a record: a caller that created its own workgroup named
// `primary` still cannot delete it, which is what AWS's flat statement says.
//
// InvalidRequestException/400 is the code, being the only 400-class error the page publishes. AWS
// publishes no message for the refusal, so the message restates the published sentence.
func (p *AthenaPlugin) deleteWorkGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		WorkGroup string `json:"WorkGroup"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.WorkGroup == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "WorkGroup is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.WorkGroup == athenaPrimaryWorkGroupName {
		return nil, &AWSError{
			Code:       "InvalidRequestException",
			Message:    "The primary workgroup cannot be deleted",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	wgKey := "workgroup:" + ctx.AccountID + "/" + ctx.Region + "/" + body.WorkGroup
	existing, _ := p.state.Get(goCtx, athenaNamespace, wgKey)
	if existing == nil {
		return nil, athenaNoSuchWorkGroup(body.WorkGroup)
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
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, athenaInvalidBody()
		}
	}

	// Before the workgroup-names index is read, for the reason listQueryExecutions states (#1086).
	offset, tokenOK := decodeOffsetPaginationToken(body.NextToken)
	if !tokenOK {
		return nil, athenaInvalidPaginationToken("ListWorkGroups")
	}

	goCtx := context.Background()
	namesKey := "workgroup_names:" + ctx.AccountID + "/" + ctx.Region
	names := athenaLoadStringIndex(goCtx, p.state, namesKey)

	// The primary workgroup is prepended rather than appended, and the choice matters to the offset
	// pagination #1086 converted this walk to. It exists before any workgroup a caller creates, so
	// creation order puts it first — and prepending is the only position that keeps every other entry's
	// offset stable, where appending would move it on each CreateWorkGroup and leave a token issued
	// mid-walk pointing at a different element.
	//
	// The guard is against the index rather than against the record, because the index is what this walk
	// orders: CreateWorkGroup appends the name it was given, so a caller that created its own workgroup
	// named `primary` is already in the list and must not appear twice.
	if !slices.Contains(names, athenaPrimaryWorkGroupName) {
		names = append([]string{athenaPrimaryWorkGroupName}, names...)
	}

	maxResults := body.MaxResults
	if maxResults <= 0 {
		maxResults = athenaListDefaultPageSize
	}

	// The index is appended to in creation order, which is stable across calls and is what
	// [pageByOffsetToken]'s offset relies on. A workgroup whose record no longer loads is skipped below,
	// so a page can be shorter than maxResults while a token is still emitted; the offset counts index
	// entries rather than rendered members, so the walk stays coherent.
	page, nextToken := pageByOffsetToken(names, offset, maxResults)

	wgs := make([]map[string]interface{}, 0, len(page))
	for _, name := range page {
		// The same reader GetWorkGroup uses, which is what makes the two agree about the primary
		// workgroup: it has no record, so reading state directly here is what left it out of every
		// listing (#1222). A load error is skipped rather than returned, matching the unmarshal skip
		// this loop already made — a listing drops an unreadable record instead of failing wholesale.
		wg, err := p.loadWorkGroup(goCtx, ctx, name)
		if err != nil || wg == nil {
			continue
		}
		wgs = append(wgs, athenaWorkGroupMembers(wg))
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
