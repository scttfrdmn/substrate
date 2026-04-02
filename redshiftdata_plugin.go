package substrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RedshiftDataPlugin emulates the AWS Redshift Data API service (redshift-data).
// It supports executing SQL statements and fetching results deterministically —
// statements complete synchronously (Status: "FINISHED") with no simulated latency.
// GetStatementResult returns configurable rows seeded via the plugin options.
type RedshiftDataPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// results holds pre-seeded statement results keyed by SQL or by "*" (wildcard).
	results map[string]*RedshiftDataResult
}

// RedshiftDataResult holds configurable result data for GetStatementResult.
// It can be pre-seeded into a RedshiftDataPlugin via the "results" option.
type RedshiftDataResult struct {
	// ColumnMetadata describes the columns in the result set.
	ColumnMetadata []RedshiftDataColumnMetadata `json:"ColumnMetadata"`
	// Records holds the result rows; each row is a list of typed field values.
	Records [][]RedshiftDataField `json:"Records"`
}

// Name returns the service name "redshift-data".
func (p *RedshiftDataPlugin) Name() string { return "redshift-data" }

// Initialize sets up the RedshiftDataPlugin with the provided configuration.
// An optional "results" key in Options may hold a
// map[string]*RedshiftDataResult or map[string]interface{} of pre-seeded results.
func (p *RedshiftDataPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	if r, ok := cfg.Options["results"].(map[string]*RedshiftDataResult); ok {
		p.results = r
	} else {
		p.results = make(map[string]*RedshiftDataResult)
	}
	return nil
}

// Shutdown is a no-op for RedshiftDataPlugin.
func (p *RedshiftDataPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Redshift Data API JSON-target request.
func (p *RedshiftDataPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "ExecuteStatement":
		return p.executeStatement(reqCtx, req)
	case "DescribeStatement":
		return p.describeStatement(reqCtx, req)
	case "GetStatementResult":
		return p.getStatementResult(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "RedshiftDataPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *RedshiftDataPlugin) executeStatement(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		Database          string `json:"Database"`
		SecretArn         string `json:"SecretArn"`
		Sql               string `json:"Sql"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "ValidationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Sql == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Sql is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Resolve statement status from HTTP control-plane override (or default to FINISHED).
	status := "FINISHED"
	errMsg := ""
	if d, _ := p.state.Get(goCtx, redshiftDataCtrlNamespace, redshiftDataCtrlStatusKey); d != nil {
		status = strings.TrimSpace(string(d))
	}
	if status == "FAILED" {
		if d, _ := p.state.Get(goCtx, redshiftDataCtrlNamespace, redshiftDataCtrlErrorKey); d != nil {
			errMsg = strings.TrimSpace(string(d))
		}
	}

	id := generateRedshiftDataID()
	stmt := RedshiftDataStatement{
		ID:                id,
		Status:            status,
		Error:             errMsg,
		QueryString:       input.Sql,
		WorkgroupName:     input.WorkgroupName,
		ClusterIdentifier: input.ClusterIdentifier,
		Database:          input.Database,
		CreatedAt:         p.tc.Now(),
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}

	d, err := json.Marshal(stmt)
	if err != nil {
		return nil, fmt.Errorf("redshift-data executeStatement marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftDataNamespace, redshiftDataStatementKey(reqCtx.AccountID, reqCtx.Region, id), d); err != nil {
		return nil, fmt.Errorf("redshift-data executeStatement put: %w", err)
	}

	return redshiftDataJSONResponse(http.StatusOK, map[string]interface{}{
		"Id": id,
	})
}

func (p *RedshiftDataPlugin) describeStatement(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID string `json:"Id"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	stmt, err := p.loadStatement(reqCtx.AccountID, reqCtx.Region, input.ID)
	if err != nil {
		return nil, err
	}

	resp := map[string]interface{}{
		"Id":          stmt.ID,
		"Status":      stmt.Status,
		"QueryString": stmt.QueryString,
		"CreatedAt":   stmt.CreatedAt.Unix(),
		"UpdatedAt":   stmt.CreatedAt.Unix(),
	}
	if stmt.Error != "" {
		resp["Error"] = stmt.Error
	}
	return redshiftDataJSONResponse(http.StatusOK, resp)
}

func (p *RedshiftDataPlugin) getStatementResult(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID string `json:"Id"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	stmt, err := p.loadStatement(reqCtx.AccountID, reqCtx.Region, input.ID)
	if err != nil {
		return nil, err
	}

	// Look up pre-seeded result by SQL or fallback to wildcard "*".
	result := p.lookupResult(stmt.QueryString)

	columnMetadata := result.ColumnMetadata
	if columnMetadata == nil {
		columnMetadata = []RedshiftDataColumnMetadata{}
	}
	records := result.Records
	if records == nil {
		records = [][]RedshiftDataField{}
	}

	return redshiftDataJSONResponse(http.StatusOK, map[string]interface{}{
		"ColumnMetadata": columnMetadata,
		"Records":        records,
		"TotalNumRows":   len(records),
	})
}

// lookupResult returns the pre-seeded result for the given SQL.
// It checks the in-memory map first (Go-level initialization), then the
// HTTP control-plane state namespace, falling back to an empty result.
func (p *RedshiftDataPlugin) lookupResult(sql string) *RedshiftDataResult {
	// 1. In-memory map (backward compat with Go test initialization).
	if r, ok := p.results[sql]; ok {
		return r
	}
	if r, ok := p.results["*"]; ok {
		return r
	}
	// 2. State (HTTP control plane, seeded via POST /v1/redshift-data/results).
	if r := p.loadStateResult(sql); r != nil {
		return r
	}
	if r := p.loadStateResult("*"); r != nil {
		return r
	}
	return &RedshiftDataResult{}
}

// loadStateResult loads a RedshiftDataResult from the HTTP control-plane
// state namespace for the given SQL pattern, returning nil if not found.
func (p *RedshiftDataPlugin) loadStateResult(sql string) *RedshiftDataResult {
	data, err := p.state.Get(context.Background(), redshiftDataCtrlNamespace, redshiftDataCtrlResultKey(sql))
	if err != nil || data == nil {
		return nil
	}
	var result RedshiftDataResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil
	}
	return &result
}

// loadStatement loads a RedshiftDataStatement from state or returns a not-found error.
func (p *RedshiftDataPlugin) loadStatement(acct, region, id string) (*RedshiftDataStatement, error) {
	if id == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Id is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, redshiftDataNamespace, redshiftDataStatementKey(acct, region, id))
	if err != nil {
		return nil, fmt.Errorf("redshift-data loadStatement get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Statement " + id + " not found.", HTTPStatus: http.StatusBadRequest}
	}
	var stmt RedshiftDataStatement
	if err := json.Unmarshal(data, &stmt); err != nil {
		return nil, fmt.Errorf("redshift-data loadStatement unmarshal: %w", err)
	}
	return &stmt, nil
}

// generateRedshiftDataID generates a UUID-style statement ID.
func generateRedshiftDataID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// redshiftDataJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/json.
func redshiftDataJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("redshift-data json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
