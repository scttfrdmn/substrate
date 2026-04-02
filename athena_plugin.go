package substrate

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// athenaNamespace is the state namespace for Amazon Athena.
const athenaNamespace = "athena"

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

	// Verify the query exists.
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

	// Return empty result set — callers use GetQueryResults to fetch rows, and
	// substrate does not execute real SQL, so return an empty valid response.
	return athenaJSONResponse(http.StatusOK, map[string]interface{}{
		"ResultSet": map[string]interface{}{
			"Rows":              []interface{}{},
			"ResultSetMetadata": map[string]interface{}{"ColumnInfo": []interface{}{}},
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
