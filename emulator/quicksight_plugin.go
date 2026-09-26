package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// quicksightNamespace is the state namespace for Amazon QuickSight.
const quicksightNamespace = "quicksight"

// QuickSightPlugin emulates the Amazon QuickSight service.
// It supports the four operations used by dataset-loader and s3-load Lambda
// tools: CreateDataSource, DescribeDataSource, CreateDataSet, and
// DescribeIngestion via the QuickSight REST/JSON API.
type QuickSightPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "quicksight".
func (p *QuickSightPlugin) Name() string { return quicksightNamespace }

// Initialize sets up the QuickSightPlugin with the provided configuration.
func (p *QuickSightPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for QuickSightPlugin.
func (p *QuickSightPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a QuickSight REST/JSON request to the appropriate handler.
func (p *QuickSightPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, accountID, resourceID, secondaryID := parseQuickSightOperation(requestMethod(req), req.Path)
	switch op {
	case "CreateDataSource":
		return p.createDataSource(ctx, req, accountID)
	case "DescribeDataSource":
		return p.describeDataSource(ctx, req, accountID, resourceID)
	case "CreateDataSet":
		return p.createDataSet(ctx, req, accountID)
	case "DescribeIngestion":
		return p.describeIngestion(ctx, req, accountID, resourceID, secondaryID)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseQuickSightOperation maps an HTTP method and path to a QuickSight operation
// name plus accountID, resourceID, and optional secondary ID.
func parseQuickSightOperation(method, path string) (op, accountID, resourceID, secondaryID string) {
	// Strip leading slash.
	rest := strings.TrimPrefix(path, "/")
	// Expected prefix: accounts/{accountId}/...
	if !strings.HasPrefix(rest, "accounts/") {
		return "", "", "", ""
	}
	rest = strings.TrimPrefix(rest, "accounts/")
	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", "", "", ""
	}
	acctID := rest[:slashIdx]
	rest = rest[slashIdx+1:]

	// data-sources[/{dataSourceId}]
	if strings.HasPrefix(rest, "data-sources") {
		suffix := strings.TrimPrefix(rest, "data-sources")
		suffix = strings.TrimPrefix(suffix, "/")
		if suffix == "" {
			if method == "POST" {
				return "CreateDataSource", acctID, "", ""
			}
		} else {
			if method == "GET" {
				return "DescribeDataSource", acctID, suffix, ""
			}
		}
	}

	// data-sets[/{dataSetId}/ingestions/{ingestionId}]
	if strings.HasPrefix(rest, "data-sets") {
		suffix := strings.TrimPrefix(rest, "data-sets")
		suffix = strings.TrimPrefix(suffix, "/")
		if suffix == "" {
			if method == "POST" {
				return "CreateDataSet", acctID, "", ""
			}
		} else {
			// /data-sets/{dataSetId}/ingestions/{ingestionId}
			if ingIdx := strings.Index(suffix, "/ingestions/"); ingIdx >= 0 {
				dsID := suffix[:ingIdx]
				ingID := suffix[ingIdx+len("/ingestions/"):]
				if method == "GET" {
					return "DescribeIngestion", acctID, dsID, ingID
				}
			}
		}
	}

	return "", "", "", ""
}

// QuickSightDataSource holds persisted state for a QuickSight data source.
type QuickSightDataSource struct {
	// DataSourceID is the unique identifier for the data source.
	DataSourceID string `json:"DataSourceId"`

	// Name is the user-supplied display name.
	Name string `json:"Name"`

	// Type is the data source type (e.g. "S3", "ATHENA").
	Type string `json:"Type"`

	// Arn is the ARN of the data source.
	Arn string `json:"Arn"`

	// Status is the creation status (CREATION_SUCCESSFUL).
	Status string `json:"Status"`

	// AccountID is the AWS account that owns the data source.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the data source resides.
	Region string `json:"Region"`
}

// QuickSightDataSet holds persisted state for a QuickSight dataset.
type QuickSightDataSet struct {
	// DataSetID is the unique identifier for the dataset.
	DataSetID string `json:"DataSetId"`

	// Name is the user-supplied display name.
	Name string `json:"Name"`

	// Arn is the ARN of the dataset.
	Arn string `json:"Arn"`

	// IngestionID is the UUID used to track the initial ingestion.
	IngestionID string `json:"IngestionId"`

	// AccountID is the AWS account that owns the dataset.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the dataset resides.
	Region string `json:"Region"`
}

func (p *QuickSightPlugin) createDataSource(ctx *RequestContext, req *AWSRequest, _ string) (*AWSResponse, error) {
	var body struct {
		DataSourceID string `json:"DataSourceId"`
		Name         string `json:"Name"`
		Type         string `json:"Type"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.DataSourceID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DataSourceId is required", HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:quicksight:%s:%s:datasource/%s", ctx.Region, ctx.AccountID, body.DataSourceID)
	ds := QuickSightDataSource{
		DataSourceID: body.DataSourceID,
		Name:         body.Name,
		Type:         body.Type,
		Arn:          arn,
		Status:       "CREATION_SUCCESSFUL",
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
	}

	goCtx := context.Background()
	key := "datasource:" + ctx.AccountID + "/" + body.DataSourceID
	data, err := json.Marshal(ds)
	if err != nil {
		return nil, fmt.Errorf("createDataSource: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, quicksightNamespace, key, data); err != nil {
		return nil, fmt.Errorf("createDataSource: put: %w", err)
	}

	reqID := generateQuickSightRequestID(ctx.IDs)
	return quicksightJSONResponse(http.StatusCreated, map[string]interface{}{
		"DataSourceId":   body.DataSourceID,
		"Arn":            arn,
		"CreationStatus": "CREATION_SUCCESSFUL",
		"RequestId":      reqID,
	})
}

func (p *QuickSightPlugin) describeDataSource(ctx *RequestContext, _ *AWSRequest, _, dataSourceID string) (*AWSResponse, error) {
	goCtx := context.Background()
	key := "datasource:" + ctx.AccountID + "/" + dataSourceID
	data, err := p.state.Get(goCtx, quicksightNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "DataSource " + dataSourceID + " not found",
			HTTPStatus: http.StatusNotFound,
		}
	}
	var ds QuickSightDataSource
	if err := json.Unmarshal(data, &ds); err != nil {
		return nil, fmt.Errorf("describeDataSource: unmarshal: %w", err)
	}
	return quicksightJSONResponse(http.StatusOK, map[string]interface{}{
		"DataSource": ds,
		"RequestId":  generateQuickSightRequestID(ctx.IDs),
		"Status":     http.StatusOK,
	})
}

func (p *QuickSightPlugin) createDataSet(ctx *RequestContext, req *AWSRequest, _ string) (*AWSResponse, error) {
	var body struct {
		DataSetID string `json:"DataSetId"`
		Name      string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.DataSetID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DataSetId is required", HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:quicksight:%s:%s:dataset/%s", ctx.Region, ctx.AccountID, body.DataSetID)
	ingestionID := generateQuickSightIngestionID(ctx.IDs)

	ds := QuickSightDataSet{
		DataSetID:   body.DataSetID,
		Name:        body.Name,
		Arn:         arn,
		IngestionID: ingestionID,
		AccountID:   ctx.AccountID,
		Region:      ctx.Region,
	}

	goCtx := context.Background()
	key := "dataset:" + ctx.AccountID + "/" + body.DataSetID
	data, err := json.Marshal(ds)
	if err != nil {
		return nil, fmt.Errorf("createDataSet: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, quicksightNamespace, key, data); err != nil {
		return nil, fmt.Errorf("createDataSet: put: %w", err)
	}

	return quicksightJSONResponse(http.StatusCreated, map[string]interface{}{
		"DataSetId":   body.DataSetID,
		"Arn":         arn,
		"IngestionId": ingestionID,
		"RequestId":   generateQuickSightRequestID(ctx.IDs),
	})
}

func (p *QuickSightPlugin) describeIngestion(ctx *RequestContext, _ *AWSRequest, _, dataSetID, ingestionID string) (*AWSResponse, error) {
	goCtx := context.Background()
	key := "dataset:" + ctx.AccountID + "/" + dataSetID
	data, err := p.state.Get(goCtx, quicksightNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "DataSet " + dataSetID + " not found",
			HTTPStatus: http.StatusNotFound,
		}
	}

	ingArn := fmt.Sprintf("arn:aws:quicksight:%s:%s:dataset/%s/ingestion/%s",
		ctx.Region, ctx.AccountID, dataSetID, ingestionID)
	createdTime := float64(p.tc.Now().UnixNano()) / 1e9

	return quicksightJSONResponse(http.StatusOK, map[string]interface{}{
		"Ingestion": map[string]interface{}{
			"Arn":             ingArn,
			"IngestionId":     ingestionID,
			"IngestionStatus": "COMPLETED",
			"RowInfo": map[string]interface{}{
				"RowsIngested": 1000,
				"RowsDropped":  0,
			},
			"CreatedTime": createdTime,
		},
		"RequestId": generateQuickSightRequestID(ctx.IDs),
		"Status":    http.StatusOK,
	})
}

// generateQuickSightRequestID mints the `RequestId` every QuickSight response carries.
//
// QuickSight is unusual in publishing the request id as a *body* member on every operation, success
// and error alike, rather than only as a header — API_CreateIngestion documents it as "the AWS request
// ID for this operation" with no type constraint beyond String. So it is an observation a caller can
// make, and #856 applies for the ordinary reason: a replayed response must carry the recorded value.
//
// It is minted rather than taken from [RequestContext.RequestID], and the reason is the shape.
// Substrate's own request id is `req-{nanos}-{8 hex}` (see generateRequestID) — deliberately
// wall-clock, per #866 — which is not a form AWS ever sends, and nothing observes the two together:
// substrate sets no `x-amzn-RequestId` header, so the body member has nothing to disagree with.
// Echoing the internal id would trade a shape a caller can parse for a correspondence no call can see.
func generateQuickSightRequestID(m *IDMint) string {
	return m.HexUUID()
}

// generateQuickSightIngestionID mints the SPICE ingestion ID a `CreateDataSet` response reports and
// `DescribeIngestion` addresses.
//
// It is its own minter rather than a second call into [generateQuickSightRequestID], which is where
// `createDataSet` used to reach: an ingestion ID is a *handle* — the recorded `DescribeIngestion` path
// contains it — where a request ID is observed once and never sent back, so one function serving both
// meant a change to how substrate renders a request ID would silently move the identifier a recorded
// URL depends on. The same split #856 made between ACM's certificate IDs and API Gateway's API keys.
//
// The rendering is unchanged from the shared generator's, and the API model permits it:
// API_CreateIngestion publishes `IngestionId` at 1–128 characters matching `^[a-zA-Z0-9-_]+$`, which
// the UUID shape satisfies — the hyphens included.
func generateQuickSightIngestionID(m *IDMint) string {
	return m.HexUUID()
}

// quicksightJSONResponse serializes v to JSON and returns an AWSResponse.
func quicksightJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("quicksight json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
