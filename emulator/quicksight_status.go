package emulator

import (
	"context"
	"fmt"
)

// QuickSight's two seeded progressions (#1155, #1168): a dataset's SPICE ingestion, and a data
// source's creation. docs/services.md, "How a progression is seeded", states the rules both share.

// quicksightIngestionCtrlNamespace and quicksightDataSourceCtrlNamespace hold each kind's seeds and
// per-resource observation counters, one namespace per kind.
const (
	quicksightIngestionCtrlNamespace  = "quicksight-ingestion-ctrl"
	quicksightDataSourceCtrlNamespace = "quicksight-datasource-ctrl"
)

// quicksightIngestionStatuses is IngestionStatus' published enumeration on API_Ingestion.
var quicksightIngestionStatuses = []string{"INITIALIZED", "QUEUED", "RUNNING", "FAILED", "COMPLETED", "CANCELLED"}

// quicksightIngestionTransientStatuses are the three an ingestion passes through before it settles.
var quicksightIngestionTransientStatuses = []string{"INITIALIZED", "QUEUED", "RUNNING"}

// quicksightIngestionErrorTypes is ErrorInfo.Type's published enumeration on API_ErrorInfo.
var quicksightIngestionErrorTypes = []string{
	"FAILURE_TO_ASSUME_ROLE", "INGESTION_SUPERSEDED", "INGESTION_CANCELED", "DATA_SET_DELETED",
	"DATA_SET_NOT_SPICE", "S3_UPLOADED_FILE_DELETED", "S3_MANIFEST_ERROR", "DATA_TOLERANCE_EXCEPTION",
	"SPICE_TABLE_NOT_FOUND", "DATA_SET_SIZE_LIMIT_EXCEEDED", "ROW_SIZE_LIMIT_EXCEEDED",
	"ACCOUNT_CAPACITY_LIMIT_EXCEEDED", "CUSTOMER_ERROR", "DATA_SOURCE_NOT_FOUND", "IAM_ROLE_NOT_AVAILABLE",
	"CONNECTION_FAILURE", "SQL_TABLE_NOT_FOUND", "PERMISSION_DENIED", "SSL_CERTIFICATE_VALIDATION_FAILURE",
	"OAUTH_TOKEN_FAILURE", "SOURCE_API_LIMIT_EXCEEDED_FAILURE", "PASSWORD_AUTHENTICATION_FAILURE",
	"SQL_SCHEMA_MISMATCH_ERROR", "INVALID_DATE_FORMAT", "INVALID_DATAPREP_SYNTAX",
	"SOURCE_RESOURCE_LIMIT_EXCEEDED", "SQL_INVALID_PARAMETER_VALUE", "QUERY_TIMEOUT", "SQL_NUMERIC_OVERFLOW",
	"UNRESOLVABLE_HOST", "UNROUTABLE_HOST", "SQL_EXCEPTION", "S3_FILE_INACCESSIBLE", "IOT_FILE_NOT_FOUND",
	"IOT_DATA_SET_FILE_EMPTY", "INVALID_DATA_SOURCE_CONFIG", "DATA_SOURCE_AUTH_FAILED",
	"DATA_SOURCE_CONNECTION_FAILED", "FAILURE_TO_PROCESS_JSON_FILE", "INTERNAL_SERVICE_ERROR",
	"REFRESH_SUPPRESSED_BY_EDIT", "PERMISSION_NOT_FOUND", "ELASTICSEARCH_CURSOR_NOT_ENABLED",
	"CURSOR_NOT_ENABLED", "DUPLICATE_COLUMN_NAMES_FOUND",
}

// quicksightIngestionSeed is the body of POST /v1/quicksight/ingestion-status: how many
// DescribeIngestion observations report a transient status, what the ingestion settles to, and what
// it reports once settled.
type quicksightIngestionSeed struct {
	// IngestionID is the ingestion the seed governs; empty or "*" governs every ingestion.
	IngestionID string `json:"ingestionId"`

	// PendingObservations is how many describes report TransientStatus before the ingestion settles.
	PendingObservations int `json:"pendingObservations"`

	// TransientStatus is what the countdown reports: INITIALIZED, QUEUED or RUNNING. Empty reports
	// RUNNING.
	TransientStatus string `json:"transientStatus,omitempty"`

	// Status is the IngestionStatus the ingestion settles to. Empty settles to COMPLETED.
	Status string `json:"status,omitempty"`

	// ErrorType and ErrorMessage are reported as ErrorInfo alongside a settled FAILED or CANCELLED.
	ErrorType    string `json:"errorType,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`

	// RowsIngested, RowsDropped and TotalRowsInDataset are reported as RowInfo once the ingestion
	// settles. Substrate ingests nothing, so these are the only source of a row count: an
	// unseeded ingestion reports no RowInfo at all, rather than a number with no provenance (#1168).
	RowsIngested       *int64 `json:"rowsIngested,omitempty"`
	RowsDropped        *int64 `json:"rowsDropped,omitempty"`
	TotalRowsInDataset *int64 `json:"totalRowsInDataset,omitempty"`
}

func (s quicksightIngestionSeed) progressionID() string        { return s.IngestionID }
func (s quicksightIngestionSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a status, transient status or error type outside its published
// enumeration (#1168), error information on a status that is neither FAILED nor CANCELLED, and a
// negative row count.
func (s quicksightIngestionSeed) validateProgression() error {
	if err := progressionStates("ingestion", quicksightIngestionStatuses, s.Status); err != nil {
		return err
	}
	if err := progressionStates("transient ingestion", quicksightIngestionTransientStatuses, s.TransientStatus); err != nil {
		return err
	}
	if err := progressionStates("ingestion error", quicksightIngestionErrorTypes, s.ErrorType); err != nil {
		return err
	}
	if (s.ErrorType != "" || s.ErrorMessage != "") && s.Status != "FAILED" && s.Status != "CANCELLED" {
		return fmt.Errorf("errorType and errorMessage apply only to status FAILED or CANCELLED, got %q", s.Status)
	}
	for name, n := range map[string]*int64{"rowsIngested": s.RowsIngested, "rowsDropped": s.RowsDropped, "totalRowsInDataset": s.TotalRowsInDataset} {
		if n != nil && *n < 0 {
			return fmt.Errorf("%s must be >= 0", name)
		}
	}
	return nil
}

// quicksightIngestionProgressions is the ingestion progression. It holds configuration only.
var quicksightIngestionProgressions = newProgression[quicksightIngestionSeed](quicksightIngestionCtrlNamespace, "ingestionId", "pendingObservations")

// quicksightDataSourceStatuses is DataSource.Status' published enumeration on API_DataSource.
var quicksightDataSourceStatuses = []string{
	"CREATION_IN_PROGRESS", "CREATION_SUCCESSFUL", "CREATION_FAILED",
	"UPDATE_IN_PROGRESS", "UPDATE_SUCCESSFUL", "UPDATE_FAILED", "DELETED",
}

// quicksightDataSourceSettledStatuses are the two a creation settles to. Substrate routes no
// UpdateDataSource or DeleteDataSource, so the update and deletion values have no transition to end.
var quicksightDataSourceSettledStatuses = []string{"CREATION_SUCCESSFUL", "CREATION_FAILED"}

// quicksightDataSourceErrorTypes is DataSourceErrorInfo.Type's published enumeration.
var quicksightDataSourceErrorTypes = []string{
	"ACCESS_DENIED", "COPY_SOURCE_NOT_FOUND", "TIMEOUT", "ENGINE_VERSION_NOT_SUPPORTED",
	"UNKNOWN_HOST", "GENERIC_SQL_FAILURE", "CONFLICT", "UNKNOWN",
}

// quicksightDataSourceSeed is the body of POST /v1/quicksight/data-source-status: how many
// observations report CREATION_IN_PROGRESS before a data source settles, and what it settles to.
type quicksightDataSourceSeed struct {
	// DataSourceID is the data source the seed governs; empty or "*" governs every data source.
	DataSourceID string `json:"dataSourceId"`

	// PendingObservations is how many observations report CREATION_IN_PROGRESS.
	PendingObservations int `json:"pendingObservations"`

	// Status is CREATION_SUCCESSFUL or CREATION_FAILED. Empty settles to the recorded
	// CREATION_SUCCESSFUL.
	Status string `json:"status,omitempty"`

	// ErrorType and ErrorMessage are reported as ErrorInfo alongside a settled CREATION_FAILED.
	ErrorType    string `json:"errorType,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func (s quicksightDataSourceSeed) progressionID() string        { return s.DataSourceID }
func (s quicksightDataSourceSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a status outside the published enumeration, a published status that is
// not one a creation settles to, an error type outside its enumeration, and error information on a
// status other than CREATION_FAILED.
func (s quicksightDataSourceSeed) validateProgression() error {
	if err := progressionStates("data source", quicksightDataSourceStatuses, s.Status); err != nil {
		return err
	}
	if err := progressionStates("settled data source", quicksightDataSourceSettledStatuses, s.Status); err != nil {
		return err
	}
	if err := progressionStates("data source error", quicksightDataSourceErrorTypes, s.ErrorType); err != nil {
		return err
	}
	if (s.ErrorType != "" || s.ErrorMessage != "") && s.Status != "CREATION_FAILED" {
		return fmt.Errorf("errorType and errorMessage apply only to status CREATION_FAILED, got %q", s.Status)
	}
	return nil
}

// quicksightDataSourceProgressions is the data-source progression. It holds configuration only.
var quicksightDataSourceProgressions = newProgression[quicksightDataSourceSeed](quicksightDataSourceCtrlNamespace, "dataSourceId", "pendingObservations")

// quicksightErrorInfoOut is ErrorInfo on an Ingestion and DataSourceErrorInfo on a DataSource; the
// two publish the same two members.
type quicksightErrorInfoOut struct {
	Message string `json:"Message,omitempty"`
	Type    string `json:"Type,omitempty"`
}

// quicksightRowInfoOut is RowInfo on an Ingestion.
type quicksightRowInfoOut struct {
	RowsDropped        *int64 `json:"RowsDropped,omitempty"`
	RowsIngested       *int64 `json:"RowsIngested,omitempty"`
	TotalRowsInDataset *int64 `json:"TotalRowsInDataset,omitempty"`
}

// quicksightIngestionView is what one observation of an ingestion reports.
type quicksightIngestionView struct {
	status    string
	errorInfo *quicksightErrorInfoOut
	rowInfo   *quicksightRowInfoOut
}

// observeIngestion reports what one DescribeIngestion of id sees, spending the observation.
func (p *QuickSightPlugin) observeIngestion(id string) (quicksightIngestionView, error) {
	view := quicksightIngestionView{status: "COMPLETED"}
	seed, seen, err := quicksightIngestionProgressions.observe(context.Background(), p.state, &p.seedMu, id)
	if err != nil {
		return view, fmt.Errorf("quicksight observe ingestion %s: %w", id, err)
	}
	if seed == nil {
		return view, nil
	}
	status, terminal := countdownState(seen, seed.PendingObservations, seed.TransientStatus, seed.Status, "RUNNING", "COMPLETED")
	view.status = status
	if !terminal {
		return view, nil
	}
	if seed.ErrorType != "" || seed.ErrorMessage != "" {
		view.errorInfo = &quicksightErrorInfoOut{Message: seed.ErrorMessage, Type: seed.ErrorType}
	}
	if seed.RowsIngested != nil || seed.RowsDropped != nil || seed.TotalRowsInDataset != nil {
		view.rowInfo = &quicksightRowInfoOut{RowsDropped: seed.RowsDropped, RowsIngested: seed.RowsIngested, TotalRowsInDataset: seed.TotalRowsInDataset}
	}
	return view, nil
}

// dataSourceView reports what an observation of ds sees. observe spends the observation, as
// DescribeDataSource does; a false observe peeks, as CreateDataSource's own answer does, so the
// create response does not spend the first describe's observation.
func (p *QuickSightPlugin) dataSourceView(ds QuickSightDataSource, observe bool) (string, *quicksightErrorInfoOut, error) {
	ctx := context.Background()
	var (
		seed *quicksightDataSourceSeed
		seen int
		err  error
	)
	if observe {
		seed, seen, err = quicksightDataSourceProgressions.observe(ctx, p.state, &p.seedMu, ds.DataSourceID)
	} else {
		seed, seen, err = quicksightDataSourceProgressions.peek(ctx, p.state, ds.DataSourceID)
	}
	if err != nil {
		return "", nil, fmt.Errorf("quicksight data source %s: %w", ds.DataSourceID, err)
	}
	if seed == nil {
		return ds.Status, nil, nil
	}
	status, terminal := countdownState(seen, seed.PendingObservations, "", seed.Status, "CREATION_IN_PROGRESS", ds.Status)
	if terminal && (seed.ErrorType != "" || seed.ErrorMessage != "") {
		return status, &quicksightErrorInfoOut{Message: seed.ErrorMessage, Type: seed.ErrorType}, nil
	}
	return status, nil, nil
}
