package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Point-in-time recovery: DescribeContinuousBackups and UpdateContinuousBackups (#1608).
//
// Terraform's aws_dynamodb_table reads DescribeContinuousBackups on every refresh, so a table could not
// be applied while the operation was unrecognized. Both pages publish the same response, a
// ContinuousBackupsDescription:
//
//   - ContinuousBackupsStatus is ENABLED for every table: "Continuous backups are ENABLED on all tables
//     at table creation."
//   - PointInTimeRecoveryDescription.PointInTimeRecoveryStatus is DISABLED until UpdateContinuousBackups
//     enables it. Only an enabled table reports the restorable window and RecoveryPeriodInDays.
//
// The window is computed from the simulated clock at read time, so it is the same live and on replay.
// LatestRestorableDateTime is the page's "typically 5 minutes before the current time", and
// EarliestRestorableDateTime is the start of the recovery period ("You can set the RecoveryPeriodInDays
// to any value between 1 and 35 days", 35 when not set). Neither reaches back before the moment recovery
// was enabled, because nothing was recorded before it; that bound is substrate's reading, as no page
// says what a freshly enabled table reports. No restore is modeled: substrate keeps no item history.

// dynamodbPITRDefaultDays is the recovery period a table reports when UpdateContinuousBackups did not
// name one ("You can restore your table to any point in time in the last 35 days").
const dynamodbPITRDefaultDays = 35

// dynamodbPITRLatestLagSeconds is how far behind the current time LatestRestorableDateTime sits:
// "LatestRestorableDateTime is typically 5 minutes before the current time", per both pages.
const dynamodbPITRLatestLagSeconds = 5 * 60

// dynamodbTableNotFound is TableNotFoundException/400, the code both continuous-backups pages publish
// for an absent table (where every other table operation publishes ResourceNotFoundException). The
// message is the exception's published description with the name substituted.
func dynamodbTableNotFound(name string) *AWSError {
	return &AWSError{
		Code: "TableNotFoundException",
		Message: "A source table with the name " + name + " does not currently exist within the subscriber's " +
			"account or the subscriber is operating in the wrong AWS Region.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// pitrTableName resolves a continuous-backups request's TableName, which may be the table's ARN ("You
// can also provide the Amazon Resource Name (ARN) of the table in this parameter").
func pitrTableName(name string) (string, error) {
	if name == "" {
		return "", &AWSError{Code: "ValidationException", Message: "TableName is required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.HasPrefix(name, "arn:") {
		return dynamodbTableNameFromARN(name), nil
	}
	return name, nil
}

// pitrTable loads the table a continuous-backups request names; see [pitrTableName].
func (p *DynamoDBPlugin) pitrTable(ctx *RequestContext, name string) (*DynamoDBTable, error) {
	tableName, err := pitrTableName(name)
	if err != nil {
		return nil, err
	}
	tbl, err := p.loadTable(context.Background(), ctx.AccountID, ctx.Region, tableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, dynamodbTableNotFound(name)
	}
	return tbl, nil
}

// dynamodbPITRDescriptionOut is PointInTimeRecoveryDescription.
type dynamodbPITRDescriptionOut struct {
	EarliestRestorableDateTime *float64 `json:"EarliestRestorableDateTime,omitempty"`
	LatestRestorableDateTime   *float64 `json:"LatestRestorableDateTime,omitempty"`
	PointInTimeRecoveryStatus  string   `json:"PointInTimeRecoveryStatus"`
	RecoveryPeriodInDays       *int     `json:"RecoveryPeriodInDays,omitempty"`
}

// dynamodbContinuousBackupsOut is the response of both operations.
type dynamodbContinuousBackupsOut struct {
	ContinuousBackupsDescription struct {
		ContinuousBackupsStatus        string                     `json:"ContinuousBackupsStatus"`
		PointInTimeRecoveryDescription dynamodbPITRDescriptionOut `json:"PointInTimeRecoveryDescription"`
	} `json:"ContinuousBackupsDescription"`
}

// continuousBackupsResponse renders a table's ContinuousBackupsDescription at the current simulated time.
func (p *DynamoDBPlugin) continuousBackupsResponse(tbl *DynamoDBTable) (*AWSResponse, error) {
	var out dynamodbContinuousBackupsOut
	out.ContinuousBackupsDescription.ContinuousBackupsStatus = "ENABLED"
	desc := dynamodbPITRDescriptionOut{PointInTimeRecoveryStatus: "DISABLED"}
	if tbl.PITREnabled {
		days := tbl.PITRRecoveryPeriodInDays
		if days == 0 {
			days = dynamodbPITRDefaultDays
		}
		now := float64(p.tc.Now().UnixNano()) / 1e9
		earliest := max(now-float64(days)*86400, tbl.PITREnabledAt)
		latest := max(now-dynamodbPITRLatestLagSeconds, earliest)
		desc = dynamodbPITRDescriptionOut{
			EarliestRestorableDateTime: &earliest,
			LatestRestorableDateTime:   &latest,
			PointInTimeRecoveryStatus:  "ENABLED",
			RecoveryPeriodInDays:       &days,
		}
	}
	out.ContinuousBackupsDescription.PointInTimeRecoveryDescription = desc
	return dynamodbJSONResponse(http.StatusOK, out)
}

func (p *DynamoDBPlugin) describeContinuousBackups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	tbl, err := p.pitrTable(ctx, input.TableName)
	if err != nil {
		return nil, err
	}
	return p.continuousBackupsResponse(tbl)
}

func (p *DynamoDBPlugin) updateContinuousBackups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		TableName                        string `json:"TableName"`
		PointInTimeRecoverySpecification *struct {
			PointInTimeRecoveryEnabled *bool `json:"PointInTimeRecoveryEnabled"`
			RecoveryPeriodInDays       *int  `json:"RecoveryPeriodInDays"`
		} `json:"PointInTimeRecoverySpecification"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	// Both members are Required: Yes on their pages. The messages are substrate's own.
	spec := input.PointInTimeRecoverySpecification
	if spec == nil {
		return nil, &AWSError{Code: "ValidationException", Message: "PointInTimeRecoverySpecification is required", HTTPStatus: http.StatusBadRequest}
	}
	if spec.PointInTimeRecoveryEnabled == nil {
		return nil, &AWSError{Code: "ValidationException",
			Message: "PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled is required", HTTPStatus: http.StatusBadRequest}
	}
	// "Valid Range: Minimum value of 1. Maximum value of 35."
	if spec.RecoveryPeriodInDays != nil && (*spec.RecoveryPeriodInDays < 1 || *spec.RecoveryPeriodInDays > dynamodbPITRDefaultDays) {
		return nil, &AWSError{Code: "ValidationException",
			Message:    fmt.Sprintf("RecoveryPeriodInDays must be between 1 and 35, got %d", *spec.RecoveryPeriodInDays),
			HTTPStatus: http.StatusBadRequest}
	}
	tableName, err := pitrTableName(input.TableName)
	if err != nil {
		return nil, err
	}
	// Under the table's stripe from load to save, so a concurrent TagResource, UpdateTable or
	// UpdateTimeToLive cannot write back a record that drops this change, or this one drop theirs.
	unlock := p.lockTableForUpdate(ctx, tableName)
	defer unlock()
	tbl, err := p.pitrTable(ctx, input.TableName)
	if err != nil {
		return nil, err
	}

	if *spec.PointInTimeRecoveryEnabled {
		// Re-enabling an enabled table keeps its window: recovery was never interrupted.
		if !tbl.PITREnabled {
			tbl.PITREnabled = true
			tbl.PITREnabledAt = float64(p.tc.Now().UnixNano()) / 1e9
		}
		// An omitted period is the default, not the previous value: API_PointInTimeRecoverySpecification
		// gives RecoveryPeriodInDays a default of 35, so a request without it asks for 35.
		tbl.PITRRecoveryPeriodInDays = 0
		if spec.RecoveryPeriodInDays != nil {
			tbl.PITRRecoveryPeriodInDays = *spec.RecoveryPeriodInDays
		}
	} else {
		tbl.PITREnabled, tbl.PITREnabledAt, tbl.PITRRecoveryPeriodInDays = false, 0, 0
	}
	if err := p.saveTable(context.Background(), ctx.AccountID, ctx.Region, tbl); err != nil {
		return nil, fmt.Errorf("dynamodb updateContinuousBackups saveTable: %w", err)
	}
	return p.continuousBackupsResponse(tbl)
}
