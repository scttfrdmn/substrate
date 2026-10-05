package emulator

import (
	"context"
	"net/http"
)

// AWS Backup's three creates take an optional CreatorRequestId, which every page glosses as what
// "allows failed requests to be retried without the risk of running the operation twice" and
// constrains to "1 to 50 alphanumeric or '-_.' characters". API_CreateBackupPlan states the contract
// outright: "If the request includes a CreatorRequestId that matches an existing backup plan, that
// plan is returned." Until #1173 only CreateBackupVault read it, and none honored it, so a retried
// create minted a second plan or selection.

// backupValidateCreatorRequestID refuses a CreatorRequestId outside its published pattern with
// InvalidParameterValueException, the code each create page publishes for a parameter's value. An
// absent value is allowed: the member is Required: No.
func backupValidateCreatorRequestID(id string) error {
	if id == "" || backupCreatorRequestIDPattern.MatchString(id) {
		return nil
	}
	return &AWSError{
		Code:       "InvalidParameterValueException",
		Message:    "CreatorRequestId must contain 1 to 50 alphanumeric or '-_.' characters.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// findPlanByCreatorRequestID returns the caller's plan created with id, or nil when none was.
func (p *BackupPlugin) findPlanByCreatorRequestID(reqCtx *RequestContext, id string) (*BackupPlan, error) {
	ids, err := loadStringIndex(context.Background(), p.state, backupNamespace, backupPlanIDsKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, err
	}
	for _, planID := range ids {
		plan, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID)
		if err != nil {
			if isBackupNotFound(err) {
				continue
			}
			return nil, err
		}
		if plan.CreatorRequestID == id {
			return plan, nil
		}
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no plan was created with this CreatorRequestId".
}

// findSelectionByCreatorRequestID returns the plan's selection created with id, or nil when none was.
func (p *BackupPlugin) findSelectionByCreatorRequestID(reqCtx *RequestContext, planID, id string) (*BackupSelection, error) {
	ids, err := loadStringIndex(context.Background(), p.state, backupNamespace, backupSelectionIDsKey(reqCtx.AccountID, reqCtx.Region, planID))
	if err != nil {
		return nil, err
	}
	for _, selectionID := range ids {
		selection, err := p.loadSelection(reqCtx.AccountID, reqCtx.Region, planID, selectionID)
		if err != nil {
			if isBackupNotFound(err) {
				continue
			}
			return nil, err
		}
		if selection.CreatorRequestID == id {
			return selection, nil
		}
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no selection was created with this CreatorRequestId".
}

// isBackupNotFound reports whether err is the published not-found refusal, which an index scan
// treats as an entry whose record is gone rather than as a failure.
func isBackupNotFound(err error) bool {
	awsErr, ok := err.(*AWSError) //nolint:errorlint // loadPlan/loadSelection return *AWSError unwrapped.
	return ok && awsErr.Code == "ResourceNotFoundException"
}
