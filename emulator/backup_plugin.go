package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// BackupPlugin emulates the AWS Backup service.
// It handles vault, plan, and selection CRUD operations using the
// AWS Backup REST/JSON API at /backup-vaults/... and /backup/plans/... paths.
type BackupPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "backup".
func (p *BackupPlugin) Name() string { return backupNamespace }

// Initialize sets up the BackupPlugin with the provided configuration.
func (p *BackupPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for BackupPlugin.
func (p *BackupPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an AWS Backup REST/JSON request to the appropriate handler.
func (p *BackupPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, vaultName, planID, selectionID := parseBackupOperation(requestMethod(req), req.Path)
	switch op {
	case "CreateBackupVault":
		return p.createBackupVault(reqCtx, req, vaultName)
	case "DescribeBackupVault":
		return p.describeBackupVault(reqCtx, vaultName)
	case "DeleteBackupVault":
		return p.deleteBackupVault(reqCtx, vaultName)
	case "ListBackupVaults":
		return p.listBackupVaults(reqCtx, req)
	case "CreateBackupPlan":
		return p.createBackupPlan(reqCtx, req)
	case "GetBackupPlan":
		return p.getBackupPlan(reqCtx, planID)
	case "UpdateBackupPlan":
		return p.updateBackupPlan(reqCtx, req, planID)
	case "DeleteBackupPlan":
		return p.deleteBackupPlan(reqCtx, planID)
	case "ListBackupPlans":
		return p.listBackupPlans(reqCtx)
	case "CreateBackupSelection":
		return p.createBackupSelection(reqCtx, req, planID)
	case "GetBackupSelection":
		return p.getBackupSelection(reqCtx, planID, selectionID)
	case "DeleteBackupSelection":
		return p.deleteBackupSelection(reqCtx, planID, selectionID)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

func (p *BackupPlugin) createBackupVault(reqCtx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	if name == "" {
		return nil, backupMissingParameter("BackupVaultName")
	}

	var input struct {
		EncryptionKeyArn string `json:"EncryptionKeyArn"`
		CreatorRequestID string `json:"CreatorRequestId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, backupInvalidBody()
		}
	}
	// API_CreateBackupVault: CreatorRequestId is optional, and "If used, this parameter must contain 1
	// to 50 alphanumeric or '-_.' characters." The refusal is the page's InvalidParameterValueException,
	// "something is wrong with a parameter's value". The value is recorded, reported, and makes a retry
	// of the create idempotent; see the existing-vault branch below.
	if input.CreatorRequestID != "" && !backupCreatorRequestIDPattern.MatchString(input.CreatorRequestID) {
		return nil, &AWSError{
			Code:       "InvalidParameterValueException",
			Message:    "CreatorRequestId must contain 1 to 50 alphanumeric or '-_.' characters.",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	key := backupVaultKey(reqCtx.AccountID, reqCtx.Region, name)
	existing, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup createBackupVault get: %w", err)
	}
	if existing != nil {
		// API_CreateBackupVault glosses CreatorRequestId as what "allows failed requests to be retried
		// without the risk of running the operation twice" (#1173). A retry carrying the value the vault
		// was created with answers that vault; any other create of the name is AlreadyExistsException.
		var prior BackupVault
		if err := json.Unmarshal(existing, &prior); err != nil {
			return nil, fmt.Errorf("backup createBackupVault unmarshal: %w", err)
		}
		if input.CreatorRequestID == "" || input.CreatorRequestID != prior.CreatorRequestID {
			return nil, &AWSError{Code: "AlreadyExistsException", Message: "Vault " + name + " already exists.", HTTPStatus: http.StatusBadRequest}
		}
		return backupJSONResponse(http.StatusOK, map[string]interface{}{
			"BackupVaultArn":  prior.BackupVaultArn,
			"BackupVaultName": prior.BackupVaultName,
			"CreationDate":    EpochSeconds(prior.CreationDate),
		})
	}

	now := p.tc.Now()
	vault := BackupVault{
		BackupVaultName:        name,
		BackupVaultArn:         fmt.Sprintf("arn:aws:backup:%s:%s:backup-vault:%s", reqCtx.Region, reqCtx.AccountID, name),
		EncryptionKeyArn:       input.EncryptionKeyArn,
		CreationDate:           now,
		NumberOfRecoveryPoints: 0,
		CreatorRequestID:       input.CreatorRequestID,
		AccountID:              reqCtx.AccountID,
		Region:                 reqCtx.Region,
	}

	data, err := json.Marshal(vault)
	if err != nil {
		return nil, fmt.Errorf("backup createBackupVault marshal: %w", err)
	}
	if err := p.state.Put(goCtx, backupNamespace, key, data); err != nil {
		return nil, fmt.Errorf("backup createBackupVault put: %w", err)
	}
	if err := updateStringIndex(goCtx, p.state, backupNamespace, backupVaultNamesKey(reqCtx.AccountID, reqCtx.Region), name); err != nil {
		return nil, fmt.Errorf("backup createBackupVault index: %w", err)
	}

	// The three members API_CreateBackupVault publishes, and not backupVaultOut's five:
	// the create response is a narrower shape than the describe one.
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupVaultArn":  vault.BackupVaultArn,
		"BackupVaultName": vault.BackupVaultName,
		"CreationDate":    EpochSeconds(vault.CreationDate),
	})
}

func (p *BackupPlugin) describeBackupVault(reqCtx *RequestContext, name string) (*AWSResponse, error) {
	vault, err := p.loadVault(reqCtx.AccountID, reqCtx.Region, name)
	if err != nil {
		return nil, err
	}
	return backupJSONResponse(http.StatusOK, backupVaultToWire(*vault))
}

// deleteBackupVault handles DeleteBackupVault. Its `{}` is faithful: API_DeleteBackupVault publishes
// "an HTTP 200 response with an empty HTTP body", unlike DeleteBackupPlan, which publishes members
// (#1177).
func (p *BackupPlugin) deleteBackupVault(reqCtx *RequestContext, name string) (*AWSResponse, error) {
	if _, err := p.loadVault(reqCtx.AccountID, reqCtx.Region, name); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := backupVaultKey(reqCtx.AccountID, reqCtx.Region, name)
	if err := p.state.Delete(goCtx, backupNamespace, key); err != nil {
		return nil, fmt.Errorf("backup deleteBackupVault delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, backupNamespace, backupVaultNamesKey(reqCtx.AccountID, reqCtx.Region), name); err != nil {
		return nil, fmt.Errorf("backup deleteBackupVault index: %w", err)
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{})
}

// listBackupVaults handles ListBackupVaults.
//
// API_ListBackupVaults publishes four query parameters, and until #1199's audit none was read: the
// list was one page with no NextToken, and both filters matched everything.
//
//   - maxResults (Valid Range 1–1000) bounds the page; outside the range is
//     InvalidParameterValueException/400, which the page glosses "the value is out of range". With
//     none the page size is the published maximum, 1000.
//   - nextToken is [encodeOffsetPaginationToken]'s offset over the vault index, which is kept sorted
//     by name, so a walk is stable. It is omitted, not emitted empty, on the last page, and one
//     substrate did not issue is InvalidParameterValueException rather than read as page one (#915).
//   - vaultType (ByVaultType) must be one of its published Valid Values. Every vault substrate
//     creates is BACKUP_VAULT — CreateLogicallyAirGappedBackupVault and
//     CreateRestoreAccessBackupVault are unrouted — so BACKUP_VAULT matches every vault and the other
//     two match none.
//   - shared (ByShared) is a boolean the page glosses as sorting "the list of vaults by shared
//     vaults"; the CLI and SDKs use it to list the vaults shared with the caller. Substrate shares no
//     vault across accounts, so true answers an empty list and false the full one. A value that is not
//     a boolean is InvalidParameterValueException.
//
// A vault whose record cannot be read is an error rather than skipped, since a skipped vault would
// shorten the list and shift every later offset.
func (p *BackupPlugin) listBackupVaults(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	invalid := func(msg string) error {
		return &AWSError{Code: "InvalidParameterValueException", Message: msg, HTTPStatus: http.StatusBadRequest}
	}
	pageSize := backupListVaultsMax
	if raw, ok := req.Params["maxResults"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > backupListVaultsMax {
			return nil, invalid(fmt.Sprintf("maxResults must be an integer from 1 to %d.", backupListVaultsMax))
		}
		pageSize = n
	}
	offset, ok := decodeOffsetPaginationToken(req.Params["nextToken"])
	if !ok {
		return nil, invalid("The nextToken is not valid.")
	}
	matchesType := true
	if vt, ok := req.Params["vaultType"]; ok {
		if !backupVaultTypes[vt] {
			return nil, invalid("vaultType must be one of BACKUP_VAULT, LOGICALLY_AIR_GAPPED_BACKUP_VAULT or RESTORE_ACCESS_BACKUP_VAULT.")
		}
		matchesType = vt == backupVaultTypeStandard
	}
	if shared, ok := req.Params["shared"]; ok {
		b, err := strconv.ParseBool(shared)
		if err != nil {
			return nil, invalid("shared must be true or false.")
		}
		if b {
			matchesType = false
		}
	}

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, backupNamespace, backupVaultNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("backup listBackupVaults load index: %w", err)
	}
	vaults := make([]BackupVault, 0, len(names))
	if matchesType {
		for _, name := range names {
			v, err := p.loadVault(reqCtx.AccountID, reqCtx.Region, name)
			var awsErr *AWSError
			if errors.As(err, &awsErr) && awsErr.Code == "ResourceNotFoundException" {
				continue // an index entry outliving its record, which is not a fault
			}
			if err != nil {
				return nil, err
			}
			vaults = append(vaults, *v)
		}
	}
	page, next := pageByOffsetToken(vaults, offset, pageSize)
	out := map[string]interface{}{"BackupVaultList": backupVaultsToWire(page)}
	if next != "" {
		out["NextToken"] = next
	}
	return backupJSONResponse(http.StatusOK, out)
}

// backupListVaultsMax is ListBackupVaults' published maxResults maximum, and its page size when the
// caller names none.
const backupListVaultsMax = 1000

// backupVaultTypeStandard is the one VaultType substrate creates: CreateBackupVault makes a
// BACKUP_VAULT, and the operations that make the other two types are unrouted.
const backupVaultTypeStandard = "BACKUP_VAULT"

// backupVaultTypes is the Valid Values list API_ListBackupVaults publishes for ByVaultType.
var backupVaultTypes = map[string]bool{
	"BACKUP_VAULT": true, "LOGICALLY_AIR_GAPPED_BACKUP_VAULT": true, "RESTORE_ACCESS_BACKUP_VAULT": true,
}

// backupCreatorRequestIDPattern is API_CreateBackupVault's "1 to 50 alphanumeric or '-_.'
// characters".
var backupCreatorRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,50}$`)

func (p *BackupPlugin) createBackupPlan(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		BackupPlan struct {
			BackupPlanName string                   `json:"BackupPlanName"`
			Rules          []map[string]interface{} `json:"Rules"`
		} `json:"BackupPlan"`
		CreatorRequestID string `json:"CreatorRequestId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, backupInvalidBody()
		}
	}
	if input.BackupPlan.BackupPlanName == "" {
		return nil, backupMissingParameter("BackupPlanName")
	}
	if err := backupValidateCreatorRequestID(input.CreatorRequestID); err != nil {
		return nil, err
	}
	// "If the request includes a CreatorRequestId that matches an existing backup plan, that plan is
	// returned" (API_CreateBackupPlan, #1173): the existing plan's create response, not a second plan.
	if input.CreatorRequestID != "" {
		prior, err := p.findPlanByCreatorRequestID(reqCtx, input.CreatorRequestID)
		if err != nil {
			return nil, err
		}
		if prior != nil {
			return backupJSONResponse(http.StatusOK, map[string]interface{}{
				"BackupPlanId":  prior.BackupPlanID,
				"BackupPlanArn": prior.BackupPlanArn,
				"CreationDate":  EpochSeconds(prior.CreationDate),
				"VersionId":     prior.VersionID,
			})
		}
	}

	planID := generateBackupUUID(reqCtx.IDs)
	versionID := generateBackupUUID(reqCtx.IDs)
	now := p.tc.Now()

	plan := BackupPlan{
		BackupPlanID:     planID,
		BackupPlanArn:    fmt.Sprintf("arn:aws:backup:%s:%s:backup-plan:%s", reqCtx.Region, reqCtx.AccountID, planID),
		BackupPlanName:   input.BackupPlan.BackupPlanName,
		Rules:            input.BackupPlan.Rules,
		VersionID:        versionID,
		CreationDate:     now,
		CreatorRequestID: input.CreatorRequestID,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("backup createBackupPlan marshal: %w", err)
	}
	key := backupPlanKey(reqCtx.AccountID, reqCtx.Region, planID)
	if err := p.state.Put(goCtx, backupNamespace, key, data); err != nil {
		return nil, fmt.Errorf("backup createBackupPlan put: %w", err)
	}
	if err := updateStringIndex(goCtx, p.state, backupNamespace, backupPlanIDsKey(reqCtx.AccountID, reqCtx.Region), planID); err != nil {
		return nil, fmt.Errorf("backup createBackupPlan index: %w", err)
	}

	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlanId":  planID,
		"BackupPlanArn": plan.BackupPlanArn,
		"CreationDate":  EpochSeconds(now),
		"VersionId":     versionID,
	})
}

func (p *BackupPlugin) getBackupPlan(reqCtx *RequestContext, planID string) (*AWSResponse, error) {
	plan, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{
		"BackupPlanId":  plan.BackupPlanID,
		"BackupPlanArn": plan.BackupPlanArn,
		"VersionId":     plan.VersionID,
		"CreationDate":  EpochSeconds(plan.CreationDate),
		"BackupPlan": map[string]interface{}{
			"BackupPlanName": plan.BackupPlanName,
			"Rules":          plan.Rules,
		},
	}
	if plan.CreatorRequestID != "" {
		out["CreatorRequestId"] = plan.CreatorRequestID
	}
	return backupJSONResponse(http.StatusOK, out)
}

func (p *BackupPlugin) updateBackupPlan(reqCtx *RequestContext, req *AWSRequest, planID string) (*AWSResponse, error) {
	plan, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID)
	if err != nil {
		return nil, err
	}

	var input struct {
		BackupPlan struct {
			BackupPlanName string                   `json:"BackupPlanName"`
			Rules          []map[string]interface{} `json:"Rules"`
		} `json:"BackupPlan"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, backupInvalidBody()
		}
	}

	if input.BackupPlan.BackupPlanName != "" {
		plan.BackupPlanName = input.BackupPlan.BackupPlanName
	}
	if input.BackupPlan.Rules != nil {
		plan.Rules = input.BackupPlan.Rules
	}
	plan.VersionID = generateBackupUUID(reqCtx.IDs)

	goCtx := context.Background()
	data, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("backup updateBackupPlan marshal: %w", err)
	}
	key := backupPlanKey(reqCtx.AccountID, reqCtx.Region, planID)
	if err := p.state.Put(goCtx, backupNamespace, key, data); err != nil {
		return nil, fmt.Errorf("backup updateBackupPlan put: %w", err)
	}

	// API_UpdateBackupPlan publishes AdvancedBackupSettings, BackupPlanArn, BackupPlanId,
	// CreationDate, ScanSettings and VersionId. The record carries no advanced or scan settings, so
	// those two are absent rather than empty. CreationDate is the plan's own, which an update does not
	// move; until #1177 the handler answered an UpdatedAt that no AWS Backup page publishes, and
	// dropped CreationDate.
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlanArn": plan.BackupPlanArn,
		"BackupPlanId":  plan.BackupPlanID,
		"CreationDate":  EpochSeconds(plan.CreationDate),
		"VersionId":     plan.VersionID,
	})
}

// deleteBackupPlan handles DeleteBackupPlan.
//
// API_DeleteBackupPlan publishes a body — BackupPlanArn, BackupPlanId, DeletionDate and VersionId —
// and the handler answered `{}`, the shape of the Backup deletes whose pages publish an empty one
// (#1206's survey; #1177). It now answers all four, from the record it deletes. DeletionDate is the
// simulated clock's now, as EpochSeconds, the Unix form every Backup date takes (#1324).
func (p *BackupPlugin) deleteBackupPlan(reqCtx *RequestContext, planID string) (*AWSResponse, error) {
	plan, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID)
	if err != nil {
		return nil, err
	}
	goCtx := context.Background()
	held, err := p.planHasSelections(goCtx, reqCtx.AccountID, reqCtx.Region, planID)
	if err != nil {
		return nil, err
	}
	if held {
		return nil, backupPlanHasSelections(planID)
	}
	key := backupPlanKey(reqCtx.AccountID, reqCtx.Region, planID)
	if err := p.state.Delete(goCtx, backupNamespace, key); err != nil {
		return nil, fmt.Errorf("backup deleteBackupPlan delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, backupNamespace, backupPlanIDsKey(reqCtx.AccountID, reqCtx.Region), planID); err != nil {
		return nil, fmt.Errorf("backup deleteBackupPlan index: %w", err)
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlanArn": plan.BackupPlanArn,
		"BackupPlanId":  plan.BackupPlanID,
		"DeletionDate":  EpochSeconds(p.tc.Now()),
		"VersionId":     plan.VersionID,
	})
}

func (p *BackupPlugin) listBackupPlans(reqCtx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, backupNamespace, backupPlanIDsKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("backup listBackupPlans load index: %w", err)
	}
	summaries := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		plan, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, id)
		if err != nil {
			continue
		}
		summary := map[string]interface{}{
			"BackupPlanId":   plan.BackupPlanID,
			"BackupPlanArn":  plan.BackupPlanArn,
			"BackupPlanName": plan.BackupPlanName,
			"VersionId":      plan.VersionID,
			"CreationDate":   EpochSeconds(plan.CreationDate),
		}
		// BackupPlansListMember publishes CreatorRequestId; it is answered when the create sent one.
		if plan.CreatorRequestID != "" {
			summary["CreatorRequestId"] = plan.CreatorRequestID
		}
		summaries = append(summaries, summary)
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlansList": summaries,
	})
}

func (p *BackupPlugin) createBackupSelection(reqCtx *RequestContext, req *AWSRequest, planID string) (*AWSResponse, error) {
	if _, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID); err != nil {
		return nil, err
	}

	var input struct {
		BackupSelection struct {
			SelectionName string   `json:"SelectionName"`
			IamRoleArn    string   `json:"IamRoleArn"`
			Resources     []string `json:"Resources"`
		} `json:"BackupSelection"`
		CreatorRequestID string `json:"CreatorRequestId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, backupInvalidBody()
		}
	}
	if input.BackupSelection.SelectionName == "" {
		return nil, backupMissingParameter("SelectionName")
	}
	if err := backupValidateCreatorRequestID(input.CreatorRequestID); err != nil {
		return nil, err
	}
	// API_CreateBackupSelection glosses CreatorRequestId as what lets a failed request be retried
	// without running it twice (#1173): a retry carrying a recorded value answers that selection.
	if input.CreatorRequestID != "" {
		prior, err := p.findSelectionByCreatorRequestID(reqCtx, planID, input.CreatorRequestID)
		if err != nil {
			return nil, err
		}
		if prior != nil {
			return backupJSONResponse(http.StatusOK, map[string]interface{}{
				"SelectionId":  prior.SelectionID,
				"BackupPlanId": prior.BackupPlanID,
				"CreationDate": EpochSeconds(prior.CreationDate),
			})
		}
	}

	selectionID := generateBackupUUID(reqCtx.IDs)
	now := p.tc.Now()
	selection := BackupSelection{
		SelectionID:      selectionID,
		SelectionName:    input.BackupSelection.SelectionName,
		BackupPlanID:     planID,
		IamRoleArn:       input.BackupSelection.IamRoleArn,
		CreatorRequestID: input.CreatorRequestID,
		Resources:        input.BackupSelection.Resources,
		CreationDate:     now,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(selection)
	if err != nil {
		return nil, fmt.Errorf("backup createBackupSelection marshal: %w", err)
	}
	key := backupSelectionKey(reqCtx.AccountID, reqCtx.Region, planID, selectionID)
	if err := p.state.Put(goCtx, backupNamespace, key, data); err != nil {
		return nil, fmt.Errorf("backup createBackupSelection put: %w", err)
	}
	if err := updateStringIndex(goCtx, p.state, backupNamespace, backupSelectionIDsKey(reqCtx.AccountID, reqCtx.Region, planID), selectionID); err != nil {
		return nil, fmt.Errorf("backup createBackupSelection index: %w", err)
	}

	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"SelectionId":  selectionID,
		"BackupPlanId": planID,
		"CreationDate": EpochSeconds(now),
	})
}

func (p *BackupPlugin) getBackupSelection(reqCtx *RequestContext, planID, selectionID string) (*AWSResponse, error) {
	selection, err := p.loadSelection(reqCtx.AccountID, reqCtx.Region, planID, selectionID)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{
		"SelectionId":  selection.SelectionID,
		"BackupPlanId": selection.BackupPlanID,
		"CreationDate": EpochSeconds(selection.CreationDate),
		"BackupSelection": map[string]interface{}{
			"SelectionName": selection.SelectionName,
			"IamRoleArn":    selection.IamRoleArn,
			"Resources":     selection.Resources,
		},
	}
	if selection.CreatorRequestID != "" {
		out["CreatorRequestId"] = selection.CreatorRequestID
	}
	return backupJSONResponse(http.StatusOK, out)
}

// deleteBackupSelection handles DeleteBackupSelection. Its `{}` is faithful: API_DeleteBackupSelection
// publishes "an HTTP 200 response with an empty HTTP body" (#1177).
func (p *BackupPlugin) deleteBackupSelection(reqCtx *RequestContext, planID, selectionID string) (*AWSResponse, error) {
	if _, err := p.loadSelection(reqCtx.AccountID, reqCtx.Region, planID, selectionID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := backupSelectionKey(reqCtx.AccountID, reqCtx.Region, planID, selectionID)
	if err := p.state.Delete(goCtx, backupNamespace, key); err != nil {
		return nil, fmt.Errorf("backup deleteBackupSelection delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, backupNamespace, backupSelectionIDsKey(reqCtx.AccountID, reqCtx.Region, planID), selectionID); err != nil {
		return nil, fmt.Errorf("backup deleteBackupSelection index: %w", err)
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{})
}

// loadVault loads a BackupVault from state or returns a not-found error.
func (p *BackupPlugin) loadVault(acct, region, name string) (*BackupVault, error) {
	if name == "" {
		return nil, backupMissingParameter("BackupVaultName")
	}
	goCtx := context.Background()
	key := backupVaultKey(acct, region, name)
	data, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup loadVault get: %w", err)
	}
	if data == nil {
		return nil, backupNotFound("Vault " + name + " does not exist.")
	}
	var vault BackupVault
	if err := json.Unmarshal(data, &vault); err != nil {
		return nil, fmt.Errorf("backup loadVault unmarshal: %w", err)
	}
	return &vault, nil
}

// loadPlan loads a BackupPlan from state or returns a not-found error.
func (p *BackupPlugin) loadPlan(acct, region, planID string) (*BackupPlan, error) {
	if planID == "" {
		return nil, backupMissingParameter("BackupPlanId")
	}
	goCtx := context.Background()
	key := backupPlanKey(acct, region, planID)
	data, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup loadPlan get: %w", err)
	}
	if data == nil {
		return nil, backupNotFound("Plan " + planID + " does not exist.")
	}
	var plan BackupPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("backup loadPlan unmarshal: %w", err)
	}
	return &plan, nil
}

// loadSelection loads a BackupSelection from state or returns a not-found error.
//
// The plan is loaded first, so a selection is reachable only while its plan exists (#1178). Before
// #1178 a plan could be deleted while it held selections, and those selections kept answering 200
// with the deleted plan's ID. State written by such a Substrate still holds them; checking the plan
// here makes them unreachable rather than resurrecting them.
func (p *BackupPlugin) loadSelection(acct, region, planID, selectionID string) (*BackupSelection, error) {
	if selectionID == "" {
		return nil, backupMissingParameter("SelectionId")
	}
	if _, err := p.loadPlan(acct, region, planID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := backupSelectionKey(acct, region, planID, selectionID)
	data, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup loadSelection get: %w", err)
	}
	if data == nil {
		return nil, backupNotFound("Selection " + selectionID + " does not exist.")
	}
	var selection BackupSelection
	if err := json.Unmarshal(data, &selection); err != nil {
		return nil, fmt.Errorf("backup loadSelection unmarshal: %w", err)
	}
	return &selection, nil
}

// generateBackupUUID mints one of AWS Backup's three identifiers from m — a backup-plan ID, a plan
// version ID or a selection ID — derived from the request id so a replayed create reports what the
// recording reported (#856).
//
// It is the tier's one generator with more than one call site, and the only one that draws *twice in
// one request*: `CreateBackupPlan` mints a plan ID and a version ID, so the ordinal is what keeps
// them apart. Both are addressed — every later `GetBackupPlan`, `UpdateBackupPlan`,
// `DeleteBackupPlan` and `CreateBackupSelection` takes the plan ID in its URL path — and a plan
// version is how Backup reports which revision a read answered from.
//
// Nothing about the shape is published: `BackupPlanId` is a String with no pattern and no length,
// and `VersionId` is documented only as "unique, randomly generated … at most 1,024 bytes long". So
// the rendering is the one the crypto/rand form produced, [IDMint.HexUUID] (#671). AWS's own sample
// ARN — `arn:aws:backup:us-east-1:123456789012:plan:8F81F553-3A74-4A3F-B93D-B3360DC80C50` — shows
// the UUID in *upper* case where substrate renders lower; that is an observed difference the model
// does not require, and changing it is a rendering change rather than part of deriving the value.
func generateBackupUUID(m *IDMint) string {
	return m.HexUUID()
}

// backupMissingParameter is the refusal every routed AWS Backup page publishes for an absent required
// member: MissingParameterValueException, "Indicates that a required parameter is missing", HTTP 400.
// It replaced InvalidRequestException (#1390), which only DeleteBackupVault's and DeleteBackupPlan's
// pages list, and which they gloss as input of the wrong type rather than input that is missing.
func backupMissingParameter(member string) *AWSError {
	return &AWSError{Code: "MissingParameterValueException", Message: member + " is required", HTTPStatus: http.StatusBadRequest}
}

// planHasSelections reports whether any selection of the plan still exists. It reads each indexed
// selection's record rather than trusting the index alone, so an index entry whose record is gone
// does not keep a plan undeletable.
func (p *BackupPlugin) planHasSelections(ctx context.Context, acct, region, planID string) (bool, error) {
	ids, err := loadStringIndex(ctx, p.state, backupNamespace, backupSelectionIDsKey(acct, region, planID))
	if err != nil {
		return false, fmt.Errorf("backup planHasSelections load index: %w", err)
	}
	for _, id := range ids {
		data, err := p.state.Get(ctx, backupNamespace, backupSelectionKey(acct, region, planID, id))
		if err != nil {
			return false, fmt.Errorf("backup planHasSelections get: %w", err)
		}
		if data != nil {
			return true, nil
		}
	}
	return false, nil
}

// backupPlanHasSelections is DeleteBackupPlan's refusal for a plan that still has selections (#1178).
//
// API_DeleteBackupPlan's first sentence states the precondition — "A backup plan can only be deleted
// after all associated selections of resources have been deleted" — and names no code for breaking
// it. Of the codes the page publishes, InvalidRequestException ("something is wrong with the input to
// the request", HTTP 400) is the only one that describes it: the request is well-formed and names a
// plan that exists, so neither InvalidParameterValueException, MissingParameterValueException nor
// ResourceNotFoundException fits, and no code is borrowed from a sibling page (#671). The message is
// substrate's own.
func backupPlanHasSelections(planID string) *AWSError {
	return &AWSError{
		Code:       "InvalidRequestException",
		Message:    "Backup plan " + planID + " has selections; delete its selections before deleting the plan.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// backupNotFound is the refusal every routed AWS Backup page publishes for a resource that does not
// exist: ResourceNotFoundException at HTTP 400, not 404 (#1390).
func backupNotFound(message string) *AWSError {
	return &AWSError{Code: "ResourceNotFoundException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// backupJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/json.
func backupJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("backup json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
