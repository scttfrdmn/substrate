package substrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
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
	op, vaultName, planID, selectionID := parseBackupOperation(req.Operation, req.Path)
	switch op {
	case "CreateBackupVault":
		return p.createBackupVault(reqCtx, req, vaultName)
	case "DescribeBackupVault":
		return p.describeBackupVault(reqCtx, vaultName)
	case "DeleteBackupVault":
		return p.deleteBackupVault(reqCtx, vaultName)
	case "ListBackupVaults":
		return p.listBackupVaults(reqCtx)
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
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "BackupPlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *BackupPlugin) createBackupVault(reqCtx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	if name == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "BackupVaultName is required", HTTPStatus: http.StatusBadRequest}
	}

	var input struct {
		EncryptionKeyArn string `json:"EncryptionKeyArn"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	goCtx := context.Background()
	key := backupVaultKey(reqCtx.AccountID, reqCtx.Region, name)
	existing, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup createBackupVault get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "AlreadyExistsException", Message: "Vault " + name + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	now := p.tc.Now()
	vault := BackupVault{
		BackupVaultName:        name,
		BackupVaultArn:         fmt.Sprintf("arn:aws:backup:%s:%s:backup-vault:%s", reqCtx.Region, reqCtx.AccountID, name),
		EncryptionKeyArn:       input.EncryptionKeyArn,
		CreationDate:           now,
		NumberOfRecoveryPoints: 0,
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
	updateStringIndex(goCtx, p.state, backupNamespace, backupVaultNamesKey(reqCtx.AccountID, reqCtx.Region), name)

	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupVaultArn":  vault.BackupVaultArn,
		"BackupVaultName": vault.BackupVaultName,
		"CreationDate":    vault.CreationDate,
	})
}

func (p *BackupPlugin) describeBackupVault(reqCtx *RequestContext, name string) (*AWSResponse, error) {
	vault, err := p.loadVault(reqCtx.AccountID, reqCtx.Region, name)
	if err != nil {
		return nil, err
	}
	return backupJSONResponse(http.StatusOK, vault)
}

func (p *BackupPlugin) deleteBackupVault(reqCtx *RequestContext, name string) (*AWSResponse, error) {
	if _, err := p.loadVault(reqCtx.AccountID, reqCtx.Region, name); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := backupVaultKey(reqCtx.AccountID, reqCtx.Region, name)
	if err := p.state.Delete(goCtx, backupNamespace, key); err != nil {
		return nil, fmt.Errorf("backup deleteBackupVault delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, backupNamespace, backupVaultNamesKey(reqCtx.AccountID, reqCtx.Region), name)
	return backupJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *BackupPlugin) listBackupVaults(reqCtx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, backupNamespace, backupVaultNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("backup listBackupVaults load index: %w", err)
	}
	vaults := make([]BackupVault, 0, len(names))
	for _, name := range names {
		v, err := p.loadVault(reqCtx.AccountID, reqCtx.Region, name)
		if err != nil {
			continue
		}
		vaults = append(vaults, *v)
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupVaultList": vaults,
	})
}

func (p *BackupPlugin) createBackupPlan(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		BackupPlan struct {
			BackupPlanName string                   `json:"BackupPlanName"`
			Rules          []map[string]interface{} `json:"Rules"`
		} `json:"BackupPlan"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.BackupPlan.BackupPlanName == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "BackupPlanName is required", HTTPStatus: http.StatusBadRequest}
	}

	planID := generateBackupUUID()
	versionID := generateBackupUUID()
	now := p.tc.Now()

	plan := BackupPlan{
		BackupPlanID:   planID,
		BackupPlanArn:  fmt.Sprintf("arn:aws:backup:%s:%s:backup-plan:%s", reqCtx.Region, reqCtx.AccountID, planID),
		BackupPlanName: input.BackupPlan.BackupPlanName,
		Rules:          input.BackupPlan.Rules,
		VersionID:      versionID,
		CreationDate:   now,
		AccountID:      reqCtx.AccountID,
		Region:         reqCtx.Region,
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
	updateStringIndex(goCtx, p.state, backupNamespace, backupPlanIDsKey(reqCtx.AccountID, reqCtx.Region), planID)

	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlanId":  planID,
		"BackupPlanArn": plan.BackupPlanArn,
		"CreationDate":  now,
		"VersionId":     versionID,
	})
}

func (p *BackupPlugin) getBackupPlan(reqCtx *RequestContext, planID string) (*AWSResponse, error) {
	plan, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID)
	if err != nil {
		return nil, err
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlanId":  plan.BackupPlanID,
		"BackupPlanArn": plan.BackupPlanArn,
		"VersionId":     plan.VersionID,
		"CreationDate":  plan.CreationDate,
		"BackupPlan": map[string]interface{}{
			"BackupPlanName": plan.BackupPlanName,
			"Rules":          plan.Rules,
		},
	})
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
		_ = json.Unmarshal(req.Body, &input)
	}

	if input.BackupPlan.BackupPlanName != "" {
		plan.BackupPlanName = input.BackupPlan.BackupPlanName
	}
	if input.BackupPlan.Rules != nil {
		plan.Rules = input.BackupPlan.Rules
	}
	plan.VersionID = generateBackupUUID()

	goCtx := context.Background()
	data, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("backup updateBackupPlan marshal: %w", err)
	}
	key := backupPlanKey(reqCtx.AccountID, reqCtx.Region, planID)
	if err := p.state.Put(goCtx, backupNamespace, key, data); err != nil {
		return nil, fmt.Errorf("backup updateBackupPlan put: %w", err)
	}

	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"BackupPlanId":  plan.BackupPlanID,
		"BackupPlanArn": plan.BackupPlanArn,
		"UpdatedAt":     p.tc.Now(),
		"VersionId":     plan.VersionID,
	})
}

func (p *BackupPlugin) deleteBackupPlan(reqCtx *RequestContext, planID string) (*AWSResponse, error) {
	if _, err := p.loadPlan(reqCtx.AccountID, reqCtx.Region, planID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := backupPlanKey(reqCtx.AccountID, reqCtx.Region, planID)
	if err := p.state.Delete(goCtx, backupNamespace, key); err != nil {
		return nil, fmt.Errorf("backup deleteBackupPlan delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, backupNamespace, backupPlanIDsKey(reqCtx.AccountID, reqCtx.Region), planID)
	return backupJSONResponse(http.StatusOK, map[string]interface{}{})
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
		summaries = append(summaries, map[string]interface{}{
			"BackupPlanId":   plan.BackupPlanID,
			"BackupPlanArn":  plan.BackupPlanArn,
			"BackupPlanName": plan.BackupPlanName,
			"VersionId":      plan.VersionID,
			"CreationDate":   plan.CreationDate,
		})
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
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.BackupSelection.SelectionName == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "SelectionName is required", HTTPStatus: http.StatusBadRequest}
	}

	selectionID := generateBackupUUID()
	now := p.tc.Now()
	selection := BackupSelection{
		SelectionID:   selectionID,
		SelectionName: input.BackupSelection.SelectionName,
		BackupPlanID:  planID,
		IamRoleArn:    input.BackupSelection.IamRoleArn,
		Resources:     input.BackupSelection.Resources,
		CreationDate:  now,
		AccountID:     reqCtx.AccountID,
		Region:        reqCtx.Region,
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
	updateStringIndex(goCtx, p.state, backupNamespace, backupSelectionIDsKey(reqCtx.AccountID, reqCtx.Region, planID), selectionID)

	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"SelectionId":  selectionID,
		"BackupPlanId": planID,
		"CreationDate": now,
	})
}

func (p *BackupPlugin) getBackupSelection(reqCtx *RequestContext, planID, selectionID string) (*AWSResponse, error) {
	selection, err := p.loadSelection(reqCtx.AccountID, reqCtx.Region, planID, selectionID)
	if err != nil {
		return nil, err
	}
	return backupJSONResponse(http.StatusOK, map[string]interface{}{
		"SelectionId":  selection.SelectionID,
		"BackupPlanId": selection.BackupPlanID,
		"CreationDate": selection.CreationDate,
		"BackupSelection": map[string]interface{}{
			"SelectionName": selection.SelectionName,
			"IamRoleArn":    selection.IamRoleArn,
			"Resources":     selection.Resources,
		},
	})
}

func (p *BackupPlugin) deleteBackupSelection(reqCtx *RequestContext, planID, selectionID string) (*AWSResponse, error) {
	if _, err := p.loadSelection(reqCtx.AccountID, reqCtx.Region, planID, selectionID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := backupSelectionKey(reqCtx.AccountID, reqCtx.Region, planID, selectionID)
	if err := p.state.Delete(goCtx, backupNamespace, key); err != nil {
		return nil, fmt.Errorf("backup deleteBackupSelection delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, backupNamespace, backupSelectionIDsKey(reqCtx.AccountID, reqCtx.Region, planID), selectionID)
	return backupJSONResponse(http.StatusOK, map[string]interface{}{})
}

// loadVault loads a BackupVault from state or returns a not-found error.
func (p *BackupPlugin) loadVault(acct, region, name string) (*BackupVault, error) {
	if name == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "BackupVaultName is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := backupVaultKey(acct, region, name)
	data, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup loadVault get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Vault " + name + " does not exist.", HTTPStatus: http.StatusNotFound}
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
		return nil, &AWSError{Code: "InvalidRequestException", Message: "BackupPlanId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := backupPlanKey(acct, region, planID)
	data, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup loadPlan get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Plan " + planID + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var plan BackupPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("backup loadPlan unmarshal: %w", err)
	}
	return &plan, nil
}

// loadSelection loads a BackupSelection from state or returns a not-found error.
func (p *BackupPlugin) loadSelection(acct, region, planID, selectionID string) (*BackupSelection, error) {
	if selectionID == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "SelectionId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := backupSelectionKey(acct, region, planID, selectionID)
	data, err := p.state.Get(goCtx, backupNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("backup loadSelection get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Selection " + selectionID + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var selection BackupSelection
	if err := json.Unmarshal(data, &selection); err != nil {
		return nil, fmt.Errorf("backup loadSelection unmarshal: %w", err)
	}
	return &selection, nil
}

// generateBackupUUID generates a UUID-style string for backup resource IDs.
func generateBackupUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
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
