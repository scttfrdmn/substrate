package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupBackupPlugin(t *testing.T) (*substrate.BackupPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.BackupPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("BackupPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-backup-1",
	}
}

func backupRequest(t *testing.T, method, path string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal backup request: %v", err)
		}
	}
	return &substrate.AWSRequest{
		Service:   "backup",
		Operation: method,
		Path:      path,
		Headers:   map[string]string{},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestBackupPlugin_VaultCRUD(t *testing.T) {
	p, ctx := setupBackupPlugin(t)

	// CreateBackupVault.
	resp, err := p.HandleRequest(ctx, backupRequest(t, "PUT", "/backup-vaults/my-vault", map[string]any{
		"EncryptionKeyArn": "arn:aws:kms:us-east-1:123456789012:key/test-key",
	}))
	if err != nil {
		t.Fatalf("CreateBackupVault: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		BackupVaultArn  string `json:"BackupVaultArn"`
		BackupVaultName string `json:"BackupVaultName"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.BackupVaultName != "my-vault" {
		t.Errorf("want BackupVaultName=my-vault, got %q", createResult.BackupVaultName)
	}
	if createResult.BackupVaultArn == "" {
		t.Error("want non-empty BackupVaultArn")
	}

	// Duplicate create.
	_, err = p.HandleRequest(ctx, backupRequest(t, "PUT", "/backup-vaults/my-vault", nil))
	if err == nil {
		t.Fatal("want error for duplicate vault, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "AlreadyExistsException" {
		t.Errorf("want AlreadyExistsException, got %v", err)
	}

	// DescribeBackupVault.
	resp, err = p.HandleRequest(ctx, backupRequest(t, "GET", "/backup-vaults/my-vault", nil))
	if err != nil {
		t.Fatalf("DescribeBackupVault: %v", err)
	}
	var descResult struct {
		BackupVaultName  string `json:"BackupVaultName"`
		EncryptionKeyArn string `json:"EncryptionKeyArn"`
	}
	if err := json.Unmarshal(resp.Body, &descResult); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if descResult.BackupVaultName != "my-vault" {
		t.Errorf("want BackupVaultName=my-vault, got %q", descResult.BackupVaultName)
	}
	if descResult.EncryptionKeyArn == "" {
		t.Error("want non-empty EncryptionKeyArn")
	}

	// DeleteBackupVault.
	_, err = p.HandleRequest(ctx, backupRequest(t, "DELETE", "/backup-vaults/my-vault", nil))
	if err != nil {
		t.Fatalf("DeleteBackupVault: %v", err)
	}

	// DescribeBackupVault after delete.
	_, err = p.HandleRequest(ctx, backupRequest(t, "GET", "/backup-vaults/my-vault", nil))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}
}

func TestBackupPlugin_ListVaults(t *testing.T) {
	p, ctx := setupBackupPlugin(t)

	for _, name := range []string{"vault-alpha", "vault-beta"} {
		_, err := p.HandleRequest(ctx, backupRequest(t, "PUT", "/backup-vaults/"+name, nil))
		if err != nil {
			t.Fatalf("CreateBackupVault %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, backupRequest(t, "GET", "/backup-vaults", nil))
	if err != nil {
		t.Fatalf("ListBackupVaults: %v", err)
	}
	var result struct {
		BackupVaultList []substrate.BackupVault `json:"BackupVaultList"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.BackupVaultList) != 2 {
		t.Errorf("want 2 vaults, got %d", len(result.BackupVaultList))
	}
}

func TestBackupPlugin_PlanCRUD(t *testing.T) {
	p, ctx := setupBackupPlugin(t)

	// CreateBackupPlan.
	resp, err := p.HandleRequest(ctx, backupRequest(t, "POST", "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan",
			"Rules": []map[string]any{
				{
					"RuleName":           "daily-rule",
					"TargetBackupVaultName": "my-vault",
					"ScheduleExpression":   "cron(0 12 * * ? *)",
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreateBackupPlan: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		BackupPlanId  string `json:"BackupPlanId"`
		BackupPlanArn string `json:"BackupPlanArn"`
		VersionId     string `json:"VersionId"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.BackupPlanId == "" {
		t.Error("want non-empty BackupPlanId")
	}
	if createResult.VersionId == "" {
		t.Error("want non-empty VersionId")
	}
	planID := createResult.BackupPlanId
	firstVersionID := createResult.VersionId

	// GetBackupPlan.
	resp, err = p.HandleRequest(ctx, backupRequest(t, "GET", "/backup/plans/"+planID, nil))
	if err != nil {
		t.Fatalf("GetBackupPlan: %v", err)
	}
	var getResult struct {
		BackupPlanId string `json:"BackupPlanId"`
		BackupPlan   struct {
			BackupPlanName string `json:"BackupPlanName"`
		} `json:"BackupPlan"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if getResult.BackupPlan.BackupPlanName != "my-plan" {
		t.Errorf("want BackupPlanName=my-plan, got %q", getResult.BackupPlan.BackupPlanName)
	}

	// UpdateBackupPlan — new VersionId should differ from original.
	resp, err = p.HandleRequest(ctx, backupRequest(t, "POST", "/backup/plans/"+planID, map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "my-plan-updated",
		},
	}))
	if err != nil {
		t.Fatalf("UpdateBackupPlan: %v", err)
	}
	var updateResult struct {
		BackupPlanId string `json:"BackupPlanId"`
		VersionId    string `json:"VersionId"`
	}
	if err := json.Unmarshal(resp.Body, &updateResult); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if updateResult.VersionId == firstVersionID {
		t.Error("want new VersionId after update, got same as original")
	}

	// DeleteBackupPlan.
	_, err = p.HandleRequest(ctx, backupRequest(t, "DELETE", "/backup/plans/"+planID, nil))
	if err != nil {
		t.Fatalf("DeleteBackupPlan: %v", err)
	}

	// GetBackupPlan after delete.
	_, err = p.HandleRequest(ctx, backupRequest(t, "GET", "/backup/plans/"+planID, nil))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}
}

func TestBackupPlugin_ListPlans(t *testing.T) {
	p, ctx := setupBackupPlugin(t)

	for _, name := range []string{"plan-one", "plan-two"} {
		_, err := p.HandleRequest(ctx, backupRequest(t, "POST", "/backup/plans", map[string]any{
			"BackupPlan": map[string]any{"BackupPlanName": name},
		}))
		if err != nil {
			t.Fatalf("CreateBackupPlan %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, backupRequest(t, "GET", "/backup/plans", nil))
	if err != nil {
		t.Fatalf("ListBackupPlans: %v", err)
	}
	var result struct {
		BackupPlansList []map[string]any `json:"BackupPlansList"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.BackupPlansList) != 2 {
		t.Errorf("want 2 plans, got %d", len(result.BackupPlansList))
	}
}

func TestBackupPlugin_SelectionCRUD(t *testing.T) {
	p, ctx := setupBackupPlugin(t)

	// Create a plan first.
	resp, err := p.HandleRequest(ctx, backupRequest(t, "POST", "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "sel-plan"},
	}))
	if err != nil {
		t.Fatalf("CreateBackupPlan: %v", err)
	}
	var planResult struct {
		BackupPlanId string `json:"BackupPlanId"`
	}
	if err := json.Unmarshal(resp.Body, &planResult); err != nil {
		t.Fatalf("unmarshal plan: %v", err)
	}
	planID := planResult.BackupPlanId

	// CreateBackupSelection.
	resp, err = p.HandleRequest(ctx, backupRequest(t, "POST", "/backup/plans/"+planID+"/selections", map[string]any{
		"BackupSelection": map[string]any{
			"SelectionName": "my-selection",
			"IamRoleArn":    "arn:aws:iam::123456789012:role/backup-role",
			"Resources":     []string{"arn:aws:dynamodb:us-east-1:123456789012:table/my-table"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateBackupSelection: %v", err)
	}
	var selResult struct {
		SelectionId  string `json:"SelectionId"`
		BackupPlanId string `json:"BackupPlanId"`
	}
	if err := json.Unmarshal(resp.Body, &selResult); err != nil {
		t.Fatalf("unmarshal selection: %v", err)
	}
	if selResult.SelectionId == "" {
		t.Error("want non-empty SelectionId")
	}
	if selResult.BackupPlanId != planID {
		t.Errorf("want BackupPlanId=%s, got %s", planID, selResult.BackupPlanId)
	}
	selectionID := selResult.SelectionId

	// GetBackupSelection.
	resp, err = p.HandleRequest(ctx, backupRequest(t, "GET", "/backup/plans/"+planID+"/selections/"+selectionID, nil))
	if err != nil {
		t.Fatalf("GetBackupSelection: %v", err)
	}
	var getSelResult struct {
		SelectionId     string `json:"SelectionId"`
		BackupSelection struct {
			SelectionName string `json:"SelectionName"`
		} `json:"BackupSelection"`
	}
	if err := json.Unmarshal(resp.Body, &getSelResult); err != nil {
		t.Fatalf("unmarshal get selection: %v", err)
	}
	if getSelResult.BackupSelection.SelectionName != "my-selection" {
		t.Errorf("want SelectionName=my-selection, got %q", getSelResult.BackupSelection.SelectionName)
	}

	// DeleteBackupSelection.
	_, err = p.HandleRequest(ctx, backupRequest(t, "DELETE", "/backup/plans/"+planID+"/selections/"+selectionID, nil))
	if err != nil {
		t.Fatalf("DeleteBackupSelection: %v", err)
	}

	// GetBackupSelection after delete.
	_, err = p.HandleRequest(ctx, backupRequest(t, "GET", "/backup/plans/"+planID+"/selections/"+selectionID, nil))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}
}
