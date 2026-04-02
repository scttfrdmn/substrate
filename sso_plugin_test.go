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

func setupSSOPlugin(t *testing.T) (*substrate.SSOPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.SSOPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("SSOPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-sso-1",
	}
}

func ssoRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal sso request: %v", err)
		}
	}
	return &substrate.AWSRequest{
		Service:   "sso",
		Operation: op,
		Path:      "/",
		Headers:   map[string]string{},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestSSOPlugin_ListInstances_AutoCreate(t *testing.T) {
	p, ctx := setupSSOPlugin(t)

	// First call — should auto-create instance.
	resp, err := p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	if err != nil {
		t.Fatalf("ListInstances first: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	var result1 struct {
		Instances []struct {
			InstanceArn     string `json:"InstanceArn"`
			IdentityStoreId string `json:"IdentityStoreId"`
		} `json:"Instances"`
	}
	if err := json.Unmarshal(resp.Body, &result1); err != nil {
		t.Fatalf("unmarshal first: %v", err)
	}
	if len(result1.Instances) != 1 {
		t.Fatalf("want 1 instance, got %d", len(result1.Instances))
	}
	firstArn := result1.Instances[0].InstanceArn
	if firstArn == "" {
		t.Error("want non-empty InstanceArn")
	}
	if result1.Instances[0].IdentityStoreId == "" {
		t.Error("want non-empty IdentityStoreId")
	}

	// Second call — should return same instance (singleton).
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	if err != nil {
		t.Fatalf("ListInstances second: %v", err)
	}
	var result2 struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	if err := json.Unmarshal(resp.Body, &result2); err != nil {
		t.Fatalf("unmarshal second: %v", err)
	}
	if len(result2.Instances) != 1 {
		t.Fatalf("want 1 instance on second call, got %d", len(result2.Instances))
	}
	if result2.Instances[0].InstanceArn != firstArn {
		t.Errorf("want same InstanceArn on second call: got %q, want %q", result2.Instances[0].InstanceArn, firstArn)
	}
}

func TestSSOPlugin_PermissionSetCRUD(t *testing.T) {
	p, ctx := setupSSOPlugin(t)

	// Get instance ARN first.
	resp, err := p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	if err != nil {
		t.Fatalf("ListInstances: %v", err)
	}
	var instResult struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	if err := json.Unmarshal(resp.Body, &instResult); err != nil {
		t.Fatalf("unmarshal instances: %v", err)
	}
	instanceArn := instResult.Instances[0].InstanceArn

	// CreatePermissionSet.
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "CreatePermissionSet", map[string]any{
		"InstanceArn":     instanceArn,
		"Name":            "AdminAccess",
		"Description":     "Admin permission set",
		"SessionDuration": "PT8H",
	}))
	if err != nil {
		t.Fatalf("CreatePermissionSet: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	var createResult struct {
		PermissionSet struct {
			PermissionSetArn string `json:"PermissionSetArn"`
			Name             string `json:"Name"`
			SessionDuration  string `json:"SessionDuration"`
		} `json:"PermissionSet"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.PermissionSet.Name != "AdminAccess" {
		t.Errorf("want Name=AdminAccess, got %q", createResult.PermissionSet.Name)
	}
	permSetArn := createResult.PermissionSet.PermissionSetArn
	if permSetArn == "" {
		t.Fatal("want non-empty PermissionSetArn")
	}

	// DescribePermissionSet.
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "DescribePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
	}))
	if err != nil {
		t.Fatalf("DescribePermissionSet: %v", err)
	}
	var descResult struct {
		PermissionSet struct {
			Name string `json:"Name"`
		} `json:"PermissionSet"`
	}
	if err := json.Unmarshal(resp.Body, &descResult); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if descResult.PermissionSet.Name != "AdminAccess" {
		t.Errorf("want Name=AdminAccess, got %q", descResult.PermissionSet.Name)
	}

	// UpdatePermissionSet.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "UpdatePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
		"Description":      "Updated description",
	}))
	if err != nil {
		t.Fatalf("UpdatePermissionSet: %v", err)
	}

	// DeletePermissionSet.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "DeletePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
	}))
	if err != nil {
		t.Fatalf("DeletePermissionSet: %v", err)
	}

	// DescribePermissionSet after delete.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "DescribePermissionSet", map[string]any{
		"PermissionSetArn": permSetArn,
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}
}

func TestSSOPlugin_ListPermissionSets(t *testing.T) {
	p, ctx := setupSSOPlugin(t)

	resp, _ := p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	var instResult struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	_ = json.Unmarshal(resp.Body, &instResult)
	instanceArn := instResult.Instances[0].InstanceArn

	for _, name := range []string{"ReadOnly", "PowerUser"} {
		_, err := p.HandleRequest(ctx, ssoRequest(t, "CreatePermissionSet", map[string]any{
			"InstanceArn": instanceArn,
			"Name":        name,
		}))
		if err != nil {
			t.Fatalf("CreatePermissionSet %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, ssoRequest(t, "ListPermissionSets", map[string]any{
		"InstanceArn": instanceArn,
	}))
	if err != nil {
		t.Fatalf("ListPermissionSets: %v", err)
	}
	var result struct {
		PermissionSets []string `json:"PermissionSets"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.PermissionSets) != 2 {
		t.Errorf("want 2 permission sets, got %d", len(result.PermissionSets))
	}
}

func TestSSOPlugin_ManagedPolicyAttachDetach(t *testing.T) {
	p, ctx := setupSSOPlugin(t)

	resp, _ := p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	var instResult struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	_ = json.Unmarshal(resp.Body, &instResult)
	instanceArn := instResult.Instances[0].InstanceArn

	resp, err := p.HandleRequest(ctx, ssoRequest(t, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceArn,
		"Name":        "TestPS",
	}))
	if err != nil {
		t.Fatalf("CreatePermissionSet: %v", err)
	}
	var psResult struct {
		PermissionSet struct {
			PermissionSetArn string `json:"PermissionSetArn"`
		} `json:"PermissionSet"`
	}
	_ = json.Unmarshal(resp.Body, &psResult)
	permSetArn := psResult.PermissionSet.PermissionSetArn

	policyArn := "arn:aws:iam::aws:policy/AdministratorAccess"

	// Attach.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "AttachManagedPolicyToPermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
		"ManagedPolicyArn": policyArn,
	}))
	if err != nil {
		t.Fatalf("AttachManagedPolicyToPermissionSet: %v", err)
	}

	// List.
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "ListManagedPoliciesInPermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
	}))
	if err != nil {
		t.Fatalf("ListManagedPoliciesInPermissionSet: %v", err)
	}
	var listResult struct {
		AttachedManagedPolicies []struct {
			Arn string `json:"Arn"`
		} `json:"AttachedManagedPolicies"`
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listResult.AttachedManagedPolicies) != 1 {
		t.Errorf("want 1 policy, got %d", len(listResult.AttachedManagedPolicies))
	}
	if listResult.AttachedManagedPolicies[0].Arn != policyArn {
		t.Errorf("want Arn=%s, got %s", policyArn, listResult.AttachedManagedPolicies[0].Arn)
	}

	// Detach.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "DetachManagedPolicyFromPermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
		"ManagedPolicyArn": policyArn,
	}))
	if err != nil {
		t.Fatalf("DetachManagedPolicyFromPermissionSet: %v", err)
	}

	// List again — should be empty.
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "ListManagedPoliciesInPermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
	}))
	if err != nil {
		t.Fatalf("ListManagedPoliciesInPermissionSet after detach: %v", err)
	}
	var listResult2 struct {
		AttachedManagedPolicies []struct {
			Arn string `json:"Arn"`
		} `json:"AttachedManagedPolicies"`
	}
	if err := json.Unmarshal(resp.Body, &listResult2); err != nil {
		t.Fatalf("unmarshal after detach: %v", err)
	}
	if len(listResult2.AttachedManagedPolicies) != 0 {
		t.Errorf("want 0 policies after detach, got %d", len(listResult2.AttachedManagedPolicies))
	}
}

func TestSSOPlugin_AccountAssignment_CreateDelete(t *testing.T) {
	p, ctx := setupSSOPlugin(t)

	resp, _ := p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	var instResult struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	_ = json.Unmarshal(resp.Body, &instResult)
	instanceArn := instResult.Instances[0].InstanceArn

	resp, _ = p.HandleRequest(ctx, ssoRequest(t, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceArn,
		"Name":        "DevAccess",
	}))
	var psResult struct {
		PermissionSet struct {
			PermissionSetArn string `json:"PermissionSetArn"`
		} `json:"PermissionSet"`
	}
	_ = json.Unmarshal(resp.Body, &psResult)
	permSetArn := psResult.PermissionSet.PermissionSetArn

	targetAccountID := "999999999999"

	// CreateAccountAssignment.
	resp, err := p.HandleRequest(ctx, ssoRequest(t, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
		"TargetId":         targetAccountID,
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "GROUP",
		"PrincipalId":      "group-id-123",
	}))
	if err != nil {
		t.Fatalf("CreateAccountAssignment: %v", err)
	}
	var createResult struct {
		AccountAssignmentCreationStatus struct {
			Status    string `json:"Status"`
			RequestId string `json:"RequestId"`
		} `json:"AccountAssignmentCreationStatus"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.AccountAssignmentCreationStatus.Status != "SUCCEEDED" {
		t.Errorf("want Status=SUCCEEDED, got %q", createResult.AccountAssignmentCreationStatus.Status)
	}
	if createResult.AccountAssignmentCreationStatus.RequestId == "" {
		t.Error("want non-empty RequestId")
	}

	// ListAccountAssignments.
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "ListAccountAssignments", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
		"AccountId":        targetAccountID,
	}))
	if err != nil {
		t.Fatalf("ListAccountAssignments: %v", err)
	}
	var listResult struct {
		AccountAssignments []struct {
			AccountId     string `json:"AccountId"`
			PrincipalType string `json:"PrincipalType"`
		} `json:"AccountAssignments"`
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listResult.AccountAssignments) != 1 {
		t.Fatalf("want 1 assignment, got %d", len(listResult.AccountAssignments))
	}
	if listResult.AccountAssignments[0].AccountId != targetAccountID {
		t.Errorf("want AccountId=%s, got %s", targetAccountID, listResult.AccountAssignments[0].AccountId)
	}

	// DeleteAccountAssignment.
	resp, err = p.HandleRequest(ctx, ssoRequest(t, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
		"TargetId":         targetAccountID,
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "GROUP",
		"PrincipalId":      "group-id-123",
	}))
	if err != nil {
		t.Fatalf("DeleteAccountAssignment: %v", err)
	}
	var deleteResult struct {
		AccountAssignmentDeletionStatus struct {
			Status string `json:"Status"`
		} `json:"AccountAssignmentDeletionStatus"`
	}
	if err := json.Unmarshal(resp.Body, &deleteResult); err != nil {
		t.Fatalf("unmarshal delete: %v", err)
	}
	if deleteResult.AccountAssignmentDeletionStatus.Status != "SUCCEEDED" {
		t.Errorf("want Status=SUCCEEDED, got %q", deleteResult.AccountAssignmentDeletionStatus.Status)
	}
}

func TestSSOPlugin_Errors(t *testing.T) {
	p, ctx := setupSSOPlugin(t)

	// CreatePermissionSet missing Name.
	_, err := p.HandleRequest(ctx, ssoRequest(t, "CreatePermissionSet", map[string]any{
		"InstanceArn": "arn:aws:sso:::instance/test",
	}))
	if err == nil {
		t.Fatal("want error for missing Name")
	}

	// DescribePermissionSet missing ARN.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "DescribePermissionSet", map[string]any{}))
	if err == nil {
		t.Fatal("want error for missing PermissionSetArn")
	}

	// DescribePermissionSet not found.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "DescribePermissionSet", map[string]any{
		"PermissionSetArn": "arn:aws:sso:::instance/test/ps-nonexistent",
	}))
	if err == nil {
		t.Fatal("want error for nonexistent permission set")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}

	// Unsupported operation.
	_, err = p.HandleRequest(ctx, ssoRequest(t, "UnknownOp", nil))
	if err == nil {
		t.Fatal("want error for unsupported operation")
	}

	// ListAccountAssignments with no filter.
	resp, _ := p.HandleRequest(ctx, ssoRequest(t, "ListInstances", nil))
	var instResult struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	_ = json.Unmarshal(resp.Body, &instResult)
	instanceArn := instResult.Instances[0].InstanceArn

	resp2, err := p.HandleRequest(ctx, ssoRequest(t, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceArn,
		"Name":        "TestPS2",
	}))
	if err != nil {
		t.Fatalf("CreatePermissionSet: %v", err)
	}
	var psResult struct {
		PermissionSet struct {
			PermissionSetArn string `json:"PermissionSetArn"`
		} `json:"PermissionSet"`
	}
	_ = json.Unmarshal(resp2.Body, &psResult)
	permSetArn := psResult.PermissionSet.PermissionSetArn

	// ListAccountAssignments (no AccountId filter) — should return all.
	resp3, err := p.HandleRequest(ctx, ssoRequest(t, "ListAccountAssignments", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": permSetArn,
	}))
	if err != nil {
		t.Fatalf("ListAccountAssignments: %v", err)
	}
	var listResult struct {
		AccountAssignments []struct{} `json:"AccountAssignments"`
	}
	if err := json.Unmarshal(resp3.Body, &listResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(listResult.AccountAssignments) != 0 {
		t.Errorf("want 0 assignments, got %d", len(listResult.AccountAssignments))
	}
}
