package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"
)

// SSOPlugin emulates the AWS IAM Identity Center (SSO) service.
// It handles permission set and account assignment CRUD operations using the
// JSON-target protocol (X-Amz-Target: AWSSSOAdminService.{Op}).
type SSOPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "sso".
func (p *SSOPlugin) Name() string { return ssoNamespace }

// Initialize sets up the SSOPlugin with the provided configuration.
func (p *SSOPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SSOPlugin.
func (p *SSOPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an IAM Identity Center JSON-target request to the appropriate handler.
func (p *SSOPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "ListInstances":
		return p.listInstances(reqCtx, req)
	case "CreatePermissionSet":
		return p.createPermissionSet(reqCtx, req)
	case "DescribePermissionSet":
		return p.describePermissionSet(reqCtx, req)
	case "UpdatePermissionSet":
		return p.updatePermissionSet(reqCtx, req)
	case "DeletePermissionSet":
		return p.deletePermissionSet(reqCtx, req)
	case "ListPermissionSets":
		return p.listPermissionSets(reqCtx, req)
	case "AttachManagedPolicyToPermissionSet":
		return p.attachManagedPolicy(reqCtx, req)
	case "DetachManagedPolicyFromPermissionSet":
		return p.detachManagedPolicy(reqCtx, req)
	case "ListManagedPoliciesInPermissionSet":
		return p.listManagedPolicies(reqCtx, req)
	case "CreateAccountAssignment":
		return p.createAccountAssignment(reqCtx, req)
	case "DeleteAccountAssignment":
		return p.deleteAccountAssignment(reqCtx, req)
	case "ListAccountAssignments":
		return p.listAccountAssignments(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "SSOPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// ensureInstance auto-creates the singleton SSO instance for an account on first access,
// following the same pattern as ensureOrganization in organizations_plugin.go.
func (p *SSOPlugin) ensureInstance(goCtx context.Context, acct string) (*SSOInstance, error) {
	key := ssoInstanceKey(acct)
	data, err := p.state.Get(goCtx, ssoNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("sso ensureInstance get: %w", err)
	}
	if data != nil {
		var inst SSOInstance
		if err := json.Unmarshal(data, &inst); err != nil {
			return nil, fmt.Errorf("sso ensureInstance unmarshal: %w", err)
		}
		return &inst, nil
	}

	inst := SSOInstance{
		InstanceArn:     generateSSOInstanceArn(),
		IdentityStoreID: generateSSOIdentityStoreID(),
		Status:          "ACTIVE",
		CreatedDate:     p.tc.Now(),
		AccountID:       acct,
	}
	d, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("sso ensureInstance marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ssoNamespace, key, d); err != nil {
		return nil, fmt.Errorf("sso ensureInstance put: %w", err)
	}
	return &inst, nil
}

func (p *SSOPlugin) listInstances(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	inst, err := p.ensureInstance(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, err
	}
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"Instances": []map[string]interface{}{
			{
				"InstanceArn":     inst.InstanceArn,
				"IdentityStoreId": inst.IdentityStoreID,
				"Status":          inst.Status,
				"CreatedDate":     inst.CreatedDate,
			},
		},
		"NextToken": "",
	})
}

func (p *SSOPlugin) createPermissionSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		InstanceArn     string `json:"InstanceArn"`
		Name            string `json:"Name"`
		Description     string `json:"Description"`
		SessionDuration string `json:"SessionDuration"`
		RelayState      string `json:"RelayState"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "ValidationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	inst, err := p.ensureInstance(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, err
	}
	instanceArn := input.InstanceArn
	if instanceArn == "" {
		instanceArn = inst.InstanceArn
	}

	permSetArn := generateSSOPermissionSetArn(instanceArn)
	ps := SSOPermissionSet{
		PermissionSetArn: permSetArn,
		Name:             input.Name,
		Description:      input.Description,
		SessionDuration:  input.SessionDuration,
		RelayState:       input.RelayState,
		CreatedDate:      p.tc.Now(),
		AccountID:        reqCtx.AccountID,
		InstanceArn:      instanceArn,
	}

	d, err := json.Marshal(ps)
	if err != nil {
		return nil, fmt.Errorf("sso createPermissionSet marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ssoNamespace, ssoPermSetKey(reqCtx.AccountID, permSetArn), d); err != nil {
		return nil, fmt.Errorf("sso createPermissionSet put: %w", err)
	}
	updateStringIndex(goCtx, p.state, ssoNamespace, ssoPermSetArnsKey(reqCtx.AccountID), permSetArn)

	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"PermissionSet": ps,
	})
}

func (p *SSOPlugin) describePermissionSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	ps, err := p.loadPermissionSet(reqCtx.AccountID, input.PermissionSetArn)
	if err != nil {
		return nil, err
	}
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"PermissionSet": ps,
	})
}

func (p *SSOPlugin) updatePermissionSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
		Description      string `json:"Description"`
		SessionDuration  string `json:"SessionDuration"`
		RelayState       string `json:"RelayState"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	ps, err := p.loadPermissionSet(reqCtx.AccountID, input.PermissionSetArn)
	if err != nil {
		return nil, err
	}

	if input.Description != "" {
		ps.Description = input.Description
	}
	if input.SessionDuration != "" {
		ps.SessionDuration = input.SessionDuration
	}
	if input.RelayState != "" {
		ps.RelayState = input.RelayState
	}

	goCtx := context.Background()
	d, err := json.Marshal(ps)
	if err != nil {
		return nil, fmt.Errorf("sso updatePermissionSet marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ssoNamespace, ssoPermSetKey(reqCtx.AccountID, ps.PermissionSetArn), d); err != nil {
		return nil, fmt.Errorf("sso updatePermissionSet put: %w", err)
	}
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSOPlugin) deletePermissionSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	if _, err := p.loadPermissionSet(reqCtx.AccountID, input.PermissionSetArn); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	if err := p.state.Delete(goCtx, ssoNamespace, ssoPermSetKey(reqCtx.AccountID, input.PermissionSetArn)); err != nil {
		return nil, fmt.Errorf("sso deletePermissionSet delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, ssoNamespace, ssoPermSetArnsKey(reqCtx.AccountID), input.PermissionSetArn)
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSOPlugin) listPermissionSets(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	arns, err := loadStringIndex(goCtx, p.state, ssoNamespace, ssoPermSetArnsKey(reqCtx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("sso listPermissionSets load index: %w", err)
	}
	if arns == nil {
		arns = []string{}
	}
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"PermissionSets": arns,
		"NextToken":      "",
	})
}

func (p *SSOPlugin) attachManagedPolicy(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
		ManagedPolicyArn string `json:"ManagedPolicyArn"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	if _, err := p.loadPermissionSet(reqCtx.AccountID, input.PermissionSetArn); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	updateStringIndex(goCtx, p.state, ssoNamespace, ssoManagedPoliciesKey(reqCtx.AccountID, input.PermissionSetArn), input.ManagedPolicyArn)
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSOPlugin) detachManagedPolicy(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
		ManagedPolicyArn string `json:"ManagedPolicyArn"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	if _, err := p.loadPermissionSet(reqCtx.AccountID, input.PermissionSetArn); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	removeFromStringIndex(goCtx, p.state, ssoNamespace, ssoManagedPoliciesKey(reqCtx.AccountID, input.PermissionSetArn), input.ManagedPolicyArn)
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSOPlugin) listManagedPolicies(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	if _, err := p.loadPermissionSet(reqCtx.AccountID, input.PermissionSetArn); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	arns, err := loadStringIndex(goCtx, p.state, ssoNamespace, ssoManagedPoliciesKey(reqCtx.AccountID, input.PermissionSetArn))
	if err != nil {
		return nil, fmt.Errorf("sso listManagedPolicies load index: %w", err)
	}
	policies := make([]map[string]interface{}, 0, len(arns))
	for _, arn := range arns {
		policies = append(policies, map[string]interface{}{
			"Arn":  arn,
			"Name": path.Base(arn),
		})
	}
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"AttachedManagedPolicies": policies,
		"NextToken":               "",
	})
}

func (p *SSOPlugin) createAccountAssignment(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		TargetID         string `json:"TargetId"`
		TargetType       string `json:"TargetType"`
		PrincipalType    string `json:"PrincipalType"`
		PrincipalID      string `json:"PrincipalId"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	assignment := SSOAccountAssignment{
		PermissionSetArn: input.PermissionSetArn,
		TargetID:         input.TargetID,
		TargetType:       input.TargetType,
		PrincipalType:    input.PrincipalType,
		PrincipalID:      input.PrincipalID,
		AccountID:        reqCtx.AccountID,
		InstanceArn:      input.InstanceArn,
	}

	goCtx := context.Background()
	d, err := json.Marshal(assignment)
	if err != nil {
		return nil, fmt.Errorf("sso createAccountAssignment marshal: %w", err)
	}
	key := ssoAssignmentKey(reqCtx.AccountID, input.PermissionSetArn, input.TargetID, input.PrincipalType, input.PrincipalID)
	if err := p.state.Put(goCtx, ssoNamespace, key, d); err != nil {
		return nil, fmt.Errorf("sso createAccountAssignment put: %w", err)
	}
	compositeKey := input.TargetID + "/" + input.PrincipalType + "/" + input.PrincipalID
	updateStringIndex(goCtx, p.state, ssoNamespace, ssoAssignmentKeysKey(reqCtx.AccountID, input.PermissionSetArn), compositeKey)

	requestID := generateSSORequestID()
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"AccountAssignmentCreationStatus": map[string]interface{}{
			"Status":           "SUCCEEDED",
			"RequestId":        requestID,
			"PermissionSetArn": input.PermissionSetArn,
			"TargetId":         input.TargetID,
			"TargetType":       input.TargetType,
			"PrincipalType":    input.PrincipalType,
			"PrincipalId":      input.PrincipalID,
		},
	})
}

func (p *SSOPlugin) deleteAccountAssignment(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		TargetID         string `json:"TargetId"`
		TargetType       string `json:"TargetType"`
		PrincipalType    string `json:"PrincipalType"`
		PrincipalID      string `json:"PrincipalId"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	goCtx := context.Background()
	key := ssoAssignmentKey(reqCtx.AccountID, input.PermissionSetArn, input.TargetID, input.PrincipalType, input.PrincipalID)
	if err := p.state.Delete(goCtx, ssoNamespace, key); err != nil {
		return nil, fmt.Errorf("sso deleteAccountAssignment delete: %w", err)
	}
	compositeKey := input.TargetID + "/" + input.PrincipalType + "/" + input.PrincipalID
	removeFromStringIndex(goCtx, p.state, ssoNamespace, ssoAssignmentKeysKey(reqCtx.AccountID, input.PermissionSetArn), compositeKey)

	requestID := generateSSORequestID()
	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"AccountAssignmentDeletionStatus": map[string]interface{}{
			"Status":    "SUCCEEDED",
			"RequestId": requestID,
		},
	})
}

func (p *SSOPlugin) listAccountAssignments(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		PermissionSetArn string `json:"PermissionSetArn"`
		AccountID        string `json:"AccountId"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	goCtx := context.Background()
	compositeKeys, err := loadStringIndex(goCtx, p.state, ssoNamespace, ssoAssignmentKeysKey(reqCtx.AccountID, input.PermissionSetArn))
	if err != nil {
		return nil, fmt.Errorf("sso listAccountAssignments load index: %w", err)
	}

	assignments := make([]map[string]interface{}, 0)
	for _, ck := range compositeKeys {
		// composite key format: targetId/principalType/principalId
		parts := splitN(ck, "/", 3)
		if len(parts) != 3 {
			continue
		}
		targetID, principalType, principalID := parts[0], parts[1], parts[2]
		// Filter by AccountId if provided.
		if input.AccountID != "" && targetID != input.AccountID {
			continue
		}
		assignments = append(assignments, map[string]interface{}{
			"AccountId":        targetID,
			"PermissionSetArn": input.PermissionSetArn,
			"PrincipalType":    principalType,
			"PrincipalId":      principalID,
		})
	}

	return ssoJSONResponse(http.StatusOK, map[string]interface{}{
		"AccountAssignments": assignments,
		"NextToken":          "",
	})
}

// loadPermissionSet loads an SSOPermissionSet from state or returns a not-found error.
func (p *SSOPlugin) loadPermissionSet(acct, permSetArn string) (*SSOPermissionSet, error) {
	if permSetArn == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "PermissionSetArn is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, ssoNamespace, ssoPermSetKey(acct, permSetArn))
	if err != nil {
		return nil, fmt.Errorf("sso loadPermissionSet get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "PermissionSet " + permSetArn + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var ps SSOPermissionSet
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, fmt.Errorf("sso loadPermissionSet unmarshal: %w", err)
	}
	return &ps, nil
}

// splitN splits s by sep at most n times.
func splitN(s, sep string, n int) []string {
	result := make([]string, 0, n)
	for i := 0; i < n-1; i++ {
		idx := indexString(s, sep)
		if idx < 0 {
			break
		}
		result = append(result, s[:idx])
		s = s[idx+len(sep):]
	}
	result = append(result, s)
	return result
}

// indexString returns the index of the first occurrence of substr in s, or -1.
func indexString(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// ssoJSONResponse serializes v to JSON and returns an AWSResponse with Content-Type application/json.
func ssoJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sso json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
