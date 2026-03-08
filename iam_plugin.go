package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// iamNamespace is the state namespace used by IAMPlugin.
const iamNamespace = "iam"

// IAMPlugin emulates the AWS Identity and Access Management (IAM) API.
// It implements the [Plugin] interface and handles JSON-protocol IAM requests
// routed via the X-Amz-Target: AmazonIdentityManagementService.<Operation> header
// or via the iam.amazonaws.com host.
type IAMPlugin struct {
	state  StateManager
	logger Logger
}

// Name returns the service name "iam".
func (p *IAMPlugin) Name() string { return "iam" }

// Initialize stores the provided state manager and logger.
func (p *IAMPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	return nil
}

// Shutdown is a no-op for IAMPlugin.
func (p *IAMPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches the IAM API operation to the appropriate handler.
func (p *IAMPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateUser":
		return p.createUser(ctx, req)
	case "GetUser":
		return p.getUser(ctx, req)
	case "DeleteUser":
		return p.deleteUser(ctx, req)
	case "ListUsers":
		return p.listUsers(ctx, req)

	case "CreateRole":
		return p.createRole(ctx, req)
	case "GetRole":
		return p.getRole(ctx, req)
	case "DeleteRole":
		return p.deleteRole(ctx, req)
	case "ListRoles":
		return p.listRoles(ctx, req)

	case "CreateGroup":
		return p.createGroup(ctx, req)
	case "GetGroup":
		return p.getGroup(ctx, req)
	case "DeleteGroup":
		return p.deleteGroup(ctx, req)
	case "ListGroups":
		return p.listGroups(ctx, req)

	case "AttachUserPolicy":
		return p.attachUserPolicy(ctx, req)
	case "DetachUserPolicy":
		return p.detachUserPolicy(ctx, req)
	case "ListAttachedUserPolicies":
		return p.listAttachedUserPolicies(ctx, req)

	case "AttachRolePolicy":
		return p.attachRolePolicy(ctx, req)
	case "DetachRolePolicy":
		return p.detachRolePolicy(ctx, req)
	case "ListAttachedRolePolicies":
		return p.listAttachedRolePolicies(ctx, req)

	case "CreatePolicy":
		return p.createPolicy(ctx, req)
	case "GetPolicy":
		return p.getPolicy(ctx, req)
	case "DeletePolicy":
		return p.deletePolicy(ctx, req)
	case "ListPolicies":
		return p.listPolicies(ctx, req)

	case "CreateAccessKey":
		return p.createAccessKey(ctx, req)
	case "DeleteAccessKey":
		return p.deleteAccessKey(ctx, req)
	case "ListAccessKeys":
		return p.listAccessKeys(ctx, req)

	default:
		return iamErrorResponse("InvalidAction",
			fmt.Sprintf("Could not find operation %s", req.Operation),
			http.StatusBadRequest), nil
	}
}

// --- User operations -------------------------------------------------------

func (p *IAMPlugin) createUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string   `json:"UserName"`
		Path     string   `json:"Path"`
		Tags     []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreateUser", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	key := "user:" + params.UserName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExistsException",
			fmt.Sprintf("User with name %s already exists.", params.UserName),
			http.StatusConflict), nil
	}

	if params.Path == "" {
		params.Path = "/"
	}
	user := &IAMUser{
		UserName:   params.UserName,
		UserID:     generateIAMID("AIDA"),
		ARN:        iamUserARN(ctx.AccountID, params.Path, params.UserName),
		Path:       params.Path,
		CreateDate: time.Now().UTC(),
		Tags:       params.Tags,
	}

	raw, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("marshal user: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put user: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"User": user})
}

func (p *IAMPlugin) getUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:GetUser", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	userName := params.UserName
	if userName == "" {
		// Caller identity.
		if ctx.Principal != nil {
			_, userName = parsePrincipalARN(ctx.Principal.ARN)
		}
	}
	if userName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	user, err := p.loadUser(goCtx, userName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The user with name %s cannot be found.", userName),
			http.StatusNotFound), nil
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"User": user})
}

func (p *IAMPlugin) deleteUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DeleteUser", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	// Check for attached policies.
	arns, err := p.loadPolicyList(goCtx, "user_policies:"+params.UserName)
	if err != nil {
		return nil, err
	}
	if len(arns) > 0 {
		return iamErrorResponse("DeleteConflictException",
			"Cannot delete entity, must detach all policies first.",
			http.StatusConflict), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "user:"+params.UserName); err != nil {
		return nil, fmt.Errorf("delete user: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listUsers(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListUsers", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "user:")
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems)

	users := make([]*IAMUser, 0, len(page))
	for _, k := range page {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var u IAMUser
		if err := json.Unmarshal(raw, &u); err != nil {
			continue
		}
		if params.PathPrefix != "" && !strings.HasPrefix(u.Path, params.PathPrefix) {
			continue
		}
		users = append(users, &u)
	}

	result := map[string]any{
		"Users":       users,
		"IsTruncated": isTruncated,
	}
	if nextMarker != "" {
		result["Marker"] = nextMarker
	}
	return iamJSONResponse(http.StatusOK, result)
}

// --- Role operations -------------------------------------------------------

func (p *IAMPlugin) createRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName                 string   `json:"RoleName"`
		AssumeRolePolicyDocument string   `json:"AssumeRolePolicyDocument"`
		Path                     string   `json:"Path"`
		Description              string   `json:"Description"`
		MaxSessionDuration       int      `json:"MaxSessionDuration"`
		Tags                     []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreateRole", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	key := "role:" + params.RoleName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get role: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExistsException",
			fmt.Sprintf("Role with name %s already exists.", params.RoleName),
			http.StatusConflict), nil
	}

	var trustPolicy PolicyDocument
	if params.AssumeRolePolicyDocument != "" {
		if err := json.Unmarshal([]byte(params.AssumeRolePolicyDocument), &trustPolicy); err != nil {
			return iamErrorResponse("MalformedPolicyDocumentException", //nolint:nilerr
				"AssumeRolePolicyDocument is not valid JSON.", http.StatusBadRequest), nil
		}
	}

	if params.Path == "" {
		params.Path = "/"
	}
	if params.MaxSessionDuration == 0 {
		params.MaxSessionDuration = 3600
	}
	role := &IAMRole{
		RoleName:                 params.RoleName,
		RoleID:                   generateIAMID("AROA"),
		ARN:                      iamRoleARN(ctx.AccountID, params.Path, params.RoleName),
		Path:                     params.Path,
		Description:              params.Description,
		MaxSessionDuration:       params.MaxSessionDuration,
		CreateDate:               time.Now().UTC(),
		AssumeRolePolicyDocument: trustPolicy,
		Tags:                     params.Tags,
	}

	raw, err := json.Marshal(role)
	if err != nil {
		return nil, fmt.Errorf("marshal role: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put role: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"Role": role})
}

func (p *IAMPlugin) getRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string `json:"RoleName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:GetRole", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"Role": role})
}

func (p *IAMPlugin) deleteRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string `json:"RoleName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DeleteRole", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	arns, err := p.loadPolicyList(goCtx, "role_policies:"+params.RoleName)
	if err != nil {
		return nil, err
	}
	if len(arns) > 0 {
		return iamErrorResponse("DeleteConflictException",
			"Cannot delete entity, must detach all policies first.",
			http.StatusConflict), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "role:"+params.RoleName); err != nil {
		return nil, fmt.Errorf("delete role: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listRoles(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListRoles", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "role:")
	if err != nil {
		return nil, fmt.Errorf("list roles: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems)

	roles := make([]*IAMRole, 0, len(page))
	for _, k := range page {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var r IAMRole
		if err := json.Unmarshal(raw, &r); err != nil {
			continue
		}
		if params.PathPrefix != "" && !strings.HasPrefix(r.Path, params.PathPrefix) {
			continue
		}
		roles = append(roles, &r)
	}

	result := map[string]any{
		"Roles":       roles,
		"IsTruncated": isTruncated,
	}
	if nextMarker != "" {
		result["Marker"] = nextMarker
	}
	return iamJSONResponse(http.StatusOK, result)
}

// --- Group operations ------------------------------------------------------

func (p *IAMPlugin) createGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		Path      string `json:"Path"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" {
		return iamErrorResponse("ValidationError", "GroupName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreateGroup", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	key := "group:" + params.GroupName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExistsException",
			fmt.Sprintf("Group with name %s already exists.", params.GroupName),
			http.StatusConflict), nil
	}

	if params.Path == "" {
		params.Path = "/"
	}
	group := &IAMGroup{
		GroupName:  params.GroupName,
		GroupID:    generateIAMID("AGPA"),
		ARN:        iamGroupARN(ctx.AccountID, params.Path, params.GroupName),
		Path:       params.Path,
		CreateDate: time.Now().UTC(),
	}

	raw, err := json.Marshal(group)
	if err != nil {
		return nil, fmt.Errorf("marshal group: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put group: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"Group": group})
}

func (p *IAMPlugin) getGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		Marker    string `json:"Marker"`
		MaxItems  int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" {
		return iamErrorResponse("ValidationError", "GroupName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:GetGroup", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "group:"+params.GroupName)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The group with name %s cannot be found.", params.GroupName),
			http.StatusNotFound), nil
	}
	var group IAMGroup
	if err := json.Unmarshal(raw, &group); err != nil {
		return nil, fmt.Errorf("unmarshal group: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{
		"Group":       group,
		"Users":       []*IAMUser{},
		"IsTruncated": false,
	})
}

func (p *IAMPlugin) deleteGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" {
		return iamErrorResponse("ValidationError", "GroupName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DeleteGroup", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	existing, err := p.state.Get(goCtx, iamNamespace, "group:"+params.GroupName)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if existing == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The group with name %s cannot be found.", params.GroupName),
			http.StatusNotFound), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "group:"+params.GroupName); err != nil {
		return nil, fmt.Errorf("delete group: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listGroups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListGroups", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "group:")
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems)

	groups := make([]*IAMGroup, 0, len(page))
	for _, k := range page {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var g IAMGroup
		if err := json.Unmarshal(raw, &g); err != nil {
			continue
		}
		groups = append(groups, &g)
	}

	result := map[string]any{
		"Groups":      groups,
		"IsTruncated": isTruncated,
	}
	if nextMarker != "" {
		result["Marker"] = nextMarker
	}
	return iamJSONResponse(http.StatusOK, result)
}

// --- Policy attachment (user) ----------------------------------------------

func (p *IAMPlugin) attachUserPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName  string `json:"UserName"`
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" || params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "UserName and PolicyArn are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:AttachUserPolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	listKey := "user_policies:" + params.UserName
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}
	for _, a := range arns {
		if a == params.PolicyArn {
			return iamJSONResponse(http.StatusOK, map[string]any{})
		}
	}
	arns = append(arns, params.PolicyArn)
	if err := p.savePolicyList(goCtx, listKey, arns); err != nil {
		return nil, err
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) detachUserPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName  string `json:"UserName"`
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" || params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "UserName and PolicyArn are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DetachUserPolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	listKey := "user_policies:" + params.UserName
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}

	newARNs := arns[:0]
	found := false
	for _, a := range arns {
		if a == params.PolicyArn {
			found = true
			continue
		}
		newARNs = append(newARNs, a)
	}
	if !found {
		return iamErrorResponse("NoSuchEntityException",
			"The policy is not attached to the specified entity.",
			http.StatusNotFound), nil
	}
	if err := p.savePolicyList(goCtx, listKey, newARNs); err != nil {
		return nil, err
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listAttachedUserPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		Marker   string `json:"Marker"`
		MaxItems int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListAttachedUserPolicies", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	arns, err := p.loadPolicyList(goCtx, "user_policies:"+params.UserName)
	if err != nil {
		return nil, err
	}

	policies := make([]IAMAttachedPolicy, 0, len(arns))
	for _, arn := range arns {
		name := arnPolicyName(arn)
		policies = append(policies, IAMAttachedPolicy{PolicyName: name, PolicyARN: arn})
	}

	return iamJSONResponse(http.StatusOK, map[string]any{
		"AttachedPolicies": policies,
		"IsTruncated":      false,
	})
}

// --- Policy attachment (role) ----------------------------------------------

func (p *IAMPlugin) attachRolePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName  string `json:"RoleName"`
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" || params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "RoleName and PolicyArn are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:AttachRolePolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	listKey := "role_policies:" + params.RoleName
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}
	for _, a := range arns {
		if a == params.PolicyArn {
			return iamJSONResponse(http.StatusOK, map[string]any{})
		}
	}
	arns = append(arns, params.PolicyArn)
	if err := p.savePolicyList(goCtx, listKey, arns); err != nil {
		return nil, err
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) detachRolePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName  string `json:"RoleName"`
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" || params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "RoleName and PolicyArn are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DetachRolePolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	listKey := "role_policies:" + params.RoleName
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}

	newARNs := arns[:0]
	found := false
	for _, a := range arns {
		if a == params.PolicyArn {
			found = true
			continue
		}
		newARNs = append(newARNs, a)
	}
	if !found {
		return iamErrorResponse("NoSuchEntityException",
			"The policy is not attached to the specified entity.",
			http.StatusNotFound), nil
	}
	if err := p.savePolicyList(goCtx, listKey, newARNs); err != nil {
		return nil, err
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listAttachedRolePolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string `json:"RoleName"`
		Marker   string `json:"Marker"`
		MaxItems int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListAttachedRolePolicies", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	arns, err := p.loadPolicyList(goCtx, "role_policies:"+params.RoleName)
	if err != nil {
		return nil, err
	}

	policies := make([]IAMAttachedPolicy, 0, len(arns))
	for _, arn := range arns {
		name := arnPolicyName(arn)
		policies = append(policies, IAMAttachedPolicy{PolicyName: name, PolicyARN: arn})
	}

	return iamJSONResponse(http.StatusOK, map[string]any{
		"AttachedPolicies": policies,
		"IsTruncated":      false,
	})
}

// --- Policy CRUD -----------------------------------------------------------

func (p *IAMPlugin) createPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyName     string `json:"PolicyName"`
		PolicyDocument string `json:"PolicyDocument"`
		Path           string `json:"Path"`
		Description    string `json:"Description"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyName == "" {
		return iamErrorResponse("ValidationError", "PolicyName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreatePolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	if params.Path == "" {
		params.Path = "/"
	}
	arn := iamPolicyARN(ctx.AccountID, params.Path, params.PolicyName)

	existing, err := p.state.Get(goCtx, iamNamespace, "policy:"+arn)
	if err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExistsException",
			fmt.Sprintf("A policy called %s already exists.", params.PolicyName),
			http.StatusConflict), nil
	}

	var doc PolicyDocument
	if params.PolicyDocument != "" {
		if err := json.Unmarshal([]byte(params.PolicyDocument), &doc); err != nil {
			return iamErrorResponse("MalformedPolicyDocumentException", //nolint:nilerr
				"PolicyDocument is not valid JSON.", http.StatusBadRequest), nil
		}
	}

	now := time.Now().UTC()
	policy := &IAMPolicy{
		PolicyName:       params.PolicyName,
		PolicyID:         generateIAMID("ANPA"),
		ARN:              arn,
		Path:             params.Path,
		Description:      params.Description,
		DefaultVersionID: "v1",
		IsAttachable:     true,
		CreateDate:       now,
		UpdateDate:       now,
		Document:         doc,
	}

	raw, err := json.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("marshal policy: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "policy:"+arn, raw); err != nil {
		return nil, fmt.Errorf("put policy: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"Policy": policy})
}

func (p *IAMPlugin) getPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:GetPolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	// Check managed policies first.
	if mp, ok := GetManagedPolicy(params.PolicyArn); ok {
		return iamJSONResponse(http.StatusOK, map[string]any{"Policy": mp})
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "policy:"+params.PolicyArn)
	if err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("Policy %s was not found.", params.PolicyArn),
			http.StatusNotFound), nil
	}
	var policy IAMPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"Policy": &policy})
}

func (p *IAMPlugin) deletePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DeletePolicy", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "policy:"+params.PolicyArn)
	if err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("Policy %s was not found.", params.PolicyArn),
			http.StatusNotFound), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "policy:"+params.PolicyArn); err != nil {
		return nil, fmt.Errorf("delete policy: %w", err)
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		Scope      string `json:"Scope"`
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListPolicies", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "policy:")
	if err != nil {
		return nil, fmt.Errorf("list policies: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems)

	policies := make([]*IAMPolicy, 0, len(page))
	for _, k := range page {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var pol IAMPolicy
		if err := json.Unmarshal(raw, &pol); err != nil {
			continue
		}
		policies = append(policies, &pol)
	}

	result := map[string]any{
		"Policies":    policies,
		"IsTruncated": isTruncated,
	}
	if nextMarker != "" {
		result["Marker"] = nextMarker
	}
	return iamJSONResponse(http.StatusOK, result)
}

// --- Access key operations -------------------------------------------------

func (p *IAMPlugin) createAccessKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreateAccessKey", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	userName := params.UserName
	if userName == "" {
		if ctx.Principal != nil {
			_, userName = parsePrincipalARN(ctx.Principal.ARN)
		}
	}
	if userName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	user, err := p.loadUser(goCtx, userName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The user with name %s cannot be found.", userName),
			http.StatusNotFound), nil
	}

	accessKey := &IAMAccessKey{
		AccessKeyID:     generateIAMID("AKIA"),
		SecretAccessKey: generateIAMID("SECRET") + generateIAMID("KEY"),
		Status:          "Active",
		UserName:        userName,
		CreateDate:      time.Now().UTC(),
	}

	raw, err := json.Marshal(accessKey)
	if err != nil {
		return nil, fmt.Errorf("marshal access key: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "accesskey:"+accessKey.AccessKeyID, raw); err != nil {
		return nil, fmt.Errorf("put access key: %w", err)
	}

	// Update user's key index.
	indexKey := "user_accesskeys:" + userName
	keyIDs, err := p.loadStringList(goCtx, indexKey)
	if err != nil {
		return nil, err
	}
	keyIDs = append(keyIDs, accessKey.AccessKeyID)
	if err := p.saveStringList(goCtx, indexKey, keyIDs); err != nil {
		return nil, err
	}

	return iamJSONResponse(http.StatusOK, map[string]any{"AccessKey": accessKey})
}

func (p *IAMPlugin) deleteAccessKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		AccessKeyID string `json:"AccessKeyId"`
		UserName    string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.AccessKeyID == "" {
		return iamErrorResponse("ValidationError", "AccessKeyId is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:DeleteAccessKey", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "accesskey:"+params.AccessKeyID)
	if err != nil {
		return nil, fmt.Errorf("get access key: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntityException",
			fmt.Sprintf("The Access Key with id %s cannot be found.", params.AccessKeyID),
			http.StatusNotFound), nil
	}
	var key IAMAccessKey
	if err := json.Unmarshal(raw, &key); err != nil {
		return nil, fmt.Errorf("unmarshal access key: %w", err)
	}

	if err := p.state.Delete(goCtx, iamNamespace, "accesskey:"+params.AccessKeyID); err != nil {
		return nil, fmt.Errorf("delete access key: %w", err)
	}

	// Remove from user index.
	indexKey := "user_accesskeys:" + key.UserName
	keyIDs, err := p.loadStringList(goCtx, indexKey)
	if err != nil {
		return nil, err
	}
	newIDs := keyIDs[:0]
	for _, id := range keyIDs {
		if id != params.AccessKeyID {
			newIDs = append(newIDs, id)
		}
	}
	if err := p.saveStringList(goCtx, indexKey, newIDs); err != nil {
		return nil, err
	}

	return iamJSONResponse(http.StatusOK, map[string]any{})
}

func (p *IAMPlugin) listAccessKeys(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		Marker   string `json:"Marker"`
		MaxItems int    `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListAccessKeys", "*"); err != nil {
		return iamErrorResponse("AccessDeniedException", err.Error(), http.StatusForbidden), nil
	}

	userName := params.UserName
	if userName == "" {
		if ctx.Principal != nil {
			_, userName = parsePrincipalARN(ctx.Principal.ARN)
		}
	}
	if userName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	keyIDs, err := p.loadStringList(goCtx, "user_accesskeys:"+userName)
	if err != nil {
		return nil, err
	}

	// Build metadata-only list (no SecretAccessKey).
	keys := make([]map[string]any, 0, len(keyIDs))
	for _, id := range keyIDs {
		raw, err := p.state.Get(goCtx, iamNamespace, "accesskey:"+id)
		if err != nil || raw == nil {
			continue
		}
		var k IAMAccessKey
		if err := json.Unmarshal(raw, &k); err != nil {
			continue
		}
		keys = append(keys, map[string]any{
			"AccessKeyId": k.AccessKeyID,
			"Status":      k.Status,
			"UserName":    k.UserName,
			"CreateDate":  k.CreateDate,
		})
	}

	return iamJSONResponse(http.StatusOK, map[string]any{
		"AccessKeyMetadata": keys,
		"IsTruncated":       false,
	})
}

// --- Authorization ---------------------------------------------------------

// authorize checks whether the caller (reqCtx.Principal) is allowed to perform
// action on resource. A nil Principal always passes (bootstrap/test mode).
func (p *IAMPlugin) authorize(goCtx context.Context, reqCtx *RequestContext, action, resource string) error {
	if reqCtx.Principal == nil {
		return nil
	}

	entityType, entityName := parsePrincipalARN(reqCtx.Principal.ARN)
	if entityName == "" {
		return &AWSError{Code: "AccessDeniedException", Message: "access denied", HTTPStatus: 403}
	}

	var listKey string
	switch entityType {
	case "user":
		listKey = "user_policies:" + entityName
	case "role":
		listKey = "role_policies:" + entityName
	default:
		return &AWSError{Code: "AccessDeniedException", Message: "access denied", HTTPStatus: 403}
	}

	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return fmt.Errorf("load policies for authorization: %w", err)
	}

	// Fast path: AdministratorAccess.
	for _, arn := range arns {
		if arn == "arn:aws:iam::aws:policy/AdministratorAccess" {
			return nil
		}
	}

	var docs []PolicyDocument
	for _, arn := range arns {
		if mp, ok := GetManagedPolicy(arn); ok {
			docs = append(docs, mp.Document)
			continue
		}
		raw, err := p.state.Get(goCtx, iamNamespace, "policy:"+arn)
		if err != nil || raw == nil {
			continue
		}
		var pol IAMPolicy
		if err := json.Unmarshal(raw, &pol); err != nil {
			continue
		}
		docs = append(docs, pol.Document)
	}

	result := Evaluate(docs, EvaluationRequest{
		Principal: reqCtx.Principal.ARN,
		Action:    action,
		Resource:  resource,
		Context:   make(map[string]string),
	})

	if result.Decision != DecisionAllow {
		return &AWSError{
			Code:       "AccessDeniedException",
			Message:    "User: " + reqCtx.Principal.ARN + " is not authorized to perform: " + action,
			HTTPStatus: http.StatusForbidden,
		}
	}
	return nil
}

// --- State helpers ---------------------------------------------------------

func (p *IAMPlugin) loadUser(goCtx context.Context, name string) (*IAMUser, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, "user:"+name)
	if err != nil {
		return nil, fmt.Errorf("load user %s: %w", name, err)
	}
	if raw == nil {
		return nil, nil
	}
	var u IAMUser
	if err := json.Unmarshal(raw, &u); err != nil {
		return nil, fmt.Errorf("unmarshal user %s: %w", name, err)
	}
	return &u, nil
}

func (p *IAMPlugin) loadRole(goCtx context.Context, name string) (*IAMRole, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, "role:"+name)
	if err != nil {
		return nil, fmt.Errorf("load role %s: %w", name, err)
	}
	if raw == nil {
		return nil, nil
	}
	var r IAMRole
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("unmarshal role %s: %w", name, err)
	}
	return &r, nil
}

func (p *IAMPlugin) loadPolicyList(goCtx context.Context, key string) ([]string, error) {
	return p.loadStringList(goCtx, key)
}

func (p *IAMPlugin) savePolicyList(goCtx context.Context, key string, arns []string) error {
	return p.saveStringList(goCtx, key, arns)
}

func (p *IAMPlugin) loadStringList(goCtx context.Context, key string) ([]string, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("load list %s: %w", key, err)
	}
	if raw == nil {
		return []string{}, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("unmarshal list %s: %w", key, err)
	}
	return list, nil
}

func (p *IAMPlugin) saveStringList(goCtx context.Context, key string, list []string) error {
	raw, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("marshal list %s: %w", key, err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return fmt.Errorf("save list %s: %w", key, err)
	}
	return nil
}

// --- Response/error helpers ------------------------------------------------

// iamErrorResponse builds a JSON-encoded IAM error response. IAM uses JSON
// errors (not XML), with a "__type" field identifying the error code.
func iamErrorResponse(code, message string, status int) *AWSResponse {
	body, _ := json.Marshal(map[string]string{
		"__type":  code,
		"message": message,
	})
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}
}

// iamJSONResponse builds a successful JSON response.
func iamJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal IAM response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// parseIAMBody unmarshals an IAM JSON request body into dst.
// An empty body is treated as an empty JSON object.
func parseIAMBody(body []byte, dst any) error {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

// paginateIAMKeys sorts keys alphabetically, applies a Marker cursor, and
// slices to maxItems. Returns the page, next Marker (base64 of last key), and
// whether more results exist.
func paginateIAMKeys(keys []string, marker string, maxItems int) (page []string, nextMarker string, isTruncated bool) {
	sort.Strings(keys)

	startIdx := 0
	if marker != "" {
		decoded, err := base64.StdEncoding.DecodeString(marker)
		if err == nil {
			for i, k := range keys {
				if k == string(decoded) {
					startIdx = i + 1
					break
				}
			}
		}
	}

	if maxItems <= 0 || maxItems > 1000 {
		maxItems = 100
	}

	end := startIdx + maxItems
	if end >= len(keys) {
		end = len(keys)
		isTruncated = false
	} else {
		isTruncated = true
	}

	page = keys[startIdx:end]
	if isTruncated && len(page) > 0 {
		nextMarker = base64.StdEncoding.EncodeToString([]byte(page[len(page)-1]))
	}
	return
}

// parsePrincipalARN extracts the entity type ("user", "role") and name from
// an IAM ARN such as "arn:aws:iam::123456789012:user/alice".
func parsePrincipalARN(arn string) (entityType, name string) {
	// arn:aws:iam::<account>:<type>/<name>
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 {
		return "", ""
	}
	resource := parts[5]
	slash := strings.IndexByte(resource, '/')
	if slash < 0 {
		return resource, ""
	}
	return resource[:slash], resource[slash+1:]
}

// arnPolicyName extracts the policy name from a policy ARN.
func arnPolicyName(arn string) string {
	if idx := strings.LastIndexByte(arn, '/'); idx >= 0 {
		return arn[idx+1:]
	}
	return arn
}
