package emulator

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
	tc     *TimeController
}

// Name returns the service name "iam".
func (p *IAMPlugin) Name() string { return "iam" }

// Initialize stores the provided state manager, logger, and optional TimeController.
func (p *IAMPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"]; ok {
		if typed, ok := tc.(*TimeController); ok {
			p.tc = typed
		}
	}
	return nil
}

// now returns the current time from the TimeController if set, else time.Now().
func (p *IAMPlugin) now() time.Time {
	if p.tc != nil {
		return p.tc.Now()
	}
	return time.Now()
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
	case "UpdateAssumeRolePolicy":
		return p.updateAssumeRolePolicy(ctx, req)
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
	case "AddUserToGroup":
		return p.addUserToGroup(ctx, req)
	case "RemoveUserFromGroup":
		return p.removeUserFromGroup(ctx, req)
	case "ListGroupsForUser":
		return p.listGroupsForUser(ctx, req)

	case "AttachGroupPolicy":
		return p.attachGroupPolicy(ctx, req)
	case "DetachGroupPolicy":
		return p.detachGroupPolicy(ctx, req)
	case "ListAttachedGroupPolicies":
		return p.listAttachedGroupPolicies(ctx, req)

	case "PutGroupPolicy":
		return p.putGroupPolicy(ctx, req)
	case "GetGroupPolicy":
		return p.getGroupPolicy(ctx, req)
	case "DeleteGroupPolicy":
		return p.deleteGroupPolicy(ctx, req)
	case "ListGroupPolicies":
		return p.listGroupPolicies(ctx, req)

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
	case "GetPolicyVersion":
		return p.getPolicyVersion(ctx, req)
	case "ListPolicyVersions":
		return p.listPolicyVersions(ctx, req)

	case "CreateAccessKey":
		return p.createAccessKey(ctx, req)
	case "DeleteAccessKey":
		return p.deleteAccessKey(ctx, req)
	case "ListAccessKeys":
		return p.listAccessKeys(ctx, req)

	case "PutUserPolicy":
		return p.putUserPolicy(ctx, req)
	case "GetUserPolicy":
		return p.getUserPolicy(ctx, req)
	case "DeleteUserPolicy":
		return p.deleteUserPolicy(ctx, req)
	case "ListUserPolicies":
		return p.listUserPolicies(ctx, req)

	case "PutRolePolicy":
		return p.putRolePolicy(ctx, req)
	case "GetRolePolicy":
		return p.getRolePolicy(ctx, req)
	case "DeleteRolePolicy":
		return p.deleteRolePolicy(ctx, req)
	case "ListRolePolicies":
		return p.listRolePolicies(ctx, req)

	case "PutUserPermissionsBoundary":
		return p.putUserPermissionsBoundary(ctx, req)
	case "DeleteUserPermissionsBoundary":
		return p.deleteUserPermissionsBoundary(ctx, req)
	case "PutRolePermissionsBoundary":
		return p.putRolePermissionsBoundary(ctx, req)
	case "DeleteRolePermissionsBoundary":
		return p.deleteRolePermissionsBoundary(ctx, req)

	case "TagUser":
		return p.tagUser(ctx, req)
	case "UntagUser":
		return p.untagUser(ctx, req)
	case "ListUserTags":
		return p.listUserTags(ctx, req)
	case "TagRole":
		return p.tagRole(ctx, req)
	case "UntagRole":
		return p.untagRole(ctx, req)
	case "ListRoleTags":
		return p.listRoleTags(ctx, req)

	case "CreateInstanceProfile":
		return p.createInstanceProfile(ctx, req)
	case "GetInstanceProfile":
		return p.getInstanceProfile(ctx, req)
	case "DeleteInstanceProfile":
		return p.deleteInstanceProfile(ctx, req)
	case "AddRoleToInstanceProfile":
		return p.addRoleToInstanceProfile(ctx, req)
	case "RemoveRoleFromInstanceProfile":
		return p.removeRoleFromInstanceProfile(ctx, req)
	case "ListInstanceProfiles":
		return p.listInstanceProfiles(ctx, req)

	case "SimulatePrincipalPolicy":
		return p.simulatePrincipalPolicy(ctx, req)
	case "SimulateCustomPolicy":
		return p.simulateCustomPolicy(ctx, req)

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
	// Tags is a query-protocol list; see tagUser and iam_query.go (#639). A user
	// created with tags kept none of them.
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreateUser", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	key := "user:" + params.UserName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExists",
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
		CreateDate: p.now().UTC(),
		Tags:       params.Tags,
	}

	raw, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("marshal user: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put user: %w", err)
	}

	return iamXMLResponse(http.StatusOK, "CreateUser", iamSingleUserXML(user))
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", userName),
			http.StatusNotFound), nil
	}

	return iamXMLResponse(http.StatusOK, "GetUser", iamSingleUserXML(user))
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	// Check for attached policies.
	arns, err := p.loadPolicyList(goCtx, "user_policies:"+params.UserName)
	if err != nil {
		return nil, err
	}
	if len(arns) > 0 {
		return iamErrorResponse("DeleteConflict",
			"Cannot delete entity, must detach all policies first.",
			http.StatusConflict), nil
	}

	// "Before attempting to delete a user, remove the following items: … Group
	// memberships (RemoveUserFromGroup)" — the DeleteUser reference. Deleting a
	// member would otherwise leave the group's side of the index naming a user that
	// no longer exists, and GetGroup would silently skip it.
	groups, err := p.loadStringList(goCtx, iamUserGroupsKey(params.UserName))
	if err != nil {
		return nil, err
	}
	if len(groups) > 0 {
		// Name them, for the same reason DeleteRole names the instance profiles: a
		// caller that wedges here needs to know what to detach from.
		return iamErrorResponse("DeleteConflict",
			fmt.Sprintf("Cannot delete entity, must remove user from all groups first. "+
				"Groups containing %s: %s", params.UserName, strings.Join(groups, ", ")),
			http.StatusConflict), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "user:"+params.UserName); err != nil {
		return nil, fmt.Errorf("delete user: %w", err)
	}

	return iamXMLEmptyResponse("DeleteUser"), nil
}

func (p *IAMPlugin) listUsers(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListUsers", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "user:")
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems.Int())

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

	xml := iamUserListXML(users) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xml += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListUsers", xml)
}

// --- Role operations -------------------------------------------------------

func (p *IAMPlugin) createRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName                 string   `json:"RoleName"`
		AssumeRolePolicyDocument string   `json:"AssumeRolePolicyDocument"`
		Path                     string   `json:"Path"`
		Description              string   `json:"Description"`
		MaxSessionDuration       iamInt   `json:"MaxSessionDuration"`
		Tags                     []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}
	// Tags is a query-protocol list; see tagUser and iam_query.go (#639). A role
	// created with tags kept none of them.
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:CreateRole", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	key := "role:" + params.RoleName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get role: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExists",
			fmt.Sprintf("Role with name %s already exists.", params.RoleName),
			http.StatusConflict), nil
	}

	var trustPolicy PolicyDocument
	if params.AssumeRolePolicyDocument != "" {
		if err := json.Unmarshal([]byte(params.AssumeRolePolicyDocument), &trustPolicy); err != nil {
			return iamErrorResponse("MalformedPolicyDocument", //nolint:nilerr
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
		MaxSessionDuration:       params.MaxSessionDuration.Int(),
		CreateDate:               p.now().UTC(),
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

	return iamXMLResponse(http.StatusOK, "CreateRole", iamSingleRoleXML(role))
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	return iamXMLResponse(http.StatusOK, "GetRole", iamSingleRoleXML(role))
}

// updateAssumeRolePolicy replaces a role's trust policy in place.
//
// The document is stored the same way createRole stores it — unmarshalled into a
// [PolicyDocument] — so the trust-policy gate STS applies (#593) and GetRole's
// read-back both see the new document with no further plumbing. That gate reads
// the stored policy at AssumeRole time, which is what makes the replacement take
// effect on the *next* assume and leave already-minted sessions alone: AWS does
// not revoke them either.
//
// PolicyDocument is required here, unlike CreateRole's AssumeRolePolicyDocument —
// the UpdateAssumeRolePolicy model lists both RoleName and PolicyDocument under
// "required", so there is no "clear the policy" form of this call. An absent one
// is a validation error rather than an unenforced role.
func (p *IAMPlugin) updateAssumeRolePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName       string `json:"RoleName"`
		PolicyDocument string `json:"PolicyDocument"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" || params.PolicyDocument == "" {
		return iamErrorResponse("ValidationError",
			"RoleName and PolicyDocument are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:UpdateAssumeRolePolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	var trustPolicy PolicyDocument
	if err := json.Unmarshal([]byte(params.PolicyDocument), &trustPolicy); err != nil {
		return iamErrorResponse("MalformedPolicyDocument", //nolint:nilerr
			"PolicyDocument is not valid JSON.", http.StatusBadRequest), nil
	}
	role.AssumeRolePolicyDocument = trustPolicy

	raw, err := json.Marshal(role)
	if err != nil {
		return nil, fmt.Errorf("marshal role: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "role:"+params.RoleName, raw); err != nil {
		return nil, fmt.Errorf("put role: %w", err)
	}

	return iamXMLEmptyResponse("UpdateAssumeRolePolicy"), nil
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	arns, err := p.loadPolicyList(goCtx, "role_policies:"+params.RoleName)
	if err != nil {
		return nil, err
	}
	if len(arns) > 0 {
		return iamErrorResponse("DeleteConflict",
			"Cannot delete entity, must detach all policies first.",
			http.StatusConflict), nil
	}

	// An instance profile holding the role is the other subordinate entity the
	// DeleteRole reference requires be removed first — "Before attempting to delete a
	// role, remove the following attached items: … Instance profile
	// (RemoveRoleFromInstanceProfile)". Without this the delete succeeded and left the
	// profile listing a role that no longer exists, and DeleteInstanceProfile then
	// refused with DeleteConflict — so the failure surfaced on the resource that was
	// still present rather than on the one that broke the invariant (#581).
	holders, err := p.instanceProfilesHoldingRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if len(holders) > 0 {
		// The reference says "The error message describes these entities", so name
		// them: a caller has to know which profiles to detach from, and a stack that
		// wedges here needs the reason to say what held it.
		return iamErrorResponse("DeleteConflict",
			fmt.Sprintf("Cannot delete entity, must remove roles from instance profile first. "+
				"Instance profiles holding %s: %s", params.RoleName, strings.Join(holders, ", ")),
			http.StatusConflict), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "role:"+params.RoleName); err != nil {
		return nil, fmt.Errorf("delete role: %w", err)
	}

	return iamXMLEmptyResponse("DeleteRole"), nil
}

func (p *IAMPlugin) listRoles(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListRoles", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "role:")
	if err != nil {
		return nil, fmt.Errorf("list roles: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems.Int())

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

	xmlStr := iamRoleListXML(roles) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListRoles", xmlStr)
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	key := "group:" + params.GroupName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExists",
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
		CreateDate: p.now().UTC(),
	}

	raw, err := json.Marshal(group)
	if err != nil {
		return nil, fmt.Errorf("marshal group: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put group: %w", err)
	}

	return iamXMLResponse(http.StatusOK, "CreateGroup", iamSingleGroupXML(group))
}

func (p *IAMPlugin) getGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		Marker    string `json:"Marker"`
		MaxItems  iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" {
		return iamErrorResponse("ValidationError", "GroupName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:GetGroup", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "group:"+params.GroupName)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The group with name %s cannot be found.", params.GroupName),
			http.StatusNotFound), nil
	}
	var group IAMGroup
	if err := json.Unmarshal(raw, &group); err != nil {
		return nil, fmt.Errorf("unmarshal group: %w", err)
	}

	// GetGroup's whole purpose is the member list — "Returns a list of IAM users
	// that are in the specified IAM group" — and this passed iamUserListXML(nil)
	// unconditionally, so every group was observably empty. Nothing noticed because
	// no operation could add a member until now.
	memberNames, err := p.loadStringList(goCtx, iamGroupUsersKey(params.GroupName))
	if err != nil {
		return nil, err
	}
	page, nextMarker, isTruncated := paginateIAMKeys(memberNames, params.Marker, params.MaxItems.Int())

	users := make([]*IAMUser, 0, len(page))
	for _, name := range page {
		user, err := p.loadUser(goCtx, name)
		if err != nil {
			return nil, err
		}
		// A member naming no stored user can only come from a state edit outside the
		// API: DeleteUser refuses while the user is in a group.
		if user == nil {
			continue
		}
		users = append(users, user)
	}

	xmlStr := iamSingleGroupXML(&group) + iamUserListXML(users) +
		"<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "GetGroup", xmlStr)
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	existing, err := p.state.Get(goCtx, iamNamespace, "group:"+params.GroupName)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if existing == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The group with name %s cannot be found.", params.GroupName),
			http.StatusNotFound), nil
	}

	// "The group must not contain any users or have any attached policies" — the
	// DeleteGroup reference. Both refusals are DeleteConflict, matching DeleteUser
	// and DeleteRole. Without them a delete would leave a user_groups entry naming a
	// group that no longer exists, and that membership would still be read by
	// loadPoliciesForPrincipal.
	members, err := p.loadStringList(goCtx, iamGroupUsersKey(params.GroupName))
	if err != nil {
		return nil, err
	}
	if len(members) > 0 {
		return iamErrorResponse("DeleteConflict",
			fmt.Sprintf("Cannot delete entity, must remove users from group first. "+
				"Users in %s: %s", params.GroupName, strings.Join(members, ", ")),
			http.StatusConflict), nil
	}
	arns, err := p.loadPolicyList(goCtx, iamGroupPoliciesKey(params.GroupName))
	if err != nil {
		return nil, err
	}
	if len(arns) > 0 {
		return iamErrorResponse("DeleteConflict",
			"Cannot delete entity, must detach all policies first.",
			http.StatusConflict), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "group:"+params.GroupName); err != nil {
		return nil, fmt.Errorf("delete group: %w", err)
	}

	return iamXMLEmptyResponse("DeleteGroup"), nil
}

func (p *IAMPlugin) listGroups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListGroups", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, "group:")
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	page, nextMarker, isTruncated := paginateIAMKeys(keys, params.Marker, params.MaxItems.Int())

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

	xmlStr := iamGroupListXML(groups) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListGroups", xmlStr)
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
	// Shape, not existence — see iam_policy_arn.go (#499).
	if message, ok := iamValidatePolicyARN(params.PolicyArn); !ok {
		return iamErrorResponse("InvalidInput", message, http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:AttachUserPolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	// Existence is not required, only reported — see iam_policy_arn.go (#499). The check runs
	// after the user lookup so an attach that is going to fail anyway does not also warn.
	p.iamWarnUnresolvedPolicyARN(goCtx, "AttachUserPolicy", params.PolicyArn)

	listKey := "user_policies:" + params.UserName
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}
	for _, a := range arns {
		if a == params.PolicyArn {
			return iamXMLEmptyResponse("AttachUserPolicy"), nil
		}
	}
	arns = append(arns, params.PolicyArn)
	if err := p.savePolicyList(goCtx, listKey, arns); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("AttachUserPolicy"), nil
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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
		return iamErrorResponse("NoSuchEntity",
			"The policy is not attached to the specified entity.",
			http.StatusNotFound), nil
	}
	if err := p.savePolicyList(goCtx, listKey, newARNs); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("DetachUserPolicy"), nil
}

func (p *IAMPlugin) listAttachedUserPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListAttachedUserPolicies", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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

	return iamXMLResponse(http.StatusOK, "ListAttachedUserPolicies", iamAttachedPoliciesXML(policies)+"<IsTruncated>false</IsTruncated>")
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
	// Shape, not existence — see iam_policy_arn.go (#499).
	if message, ok := iamValidatePolicyARN(params.PolicyArn); !ok {
		return iamErrorResponse("InvalidInput", message, http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:AttachRolePolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	// Existence is not required, only reported — see iam_policy_arn.go (#499). The check runs
	// after the role lookup so an attach that is going to fail anyway does not also warn.
	p.iamWarnUnresolvedPolicyARN(goCtx, "AttachRolePolicy", params.PolicyArn)

	listKey := "role_policies:" + params.RoleName
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}
	for _, a := range arns {
		if a == params.PolicyArn {
			return iamXMLEmptyResponse("AttachRolePolicy"), nil
		}
	}
	arns = append(arns, params.PolicyArn)
	if err := p.savePolicyList(goCtx, listKey, arns); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("AttachRolePolicy"), nil
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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
		return iamErrorResponse("NoSuchEntity",
			"The policy is not attached to the specified entity.",
			http.StatusNotFound), nil
	}
	if err := p.savePolicyList(goCtx, listKey, newARNs); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("DetachRolePolicy"), nil
}

func (p *IAMPlugin) listAttachedRolePolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string `json:"RoleName"`
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListAttachedRolePolicies", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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

	return iamXMLResponse(http.StatusOK, "ListAttachedRolePolicies", iamAttachedPoliciesXML(policies)+"<IsTruncated>false</IsTruncated>")
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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
		return iamErrorResponse("EntityAlreadyExists",
			fmt.Sprintf("A policy called %s already exists.", params.PolicyName),
			http.StatusConflict), nil
	}

	var doc PolicyDocument
	if params.PolicyDocument != "" {
		if err := json.Unmarshal([]byte(params.PolicyDocument), &doc); err != nil {
			return iamErrorResponse("MalformedPolicyDocument", //nolint:nilerr
				"PolicyDocument is not valid JSON.", http.StatusBadRequest), nil
		}
	}

	now := p.now().UTC()
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

	return iamXMLResponse(http.StatusOK, "CreatePolicy", iamSinglePolicyXML(policy))
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	// Check managed policies first.
	if mp, ok := GetManagedPolicy(params.PolicyArn); ok {
		return iamXMLResponse(http.StatusOK, "GetPolicy", iamSinglePolicyXML(mp))
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "policy:"+params.PolicyArn)
	if err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Policy %s was not found.", params.PolicyArn),
			http.StatusNotFound), nil
	}
	var policy IAMPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}

	return iamXMLResponse(http.StatusOK, "GetPolicy", iamSinglePolicyXML(&policy))
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "policy:"+params.PolicyArn)
	if err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Policy %s was not found.", params.PolicyArn),
			http.StatusNotFound), nil
	}

	if err := p.state.Delete(goCtx, iamNamespace, "policy:"+params.PolicyArn); err != nil {
		return nil, fmt.Errorf("delete policy: %w", err)
	}

	return iamXMLEmptyResponse("DeletePolicy"), nil
}

// listPolicies lives in iam_list_policies.go, with the filters it applies.

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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", userName),
			http.StatusNotFound), nil
	}

	accessKey := &IAMAccessKey{
		AccessKeyID:     generateIAMID("AKIA"),
		SecretAccessKey: generateIAMID("SECRET") + generateIAMID("KEY"),
		Status:          "Active",
		UserName:        userName,
		CreateDate:      p.now().UTC(),
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

	return iamXMLResponse(http.StatusOK, "CreateAccessKey", iamAccessKeyXML(accessKey, true))
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
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, "accesskey:"+params.AccessKeyID)
	if err != nil {
		return nil, fmt.Errorf("get access key: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntity",
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

	return iamXMLEmptyResponse("DeleteAccessKey"), nil
}

func (p *IAMPlugin) listAccessKeys(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListAccessKeys", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
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

	return iamXMLResponse(http.StatusOK, "ListAccessKeys", iamAccessKeyMetaListXML(keys)+"<IsTruncated>false</IsTruncated>")
}

// --- Authorization ---------------------------------------------------------

// iamAccessDeniedCode is the code every IAM denial reports.
//
// IAM speaks the query protocol, so this is the bare form — the same value
// [accessDeniedCodeFor] derives for the service, and the plugin's denials must
// agree with the generic gate's or substrate would report two codes for one
// outcome on the same service (#595). Written as a constant rather than a call
// because the answer is fixed for this plugin: its service is always "iam".
const iamAccessDeniedCode = "AccessDenied"

// authorize checks whether the caller (reqCtx.Principal) is allowed to perform
// action on resource. A nil Principal always passes (bootstrap/test mode).
func (p *IAMPlugin) authorize(goCtx context.Context, reqCtx *RequestContext, action, resource string) error {
	if reqCtx.Principal == nil {
		return nil
	}

	// The same resolver AuthController.CheckAccess uses, so one ARN gets one
	// answer. These two disagreed: an entity type outside user/role was denied
	// here and allowed there, and an assumed-role session — the shape STS mints —
	// hit that arm on both paths. An IAM call by such a session now evaluates
	// against the role it assumed instead of being refused outright.
	entity, exists, err := resolveIAMEntity(goCtx, p.state, reqCtx.Principal.ARN)
	if err != nil {
		return fmt.Errorf("resolve principal for authorization: %w", err)
	}
	if !exists {
		// Not enforced, matching CheckAccess: enforcement is opt-in by creating the
		// principal. Denying here instead would refuse every IAM call made with a
		// credential that has no IAM entity behind it — which is how substrate's own
		// documented credentials bootstrap the users these policies are attached to.
		return nil
	}
	entityType, entityName := entity.Kind, entity.Name

	// A user's groups carry policies too, and this gate loads its own documents
	// rather than calling AuthController — so the group arm has to exist on both
	// paths or an IAM call granted through a group would be refused here and allowed
	// there. That is the #411 split exactly: one ARN, two loaders, two answers.
	sources := []iamEntity{entity}
	if entityType == "user" {
		groups, err := p.loadStringList(goCtx, iamUserGroupsKey(entityName))
		if err != nil {
			return fmt.Errorf("load group memberships for authorization: %w", err)
		}
		for _, name := range groups {
			sources = append(sources, iamEntity{Kind: "group", Name: name})
		}
	}

	// Check for AdministratorAccess: substitute a synthetic allow-all document
	// so that permission boundary evaluation still runs below.
	arnsBySource := make([][]string, 0, len(sources))
	hasAdminAccess := false
	for _, source := range sources {
		arns, err := p.loadPolicyList(goCtx, source.Kind+"_policies:"+source.Name)
		if err != nil {
			return fmt.Errorf("load policies for authorization: %w", err)
		}
		arnsBySource = append(arnsBySource, arns)
		for _, arn := range arns {
			if arn == authzAdministratorAccessARN {
				hasAdminAccess = true
				break
			}
		}
	}

	var docs []PolicyDocument
	if hasAdminAccess {
		docs = []PolicyDocument{{
			Version: "2012-10-17",
			Statement: []PolicyStatement{{
				Effect:   IAMEffectAllow,
				Action:   StringOrSlice{"*"},
				Resource: StringOrSlice{"*"},
			}},
		}}
	}
	for i, source := range sources {
		for _, arn := range arnsBySource[i] {
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

		// Also load inline policies for the entity.
		inlineNames, _ := p.loadInlinePolicyNames(goCtx, source.Kind, source.Name)
		for _, name := range inlineNames {
			doc, _ := p.loadInlinePolicyDoc(goCtx, source.Kind, source.Name, name)
			if doc != nil {
				docs = append(docs, *doc)
			}
		}
	}

	result := Evaluate(docs, EvaluationRequest{
		Principal: reqCtx.Principal.ARN,
		Action:    action,
		Resource:  resource,
		Context:   make(map[string]string),
	})

	if result.Decision != DecisionAllow {
		return &AWSError{
			Code:       iamAccessDeniedCode,
			Message:    "User: " + reqCtx.Principal.ARN + " is not authorized to perform: " + action,
			HTTPStatus: http.StatusForbidden,
		}
	}

	// If a permission boundary is set it must also allow the action. The resolver
	// above guarantees Kind is "user" or "role" and that the record exists, so this
	// is one read rather than a switch that had to repeat the same entity-type
	// decision a third time.
	entityRaw, _ := p.state.Get(goCtx, iamNamespace, entityType+":"+entityName)
	if entityRaw != nil {
		var stored struct {
			PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary,omitempty"`
		}
		if unmarshalErr := json.Unmarshal(entityRaw, &stored); unmarshalErr == nil && stored.PermissionsBoundary != nil {
			boundaryDocs := p.loadBoundaryPolicyDocs(goCtx, stored.PermissionsBoundary.PolicyARN)
			if len(boundaryDocs) > 0 {
				boundaryResult := Evaluate(boundaryDocs, EvaluationRequest{
					Principal: reqCtx.Principal.ARN,
					Action:    action,
					Resource:  resource,
					Context:   make(map[string]string),
				})
				if boundaryResult.Decision != DecisionAllow {
					return &AWSError{
						Code:       iamAccessDeniedCode,
						Message:    "User: " + reqCtx.Principal.ARN + " is not authorized to perform: " + action + " (blocked by permission boundary)",
						HTTPStatus: http.StatusForbidden,
					}
				}
			}
		}
	}

	return nil
}

// --- Inline policies -------------------------------------------------------

func (p *IAMPlugin) putUserPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.putInlinePolicy(ctx, req, "user")
}

func (p *IAMPlugin) getUserPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.getInlinePolicy(ctx, req, "user")
}

func (p *IAMPlugin) deleteUserPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.deleteInlinePolicy(ctx, req, "user")
}

func (p *IAMPlugin) listUserPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.listInlinePolicies(ctx, req, "user")
}

func (p *IAMPlugin) putRolePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.putInlinePolicy(ctx, req, "role")
}

func (p *IAMPlugin) getRolePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.getInlinePolicy(ctx, req, "role")
}

func (p *IAMPlugin) deleteRolePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.deleteInlinePolicy(ctx, req, "role")
}

func (p *IAMPlugin) listRolePolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.listInlinePolicies(ctx, req, "role")
}

// iamInlineEntity selects which of an inline-policy request's mutually exclusive
// name parameters applies, and returns the operation-name suffix for the entity
// type: Put*Policy, Get*Policy, Delete*Policy and List*Policies all differ only in
// that word, as does the entity-name element in a Get response.
//
// The four inline handlers previously each carried a two-arm branch defaulting to
// role, so an unrecognized entity type read a role's state and reported a role's
// operation name. One helper means a type is either known here or produces an
// empty name, which every caller already refuses with ValidationError.
func iamInlineEntity(entityType, userName, roleName, groupName string) (entityName, actionSuffix string) {
	switch entityType {
	case "role":
		return roleName, "Role"
	case "group":
		return groupName, "Group"
	case "user":
		return userName, "User"
	default:
		return "", ""
	}
}

// putInlinePolicy stores an inline policy document for a user, role or group.
func (p *IAMPlugin) putInlinePolicy(ctx *RequestContext, req *AWSRequest, entityType string) (*AWSResponse, error) {
	var params struct {
		UserName       string `json:"UserName"`
		RoleName       string `json:"RoleName"`
		GroupName      string `json:"GroupName"`
		PolicyName     string `json:"PolicyName"`
		PolicyDocument string `json:"PolicyDocument"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	entityName, actionSuffix := iamInlineEntity(entityType, params.UserName, params.RoleName, params.GroupName)
	if entityName == "" || params.PolicyName == "" || params.PolicyDocument == "" {
		return iamErrorResponse("ValidationError",
			"EntityName, PolicyName, and PolicyDocument are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:Put"+actionSuffix+"Policy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	// Verify the entity exists. The key prefix is the entity type itself, so a new
	// type needs no arm here — the previous form branched on "user" with a default
	// arm reading "role:", which would have silently looked a group up as a role.
	entityRaw, getErr := p.state.Get(goCtx, iamNamespace, entityType+":"+entityName)
	if getErr != nil {
		return nil, fmt.Errorf("get %s: %w", entityType, getErr)
	}
	if entityRaw == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The %s with name %s cannot be found.", entityType, entityName),
			http.StatusNotFound), nil
	}

	// Parse and validate the policy document.
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(params.PolicyDocument), &doc); err != nil {
		return iamErrorResponse("MalformedPolicyDocument", //nolint:nilerr
			"PolicyDocument is not valid JSON.", http.StatusBadRequest), nil
	}

	docRaw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal inline policy: %w", err)
	}
	stateKey := entityType + "_inline:" + entityName + ":" + params.PolicyName
	if err := p.state.Put(goCtx, iamNamespace, stateKey, docRaw); err != nil {
		return nil, fmt.Errorf("put inline policy: %w", err)
	}

	// Update the names list.
	namesKey := entityType + "_inline_names:" + entityName
	names, err := p.loadStringList(goCtx, namesKey)
	if err != nil {
		return nil, err
	}
	found := false
	for _, n := range names {
		if n == params.PolicyName {
			found = true
			break
		}
	}
	if !found {
		names = append(names, params.PolicyName)
		sort.Strings(names)
		if err := p.saveStringList(goCtx, namesKey, names); err != nil {
			return nil, err
		}
	}

	return iamXMLEmptyResponse("Put" + actionSuffix + "Policy"), nil
}

// getInlinePolicy retrieves an inline policy document for a user, role or group.
func (p *IAMPlugin) getInlinePolicy(ctx *RequestContext, req *AWSRequest, entityType string) (*AWSResponse, error) {
	var params struct {
		UserName   string `json:"UserName"`
		RoleName   string `json:"RoleName"`
		GroupName  string `json:"GroupName"`
		PolicyName string `json:"PolicyName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	entityName, actionSuffix := iamInlineEntity(entityType, params.UserName, params.RoleName, params.GroupName)
	if entityName == "" || params.PolicyName == "" {
		return iamErrorResponse("ValidationError",
			"EntityName and PolicyName are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:Get"+actionSuffix+"Policy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, entityType+"_inline:"+entityName+":"+params.PolicyName)
	if err != nil {
		return nil, fmt.Errorf("get inline policy: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The policy %s was not found.", params.PolicyName),
			http.StatusNotFound), nil
	}

	// The entity-name element is named for the entity type, and the operation name is
	// the same suffix, so both come from one place rather than a branch per type.
	return iamXMLResponse(http.StatusOK, "Get"+actionSuffix+"Policy",
		"<"+actionSuffix+"Name>"+xmlEsc(entityName)+"</"+actionSuffix+"Name>"+
			"<PolicyName>"+xmlEsc(params.PolicyName)+"</PolicyName>"+
			"<PolicyDocument>"+xmlEsc(string(raw))+"</PolicyDocument>")
}

// deleteInlinePolicy removes an inline policy from a user, role or group.
func (p *IAMPlugin) deleteInlinePolicy(ctx *RequestContext, req *AWSRequest, entityType string) (*AWSResponse, error) {
	var params struct {
		UserName   string `json:"UserName"`
		RoleName   string `json:"RoleName"`
		GroupName  string `json:"GroupName"`
		PolicyName string `json:"PolicyName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	entityName, actionSuffix := iamInlineEntity(entityType, params.UserName, params.RoleName, params.GroupName)
	if entityName == "" || params.PolicyName == "" {
		return iamErrorResponse("ValidationError",
			"EntityName and PolicyName are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:Delete"+actionSuffix+"Policy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	stateKey := entityType + "_inline:" + entityName + ":" + params.PolicyName
	existing, err := p.state.Get(goCtx, iamNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("check inline policy: %w", err)
	}
	if existing == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The policy %s was not found.", params.PolicyName),
			http.StatusNotFound), nil
	}
	if err := p.state.Delete(goCtx, iamNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("delete inline policy: %w", err)
	}

	// Remove from names list.
	namesKey := entityType + "_inline_names:" + entityName
	names, err := p.loadStringList(goCtx, namesKey)
	if err != nil {
		return nil, err
	}
	newNames := names[:0]
	for _, n := range names {
		if n != params.PolicyName {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveStringList(goCtx, namesKey, newNames); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("Delete" + actionSuffix + "Policy"), nil
}

// listInlinePolicies returns the names of all inline policies for a user, role or
// group.
func (p *IAMPlugin) listInlinePolicies(ctx *RequestContext, req *AWSRequest, entityType string) (*AWSResponse, error) {
	var params struct {
		UserName  string `json:"UserName"`
		RoleName  string `json:"RoleName"`
		GroupName string `json:"GroupName"`
		Marker    string `json:"Marker"`
		MaxItems  iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	entityName, actionSuffix := iamInlineEntity(entityType, params.UserName, params.RoleName, params.GroupName)
	if entityName == "" {
		return iamErrorResponse("ValidationError", "EntityName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:List"+actionSuffix+"Policies", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	names, err := p.loadStringList(goCtx, entityType+"_inline_names:"+entityName)
	if err != nil {
		return nil, err
	}

	page, nextMarker, isTruncated := paginateIAMKeys(names, params.Marker, params.MaxItems.Int())

	xmlStr := iamStringListXML("PolicyNames", page) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "List"+actionSuffix+"Policies", xmlStr)
}

// --- Permission boundaries -------------------------------------------------

func (p *IAMPlugin) putUserPermissionsBoundary(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.putPermissionsBoundary(ctx, req, "user")
}

func (p *IAMPlugin) deleteUserPermissionsBoundary(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.deletePermissionsBoundary(ctx, req, "user")
}

func (p *IAMPlugin) putRolePermissionsBoundary(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.putPermissionsBoundary(ctx, req, "role")
}

func (p *IAMPlugin) deleteRolePermissionsBoundary(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.deletePermissionsBoundary(ctx, req, "role")
}

// putPermissionsBoundary sets the permissions boundary on a user or role.
func (p *IAMPlugin) putPermissionsBoundary(ctx *RequestContext, req *AWSRequest, entityType string) (*AWSResponse, error) {
	var params struct {
		UserName            string `json:"UserName"`
		RoleName            string `json:"RoleName"`
		PermissionsBoundary string `json:"PermissionsBoundary"` // policy ARN
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	entityName := params.UserName
	actionSuffix := "User"
	if entityType == "role" {
		entityName = params.RoleName
		actionSuffix = "Role"
	}
	if entityName == "" || params.PermissionsBoundary == "" {
		return iamErrorResponse("ValidationError",
			"EntityName and PermissionsBoundary are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:Put"+actionSuffix+"PermissionsBoundary", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	boundary := &IAMAttachedPolicy{
		PolicyARN:  params.PermissionsBoundary,
		PolicyName: arnPolicyName(params.PermissionsBoundary),
	}

	stateKey := entityType + ":" + entityName
	switch entityType {
	case "user":
		user, err := p.loadUser(goCtx, entityName)
		if err != nil {
			return nil, err
		}
		if user == nil {
			return iamErrorResponse("NoSuchEntity",
				fmt.Sprintf("The user with name %s cannot be found.", entityName),
				http.StatusNotFound), nil
		}
		user.PermissionsBoundary = boundary
		raw, err := json.Marshal(user)
		if err != nil {
			return nil, fmt.Errorf("marshal user: %w", err)
		}
		if err := p.state.Put(goCtx, iamNamespace, stateKey, raw); err != nil {
			return nil, fmt.Errorf("put user: %w", err)
		}
	default:
		role, err := p.loadRole(goCtx, entityName)
		if err != nil {
			return nil, err
		}
		if role == nil {
			return iamErrorResponse("NoSuchEntity",
				fmt.Sprintf("The role with name %s cannot be found.", entityName),
				http.StatusNotFound), nil
		}
		role.PermissionsBoundary = boundary
		raw, err := json.Marshal(role)
		if err != nil {
			return nil, fmt.Errorf("marshal role: %w", err)
		}
		if err := p.state.Put(goCtx, iamNamespace, stateKey, raw); err != nil {
			return nil, fmt.Errorf("put role: %w", err)
		}
	}

	return iamXMLEmptyResponse("Put" + actionSuffix + "PermissionsBoundary"), nil
}

// deletePermissionsBoundary clears the permissions boundary from a user or role.
func (p *IAMPlugin) deletePermissionsBoundary(ctx *RequestContext, req *AWSRequest, entityType string) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		RoleName string `json:"RoleName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	entityName := params.UserName
	actionSuffix := "User"
	if entityType == "role" {
		entityName = params.RoleName
		actionSuffix = "Role"
	}
	if entityName == "" {
		return iamErrorResponse("ValidationError", "EntityName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:Delete"+actionSuffix+"PermissionsBoundary", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	stateKey := entityType + ":" + entityName
	switch entityType {
	case "user":
		user, err := p.loadUser(goCtx, entityName)
		if err != nil {
			return nil, err
		}
		if user == nil {
			return iamErrorResponse("NoSuchEntity",
				fmt.Sprintf("The user with name %s cannot be found.", entityName),
				http.StatusNotFound), nil
		}
		user.PermissionsBoundary = nil
		raw, err := json.Marshal(user)
		if err != nil {
			return nil, fmt.Errorf("marshal user: %w", err)
		}
		if err := p.state.Put(goCtx, iamNamespace, stateKey, raw); err != nil {
			return nil, fmt.Errorf("put user: %w", err)
		}
	default:
		role, err := p.loadRole(goCtx, entityName)
		if err != nil {
			return nil, err
		}
		if role == nil {
			return iamErrorResponse("NoSuchEntity",
				fmt.Sprintf("The role with name %s cannot be found.", entityName),
				http.StatusNotFound), nil
		}
		role.PermissionsBoundary = nil
		raw, err := json.Marshal(role)
		if err != nil {
			return nil, fmt.Errorf("marshal role: %w", err)
		}
		if err := p.state.Put(goCtx, iamNamespace, stateKey, raw); err != nil {
			return nil, fmt.Errorf("put role: %w", err)
		}
	}

	return iamXMLEmptyResponse("Delete" + actionSuffix + "PermissionsBoundary"), nil
}

// --- Tagging operations ----------------------------------------------------

func (p *IAMPlugin) tagUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string   `json:"UserName"`
		Tags     []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	// Tags is a query-protocol list, so it arrives as Tags.member.N.Key/Value and
	// the JSON unmarshal above leaves it nil. Reading req.Params is how every real
	// client's tags reach this handler; the JSON path is how a unit test drives it.
	// Only the second was ever exercised, which is why this shipped storing nothing
	// (#639). See iam_query.go.
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:TagUser", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	// Merge tags by key.
	tagMap := make(map[string]string, len(user.Tags))
	for _, t := range user.Tags {
		tagMap[t.Key] = t.Value
	}
	for _, t := range params.Tags {
		tagMap[t.Key] = t.Value
	}
	merged := make([]IAMTag, 0, len(tagMap))
	for k, v := range tagMap {
		merged = append(merged, IAMTag{Key: k, Value: v})
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })
	user.Tags = merged

	raw, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("tagUser marshal: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "user:"+params.UserName, raw); err != nil {
		return nil, fmt.Errorf("tagUser put: %w", err)
	}

	return iamXMLEmptyResponse("TagUser"), nil
}

func (p *IAMPlugin) untagUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string   `json:"UserName"`
		TagKeys  []string `json:"TagKeys"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}
	// TagKeys is a flat query-protocol list; see tagUser above and iam_query.go (#639).
	if keys := iamMemberList(req.Params, "TagKeys"); keys != nil {
		params.TagKeys = keys
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:UntagUser", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	removeKeys := make(map[string]struct{}, len(params.TagKeys))
	for _, k := range params.TagKeys {
		removeKeys[k] = struct{}{}
	}
	filtered := user.Tags[:0]
	for _, t := range user.Tags {
		if _, remove := removeKeys[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}
	user.Tags = filtered

	raw, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("untagUser marshal: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "user:"+params.UserName, raw); err != nil {
		return nil, fmt.Errorf("untagUser put: %w", err)
	}

	return iamXMLEmptyResponse("UntagUser"), nil
}

func (p *IAMPlugin) listUserTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		UserName string `json:"UserName"`
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.UserName == "" {
		return iamErrorResponse("ValidationError", "UserName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListUserTags", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	user, err := p.loadUser(goCtx, params.UserName)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The user with name %s cannot be found.", params.UserName),
			http.StatusNotFound), nil
	}

	tags := user.Tags
	if tags == nil {
		tags = []IAMTag{}
	}

	maxItems := params.MaxItems.Int()
	if maxItems <= 0 {
		maxItems = 100
	}

	// Simple pagination over sorted tags.
	startIdx := 0
	if params.Marker != "" {
		for i, t := range tags {
			if t.Key == params.Marker {
				startIdx = i
				break
			}
		}
	}
	end := startIdx + maxItems
	isTruncated := false
	var nextMarker string
	if end < len(tags) {
		isTruncated = true
		nextMarker = tags[end].Key
	} else {
		end = len(tags)
	}

	xmlStr := iamTagListXML(tags[startIdx:end]) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListUserTags", xmlStr)
}

func (p *IAMPlugin) tagRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string   `json:"RoleName"`
		Tags     []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}
	// Tags is a query-protocol list; see tagUser and iam_query.go (#639).
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:TagRole", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	// Merge tags by key.
	tagMap := make(map[string]string, len(role.Tags))
	for _, t := range role.Tags {
		tagMap[t.Key] = t.Value
	}
	for _, t := range params.Tags {
		tagMap[t.Key] = t.Value
	}
	merged := make([]IAMTag, 0, len(tagMap))
	for k, v := range tagMap {
		merged = append(merged, IAMTag{Key: k, Value: v})
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })
	role.Tags = merged

	raw, err := json.Marshal(role)
	if err != nil {
		return nil, fmt.Errorf("tagRole marshal: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "role:"+params.RoleName, raw); err != nil {
		return nil, fmt.Errorf("tagRole put: %w", err)
	}

	return iamXMLEmptyResponse("TagRole"), nil
}

func (p *IAMPlugin) untagRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string   `json:"RoleName"`
		TagKeys  []string `json:"TagKeys"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}
	// TagKeys is a flat query-protocol list; see tagUser and iam_query.go (#639).
	if keys := iamMemberList(req.Params, "TagKeys"); keys != nil {
		params.TagKeys = keys
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:UntagRole", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	removeKeys := make(map[string]struct{}, len(params.TagKeys))
	for _, k := range params.TagKeys {
		removeKeys[k] = struct{}{}
	}
	filtered := role.Tags[:0]
	for _, t := range role.Tags {
		if _, remove := removeKeys[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}
	role.Tags = filtered

	raw, err := json.Marshal(role)
	if err != nil {
		return nil, fmt.Errorf("untagRole marshal: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "role:"+params.RoleName, raw); err != nil {
		return nil, fmt.Errorf("untagRole put: %w", err)
	}

	return iamXMLEmptyResponse("UntagRole"), nil
}

func (p *IAMPlugin) listRoleTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName string `json:"RoleName"`
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListRoleTags", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}

	tags := role.Tags
	if tags == nil {
		tags = []IAMTag{}
	}

	maxItems := params.MaxItems.Int()
	if maxItems <= 0 {
		maxItems = 100
	}

	startIdx := 0
	if params.Marker != "" {
		for i, t := range tags {
			if t.Key == params.Marker {
				startIdx = i
				break
			}
		}
	}
	end := startIdx + maxItems
	isTruncated := false
	var nextMarker string
	if end < len(tags) {
		isTruncated = true
		nextMarker = tags[end].Key
	} else {
		end = len(tags)
	}

	xmlStr := iamTagListXML(tags[startIdx:end]) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListRoleTags", xmlStr)
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

// loadInlinePolicyNames returns the sorted list of inline policy names for the
// given entity (entityType = "user" or "role").
func (p *IAMPlugin) loadInlinePolicyNames(goCtx context.Context, entityType, entityName string) ([]string, error) {
	return p.loadStringList(goCtx, entityType+"_inline_names:"+entityName)
}

// loadInlinePolicyDoc loads and parses an inline policy document. Returns nil
// when the key is not found.
func (p *IAMPlugin) loadInlinePolicyDoc(goCtx context.Context, entityType, entityName, policyName string) (*PolicyDocument, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, entityType+"_inline:"+entityName+":"+policyName)
	if err != nil {
		return nil, fmt.Errorf("load inline policy %s/%s/%s: %w", entityType, entityName, policyName, err)
	}
	if raw == nil {
		return nil, nil
	}
	var doc PolicyDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal inline policy %s/%s/%s: %w", entityType, entityName, policyName, err)
	}
	return &doc, nil
}

// loadBoundaryPolicyDocs loads the policy documents for a permissions-boundary
// ARN. Returns nil when the policy cannot be found.
func (p *IAMPlugin) loadBoundaryPolicyDocs(goCtx context.Context, arn string) []PolicyDocument {
	if mp, ok := GetManagedPolicy(arn); ok {
		return []PolicyDocument{mp.Document}
	}
	raw, err := p.state.Get(goCtx, iamNamespace, "policy:"+arn)
	if err != nil || raw == nil {
		return nil
	}
	var pol IAMPolicy
	if err := json.Unmarshal(raw, &pol); err != nil {
		return nil
	}
	return []PolicyDocument{pol.Document}
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
// iamErrorResponse is defined in iam_xml.go and returns an XML error response.
// iamXMLResponse and iamXMLEmptyResponse are also defined in iam_xml.go.

// parseIAMBody unmarshals an IAM JSON request body into dst.
// An empty body is treated as an empty JSON object.
//
// A failure is reported through iamParamMessage, so a bad scalar names its
// parameter — "MaxItems must be an integer, got \"abc\"". Every handler surfaces
// this as a ValidationError, and before #642 the message a caller got for a
// perfectly valid `MaxItems=1` was an encoding/json internal about a Go struct
// field.
func parseIAMBody(body []byte, dst any) error {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("invalid request body: %s", iamParamMessage(err))
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

// --- Instance profiles -------------------------------------------------------

// --- Instance profile operations ---

func (p *IAMPlugin) createInstanceProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string `json:"InstanceProfileName"`
		Path                string `json:"Path"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName is required", http.StatusBadRequest), nil
	}
	if params.Path == "" {
		params.Path = "/"
	}

	goCtx := context.Background()
	key := "instance_profile:" + params.InstanceProfileName
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get instance profile: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExists",
			fmt.Sprintf("Instance Profile %s already exists.", params.InstanceProfileName),
			http.StatusConflict), nil
	}

	profile := &IAMInstanceProfile{
		InstanceProfileName: params.InstanceProfileName,
		InstanceProfileID:   generateIAMID("AIPA"),
		ARN:                 fmt.Sprintf("arn:aws:iam::%s:instance-profile%s%s", ctx.AccountID, params.Path, params.InstanceProfileName),
		Path:                params.Path,
		Roles:               []IAMRole{},
		CreateDate:          p.now().UTC(),
	}

	raw, err := json.Marshal(profile)
	if err != nil {
		return nil, fmt.Errorf("marshal instance profile: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put instance profile: %w", err)
	}
	return iamXMLResponse(http.StatusOK, "CreateInstanceProfile", iamSingleInstanceProfileXML(profile))
}

func (p *IAMPlugin) getInstanceProfile(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string `json:"InstanceProfileName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName)
	if err != nil {
		return nil, fmt.Errorf("get instance profile: %w", err)
	}
	if data == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Instance Profile %s cannot be found.", params.InstanceProfileName),
			http.StatusNotFound), nil
	}
	var profile IAMInstanceProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		return nil, fmt.Errorf("unmarshal instance profile: %w", err)
	}
	return iamXMLResponse(http.StatusOK, "GetInstanceProfile", iamSingleInstanceProfileXML(&profile))
}

func (p *IAMPlugin) deleteInstanceProfile(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string `json:"InstanceProfileName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName)
	if err != nil {
		return nil, fmt.Errorf("get instance profile: %w", err)
	}
	if data == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Instance Profile %s cannot be found.", params.InstanceProfileName),
			http.StatusNotFound), nil
	}
	var profile IAMInstanceProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		return nil, fmt.Errorf("unmarshal instance profile: %w", err)
	}
	if len(profile.Roles) > 0 {
		return iamErrorResponse("DeleteConflict",
			"Cannot delete entity, must detach all roles first.",
			http.StatusConflict), nil
	}
	if err := p.state.Delete(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName); err != nil {
		return nil, fmt.Errorf("delete instance profile: %w", err)
	}
	return iamXMLEmptyResponse("DeleteInstanceProfile"), nil
}

func (p *IAMPlugin) addRoleToInstanceProfile(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string `json:"InstanceProfileName"`
		RoleName            string `json:"RoleName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" || params.RoleName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName and RoleName are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	profData, err := p.state.Get(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName)
	if err != nil {
		return nil, fmt.Errorf("get instance profile: %w", err)
	}
	if profData == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Instance Profile %s cannot be found.", params.InstanceProfileName),
			http.StatusNotFound), nil
	}
	var profile IAMInstanceProfile
	if err := json.Unmarshal(profData, &profile); err != nil {
		return nil, fmt.Errorf("unmarshal instance profile: %w", err)
	}
	if len(profile.Roles) > 0 {
		return iamErrorResponse("LimitExceeded",
			"Instance profile "+params.InstanceProfileName+" already has a role.",
			http.StatusConflict), nil
	}

	roleData, err := p.state.Get(goCtx, iamNamespace, "role:"+params.RoleName)
	if err != nil {
		return nil, fmt.Errorf("get role: %w", err)
	}
	if roleData == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Role %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}
	var role IAMRole
	if err := json.Unmarshal(roleData, &role); err != nil {
		return nil, fmt.Errorf("unmarshal role: %w", err)
	}

	profile.Roles = []IAMRole{role}
	raw, err := json.Marshal(profile)
	if err != nil {
		return nil, fmt.Errorf("marshal instance profile: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName, raw); err != nil {
		return nil, fmt.Errorf("put instance profile: %w", err)
	}
	return iamXMLEmptyResponse("AddRoleToInstanceProfile"), nil
}

func (p *IAMPlugin) removeRoleFromInstanceProfile(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string `json:"InstanceProfileName"`
		RoleName            string `json:"RoleName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" || params.RoleName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName and RoleName are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	profData, err := p.state.Get(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName)
	if err != nil {
		return nil, fmt.Errorf("get instance profile: %w", err)
	}
	if profData == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Instance Profile %s cannot be found.", params.InstanceProfileName),
			http.StatusNotFound), nil
	}
	var profile IAMInstanceProfile
	if err := json.Unmarshal(profData, &profile); err != nil {
		return nil, fmt.Errorf("unmarshal instance profile: %w", err)
	}

	filtered := profile.Roles[:0]
	for _, r := range profile.Roles {
		if r.RoleName != params.RoleName {
			filtered = append(filtered, r)
		}
	}
	profile.Roles = filtered
	if profile.Roles == nil {
		profile.Roles = []IAMRole{}
	}

	raw, err := json.Marshal(profile)
	if err != nil {
		return nil, fmt.Errorf("marshal instance profile: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, "instance_profile:"+params.InstanceProfileName, raw); err != nil {
		return nil, fmt.Errorf("put instance profile: %w", err)
	}
	return iamXMLEmptyResponse("RemoveRoleFromInstanceProfile"), nil
}

// instanceProfilesHoldingRole returns the names of the instance profiles that list
// roleName, in state order, so [IAMPlugin.deleteRole] can refuse and name them.
//
// It sweeps the instance_profile: prefix the same way listInstanceProfiles does,
// rather than maintaining a reverse index: a profile's roles are already stored on
// the profile, and a second index would be a consistency problem of its own for a
// read that happens once per role delete. An unreadable or unparsable profile is
// skipped, matching listInstanceProfiles — one corrupt record must not make a role
// undeletable.
func (p *IAMPlugin) instanceProfilesHoldingRole(
	ctx context.Context, roleName string,
) ([]string, error) {
	keys, err := p.state.List(ctx, iamNamespace, "instance_profile:")
	if err != nil {
		return nil, fmt.Errorf("list instance profiles: %w", err)
	}

	var holders []string
	for _, k := range keys {
		data, getErr := p.state.Get(ctx, iamNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var profile IAMInstanceProfile
		if err := json.Unmarshal(data, &profile); err != nil {
			continue
		}
		for _, r := range profile.Roles {
			if r.RoleName == roleName {
				holders = append(holders, profile.InstanceProfileName)
				break
			}
		}
	}
	return holders, nil
}

// listInstanceProfiles returns persisted IAM instance profiles.
func (p *IAMPlugin) listInstanceProfiles(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, iamNamespace, "instance_profile:")
	if err != nil {
		return nil, fmt.Errorf("list instance profiles: %w", err)
	}
	profiles := make([]IAMInstanceProfile, 0, len(keys))
	for _, k := range keys {
		data, getErr := p.state.Get(goCtx, iamNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var profile IAMInstanceProfile
		if err := json.Unmarshal(data, &profile); err != nil {
			continue
		}
		profiles = append(profiles, profile)
	}
	return iamXMLResponse(http.StatusOK, "ListInstanceProfiles", iamInstanceProfileListXML(profiles)+"<IsTruncated>false</IsTruncated>")
}
