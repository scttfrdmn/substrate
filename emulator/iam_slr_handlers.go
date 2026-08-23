package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// iamSLRServiceNamePattern is AWS's published pattern for AWSServiceName, `[\w+=,.@-]+`.
//
// It is the same pattern Config's own service-principal parameter carries
// ([cfgsvcServicePrincipalPattern]), which is worth noting because it admits strings
// that are not service principals at all — "lambda", "%" is refused but "lambda" is not.
// AWS validates the shape here and the principal's existence separately, and substrate
// does the same: a well-formed name it has no table row for gets a derived role name
// rather than a refusal, because refusing would make substrate the authority on which
// services exist.
var iamSLRServiceNamePattern = regexp.MustCompile(`^[\w+=,.@-]+$`)

// iamSLRCustomSuffixPattern is AWS's published pattern for CustomSuffix, identical to
// AWSServiceName's.
var iamSLRCustomSuffixPattern = regexp.MustCompile(`^[\w+=,.@-]+$`)

// AWS's published length limits for the three service-linked-role parameters.
const (
	iamSLRServiceNameMaxLen  = 128
	iamSLRCustomSuffixMaxLen = 64
	iamSLRDescriptionMaxLen  = 1000
	iamSLRRoleNameMaxLen     = 64
)

// iamSLRInvalidInputCode is the refusal every malformed service-linked-role request
// gets.
//
// CreateServiceLinkedRole publishes exactly four errors — InvalidInput 400,
// LimitExceeded 409, NoSuchEntity 404 and ServiceFailure 500 — and notably **not**
// EntityAlreadyExists, which is what CreateRole answers for a duplicate name. So the
// duplicate-suffix refusal cannot be a copy of createRole's: substrate answers
// InvalidInput, which is its reading of AWS's "if a role with that name already exists,
// the request fails with a duplicate role name error" against the four codes the
// operation actually publishes.
const iamSLRInvalidInputCode = "InvalidInput"

// createServiceLinkedRole handles CreateServiceLinkedRole.
//
// AWS: "Creates an IAM role that is linked to a specific AWS service. The service
// controls the permissions of the role, and the role can only be used by that service."
// Substrate mints the role under the reserved `/aws-service-role/<principal>/` path with
// a trust policy naming the linked service, which is what makes it visible to GetRole,
// ListRoles and the STS trust-policy gate like any other role.
//
// The operation exists here principally because eleven of the condition blocks in
// substrate's bundled managed policies condition on `iam:AWSServiceName`, and before
// #747 the operation carrying that parameter answered InvalidAction — so none of the
// eleven could be evaluated at all, let alone satisfied.
func (p *IAMPlugin) createServiceLinkedRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		AWSServiceName string `json:"AWSServiceName"`
		CustomSuffix   string `json:"CustomSuffix"`
		Description    string `json:"Description"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.AWSServiceName == "" {
		return iamErrorResponse("ValidationError", "AWSServiceName is required", http.StatusBadRequest), nil
	}
	if len(params.AWSServiceName) > iamSLRServiceNameMaxLen ||
		!iamSLRServiceNamePattern.MatchString(params.AWSServiceName) {
		return iamErrorResponse(iamSLRInvalidInputCode,
			fmt.Sprintf("The value %q for parameter AWSServiceName is invalid.", params.AWSServiceName),
			http.StatusBadRequest), nil
	}
	if params.CustomSuffix != "" &&
		(len(params.CustomSuffix) > iamSLRCustomSuffixMaxLen ||
			!iamSLRCustomSuffixPattern.MatchString(params.CustomSuffix)) {
		return iamErrorResponse(iamSLRInvalidInputCode,
			fmt.Sprintf("The value %q for parameter CustomSuffix is invalid.", params.CustomSuffix),
			http.StatusBadRequest), nil
	}
	if len(params.Description) > iamSLRDescriptionMaxLen {
		return iamErrorResponse(iamSLRInvalidInputCode,
			fmt.Sprintf("The value for parameter Description exceeds the maximum length of %d.",
				iamSLRDescriptionMaxLen),
			http.StatusBadRequest), nil
	}

	roleName := iamSLRRoleName(params.AWSServiceName, params.CustomSuffix)
	if len(roleName) > iamSLRRoleNameMaxLen {
		// A role name is capped at 64 characters everywhere in IAM, and a long derived
		// name plus a long suffix can exceed it. Refusing here rather than storing a
		// role no other operation could address is what keeps DeleteServiceLinkedRole
		// and GetRole able to name what this created.
		return iamErrorResponse(iamSLRInvalidInputCode,
			fmt.Sprintf("The combined role name %q exceeds the maximum length of %d characters.",
				roleName, iamSLRRoleNameMaxLen),
			http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	// The resource is the ARN this is about to mint, not "*", and the context carries
	// iam:AWSServiceName — both of which the generic gate publishes too
	// ([iamAuthzResources], [iamAuthzRequestContext]). A statement scoped to
	// `arn:aws:iam::*:role/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda`
	// with a StringLike on the service name is the exact shape five of the bundled
	// policies use, and it has to reach the same verdict at both doors (#747).
	slrPath := iamSLRPath(params.AWSServiceName)
	roleARN := iamRoleARN(ctx.AccountID, slrPath, roleName)
	if err := p.authorizeWith(goCtx, ctx, "iam:CreateServiceLinkedRole", roleARN,
		map[string]string{iamSLRAWSServiceNameCondKey: params.AWSServiceName}); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	key := iamRoleKey(ctx.AccountID, roleName)
	existing, err := p.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get service-linked role: %w", err)
	}
	if existing != nil {
		return iamErrorResponse(iamSLRInvalidInputCode,
			fmt.Sprintf("Service role name %s has been taken in this account, please try a different suffix.",
				roleName),
			http.StatusBadRequest), nil
	}

	role := &IAMRole{
		RoleName:                 roleName,
		RoleID:                   generateIAMID("AROA"),
		ARN:                      roleARN,
		Path:                     slrPath,
		Description:              params.Description,
		MaxSessionDuration:       3600,
		CreateDate:               p.now().UTC(),
		AssumeRolePolicyDocument: iamSLRTrustPolicy(params.AWSServiceName),
	}
	raw, err := json.Marshal(role)
	if err != nil {
		return nil, fmt.Errorf("marshal service-linked role: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, key, raw); err != nil {
		return nil, fmt.Errorf("put service-linked role: %w", err)
	}

	return iamXMLResponse(http.StatusOK, "CreateServiceLinkedRole", iamSingleRoleXML(role))
}

// deleteServiceLinkedRole handles DeleteServiceLinkedRole.
//
// AWS: "Submits a service-linked role deletion request and returns a DeletionTaskId,
// which you can use to check the status of the deletion." The submission is
// asynchronous, so this returns a task rather than an outcome, and
// [IAMPlugin.getServiceLinkedRoleDeletionStatus] reports the outcome.
//
// Substrate deletes the role record immediately and reports the task SUCCEEDED, because
// nothing here is using it — the linked service does not run. A test that needs the
// failure AWS documents ("If you submit a deletion request for a service-linked role
// whose linked service is still accessing a resource, then the deletion task fails")
// seeds it through `POST /v1/iam/slr-deletion-status`, which is how a poll loop's
// failure branch becomes reachable without wall-clock time.
func (p *IAMPlugin) deleteServiceLinkedRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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

	role, err := p.loadRole(goCtx, ctx.AccountID, params.RoleName)
	if err != nil {
		return nil, err
	}

	// The role's own path names the service, so authorization can name the same ARN the
	// gate built — and can publish iam:AWSServiceName for the delete half of the two
	// bundled statements that condition on it. A role that does not exist yields no path
	// and authorizes against "*", so the refusal that follows is NoSuchEntity rather than
	// a denial that would leak whether the role is there.
	resource, extra := "*", map[string]string(nil)
	if role != nil && iamIsSLRPath(role.Path) {
		resource = role.ARN
		if service := iamSLRServiceFromPath(role.Path); service != "" {
			extra = map[string]string{iamSLRAWSServiceNameCondKey: service}
		}
	}
	if err := p.authorizeWith(goCtx, ctx, "iam:DeleteServiceLinkedRole", resource, extra); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}
	if !iamIsSLRPath(role.Path) {
		// Not a service-linked role. AWS publishes no InvalidInput on this operation,
		// and the four codes it does publish leave NoSuchEntity as the only one that
		// fits: there is no *service-linked* role by that name. The message says which
		// so a caller is not left hunting a role that plainly exists.
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s is not a service-linked role.", params.RoleName),
			http.StatusNotFound), nil
	}

	// An incomplete task for the same role is returned again rather than duplicated:
	// "If the deletion task is not yet complete, the DeletionTaskId of the existing task
	// is returned." Which is also what keeps a caller's retry from accumulating tasks.
	if existing, err := p.findIncompleteSLRDeletionTask(goCtx, ctx.AccountID, params.RoleName); err != nil {
		return nil, err
	} else if existing != nil {
		return iamXMLResponse(http.StatusOK, "DeleteServiceLinkedRole",
			"<DeletionTaskId>"+xmlEsc(existing.DeletionTaskID)+"</DeletionTaskId>")
	}

	service := iamSLRServiceFromPath(role.Path)
	task := &IAMSLRDeletionTask{
		DeletionTaskID: iamSLRDeletionTaskID(service, role.RoleName, iamSLRTaskUUID()),
		RoleName:       role.RoleName,
		ServiceName:    service,
		Status:         iamSLRDeletionSucceeded,
	}
	// A seed decides the status, so a consumer's poll loop can be driven to FAILED or
	// held at IN_PROGRESS. It is read at submission rather than at each status poll
	// because the deletion itself is conditional on it: a task that fails must leave the
	// role in place, which is the whole observable difference between the two outcomes.
	if seed := p.slrDeletionSeed(goCtx, role.RoleName); seed != nil {
		task.Status, task.Reason, task.RoleUsageList = seed.Status, seed.Reason, seed.RoleUsageList
	}

	if task.Status == iamSLRDeletionSucceeded {
		if err := p.state.Delete(goCtx, iamNamespace, iamRoleKey(ctx.AccountID, role.RoleName)); err != nil {
			return nil, fmt.Errorf("delete service-linked role: %w", err)
		}
	}

	raw, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("marshal service-linked role deletion task: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace,
		iamSLRDeletionTaskKey(ctx.AccountID, task.DeletionTaskID), raw); err != nil {
		return nil, fmt.Errorf("put service-linked role deletion task: %w", err)
	}

	return iamXMLResponse(http.StatusOK, "DeleteServiceLinkedRole",
		"<DeletionTaskId>"+xmlEsc(task.DeletionTaskID)+"</DeletionTaskId>")
}

// getServiceLinkedRoleDeletionStatus handles GetServiceLinkedRoleDeletionStatus.
//
// AWS: "Retrieves the status of your service-linked role deletion. After you use
// DeleteServiceLinkedRole to submit a service-linked role for deletion, you can use the
// DeletionTaskId parameter … to check the status of the deletion."
//
// Status is one of SUCCEEDED, IN_PROGRESS, FAILED or NOT_STARTED, and a FAILED task
// also reports a Reason naming the resources that must be deleted first — which is the
// shape a consumer's poll loop branches on and the reason the outcome is seedable.
func (p *IAMPlugin) getServiceLinkedRoleDeletionStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		DeletionTaskID string `json:"DeletionTaskId"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.DeletionTaskID == "" {
		return iamErrorResponse("ValidationError", "DeletionTaskId is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	// The task ID embeds the service and the role, so the resource resolves with no
	// state read — which matters here, because a SUCCEEDED task's role is gone and a
	// resource derived from state would fall back to "*" exactly when the poll loop
	// finally gets its answer.
	//
	// No iam:AWSServiceName is published: the reference lists the key on the create and
	// the delete, not here, and publishing one AWS does not is the permissive direction.
	// See [iamAuthzRequestContext], which makes the same split at the other door.
	resource := "*"
	if service, roleName, ok := iamSLRRoleFromDeletionTaskID(params.DeletionTaskID); ok {
		resource = iamRoleARN(ctx.AccountID, iamSLRPath(service), roleName)
	}
	if err := p.authorize(goCtx, ctx, "iam:GetServiceLinkedRoleDeletionStatus", resource); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace,
		iamSLRDeletionTaskKey(ctx.AccountID, params.DeletionTaskID))
	if err != nil {
		return nil, fmt.Errorf("get service-linked role deletion task: %w", err)
	}
	if raw == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The deletion task with ID %s cannot be found.", params.DeletionTaskID),
			http.StatusNotFound), nil
	}
	var task IAMSLRDeletionTask
	if err := json.Unmarshal(raw, &task); err != nil {
		return nil, fmt.Errorf("unmarshal service-linked role deletion task: %w", err)
	}

	return iamXMLResponse(http.StatusOK, "GetServiceLinkedRoleDeletionStatus",
		iamSLRDeletionStatusXML(&task))
}

// iamSLRDeletionStatusXML renders a deletion task as a
// GetServiceLinkedRoleDeletionStatus result.
//
// Reason is a DeletionTaskFailureReasonType — a Reason string plus a RoleUsageList of
// {Region, Resources} — and is emitted only when there is one, because AWS reports the
// member only for a task that failed.
func iamSLRDeletionStatusXML(task *IAMSLRDeletionTask) string {
	var b strings.Builder
	b.WriteString("<Status>")
	b.WriteString(xmlEsc(task.Status))
	b.WriteString("</Status>")
	if task.Reason == "" && len(task.RoleUsageList) == 0 {
		return b.String()
	}
	b.WriteString("<Reason>")
	if task.Reason != "" {
		b.WriteString("<Reason>")
		b.WriteString(xmlEsc(task.Reason))
		b.WriteString("</Reason>")
	}
	b.WriteString("<RoleUsageList>")
	for _, usage := range task.RoleUsageList {
		b.WriteString("<member><Region>")
		b.WriteString(xmlEsc(usage.Region))
		b.WriteString("</Region>")
		b.WriteString(iamStringListXML("Resources", usage.Resources))
		b.WriteString("</member>")
	}
	b.WriteString("</RoleUsageList></Reason>")
	return b.String()
}

// findIncompleteSLRDeletionTask returns the account's existing unfinished deletion task
// for a role, or nil when there is none.
//
// "Incomplete" is anything that is not SUCCEEDED: a FAILED task is complete in the sense
// that it has an answer, but AWS's rule is about a task still outstanding, so only
// IN_PROGRESS and NOT_STARTED hold a resubmission back. A FAILED task must not, or a
// caller who deleted the blocking resources could never retry.
func (p *IAMPlugin) findIncompleteSLRDeletionTask(goCtx context.Context, accountID, roleName string) (*IAMSLRDeletionTask, error) {
	keys, err := p.state.List(goCtx, iamNamespace, iamSLRDeletionTaskPrefix(accountID))
	if err != nil {
		return nil, fmt.Errorf("list service-linked role deletion tasks: %w", err)
	}
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var task IAMSLRDeletionTask
		if err := json.Unmarshal(raw, &task); err != nil {
			continue
		}
		if task.RoleName != roleName {
			continue
		}
		if task.Status == iamSLRDeletionInProgress || task.Status == iamSLRDeletionNotStarted {
			return &task, nil
		}
	}
	return nil, nil
}

// slrDeletionSeed returns the seeded deletion outcome for a role name, preferring an
// exact match over the "*" wildcard, or nil when nothing is seeded.
func (p *IAMPlugin) slrDeletionSeed(goCtx context.Context, roleName string) *IAMSLRDeletionTask {
	for _, name := range []string{roleName, "*"} {
		raw, err := p.state.Get(goCtx, iamCtrlNamespace, iamCtrlSLRDeletionStatusKey(name))
		if err != nil || raw == nil {
			continue
		}
		var seed IAMSLRDeletionTask
		if err := json.Unmarshal(raw, &seed); err != nil {
			continue
		}
		if seed.Status == "" {
			continue
		}
		return &seed
	}
	return nil
}
