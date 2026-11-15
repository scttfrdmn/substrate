package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// iamMinSessionDuration and iamMaxSessionDuration bound a role's MaxSessionDuration: "Valid Range:
// Minimum value of 3600. Maximum value of 43200" (API_UpdateRole, API_CreateRole).
const (
	iamMinSessionDuration = 3600
	iamMaxSessionDuration = 43200
)

// updateRole changes a role's description or maximum session duration in place — "Updates the
// description or maximum session duration setting of a role" (API_UpdateRole). It is what
// CloudFormation's in-place update of an AWS::IAM::Role's Description or MaxSessionDuration goes
// through (#1545).
//
// Both members are optional and only a member the request carries is changed: an absent
// Description leaves the stored one, while an empty one clears it. The page publishes
// NoSuchEntity, UnmodifiableEntity and ServiceFailure; a MaxSessionDuration outside the published
// range is ValidationError, the common error for a value outside a member's constraints, with a
// message that is substrate's own. A service-linked role is UnmodifiableEntity, with the message
// [IAMPlugin.deleteRole] uses for the same refusal.
func (p *IAMPlugin) updateRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		RoleName           string  `json:"RoleName"`
		Description        *string `json:"Description"`
		MaxSessionDuration iamInt  `json:"MaxSessionDuration"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.RoleName == "" {
		return iamErrorResponse("ValidationError", "RoleName is required", http.StatusBadRequest), nil
	}
	if d := params.MaxSessionDuration.Int(); d != 0 && (d < iamMinSessionDuration || d > iamMaxSessionDuration) {
		return iamErrorResponse("ValidationError",
			fmt.Sprintf("MaxSessionDuration must be between %d and %d seconds, got %d.",
				iamMinSessionDuration, iamMaxSessionDuration, d),
			http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:UpdateRole", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	role, err := p.loadRole(goCtx, ctx.AccountID, params.RoleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The role with name %s cannot be found.", params.RoleName),
			http.StatusNotFound), nil
	}
	if iamIsSLRPath(role.Path) {
		return iamErrorResponse("UnmodifiableEntity",
			"Service linked roles are protected AWS resources. "+
				"Only the service that depends on the service-linked role can modify or delete "+
				"the role on your behalf.",
			http.StatusBadRequest), nil
	}

	if params.Description != nil {
		role.Description = *params.Description
	}
	if d := params.MaxSessionDuration.Int(); d != 0 {
		role.MaxSessionDuration = d
	}

	raw, err := json.Marshal(role)
	if err != nil {
		return nil, fmt.Errorf("marshal role: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, iamRoleKey(ctx.AccountID, params.RoleName), raw); err != nil {
		return nil, fmt.Errorf("put role: %w", err)
	}
	return iamXMLEmptyResponse("UpdateRole"), nil
}
