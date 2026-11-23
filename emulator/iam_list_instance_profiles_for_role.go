package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ListInstanceProfilesForRole (#1519).
//
// The Terraform AWS provider calls it before deleting an aws_iam_role, to detach the role from
// every instance profile first, and the destroy failed because the operation was unrouted.
// Everything it needs was already held: AddRoleToInstanceProfile stores the role inside the
// profile record, which is what [IAMPlugin.instanceProfilesHoldingRole] reads for DeleteRole's
// DeleteConflict check.
//
// The role must exist: the reference publishes NoSuchEntity for "a resource that doesn't exist",
// and the message is the one GetRole uses. MaxItems and Marker page the profiles the way
// ListInstanceProfiles does, over the sorted state keys.
func (p *IAMPlugin) listInstanceProfilesForRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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
	if errResp := iamValidateMaxItems(req, params.MaxItems); errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListInstanceProfilesForRole", p.authzResource(ctx, req)); err != nil {
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

	keys, err := p.state.List(goCtx, iamNamespace, iamInstanceProfilePrefix(ctx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("list instance profiles for role: %w", err)
	}
	// Filtered before paging, as ListInstanceProfiles filters on PathPrefix, so a page is never
	// short of profiles that hold the role. An unreadable record is skipped, matching
	// instanceProfilesHoldingRole.
	byKey := make(map[string]IAMInstanceProfile, len(keys))
	matched := make([]string, 0, len(keys))
	for _, k := range keys {
		data, getErr := p.state.Get(goCtx, iamNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var profile IAMInstanceProfile
		if err := json.Unmarshal(data, &profile); err != nil {
			continue
		}
		for _, r := range profile.Roles {
			if r.RoleName == params.RoleName {
				byKey[k] = profile
				matched = append(matched, k)
				break
			}
		}
	}
	page, nextMarker, isTruncated := paginateIAMKeys(matched, params.Marker, params.MaxItems.Int())
	profiles := make([]IAMInstanceProfile, 0, len(page))
	for _, k := range page {
		profiles = append(profiles, byKey[k])
	}

	xmlStr := iamInstanceProfileListXML("InstanceProfiles", profiles, false) +
		"<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListInstanceProfilesForRole", xmlStr)
}
