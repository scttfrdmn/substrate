package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
)

// IAM groups: membership and the policy operations that make a group worth
// belonging to.
//
// Before this file, substrate routed CreateGroup/GetGroup/DeleteGroup/ListGroups
// and nothing else — a group could be created, but no user could join it and no
// policy could be put on it, so GetGroup answered with an empty member list
// unconditionally and a group had no observable effect on any request. That is
// also why no evaluation consulted group policies: there were none to consult.
//
// Membership is stored on *both* sides, "group_users:<group>" and
// "user_groups:<user>", because both directions are read by an API: GetGroup
// lists a group's users and ListGroupsForUser lists a user's groups. Every write
// goes through addGroupMembership/removeGroupMembership so an index cannot come
// to exist on one side only — the v0.99.0 saveAccount lesson.
//
// Group policies reuse the existing storage exactly: managed attachments land in
// "group_policies:<name>" through loadPolicyList/savePolicyList, and inline
// documents go through the putInlinePolicy family, which is entity-type
// parameterized. Nothing here invents a second way to store a policy, which is
// what lets loadPoliciesForPrincipal (authz.go) read a group's policies with the
// same code it already uses for a user's.

// --- Membership ------------------------------------------------------------

// addUserToGroup implements the AddUserToGroup operation.
func (p *IAMPlugin) addUserToGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		UserName  string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" || params.UserName == "" {
		return iamErrorResponse("ValidationError", "GroupName and UserName are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:AddUserToGroup", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	if resp, err := p.requireGroupAndUser(goCtx, params.GroupName, params.UserName); resp != nil || err != nil {
		return resp, err
	}

	if err := p.addGroupMembership(goCtx, params.GroupName, params.UserName); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("AddUserToGroup"), nil
}

// removeUserFromGroup implements the RemoveUserFromGroup operation.
func (p *IAMPlugin) removeUserFromGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		UserName  string `json:"UserName"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" || params.UserName == "" {
		return iamErrorResponse("ValidationError", "GroupName and UserName are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:RemoveUserFromGroup", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	if resp, err := p.requireGroupAndUser(goCtx, params.GroupName, params.UserName); resp != nil || err != nil {
		return resp, err
	}

	// AWS declares only NoSuchEntity for the group and the user, not for the
	// membership: removing a user who is not a member is not an error, so the write
	// is idempotent in both directions.
	if err := p.removeGroupMembership(goCtx, params.GroupName, params.UserName); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("RemoveUserFromGroup"), nil
}

// listGroupsForUser implements the ListGroupsForUser operation.
func (p *IAMPlugin) listGroupsForUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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
	if err := p.authorize(goCtx, ctx, "iam:ListGroupsForUser", "*"); err != nil {
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

	names, err := p.loadStringList(goCtx, iamUserGroupsKey(params.UserName))
	if err != nil {
		return nil, err
	}

	page, nextMarker, isTruncated := paginateIAMKeys(names, params.Marker, params.MaxItems.Int())

	groups := make([]*IAMGroup, 0, len(page))
	for _, name := range page {
		group, err := p.loadGroup(goCtx, name)
		if err != nil {
			return nil, err
		}
		// A membership naming a group that no longer exists is skipped rather than
		// reported: DeleteGroup refuses while the group has members, so this can only
		// arise from a state edit outside the API.
		if group == nil {
			continue
		}
		groups = append(groups, group)
	}

	xmlStr := iamGroupListXML(groups) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListGroupsForUser", xmlStr)
}

// --- Managed policy attachment (group) -------------------------------------

// attachGroupPolicy implements the AttachGroupPolicy operation.
func (p *IAMPlugin) attachGroupPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" || params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "GroupName and PolicyArn are required", http.StatusBadRequest), nil
	}
	// Shape, not existence — see iam_policy_arn.go (#499). Applied here too, so the asymmetry
	// #499 reports is not recreated in an operation shipping in the same release.
	if message, ok := iamValidatePolicyARN(params.PolicyArn); !ok {
		return iamErrorResponse("InvalidInput", message, http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:AttachGroupPolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	group, err := p.loadGroup(goCtx, params.GroupName)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The group with name %s cannot be found.", params.GroupName),
			http.StatusNotFound), nil
	}

	// Existence is not required, only reported — see iam_policy_arn.go (#499). The check runs
	// after the group lookup so an attach that is going to fail anyway does not also warn.
	p.iamWarnUnresolvedPolicyARN(goCtx, "AttachGroupPolicy", params.PolicyArn)

	listKey := iamGroupPoliciesKey(params.GroupName)
	arns, err := p.loadPolicyList(goCtx, listKey)
	if err != nil {
		return nil, err
	}
	for _, a := range arns {
		if a == params.PolicyArn {
			return iamXMLEmptyResponse("AttachGroupPolicy"), nil
		}
	}
	arns = append(arns, params.PolicyArn)
	if err := p.savePolicyList(goCtx, listKey, arns); err != nil {
		return nil, err
	}

	return iamXMLEmptyResponse("AttachGroupPolicy"), nil
}

// detachGroupPolicy implements the DetachGroupPolicy operation.
func (p *IAMPlugin) detachGroupPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		GroupName string `json:"GroupName"`
		PolicyArn string `json:"PolicyArn"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.GroupName == "" || params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "GroupName and PolicyArn are required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:DetachGroupPolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	listKey := iamGroupPoliciesKey(params.GroupName)
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

	return iamXMLEmptyResponse("DetachGroupPolicy"), nil
}

// listAttachedGroupPolicies implements the ListAttachedGroupPolicies operation.
func (p *IAMPlugin) listAttachedGroupPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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
	if err := p.authorize(goCtx, ctx, "iam:ListAttachedGroupPolicies", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	arns, err := p.loadPolicyList(goCtx, iamGroupPoliciesKey(params.GroupName))
	if err != nil {
		return nil, err
	}

	policies := make([]IAMAttachedPolicy, 0, len(arns))
	for _, arn := range arns {
		policies = append(policies, IAMAttachedPolicy{PolicyName: arnPolicyName(arn), PolicyARN: arn})
	}

	return iamXMLResponse(http.StatusOK, "ListAttachedGroupPolicies",
		iamAttachedPoliciesXML(policies)+"<IsTruncated>false</IsTruncated>")
}

// --- Inline policies (group) -----------------------------------------------

func (p *IAMPlugin) putGroupPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.putInlinePolicy(ctx, req, "group")
}

func (p *IAMPlugin) getGroupPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.getInlinePolicy(ctx, req, "group")
}

func (p *IAMPlugin) deleteGroupPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.deleteInlinePolicy(ctx, req, "group")
}

func (p *IAMPlugin) listGroupPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.listInlinePolicies(ctx, req, "group")
}

// --- State helpers ---------------------------------------------------------

// iamGroupUsersKey is the state key holding a group's member user names.
func iamGroupUsersKey(groupName string) string { return "group_users:" + groupName }

// iamUserGroupsKey is the state key holding the group names a user belongs to.
func iamUserGroupsKey(userName string) string { return "user_groups:" + userName }

// iamGroupPoliciesKey is the state key holding a group's attached managed policy
// ARNs. It follows the "<kind>_policies:<name>" form the user and role keys use,
// which is what lets loadPoliciesForPrincipal read it with the same code.
func iamGroupPoliciesKey(groupName string) string { return "group_policies:" + groupName }

// loadGroup returns the stored group, or nil when it does not exist.
func (p *IAMPlugin) loadGroup(goCtx context.Context, name string) (*IAMGroup, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, "group:"+name)
	if err != nil {
		return nil, fmt.Errorf("load group %s: %w", name, err)
	}
	if raw == nil {
		return nil, nil
	}
	var g IAMGroup
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, fmt.Errorf("unmarshal group %s: %w", name, err)
	}
	return &g, nil
}

// requireGroupAndUser returns a NoSuchEntity response when either entity is
// missing, or (nil, nil) when both exist.
//
// The group is checked first because it is the operation's first parameter, so a
// call naming two unknown entities reports the one a caller reads first.
func (p *IAMPlugin) requireGroupAndUser(goCtx context.Context, groupName, userName string) (*AWSResponse, error) {
	group, err := p.loadGroup(goCtx, groupName)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The group with name %s cannot be found.", groupName),
			http.StatusNotFound), nil
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
	return nil, nil
}

// addGroupMembership records the membership on both sides of the index. It is
// idempotent: adding a user who is already a member changes nothing.
//
// Both writes live here, and nowhere else, so the two directions cannot disagree.
// A membership visible to GetGroup but not to ListGroupsForUser — or worse, one
// that grants the group's policies through CheckAccess while the API reports the
// user as not a member — would be a state invariant broken by a missing line.
func (p *IAMPlugin) addGroupMembership(goCtx context.Context, groupName, userName string) error {
	if err := p.addToSortedList(goCtx, iamGroupUsersKey(groupName), userName); err != nil {
		return err
	}
	return p.addToSortedList(goCtx, iamUserGroupsKey(userName), groupName)
}

// removeGroupMembership drops the membership from both sides of the index. It is
// idempotent: removing a non-member changes nothing.
func (p *IAMPlugin) removeGroupMembership(goCtx context.Context, groupName, userName string) error {
	if err := p.removeFromList(goCtx, iamGroupUsersKey(groupName), userName); err != nil {
		return err
	}
	return p.removeFromList(goCtx, iamUserGroupsKey(userName), groupName)
}

// addToSortedList adds value to the string list at key, keeping it sorted and
// free of duplicates. Sorting is what makes the paginated reads stable.
func (p *IAMPlugin) addToSortedList(goCtx context.Context, key, value string) error {
	list, err := p.loadStringList(goCtx, key)
	if err != nil {
		return err
	}
	for _, v := range list {
		if v == value {
			return nil
		}
	}
	list = append(list, value)
	sort.Strings(list)
	return p.saveStringList(goCtx, key, list)
}

// removeFromList drops value from the string list at key.
func (p *IAMPlugin) removeFromList(goCtx context.Context, key, value string) error {
	list, err := p.loadStringList(goCtx, key)
	if err != nil {
		return err
	}
	out := list[:0]
	found := false
	for _, v := range list {
		if v == value {
			found = true
			continue
		}
		out = append(out, v)
	}
	if !found {
		return nil
	}
	return p.saveStringList(goCtx, key, out)
}
