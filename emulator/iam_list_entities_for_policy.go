package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// iamEntityFilters are ListEntitiesForPolicy's EntityFilter values.
var iamEntityFilters = []string{"User", "Role", "Group", "LocalManagedPolicy", "AWSManagedPolicy"}

// iamPolicyEntity is one user, group or role ListEntitiesForPolicy reports.
type iamPolicyEntity struct {
	kind, name, id, path string
}

// listEntitiesForPolicy reports the users, groups and roles a managed policy is attached to — and,
// unless PolicyUsageFilter says otherwise, the users and roles using it as a permissions boundary.
//
// The attachments are [IAMPlugin.iamPolicyAttachedEntities]', the reading DeletePolicy's refusal and
// AttachmentCount share. EntityFilter's User, Role and Group narrow the kinds, as the page says. Its
// LocalManagedPolicy and AWSManagedPolicy values have no published meaning for this operation;
// substrate's reading is that they select by the policy's own kind, so naming the other kind reports
// nothing and naming the policy's kind reports every entity. PathPrefix is applied to each entity's path before the page is cut.
//
// The three kinds are paged through one ordered key space — `group/<name>`, `role/<name>`,
// `user/<name>` — the way GetAccountAuthorizationDetails pages its four populations, so one Marker
// resumes across all of them.
func (p *IAMPlugin) listEntitiesForPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn         string `json:"PolicyArn"`
		EntityFilter      string `json:"EntityFilter"`
		PathPrefix        string `json:"PathPrefix"`
		PolicyUsageFilter string `json:"PolicyUsageFilter"`
		Marker            string `json:"Marker"`
		MaxItems          iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}
	if params.EntityFilter != "" && !slices.Contains(iamEntityFilters, params.EntityFilter) {
		return iamErrorResponse("ValidationError",
			iamEnumMessage("entityFilter", params.EntityFilter, iamEntityFilters), http.StatusBadRequest), nil
	}
	if params.PolicyUsageFilter != "" && !slices.Contains(iamPolicyUsageFilters, params.PolicyUsageFilter) {
		return iamErrorResponse("ValidationError",
			iamEnumMessage("policyUsageFilter", params.PolicyUsageFilter, iamPolicyUsageFilters), http.StatusBadRequest), nil
	}
	if params.PathPrefix == "" {
		params.PathPrefix = "/"
	}
	if !iamValidPath(params.PathPrefix) {
		return iamErrorResponse("ValidationError",
			fmt.Sprintf("PathPrefix %q must be \"/\" or begin and end with \"/\".", params.PathPrefix),
			http.StatusBadRequest), nil
	}
	pg, errResp := iamMarker(req, params.MaxItems, params.Marker)
	if errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListEntitiesForPolicy", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if _, refusal, err := p.resolveManagedPolicy(goCtx, params.PolicyArn); err != nil || refusal != nil {
		return refusal, err
	}

	entities, err := p.policyEntities(goCtx, ctx.AccountID, params.PolicyArn, params.EntityFilter, params.PolicyUsageFilter)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]iamPolicyEntity, len(entities))
	keys := make([]string, 0, len(entities))
	for _, e := range entities {
		if !strings.HasPrefix(e.path, params.PathPrefix) {
			continue
		}
		key := e.kind + "/" + e.name
		if _, dup := byKey[key]; dup {
			continue
		}
		byKey[key] = e
		keys = append(keys, key)
	}
	slices.Sort(keys)
	page, nextMarker, isTruncated := paginateIAMKeys(keys, pg)

	var groups, users, roles strings.Builder
	for _, key := range page {
		e := byKey[key]
		switch e.kind {
		case "group":
			groups.WriteString("<member><GroupName>" + xmlEsc(e.name) + "</GroupName><GroupId>" + xmlEsc(e.id) + "</GroupId></member>")
		case "role":
			roles.WriteString("<member><RoleName>" + xmlEsc(e.name) + "</RoleName><RoleId>" + xmlEsc(e.id) + "</RoleId></member>")
		default:
			users.WriteString("<member><UserName>" + xmlEsc(e.name) + "</UserName><UserId>" + xmlEsc(e.id) + "</UserId></member>")
		}
	}
	body := "<PolicyGroups>" + groups.String() + "</PolicyGroups><PolicyUsers>" + users.String() +
		"</PolicyUsers><PolicyRoles>" + roles.String() + "</PolicyRoles><IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		body += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListEntitiesForPolicy", body)
}

// policyEntities returns the entities arn is attached to and those using it as a boundary,
// narrowed by entityFilter and usageFilter, each with the ID and path its record holds.
func (p *IAMPlugin) policyEntities(goCtx context.Context, accountID, arn, entityFilter, usageFilter string,
) ([]iamPolicyEntity, error) {
	awsManaged := strings.HasPrefix(arn, "arn:aws:iam::aws:")
	if (entityFilter == "LocalManagedPolicy" && awsManaged) || (entityFilter == "AWSManagedPolicy" && !awsManaged) {
		return nil, nil
	}
	wantKind := func(kind string) bool {
		switch entityFilter {
		case "User", "Role", "Group":
			return strings.EqualFold(entityFilter, kind)
		}
		return true
	}

	var out []iamPolicyEntity
	if usageFilter != "PermissionsBoundary" {
		attached, err := p.iamPolicyAttachedEntities(goCtx, accountID, arn)
		if err != nil {
			return nil, err
		}
		for _, kind := range []struct {
			name  string
			names []string
		}{{"user", attached.Users}, {"group", attached.Groups}, {"role", attached.Roles}} {
			if !wantKind(kind.name) {
				continue
			}
			for _, name := range kind.names {
				e, ok, err := p.policyEntityRecord(goCtx, accountID, kind.name, name)
				if err != nil {
					return nil, err
				}
				if ok {
					out = append(out, e)
				}
			}
		}
	}
	if usageFilter != "PermissionsPolicy" {
		// A group cannot have a permissions boundary, so only users and roles are scanned.
		for _, kind := range []string{"user", "role"} {
			if !wantKind(kind) {
				continue
			}
			prefix := iamStatePrefix(kind, accountID)
			keys, err := p.state.List(goCtx, iamNamespace, prefix)
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", prefix, err)
			}
			for _, key := range keys {
				raw, err := p.state.Get(goCtx, iamNamespace, key)
				if err != nil {
					return nil, fmt.Errorf("get %s: %w", key, err)
				}
				if raw == nil {
					continue
				}
				var rec struct {
					PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary"`
				}
				if err := json.Unmarshal(raw, &rec); err != nil {
					return nil, fmt.Errorf("unmarshal %s: %w", key, err)
				}
				if rec.PermissionsBoundary == nil || rec.PermissionsBoundary.PolicyARN != arn {
					continue
				}
				e, ok, err := p.policyEntityRecord(goCtx, accountID, kind, strings.TrimPrefix(key, prefix))
				if err != nil {
					return nil, err
				}
				if ok {
					out = append(out, e)
				}
			}
		}
	}
	return out, nil
}

// policyEntityRecord reads the ID and path of the named user, group or role. An attachment list
// naming an entity that no longer exists is skipped, as ListGroupsForUser skips a vanished group.
func (p *IAMPlugin) policyEntityRecord(goCtx context.Context, accountID, kind, name string,
) (iamPolicyEntity, bool, error) {
	raw, err := p.state.Get(goCtx, iamNamespace, iamEntityKey(accountID, kind, name))
	if err != nil {
		return iamPolicyEntity{}, false, fmt.Errorf("get %s %s: %w", kind, name, err)
	}
	if raw == nil {
		return iamPolicyEntity{}, false, nil
	}
	var rec struct {
		UserID  string `json:"UserId"`
		GroupID string `json:"GroupId"`
		RoleID  string `json:"RoleId"`
		Path    string `json:"Path"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return iamPolicyEntity{}, false, fmt.Errorf("unmarshal %s %s: %w", kind, name, err)
	}
	return iamPolicyEntity{kind: kind, name: name, id: rec.UserID + rec.GroupID + rec.RoleID,
		path: normalisePath(rec.Path)}, true, nil
}
