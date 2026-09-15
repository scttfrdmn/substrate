package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// GetAccountAuthorizationDetails — the account-wide authorization snapshot (#848).
//
// Substrate answered `unknownActionError` for the one operation that reports every user,
// group, role and managed policy in an account together with the policies attached to each.
// The action was already authorizable — `authzref/iam.json` and `authz_reference_gen.go` both
// carry `iam:GetAccountAuthorizationDetails` with an empty resource list, meaning AWS supports
// no resource-level permissions for it — so a caller could be granted a permission for an
// operation substrate refused to perform.
//
// The absence mattered beyond one missing action. This is the only operation that reports all
// four entity shapes in a single body, so it is the only place a divergence between shapes is
// directly observable: `AttachmentCount` here has to agree with `GetPolicy`'s and
// `ListPolicies`', `PermissionsBoundaryUsageCount` likewise, and a role's `RoleLastUsed` has to
// agree with `GetRole`'s. Every one of those three was a defect in an earlier release (#847,
// #815, #816) precisely because no operation put them side by side. Nothing new is stored for
// any of it: the four values are derived from the same state the single-entity operations read,
// through the same helpers, which is what makes the agreement structural rather than tested.
//
// Three decisions this operation forces, each recorded here because AWS's page does not settle
// it:
//
//   - **`MaxItems` counts across the four lists combined**, not per list. The page says only
//     "the maximum number of items to return", and substrate pages one ordered key space whose
//     members are spelled `"group/<name>"`, `"policy/<arn>"`, `"role/<name>"` and
//     `"user/<name>"` so a `Marker` names exactly one item whichever list it came from — the
//     interleaving precedent is `ListPolicies`, which pages the catalog and state arms through
//     one ARN-ordered space. With 52 bundled managed policies against a default of 100,
//     truncation is on the *default* path here rather than an edge case, so an unambiguous
//     marker is load-bearing rather than tidy.
//
//   - **Policy documents follow the per-shape pages, not this operation's blanket note.** The
//     operation page claims every policy document it returns is "URL encoded compliant with RFC
//     3986" while *its own sample response renders all of them as plain JSON*. Only
//     `PolicyVersion.Document`'s own type page repeats the mandate; `PolicyDetail.PolicyDocument`
//     and `RoleDetail.AssumeRolePolicyDocument` carry no such sentence. So
//     `PolicyVersionList[].Document` goes through [iamPolicyVersionXML] and is percent-encoded,
//     byte-for-byte as `GetPolicyVersion` sends it, while the inline documents and the trust
//     policy are plain JSON, byte-for-byte as `GetUserPolicy`, `GetRolePolicy`, `GetGroupPolicy`
//     and `GetRole` send them. Encoding everything uniformly would be a breaking wire change to
//     five shipped operations in exchange for matching a sentence AWS's own example contradicts.
//     #848's fourth acceptance criterion asked for the opposite and is reversed for that reason.
//
//   - **An embedded role is re-read from state, never taken from the instance-profile record.**
//     `AddRoleToInstanceProfile` stores a copy of the role inside the profile, and STS writes
//     `RoleLastUsed` onto the role record, so the copy is frozen at attach time. AWS's sample
//     renders `<RoleLastUsed>` inside `RoleDetailList → InstanceProfileList → Roles → member`,
//     which is where reporting the snapshot would show `nil` for a role that has in fact been
//     assumed — see [IAMPlugin.iamInstanceProfilesByRole].
//
// Errors are `ServiceFailure`/500 only on the operation's own page, plus `CommonErrors` —
// which is where the `ValidationError` for a bad `Filter` or an out-of-range `MaxItems`
// (#868, [iamValidateMaxItems]) comes from.

// iamAuthorizationDetailFilters are the values entityType permits, from the operation's
// `Filter` parameter: "User | Role | Group | LocalManagedPolicy | AWSManagedPolicy".
//
// `Filter` is a list, so a caller may name several; an empty list means all four populations,
// which is what the page's "If it is not included, all attached entities (user, group, role,
// and policy) are returned" says. AWS's model also lists a sixth value, `SAMLProviderList`,
// which substrate does not offer because it models no SAML provider at all — naming it here
// would accept a filter that then selected nothing, and reporting an empty list for a
// population substrate cannot hold is less honest than refusing the filter.
var iamAuthorizationDetailFilters = []string{"User", "Role", "Group", "LocalManagedPolicy", "AWSManagedPolicy"}

// Composite key prefixes for the single ordered space the four populations are paged through.
//
// They sort `group/` < `policy/` < `role/` < `user/`, which is arbitrary but stable, and that
// is the whole requirement: a `Marker` is a position in one order, so the order only has to be
// the same on every call. The trailing slash cannot appear in an entity name (IAM names exclude
// "/") so a key decodes back to exactly one population.
const (
	iamDetailKindUser   = "user/"
	iamDetailKindGroup  = "group/"
	iamDetailKindRole   = "role/"
	iamDetailKindPolicy = "policy/"
)

// getAccountAuthorizationDetails reports every user, group, role and managed policy in the
// account, with the policies attached to each.
func (p *IAMPlugin) getAccountAuthorizationDetails(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		Filter   []string `json:"Filter"`
		Marker   string   `json:"Marker"`
		MaxItems iamInt   `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		// parseIAMBody has already named the parameter, so the wrapped message is final.
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	// Filter is a query-protocol list, so over the wire it arrives as Filter.member.N and never
	// as the JSON array the struct tag decodes; the tag serves a hand-marshaled body only. Same
	// shape as UntagPolicy's TagKeys — see iam_query.go (#639).
	if filter := iamMemberList(req.Params, "Filter"); filter != nil {
		params.Filter = filter
	}
	for _, f := range params.Filter {
		if !slices.Contains(iamAuthorizationDetailFilters, f) {
			return iamErrorResponse("ValidationError",
				iamEnumMessage("filter", f, iamAuthorizationDetailFilters),
				http.StatusBadRequest), nil
		}
	}

	if errResp := iamValidateMaxItems(req, params.MaxItems); errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:GetAccountAuthorizationDetails", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	details, err := p.iamAuthorizationDetails(goCtx, ctx.AccountID, params.Filter)
	if err != nil {
		return nil, err
	}

	page, nextMarker, isTruncated := paginateIAMKeys(details.keys, params.Marker, params.MaxItems.Int())

	body, err := details.render(page)
	if err != nil {
		return nil, err
	}
	body += "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		body += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "GetAccountAuthorizationDetails", body)
}

// --- Assembled shapes -------------------------------------------------------

// iamInlinePolicy is one member of a `UserPolicyList`, `GroupPolicyList` or `RolePolicyList`
// — AWS's `PolicyDetail`, whose only two members are `PolicyName` and `PolicyDocument`.
//
// The document is the stored bytes rather than a decoded [PolicyDocument], so this operation
// reports exactly what `GetUserPolicy` reports for the same policy. Decoding and re-marshaling
// would normalize it — the round trip that already makes a trust policy come back in an
// equivalent-but-different form (see [iamAssumeRolePolicyDocumentXML]) — and the whole point of
// this operation is that a shape cannot disagree with the operation that reports it singly.
type iamInlinePolicy struct {
	// name is the policy's name, as PutUserPolicy and its siblings were given it.
	name string

	// document is the policy document exactly as stored.
	document []byte
}

// iamUserDetail is one member of `UserDetailList`.
//
// Ten members, and `PasswordLastUsed` is not among them even though `User` has it — which is
// why [iamUserIdentityXML] exists separately from [iamUserXMLFields].
type iamUserDetail struct {
	user     *IAMUser
	groups   []string
	inline   []iamInlinePolicy
	attached []IAMAttachedPolicy
}

// iamGroupDetail is one member of `GroupDetailList`.
//
// Seven members, and neither `Tags` nor `PermissionsBoundary` is among them: a group has no
// boundary in AWS's model at all, and substrate stores no tags on one ([IAMGroup] has no field
// for them, because IAM does not let a group be tagged).
type iamGroupDetail struct {
	group    *IAMGroup
	inline   []iamInlinePolicy
	attached []IAMAttachedPolicy
}

// iamRoleDetail is one member of `RoleDetailList`.
//
// Twelve members, and neither `Description` nor `MaxSessionDuration` is among them — see
// [iamRoleIdentityXML]. The roles nested inside instanceProfiles are `Role` and not
// `RoleDetail`, so both members *are* admissible there.
type iamRoleDetail struct {
	role             *IAMRole
	inline           []iamInlinePolicy
	attached         []IAMAttachedPolicy
	instanceProfiles []IAMInstanceProfile
}

// iamManagedPolicyDetail is one member of `Policies`.
//
// Twelve members, `Tags` not among them: a policy's tags exist in substrate and are read
// through `ListPolicyTags`, because `ManagedPolicyDetail` has no member to put them in.
type iamManagedPolicyDetail struct {
	// policy is a copy, with AttachmentCount set from state — never the catalog's shared
	// pointer, which writing through would leak a count into GetPolicy and every later
	// listing (the reason `ListPolicies` copies too).
	policy *IAMPolicy

	// boundaryUsage is this policy's PermissionsBoundaryUsageCount.
	boundaryUsage int
}

// iamAuthorizationDetails is one request's assembled snapshot, keyed by the composite keys
// pagination orders.
//
// Everything is assembled before a page is cut rather than after, because the page boundary
// cannot be known until the whole key space is: `MaxItems` counts across the four populations
// combined, so which users fall on page 1 depends on how many groups and policies precede them.
// The cost is bounded by the account's own size, and the alternative — paging each population
// separately — is the ambiguous `Marker` this design exists to avoid.
type iamAuthorizationDetails struct {
	keys     []string
	users    map[string]*iamUserDetail
	groups   map[string]*iamGroupDetail
	roles    map[string]*iamRoleDetail
	policies map[string]*iamManagedPolicyDetail
}

// render encodes the four detail lists for the keys on one page, in the order AWS's sample
// response uses.
//
// A list is emitted even when the page holds none of its members, which is what AWS's sample
// does for a population an account is empty of, and what lets a consumer decode the response
// without branching on which lists happen to be present.
func (d *iamAuthorizationDetails) render(page []string) (string, error) {
	var users, groups, roles, policies strings.Builder
	for _, key := range page {
		switch {
		case strings.HasPrefix(key, iamDetailKindUser):
			users.WriteString(iamUserDetailXML(d.users[key]))
		case strings.HasPrefix(key, iamDetailKindGroup):
			groups.WriteString(iamGroupDetailXML(d.groups[key]))
		case strings.HasPrefix(key, iamDetailKindRole):
			roles.WriteString(iamRoleDetailXML(d.roles[key]))
		case strings.HasPrefix(key, iamDetailKindPolicy):
			member, err := iamManagedPolicyDetailXML(d.policies[key])
			if err != nil {
				return "", err
			}
			policies.WriteString(member)
		}
	}
	return "<UserDetailList>" + users.String() + "</UserDetailList>" +
		"<GroupDetailList>" + groups.String() + "</GroupDetailList>" +
		"<RoleDetailList>" + roles.String() + "</RoleDetailList>" +
		"<Policies>" + policies.String() + "</Policies>", nil
}

// --- Assembly ---------------------------------------------------------------

// iamAuthorizationDetails assembles every population the filter selects.
//
// An empty filter selects all four, per the page's "If it is not included, all attached
// entities (user, group, role, and policy) are returned".
func (p *IAMPlugin) iamAuthorizationDetails(goCtx context.Context, accountID string, filter []string) (*iamAuthorizationDetails, error) {
	all := len(filter) == 0
	details := &iamAuthorizationDetails{
		users:    make(map[string]*iamUserDetail),
		groups:   make(map[string]*iamGroupDetail),
		roles:    make(map[string]*iamRoleDetail),
		policies: make(map[string]*iamManagedPolicyDetail),
	}

	if all || slices.Contains(filter, "User") {
		if err := p.iamAppendUserDetails(goCtx, accountID, details); err != nil {
			return nil, err
		}
	}
	if all || slices.Contains(filter, "Group") {
		if err := p.iamAppendGroupDetails(goCtx, accountID, details); err != nil {
			return nil, err
		}
	}
	if all || slices.Contains(filter, "Role") {
		if err := p.iamAppendRoleDetails(goCtx, accountID, details); err != nil {
			return nil, err
		}
	}
	if scope, want := iamAuthorizationDetailPolicyScope(filter); want {
		if err := p.iamAppendPolicyDetails(goCtx, accountID, scope, details); err != nil {
			return nil, err
		}
	}
	return details, nil
}

// iamAuthorizationDetailPolicyScope maps the two policy filter values onto the `Scope` this
// operation's policy population shares with `ListPolicies`, reporting whether any policy is
// wanted at all.
//
// Naming both values is `All`, naming neither with a non-empty filter is none, and an empty
// filter is `All` — so the same [IAMPlugin.iamPolicyCandidates] serves both operations and a
// policy cannot appear in one and not the other.
func iamAuthorizationDetailPolicyScope(filter []string) (scope string, want bool) {
	if len(filter) == 0 {
		return "All", true
	}
	local := slices.Contains(filter, "LocalManagedPolicy")
	aws := slices.Contains(filter, "AWSManagedPolicy")
	switch {
	case local && aws:
		return "All", true
	case local:
		return "Local", true
	case aws:
		return "AWS", true
	}
	return "", false
}

// iamAppendUserDetails loads every user in the account with its groups, inline policies and
// attached managed policies.
func (p *IAMPlugin) iamAppendUserDetails(goCtx context.Context, accountID string, details *iamAuthorizationDetails) error {
	keys, err := p.state.List(goCtx, iamNamespace, iamUserPrefix(accountID))
	if err != nil {
		return fmt.Errorf("list users: %w", err)
	}
	for _, key := range keys {
		name := strings.TrimPrefix(key, iamUserPrefix(accountID))
		user, err := p.loadUser(goCtx, accountID, name)
		if err != nil {
			return err
		}
		if user == nil {
			continue
		}
		groups, err := p.loadStringList(goCtx, iamUserGroupsKey(accountID, name))
		if err != nil {
			return err
		}
		inline, err := p.iamInlinePolicies(goCtx, accountID, "user", name)
		if err != nil {
			return err
		}
		attached, err := p.iamAttachedManagedPolicies(goCtx, accountID, "user", name)
		if err != nil {
			return err
		}
		detailKey := iamDetailKindUser + name
		details.users[detailKey] = &iamUserDetail{user: user, groups: groups, inline: inline, attached: attached}
		details.keys = append(details.keys, detailKey)
	}
	return nil
}

// iamAppendGroupDetails loads every group in the account with its inline and attached policies.
func (p *IAMPlugin) iamAppendGroupDetails(goCtx context.Context, accountID string, details *iamAuthorizationDetails) error {
	keys, err := p.state.List(goCtx, iamNamespace, iamGroupPrefix(accountID))
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}
	for _, key := range keys {
		name := strings.TrimPrefix(key, iamGroupPrefix(accountID))
		group, err := p.loadGroup(goCtx, accountID, name)
		if err != nil {
			return err
		}
		if group == nil {
			continue
		}
		inline, err := p.iamInlinePolicies(goCtx, accountID, "group", name)
		if err != nil {
			return err
		}
		attached, err := p.iamAttachedManagedPolicies(goCtx, accountID, "group", name)
		if err != nil {
			return err
		}
		detailKey := iamDetailKindGroup + name
		details.groups[detailKey] = &iamGroupDetail{group: group, inline: inline, attached: attached}
		details.keys = append(details.keys, detailKey)
	}
	return nil
}

// iamAppendRoleDetails loads every role in the account with its inline policies, attached
// managed policies and containing instance profiles.
func (p *IAMPlugin) iamAppendRoleDetails(goCtx context.Context, accountID string, details *iamAuthorizationDetails) error {
	keys, err := p.state.List(goCtx, iamNamespace, iamRolePrefix(accountID))
	if err != nil {
		return fmt.Errorf("list roles: %w", err)
	}
	// One scan of the instance profiles for the whole request, not one per role: the profiles
	// are the same records whichever role is being reported, so scanning per role would make
	// the operation O(roles × profiles).
	profilesByRole, err := p.iamInstanceProfilesByRole(goCtx, accountID)
	if err != nil {
		return err
	}
	for _, key := range keys {
		name := strings.TrimPrefix(key, iamRolePrefix(accountID))
		role, err := p.loadRole(goCtx, accountID, name)
		if err != nil {
			return err
		}
		if role == nil {
			continue
		}
		inline, err := p.iamInlinePolicies(goCtx, accountID, "role", name)
		if err != nil {
			return err
		}
		attached, err := p.iamAttachedManagedPolicies(goCtx, accountID, "role", name)
		if err != nil {
			return err
		}
		detailKey := iamDetailKindRole + name
		details.roles[detailKey] = &iamRoleDetail{
			role:             role,
			inline:           inline,
			attached:         attached,
			instanceProfiles: profilesByRole[name],
		}
		details.keys = append(details.keys, detailKey)
	}
	return nil
}

// iamAppendPolicyDetails loads the managed policies in scope with their derived counts.
//
// The two counts are hoisted to one scan each for the whole request, for the reason
// `ListPolicies` hoists them: counting per policy would re-walk every attachment list and every
// user and role once per member, making the operation O(policies × entities) — and with 52
// bundled policies that is not a hypothetical cost.
func (p *IAMPlugin) iamAppendPolicyDetails(goCtx context.Context, accountID, scope string, details *iamAuthorizationDetails) error {
	candidates, err := p.iamPolicyCandidates(goCtx, accountID, scope)
	if err != nil {
		return err
	}
	attachments, err := p.iamPolicyAttachmentCounts(goCtx, accountID)
	if err != nil {
		return err
	}
	boundaryUsage, err := p.iamBoundaryUsageCounts(goCtx, accountID)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		// Copied before AttachmentCount is assigned, because ListManagedPolicies hands back
		// the catalog's shared pointers (`ListPolicies` copies for the same reason).
		listed := *candidate
		listed.AttachmentCount = attachments[listed.ARN]
		detailKey := iamDetailKindPolicy + listed.ARN
		details.policies[detailKey] = &iamManagedPolicyDetail{
			policy:        &listed,
			boundaryUsage: boundaryUsage[listed.ARN],
		}
		details.keys = append(details.keys, detailKey)
	}
	return nil
}

// iamInlinePolicies returns one entity's inline policies with their stored documents.
//
// A name whose document is missing is skipped rather than reported with an empty document: the
// names list and the documents are separate state keys, so a record written directly could hold
// one without the other, and an empty `PolicyDocument` would claim the entity has a policy that
// permits nothing.
func (p *IAMPlugin) iamInlinePolicies(goCtx context.Context, accountID, kind, entityName string) ([]iamInlinePolicy, error) {
	names, err := p.loadInlinePolicyNames(goCtx, accountID, kind, entityName)
	if err != nil {
		return nil, err
	}
	policies := make([]iamInlinePolicy, 0, len(names))
	for _, name := range names {
		raw, err := p.state.Get(goCtx, iamNamespace, iamInlinePolicyKey(accountID, kind, entityName, name))
		if err != nil {
			return nil, fmt.Errorf("get inline policy %s/%s/%s: %w", kind, entityName, name, err)
		}
		if raw == nil {
			continue
		}
		policies = append(policies, iamInlinePolicy{name: name, document: raw})
	}
	return policies, nil
}

// iamAttachedManagedPolicies returns one entity's attached managed policies, name and ARN.
//
// The name comes from the ARN rather than from the policy record, exactly as the four
// `ListAttached*Policies` operations derive it, so an ARN naming a policy substrate does not
// bundle still reports a usable name instead of an empty one.
func (p *IAMPlugin) iamAttachedManagedPolicies(goCtx context.Context, accountID, kind, name string) ([]IAMAttachedPolicy, error) {
	arns, err := p.loadPolicyList(goCtx, iamAttachedPoliciesKey(accountID, kind, name))
	if err != nil {
		return nil, err
	}
	policies := make([]IAMAttachedPolicy, 0, len(arns))
	for _, arn := range arns {
		policies = append(policies, IAMAttachedPolicy{PolicyName: arnPolicyName(arn), PolicyARN: arn})
	}
	return policies, nil
}

// iamInstanceProfilesByRole returns the account's instance profiles indexed by the name of
// each role they contain, with every embedded role re-read from its own record.
//
// The re-read is the point. `AddRoleToInstanceProfile` stores a copy of the role inside the
// profile, and nothing updates that copy afterwards — STS writes `RoleLastUsed` onto the role
// record, `UpdateRole` writes `Description` and `MaxSessionDuration` there, and
// `UpdateAssumeRolePolicy` writes the trust policy there. AWS's sample response for this
// operation renders `<RoleLastUsed>` inside `InstanceProfileList → Roles → member`, so
// rendering the stored copy would report no last use for a role that has been assumed, and a
// stale trust policy for one whose policy was updated after the profile was created. A test
// that attaches a profile and immediately reads it back cannot see the difference, which is why
// the behavioral test assumes the role first.
//
// A role the profile names but state no longer holds keeps the embedded copy, since that is the
// only record of it left and dropping the member would report a profile with no roles.
func (p *IAMPlugin) iamInstanceProfilesByRole(goCtx context.Context, accountID string) (map[string][]IAMInstanceProfile, error) {
	keys, err := p.state.List(goCtx, iamNamespace, iamInstanceProfilePrefix(accountID))
	if err != nil {
		return nil, fmt.Errorf("list instance profiles: %w", err)
	}
	byRole := make(map[string][]IAMInstanceProfile)
	for _, key := range keys {
		raw, err := p.state.Get(goCtx, iamNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("get instance profile %s: %w", key, err)
		}
		if raw == nil {
			continue
		}
		var profile IAMInstanceProfile
		if err := json.Unmarshal(raw, &profile); err != nil {
			// A record that does not decode is skipped rather than failing the whole
			// snapshot, which is how `ListPolicies` treats an undecodable policy: one
			// corrupt key should not make an account's authorization unreadable.
			continue
		}
		// Every embedded role is refreshed before the profile is indexed, not as each is
		// reached: a profile holding two roles is reported under both names, and indexing
		// mid-refresh would give the first name a copy in which the second role is still
		// stale.
		for i := range profile.Roles {
			current, err := p.loadRole(goCtx, accountID, profile.Roles[i].RoleName)
			if err != nil {
				return nil, err
			}
			if current != nil {
				profile.Roles[i] = *current
			}
		}
		for i := range profile.Roles {
			name := profile.Roles[i].RoleName
			byRole[name] = append(byRole[name], profile)
		}
	}
	return byRole, nil
}

// --- Encoders ---------------------------------------------------------------

// iamInlinePolicyListXML builds a named list of `PolicyDetail` members.
//
// The wrapper is a parameter because AWS spells one shape three ways —
// `UserPolicyList`, `GroupPolicyList`, `RolePolicyList` — following [iamStringListXML].
//
// The document is written as stored, not percent-encoded: `PolicyDetail.PolicyDocument`'s type
// page carries no RFC 3986 sentence, and `GetUserPolicy` already returns these same bytes
// plain, so encoding here would make one document arrive two ways depending on which operation
// asked for it. See this file's preamble for the contradiction in AWS's own page.
func iamInlinePolicyListXML(wrapper string, policies []iamInlinePolicy) string {
	var b strings.Builder
	b.WriteString("<")
	b.WriteString(wrapper)
	b.WriteString(">")
	for _, policy := range policies {
		b.WriteString("<member><PolicyName>")
		b.WriteString(xmlEsc(policy.name))
		b.WriteString("</PolicyName><PolicyDocument>")
		b.WriteString(xmlEsc(string(policy.document)))
		b.WriteString("</PolicyDocument></member>")
	}
	b.WriteString("</")
	b.WriteString(wrapper)
	b.WriteString(">")
	return b.String()
}

// iamUserDetailXML renders one `UserDetail` member.
func iamUserDetailXML(d *iamUserDetail) string {
	if d == nil {
		return ""
	}
	return "<member>" + iamUserIdentityXML(d.user) +
		iamInlinePolicyListXML("UserPolicyList", d.inline) +
		iamStringListXML("GroupList", d.groups) +
		iamAttachedPoliciesXML("AttachedManagedPolicies", d.attached) +
		iamPermissionsBoundaryXML(d.user.PermissionsBoundary) +
		iamEntityTagsXML(d.user.Tags) +
		"</member>"
}

// iamGroupDetailXML renders one `GroupDetail` member.
func iamGroupDetailXML(d *iamGroupDetail) string {
	if d == nil {
		return ""
	}
	return "<member>" + iamGroupXMLFields(d.group) +
		iamInlinePolicyListXML("GroupPolicyList", d.inline) +
		iamAttachedPoliciesXML("AttachedManagedPolicies", d.attached) +
		"</member>"
}

// iamRoleDetailXML renders one `RoleDetail` member.
//
// `RoleLastUsed` is rendered here and not by [iamRoleListXML], because `RoleLastUsed`'s own
// type page names `GetRole` and this operation as the two that return it — see
// [iamRoleLastUsedXML].
func iamRoleDetailXML(d *iamRoleDetail) string {
	if d == nil {
		return ""
	}
	return "<member>" + iamRoleIdentityXML(d.role) +
		iamAssumeRolePolicyDocumentXML(d.role) +
		iamInstanceProfileListXML("InstanceProfileList", d.instanceProfiles, true) +
		iamInlinePolicyListXML("RolePolicyList", d.inline) +
		iamAttachedPoliciesXML("AttachedManagedPolicies", d.attached) +
		iamPermissionsBoundaryXML(d.role.PermissionsBoundary) +
		iamEntityTagsXML(d.role.Tags) +
		iamRoleLastUsedXML(d.role.RoleLastUsed) +
		"</member>"
}

// iamManagedPolicyDetailXML renders one `ManagedPolicyDetail` member.
//
// `Description` is rendered here even though `ListPolicies` omits it, because
// `ManagedPolicyDetail` lists the member and this operation's sample response carries it: the
// carve-out AWS documents is on `Policy`, the listing shape, not on this one.
//
// `PolicyVersionList` holds the one version substrate stores, with its document
// percent-encoded through [iamPolicyVersionXML] — the single place in this operation's response
// where a document is not plain JSON, because `PolicyVersion.Document` is the single shape
// whose own type page mandates RFC 3986.
func iamManagedPolicyDetailXML(d *iamManagedPolicyDetail) (string, error) {
	if d == nil {
		return "", nil
	}
	version, err := iamPolicyVersionXML(iamPolicyDefaultVersion(d.policy), true)
	if err != nil {
		return "", err
	}
	return "<member>" + iamPolicyXMLFields(d.policy, d.boundaryUsage) +
		iamPolicyDescriptionXML(d.policy.Description) +
		"<PolicyVersionList><member>" + version + "</member></PolicyVersionList>" +
		"</member>", nil
}
