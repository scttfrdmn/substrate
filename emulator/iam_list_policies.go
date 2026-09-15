package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

// ListPolicies and its filters (#497).
//
// The operation parsed Scope and PathPrefix and applied neither, and it enumerated only
// policies created through CreatePolicy — so the 52 bundled AWS managed policies were
// invisible to every listing. `--scope AWS` returned whatever the caller happened to have
// created and `--path-prefix /service-role/` returned the same thing. That broke the
// pairing a reader expects: an ARN GetPolicy resolves did not appear in any listing, so a
// consumer discovering a policy rather than hardcoding its ARN had no testable path.
//
// Substrate can only ever return the 52 policies it bundles where real IAM returns roughly
// 1,200. That is a documented scope limit (see docs/services.md); returning *zero* was the
// bug.

// iamPathPrefixPattern is policyPathType from the API model:
// "((/[A-Za-z0-9\.,\+@=_-]+)*)/".
//
// A prefix is required to begin and end with a slash, which is worth enforcing rather than
// treating as advisory: "/service-role" without the trailing slash is the natural typo, and
// AWS refuses it, so accepting it here would let a consumer write a call that works against
// substrate and fails against IAM.
var iamPathPrefixPattern = regexp.MustCompile(`^((/[A-Za-z0-9.,+@=_-]+)*)/$`)

// iamPolicyScopes are the values policyScopeType permits.
var iamPolicyScopes = []string{"All", "AWS", "Local"}

// iamPolicyUsageFilters are the values PolicyUsageType permits.
var iamPolicyUsageFilters = []string{"PermissionsPolicy", "PermissionsBoundary"}

// listPolicies lists managed policies, narrowed by Scope, PathPrefix and OnlyAttached.
func (p *IAMPlugin) listPolicies(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		Scope             string  `json:"Scope"`
		PathPrefix        string  `json:"PathPrefix"`
		OnlyAttached      iamBool `json:"OnlyAttached"`
		PolicyUsageFilter string  `json:"PolicyUsageFilter"`
		Marker            string  `json:"Marker"`
		MaxItems          iamInt  `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		// parseIAMBody has already named the parameter, so the wrapped message is final.
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	scope := params.Scope
	if scope == "" {
		scope = "All"
	}
	if !slices.Contains(iamPolicyScopes, scope) {
		return iamErrorResponse("ValidationError", iamEnumMessage("scope", scope, iamPolicyScopes),
			http.StatusBadRequest), nil
	}
	// PolicyUsageFilter is validated and then not applied, which is deliberate and stated in
	// docs/services.md rather than left for a reader to discover. The reference says only
	// that PermissionsPolicy lists "permissions policies" and PermissionsBoundary lists "the
	// policies used to set permissions boundaries"; it does not say which side an
	// entirely-unused policy falls on, and every bundled policy is unused in a fresh
	// substrate. Guessing "unattached means not a permissions policy" would silently drop all
	// 52 from a filtered listing — the same failure #497 reports, reintroduced under a
	// different parameter. Refusing a bad value is still worth doing: that is a caller error
	// either way.
	if params.PolicyUsageFilter != "" && !slices.Contains(iamPolicyUsageFilters, params.PolicyUsageFilter) {
		return iamErrorResponse("ValidationError",
			iamEnumMessage("policyUsageFilter", params.PolicyUsageFilter, iamPolicyUsageFilters),
			http.StatusBadRequest), nil
	}
	if params.PathPrefix != "" && !iamPathPrefixPattern.MatchString(params.PathPrefix) {
		return iamErrorResponse("ValidationError",
			fmt.Sprintf("The specified value for pathPrefix is invalid. "+
				"It must begin and end with / and contain only alphanumeric characters and/or "+
				"one of the following: ,.+@=_- (got %q).", params.PathPrefix),
			http.StatusBadRequest), nil
	}

	if errResp := iamValidateMaxItems(req, params.MaxItems); errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListPolicies", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	candidates, err := p.iamPolicyCandidates(goCtx, ctx.AccountID, scope)
	if err != nil {
		return nil, err
	}

	// The attachment counts are computed once for the whole listing rather than per policy,
	// because they come from walking the same state: one pass over the attachment lists
	// reaches every ARN. Counting per policy would re-read every list 52 times.
	attachments, err := p.iamPolicyAttachmentCounts(goCtx, ctx.AccountID)
	if err != nil {
		return nil, err
	}

	// PermissionsBoundaryUsageCount is hoisted for the same reason, and matters more here: the
	// scan is over users and roles, so counting per policy would repeat it once per member and
	// make the listing O(policies × entities) — the cost #815 names. One scan makes it
	// O(policies + entities).
	boundaryUsage, err := p.iamBoundaryUsageCounts(goCtx, ctx.AccountID)
	if err != nil {
		return nil, err
	}

	byARN := make(map[string]*IAMPolicy, len(candidates))
	arns := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if params.PathPrefix != "" && !strings.HasPrefix(candidate.Path, params.PathPrefix) {
			continue
		}
		count := attachments[candidate.ARN]
		if bool(params.OnlyAttached) && count == 0 {
			continue
		}

		// AttachmentCount is reported from state, not from the stored field. The catalog
		// carries 0 for every bundled policy and no attach operation increments a stored
		// count, so reading the field would leave OnlyAttached unable to ever return a
		// managed policy — the issue's fourth acceptance criterion. The copy is local so the
		// catalog's own value is not mutated: ListManagedPolicies hands back the shared
		// pointers, and writing through one would make a count leak into GetPolicy and into
		// every later listing.
		listed := *candidate
		listed.AttachmentCount = count
		byARN[listed.ARN] = &listed
		arns = append(arns, listed.ARN)
	}

	// Paginated by ARN rather than by state key, so the catalog and state arms interleave in
	// one stable order instead of the bundled policies landing on their own pages.
	page, nextMarker, isTruncated := paginateIAMKeys(arns, params.Marker, params.MaxItems.Int())

	policies := make([]*IAMPolicy, 0, len(page))
	for _, arn := range page {
		policies = append(policies, byARN[arn])
	}

	xmlStr := iamPolicyListXML(policies, boundaryUsage) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, "ListPolicies", xmlStr)
}

// iamPolicyCandidates returns the policies in scope, catalog before state.
//
// An ARN already seen is skipped, and that guard is load-bearing rather than defensive:
// pagination keys its marker on the ARN, so two members sharing one ARN would make a marker
// ambiguous and a page could repeat or skip. A bundled ARN is under "::aws:" and
// CreatePolicy builds a "::<account>:" one, so the two arms cannot collide through the API —
// only a state key written directly could.
func (p *IAMPlugin) iamPolicyCandidates(goCtx context.Context, accountID, scope string) ([]*IAMPolicy, error) {
	seen := make(map[string]bool)
	candidates := make([]*IAMPolicy, 0, len(ListManagedPolicies()))

	if scope == "All" || scope == "AWS" {
		for _, mp := range ListManagedPolicies() {
			if seen[mp.ARN] {
				continue
			}
			seen[mp.ARN] = true
			candidates = append(candidates, mp)
		}
	}
	if scope == "All" || scope == "Local" {
		keys, err := p.state.List(goCtx, iamNamespace, iamPolicyPrefix(accountID))
		if err != nil {
			return nil, fmt.Errorf("list policies: %w", err)
		}
		for _, k := range keys {
			raw, err := p.state.Get(goCtx, iamNamespace, k)
			if err != nil {
				return nil, fmt.Errorf("get policy %s: %w", k, err)
			}
			if raw == nil {
				continue
			}
			var pol IAMPolicy
			if err := json.Unmarshal(raw, &pol); err != nil {
				// A record that does not decode is skipped rather than failing the listing:
				// one corrupt key should not make every policy unlistable.
				continue
			}
			if seen[pol.ARN] {
				continue
			}
			seen[pol.ARN] = true
			candidates = append(candidates, &pol)
		}
	}
	return candidates, nil
}

// iamPolicyAttachmentCounts counts, for every policy ARN state mentions, how many users,
// groups and roles it is attached to.
//
// The count is derived rather than stored. AttachUserPolicy and its siblings record the ARN
// on the entity's list and never touch a count on the policy, and the bundled catalog is
// immutable — so a stored count would be zero for a bundled policy and could go stale for a
// created one. Deriving it means an attach and a detach are both immediately visible.
func (p *IAMPlugin) iamPolicyAttachmentCounts(goCtx context.Context, accountID string) (map[string]int, error) {
	counts := make(map[string]int)

	// Read from the attachment lists themselves rather than by enumerating entities:
	// "<kind>_policies:<name>" is where every attach writes, so three prefixes reach users,
	// groups and roles without loading a single entity record.
	for _, kind := range []string{"user", "group", "role"} {
		prefix := iamAttachedPoliciesPrefix(accountID, kind)
		keys, err := p.state.List(goCtx, iamNamespace, prefix)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, k := range keys {
			arns, err := p.loadPolicyList(goCtx, k)
			if err != nil {
				return nil, err
			}
			for _, arn := range arns {
				counts[arn]++
			}
		}
	}
	return counts, nil
}

// iamPolicyAttachments names the entities one managed policy is attached to, by kind.
type iamPolicyAttachments struct {
	Users  []string
	Groups []string
	Roles  []string
}

// Total reports how many entities the policy is attached to, across all three kinds.
func (a iamPolicyAttachments) Total() int {
	return len(a.Users) + len(a.Groups) + len(a.Roles)
}

// Describe names the attached entities, kind by kind, omitting a kind with none.
//
// This is the whole point of the type. API_DeletePolicy's DeleteConflict says "The error
// message describes these entities", and in substrate the message is the *only* way a caller
// can learn them: ListEntitiesForPolicy — the operation AWS's own description points the
// caller at — is not implemented, so there is no discovery call to fall back on. The three
// sibling refusals that name what to remove (DeleteUser's group memberships, DeleteRole's
// instance profiles, DeleteGroup's users) set the house style; the four that refuse over
// attached policies do not name anything, which is why this one is written rather than
// copied.
func (a iamPolicyAttachments) Describe() string {
	var parts []string
	for _, kind := range []struct {
		label string
		names []string
	}{
		{"Users", a.Users},
		{"Groups", a.Groups},
		{"Roles", a.Roles},
	} {
		if len(kind.names) > 0 {
			parts = append(parts, kind.label+": "+strings.Join(kind.names, ", "))
		}
	}
	return strings.Join(parts, ". ")
}

// iamPolicyAttachedEntities names the users, groups and roles arn is attached to.
//
// The same three "<kind>_policies:" prefixes and the same loadPolicyList as
// [IAMPlugin.iamPolicyAttachmentCounts], deliberately: DeletePolicy's refusal and the
// AttachmentCount GetPolicy and ListPolicies report must not be able to disagree about what
// "attached" means, and reading the same keys through the same loader is what guarantees it
// (#853). The counts helper cannot serve both, because it accumulates counts[arn]++ and so
// answers whether a policy is attached rather than to what — and five callers depend on its
// signature and its once-per-request hoisting.
//
// Scoped to one ARN rather than building a whole-account map, because the one caller asks
// about the ARN it was handed and would read a single key out of such a map.
//
// Each kind is sorted. MemoryStateManager.List returns keys lexicographically (#865), so the
// order is already stable, but the sort states the guarantee at the point the message is built
// rather than leaving a refusal's wording resting on a contract established two files away.
func (p *IAMPlugin) iamPolicyAttachedEntities(goCtx context.Context, accountID, arn string) (iamPolicyAttachments, error) {
	var attached iamPolicyAttachments

	for _, kind := range []struct {
		name  string
		names *[]string
	}{
		{"user", &attached.Users},
		{"group", &attached.Groups},
		{"role", &attached.Roles},
	} {
		prefix := iamAttachedPoliciesPrefix(accountID, kind.name)
		keys, err := p.state.List(goCtx, iamNamespace, prefix)
		if err != nil {
			return iamPolicyAttachments{}, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, key := range keys {
			arns, err := p.loadPolicyList(goCtx, key)
			if err != nil {
				return iamPolicyAttachments{}, err
			}
			if slices.Contains(arns, arn) {
				// The entity name is what follows the prefix. Neither an account ID nor an
				// entity name may contain "/", so the trim is unambiguous (iam_state_keys.go).
				*kind.names = append(*kind.names, strings.TrimPrefix(key, prefix))
			}
		}
		slices.Sort(*kind.names)
	}

	return attached, nil
}

// iamEnumMessage renders the message AWS returns for a value outside an enum, naming the
// permitted set so a caller can fix the call from the response alone.
func iamEnumMessage(param, value string, allowed []string) string {
	return fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed to satisfy "+
		"constraint: Member must satisfy enum value set: [%s]",
		value, param, strings.Join(allowed, ", "))
}
