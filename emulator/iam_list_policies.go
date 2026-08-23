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
		return iamErrorResponse("ValidationError", iamParamMessage(err), http.StatusBadRequest), nil
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

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListPolicies", "*"); err != nil {
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

	xmlStr := iamPolicyListXML(policies) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
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

// iamEnumMessage renders the message AWS returns for a value outside an enum, naming the
// permitted set so a caller can fix the call from the response alone.
func iamEnumMessage(param, value string, allowed []string) string {
	return fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed to satisfy "+
		"constraint: Member must satisfy enum value set: [%s]",
		value, param, strings.Join(allowed, ", "))
}
