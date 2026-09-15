package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// ListInstanceProfiles' three request parameters (#873).
//
// The operation decoded nothing at all — no parseIAMBody call and no params struct — so it
// returned every instance profile in the account with IsTruncated hardcoded false, whichever
// of MaxItems, Marker and PathPrefix the caller sent. That is the accepted-and-ignored class:
// a consumer's paginator asks for MaxItems=10, is told the one page it received is complete,
// and concludes correctly from substrate's answer and incorrectly about IAM. It is also why
// #868 could not reach this operation — that fix range-checks a decoded MaxItems, and there
// was nothing decoded here to check.
//
// PathPrefix is applied *before* the page is cut. listUsers and listRoles filter after
// paginateIAMKeys has already sliced, which under-fills a page whenever the profiles outside
// the prefix outnumber it — ask for one profile under /service-role/ with two profiles under
// / sorting ahead of it and the page comes back empty while IsTruncated says there is more.
// That is a separate defect and this operation deliberately does not copy it; listPolicies
// (iam_list_policies.go) is the arm that already gets the order right, and this follows it.
//
// The cost of filtering first is that every profile in the account is decoded on every call,
// not just the page's worth: PathPrefix matches on Path, which lives inside the record rather
// than in its state key, so there is nothing to filter on until the record is read. That is
// the same trade listPolicies makes, and correctness of the page is worth more than the reads
// — a page that under-fills is wrong in a way a caller cannot detect, while an extra state
// read is only slower.

// iamInstanceProfilePathPrefixMaxLen is the published maximum length of PathPrefix.
const iamInstanceProfilePathPrefixMaxLen = 512

// iamInstanceProfilePathPrefixPattern is the pattern API_ListInstanceProfiles publishes for
// PathPrefix, "\u002F[\u0021-\u007F]*": a leading slash followed by any number of
// characters from ! (U+0021) through DEL (U+007F).
//
// Deliberately *not* iamPathPrefixPattern (iam_list_policies.go), which is policyPathType and
// requires a trailing slash as well. The two operations publish different patterns, and
// sharing one would refuse "/service-role" here — a prefix ListInstanceProfiles accepts,
// because a prefix match needs no trailing slash to be meaningful. Refusing it would fail a
// call that succeeds against IAM, which is the worse of the two errors.
//
// The leading slash is worth enforcing rather than treating as advisory: "service-role/"
// without it matches no profile at all, and a silent empty result is indistinguishable from
// "no profiles under that path" — the same accepted-and-ignored failure this file exists to
// remove, moved from the parameter to its value.
var iamInstanceProfilePathPrefixPattern = regexp.MustCompile(`^/[\x21-\x7f]*$`)

// iamValidateInstanceProfilePathPrefix returns the response to refuse prefix with, or nil when
// it is acceptable.
//
// An empty prefix is accepted as absent rather than refused against the published minimum
// length of 1, matching iamValidateMaxItems' rule for a present-but-empty parameter
// (iam_max_items.go): a form body carrying "PathPrefix=" expressed no filter, so it takes the
// documented default of "/", which selects every profile. The two parameters must not
// disagree about what an empty value means within one request.
func iamValidateInstanceProfilePathPrefix(prefix string) *AWSResponse {
	if prefix == "" {
		return nil
	}
	// Length first, so an oversized string is refused on its size rather than run through the
	// pattern — the same ordering iamValidatePolicyARN uses.
	if len(prefix) > iamInstanceProfilePathPrefixMaxLen {
		return iamErrorResponse("ValidationError",
			fmt.Sprintf("The specified value for pathPrefix is invalid. "+
				"It must be between 1 and %d characters long (got %d).",
				iamInstanceProfilePathPrefixMaxLen, len(prefix)),
			http.StatusBadRequest)
	}
	if !iamInstanceProfilePathPrefixPattern.MatchString(prefix) {
		return iamErrorResponse("ValidationError",
			fmt.Sprintf("The specified value for pathPrefix is invalid. "+
				"It must begin with / and contain only printable ASCII characters (got %q).",
				prefix),
			http.StatusBadRequest)
	}
	return nil
}

// listInstanceProfiles returns the account's IAM instance profiles, narrowed by PathPrefix and
// paged by MaxItems and Marker.
//
// AWS publishes no resource types for this action, so [IAMPlugin.authzResource] answers every
// IAM resource in the account and a statement scoped to one profile grants nothing here — the
// same treatment ListUsers, ListRoles, ListGroups and ListPolicies get, and for the same
// reason: a list operation names no resource to scope to.
//
// The parameters are validated before authorization, matching the other IAM listings: a
// MaxItems of 1001 or a PathPrefix without its leading slash is invalid whatever the caller is
// permitted to do, so reporting AccessDenied for it would name the wrong problem.
func (p *IAMPlugin) listInstanceProfiles(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PathPrefix string `json:"PathPrefix"`
		Marker     string `json:"Marker"`
		MaxItems   iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		// parseIAMBody has already named the parameter, so the wrapped message is final.
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	if errResp := iamValidateInstanceProfilePathPrefix(params.PathPrefix); errResp != nil {
		return errResp, nil
	}
	if errResp := iamValidateMaxItems(req, params.MaxItems); errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListInstanceProfiles", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	keys, err := p.state.List(goCtx, iamNamespace, iamInstanceProfilePrefix(ctx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("list instance profiles: %w", err)
	}

	// An unreadable or unparsable record is skipped rather than failing the listing, matching
	// instanceProfilesHoldingRole: one corrupt record must not make every profile unlistable.
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
		if params.PathPrefix != "" && !strings.HasPrefix(profile.Path, params.PathPrefix) {
			continue
		}
		byKey[k] = profile
		matched = append(matched, k)
	}

	// Paged over the *matched* keys, which is what makes the marker consistent with the filter:
	// paginateIAMKeys sorts and then resumes after the key the marker names, so a walk that
	// keeps sending the same PathPrefix — as AWS requires — sees each profile exactly once.
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
	return iamXMLResponse(http.StatusOK, "ListInstanceProfiles", xmlStr)
}
