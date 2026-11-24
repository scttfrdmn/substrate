package emulator

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// Trusted service access: EnableAWSServiceAccess, DisableAWSServiceAccess and
// ListAWSServiceAccessForOrganization (#1627). Terraform's aws_organizations_organization reads the
// list on every refresh of an ALL-features organization, so without it the resource could not be
// applied.
//
// All three are refused in a CONSOLIDATED_BILLING organization; see requireAllFeatures.
//
// The organization keeps the enabled service principals, each with the simulated time it was
// enabled. Enabling one already enabled keeps its original date, and disabling one not enabled
// succeeds; no published error covers either case. Enabling records intent only: substrate creates
// no service-linked roles in member accounts. Only the management account may call them, as the pages
// state; a delegated administrator, which the list's page also admits, is not modeled. The list is sorted by principal and paged like the plugin's other listings. The
// refusal messages are substrate's own.

// requireAllFeatures refuses trusted service access in a CONSOLIDATED_BILLING organization with
// ConstraintViolationException/ORGANIZATION_NOT_IN_ALL_FEATURES_MODE. Trusted access works through
// service-linked roles, which the user guide says a consolidated-billing organization never uses.
// The message is the reason's description in the API reference (ConstraintViolationException).
func (p *OrganizationsPlugin) requireAllFeatures(ctx context.Context, acct string) error {
	featureSet, err := p.effectiveFeatureSet(ctx, acct)
	if err != nil {
		return err
	}
	if featureSet == orgFeatureSetAll {
		return nil
	}
	return orgConstraintViolation("ORGANIZATION_NOT_IN_ALL_FEATURES_MODE",
		"You attempted to perform an operation that requires the organization to be configured to support all features. "+
			"An organization that supports only consolidated billing features can't perform this operation.")
}

// orgServiceAccessKey holds the service principals enabled for acct's organization.
func orgServiceAccessKey(acct string) string { return "service_access:" + acct }

// orgEnabledServicePrincipal is API_EnabledServicePrincipal.
type orgEnabledServicePrincipal struct {
	ServicePrincipal string       `json:"ServicePrincipal"`
	DateEnabled      EpochSeconds `json:"DateEnabled"`
}

// orgServicePrincipalMaxLength is ServicePrincipal's published maximum length.
const orgServicePrincipalMaxLength = 128

// serviceAccessOperation claims the three trusted-service-access operations.
func (p *OrganizationsPlugin) serviceAccessOperation(op string) (orgHandler, bool) {
	switch op {
	case "EnableAWSServiceAccess":
		return p.enableAWSServiceAccess, true
	case "DisableAWSServiceAccess":
		return p.disableAWSServiceAccess, true
	case "ListAWSServiceAccessForOrganization":
		return p.listAWSServiceAccess, true
	default:
		return nil, false
	}
}

// orgCheckServicePrincipal refuses a ServicePrincipal outside its published 1–128 length and
// [\w+=,.@-]* pattern.
func orgCheckServicePrincipal(principal string) error {
	if principal == "" {
		return orgInvalidInput("INPUT_REQUIRED", "you must specify a ServicePrincipal")
	}
	if len(principal) > orgServicePrincipalMaxLength {
		return orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("the ServicePrincipal must be at most %d characters", orgServicePrincipalMaxLength))
	}
	if strings.IndexFunc(principal, func(r rune) bool { return !orgServicePrincipalRune(r) }) >= 0 {
		return orgInvalidInput("INVALID_PATTERN", "the ServicePrincipal must match [\\w+=,.@-]*")
	}
	return nil
}

// orgServicePrincipalRune reports whether r is in ServicePrincipal's [\w+=,.@-] class.
func orgServicePrincipalRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_+=,.@-", r)
}

// orgServiceAccessDenied refuses a member account's enable or disable.
func orgServiceAccessDenied(op string) *AWSError {
	return &AWSError{
		Code: "AccessDeniedException",
		Message: "You don't have permissions to perform the requested operation. " +
			op + " can be called only from the management account of the organization",
		HTTPStatus: http.StatusForbidden,
	}
}

// changeServiceAccess is both the enable and the disable; enable picks which.
func (p *OrganizationsPlugin) changeServiceAccess(reqCtx *orgCaller, req *AWSRequest, op string, enable bool) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	if reqCtx.isMember() {
		return nil, orgServiceAccessDenied(op)
	}
	var input struct {
		ServicePrincipal string `json:"ServicePrincipal"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckServicePrincipal(input.ServicePrincipal); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("%s ensure org: %w", op, err)
	}
	if err := p.requireAllFeatures(goCtx, reqCtx.AccountID); err != nil {
		return nil, err
	}
	var enabled []orgEnabledServicePrincipal
	if _, err := p.orgGetJSON(goCtx, orgServiceAccessKey(reqCtx.AccountID), &enabled); err != nil {
		return nil, err
	}
	i := slices.IndexFunc(enabled, func(e orgEnabledServicePrincipal) bool { return e.ServicePrincipal == input.ServicePrincipal })
	switch {
	case enable && i < 0:
		enabled = append(enabled, orgEnabledServicePrincipal{
			ServicePrincipal: input.ServicePrincipal, DateEnabled: EpochSeconds(p.tc.Now()),
		})
		slices.SortFunc(enabled, func(a, b orgEnabledServicePrincipal) int {
			return strings.Compare(a.ServicePrincipal, b.ServicePrincipal)
		})
	case !enable && i >= 0:
		enabled = slices.Delete(enabled, i, i+1)
	default:
		return orgEmptyResponse(), nil
	}
	if err := p.orgPutJSON(goCtx, orgServiceAccessKey(reqCtx.AccountID), enabled); err != nil {
		return nil, err
	}
	return orgEmptyResponse(), nil
}

func (p *OrganizationsPlugin) enableAWSServiceAccess(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	return p.changeServiceAccess(reqCtx, req, "EnableAWSServiceAccess", true)
}

func (p *OrganizationsPlugin) disableAWSServiceAccess(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	return p.changeServiceAccess(reqCtx, req, "DisableAWSServiceAccess", false)
}

func (p *OrganizationsPlugin) listAWSServiceAccess(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	// The page also admits a delegated administrator, which substrate does not model, so every
	// member is refused.
	if reqCtx.isMember() {
		return nil, orgServiceAccessDenied("ListAWSServiceAccessForOrganization")
	}
	var input struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listAWSServiceAccess ensure org: %w", err)
	}
	if err := p.requireAllFeatures(goCtx, reqCtx.AccountID); err != nil {
		return nil, err
	}
	var enabled []orgEnabledServicePrincipal
	if _, err := p.orgGetJSON(goCtx, orgServiceAccessKey(reqCtx.AccountID), &enabled); err != nil {
		return nil, err
	}
	byName := make(map[string]orgEnabledServicePrincipal, len(enabled))
	names := make([]string, 0, len(enabled))
	for _, e := range enabled {
		byName[e.ServicePrincipal] = e
		names = append(names, e.ServicePrincipal)
	}
	pageNames, next, err := orgPaginate(names, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}
	out := make([]orgEnabledServicePrincipal, 0, len(pageNames))
	for _, name := range pageNames {
		out = append(out, byName[name])
	}
	resp := map[string]interface{}{"EnabledServicePrincipals": out}
	if next != "" {
		resp["NextToken"] = next
	}
	return orgJSONResponse(resp, "listAWSServiceAccess")
}
