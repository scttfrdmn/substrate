package emulator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// CreateOrganization and DeleteOrganization (#1627).
//
// An account that has never had an organization is still given one on its first observation, by
// [OrganizationsPlugin.ensureOrganization], so a test that never calls CreateOrganization works as
// it always has. CreateOrganization on such an account creates the organization with the requested
// FeatureSet (ALL when absent) instead, and on an account that already has one, auto-created or not,
// or that is a member of one, answers AlreadyInOrganizationException.
//
// An explicitly created organization has no policy type enabled at its root, so no FullAWSAccess
// attachments, until EnablePolicyType. The API reference says an ALL-features organization is
// created with "service control policies automatically enabled in the root", but the observed
// answer is otherwise: the Terraform AWS provider's acceptance test testAccOrganization_basic
// (v6.73.0, internal/service/organizations/organization_test.go) asserts roots.0.policy_types.# is 0
// after a CreateOrganization with FeatureSet ALL, against real AWS. The auto-created organization
// keeps SCPs enabled, as it always has.
//
// DeleteOrganization refuses an organization with any account besides its management account
// (OrganizationNotEmptyException), deletes every record the organization holds — its root, OUs,
// policies, attachments, tags, resource policy, trusted service access and CreateAccount statuses —
// and leaves a marker keyed by the management account. While the marker stands, every operation
// that account calls except CreateOrganization answers AWSOrganizationsNotInUseException, rather
// than auto-creating a fresh organization; CreateOrganization removes it. The marker is per account, so another account
// that has never had an organization still gets one on first observation.

// orgDeletedKey marks that acct's organization was deleted and none has been created since.
func orgDeletedKey(acct string) string { return "org_deleted:" + acct }

// orgNotInUse is AWSOrganizationsNotInUseException. The message is the exception's description in
// the Organizations API reference (DeleteOrganization, Errors).
func orgNotInUse() *AWSError {
	return orgErr("AWSOrganizationsNotInUseException",
		"Your account isn't a member of an organization. To make this request, you must use the "+
			"credentials of an account that belongs to an organization.")
}

// requireNotDeleted answers AWSOrganizationsNotInUseException for an account whose organization was
// deleted, and nil for every other account.
func (p *OrganizationsPlugin) requireNotDeleted(ctx context.Context, acct string) error {
	var deleted bool
	found, err := p.orgGetJSON(ctx, orgDeletedKey(acct), &deleted)
	if err != nil {
		return err
	}
	if found {
		return orgNotInUse()
	}
	return nil
}

// requireInUse refuses a request from an account whose organization was deleted. HandleRequest runs
// it before every handler but CreateOrganization's, so an operation that reads a record by ID
// answers the same code as one that reaches ensureOrganization.
func (p *OrganizationsPlugin) requireInUse(ctx context.Context, acct string) error {
	found, err := p.orgGetJSON(ctx, orgKey(acct), &Organization{})
	if err != nil || found {
		return err
	}
	return p.requireNotDeleted(ctx, acct)
}

// orgUnwrapNotInUse returns the AWSOrganizationsNotInUseException err wraps, if it wraps one, so a
// handler that wrapped ensureOrganization's error with context still answers the code.
func orgUnwrapNotInUse(err error) error {
	var awsErr *AWSError
	if errors.As(err, &awsErr) && awsErr.Code == "AWSOrganizationsNotInUseException" {
		return awsErr
	}
	return err
}

// lifecycleOperation claims CreateOrganization and DeleteOrganization.
func (p *OrganizationsPlugin) lifecycleOperation(op string) (orgHandler, bool) {
	switch op {
	case "CreateOrganization":
		return p.createOrganization, true
	case "DeleteOrganization":
		return p.deleteOrganization, true
	default:
		return nil, false
	}
}

func (p *OrganizationsPlugin) createOrganization(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	var input struct {
		FeatureSet string `json:"FeatureSet"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	featureSet := input.FeatureSet
	switch featureSet {
	case "":
		featureSet = orgFeatureSetAll
	case orgFeatureSetAll, orgFeatureSetConsolidatedBilling:
	default:
		return nil, orgInvalidInput("INVALID_ENUM",
			fmt.Sprintf("the FeatureSet must be %s or %s", orgFeatureSetAll, orgFeatureSetConsolidatedBilling))
	}

	found, err := p.orgGetJSON(goCtx, orgKey(reqCtx.AccountID), &Organization{})
	if err != nil {
		return nil, fmt.Errorf("createOrganization load: %w", err)
	}
	if found || reqCtx.isMember() {
		// AlreadyInOrganizationException's description in the API reference (CreateOrganization, Errors).
		return nil, orgErr("AlreadyInOrganizationException",
			"This account is already a member of an organization. An account can belong to only one organization at a time.")
	}
	if err := p.orgDeleteKey(goCtx, orgDeletedKey(reqCtx.AccountID)); err != nil {
		return nil, err
	}
	org, err := p.foundOrganization(goCtx, reqCtx.AccountID, featureSet, false)
	if err != nil {
		return nil, fmt.Errorf("createOrganization: %w", err)
	}
	if err := p.renderOrganization(goCtx, reqCtx.AccountID, org); err != nil {
		return nil, err
	}
	return orgJSONResponse(map[string]interface{}{"Organization": org}, "createOrganization")
}

func (p *OrganizationsPlugin) deleteOrganization(reqCtx *orgCaller, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	if reqCtx.isMember() {
		return nil, &AWSError{
			Code: "AccessDeniedException",
			Message: "You don't have permissions to perform the requested operation. " +
				"DeleteOrganization can be called only from the management account of the organization",
			HTTPStatus: http.StatusForbidden,
		}
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("deleteOrganization ensure org: %w", err)
	}
	acct := reqCtx.AccountID
	accounts, err := p.loadAccountIDs(goCtx, acct)
	if err != nil {
		return nil, fmt.Errorf("deleteOrganization accounts: %w", err)
	}
	for _, id := range accounts {
		if id != acct {
			// OrganizationNotEmptyException's description in the API reference (DeleteOrganization, Errors).
			return nil, orgErr("OrganizationNotEmptyException",
				"The organization isn't empty. To delete an organization, you must first remove all accounts except the management account.")
		}
	}
	if err := p.deleteOrganizationRecords(goCtx, acct, accounts); err != nil {
		return nil, fmt.Errorf("deleteOrganization: %w", err)
	}
	if err := p.orgPutJSON(goCtx, orgDeletedKey(acct), true); err != nil {
		return nil, fmt.Errorf("deleteOrganization marker: %w", err)
	}
	return orgEmptyResponse(), nil
}

// deleteOrganizationRecords deletes every record acct's organization holds. A target's policy
// attachments are removed from each policy's target list as well, because p-FullAWSAccess's list
// is shared by every organization.
func (p *OrganizationsPlugin) deleteOrganizationRecords(ctx context.Context, acct string, accounts []string) error {
	var root OrgRoot
	if _, err := p.orgGetJSON(ctx, orgRootKey(acct), &root); err != nil {
		return err
	}
	ous, err := p.loadOUIDs(ctx, acct)
	if err != nil {
		return err
	}
	policies, err := p.loadPolicyIDs(ctx, acct)
	if err != nil {
		return err
	}
	requests, err := p.loadCreateAccountStatusIDs(ctx, acct)
	if err != nil {
		return err
	}
	var resourcePolicy OrgResourcePolicy
	if _, err := p.orgGetJSON(ctx, orgResourcePolicyKey(acct), &resourcePolicy); err != nil {
		return err
	}

	var keys []string
	targets := append(append([]string{root.ID}, ous...), accounts...)
	for _, target := range targets {
		attached, err := p.loadAttachments(ctx, target)
		if err != nil {
			return err
		}
		for _, policyID := range attached {
			if _, err := p.orgRemoveID(ctx, orgPolicyTargetsKey(policyID), target); err != nil {
				return err
			}
		}
		keys = append(keys, orgAttachmentsKey(target), orgParentKey(target), orgChildrenKey(target), orgTagsKey(target))
	}
	for _, id := range ous {
		keys = append(keys, orgOUKey(id))
	}
	for _, id := range policies {
		keys = append(keys, orgPolicyKey(id), orgPolicyTargetsKey(id), orgTagsKey(id))
	}
	for _, id := range accounts {
		keys = append(keys, orgAccountKey(id), orgMemberOwnerKey(id))
	}
	for _, id := range requests {
		keys = append(keys, orgCreateStatusKey(id), orgCreatePendingKey(id))
	}
	if id := resourcePolicy.ResourcePolicySummary.ID; id != "" {
		keys = append(keys, orgTagsKey(id))
	}
	keys = append(keys, orgResourcePolicyKey(acct), orgServiceAccessKey(acct), orgAccountIDsKey(acct), orgOUIDsKey(acct),
		orgPolicyIDsKey(acct), orgCreateStatusIDsKey(acct), orgRootKey(acct), orgKey(acct))
	for _, key := range keys {
		if err := p.orgDeleteKey(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
