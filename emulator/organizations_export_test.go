package emulator

import "context"

// This file exports the Organizations storage layer for the external test
// package. It lives apart from export_test.go so the per-cluster Organizations
// files can each add what they need without contending for one file, and it is
// compiled only when running tests.

// Organizations quotas, exported so a test can pin each against its documented
// value. The depth and attachment limits in particular are boundary conditions:
// an off-by-one in either is invisible in a nominal run.
const (
	OrgMaxAccountsForTest        = orgMaxAccounts
	OrgMaxOUsPerOrgForTest       = orgMaxOUsPerOrg
	OrgMaxOUDepthForTest         = orgMaxOUDepth
	OrgMaxSCPsPerOrgForTest      = orgMaxSCPsPerOrg
	OrgMaxSCPsPerTargetForTest   = orgMaxSCPsPerTarget
	OrgMinSCPsPerTargetForTest   = orgMinSCPsPerTarget
	OrgMaxSCPBytesForTest        = orgMaxSCPBytes
	OrgMaxTagsPerResourceForTest = orgMaxTagsPerResource
	OrgMaxResultsForTest         = orgMaxResults
)

// Organizations entity kinds, exported for resolveOrgTarget assertions.
const (
	OrgKindRootForTest    = orgKindRoot
	OrgKindOUForTest      = orgKindOU
	OrgKindAccountForTest = orgKindAccount
	OrgKindPolicyForTest  = orgKindPolicy
)

// OrgFullAWSAccessIDForTest is the AWS-managed SCP's policy ID.
const OrgFullAWSAccessIDForTest = orgFullAWSAccessID

// OrgPolicyTypeSCPForTest is the one policy type substrate models.
const OrgPolicyTypeSCPForTest = orgPolicyTypeSCP

// OrgPaginateForTest wraps orgPaginate for external tests.
func OrgPaginateForTest(ids []string, nextToken string, maxResults int) (page []string, next string, err error) {
	return orgPaginate(ids, nextToken, maxResults)
}

// OrgConstraintViolationForTest wraps orgConstraintViolation for external tests.
func OrgConstraintViolationForTest(reason, message string) *AWSError {
	return orgConstraintViolation(reason, message)
}

// IsOrgOUIDForTest wraps isOrgOUID for external tests.
func IsOrgOUIDForTest(id string) bool { return isOrgOUID(id) }

// IsOrgRootIDForTest wraps isOrgRootID for external tests.
func IsOrgRootIDForTest(id string) bool { return isOrgRootID(id) }

// FullAWSAccessPolicyForTest wraps fullAWSAccessPolicy for external tests.
func FullAWSAccessPolicyForTest() OrgPolicy { return fullAWSAccessPolicy() }

// OrgEmptyResponseForTest wraps orgEmptyResponse for external tests.
func OrgEmptyResponseForTest() *AWSResponse { return orgEmptyResponse() }

// NewOrganizationsPluginForTest constructs a bare OrganizationsPlugin wired to
// the given state and clock, so a test can drive the storage layer without going
// through the HTTP surface.
func NewOrganizationsPluginForTest(state StateManager, tc *TimeController) *OrganizationsPlugin {
	return &OrganizationsPlugin{state: state, tc: tc, logger: NewDefaultLogger(0, false)}
}

// EnsureOrganizationForTest wraps ensureOrganization for external tests.
func (p *OrganizationsPlugin) EnsureOrganizationForTest(ctx context.Context, acct string) (*Organization, error) {
	return p.ensureOrganization(ctx, acct)
}

// OrganizationOwnerForTest wraps organizationOwner for external tests, so the
// reverse index #623 added can be asserted directly rather than only through the
// organization a handler happens to answer with.
func (p *OrganizationsPlugin) OrganizationOwnerForTest(ctx context.Context, acct string) (string, error) {
	return p.organizationOwner(ctx, acct)
}

// LoadRootForTest wraps loadRoot for external tests.
func (p *OrganizationsPlugin) LoadRootForTest(ctx context.Context, acct string) (*OrgRoot, error) {
	return p.loadRoot(ctx, acct)
}

// LoadStoredRootForTest wraps loadStoredRoot for external tests.
func (p *OrganizationsPlugin) LoadStoredRootForTest(ctx context.Context, acct string) (*OrgRoot, error) {
	return p.loadStoredRoot(ctx, acct)
}

// SaveRootForTest wraps saveRoot for external tests.
func (p *OrganizationsPlugin) SaveRootForTest(ctx context.Context, acct string, root OrgRoot) error {
	return p.saveRoot(ctx, acct, root)
}

// SCPEnabledForTest wraps scpEnabled for external tests.
func (p *OrganizationsPlugin) SCPEnabledForTest(ctx context.Context, acct string) (bool, error) {
	return p.scpEnabled(ctx, acct)
}

// EffectiveFeatureSetForTest wraps effectiveFeatureSet for external tests.
func (p *OrganizationsPlugin) EffectiveFeatureSetForTest(ctx context.Context, acct string) (string, error) {
	return p.effectiveFeatureSet(ctx, acct)
}

// ResolveSeededCreateFailureForTest wraps resolveSeededCreateFailure for external
// tests, returning the reason and whether a seed applied.
func (p *OrganizationsPlugin) ResolveSeededCreateFailureForTest(ctx context.Context, accountName string) (reason string, seeded bool, err error) {
	seed, err := p.resolveSeededCreateFailure(ctx, accountName)
	if err != nil || seed == nil {
		return "", false, err
	}
	return seed.FailureReason, true, nil
}

// SaveAccountForTest wraps saveAccount for external tests.
func (p *OrganizationsPlugin) SaveAccountForTest(ctx context.Context, acct string, a OrgAccount) error {
	return p.saveAccount(ctx, acct, a)
}

// LoadAccountForTest wraps loadAccount for external tests.
func (p *OrganizationsPlugin) LoadAccountForTest(ctx context.Context, accountID string) (*OrgAccount, error) {
	return p.loadAccount(ctx, accountID)
}

// LoadAccountIDsForTest wraps loadAccountIDs for external tests.
func (p *OrganizationsPlugin) LoadAccountIDsForTest(ctx context.Context, acct string) ([]string, error) {
	return p.loadAccountIDs(ctx, acct)
}

// PlaceChildForTest wraps placeChild for external tests.
func (p *OrganizationsPlugin) PlaceChildForTest(ctx context.Context, parent, child string) error {
	return p.placeChild(ctx, parent, child)
}

// LoadParentForTest wraps loadParent for external tests.
func (p *OrganizationsPlugin) LoadParentForTest(ctx context.Context, child string) (string, error) {
	return p.loadParent(ctx, child)
}

// LoadChildrenForTest wraps loadChildren for external tests.
func (p *OrganizationsPlugin) LoadChildrenForTest(ctx context.Context, parent string) ([]string, error) {
	return p.loadChildren(ctx, parent)
}

// OUDepthForTest wraps ouDepth for external tests.
func (p *OrganizationsPlugin) OUDepthForTest(ctx context.Context, ouID string) (int, error) {
	return p.ouDepth(ctx, ouID)
}

// SaveOUForTest wraps saveOU for external tests.
func (p *OrganizationsPlugin) SaveOUForTest(ctx context.Context, acct string, ou OrgOrganizationalUnit) error {
	return p.saveOU(ctx, acct, ou)
}

// LoadOUForTest wraps loadOU for external tests.
func (p *OrganizationsPlugin) LoadOUForTest(ctx context.Context, ouID string) (*OrgOrganizationalUnit, error) {
	return p.loadOU(ctx, ouID)
}

// LoadOUIDsForTest wraps loadOUIDs for external tests.
func (p *OrganizationsPlugin) LoadOUIDsForTest(ctx context.Context, acct string) ([]string, error) {
	return p.loadOUIDs(ctx, acct)
}

// SavePolicyForTest wraps savePolicy for external tests.
func (p *OrganizationsPlugin) SavePolicyForTest(ctx context.Context, acct string, pol OrgPolicy) error {
	return p.savePolicy(ctx, acct, pol)
}

// LoadPolicyForTest wraps loadPolicy for external tests.
func (p *OrganizationsPlugin) LoadPolicyForTest(ctx context.Context, policyID string) (*OrgPolicy, error) {
	return p.loadPolicy(ctx, policyID)
}

// LoadPolicyIDsForTest wraps loadPolicyIDs for external tests.
func (p *OrganizationsPlugin) LoadPolicyIDsForTest(ctx context.Context, acct string) ([]string, error) {
	return p.loadPolicyIDs(ctx, acct)
}

// AttachPolicyToForTest wraps attachPolicyTo for external tests.
func (p *OrganizationsPlugin) AttachPolicyToForTest(ctx context.Context, policyID, targetID string) (bool, error) {
	return p.attachPolicyTo(ctx, policyID, targetID)
}

// DetachPolicyFromForTest wraps detachPolicyFrom for external tests.
func (p *OrganizationsPlugin) DetachPolicyFromForTest(ctx context.Context, policyID, targetID string) (bool, error) {
	return p.detachPolicyFrom(ctx, policyID, targetID)
}

// AttachFullAWSAccessForTest wraps attachFullAWSAccess for external tests.
func (p *OrganizationsPlugin) AttachFullAWSAccessForTest(ctx context.Context, acct, targetID string) error {
	return p.attachFullAWSAccess(ctx, acct, targetID)
}

// LoadAttachmentsForTest wraps loadAttachments for external tests.
func (p *OrganizationsPlugin) LoadAttachmentsForTest(ctx context.Context, target string) ([]string, error) {
	return p.loadAttachments(ctx, target)
}

// LoadPolicyTargetsForTest wraps loadPolicyTargets for external tests.
func (p *OrganizationsPlugin) LoadPolicyTargetsForTest(ctx context.Context, policyID string) ([]string, error) {
	return p.loadPolicyTargets(ctx, policyID)
}

// LoadTagsForTest wraps loadTags for external tests.
func (p *OrganizationsPlugin) LoadTagsForTest(ctx context.Context, resourceID string) ([]OrgTag, error) {
	return p.loadTags(ctx, resourceID)
}

// SaveTagsForTest wraps saveTags for external tests.
func (p *OrganizationsPlugin) SaveTagsForTest(ctx context.Context, resourceID string, tags []OrgTag) error {
	return p.saveTags(ctx, resourceID, tags)
}

// ResolveOrgTargetForTest wraps resolveOrgTarget for external tests.
func (p *OrganizationsPlugin) ResolveOrgTargetForTest(ctx context.Context, acct, id string) (string, error) {
	return p.resolveOrgTarget(ctx, acct, id)
}

// SaveCreateAccountStatusForTest wraps saveCreateAccountStatus for external tests.
func (p *OrganizationsPlugin) SaveCreateAccountStatusForTest(ctx context.Context, acct string, st OrgCreateAccountStatus) error {
	return p.saveCreateAccountStatus(ctx, acct, st)
}

// LoadCreateAccountStatusForTest wraps loadCreateAccountStatus for external tests.
func (p *OrganizationsPlugin) LoadCreateAccountStatusForTest(ctx context.Context, id string) (*OrgCreateAccountStatus, error) {
	return p.loadCreateAccountStatus(ctx, id)
}

// LoadCreateAccountStatusIDsForTest wraps loadCreateAccountStatusIDs for external
// tests.
func (p *OrganizationsPlugin) LoadCreateAccountStatusIDsForTest(ctx context.Context, acct string) ([]string, error) {
	return p.loadCreateAccountStatusIDs(ctx, acct)
}
