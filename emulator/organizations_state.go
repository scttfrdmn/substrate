package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"
)

// Organizations quotas. Every value below is from "Quotas for AWS Organizations"
// in the Organizations User Guide:
// https://docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html
const (
	// orgMaxAccounts is the default number of accounts in an organization. AWS
	// raises this on request; the default is what an untouched account gets, and
	// it is the value a vending tool's quota handling has to survive.
	orgMaxAccounts = 10

	// orgMaxOUsPerOrg is the number of OUs an organization may contain.
	orgMaxOUsPerOrg = 2000

	// orgMaxOUDepth is the number of OU levels below the root. A sixth level is
	// refused with ConstraintViolationException/OU_DEPTH_LIMIT_EXCEEDED.
	orgMaxOUDepth = 5

	// orgMaxSCPsPerOrg is the number of service control policies an organization
	// may contain.
	orgMaxSCPsPerOrg = 10000

	// orgMaxSCPsPerTarget is the number of SCPs attachable to one root, OU, or
	// account. Note this is the SCP number: the 5-per-target figure belongs to
	// resource control policies, which substrate does not model.
	orgMaxSCPsPerTarget = 10

	// orgMinSCPsPerTarget is the number of SCPs that must remain attached to an
	// entity while the SCP type is enabled — which is why p-FullAWSAccess exists
	// and why detaching the last policy is refused.
	orgMinSCPsPerTarget = 1

	// orgMaxSCPBytes is the maximum size of an SCP document, in characters.
	// Again the SCP number: 5120 is the RCP limit.
	orgMaxSCPBytes = 10240

	// orgMaxTagsPerResource is the number of tags attachable to one account, OU,
	// root, or policy.
	orgMaxTagsPerResource = 50

	// orgMaxResults is the ceiling the API model puts on MaxResults for every
	// paginated Organizations operation.
	orgMaxResults = 20
)

// Organizations entity kinds, as ParentType, ChildType and the TargetType of a
// PolicyTargetSummary spell them.
const (
	orgKindRoot    = "ROOT"
	orgKindOU      = "ORGANIZATIONAL_UNIT"
	orgKindAccount = "ACCOUNT"

	// orgKindPolicy is not a ParentType or ChildType — it exists because a policy
	// is taggable, so resolveOrgTarget has to be able to name one.
	orgKindPolicy = "POLICY"

	// orgKindResourcePolicy exists for the same reason as orgKindPolicy: the
	// organization's resource policy is taggable, because PutResourcePolicy accepts
	// Tags. It is not a ParentType or ChildType either, and every caller that
	// resolves a hierarchy target checks against an allowlist of ROOT/
	// ORGANIZATIONAL_UNIT/ACCOUNT, so naming it here cannot make it attachable or
	// movable.
	orgKindResourcePolicy = "RESOURCE_POLICY"
)

// Organizations policy types substrate models.
const orgPolicyTypeSCP = "SERVICE_CONTROL_POLICY"

// Organization feature sets.
const (
	orgFeatureSetAll                 = "ALL"
	orgFeatureSetConsolidatedBilling = "CONSOLIDATED_BILLING"
)

// Account statuses, the model's AccountStatus enum in full. There is no CLOSED
// member: a closed account reports SUSPENDED, and PENDING_CLOSURE is the
// in-flight status CloseAccount's asynchronous request passes through.
const (
	orgAccountStatusActive         = "ACTIVE"
	orgAccountStatusSuspended      = "SUSPENDED"
	orgAccountStatusPendingClosure = "PENDING_CLOSURE"
)

// p-FullAWSAccess, the AWS-managed SCP that permits everything. AWS attaches it
// to the root, to every OU, and to every account when the SCP type is enabled,
// and the minimum-attachment rule then prevents it being detached without
// another SCP in its place. Substrate synthesizes it rather than storing it, so
// it cannot be updated or deleted, and its ARN is owned by "aws" rather than by
// the organization's management account.
const (
	orgFullAWSAccessID      = "p-FullAWSAccess"
	orgFullAWSAccessName    = "FullAWSAccess"
	orgFullAWSAccessArn     = "arn:aws:organizations::aws:policy/service_control_policy/p-FullAWSAccess"
	orgFullAWSAccessContent = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
)

// --- state keys ---

func orgKey(acct string) string           { return "org:" + acct }
func orgRootKey(acct string) string       { return "root:" + acct }
func orgAccountKey(id string) string      { return "account:" + id }
func orgAccountIDsKey(acct string) string { return "account_ids:" + acct }

// orgParentKey holds the ID of the root or OU that directly contains child.
func orgParentKey(child string) string { return "parent:" + child }

// orgChildrenKey holds the sorted IDs of the accounts and OUs directly inside
// parent. The two directions are stored separately so ListParents and
// ListChildren are both single reads.
func orgChildrenKey(parent string) string { return "children:" + parent }

func orgOUKey(id string) string      { return "ou:" + id }
func orgOUIDsKey(acct string) string { return "ou_ids:" + acct }

func orgPolicyKey(id string) string      { return "policy:" + id }
func orgPolicyIDsKey(acct string) string { return "policy_ids:" + acct }

// orgAttachmentsKey holds the sorted policy IDs attached to a target.
func orgAttachmentsKey(target string) string { return "attachments:" + target }

// orgPolicyTargetsKey holds the sorted target IDs a policy is attached to.
func orgPolicyTargetsKey(policy string) string { return "policy_targets:" + policy }

// orgTagsKey holds the tags on an account, OU, root, or policy.
func orgTagsKey(resource string) string { return "tags:" + resource }

func orgCreateStatusKey(id string) string      { return "car:" + id }
func orgCreateStatusIDsKey(acct string) string { return "car_ids:" + acct }

// orgResourcePolicyKey holds the organization's single resource policy. It is
// keyed by the management account, like orgKey, because an organization has
// exactly one — not a list keyed by ID, which is what makes this cluster's shape
// unlike the rest of the service.
func orgResourcePolicyKey(acct string) string { return "resource_policy:" + acct }

// orgMemberOwnerKey holds the management account of the organization an account
// belongs to. Every other index here runs the other way — orgAccountIDsKey lists
// the members of one management account — so before this key existed there was no
// way to answer "whose organization is this caller in", and a member calling any
// operation was given a private organization of its own (#623).
//
// The management account is indexed to itself, so the lookup has one answer for
// every account substrate knows rather than two cases the callers have to
// distinguish.
func orgMemberOwnerKey(id string) string { return "member_owner:" + id }

// --- errors ---
//
// Every Organizations exception is HTTP 400. The API model declares no 404 for
// any of them, including AccountNotFoundException, and the SDKs match on the
// error code rather than the status — so a 404 here is a shape a caller cannot
// reproduce against AWS.

// orgErr returns an Organizations error with the given code at HTTP 400.
func orgErr(code, message string) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: http.StatusBadRequest}
}

// orgInvalidInput returns InvalidInputException. reason is a member of the
// model's InvalidInputExceptionReason enum and is included in the message,
// because the JSON-RPC error document substrate emits carries only a code and a
// message — a caller that needs to distinguish two InvalidInputExceptions has
// nothing else to read.
func orgInvalidInput(reason, message string) *AWSError {
	return orgErr("InvalidInputException", fmt.Sprintf("%s: %s", reason, message))
}

// orgConstraintViolation returns ConstraintViolationException with a reason from
// the model's ConstraintViolationExceptionReason enum, carried in the message
// for the same reason orgInvalidInput carries its own.
func orgConstraintViolation(reason, message string) *AWSError {
	return orgErr("ConstraintViolationException", fmt.Sprintf("%s: %s", reason, message))
}

// orgUnmarshal decodes a request body, answering InvalidInputException rather
// than a generic malformed-data code: the caller's catch branch is written
// against the Organizations exception, and no Organizations operation can return
// anything else for a body it cannot parse.
func orgUnmarshal(body []byte, out interface{}) error {
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, out); err != nil {
		return orgInvalidInput("INVALID_PATTERN", "could not parse request body: "+err.Error())
	}
	return nil
}

// --- generic state helpers ---

// orgGetJSON loads and decodes the value at key. It reports found=false when the
// key is absent, so callers can distinguish "no such entity" from a read error.
func (p *OrganizationsPlugin) orgGetJSON(ctx context.Context, key string, out interface{}) (bool, error) {
	data, err := p.state.Get(ctx, organizationsNamespace, key)
	if err != nil {
		return false, fmt.Errorf("get %s: %w", key, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return true, nil
}

// orgPutJSON encodes and stores v at key.
func (p *OrganizationsPlugin) orgPutJSON(ctx context.Context, key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	if err := p.state.Put(ctx, organizationsNamespace, key, data); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// orgLoadIDs loads a sorted ID index, returning an empty slice when absent.
func (p *OrganizationsPlugin) orgLoadIDs(ctx context.Context, key string) ([]string, error) {
	var ids []string
	if _, err := p.orgGetJSON(ctx, key, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// orgAddID adds id to the sorted index at key if it is not already there, and
// reports whether it added it.
func (p *OrganizationsPlugin) orgAddID(ctx context.Context, key, id string) (bool, error) {
	ids, err := p.orgLoadIDs(ctx, key)
	if err != nil {
		return false, err
	}
	for _, existing := range ids {
		if existing == id {
			return false, nil
		}
	}
	ids = append(ids, id)
	sort.Strings(ids)
	if err := p.orgPutJSON(ctx, key, ids); err != nil {
		return false, err
	}
	return true, nil
}

// orgRemoveID removes id from the sorted index at key and reports whether it was
// present.
func (p *OrganizationsPlugin) orgRemoveID(ctx context.Context, key, id string) (bool, error) {
	ids, err := p.orgLoadIDs(ctx, key)
	if err != nil {
		return false, err
	}
	kept := make([]string, 0, len(ids))
	found := false
	for _, existing := range ids {
		if existing == id {
			found = true
			continue
		}
		kept = append(kept, existing)
	}
	if !found {
		return false, nil
	}
	if err := p.orgPutJSON(ctx, key, kept); err != nil {
		return false, err
	}
	return true, nil
}

// orgDeleteKey removes a state key outright. Deletion is the one thing the
// foundation's storage helpers do not cover, and an emptied-but-present index key
// would keep a deleted entity's shadow in a state dump.
func (p *OrganizationsPlugin) orgDeleteKey(ctx context.Context, key string) error {
	if err := p.state.Delete(ctx, organizationsNamespace, key); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// --- pagination ---

// orgPaginate returns one page of ids and the token for the next, honoring the
// MaxResults ceiling the API model declares (1-20; 0 means "unset", which AWS
// treats as the maximum). The token is the opaque encoding of the last ID
// returned, and is empty when the listing is exhausted — a caller looping until
// NextToken is empty terminates.
//
// An unreadable token is InvalidInputException/INVALID_NEXT_TOKEN rather than a
// silent restart from the beginning: a paginating caller that restarts sees
// duplicates instead of an error, which is the failure mode hardest to notice.
func orgPaginate(ids []string, nextToken string, maxResults int) (page []string, next string, err error) {
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)

	start := 0
	if nextToken != "" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(nextToken)
		if decodeErr != nil {
			return nil, "", orgInvalidInput("INVALID_NEXT_TOKEN", "the NextToken value is not valid")
		}
		found := false
		for i, id := range sorted {
			if id == string(decoded) {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", orgInvalidInput("INVALID_NEXT_TOKEN", "the NextToken value is not valid")
		}
	}

	limit := maxResults
	if limit <= 0 || limit > orgMaxResults {
		limit = orgMaxResults
	}

	end := start + limit
	if end > len(sorted) {
		end = len(sorted)
	}
	page = sorted[start:end]
	if end < len(sorted) && len(page) > 0 {
		next = base64.StdEncoding.EncodeToString([]byte(page[len(page)-1]))
	}
	return page, next, nil
}

// --- organization, root and feature set ---

// organizationOwner returns the management account of the organization acct
// belongs to, or "" when substrate has never seen acct.
//
// "" is not an error and not a default: it is the answer for an account that has
// joined no organization, and the auto-create path depends on being able to tell
// it from a real answer. Collapsing the two — returning acct itself for an
// unknown account — would make a member of a *deleted* organization look like a
// management account.
func (p *OrganizationsPlugin) organizationOwner(ctx context.Context, acct string) (string, error) {
	var owner string
	if _, err := p.orgGetJSON(ctx, orgMemberOwnerKey(acct), &owner); err != nil {
		return "", err
	}
	return owner, nil
}

// ensureOrganization returns the organization for acct, creating it — along with
// its root, its management account, and the FullAWSAccess attachments AWS makes
// — on first call.
//
// A known member account resolves to the organization it belongs to rather than
// getting one of its own. Handlers reach here with an already-resolved management
// account (see orgCaller), so on the request path the reverse-index lookup below
// is a no-op; it is what makes the function correct for any caller, including a
// direct one, rather than only for the resolved path (#623).
func (p *OrganizationsPlugin) ensureOrganization(ctx context.Context, acct string) (*Organization, error) {
	owner, err := p.organizationOwner(ctx, acct)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		acct = owner
	}

	var org Organization
	found, err := p.orgGetJSON(ctx, orgKey(acct), &org)
	if err != nil {
		return nil, err
	}
	if found {
		return &org, nil
	}

	// Auto-create. The feature set comes from the control-plane seed when one is
	// set, so a test can have the organization exist in CONSOLIDATED_BILLING mode
	// from its very first observation rather than being switched afterwards.
	featureSet, err := p.effectiveFeatureSet(ctx, acct)
	if err != nil {
		return nil, err
	}
	orgID := "o-" + randomLowerAlphanum(10)
	org = Organization{
		ID:                 orgID,
		Arn:                fmt.Sprintf("arn:aws:organizations::%s:organization/%s", acct, orgID),
		FeatureSet:         featureSet,
		MasterAccountID:    acct,
		MasterAccountArn:   fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", acct, orgID, acct),
		MasterAccountEmail: "master@example.com",
	}
	if err := p.orgPutJSON(ctx, orgKey(acct), org); err != nil {
		return nil, err
	}

	// The root is created once, with the organization, and persisted. Minting it
	// per request left it with no stable identity, so nothing could reference it
	// (#577).
	rootID := "r-" + randomLowerHex(4)
	root := OrgRoot{
		ID:   rootID,
		Arn:  fmt.Sprintf("arn:aws:organizations::%s:root/%s/%s", acct, orgID, rootID),
		Name: "Root",
	}
	// An ALL-features organization has SERVICE_CONTROL_POLICY enabled from the
	// start; under CONSOLIDATED_BILLING no policy type is available at all, which
	// is what CreateOrganization documents for that mode.
	if featureSet == orgFeatureSetAll {
		root.PolicyTypes = []OrgPolicyTypeSummary{{Type: orgPolicyTypeSCP, Status: "ENABLED"}}
	}
	if err := p.orgPutJSON(ctx, orgRootKey(acct), root); err != nil {
		return nil, err
	}
	if featureSet == orgFeatureSetAll {
		if _, err := p.attachPolicyTo(ctx, orgFullAWSAccessID, rootID); err != nil {
			return nil, err
		}
	}

	// Auto-create the management account, placed in the root.
	masterAccount := OrgAccount{
		ID:           acct,
		Arn:          org.MasterAccountArn,
		Name:         "master",
		Email:        "master@example.com",
		Status:       orgAccountStatusActive,
		JoinedMethod: "INVITED",
		JoinedAt:     EpochSeconds(p.tc.Now()),
	}
	if err := p.saveAccount(ctx, acct, masterAccount); err != nil {
		return nil, err
	}
	if err := p.placeChild(ctx, rootID, acct); err != nil {
		return nil, err
	}
	if err := p.attachFullAWSAccess(ctx, acct, acct); err != nil {
		return nil, err
	}

	return &org, nil
}

// loadRoot returns the organization's root, creating the organization if needed.
// The returned root's PolicyTypes reflect the effective feature set: under
// CONSOLIDATED_BILLING no policy type is available, which is what
// CreateOrganization documents for that mode.
func (p *OrganizationsPlugin) loadRoot(ctx context.Context, acct string) (*OrgRoot, error) {
	if _, err := p.ensureOrganization(ctx, acct); err != nil {
		return nil, err
	}
	var root OrgRoot
	found, err := p.orgGetJSON(ctx, orgRootKey(acct), &root)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("organizations: root missing for account %s", acct)
	}
	featureSet, err := p.effectiveFeatureSet(ctx, acct)
	if err != nil {
		return nil, err
	}
	if featureSet != orgFeatureSetAll {
		root.PolicyTypes = nil
	}
	return &root, nil
}

// saveRoot persists the root as-is. Callers that mutate PolicyTypes must read
// the stored root rather than the one loadRoot returns, since loadRoot masks
// PolicyTypes under CONSOLIDATED_BILLING.
func (p *OrganizationsPlugin) saveRoot(ctx context.Context, acct string, root OrgRoot) error {
	return p.orgPutJSON(ctx, orgRootKey(acct), root)
}

// loadStoredRoot returns the root exactly as persisted, without the feature-set
// masking loadRoot applies.
func (p *OrganizationsPlugin) loadStoredRoot(ctx context.Context, acct string) (*OrgRoot, error) {
	if _, err := p.ensureOrganization(ctx, acct); err != nil {
		return nil, err
	}
	var root OrgRoot
	found, err := p.orgGetJSON(ctx, orgRootKey(acct), &root)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("organizations: root missing for account %s", acct)
	}
	return &root, nil
}

// scpEnabled reports whether SERVICE_CONTROL_POLICY is available and enabled for
// the organization's root. Two separate conditions collapse into one answer: the
// type is unavailable under CONSOLIDATED_BILLING, and available-but-disabled
// after DisablePolicyType. Attaching is refused either way, but with different
// codes, so callers that need to tell them apart consult effectiveFeatureSet.
func (p *OrganizationsPlugin) scpEnabled(ctx context.Context, acct string) (bool, error) {
	root, err := p.loadRoot(ctx, acct)
	if err != nil {
		return false, err
	}
	for _, pt := range root.PolicyTypes {
		if pt.Type == orgPolicyTypeSCP && pt.Status == "ENABLED" {
			return true, nil
		}
	}
	return false, nil
}

// --- accounts ---

// saveAccount persists an account and records both directions of its membership:
// the management account's list of members, and the member's own pointer back at
// the management account.
//
// Both writes live here rather than at the call sites because every path that
// creates or joins an account already goes through this one function —
// ensureOrganization's management account, vendAccount's member — so an index
// written by one operation and not another, which is worse than no index at all,
// is not a state this can reach.
func (p *OrganizationsPlugin) saveAccount(ctx context.Context, masterAcct string, a OrgAccount) error {
	if err := p.orgPutJSON(ctx, orgAccountKey(a.ID), a); err != nil {
		return err
	}
	if _, err := p.orgAddID(ctx, orgAccountIDsKey(masterAcct), a.ID); err != nil {
		return err
	}
	return p.orgPutJSON(ctx, orgMemberOwnerKey(a.ID), masterAcct)
}

// loadAccount returns the account, or (nil, nil) when there is no such account.
func (p *OrganizationsPlugin) loadAccount(ctx context.Context, accountID string) (*OrgAccount, error) {
	var a OrgAccount
	found, err := p.orgGetJSON(ctx, orgAccountKey(accountID), &a)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil //nolint:nilnil // (nil, nil) = "no such account", handled by caller.
	}
	return &a, nil
}

func (p *OrganizationsPlugin) loadAccountIDs(ctx context.Context, masterAcct string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgAccountIDsKey(masterAcct))
}

// --- hierarchy ---

// placeChild makes parent the direct container of child, removing child from any
// previous parent. Both directions of the index are updated together so a move
// cannot leave a child reachable from two parents.
func (p *OrganizationsPlugin) placeChild(ctx context.Context, parent, child string) error {
	prev, err := p.loadParent(ctx, child)
	if err != nil {
		return err
	}
	if prev == parent {
		return nil
	}
	if prev != "" {
		if _, err := p.orgRemoveID(ctx, orgChildrenKey(prev), child); err != nil {
			return err
		}
	}
	if _, err := p.orgAddID(ctx, orgChildrenKey(parent), child); err != nil {
		return err
	}
	return p.orgPutJSON(ctx, orgParentKey(child), parent)
}

// loadParent returns the ID of the entity directly containing child, or "" when
// child has no recorded parent.
func (p *OrganizationsPlugin) loadParent(ctx context.Context, child string) (string, error) {
	var parent string
	if _, err := p.orgGetJSON(ctx, orgParentKey(child), &parent); err != nil {
		return "", err
	}
	return parent, nil
}

// loadChildren returns the sorted IDs directly inside parent.
func (p *OrganizationsPlugin) loadChildren(ctx context.Context, parent string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgChildrenKey(parent))
}

// ouDepth returns how many OU levels separate ouID from the root: 1 for an OU
// directly in the root, 2 for one nested inside that, and so on. It returns 0
// for a root. The walk is bounded by orgMaxOUDepth+2 so a corrupt index cannot
// loop forever.
func (p *OrganizationsPlugin) ouDepth(ctx context.Context, ouID string) (int, error) {
	if !isOrgOUID(ouID) {
		return 0, nil
	}
	depth := 0
	current := ouID
	for i := 0; i <= orgMaxOUDepth+2; i++ {
		if !isOrgOUID(current) {
			return depth, nil
		}
		depth++
		parent, err := p.loadParent(ctx, current)
		if err != nil {
			return 0, err
		}
		if parent == "" {
			return depth, nil
		}
		current = parent
	}
	return depth, nil
}

// isOrgOUID reports whether id has the "ou-" prefix that distinguishes an OU
// from a root or an account in a ParentId or ChildId.
func isOrgOUID(id string) bool { return len(id) > 3 && id[:3] == "ou-" }

// isOrgRootID reports whether id has the "r-" prefix of a root.
func isOrgRootID(id string) bool { return len(id) > 2 && id[:2] == "r-" }

// isOrgAccountID reports whether id has the exactly-12-digits shape the model's
// AccountId pattern requires.
func isOrgAccountID(id string) bool {
	if len(id) != 12 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

// isOrgParentID reports whether id has the shape of a ParentId — a root or an OU
// — which is the pattern the model puts on both of MoveAccount's parent members.
func isOrgParentID(id string) bool { return isOrgRootID(id) || isOrgOUID(id) }

// orgOUNamesRoot reports whether an OU ID's embedded root segment is rootID. An
// OU ID is "ou-" plus the containing root's suffix, a dash, and the OU's own
// suffix, so the root an OU belongs to is readable from its ID alone.
func orgOUNamesRoot(ouID, rootID string) bool {
	if !isOrgOUID(ouID) || !isOrgRootID(rootID) {
		return false
	}
	rest := ouID[len("ou-"):]
	dash := strings.Index(rest, "-")
	if dash <= 0 {
		return false
	}
	return rest[:dash] == rootID[len("r-"):]
}

// --- organizational units ---

func (p *OrganizationsPlugin) saveOU(ctx context.Context, acct string, ou OrgOrganizationalUnit) error {
	if err := p.orgPutJSON(ctx, orgOUKey(ou.ID), ou); err != nil {
		return err
	}
	if _, err := p.orgAddID(ctx, orgOUIDsKey(acct), ou.ID); err != nil {
		return err
	}
	return nil
}

// loadOU returns the OU, or (nil, nil) when there is no such OU.
func (p *OrganizationsPlugin) loadOU(ctx context.Context, ouID string) (*OrgOrganizationalUnit, error) {
	var ou OrgOrganizationalUnit
	found, err := p.orgGetJSON(ctx, orgOUKey(ouID), &ou)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil //nolint:nilnil // (nil, nil) = "no such OU", handled by caller.
	}
	return &ou, nil
}

func (p *OrganizationsPlugin) loadOUIDs(ctx context.Context, acct string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgOUIDsKey(acct))
}

// --- policies ---

func (p *OrganizationsPlugin) savePolicy(ctx context.Context, acct string, pol OrgPolicy) error {
	if err := p.orgPutJSON(ctx, orgPolicyKey(pol.PolicySummary.ID), pol); err != nil {
		return err
	}
	if _, err := p.orgAddID(ctx, orgPolicyIDsKey(acct), pol.PolicySummary.ID); err != nil {
		return err
	}
	return nil
}

// loadPolicy returns the policy, or (nil, nil) when there is no such policy.
// p-FullAWSAccess is synthesized rather than read, so it exists in every
// organization and cannot be written to.
func (p *OrganizationsPlugin) loadPolicy(ctx context.Context, policyID string) (*OrgPolicy, error) {
	if policyID == orgFullAWSAccessID {
		full := fullAWSAccessPolicy()
		return &full, nil
	}
	var pol OrgPolicy
	found, err := p.orgGetJSON(ctx, orgPolicyKey(policyID), &pol)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil //nolint:nilnil // (nil, nil) = "no such policy", handled by caller.
	}
	return &pol, nil
}

func (p *OrganizationsPlugin) loadPolicyIDs(ctx context.Context, acct string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgPolicyIDsKey(acct))
}

// fullAWSAccessPolicy returns the AWS-managed SCP that allows everything.
func fullAWSAccessPolicy() OrgPolicy {
	return OrgPolicy{
		PolicySummary: OrgPolicySummary{
			ID:          orgFullAWSAccessID,
			Arn:         orgFullAWSAccessArn,
			Name:        orgFullAWSAccessName,
			Description: "Allows access to every operation",
			Type:        orgPolicyTypeSCP,
			AwsManaged:  true,
		},
		Content: orgFullAWSAccessContent,
	}
}

// --- availability, the outermost gate ---
//
// Two different conditions stop an SCP being useful, and they are not the same
// refusal:
//
//   - Not *available*: the organization is in CONSOLIDATED_BILLING mode, where the
//     policy type does not exist at all. Nothing can create, read, attach or enable
//     one, and the fix is a migration to all features.
//   - Available but not *enabled*: an all-features organization whose root has had
//     DisablePolicyType called on it. Policies still exist and can be created; only
//     attachment is refused, and the fix is one EnablePolicyType call.
//
// The second is the dangerous state issue #578 point 6 is about, and it is only
// distinguishable from the first if the two report differently. Availability is
// modeled as visibility: while SCPs are unavailable no policy is visible, so every
// operation that names one answers with its own documented not-found code. Only
// CreatePolicy and EnablePolicyType name the feature set as the reason, because
// those are the two operations whose error list in the API model declares
// PolicyTypeNotAvailableForOrganizationException — emitting it from an operation
// that does not declare it would hand a caller an exception its SDK cannot catch
// by type, which is worse than a truthful "no such policy".

// policyTypeAvailable reports whether service control policies exist at all for
// the organization, which is true only under the ALL feature set.
func (p *OrganizationsPlugin) policyTypeAvailable(ctx context.Context, acct string) (bool, error) {
	featureSet, err := p.effectiveFeatureSet(ctx, acct)
	if err != nil {
		return false, err
	}
	return featureSet == orgFeatureSetAll, nil
}

// loadVisiblePolicy returns the policy only when service control policies are
// available to the organization, and (nil, nil) otherwise. p-FullAWSAccess is
// synthesized by loadPolicy rather than stored, so without this gate it would stay
// readable in a CONSOLIDATED_BILLING organization that can hold no SCP at all.
func (p *OrganizationsPlugin) loadVisiblePolicy(ctx context.Context, acct, policyID string) (*OrgPolicy, error) {
	available, err := p.policyTypeAvailable(ctx, acct)
	if err != nil {
		return nil, err
	}
	if !available {
		return nil, nil //nolint:nilnil // (nil, nil) = "no visible policy", handled by caller.
	}
	return p.loadPolicy(ctx, policyID)
}

// rootSubtree returns every entity a policy can be attached to: the root, every
// OU, and every account. DisablePolicyType clears the attachments of all of them
// and EnablePolicyType restores FullAWSAccess to all of them, which is what AWS
// does — an OU or account created while the type was off would otherwise come back
// with no SCP at all, and the minimum-attachment rule would then be unenforceable
// for it.
func (p *OrganizationsPlugin) rootSubtree(ctx context.Context, acct, rootID string) ([]string, error) {
	entities := []string{rootID}
	ouIDs, err := p.loadOUIDs(ctx, acct)
	if err != nil {
		return nil, err
	}
	entities = append(entities, ouIDs...)
	accountIDs, err := p.loadAccountIDs(ctx, acct)
	if err != nil {
		return nil, err
	}
	return append(entities, accountIDs...), nil
}

// --- policy input validation ---

// orgCheckPolicyType validates a PolicyType or a ListPolicies Filter against the
// model's enum. A value outside the enum is a caller typo and gets
// INVALID_ENUM_POLICY_TYPE; a valid value substrate does not model is refused
// separately by each operation, because what the caller should do about it differs.
func orgCheckPolicyType(policyType string) error {
	if !slices.Contains(orgPolicyTypes, policyType) {
		return orgInvalidInput("INVALID_ENUM_POLICY_TYPE", "You specified an invalid policy type string: "+policyType)
	}
	return nil
}

// orgCheckPolicyID validates a PolicyId against the model's pattern. A malformed ID
// is INVALID_SYNTAX_POLICY_ID rather than PolicyNotFoundException, so a caller that
// passed a policy name where an ID belongs learns that instead of concluding the
// policy was deleted.
func orgCheckPolicyID(policyID string) error {
	if policyID == "" {
		return orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter PolicyId.")
	}
	if !isOrgPolicyIDSyntax(policyID) {
		return orgInvalidInput("INVALID_SYNTAX_POLICY_ID", "You specified an invalid policy ID: "+policyID)
	}
	return nil
}

// orgCheckTargetID validates a TargetId against the model's pattern, which admits
// a root, a 12-digit account, or an OU — and not a policy ID.
func orgCheckTargetID(targetID string) error {
	if targetID == "" {
		return orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter TargetId.")
	}
	if !isOrgTargetIDSyntax(targetID) {
		return orgInvalidInput("INVALID_PATTERN_TARGET_ID", "You specified a target that doesn't match the required pattern: "+targetID)
	}
	return nil
}

// orgCheckPolicyName validates a policy name against the model's PolicyName shape
// (1 to 128 characters).
func orgCheckPolicyName(name string) error {
	switch {
	case name == "":
		return orgInvalidInput("MIN_LENGTH_EXCEEDED", "You provided a name that is shorter than the minimum of 1 character")
	case utf8.RuneCountInString(name) > orgMaxPolicyNameChars:
		return orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("You provided a name longer than the maximum of %d characters", orgMaxPolicyNameChars))
	default:
		return nil
	}
}

// orgCheckPolicyDescription validates a description against the model's
// PolicyDescription shape (up to 512 characters; empty is permitted).
func orgCheckPolicyDescription(description string) error {
	if utf8.RuneCountInString(description) > orgMaxPolicyDescriptionChars {
		return orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("You provided a description longer than the maximum of %d characters", orgMaxPolicyDescriptionChars))
	}
	return nil
}

// orgCheckPolicyContent validates an SCP document: the size quota first, then
// whether it parses at all. The two are different refusals because they call for
// different fixes — a document over the limit has to be split across policies,
// while an unparseable one has to be corrected — and a caller cannot tell them
// apart from one code.
func orgCheckPolicyContent(content string) error {
	if content == "" {
		return orgInvalidInput("MIN_LENGTH_EXCEEDED", "You provided a policy document that is shorter than the minimum of 1 character")
	}
	// The quota is stated in characters, so a multi-byte document is measured in
	// runes: counting bytes would refuse a document AWS accepts.
	if utf8.RuneCountInString(content) > orgMaxSCPBytes {
		return orgConstraintViolation("POLICY_CONTENT_LIMIT_EXCEEDED",
			fmt.Sprintf("You have exceeded the maximum size of a policy document (%d characters)", orgMaxSCPBytes))
	}
	// Substrate does not evaluate an SCP, so it checks only that the document is a
	// JSON object — the boundary between "the caller sent something a policy engine
	// could read" and "the caller sent a string it never templated". Judging the
	// statement semantics would be modeling the authorization engine, not the API.
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(content), &doc); err != nil {
		return orgErr("MalformedPolicyDocumentException",
			"The provided policy document doesn't meet the requirements of the specified policy type: "+err.Error())
	}
	return nil
}

// --- policy attachments ---

// attachPolicyTo records an attachment in both directions and reports whether it
// was new.
func (p *OrganizationsPlugin) attachPolicyTo(ctx context.Context, policyID, targetID string) (bool, error) {
	added, err := p.orgAddID(ctx, orgAttachmentsKey(targetID), policyID)
	if err != nil {
		return false, err
	}
	if _, err := p.orgAddID(ctx, orgPolicyTargetsKey(policyID), targetID); err != nil {
		return false, err
	}
	return added, nil
}

// detachPolicyFrom removes an attachment in both directions and reports whether
// it was there.
func (p *OrganizationsPlugin) detachPolicyFrom(ctx context.Context, policyID, targetID string) (bool, error) {
	removed, err := p.orgRemoveID(ctx, orgAttachmentsKey(targetID), policyID)
	if err != nil {
		return false, err
	}
	if _, err := p.orgRemoveID(ctx, orgPolicyTargetsKey(policyID), targetID); err != nil {
		return false, err
	}
	return removed, nil
}

// attachFullAWSAccess attaches p-FullAWSAccess to a newly created OU or account,
// which is what AWS does while the SCP type is enabled. It is a no-op when the
// type is not enabled, matching an organization whose entities never carried an
// SCP. The root's own attachment is made inline by ensureOrganization, before
// the root record is readable.
func (p *OrganizationsPlugin) attachFullAWSAccess(ctx context.Context, acct, targetID string) error {
	enabled, err := p.scpEnabled(ctx, acct)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	if _, err := p.attachPolicyTo(ctx, orgFullAWSAccessID, targetID); err != nil {
		return err
	}
	return nil
}

// loadAttachments returns the sorted policy IDs attached to target.
func (p *OrganizationsPlugin) loadAttachments(ctx context.Context, target string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgAttachmentsKey(target))
}

// loadPolicyTargets returns the sorted target IDs a policy is attached to.
func (p *OrganizationsPlugin) loadPolicyTargets(ctx context.Context, policyID string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgPolicyTargetsKey(policyID))
}

// --- tags ---

// loadTags returns the tags on a resource, in key order.
func (p *OrganizationsPlugin) loadTags(ctx context.Context, resourceID string) ([]OrgTag, error) {
	var tags []OrgTag
	if _, err := p.orgGetJSON(ctx, orgTagsKey(resourceID), &tags); err != nil {
		return nil, err
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })
	return tags, nil
}

// saveTags replaces the tags on a resource.
func (p *OrganizationsPlugin) saveTags(ctx context.Context, resourceID string, tags []OrgTag) error {
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })
	return p.orgPutJSON(ctx, orgTagsKey(resourceID), tags)
}

// --- target resolution ---

// resolveOrgTarget names what id refers to — a root, OU, account, or policy —
// or returns "" when the organization contains no such entity. Callers turn ""
// into whichever not-found code their operation documents, which differs per
// operation: TargetNotFoundException for an attachment or a tag,
// ParentNotFoundException for a listing, AccountNotFoundException for a move.
func (p *OrganizationsPlugin) resolveOrgTarget(ctx context.Context, acct, id string) (string, error) {
	if id == "" {
		return "", nil
	}
	switch {
	case isOrgRootID(id):
		root, err := p.loadRoot(ctx, acct)
		if err != nil {
			return "", err
		}
		if root.ID == id {
			return orgKindRoot, nil
		}
		return "", nil
	case isOrgOUID(id):
		ou, err := p.loadOU(ctx, id)
		if err != nil {
			return "", err
		}
		if ou != nil {
			return orgKindOU, nil
		}
		return "", nil
	case len(id) > 2 && id[:2] == "p-":
		pol, err := p.loadPolicy(ctx, id)
		if err != nil {
			return "", err
		}
		if pol != nil {
			return orgKindPolicy, nil
		}
		return "", nil
	case len(id) > 3 && id[:3] == "rp-":
		// The organization holds at most one resource policy, so the ID has to match
		// the stored one rather than index into a collection. A well-formed rp- ID
		// that is not it names nothing, which is the same answer AWS gives.
		policy, found, err := p.loadResourcePolicy(ctx, acct)
		if err != nil {
			return "", err
		}
		if found && policy.ResourcePolicySummary.ID == id {
			return orgKindResourcePolicy, nil
		}
		return "", nil
	default:
		a, err := p.loadAccount(ctx, id)
		if err != nil {
			return "", err
		}
		if a != nil {
			return orgKindAccount, nil
		}
		return "", nil
	}
}

// --- create-account status ---

func (p *OrganizationsPlugin) saveCreateAccountStatus(ctx context.Context, acct string, st OrgCreateAccountStatus) error {
	if err := p.orgPutJSON(ctx, orgCreateStatusKey(st.ID), st); err != nil {
		return err
	}
	if _, err := p.orgAddID(ctx, orgCreateStatusIDsKey(acct), st.ID); err != nil {
		return err
	}
	return nil
}

// loadCreateAccountStatus returns the status, or (nil, nil) when there is no
// request with that ID.
func (p *OrganizationsPlugin) loadCreateAccountStatus(ctx context.Context, id string) (*OrgCreateAccountStatus, error) {
	var st OrgCreateAccountStatus
	found, err := p.orgGetJSON(ctx, orgCreateStatusKey(id), &st)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil //nolint:nilnil // (nil, nil) = "no such request", handled by caller.
	}
	return &st, nil
}

func (p *OrganizationsPlugin) loadCreateAccountStatusIDs(ctx context.Context, acct string) ([]string, error) {
	return p.orgLoadIDs(ctx, orgCreateStatusIDsKey(acct))
}
