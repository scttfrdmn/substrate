package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Resource policy bounds, from the Organizations API model's ResourcePolicyContent
// shape. The maximum is corroborated by "Maximum size of the resource-based
// delegation policy: 40,000 characters" in the Organizations User Guide.
const (
	// orgMinResourcePolicyChars is ResourcePolicyContent's minimum length. An
	// empty document is refused rather than stored: it would delegate nothing
	// while DescribeResourcePolicy reported a policy in place, which is the one
	// answer a caller checking for delegation must not get wrong.
	orgMinResourcePolicyChars = 1

	// orgMaxResourcePolicyChars is ResourcePolicyContent's maximum length, in
	// characters rather than bytes — which is what the model's max means, and what
	// a caller with non-ASCII principals in its document depends on.
	orgMaxResourcePolicyChars = 40000
)

// resourcePolicyOperation claims the organization resource-policy operations.
//
// The cluster's shape is unlike every other one here: an organization holds
// exactly one resource policy rather than a list keyed by ID, PutResourcePolicy
// replaces it wholesale, and there is no per-statement update. Describe and Delete
// take no input at all, which is why neither reads req.Body.
//
// That absence of input is also what bounds the authorization model below. None of
// the three operations names an organization, so a caller can only ever ask about
// its own — the "caller outside the organization" case #619 and #623 both describe
// is not reachable through this API, because there is no member to put an outside
// organization's ID in. The asymmetry that *is* reachable is the documented one:
// Describe is callable "from the management account or a member account that is a
// delegated administrator", Put and Delete "only from the management account".
func (p *OrganizationsPlugin) resourcePolicyOperation(op string) (orgHandler, bool) {
	switch op {
	case "PutResourcePolicy":
		return p.putResourcePolicy, true
	case "DescribeResourcePolicy":
		return p.describeResourcePolicy, true
	case "DeleteResourcePolicy":
		return p.deleteResourcePolicy, true
	default:
		return nil, false
	}
}

// --- operations ---

// putResourcePolicy stores the organization's resource policy, replacing any
// existing one.
//
// The ID and ARN are minted once and survive every replacement. Re-minting them
// would make a caller holding the ARN conclude its policy had been replaced by a
// different one, when in fact the same single policy was updated — and since an
// organization has exactly one, there is nothing the new ID could distinguish it
// from.
func (p *OrganizationsPlugin) putResourcePolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if reqCtx.isMember() {
		return nil, orgResourcePolicyAccessDenied("PutResourcePolicy")
	}
	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("putResourcePolicy ensure org: %w", err)
	}

	var input struct {
		Content *string       `json:"Content"`
		Tags    []orgTagInput `json:"Tags"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	// A nil Content is an absent required member, which is a different refusal
	// from a present-but-empty one: the model marks Content required and gives it a
	// minimum length of 1, so both are errors but a caller debugging its request
	// needs to know which.
	if input.Content == nil {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify Content")
	}
	if err := validateOrgResourcePolicyContent(*input.Content); err != nil {
		return nil, err
	}

	existing, found, err := p.loadResourcePolicy(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("putResourcePolicy load: %w", err)
	}

	// Tags apply only to the initial creation: "Calls with tags apply to the
	// initial creation of the resource policy, otherwise an exception is thrown."
	// Accepting them on an update would silently drop them, leaving a tag-gated
	// authorization decision reading a tag set the caller believes it just wrote.
	//
	// The refusal carries no reason prefix, unlike every other InvalidInputException
	// here: no member of the model's InvalidInputExceptionReason enum describes it,
	// and inventing one would hand a caller a reason AWS never sends — the same
	// hazard as a typo'd CreateAccount failureReason, which no SDK catch branch
	// matches.
	if found && len(input.Tags) > 0 {
		return nil, orgErr("InvalidInputException",
			"tags can be attached only when the resource policy is first created")
	}
	tags, err := validateOrgCreateTags(input.Tags)
	if err != nil {
		return nil, err
	}

	policy := OrgResourcePolicy{Content: *input.Content}
	if found {
		policy.ResourcePolicySummary = existing.ResourcePolicySummary
	} else {
		id := "rp-" + randomLowerAlphanum(8)
		policy.ResourcePolicySummary = OrgResourcePolicySummary{
			ID:  id,
			Arn: fmt.Sprintf("arn:aws:organizations::%s:resourcepolicy/%s/%s", reqCtx.AccountID, org.ID, id),
		}
	}

	if err := p.orgPutJSON(goCtx, orgResourcePolicyKey(reqCtx.AccountID), policy); err != nil {
		return nil, fmt.Errorf("putResourcePolicy save: %w", err)
	}
	// Tags on the resource policy are tags on a resource, so they have to be
	// readable through ListTagsForResource like every other Organizations tag.
	if len(tags) > 0 {
		if err := p.saveTags(goCtx, policy.ResourcePolicySummary.ID, tags); err != nil {
			return nil, fmt.Errorf("putResourcePolicy save tags: %w", err)
		}
	}

	return orgJSONResponse(map[string]interface{}{"ResourcePolicy": policy}, "putResourcePolicy")
}

// describeResourcePolicy returns the organization's resource policy.
//
// A member account reads the document management set, provided that document names
// it as a principal — which is what makes it a delegated administrator. That is
// the asymmetry #619 asked for, and the three answers it produces are all
// distinguishable:
//
//   - management: the policy, or ResourcePolicyNotFoundException
//   - a member the policy names: the identical Content, Id and Arn
//   - a member it does not name: AccessDeniedException
//
// The refusal is the normal case for management, not an edge case: most
// organizations have no resource policy, and a caller checking whether management
// delegated anything has to tell ResourcePolicyNotFoundException apart from
// AccessDeniedException. Answering an empty policy instead would make "no
// delegation" and "delegation I cannot read" the same observation.
//
// The operation takes no input, so req.Body is not read.
func (p *OrganizationsPlugin) describeResourcePolicy(reqCtx *orgCaller, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("describeResourcePolicy ensure org: %w", err)
	}

	policy, found, err := p.loadResourcePolicy(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeResourcePolicy load: %w", err)
	}
	// A member is refused before the absence is reported, so a member cannot use
	// this operation to learn whether the organization has a policy at all: the two
	// answers it can get are the document that names it and a denial. Reporting
	// ResourcePolicyNotFoundException to a member of an organization that does have
	// one would leak the policy's existence to a caller not permitted to read it.
	if reqCtx.isMember() && !orgPolicyDelegatesTo(policy, found, reqCtx.callerAccount) {
		return nil, orgResourcePolicyAccessDenied("DescribeResourcePolicy")
	}
	if !found {
		return nil, orgResourcePolicyNotFound()
	}
	return orgJSONResponse(map[string]interface{}{"ResourcePolicy": policy}, "describeResourcePolicy")
}

// deleteResourcePolicy removes the organization's resource policy.
//
// Deleting when none is set is ResourcePolicyNotFoundException rather than a
// silent success, so a second run of a teardown script is a refusal it can branch
// on rather than an outcome indistinguishable from the first — the same
// re-runnability property MoveAccount's DuplicateAccountException provides.
//
// The operation takes no input and the model gives it no output shape.
func (p *OrganizationsPlugin) deleteResourcePolicy(reqCtx *orgCaller, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if reqCtx.isMember() {
		return nil, orgResourcePolicyAccessDenied("DeleteResourcePolicy")
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("deleteResourcePolicy ensure org: %w", err)
	}

	policy, found, err := p.loadResourcePolicy(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("deleteResourcePolicy load: %w", err)
	}
	if !found {
		return nil, orgResourcePolicyNotFound()
	}

	if err := p.state.Delete(goCtx, organizationsNamespace, orgResourcePolicyKey(reqCtx.AccountID)); err != nil {
		return nil, fmt.Errorf("deleteResourcePolicy delete: %w", err)
	}
	// The policy's tags go with it. Leaving them behind would let a later
	// PutResourcePolicy that mints a fresh ID collide with a stale tag set, and
	// would keep an aws:ResourceTag condition matching a resource that is gone.
	if err := p.state.Delete(goCtx, organizationsNamespace, orgTagsKey(policy.ResourcePolicySummary.ID)); err != nil {
		return nil, fmt.Errorf("deleteResourcePolicy delete tags: %w", err)
	}
	return orgEmptyResponse(), nil
}

// --- helpers ---

// loadResourcePolicy returns the organization's resource policy and whether one
// is set.
func (p *OrganizationsPlugin) loadResourcePolicy(ctx context.Context, acct string) (OrgResourcePolicy, bool, error) {
	var policy OrgResourcePolicy
	found, err := p.orgGetJSON(ctx, orgResourcePolicyKey(acct), &policy)
	if err != nil {
		return OrgResourcePolicy{}, false, err
	}
	return policy, found, nil
}

// orgResourcePolicyNotFound returns ResourcePolicyNotFoundException.
func orgResourcePolicyNotFound() *AWSError {
	return orgErr("ResourcePolicyNotFoundException",
		"we can't find a resource policy for this organization")
}

// orgResourcePolicyAccessDenied returns AccessDeniedException for a member account
// calling an operation reserved to the management account.
//
// This is HTTP 403, not the 400 every other Organizations error here uses:
// AccessDeniedException is a *common* error rather than one the service's model
// declares, and the Organizations API reference's Common Errors page gives it 403.
// The distinction is what an SDK's retry classifier reads, so answering 400 would
// make a denial look like a malformed request.
//
// The message names the operation because all three refuse in the same place: a
// caller that logged only the message would otherwise not know which of its calls
// was the one management reserved.
func orgResourcePolicyAccessDenied(op string) *AWSError {
	return &AWSError{
		Code: "AccessDeniedException",
		Message: fmt.Sprintf(
			"You don't have permissions to perform the requested operation. %s can be called only from the management account%s",
			op, orgResourcePolicyCallerScope(op)),
		HTTPStatus: http.StatusForbidden,
	}
}

// orgResourcePolicyCallerScope completes the denial message with the set of
// callers the operation does admit, which differs between the read and the writes.
func orgResourcePolicyCallerScope(op string) string {
	if op == "DescribeResourcePolicy" {
		return " or a member account that is a delegated administrator."
	}
	return "."
}

// orgPolicyDelegatesTo reports whether the organization's resource policy makes
// account a delegated administrator.
//
// Naming the account as a Principal is the test, and it is the whole test. Every
// delegation policy AWS documents names the member as
// "arn:aws:iam::<account>:root" in the Principal of each statement, so the
// principal is what distinguishes a delegated administrator from any other member.
//
// The Action element deliberately is not consulted. DescribeResourcePolicy appears
// in none of AWS's example delegation policies — the actions delegated are the
// *policy-management* ones a delegated administrator goes on to call — so
// requiring an Allow for it would deny every member that AWS's own examples
// intend to admit. Evaluating the document as an authorization decision per
// operation is a larger design than #619 asks for and would need the
// organizations:PolicyType condition key substrate does not model; the reason
// it is not attempted here is that a guessed denial fails a request AWS accepts.
//
// An unparseable document delegates to nobody. PutResourcePolicy already refuses
// content that is not JSON, so this is reachable only from a hand-seeded state
// store, and treating garbage as a grant would be the wrong direction to fail.
func orgPolicyDelegatesTo(policy OrgResourcePolicy, found bool, account string) bool {
	if !found || account == "" {
		return false
	}
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(policy.Content), &doc); err != nil {
		return false
	}
	principal := fmt.Sprintf("arn:aws:iam::%s:root", account)
	for _, stmt := range doc.Statement {
		// Only an Allow delegates. A Deny naming the account is the opposite, and a
		// statement with no Principal at all names nobody in a resource policy —
		// principalMatches treats an absent Principal as matching every caller,
		// which is right for an identity policy and wrong here.
		if !strings.EqualFold(stmt.Effect, "Allow") || stmt.Principal == nil {
			continue
		}
		if principalCovered(stmt.Principal, principal) {
			return true
		}
	}
	return false
}

// validateOrgResourcePolicyContent enforces ResourcePolicyContent's bounds and
// checks the document parses.
//
// The length bounds come first, so an empty document is MIN_LENGTH_EXCEEDED rather
// than a JSON complaint about a document the caller never wrote.
//
// The JSON check is not implied by the model's pattern, which is "[\s\S]*" — any
// text at all — but by the INVALID_RESOURCE_POLICY_JSON member of the model's
// InvalidInputExceptionReason enum: AWS parses the document, so accepting an
// unparseable one here would pass a test that real Organizations refuses.
//
// Only parseability is checked. The enum also carries INVALID_PRINCIPAL,
// UNSUPPORTED_ACTION_IN_RESOURCE_POLICY, UNSUPPORTED_RESOURCE_IN_RESOURCE_POLICY and
// UNSUPPORTED_POLICY_TYPE_IN_RESOURCE_POLICY, which are refusals about the
// document's *meaning*. The sets of principals, actions and resources AWS accepts in
// a resource policy are not in the API model, so emitting those reasons would mean
// guessing at their boundaries — and a guessed refusal is worse than a missing one:
// it fails a document AWS would have accepted, with a reason the caller cannot act
// on.
func validateOrgResourcePolicyContent(content string) error {
	n := utf8.RuneCountInString(content)
	switch {
	case n < orgMinResourcePolicyChars:
		return orgInvalidInput("MIN_LENGTH_EXCEEDED",
			fmt.Sprintf("the resource policy content must be at least %d character long", orgMinResourcePolicyChars))
	case n > orgMaxResourcePolicyChars:
		return orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("the resource policy content is longer than the %d-character maximum", orgMaxResourcePolicyChars))
	}
	if !json.Valid([]byte(content)) {
		return orgInvalidInput("INVALID_RESOURCE_POLICY_JSON",
			"the resource policy content is not valid JSON")
	}
	return nil
}
