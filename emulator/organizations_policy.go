package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// orgPolicyTypes is the PolicyType enum from the Organizations API model. A value
// outside it is a syntax error the caller can fix by correcting a string; a value
// inside it that substrate does not model is a different problem entirely, and
// the two get different codes so a caller can tell a typo from an unsupported
// feature.
var orgPolicyTypes = []string{
	"SERVICE_CONTROL_POLICY",
	"RESOURCE_CONTROL_POLICY",
	"TAG_POLICY",
	"BACKUP_POLICY",
	"AISERVICES_OPT_OUT_POLICY",
	"CHATBOT_POLICY",
	"DECLARATIVE_POLICY_EC2",
	"SECURITYHUB_POLICY",
	"INSPECTOR_POLICY",
	"UPGRADE_ROLLOUT_POLICY",
	"BEDROCK_POLICY",
	"S3_POLICY",
	"NETWORK_SECURITY_DIRECTOR_POLICY",
}

// orgPolicyTypeStatusEnabled is the PolicyTypeStatus a root reports for a policy
// type EnablePolicyType has turned on. The model also declares PENDING_ENABLE and
// PENDING_DISABLE; substrate does not use them, because a transition no caller can
// observe the end of would make a poll loop wait for a state change that never
// arrives.
const orgPolicyTypeStatusEnabled = "ENABLED"

// policyOperation claims the service control policy lifecycle operations. The
// policy, attachment and FullAWSAccess stores live in organizations_state.go;
// this file owns only the operations over them.
func (p *OrganizationsPlugin) policyOperation(op string) (orgHandler, bool) {
	switch op {
	case "CreatePolicy":
		return p.createPolicy, true
	case "UpdatePolicy":
		return p.updatePolicy, true
	case "DeletePolicy":
		return p.deletePolicy, true
	case "DescribePolicy":
		return p.describePolicy, true
	case "ListPolicies":
		return p.listPolicies, true
	case "AttachPolicy":
		return p.attachPolicy, true
	case "DetachPolicy":
		return p.detachPolicy, true
	case "ListPoliciesForTarget":
		return p.listPoliciesForTarget, true
	case "ListTargetsForPolicy":
		return p.listTargetsForPolicy, true
	case "EnablePolicyType":
		return p.enablePolicyType, true
	case "DisablePolicyType":
		return p.disablePolicyType, true
	default:
		return nil, false
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

// --- CreatePolicy ---

func (p *OrganizationsPlugin) createPolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		Content     string        `json:"Content"`
		Description *string       `json:"Description"`
		Name        string        `json:"Name"`
		Type        string        `json:"Type"`
		Tags        []orgTagInput `json:"Tags"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	// Tags go through the same validation TagResource applies, before anything is
	// written: a key that operation refuses must not be plantable through a create,
	// and a half-created policy would make the caller's retry collide with a
	// DuplicatePolicyException for a policy it does not believe it created.
	tags, err := validateOrgCreateTags(input.Tags)
	if err != nil {
		return nil, err
	}

	// Content, Description, Name and Type are all required in the model — including
	// Description, which reads as optional but is not.
	if input.Name == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter Name.")
	}
	if input.Content == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter Content.")
	}
	if input.Description == nil {
		return nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter Description.")
	}
	if input.Type == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter Type.")
	}
	if err := orgCheckPolicyType(input.Type); err != nil {
		return nil, err
	}

	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createPolicy ensure org: %w", err)
	}
	available, err := p.policyTypeAvailable(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createPolicy availability: %w", err)
	}
	// A type that is valid in the enum but not SERVICE_CONTROL_POLICY gets the same
	// refusal as an unavailable one: substrate models no other policy type, so from
	// the caller's side the type is not usable in this organization. Under
	// CONSOLIDATED_BILLING even SCPs are refused here, and this is deliberately the
	// one place the feature set is named — it is the first call a governance tool
	// makes, so it is where the diagnosis has to be legible.
	if !available || input.Type != orgPolicyTypeSCP {
		return nil, orgErr("PolicyTypeNotAvailableForOrganizationException",
			"You can't use the specified policy type with the feature set currently enabled for this organization")
	}

	if err := orgCheckPolicyName(input.Name); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyDescription(*input.Description); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyContent(input.Content); err != nil {
		return nil, err
	}

	ids, err := p.loadPolicyIDs(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createPolicy load ids: %w", err)
	}
	// The quota counts the policies the organization owns; p-FullAWSAccess is
	// AWS-managed and is not stored, so it is not one of them.
	if orgPolicyNumberLimitExceeded(len(ids)) {
		return nil, orgConstraintViolation("POLICY_NUMBER_LIMIT_EXCEEDED",
			fmt.Sprintf("You have exceeded the number of policies allowed in an organization (%d)", orgMaxSCPsPerOrg))
	}
	taken, err := p.policyNameTaken(goCtx, ids, input.Name, "")
	if err != nil {
		return nil, fmt.Errorf("createPolicy name check: %w", err)
	}
	// Creating is not idempotent: a re-run of a governance script has to be able to
	// tell "I already made this" from "I made it now", and the name is the only
	// handle it has before it knows the ID.
	if taken {
		return nil, orgErr("DuplicatePolicyException", "A policy with the same name already exists")
	}

	policyID := "p-" + randomLowerAlphanum(orgPolicyIDSuffixLen)
	pol := OrgPolicy{
		PolicySummary: OrgPolicySummary{
			ID:          policyID,
			Arn:         orgPolicyArn(reqCtx.AccountID, org.ID, policyID),
			Name:        input.Name,
			Description: *input.Description,
			Type:        orgPolicyTypeSCP,
			AwsManaged:  false,
		},
		Content: input.Content,
	}
	if err := p.savePolicy(goCtx, reqCtx.AccountID, pol); err != nil {
		return nil, fmt.Errorf("createPolicy save: %w", err)
	}
	// Tags supplied at creation are stored so ListTagsForResource can read them
	// back, and so a policy condition on aws:ResourceTag sees them.
	if len(tags) > 0 {
		if err := p.saveTags(goCtx, policyID, tags); err != nil {
			return nil, fmt.Errorf("createPolicy save tags: %w", err)
		}
	}
	return orgJSONResponse(map[string]interface{}{"Policy": pol}, "createPolicy")
}

// --- UpdatePolicy ---

func (p *OrganizationsPlugin) updatePolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	// Name, Description and Content are pointers because UpdatePolicy is a partial
	// update: an omitted member leaves the stored value alone, while an empty string
	// is a value the caller asked for. Collapsing the two would silently blank a
	// description on every rename.
	var input struct {
		PolicyID    string  `json:"PolicyId"`
		Name        *string `json:"Name"`
		Description *string `json:"Description"`
		Content     *string `json:"Content"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyID(input.PolicyID); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("updatePolicy ensure org: %w", err)
	}
	if input.PolicyID == orgFullAWSAccessID {
		return nil, orgImmutablePolicy()
	}

	pol, err := p.loadVisiblePolicy(goCtx, reqCtx.AccountID, input.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("updatePolicy load: %w", err)
	}
	if pol == nil {
		return nil, orgPolicyNotFound()
	}

	if input.Name != nil && *input.Name != pol.PolicySummary.Name {
		if err := orgCheckPolicyName(*input.Name); err != nil {
			return nil, err
		}
		ids, idErr := p.loadPolicyIDs(goCtx, reqCtx.AccountID)
		if idErr != nil {
			return nil, fmt.Errorf("updatePolicy load ids: %w", idErr)
		}
		taken, takenErr := p.policyNameTaken(goCtx, ids, *input.Name, input.PolicyID)
		if takenErr != nil {
			return nil, fmt.Errorf("updatePolicy name check: %w", takenErr)
		}
		if taken {
			return nil, orgErr("DuplicatePolicyException", "A policy with the same name already exists")
		}
		pol.PolicySummary.Name = *input.Name
	}
	if input.Description != nil {
		if err := orgCheckPolicyDescription(*input.Description); err != nil {
			return nil, err
		}
		pol.PolicySummary.Description = *input.Description
	}
	if input.Content != nil {
		if err := orgCheckPolicyContent(*input.Content); err != nil {
			return nil, err
		}
		pol.Content = *input.Content
	}

	if err := p.savePolicy(goCtx, reqCtx.AccountID, *pol); err != nil {
		return nil, fmt.Errorf("updatePolicy save: %w", err)
	}
	return orgJSONResponse(map[string]interface{}{"Policy": *pol}, "updatePolicy")
}

// --- DeletePolicy ---

func (p *OrganizationsPlugin) deletePolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		PolicyID string `json:"PolicyId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyID(input.PolicyID); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("deletePolicy ensure org: %w", err)
	}
	if input.PolicyID == orgFullAWSAccessID {
		return nil, orgImmutablePolicy()
	}

	pol, err := p.loadVisiblePolicy(goCtx, reqCtx.AccountID, input.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("deletePolicy load: %w", err)
	}
	if pol == nil {
		return nil, orgPolicyNotFound()
	}

	targets, err := p.loadPolicyTargets(goCtx, input.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("deletePolicy load targets: %w", err)
	}
	// Deleting an attached policy is refused rather than cascading. A cascade would
	// silently remove a guardrail from every entity it was attached to, and a
	// teardown script that deletes in the wrong order would look like it worked.
	if len(targets) > 0 {
		return nil, orgErr("PolicyInUseException",
			"The policy is attached to one or more entities. You must detach it from all roots, OUs, and accounts before performing this operation")
	}

	if err := p.deletePolicyRecord(goCtx, reqCtx.AccountID, input.PolicyID); err != nil {
		return nil, fmt.Errorf("deletePolicy delete: %w", err)
	}
	return orgEmptyResponse(), nil
}

// deletePolicyRecord removes a policy, its index entry, its (empty) target index
// and its tags. The tags go with it so a later resource never inherits the tags of
// a policy that no longer exists — which would fail open on a tag-gated policy.
func (p *OrganizationsPlugin) deletePolicyRecord(ctx context.Context, acct, policyID string) error {
	if _, err := p.orgRemoveID(ctx, orgPolicyIDsKey(acct), policyID); err != nil {
		return err
	}
	for _, key := range []string{orgPolicyKey(policyID), orgPolicyTargetsKey(policyID), orgTagsKey(policyID)} {
		if err := p.state.Delete(ctx, organizationsNamespace, key); err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}

// --- DescribePolicy ---

func (p *OrganizationsPlugin) describePolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		PolicyID string `json:"PolicyId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyID(input.PolicyID); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("describePolicy ensure org: %w", err)
	}

	pol, err := p.loadVisiblePolicy(goCtx, reqCtx.AccountID, input.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("describePolicy load: %w", err)
	}
	if pol == nil {
		return nil, orgPolicyNotFound()
	}
	return orgJSONResponse(map[string]interface{}{"Policy": *pol}, "describePolicy")
}

// --- ListPolicies ---

func (p *OrganizationsPlugin) listPolicies(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		Filter     string `json:"Filter"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	// Filter is required in the model, and defaulting it would be the wrong kindness:
	// a caller that meant to list tag policies and got SCPs would draw the opposite
	// conclusion about what governs its accounts.
	if input.Filter == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter Filter.")
	}
	if err := orgCheckPolicyType(input.Filter); err != nil {
		return nil, err
	}

	ids, err := p.visiblePolicyIDs(goCtx, reqCtx.AccountID, input.Filter)
	if err != nil {
		return nil, fmt.Errorf("listPolicies load ids: %w", err)
	}
	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}
	summaries, err := p.policySummaries(goCtx, page)
	if err != nil {
		return nil, fmt.Errorf("listPolicies load policies: %w", err)
	}

	out := map[string]interface{}{"Policies": summaries}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listPolicies")
}

// visiblePolicyIDs returns the policy IDs a listing filtered by policyType should
// report. Only SERVICE_CONTROL_POLICY has any members: substrate models no other
// type, so a valid filter naming one answers with an empty page rather than a
// refusal — an organization with no tag policies is exactly what AWS reports as an
// empty list, and the API model offers no code to refuse with here.
func (p *OrganizationsPlugin) visiblePolicyIDs(ctx context.Context, acct, policyType string) ([]string, error) {
	if policyType != orgPolicyTypeSCP {
		return nil, nil
	}
	available, err := p.policyTypeAvailable(ctx, acct)
	if err != nil {
		return nil, err
	}
	if !available {
		return nil, nil
	}
	ids, err := p.loadPolicyIDs(ctx, acct)
	if err != nil {
		return nil, err
	}
	// FullAWSAccess is not in the stored index because it is synthesized, but it is
	// a policy of the organization and every listing has to show it — a caller that
	// concludes a fresh organization has no SCPs would also conclude nothing is
	// allowed, which is the reverse of the truth.
	return append(ids, orgFullAWSAccessID), nil
}

// policySummaries loads a page of policies as summaries, skipping an ID whose
// record has gone. A zero-valued summary in a listing is worse than a short page:
// a caller iterating it would call DescribePolicy with "" and get a refusal it has
// nothing to trace to.
func (p *OrganizationsPlugin) policySummaries(ctx context.Context, ids []string) ([]OrgPolicySummary, error) {
	summaries := make([]OrgPolicySummary, 0, len(ids))
	for _, id := range ids {
		pol, err := p.loadPolicy(ctx, id)
		if err != nil {
			return nil, err
		}
		if pol == nil {
			continue
		}
		summaries = append(summaries, pol.PolicySummary)
	}
	return summaries, nil
}

// --- AttachPolicy ---

func (p *OrganizationsPlugin) attachPolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		PolicyID string `json:"PolicyId"`
		TargetID string `json:"TargetId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyID(input.PolicyID); err != nil {
		return nil, err
	}
	if err := orgCheckTargetID(input.TargetID); err != nil {
		return nil, err
	}

	pol, kind, err := p.resolveAttachment(goCtx, reqCtx.AccountID, input.PolicyID, input.TargetID)
	if err != nil {
		return nil, fmt.Errorf("attachPolicy resolve: %w", err)
	}
	if pol == nil {
		return nil, orgPolicyNotFound()
	}
	if kind == "" {
		return nil, orgTargetNotFound()
	}

	enabled, err := p.scpEnabled(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("attachPolicy enablement: %w", err)
	}
	// This is issue #578's point 6. The policy exists and the target exists, and a
	// caller that treats create-then-attach as one step has already recorded the
	// policy as applied. Refusing here — rather than attaching something the root
	// would not evaluate — is what makes the unenforced-guardrail state observable.
	if !enabled {
		return nil, orgErr("PolicyTypeNotEnabledException",
			"The specified policy type isn't currently enabled in this root. You can't attach policies of the specified type to entities in a root until you enable that type in the root")
	}

	attached, err := p.loadAttachments(goCtx, input.TargetID)
	if err != nil {
		return nil, fmt.Errorf("attachPolicy load attachments: %w", err)
	}
	if slices.Contains(attached, input.PolicyID) {
		return nil, orgErr("DuplicatePolicyAttachmentException", "The selected policy is already attached to the specified target")
	}
	if len(attached) >= orgMaxSCPsPerTarget {
		return nil, orgConstraintViolation("MAX_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED",
			fmt.Sprintf("You have exceeded the number of policies of type SERVICE_CONTROL_POLICY that can be attached to one entity (%d)", orgMaxSCPsPerTarget))
	}

	if _, err := p.attachPolicyTo(goCtx, input.PolicyID, input.TargetID); err != nil {
		return nil, fmt.Errorf("attachPolicy attach: %w", err)
	}
	return orgEmptyResponse(), nil
}

// --- DetachPolicy ---

func (p *OrganizationsPlugin) detachPolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		PolicyID string `json:"PolicyId"`
		TargetID string `json:"TargetId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyID(input.PolicyID); err != nil {
		return nil, err
	}
	if err := orgCheckTargetID(input.TargetID); err != nil {
		return nil, err
	}

	pol, kind, err := p.resolveAttachment(goCtx, reqCtx.AccountID, input.PolicyID, input.TargetID)
	if err != nil {
		return nil, fmt.Errorf("detachPolicy resolve: %w", err)
	}
	if pol == nil {
		return nil, orgPolicyNotFound()
	}
	if kind == "" {
		return nil, orgTargetNotFound()
	}

	attached, err := p.loadAttachments(goCtx, input.TargetID)
	if err != nil {
		return nil, fmt.Errorf("detachPolicy load attachments: %w", err)
	}
	if !slices.Contains(attached, input.PolicyID) {
		return nil, orgErr("PolicyNotAttachedException", "The policy isn't attached to the specified target in the specified root")
	}

	enabled, err := p.scpEnabled(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("detachPolicy enablement: %w", err)
	}
	// The floor is why FullAWSAccess can be detached from a target that has another
	// SCP but not from one that has only FullAWSAccess. An entity with the type
	// enabled and no SCP attached denies everything in real AWS, so a detach that
	// took the last one would produce the opposite of what the caller intended, and
	// would produce it silently. The floor does not apply while the type is
	// disabled: nothing is attached then, and nothing is evaluated either.
	if enabled && len(attached) <= orgMinSCPsPerTarget {
		return nil, orgConstraintViolation("MIN_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED",
			fmt.Sprintf("You must have at least %d policy of type SERVICE_CONTROL_POLICY attached to this entity", orgMinSCPsPerTarget))
	}

	if _, err := p.detachPolicyFrom(goCtx, input.PolicyID, input.TargetID); err != nil {
		return nil, fmt.Errorf("detachPolicy detach: %w", err)
	}
	return orgEmptyResponse(), nil
}

// resolveAttachment loads the policy and resolves the target for an attach or a
// detach. It returns a nil policy for "no such policy" and an empty kind for "no
// such target", so the caller can pick the code its operation documents.
func (p *OrganizationsPlugin) resolveAttachment(ctx context.Context, acct, policyID, targetID string) (*OrgPolicy, string, error) {
	if _, err := p.ensureOrganization(ctx, acct); err != nil {
		return nil, "", err
	}
	pol, err := p.loadVisiblePolicy(ctx, acct, policyID)
	if err != nil {
		return nil, "", err
	}
	if pol == nil {
		return nil, "", nil
	}
	kind, err := p.resolveOrgTarget(ctx, acct, targetID)
	if err != nil {
		return nil, "", err
	}
	// A policy is a valid tag target but not a valid attachment target; the syntax
	// check upstream already rules its ID out, so anything else is not attachable.
	if kind == orgKindPolicy {
		kind = ""
	}
	return pol, kind, nil
}

// --- ListPoliciesForTarget ---

func (p *OrganizationsPlugin) listPoliciesForTarget(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		TargetID   string `json:"TargetId"`
		Filter     string `json:"Filter"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckTargetID(input.TargetID); err != nil {
		return nil, err
	}
	if input.Filter == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter Filter.")
	}
	if err := orgCheckPolicyType(input.Filter); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listPoliciesForTarget ensure org: %w", err)
	}

	kind, err := p.resolveOrgTarget(goCtx, reqCtx.AccountID, input.TargetID)
	if err != nil {
		return nil, fmt.Errorf("listPoliciesForTarget resolve target: %w", err)
	}
	if kind == "" || kind == orgKindPolicy {
		return nil, orgTargetNotFound()
	}

	available, err := p.policyTypeAvailable(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("listPoliciesForTarget availability: %w", err)
	}
	// An empty page, not a refusal, for a type substrate does not model or an
	// organization that can hold none: that is what AWS reports for a target with no
	// policies of the filtered type, and the model gives this operation no code to
	// refuse with. The listing reports only *directly* attached policies, never
	// inherited ones — which is why a caller checking whether an account is governed
	// has to walk the hierarchy itself rather than trusting one call.
	var ids []string
	if available && input.Filter == orgPolicyTypeSCP {
		ids, err = p.loadAttachments(goCtx, input.TargetID)
		if err != nil {
			return nil, fmt.Errorf("listPoliciesForTarget load attachments: %w", err)
		}
	}

	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}
	summaries, err := p.policySummaries(goCtx, page)
	if err != nil {
		return nil, fmt.Errorf("listPoliciesForTarget load policies: %w", err)
	}

	out := map[string]interface{}{"Policies": summaries}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listPoliciesForTarget")
}

// --- ListTargetsForPolicy ---

func (p *OrganizationsPlugin) listTargetsForPolicy(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		PolicyID   string `json:"PolicyId"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := orgCheckPolicyID(input.PolicyID); err != nil {
		return nil, err
	}
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listTargetsForPolicy ensure org: %w", err)
	}

	pol, err := p.loadVisiblePolicy(goCtx, reqCtx.AccountID, input.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("listTargetsForPolicy load: %w", err)
	}
	if pol == nil {
		return nil, orgPolicyNotFound()
	}

	ids, err := p.loadPolicyTargets(goCtx, input.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("listTargetsForPolicy load targets: %w", err)
	}
	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	targets := make([]OrgPolicyTargetSummary, 0, len(page))
	for _, id := range page {
		summary, sumErr := p.policyTargetSummary(goCtx, reqCtx.AccountID, id)
		if sumErr != nil {
			return nil, fmt.Errorf("listTargetsForPolicy load target: %w", sumErr)
		}
		if summary == nil {
			continue
		}
		targets = append(targets, *summary)
	}

	out := map[string]interface{}{"Targets": targets}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listTargetsForPolicy")
}

// policyTargetSummary describes an attachment target, or returns (nil, nil) when
// the entity behind the index entry has gone.
func (p *OrganizationsPlugin) policyTargetSummary(ctx context.Context, acct, targetID string) (*OrgPolicyTargetSummary, error) {
	switch {
	case isOrgRootID(targetID):
		root, err := p.loadRoot(ctx, acct)
		if err != nil {
			return nil, err
		}
		if root.ID != targetID {
			return nil, nil //nolint:nilnil // (nil, nil) = "the entity has gone", skipped by caller.
		}
		return &OrgPolicyTargetSummary{TargetID: root.ID, Arn: root.Arn, Name: root.Name, Type: orgKindRoot}, nil
	case isOrgOUID(targetID):
		ou, err := p.loadOU(ctx, targetID)
		if err != nil {
			return nil, err
		}
		if ou == nil {
			return nil, nil //nolint:nilnil // (nil, nil) = "the entity has gone", skipped by caller.
		}
		return &OrgPolicyTargetSummary{TargetID: ou.ID, Arn: ou.Arn, Name: ou.Name, Type: orgKindOU}, nil
	default:
		a, err := p.loadAccount(ctx, targetID)
		if err != nil {
			return nil, err
		}
		if a == nil {
			return nil, nil //nolint:nilnil // (nil, nil) = "the entity has gone", skipped by caller.
		}
		return &OrgPolicyTargetSummary{TargetID: a.ID, Arn: a.Arn, Name: a.Name, Type: orgKindAccount}, nil
	}
}

// --- EnablePolicyType ---

func (p *OrganizationsPlugin) enablePolicyType(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	input, root, err := p.policyTypeRequest(goCtx, reqCtx, req)
	if err != nil {
		return nil, err
	}

	available, err := p.policyTypeAvailable(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("enablePolicyType availability: %w", err)
	}
	// EnablePolicyType is the second operation whose model error list declares the
	// feature-set refusal, and it is the natural place a caller lands after
	// CreatePolicy told it the type is unavailable. Answering anything else here
	// would send it round the same loop.
	if !available || input.PolicyType != orgPolicyTypeSCP {
		return nil, orgErr("PolicyTypeNotAvailableForOrganizationException",
			"You can't use the specified policy type with the feature set currently enabled for this organization")
	}
	if scpEnabledOnStoredRoot(*root) {
		return nil, orgErr("PolicyTypeAlreadyEnabledException", "The specified policy type is already enabled in the specified root")
	}

	root.PolicyTypes = append(root.PolicyTypes, OrgPolicyTypeSummary{Type: orgPolicyTypeSCP, Status: orgPolicyTypeStatusEnabled})
	// The root is written before the attachments, because attachFullAWSAccess reads
	// the root to decide whether the type is enabled at all.
	if err := p.saveRoot(goCtx, reqCtx.AccountID, *root); err != nil {
		return nil, fmt.Errorf("enablePolicyType save root: %w", err)
	}

	// Re-enabling restores only FullAWSAccess. The attachments a previous
	// DisablePolicyType removed are gone, which is what the User Guide documents and
	// what makes disable-then-enable a destructive round trip rather than a toggle:
	// a caller that expected its SCPs back has to reattach them, and this is where
	// it finds out.
	entities, err := p.rootSubtree(goCtx, reqCtx.AccountID, root.ID)
	if err != nil {
		return nil, fmt.Errorf("enablePolicyType load entities: %w", err)
	}
	for _, id := range entities {
		if _, err := p.attachPolicyTo(goCtx, orgFullAWSAccessID, id); err != nil {
			return nil, fmt.Errorf("enablePolicyType attach FullAWSAccess: %w", err)
		}
	}
	return orgJSONResponse(map[string]interface{}{"Root": orgRootWithPolicyTypes(*root)}, "enablePolicyType")
}

// --- DisablePolicyType ---

func (p *OrganizationsPlugin) disablePolicyType(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	input, root, err := p.policyTypeRequest(goCtx, reqCtx, req)
	if err != nil {
		return nil, err
	}

	available, err := p.policyTypeAvailable(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("disablePolicyType availability: %w", err)
	}
	// One code covers "never available", "not the type substrate models" and
	// "already disabled", because PolicyTypeNotEnabledException is the only refusal
	// DisablePolicyType's error list declares — the model gives it no feature-set
	// code — and all three mean the same thing to the caller: there is nothing here
	// to turn off.
	if !available || input.PolicyType != orgPolicyTypeSCP || !scpEnabledOnStoredRoot(*root) {
		return nil, orgErr("PolicyTypeNotEnabledException",
			"The specified policy type isn't currently enabled in this root")
	}

	// Every SCP is detached from every entity in the root, and the attachments are
	// not remembered. This is the User Guide's behavior, and it is the whole reason
	// this operation exists in substrate: it is the only route to an all-features
	// organization with SCPs off, the state where CreatePolicy still succeeds and
	// nothing is enforced.
	entities, err := p.rootSubtree(goCtx, reqCtx.AccountID, root.ID)
	if err != nil {
		return nil, fmt.Errorf("disablePolicyType load entities: %w", err)
	}
	for _, id := range entities {
		attached, attErr := p.loadAttachments(goCtx, id)
		if attErr != nil {
			return nil, fmt.Errorf("disablePolicyType load attachments: %w", attErr)
		}
		for _, policyID := range attached {
			if _, detErr := p.detachPolicyFrom(goCtx, policyID, id); detErr != nil {
				return nil, fmt.Errorf("disablePolicyType detach: %w", detErr)
			}
		}
	}

	root.PolicyTypes = slices.DeleteFunc(root.PolicyTypes, func(pt OrgPolicyTypeSummary) bool {
		return pt.Type == orgPolicyTypeSCP
	})
	if err := p.saveRoot(goCtx, reqCtx.AccountID, *root); err != nil {
		return nil, fmt.Errorf("disablePolicyType save root: %w", err)
	}
	return orgJSONResponse(map[string]interface{}{"Root": orgRootWithPolicyTypes(*root)}, "disablePolicyType")
}

// orgPolicyTypeInput is the input EnablePolicyType and DisablePolicyType share.
type orgPolicyTypeInput struct {
	RootID     string `json:"RootId"`
	PolicyType string `json:"PolicyType"`
}

// policyTypeRequest decodes and validates the input EnablePolicyType and
// DisablePolicyType share, and returns the stored root. The stored root is
// deliberately not the one loadRoot returns: loadRoot masks PolicyTypes under
// CONSOLIDATED_BILLING, so writing it back would erase an enablement the
// organization would get back if the feature-set seed were cleared.
func (p *OrganizationsPlugin) policyTypeRequest(ctx context.Context, reqCtx *orgCaller, req *AWSRequest) (orgPolicyTypeInput, *OrgRoot, error) {
	var input orgPolicyTypeInput
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return input, nil, err
	}
	if input.RootID == "" {
		return input, nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter RootId.")
	}
	if input.PolicyType == "" {
		return input, nil, orgInvalidInput("INPUT_REQUIRED", "You must specify a value for the parameter PolicyType.")
	}
	if err := orgCheckPolicyType(input.PolicyType); err != nil {
		return input, nil, err
	}

	root, err := p.loadStoredRoot(ctx, reqCtx.AccountID)
	if err != nil {
		return input, nil, fmt.Errorf("policy type request load root: %w", err)
	}
	// The root ID is checked before the feature set: a caller that named the wrong
	// root has a different bug from one whose organization cannot hold the type, and
	// telling it about the feature set would send it to fix the wrong thing.
	if root.ID != input.RootID {
		return input, nil, orgErr("RootNotFoundException", "We can't find a root with the RootId that you specified")
	}
	return input, root, nil
}

// scpEnabledOnStoredRoot reports whether root carries SERVICE_CONTROL_POLICY as
// ENABLED.
func scpEnabledOnStoredRoot(root OrgRoot) bool {
	return slices.ContainsFunc(root.PolicyTypes, func(pt OrgPolicyTypeSummary) bool {
		return pt.Type == orgPolicyTypeSCP && pt.Status == orgPolicyTypeStatusEnabled
	})
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

// orgRootWithPolicyTypes returns the root with PolicyTypes as an empty list rather
// than null when nothing is enabled. The model's member is a list, and a caller
// ranging over a null gets a nil-pointer panic in a typed SDK rather than an empty
// loop.
func orgRootWithPolicyTypes(root OrgRoot) OrgRoot {
	if root.PolicyTypes == nil {
		root.PolicyTypes = []OrgPolicyTypeSummary{}
	}
	return root
}

// --- shared refusals ---

// orgPolicyNotFound reports a policy the organization does not contain.
func orgPolicyNotFound() *AWSError {
	return orgErr("PolicyNotFoundException", "We can't find a policy with the PolicyId that you specified")
}

// orgTargetNotFound reports an attachment target the organization does not
// contain.
func orgTargetNotFound() *AWSError {
	return orgErr("TargetNotFoundException",
		"We can't find a root, OU, account, or policy with the TargetId that you specified")
}

// orgImmutablePolicy refuses a write to an AWS-managed policy. p-FullAWSAccess is
// the only one substrate has, and the refusal matters because a teardown that
// deletes every policy it can list would otherwise remove the policy the
// minimum-attachment rule depends on.
func orgImmutablePolicy() *AWSError {
	return orgInvalidInput("IMMUTABLE_POLICY",
		"You specified a policy that is managed by Amazon Web Services and can't be modified")
}

// --- input validation ---

// orgPolicyIDSuffixLen is the length of the generated part of a policy ID. The
// model gives two patterns for the same identity: PolicyId admits 8 to 128
// characters, while PolicyArn requires 10 to 32 in the ARN it embeds. Ten
// satisfies both, so a generated policy's ID and ARN are each valid against the
// model's own pattern.
const orgPolicyIDSuffixLen = 10

// orgPolicyNumberLimitExceeded reports whether an organization already holding
// count policies is at the quota. It is a function rather than an inline
// comparison so the boundary can be pinned without creating orgMaxSCPsPerOrg
// policies: an off-by-one here is invisible in every ordinary run.
func orgPolicyNumberLimitExceeded(count int) bool { return count >= orgMaxSCPsPerOrg }

// orgPolicyArn returns the ARN of an organization-owned policy. AWS-managed
// policies use a different form, owned by "aws" rather than by the management
// account; see orgFullAWSAccessArn.
func orgPolicyArn(acct, orgID, policyID string) string {
	return fmt.Sprintf("arn:aws:organizations::%s:policy/%s/service_control_policy/%s", acct, orgID, policyID)
}

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

// Policy string-length quotas, from the model's PolicyName and PolicyDescription
// shapes.
const (
	orgMaxPolicyNameChars        = 128
	orgMaxPolicyDescriptionChars = 512
)

// policyNameTaken reports whether any policy other than exceptID already carries
// name. FullAWSAccess is included: it is a policy of the organization, so a
// caller cannot shadow its name either.
func (p *OrganizationsPlugin) policyNameTaken(ctx context.Context, ids []string, name, exceptID string) (bool, error) {
	if name == orgFullAWSAccessName && exceptID != orgFullAWSAccessID {
		return true, nil
	}
	for _, id := range ids {
		if id == exceptID {
			continue
		}
		pol, err := p.loadPolicy(ctx, id)
		if err != nil {
			return false, err
		}
		if pol != nil && pol.PolicySummary.Name == name {
			return true, nil
		}
	}
	return false, nil
}

// isOrgPolicyIDSyntax reports whether id matches the model's PolicyId pattern,
// ^p-[0-9a-zA-Z_]{8,128}$.
func isOrgPolicyIDSyntax(id string) bool {
	rest, ok := strings.CutPrefix(id, "p-")
	if !ok {
		return false
	}
	return orgIDSegmentOK(rest, 8, 128, true)
}

// isOrgTargetIDSyntax reports whether id matches the model's PolicyTargetId
// pattern: a root, a 12-digit account ID, or an OU.
func isOrgTargetIDSyntax(id string) bool {
	if len(id) == 12 && orgIDSegmentOK(id, 12, 12, false) && !strings.ContainsFunc(id, func(r rune) bool {
		return r < '0' || r > '9'
	}) {
		return true
	}
	if rest, ok := strings.CutPrefix(id, "r-"); ok {
		return orgIDSegmentOK(rest, 4, 32, false)
	}
	if rest, ok := strings.CutPrefix(id, "ou-"); ok {
		parts := strings.Split(rest, "-")
		if len(parts) != 2 {
			return false
		}
		return orgIDSegmentOK(parts[0], 4, 32, false) && orgIDSegmentOK(parts[1], 8, 32, false)
	}
	return false
}

// orgIDSegmentOK reports whether s is minLen to maxLen characters drawn from
// lowercase letters and digits, widening to uppercase letters and "_" when wide is
// set — the two character classes the model's identifier patterns use.
func orgIDSegmentOK(s string, minLen, maxLen int, wide bool) bool {
	if len(s) < minLen || len(s) > maxLen {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case wide && (r >= 'A' && r <= 'Z' || r == '_'):
		default:
			return false
		}
	}
	return true
}
