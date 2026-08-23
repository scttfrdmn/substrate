package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The IAM policy simulator: SimulatePrincipalPolicy and SimulateCustomPolicy.
//
// These are the only AWS APIs that answer "may I?" without doing the thing. Every
// other operation substrate emulates answers "here is what happened", so the one
// code path in a consumer that decides whether to proceed at all was the one path
// with no emulator coverage (#579) — a preflight built on SimulatePrincipalPolicy
// could not be tested against substrate at all, and iam_managed.go had been
// granting iam:Simulate* in its bundled policies the whole time, advertising
// permission for an operation that answered InvalidAction.
//
// Both operations run the *same* evaluator substrate enforces with, EvaluateSourced
// (iam_eval.go), and differ only in where the policy set comes from. That is the
// point: a simulated decision that could disagree with an enforced one would be
// worse than no simulator, because a consumer would trust it. The three decisions
// the API reports map 1:1 to substrate's — allowed/explicitDeny/implicitDeny
// against DecisionAllow/DecisionDeny/DecisionImplicitDeny — so nothing is
// collapsed. #579 is explicit that collapsing them "would be worse than none,
// because it would look like coverage".
//
// Deliberately not evaluated, and stated in docs/services.md rather than answered
// wrongly:
//
//   - Service control policies. Organizations stores them and CheckAccess never
//     consults them either, so reporting an SCP bound here would report a limit
//     substrate does not enforce. OrganizationsDecisionDetail is therefore absent
//     rather than present-and-false.
//   - StartPosition/EndPosition on a matched statement: offsets into the document
//     as submitted, and substrate stores a parsed PolicyDocument.
//   - ResourceHandlingOption, PolicyExclusionList, EvalDecisionDetails and
//     ResourceSpecificResults — cross-account and EC2-scenario constructs whose
//     inputs substrate has nothing to resolve against.

// iamSimulateDefaultMaxItems is the page size the reference specifies: "If you do
// not include this parameter, the number of items defaults to 100".
const iamSimulateDefaultMaxItems = 100

// iamSimulateMaxMaxItems is the reference's "Valid Range: Minimum value of 1.
// Maximum value of 1000". There is no cap on the number of *actions* a call may
// name — #579 asserted one at 25, and no such limit appears anywhere in the API
// reference, so modeling it would refuse requests AWS accepts.
const iamSimulateMaxMaxItems = 1000

// iamSimulateRequest is the union of both operations' inputs. They differ only in
// which member is required, so one parser serves both.
type iamSimulateRequest struct {
	PolicySourceArn                    string   `json:"PolicySourceArn"`
	PolicyInputList                    []string `json:"PolicyInputList"`
	PermissionsBoundaryPolicyInputList []string `json:"PermissionsBoundaryPolicyInputList"`
	ActionNames                        []string `json:"ActionNames"`
	ResourceArns                       []string `json:"ResourceArns"`
	ResourcePolicy                     string   `json:"ResourcePolicy"`
	ResourceOwner                      string   `json:"ResourceOwner"`
	CallerArn                          string   `json:"CallerArn"`
	Marker                             string   `json:"Marker"`
	MaxItems                           iamInt   `json:"MaxItems"`

	// context holds the condition keys decoded from ContextEntries. It is not a
	// request member, so it carries no JSON tag: only parseSimulateRequest fills it,
	// from req.Params, because a nested member list has no JSON-body form at all.
	context map[string]string

	// contextMulti holds the same entries as context, keeping *every* value a caller
	// supplied for a key rather than only the first. A ContextEntry is a list by
	// construction (ContextKeyValues.member.N), so this is the map that matches the
	// wire shape; context is the single-valued projection of it (#690).
	contextMulti map[string][]string
}

// iamSimulationResult is one (action, resource) outcome, in the terms the response
// reports rather than the terms the evaluator answers in.
type iamSimulationResult struct {
	ActionName           string
	ResourceName         string
	Decision             string
	MatchedStatements    []MatchedStatement
	MissingContextValues []string

	// BoundaryChecked records whether a permissions boundary applied at all.
	// PermissionsBoundaryDecisionDetail is emitted only when one did — a `false`
	// there means "the boundary blocked this", so emitting it for an entity with no
	// boundary would report a block that never happened.
	BoundaryChecked bool

	// AllowedByBoundary is the boundary's answer when BoundaryChecked is true.
	AllowedByBoundary bool
}

// simulatePrincipalPolicy implements the SimulatePrincipalPolicy operation.
func (p *IAMPlugin) simulatePrincipalPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	params, errResp := p.parseSimulateRequest(req)
	if errResp != nil {
		return errResp, nil
	}
	if params.PolicySourceArn == "" {
		return iamErrorResponse("InvalidInput", "PolicySourceArn is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:SimulatePrincipalPolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	docs, boundaryDocs, errResp, err := p.simulationPolicySet(goCtx, params)
	if err != nil {
		// The model's PolicyEvaluation 500: "The request failed because a provided
		// policy could not be successfully evaluated." A state read that fails is
		// substrate's fault, not the caller's, so it must not be reported as
		// InvalidInput — and it must not be reported as an *answer*, because an
		// evaluation over a policy set that failed to load would silently be an
		// implicitDeny the caller would read as a real decision.
		return iamErrorResponse("PolicyEvaluation", err.Error(), http.StatusInternalServerError), nil
	}
	if errResp != nil {
		return errResp, nil
	}

	// "If you do not specify a CallerArn, it defaults to the ARN of the user, group,
	// or role that you specify in PolicySourceArn."
	callerArn := params.CallerArn
	if callerArn == "" {
		callerArn = params.PolicySourceArn
	}

	return p.simulateResponse("SimulatePrincipalPolicy", params, docs, boundaryDocs, callerArn)
}

// simulateCustomPolicy implements the SimulateCustomPolicy operation.
//
// It resolves no entity, which is why the model declares NoSuchEntity for
// SimulatePrincipalPolicy and not for this one: there is nothing to fail to find.
func (p *IAMPlugin) simulateCustomPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	params, errResp := p.parseSimulateRequest(req)
	if errResp != nil {
		return errResp, nil
	}
	if len(params.PolicyInputList) == 0 {
		return iamErrorResponse("InvalidInput", "PolicyInputList is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:SimulateCustomPolicy", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	docs, errResp := parseSimulationPolicyInputs(params.PolicyInputList, "PolicyInputList")
	if errResp != nil {
		return errResp, nil
	}
	resourceDocs, errResp := simulationResourcePolicyDocs(params.ResourcePolicy)
	if errResp != nil {
		return errResp, nil
	}
	docs = append(docs, resourceDocs...)

	boundaryDocs, errResp := parseSimulationPolicyInputs(
		params.PermissionsBoundaryPolicyInputList, "PermissionsBoundaryPolicyInputList")
	if errResp != nil {
		return errResp, nil
	}

	return p.simulateResponse("SimulateCustomPolicy", params, docs, boundaryDocs, params.CallerArn)
}

// parseSimulateRequest decodes the members shared by both operations, reading the
// query-protocol lists through iam_query.go's decoders and falling back to a JSON
// body for the tests that drive the plugin directly (#639).
func (p *IAMPlugin) parseSimulateRequest(req *AWSRequest) (*iamSimulateRequest, *AWSResponse) {
	var params iamSimulateRequest
	if err := parseIAMBody(req.Body, &params); err != nil {
		return nil, iamErrorResponse("InvalidInput", err.Error(), http.StatusBadRequest)
	}

	// Every list this operation takes arrives as prefix.member.N over the wire, which
	// is why #639 had to land first: without its decoder ActionNames — a *required*
	// member — was nil on every real request.
	if list := iamMemberList(req.Params, "ActionNames"); list != nil {
		params.ActionNames = list
	}
	if list := iamMemberList(req.Params, "ResourceArns"); list != nil {
		params.ResourceArns = list
	}
	if list := iamMemberList(req.Params, "PolicyInputList"); list != nil {
		params.PolicyInputList = list
	}
	if list := iamMemberList(req.Params, "PermissionsBoundaryPolicyInputList"); list != nil {
		params.PermissionsBoundaryPolicyInputList = list
	}

	if len(params.ActionNames) == 0 {
		return nil, iamErrorResponse("InvalidInput", "ActionNames is required", http.StatusBadRequest)
	}
	for _, action := range params.ActionNames {
		// "Each operation must include the service identifier, such as
		// iam:CreateUser." An action with no service prefix can never match a
		// statement, so accepting it would answer implicitDeny for what is really a
		// malformed request.
		if !strings.Contains(action, ":") {
			return nil, iamErrorResponse("InvalidInput",
				fmt.Sprintf("Invalid Action name: %s; an action must name its service, as in iam:CreateUser", action),
				http.StatusBadRequest)
		}
	}

	// The model's maxItemsType is min 1 / max 1000. Zero means absent, which the
	// reference gives a default for. #579 asserted a 25-action cap; no action-count
	// limit appears anywhere in the reference or the model, so none is modeled —
	// refusing requests AWS accepts would be worse than accepting a large one.
	if params.MaxItems != 0 &&
		(params.MaxItems < 1 || params.MaxItems > iamSimulateMaxMaxItems) {
		return nil, iamErrorResponse("InvalidInput",
			fmt.Sprintf("MaxItems must be between 1 and %d", iamSimulateMaxMaxItems),
			http.StatusBadRequest)
	}

	// "If this parameter is not provided, then the value defaults to * (all
	// resources)."
	if len(params.ResourceArns) == 0 {
		params.ResourceArns = []string{"*"}
	}

	params.context, params.contextMulti = simulationContextEntries(req.Params)
	return &params, nil
}

// simulationContextEntries decodes ContextEntries into the condition-key map the
// evaluator takes.
//
// The wire shape is a list *of structures containing a list*:
//
//	ContextEntries.member.1.ContextKeyName=aws:SourceIp
//	ContextEntries.member.1.ContextKeyValues.member.1=10.0.0.1
//	ContextEntries.member.1.ContextKeyType=ip
//
// which is the case iamMemberStructs keeps its suffixes verbatim for — the member
// map holds "ContextKeyValues.member.1", so iamMemberList applies to it directly.
// There is no JSON-body form of this at all, so unlike every other member it is read
// only from req.Params.
//
// Every value is kept, in two shapes: the multi map holds the caller's whole list, so
// a ForAllValues or ForAnyValue condition is evaluated over exactly what was supplied,
// and the single map holds the first value, which is what an unqualified operator
// compares. Until #690 only the first survived, so a caller simulating a multivalued
// key was answered against a policy that had seen one of its values — an answer that
// could differ from what enforcement would decide, which is the one thing a simulator
// must not do.
//
// ContextKeyType is recorded by AWS but not consulted here — every condition operator
// substrate implements compares strings.
//
// Two entries whose names differ only by case are one key, since that is how the
// evaluator resolves a name (#704) — so the later entry overwrites the earlier, under
// the spelling the earlier used. Letting both survive would leave the answer to
// [condResolveKey]'s sorted tie-break, making a caller who sent AWS:PrincipalArn and
// aws:PrincipalArn simulated against whichever sorts first rather than against what
// they last said. Entries arrive in wire-index order, so "later" is the caller's own
// ordering and not Go's.
func simulationContextEntries(params map[string]string) (single map[string]string, multi map[string][]string) {
	entries := iamMemberStructs(params, "ContextEntries")
	if len(entries) == 0 {
		return nil, nil
	}
	single = make(map[string]string, len(entries))
	multi = make(map[string][]string, len(entries))
	for _, entry := range entries {
		name := entry["ContextKeyName"]
		if name == "" {
			continue
		}
		if existing, ok := condResolveKey(name, single); ok {
			name = existing
		}
		values := iamMemberList(entry, "ContextKeyValues")
		if len(values) == 0 {
			// A key named with no value is still *set* as far as the Null operator is
			// concerned, and it must not be reported as a missing context value: the
			// caller supplied it. It is recorded in both maps for that reason — the
			// multi entry is present and empty, which is what a ForAllValues condition
			// reads as "no values in the request".
			single[name] = ""
			multi[name] = []string{}
			continue
		}
		single[name] = values[0]
		multi[name] = values
	}
	return single, multi
}

// simulationPolicySet assembles the documents SimulatePrincipalPolicy evaluates:
// the entity's own identity policies, its groups' when it is a user, anything
// supplied through PolicyInputList, and the boundary.
//
// The boundary follows the reference: "if a permissions boundary is attached to an
// entity and you pass in a different permissions boundary policy using this
// parameter, then the new permissions boundary policy is used for the simulation."
//
// A returned errResp is an answer for the caller (InvalidInput, NoSuchEntity); a
// returned error is substrate's own failure, which the caller reports as
// PolicyEvaluation 500. The two are kept apart deliberately: a state read that failed
// must not degrade into an evaluation over a partial policy set, because the result
// would be an implicitDeny indistinguishable from a real one.
func (p *IAMPlugin) simulationPolicySet(goCtx context.Context, params *iamSimulateRequest) (
	docs, boundaryDocs []SourcedPolicyDocument, errResp *AWSResponse, err error,
) {
	entityType, entityName := parsePrincipalARN(params.PolicySourceArn)
	// The simulated entity belongs to the account its own ARN names, which is what
	// lets a caller simulate a principal in another account (#737).
	entityAccount := arnAccountID(params.PolicySourceArn)
	switch entityType {
	case "user", "group", "role":
		// "The entity can be an IAM user, group, or role."
	default:
		return nil, nil, iamErrorResponse("InvalidInput",
			fmt.Sprintf("Invalid Entity Arn: %s must be a user, group or role ARN", params.PolicySourceArn),
			http.StatusBadRequest), nil
	}
	if entityName == "" {
		return nil, nil, iamErrorResponse("InvalidInput",
			fmt.Sprintf("Invalid Entity Arn: %s names no entity", params.PolicySourceArn),
			http.StatusBadRequest), nil
	}

	raw, err := p.state.Get(goCtx, iamNamespace, iamEntityKey(entityAccount, entityType, entityName))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load %s %s: %w", entityType, entityName, err)
	}
	if raw == nil {
		return nil, nil, iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("The %s with name %s cannot be found.", entityType, entityName),
			http.StatusNotFound), nil
	}

	docs, err = p.simulationIdentityDocs(goCtx, iamEntity{Account: entityAccount, Kind: entityType, Name: entityName})
	if err != nil {
		return nil, nil, nil, err
	}

	inputDocs, errResp := parseSimulationPolicyInputs(params.PolicyInputList, "PolicyInputList")
	if errResp != nil {
		return nil, nil, errResp, nil
	}
	docs = append(docs, inputDocs...)

	resourceDocs, errResp := simulationResourcePolicyDocs(params.ResourcePolicy)
	if errResp != nil {
		return nil, nil, errResp, nil
	}
	docs = append(docs, resourceDocs...)

	if len(params.PermissionsBoundaryPolicyInputList) > 0 {
		boundaryDocs, errResp = parseSimulationPolicyInputs(
			params.PermissionsBoundaryPolicyInputList, "PermissionsBoundaryPolicyInputList")
		if errResp != nil {
			return nil, nil, errResp, nil
		}
		return docs, boundaryDocs, nil, nil
	}

	// No boundary was supplied, so the entity's own applies. A group holds none —
	// IAM has no PutGroupPermissionsBoundary — so for a group this finds nothing,
	// which is correct rather than a gap.
	var stored struct {
		PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary,omitempty"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, nil, nil, fmt.Errorf("unmarshal %s %s: %w", entityType, entityName, err)
	}
	if stored.PermissionsBoundary != nil {
		for _, doc := range p.loadBoundaryPolicyDocs(goCtx, stored.PermissionsBoundary.PolicyARN) {
			boundaryDocs = append(boundaryDocs, SourcedPolicyDocument{
				Document:   doc,
				SourceID:   stored.PermissionsBoundary.PolicyName,
				SourceType: PolicySourceIAMPolicy,
			})
		}
	}
	return docs, boundaryDocs, nil, nil
}

// simulationResourcePolicyDocs parses the optional ResourcePolicy into a document
// tagged as a resource policy, which is what makes a matched statement report
// "Resource Policy" rather than "IAM Policy".
//
// It is folded into the identity set rather than evaluated separately because
// substrate's evaluator makes one same-account decision: the reference's separate
// EvalDecisionDetails breakdown is a cross-account construct, and it is documented as
// out of scope in this file's header.
func simulationResourcePolicyDocs(resourcePolicy string) ([]SourcedPolicyDocument, *AWSResponse) {
	if resourcePolicy == "" {
		return nil, nil
	}
	docs, errResp := parseSimulationPolicyInputs([]string{resourcePolicy}, "ResourcePolicy")
	if errResp != nil {
		return nil, errResp
	}
	docs[0].SourceID = "ResourcePolicy"
	docs[0].SourceType = PolicySourceResourcePolicy
	return docs, nil
}

// simulationIdentityDocs loads an entity's managed and inline policy documents,
// including those of every group a user belongs to.
//
// AWS: "If you specify a user, then the simulation also includes all of the
// policies that are attached to groups that the user belongs to." The group arm is
// the reason Lane B had to land first — there was no group state to read.
//
// Unlike loadPoliciesForPrincipal, AdministratorAccess is not short-circuited into
// a synthetic allow-all: the simulation must report *which* policy decided, and a
// synthesized document has no name to report.
func (p *IAMPlugin) simulationIdentityDocs(goCtx context.Context, entity iamEntity) ([]SourcedPolicyDocument, error) {
	sources := []iamEntity{entity}
	if entity.Kind == "user" {
		groups, err := p.loadStringList(goCtx, iamUserGroupsKey(entity.Account, entity.Name))
		if err != nil {
			return nil, fmt.Errorf("load group memberships for simulation: %w", err)
		}
		for _, name := range groups {
			sources = append(sources, iamEntity{Account: entity.Account, Kind: "group", Name: name})
		}
	}

	var docs []SourcedPolicyDocument
	for _, source := range sources {
		arns, err := p.loadPolicyList(goCtx, iamAttachedPoliciesKey(source.Account, source.Kind, source.Name))
		if err != nil {
			return nil, fmt.Errorf("load attached policies for simulation: %w", err)
		}
		for _, arn := range arns {
			doc, ok := p.resolveSimulationManagedDoc(goCtx, arn)
			if !ok {
				// One of the ~1,150 AWS managed policies substrate does not bundle. Its
				// statements are unknown, so it contributes nothing rather than a guess.
				continue
			}
			docs = append(docs, SourcedPolicyDocument{
				Document: doc,
				// The sample response reports a managed policy by name
				// ("AmazonS3ReadOnlyAccess"), not by ARN.
				SourceID:   arnPolicyName(arn),
				SourceType: PolicySourceIAMPolicy,
			})
		}

		names, err := p.loadInlinePolicyNames(goCtx, source.Account, source.Kind, source.Name)
		if err != nil {
			return nil, fmt.Errorf("load inline policy names for simulation: %w", err)
		}
		for _, name := range names {
			doc, err := p.loadInlinePolicyDoc(goCtx, source.Account, source.Kind, source.Name, name)
			if err != nil {
				return nil, fmt.Errorf("load inline policy for simulation: %w", err)
			}
			if doc == nil {
				continue
			}
			docs = append(docs, SourcedPolicyDocument{
				Document:   *doc,
				SourceID:   name,
				SourceType: PolicySourceIAMPolicy,
			})
		}
	}
	return docs, nil
}

// resolveSimulationManagedDoc resolves a managed policy ARN to its document,
// catalog before state — the same order every other IAM caller uses, so a bundled
// and a customer-managed policy behave alike.
func (p *IAMPlugin) resolveSimulationManagedDoc(goCtx context.Context, arn string) (PolicyDocument, bool) {
	if mp, ok := GetManagedPolicy(arn); ok {
		return mp.Document, true
	}
	raw, err := p.state.Get(goCtx, iamNamespace, iamPolicyKey(arn))
	if err != nil || raw == nil {
		return PolicyDocument{}, false
	}
	var pol IAMPolicy
	if err := json.Unmarshal(raw, &pol); err != nil {
		return PolicyDocument{}, false
	}
	return pol.Document, true
}

// parseSimulationPolicyInputs parses each policy string into a document, naming it
// "<prefix>.N" as the sample response does for PolicyInputList.
//
// A document that will not parse is InvalidInput 400 rather than PolicyEvaluation
// 500: the input is malformed, which is the caller's error, and the reference
// reserves PolicyEvaluation for a policy that "could not be successfully
// evaluated".
func parseSimulationPolicyInputs(inputs []string, prefix string) ([]SourcedPolicyDocument, *AWSResponse) {
	if len(inputs) == 0 {
		return nil, nil
	}
	docs := make([]SourcedPolicyDocument, 0, len(inputs))
	for i, input := range inputs {
		var doc PolicyDocument
		if err := json.Unmarshal([]byte(input), &doc); err != nil {
			return nil, iamErrorResponse("InvalidInput",
				fmt.Sprintf("Syntax errors in policy: %s", err.Error()),
				http.StatusBadRequest)
		}
		docs = append(docs, SourcedPolicyDocument{
			Document:   doc,
			SourceID:   prefix + "." + strconv.Itoa(i+1),
			SourceType: PolicySourceIAMPolicy,
		})
	}
	return docs, nil
}

// simulateResponse evaluates every (action, resource) pair and encodes the page.
//
// Every action is evaluated against every resource, per the reference: "Each API in
// the ActionNames parameter is evaluated for each resource in this list".
func (p *IAMPlugin) simulateResponse(op string, params *iamSimulateRequest,
	docs, boundaryDocs []SourcedPolicyDocument, callerArn string,
) (*AWSResponse, error) {
	condCtx := simulationConditionContext(params, callerArn, p.now())

	results := make([]iamSimulationResult, 0, len(params.ActionNames)*len(params.ResourceArns))
	for _, action := range params.ActionNames {
		for _, resource := range params.ResourceArns {
			results = append(results, simulateOne(docs, boundaryDocs, EvaluationRequest{
				Principal:    callerArn,
				Action:       action,
				Resource:     resource,
				Context:      condCtx,
				MultiContext: params.contextMulti,
			}))
		}
	}

	page, nextMarker, isTruncated := paginateSimulationResults(results, params.Marker, params.MaxItems.Int())

	xmlStr := iamEvaluationResultsXML(page) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		xmlStr += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, op, xmlStr)
}

// simulateOne evaluates one (action, resource) pair, applying the boundary.
//
// The boundary arm mirrors CheckAccess's second Evaluate call rather than
// reimplementing the rule: a boundary that does not allow turns an allow into an
// implicit deny. The reference states the same in reverse — AllowedByPermissionsBoundary
// false "means that either the requested action is not allowed (implicitly denied)
// or that the action is explicitly denied by the permissions boundary. In both of
// these cases, the action is not allowed, regardless of the identity-based policy".
func simulateOne(docs, boundaryDocs []SourcedPolicyDocument, req EvaluationRequest) iamSimulationResult {
	result := EvaluateSourced(docs, req)
	out := iamSimulationResult{
		ActionName:           req.Action,
		ResourceName:         req.Resource,
		Decision:             simulationDecision(result.Decision),
		MatchedStatements:    result.MatchedStatements,
		MissingContextValues: result.MissingContextValues,
	}

	if len(boundaryDocs) == 0 {
		return out
	}
	boundaryResult := EvaluateSourced(boundaryDocs, req)
	out.BoundaryChecked = true
	out.AllowedByBoundary = boundaryResult.Decision == DecisionAllow
	if !out.AllowedByBoundary && out.Decision == iamEvalDecisionAllowed {
		// An identity allow the boundary does not permit is an implicit deny, not an
		// explicit one: the boundary caps what is reachable rather than denying the
		// action by name.
		out.Decision = iamEvalDecisionImplicitDeny
		out.MatchedStatements = nil
	}
	return out
}

// EvalDecision values, per the model's PolicyEvaluationDecisionType enum.
const (
	iamEvalDecisionAllowed      = "allowed"
	iamEvalDecisionExplicitDeny = "explicitDeny"
	iamEvalDecisionImplicitDeny = "implicitDeny"
)

// simulationDecision maps substrate's decision onto the API's enum.
//
// The three-way distinction is the feature. An emulator that answered a bare
// allow/deny would let a consumer's "the policy explicitly forbids this" assertion
// pass on what is really a missing grant, and #579 is explicit that such coverage
// "would be worse than none, because it would look like coverage".
func simulationDecision(decision string) string {
	switch decision {
	case DecisionAllow:
		return iamEvalDecisionAllowed
	case DecisionDeny:
		return iamEvalDecisionExplicitDeny
	default:
		return iamEvalDecisionImplicitDeny
	}
}

// simulationConditionContext builds the condition-key context for the evaluation:
// the keys the caller supplied through ContextEntries, plus the two the simulation
// itself determines.
//
// aws:PrincipalArn and aws:ResourceAccount are filled in from CallerArn and
// ResourceOwner because the simulation *knows* them — a policy conditioned on either
// would otherwise report the key as a missing context value when the caller had in
// fact told substrate what it is, through a differently-named parameter.
//
// aws:CurrentTime and aws:EpochTime come from the plugin's clock, for the same reason
// and with the same rendering the enforcement path uses ([AuthController.authzTimeContext]):
// a simulation that reported a date condition as a missing context value while the
// request gate evaluated it would contradict the enforcement it exists to predict.
//
// An explicit ContextEntry wins over both, whatever case it is spelled in. A caller
// who names a key is stating what to simulate with, and overriding that with a derived
// value would make the parameter unusable — which is what a plain assignment would do
// for AWS:PrincipalArn before #704, since it left the derived aws:PrincipalArn in the
// map beside it and the evaluator would then resolve to whichever sorted first.
// Deleting the folded-equal name is what makes the caller's entry win.
func simulationConditionContext(params *iamSimulateRequest, callerArn string, now time.Time) map[string]string {
	ctx := make(map[string]string, len(params.context)+4)
	if callerArn != "" {
		ctx["aws:PrincipalArn"] = callerArn
	}
	if params.ResourceOwner != "" {
		ctx["aws:ResourceAccount"] = params.ResourceOwner
	}
	ctx["aws:CurrentTime"] = now.UTC().Format(time.RFC3339)
	ctx["aws:EpochTime"] = strconv.FormatInt(now.Unix(), 10)
	for key, value := range params.context {
		if derived, ok := condResolveKey(key, ctx); ok && derived != key {
			delete(ctx, derived)
		}
		ctx[key] = value
	}
	return ctx
}

// paginateSimulationResults applies the Marker cursor and MaxItems to the flattened
// result list.
//
// The cursor is the index of the last item returned rather than a key, because two
// results can name the same (action, resource) pair only if the caller listed a
// duplicate — and a key-based cursor would then skip or repeat. It follows
// paginateIAMKeys' base64 convention so a caller cannot tell the two apart.
func paginateSimulationResults(results []iamSimulationResult, marker string, maxItems int) (
	page []iamSimulationResult, nextMarker string, isTruncated bool,
) {
	start := decodeSimulationMarker(marker)
	if start > len(results) {
		start = len(results)
	}
	if maxItems <= 0 || maxItems > iamSimulateMaxMaxItems {
		maxItems = iamSimulateDefaultMaxItems
	}
	end := start + maxItems
	if end >= len(results) {
		return results[start:], "", false
	}
	return results[start:end], encodeSimulationMarker(end), true
}

// encodeSimulationMarker encodes a result index as an opaque page cursor, base64 as
// paginateIAMKeys does so a caller cannot tell substrate's two cursor kinds apart.
func encodeSimulationMarker(index int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(index)))
}

// decodeSimulationMarker returns the index a cursor names, or 0 for anything it
// cannot read.
//
// A marker substrate did not mint is treated as the start of the list rather than
// as an error: paginateIAMKeys does the same with an undecodable marker, and the
// alternative — InvalidInput on a cursor a caller round-tripped — would be worse
// than restarting a page.
func decodeSimulationMarker(marker string) int {
	if marker == "" {
		return 0
	}
	raw, err := base64.StdEncoding.DecodeString(marker)
	if err != nil {
		return 0
	}
	index, err := strconv.Atoi(string(raw))
	if err != nil || index < 0 {
		return 0
	}
	return index
}

// iamEvaluationResultsXML encodes <EvaluationResults> for a page.
//
// MatchedStatements and MissingContextValues are always emitted, empty when there
// is nothing to report — the reference's sample response shows
// `<MatchedStatements/>` and `<MissingContextValues/>` on an implicitDeny result,
// so a caller reading them unconditionally is following the documented shape.
func iamEvaluationResultsXML(results []iamSimulationResult) string {
	var b strings.Builder
	b.WriteString("<EvaluationResults>")
	for _, r := range results {
		b.WriteString("<member><EvalActionName>")
		b.WriteString(xmlEsc(r.ActionName))
		b.WriteString("</EvalActionName><EvalResourceName>")
		b.WriteString(xmlEsc(r.ResourceName))
		b.WriteString("</EvalResourceName><EvalDecision>")
		b.WriteString(xmlEsc(r.Decision))
		b.WriteString("</EvalDecision>")
		b.WriteString(iamMatchedStatementsXML(r.MatchedStatements))
		b.WriteString(iamStringListXML("MissingContextValues", r.MissingContextValues))
		if r.BoundaryChecked {
			b.WriteString("<PermissionsBoundaryDecisionDetail><AllowedByPermissionsBoundary>")
			b.WriteString(iamBoolXML(r.AllowedByBoundary))
			b.WriteString("</AllowedByPermissionsBoundary></PermissionsBoundaryDecisionDetail>")
		}
		b.WriteString("</member>")
	}
	b.WriteString("</EvaluationResults>")
	return b.String()
}

// iamMatchedStatementsXML encodes <MatchedStatements>.
//
// StartPosition/EndPosition are omitted: they are offsets into the submitted
// document text, which substrate does not keep (see this file's header).
func iamMatchedStatementsXML(statements []MatchedStatement) string {
	var b strings.Builder
	b.WriteString("<MatchedStatements>")
	for _, s := range statements {
		b.WriteString("<member><SourcePolicyId>")
		b.WriteString(xmlEsc(s.SourcePolicyID))
		b.WriteString("</SourcePolicyId><SourcePolicyType>")
		b.WriteString(xmlEsc(s.SourcePolicyType))
		b.WriteString("</SourcePolicyType></member>")
	}
	b.WriteString("</MatchedStatements>")
	return b.String()
}
