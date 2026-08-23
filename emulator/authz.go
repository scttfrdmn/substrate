package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AuthController enforces IAM policy decisions for all AWS service requests.
// It is wired into the server pipeline as ServerOptions.Auth and called before
// quota and consistency checks. A nil Principal (bootstrap / test mode) always
// passes.
type AuthController struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// AuthControllerOption configures an [AuthController].
type AuthControllerOption func(*AuthController)

// WithAuthTimeController gives the controller the simulated clock, which is what makes
// aws:CurrentTime and aws:EpochTime available to a policy condition.
//
// Without it those two keys are absent from every request context, so a condition
// naming either is a false deny rather than a wall-clock answer. That is deliberate:
// substrate's guarantee is that a recorded run replays identically, and a date
// condition evaluated against time.Now() would decide differently on replay. A caller
// wiring authorization into a server has a TimeController already — the server reads
// it one line above the authorization call — so the honest default is to say the keys
// are unavailable rather than to reach for the wall clock (#714).
func WithAuthTimeController(tc *TimeController) AuthControllerOption {
	return func(a *AuthController) {
		a.tc = tc
	}
}

// NewAuthController creates an AuthController backed by state and logger.
//
// The options are variadic so that adding the clock did not change the signature
// every caller and every test already uses.
func NewAuthController(state StateManager, logger Logger, opts ...AuthControllerOption) *AuthController {
	a := &AuthController{state: state, logger: logger}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// authzTimeContext writes the two clock-derived condition keys into ctx, if the
// controller was given a clock.
//
// AWS documents the pair as the keys its date operators are used with:
// aws:CurrentTime is "the date and time of the request" in ISO 8601 / RFC 3339 form,
// aws:EpochTime the same instant "in Unix time" — so they are one reading of one clock
// rendered two ways, and a policy testing them against each other cannot disagree.
//
// The instant is read once per request rather than once per resource, so a request
// naming several resources is evaluated at one time. reqCtx.Timestamp is deliberately
// not the source: it is time.Now() on the HTTP path, so a condition sourced from it
// would be unreplayable for every real SDK request.
func (a *AuthController) authzTimeContext(ctx map[string]string, now time.Time, ok bool) {
	if !ok {
		return
	}
	ctx["aws:CurrentTime"] = now.UTC().Format(time.RFC3339)
	ctx["aws:EpochTime"] = strconv.FormatInt(now.Unix(), 10)
}

// authzPrincipalContext writes the condition keys that name the caller.
//
// AWS documents aws:PrincipalArn as "the ARN of the principal that made the request",
// present in the context of every request a principal signs. Substrate knows it with
// certainty — it is the ARN the evaluation below is run for, and the one the denial
// message names — so leaving it out reported as missing a key the request itself carries.
// The simulator has always derived it from CallerArn ([simulationConditionContext]), so
// the two paths disagreed about the one key a simulation is most often written about.
//
// Omitting it stopped being merely a false deny once the negated operators arrived
// (#714). AWS's rule is that a negated operator over an absent key is *true*, so with the
// key missing, `"ArnNotEquals": {"aws:PrincipalArn": "arn:aws:iam::*:user/carol"}` — the
// shape of an exemption — was satisfied by every caller: as a Deny it fired against carol
// herself, and as an Allow it granted everyone. The absence turned an exemption into its
// opposite, which is why the producer lands with the operators rather than after them.
//
// aws:username is written only when the caller *has* a user name, which is the second
// half of #745: a policy naming ${aws:username} — the shape of AWS's own
// IAMUserChangePassword, and of every "let a user manage their own things" statement —
// resolved to nothing, so the statement matched no resource and every such grant was a
// false deny. The name comes from [Principal.UserName], recorded when the credential was
// resolved, and never from the ARN: [Server.resolveCallerPrincipal] synthesizes
// `…:user/<access-key-id>` for a registry hit with no IAM entity behind it, so parsing
// this ARN would publish a credential ID as a user name.
//
// It is absent, not empty, for every principal for which AWS's own table reads
// "(not present)" — the account root, an assumed role, an EC2 instance role, and a
// federated or SAML/OIDC caller. Absent is what makes the negated forms answer as AWS
// specifies, since [condKeyPresent] reads an empty value as absent anyway; writing "" and
// omitting the key are the same observation, and omitting it is the honest one.
//
// aws:PrincipalAccount and aws:userid are deliberately still absent: each needs a
// derivation this function cannot do honestly for every principal kind — a unique ID
// substrate does not mint, an account that a cross-account credential makes ambiguous —
// and a key guessed wrong is worse than one a policy can test for with Null. Tracked as
// its own issue rather than left as an unstated gap.
func authzPrincipalContext(ctx map[string]string, principal *Principal) {
	if principal == nil || principal.ARN == "" {
		return
	}
	ctx["aws:PrincipalArn"] = principal.ARN
	if principal.UserName != "" {
		ctx["aws:username"] = principal.UserName
	}
}

// now reports the controlled time, and whether there is a clock to read it from.
func (a *AuthController) now() (time.Time, bool) {
	if a.tc == nil {
		return time.Time{}, false
	}
	return a.tc.Now(), true
}

// CheckAccess returns nil when the caller is allowed or when reqCtx.Principal
// is nil. Returns an *AWSError with HTTP 403 and the denial code the requested
// service's wire protocol uses — "AccessDenied" for the XML protocols,
// "AccessDeniedException" for the JSON ones; see [accessDeniedCodeFor] (#595).
func (a *AuthController) CheckAccess(reqCtx *RequestContext, req *AWSRequest) error {
	if reqCtx.Principal == nil {
		return nil
	}

	action := serviceToAction(req, req.Service, req.Operation)
	if authzNoPermissionsRequired[action] {
		return nil
	}
	resources := a.buildResourceARNs(reqCtx, req)
	now, haveClock := a.now()

	goCtx := context.Background()
	entity, exists, err := resolveIAMEntity(goCtx, a.state, reqCtx.Principal.ARN)
	if err != nil {
		// A state read that actually failed — a broken backend, not an unknown
		// caller. Fail open and say so loudly: substrate cannot decide, and denying
		// on a storage error would turn an infrastructure fault into a
		// misleading AccessDeniedException.
		a.logger.Warn("authz: failed to resolve principal", "principal", reqCtx.Principal.ARN, "err", err)
		return nil
	}
	if !exists {
		// Enforcement is opt-in by creating the principal. A credential that
		// resolves to no IAM entity is not enforced — Debug rather than Warn,
		// because this is the normal case for every caller that never touched IAM
		// and warning on each would bury the signal it is meant to carry.
		a.logger.Debug("authz: principal has no IAM entity, not enforced",
			"principal", reqCtx.Principal.ARN, "action", action)
		return nil
	}

	docs, err := a.loadPoliciesForPrincipal(entity)
	if err != nil {
		// Fail open: if we cannot load policies we cannot block the caller.
		a.logger.Warn("authz: failed to load policies", "principal", reqCtx.Principal.ARN, "err", err)
		return nil
	}

	// Both denial arms take their code from the same helper, so an identity-policy
	// refusal and a boundary refusal cannot drift apart — and neither can drift from
	// the trust-policy gate #593 added, which resolves the same way for STS.
	deniedCode := accessDeniedCodeFor(req.Service, "")

	// If a permission boundary is set it must also allow the action, against every
	// resource the request names — loaded once, outside the loop, because it is a
	// property of the principal rather than of a resource.
	boundary, err := a.loadPermissionBoundary(entity)
	if err != nil {
		a.logger.Warn("authz: failed to load permission boundary", "principal", reqCtx.Principal.ARN, "err", err)
	}

	// The request tags describe the request, not any one resource, so they are read
	// once and merged under each resource's own tags below.
	requestTags := make(map[string]string)
	addRequestTags(requestTags, req)

	// aws:TagKeys is the same reading of the same request, so it is derived from the
	// map above rather than gathered a second time per service. That is what keeps the
	// two in step: every arm of addRequestTags records one aws:RequestTag/<key> entry
	// per tag key in the request, so the keys *are* the request's tag keys, and a
	// service arm added later populates both without knowing this key exists.
	//
	// It is request-level, like the tags it comes from, so one map serves every
	// resource in the loop below — there is nothing per-resource in it to keep apart,
	// which is the concern the per-resource condCtx exists for.
	requestMulti := make(map[string][]string, 1)
	if keys := requestTagKeys(requestTags); len(keys) > 0 {
		requestMulti["aws:TagKeys"] = keys
	}

	// Service-specific keys that describe the request rather than any resource it names
	// — currently iam:AWSServiceName (#747). Request-level for the same reason
	// requestTags is: the value is one reading of one request, so it is read once and
	// merged under every resource below.
	requestKeys := make(map[string]string, 1)
	iamAuthzRequestContext(requestKeys, req)

	// The prefixes this service reports a resource's tags under, resolved once: it is a
	// property of the service, not of any one resource.
	tagPrefixes := authzResourceTagPrefixes(req.Service)

	// Every resource the request names must be allowed. A request naming several —
	// organizations:MoveAccount and ec2:RunInstances — is denied unless the caller's
	// policies admit all of them, which is how AWS evaluates a statement against
	// "every resource that is required" for the action (#660, #662).
	for _, res := range resources {
		condCtx := make(map[string]string,
			len(requestTags)+len(requestKeys)+len(res.Tags)*len(tagPrefixes)+len(res.Context))
		for k, v := range requestTags {
			condCtx[k] = v
		}
		for k, v := range requestKeys {
			condCtx[k] = v
		}
		// The tags travel with the ARN they belong to: one merged map across several
		// resources would let a tag on one satisfy a condition written about another,
		// which is the false allow orgAuthzResourceID's comment exists to prevent.
		//
		// One tag is reported under every prefix the service publishes, because a policy
		// may write the condition either way about the same tag and AWS answers both from
		// the resource's single tag set.
		for k, v := range res.Tags {
			for _, prefix := range tagPrefixes {
				condCtx[prefix+k] = v
			}
		}
		// Keys that describe this resource, on the same reasoning: they are scoped to
		// the resource the reference lists them on, so a condition written about a
		// launch's network interface cannot be satisfied by its AMI or its instance.
		for k, v := range res.Context {
			condCtx[k] = v
		}
		a.authzTimeContext(condCtx, now, haveClock)
		authzPrincipalContext(condCtx, reqCtx.Principal)

		result := Evaluate(docs, EvaluationRequest{
			Principal:    reqCtx.Principal.ARN,
			Action:       action,
			Resource:     res.ARN,
			Context:      condCtx,
			MultiContext: requestMulti,
		})
		if result.Decision != DecisionAllow {
			return &AWSError{
				Code: deniedCode,
				Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
					reqCtx.Principal.ARN, action, res.ARN),
				HTTPStatus: http.StatusForbidden,
			}
		}

		if boundary != nil {
			boundaryResult := Evaluate([]PolicyDocument{*boundary}, EvaluationRequest{
				Principal:    reqCtx.Principal.ARN,
				Action:       action,
				Resource:     res.ARN,
				Context:      condCtx,
				MultiContext: requestMulti,
			})
			if boundaryResult.Decision != DecisionAllow {
				return &AWSError{
					Code: deniedCode,
					Message: fmt.Sprintf("User: %s is not authorized to perform: %s (blocked by permission boundary)",
						reqCtx.Principal.ARN, action),
					HTTPStatus: http.StatusForbidden,
				}
			}
		}
	}

	// The create is allowed. If it also applies tags, AWS authorizes those separately
	// against the service's tagging action, and the caller needs that permission too —
	// ec2:CreateTags (#691) and elasticloadbalancing:AddTags (#748). Each pass answers nil
	// for a request of the other's service, so the two are chained rather than switched on
	// here, and a third service arriving adds a line rather than a branch.
	if err := a.checkEC2TagOnCreate(reqCtx, req, docs, boundary, requestTags, deniedCode, now, haveClock); err != nil {
		return err
	}
	return a.checkELBTagOnCreate(reqCtx, req, docs, boundary, requestTags, deniedCode, now, haveClock)
}

// checkEC2TagOnCreate runs the second authorization pass AWS performs on
// ec2:CreateTags when an EC2 create applies tags, returning nil when the request
// applies none or when the caller is permitted to apply them.
//
// AWS: "If tags are specified in the resource-creating action, Amazon performs
// additional authorization on the ec2:CreateTags action to verify if users have
// permissions to create tags. Therefore, users must also have explicit permissions to
// use the ec2:CreateTags action." Substrate authorized a tagged create as the creating
// action alone, so every policy AWS documents for tag-on-create — each of which
// carries a separate ec2:CreateTags statement — permitted more than it says.
//
// It runs after the primary decision rather than beside it because it is a second
// decision, not a second resource: the action is different, the resources are the ones
// the create is about to *make* rather than the ones it reads, and the context carries
// a key the create's own decision must not see. A caller who cannot perform the create
// never reaches it, which is also the order AWS describes ("they must have permissions
// to use the action that creates the resource … If tags are specified …").
//
// requestTags is the aws:RequestTag/* map the primary decision used, so the tags this
// pass evaluates are the same reading of the same request, plus whatever a launch
// template supplies.
func (a *AuthController) checkEC2TagOnCreate(reqCtx *RequestContext, req *AWSRequest,
	docs []PolicyDocument, boundary *PolicyDocument, requestTags map[string]string, deniedCode string,
	now time.Time, haveClock bool) error {
	if req.Service != "ec2" {
		return nil
	}
	pass := ec2AuthzCreateTagsPass(a.state, reqCtx, req)
	if pass == nil {
		return nil
	}

	condCtx := make(map[string]string, len(requestTags)+len(pass.templateTags)+1)
	for k, v := range requestTags {
		condCtx[k] = v
	}
	// A template's tags are new information: they never appear in the request's params,
	// so addRequestTags could not have read them, and AWS evaluates them all the same —
	// "The ec2:CreateTags action is also evaluated if tags are provided in a launch
	// template."
	for k, v := range pass.templateTags {
		condCtx["aws:RequestTag/"+k] = v
	}
	// The key that separates tagging during a create from standalone tagging. It is set
	// only here, which is why a direct CreateTags cannot satisfy a statement conditioned
	// on it — the behavior AWS's examples call "users cannot tag existing resources".
	condCtx[ec2CreateActionCondKey] = req.Operation
	a.authzTimeContext(condCtx, now, haveClock)
	// The same caller signed the same request, so this gate must name the same principal
	// as the primary decision — a request authorized twice against two different answers
	// for aws:PrincipalArn would deny on one door and allow on the other (#714).
	authzPrincipalContext(condCtx, reqCtx.Principal)

	// Derived from the merged map above rather than reused from the primary decision, so
	// a template-supplied key is in aws:TagKeys too — AWS's combined example conditions
	// the tagging grant on both keys at once.
	tagKeysMulti := make(map[string][]string, 1)
	if keys := requestTagKeys(condCtx); len(keys) > 0 {
		tagKeysMulti["aws:TagKeys"] = keys
	}

	// No aws:ResourceTag/*: these resources do not exist yet, so a condition about
	// tags already on them is unsatisfiable, and fabricating one would let the tags
	// being applied stand in for tags already present.
	for _, arn := range pass.resourceARNs(reqCtx) {
		evalReq := EvaluationRequest{
			Principal:    reqCtx.Principal.ARN,
			Action:       ec2CreateTagsAction,
			Resource:     arn,
			Context:      condCtx,
			MultiContext: tagKeysMulti,
		}
		if Evaluate(docs, evalReq).Decision != DecisionAllow {
			return &AWSError{
				Code: deniedCode,
				Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
					reqCtx.Principal.ARN, ec2CreateTagsAction, arn),
				HTTPStatus: http.StatusForbidden,
			}
		}
		if boundary == nil {
			continue
		}
		if Evaluate([]PolicyDocument{*boundary}, evalReq).Decision != DecisionAllow {
			return &AWSError{
				Code: deniedCode,
				Message: fmt.Sprintf("User: %s is not authorized to perform: %s (blocked by permission boundary)",
					reqCtx.Principal.ARN, ec2CreateTagsAction),
				HTTPStatus: http.StatusForbidden,
			}
		}
	}
	return nil
}

// checkELBTagOnCreate runs the second authorization pass AWS performs on
// elasticloadbalancing:AddTags when an ELBv2 create applies tags, returning nil when the
// request applies none or when the caller is permitted to apply them.
//
// AWS: "If tags are specified in the resource-creating action, additional authorization is
// required on the elasticloadbalancing:AddTags action to verify if users have permissions
// to apply tags to the resources being created." Substrate authorized a tagged ELB create
// as the creating action alone, so the bundled ELBTaggingPolicy — whose whole content is a
// separate AddTags statement conditioned on elasticloadbalancing:CreateAction — permitted
// more than it says.
//
// It mirrors [AuthController.checkEC2TagOnCreate] deliberately, because AWS documents the
// two mechanisms in the same terms, but it is a copy rather than a shared helper: the
// action, the condition key, the request member the tags arrive under and the resource ARN
// are all service-specific, and folding them into one function would take four parameters
// to express two behaviors. The two differences that are not cosmetic are that an ELB
// create makes one resource rather than a list, and that ELB has no launch-template
// equivalent — every tag it evaluates is in the request, so requestTags is the whole set.
func (a *AuthController) checkELBTagOnCreate(reqCtx *RequestContext, req *AWSRequest,
	docs []PolicyDocument, boundary *PolicyDocument, requestTags map[string]string, deniedCode string,
	now time.Time, haveClock bool) error {
	if req.Service != elbServiceName {
		return nil
	}
	arn := elbAuthzCreateTagsPass(reqCtx, req)
	if arn == "" {
		return nil
	}

	condCtx := make(map[string]string, len(requestTags)+1)
	for k, v := range requestTags {
		condCtx[k] = v
	}
	// The key that separates tagging during a create from standalone tagging, set only
	// here — which is what makes the bundled policy's StringEquals mean what it says, and
	// what keeps a direct AddTags from satisfying it.
	condCtx[elbCreateActionCondKey] = req.Operation
	a.authzTimeContext(condCtx, now, haveClock)
	// Same request, same caller, so this door must name the same principal as the primary
	// decision; see checkEC2TagOnCreate (#714).
	authzPrincipalContext(condCtx, reqCtx.Principal)

	tagKeysMulti := make(map[string][]string, 1)
	if keys := requestTagKeys(condCtx); len(keys) > 0 {
		tagKeysMulti["aws:TagKeys"] = keys
	}

	// No aws:ResourceTag/* or elasticloadbalancing:ResourceTag/*: the resource does not
	// exist yet, so a condition about tags already on it is unsatisfiable, and fabricating
	// one would let the tags being applied stand in for tags already present.
	evalReq := EvaluationRequest{
		Principal:    reqCtx.Principal.ARN,
		Action:       elbAddTagsAction,
		Resource:     arn,
		Context:      condCtx,
		MultiContext: tagKeysMulti,
	}
	if Evaluate(docs, evalReq).Decision != DecisionAllow {
		return &AWSError{
			Code: deniedCode,
			Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
				reqCtx.Principal.ARN, elbAddTagsAction, arn),
			HTTPStatus: http.StatusForbidden,
		}
	}
	if boundary != nil && Evaluate([]PolicyDocument{*boundary}, evalReq).Decision != DecisionAllow {
		return &AWSError{
			Code: deniedCode,
			Message: fmt.Sprintf("User: %s is not authorized to perform: %s (blocked by permission boundary)",
				reqCtx.Principal.ARN, elbAddTagsAction),
			HTTPStatus: http.StatusForbidden,
		}
	}
	return nil
}

// iamEntity is the IAM entity a principal ARN names, in the terms the policy
// keys are written under.
//
// Kind is "user" or "role" — never "assumed-role", because a session holds no
// policies of its own: IAM resolves an assumed-role's permissions against the
// role it assumed, so a session's Kind is "role" and its Name is the role's.
type iamEntity struct {
	// Account is the account the entity belongs to, taken from the principal ARN.
	//
	// It is part of the identity of the entity, not context about the request: two
	// accounts may both hold a role called "deploy", and every key below is scoped by
	// this (#737). Taking it from the ARN rather than from the request is what makes a
	// cross-account principal resolve against its own account's policies.
	Account string

	// Kind is the entity type, matching the prefix IAM stores policies under.
	Kind string

	// Name is the entity name, which is the policy-key suffix.
	Name string
}

// resolveIAMEntity maps a principal ARN onto the IAM entity whose policies apply,
// reporting separately whether that entity exists in state.
//
// Splitting "which entity" from "does it exist" is the point. Before #411 both
// loaders folded them into one error and then disagreed about what to do with it:
// [AuthController.loadPoliciesForPrincipal] returned "unsupported entity type"
// for anything outside user/role and [AuthController.CheckAccess] failed *open*
// on that error, so an arn:aws:sts::…:assumed-role/worker/sess1 — the exact shape
// STS mints — was allowed with no policies at all, while IAMPlugin.authorize
// *denied* it. One ARN, two loaders, two opposite answers.
//
// Existence is what makes enforcement opt-in. An entity present in state is
// evaluated, and having no policies is an implicit deny, which is real IAM
// behavior. An ARN that names nothing in state is not enforced at all, so the
// thousands of calls made with a credential that never touched IAM are
// unaffected. A read that genuinely *fails* is neither: see the error return.
func resolveIAMEntity(ctx context.Context, state StateManager, principalARN string) (entity iamEntity, exists bool, err error) {
	entityType, entityName := parsePrincipalARN(principalARN)
	if entityName == "" {
		return iamEntity{}, false, nil
	}
	// The account is in the ARN and was previously discarded, which is what made one
	// account's policies apply to another account's principal of the same name (#737).
	account := arnAccountID(principalARN)

	switch entityType {
	case "user", "role":
		entity = iamEntity{Account: account, Kind: entityType, Name: entityName}
	case "assumed-role":
		// arn:aws:sts::<acct>:assumed-role/<RoleName>/<SessionName>. Only the role
		// carries policies, and parsePrincipalARN splits on the first slash — so
		// without dropping the session the lookup would be
		// role_policies:worker/sess1, a key nothing is ever stored under. Every
		// session of the same role would then evaluate as a distinct nameless
		// entity.
		roleName := entityName
		if slash := strings.IndexByte(roleName, '/'); slash >= 0 {
			roleName = roleName[:slash]
		}
		if roleName == "" {
			return iamEntity{}, false, nil
		}
		entity = iamEntity{Account: account, Kind: "role", Name: roleName}
	default:
		// A principal substrate does not model as a policy-holding entity — a
		// service principal, a federated user, the account root. Not enforced,
		// rather than denied: refusing a caller whose policies cannot be looked up
		// would deny requests that pass today for a reason no test could fix.
		return iamEntity{}, false, nil
	}

	raw, err := state.Get(ctx, iamNamespace, iamEntityKey(entity.Account, entity.Kind, entity.Name))
	if err != nil {
		return entity, false, fmt.Errorf("load %s %q: %w", entity.Kind, entity.Name, err)
	}
	return entity, raw != nil, nil
}

// authzAdministratorAccessARN is the managed policy whose presence short-circuits
// evaluation, since it allows every action on every resource.
const authzAdministratorAccessARN = "arn:aws:iam::aws:policy/AdministratorAccess"

// loadPoliciesForPrincipal loads attached managed + inline policies for the
// entity resolved from reqCtx.Principal.ARN.
//
// For a user this includes the policies of every group the user belongs to, which
// is how IAM works — "a group's permissions apply to all of its users" — and what
// makes group membership have any effect at all. Before this, authz.go read only
// the entity's own "<kind>_policies:" and "<kind>_inline*:" keys, so a user whose
// sole grant came from a group was implicitly denied.
//
// A group is never itself a principal: nothing can call AWS *as* a group, so the
// group arm is reached only through a user.
func (a *AuthController) loadPoliciesForPrincipal(entity iamEntity) ([]PolicyDocument, error) {
	goCtx := context.Background()

	// The entity's own policies come first, then each group's, so the order a
	// document appears in is stable — Evaluate is order-independent (an explicit Deny
	// wins wherever it sits), but a stable order keeps MatchedStatements reproducible
	// for the callers that report them.
	sources := []iamEntity{entity}
	if entity.Kind == "user" {
		groups, err := a.loadAuthzStringList(goCtx, iamUserGroupsKey(entity.Account, entity.Name))
		if err != nil {
			return nil, fmt.Errorf("load group memberships: %w", err)
		}
		for _, name := range groups {
			sources = append(sources, iamEntity{Account: entity.Account, Kind: "group", Name: name})
		}
	}

	arnsBySource := make([][]string, 0, len(sources))
	for _, source := range sources {
		arns, err := a.loadAuthzStringList(goCtx, iamAttachedPoliciesKey(source.Account, source.Kind, source.Name))
		if err != nil {
			return nil, fmt.Errorf("load policy list: %w", err)
		}
		arnsBySource = append(arnsBySource, arns)

		// Fast path: AdministratorAccess grants all actions, from whichever source it
		// is attached to.
		for _, arn := range arns {
			if arn == authzAdministratorAccessARN {
				return []PolicyDocument{{
					Version: "2012-10-17",
					Statement: []PolicyStatement{{
						Effect:   IAMEffectAllow,
						Action:   StringOrSlice{"*"},
						Resource: StringOrSlice{"*"},
					}},
				}}, nil
			}
		}
	}

	var docs []PolicyDocument
	for i, source := range sources {
		for _, arn := range arnsBySource[i] {
			if doc, ok := a.resolveManagedPolicyDoc(goCtx, arn); ok {
				docs = append(docs, doc)
			}
		}
		docs = append(docs, a.loadInlinePolicyDocs(goCtx, source)...)
	}

	return docs, nil
}

// loadAuthzStringList reads a JSON string list from IAM state, treating an absent
// key as an empty list.
func (a *AuthController) loadAuthzStringList(goCtx context.Context, key string) ([]string, error) {
	raw, err := a.state.Get(goCtx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	if raw == nil {
		return nil, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return list, nil
}

// resolveManagedPolicyDoc resolves a managed policy ARN to its document, checking
// the bundled catalog before state — the same order every other IAM caller uses, so
// a bundled and a customer-managed policy behave alike.
//
// An ARN that resolves to neither is reported as not found rather than as an
// error: substrate bundles 52 of the ~1,200 AWS managed policies, so an attachment
// naming an unbundled one is expected and must not fail the whole load.
func (a *AuthController) resolveManagedPolicyDoc(goCtx context.Context, arn string) (PolicyDocument, bool) {
	if mp, ok := GetManagedPolicy(arn); ok {
		return mp.Document, true
	}
	polRaw, err := a.state.Get(goCtx, iamNamespace, iamPolicyKey(arn))
	if err != nil || polRaw == nil {
		return PolicyDocument{}, false
	}
	var pol IAMPolicy
	if err := json.Unmarshal(polRaw, &pol); err != nil {
		return PolicyDocument{}, false
	}
	return pol.Document, true
}

// loadInlinePolicyDocs returns the inline policy documents embedded in an entity.
//
// A read that fails yields no document rather than an error, matching the
// behavior this replaced: an unreadable inline policy must not turn into a
// blanket deny, and CheckAccess already fails open on the errors it does see.
func (a *AuthController) loadInlinePolicyDocs(goCtx context.Context, entity iamEntity) []PolicyDocument {
	namesRaw, err := a.state.Get(goCtx, iamNamespace, iamInlinePolicyNamesKey(entity.Account, entity.Kind, entity.Name))
	if err != nil || namesRaw == nil {
		return nil
	}
	var names []string
	if err := json.Unmarshal(namesRaw, &names); err != nil {
		return nil
	}
	var docs []PolicyDocument
	for _, name := range names {
		docRaw, err := a.state.Get(goCtx, iamNamespace, iamInlinePolicyKey(entity.Account, entity.Kind, entity.Name, name))
		if err != nil || docRaw == nil {
			continue
		}
		var doc PolicyDocument
		if err := json.Unmarshal(docRaw, &doc); err != nil {
			continue
		}
		docs = append(docs, doc)
	}
	return docs
}

// loadPermissionBoundary loads the permission boundary PolicyDocument for the
// resolved entity, or nil if none is set.
//
// It takes the same resolved entity the policy load did, so a boundary applies to
// an assumed-role session through the role it assumed. Reading the ARN again here
// is what silently stopped boundaries applying to exactly the principals #411
// made enforceable.
func (a *AuthController) loadPermissionBoundary(entity iamEntity) (*PolicyDocument, error) {
	goCtx := context.Background()

	entityRaw, err := a.state.Get(goCtx, iamNamespace, iamEntityKey(entity.Account, entity.Kind, entity.Name))
	if err != nil || entityRaw == nil {
		return nil, err
	}

	var stored struct {
		PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary,omitempty"`
	}
	if err := json.Unmarshal(entityRaw, &stored); err != nil {
		return nil, fmt.Errorf("unmarshal entity for boundary: %w", err)
	}
	if stored.PermissionsBoundary == nil {
		return nil, nil
	}

	// Resolve the boundary policy document.
	arn := stored.PermissionsBoundary.PolicyARN
	if mp, ok := GetManagedPolicy(arn); ok {
		return &mp.Document, nil
	}
	polRaw, err := a.state.Get(goCtx, iamNamespace, iamPolicyKey(arn))
	if err != nil || polRaw == nil {
		return nil, err
	}
	var pol IAMPolicy
	if err := json.Unmarshal(polRaw, &pol); err != nil {
		return nil, fmt.Errorf("unmarshal boundary policy: %w", err)
	}
	return &pol.Document, nil
}

// authzNoPermissionsRequired holds the actions AWS documents as requiring no
// permissions at all, so a policy — including an explicit Deny — cannot block
// them.
//
// These two are the whole set: a sweep of every botocore service model finds the
// phrase "no permissions are required" on nothing else. Both are STS operations
// whose purpose is to answer a question about the caller itself.
// GetCallerIdentity is the stronger case — "if an administrator attaches a policy
// to your identity that explicitly denies access to the sts:GetCallerIdentity
// action, you can still perform this operation", because the same information is
// returned when access is denied — and GetSessionToken's purpose is to
// authenticate a user via MFA, which a policy cannot gate ("you cannot use
// policies to control authentication operations").
//
// This matters here because it is only reachable now. Before principal
// resolution (#411) a caller identified as nobody and the evaluator never ran, so
// substrate denied neither; the first credential that resolved to a real IAM user
// was denied `aws sts get-caller-identity`, which on AWS always answers.
var authzNoPermissionsRequired = map[string]bool{
	"sts:GetCallerIdentity": true,
	"sts:GetSessionToken":   true,
}

// verbActionServices are the services whose IAM actions are HTTP verbs rather
// than operation names.
//
// API Gateway v1 is the case that matters: its Service Authorization Reference
// defines apigateway:GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS and no
// apigateway:CreateRestApi, because permissions map to the HTTP method used
// against a management resource. API Gateway v2 is a different service prefix
// (apigatewayv2) and does use operation names, so it is deliberately absent.
//
// For these services the verb ParseAWSRequest started with is the correct action,
// which is why authorization reads HTTPMethod rather than the resolved
// Operation. See serviceToAction.
var verbActionServices = map[string]bool{
	"apigateway": true,
}

// operationActionOverrides maps a (service, operation) pair whose IAM action is
// not its operation name to the action a policy must actually name.
//
// The IAM action is usually the operation name, but "usually" is not "always",
// and a wrong action string denies a caller whose policy is correct. Every entry
// is from the operation's own API reference or its Service Authorization
// Reference:
//
//   - Lambda's Invoke requires lambda:InvokeFunction — the one rename in Lambda's
//     otherwise identical mapping.
//   - S3's list operations authorize against the bucket, not the listing:
//     ListObjects/ListObjectsV2 and HeadBucket require s3:ListBucket ("You must
//     have permission to perform the s3:ListBucket action"), ListBuckets requires
//     s3:ListAllMyBuckets, and the versioned and multipart listings have their own
//     bucket-scoped actions.
//   - DeleteObjects is the batch form of DeleteObject and requires that action.
//
// Keyed by "service:operation" so the lookup is one map access on the string
// serviceToAction was going to build anyway.
var operationActionOverrides = map[string]string{
	"lambda:Invoke": "lambda:InvokeFunction",

	"s3:ListObjects":          "s3:ListBucket",
	"s3:ListObjectsV2":        "s3:ListBucket",
	"s3:HeadBucket":           "s3:ListBucket",
	"s3:ListBuckets":          "s3:ListAllMyBuckets",
	"s3:ListObjectVersions":   "s3:ListBucketVersions",
	"s3:ListMultipartUploads": "s3:ListBucketMultipartUploads",
	"s3:DeleteObjects":        "s3:DeleteObject",
}

// serviceToAction maps a request to the IAM action a policy must allow,
// e.g. ("s3", "PutObject") → "s3:PutObject".
//
// It is not simply service + ":" + operation. Two things break that identity, and
// both produce a denial for a caller whose policy is right:
//
//   - a handful of operations authorize under a different action name
//     (operationActionOverrides);
//   - a few services authorize by HTTP verb instead of operation name
//     (verbActionServices), so for those the resolved operation name must be
//     ignored in favor of the verb.
//
// req carries the HTTP verb a verb-action service needs; a nil req falls back to
// the operation name, which is what an in-process caller that never set one has.
func serviceToAction(req *AWSRequest, service, operation string) string {
	if verbActionServices[service] {
		if method := requestMethod(req); method != "" {
			return service + ":" + method
		}
	}
	action := service + ":" + operation
	if override, ok := operationActionOverrides[action]; ok {
		return override
	}
	return action
}

// authzResource is one resource a request names, paired with its own tags.
//
// The pairing is the point: the tags belong to the resource the ARN names, so a
// condition written about one resource cannot be satisfied by a tag on another.
// Tags are the raw key-value pairs, not yet prefixed with aws:ResourceTag/.
type authzResource struct {
	// ARN is the resource ARN a policy statement's Resource element is matched
	// against. It is never "" — resourceMatches treats the empty string as
	// matching every statement, so an unresolvable resource is "*".
	ARN string

	// Tags are the tags on the resource ARN names, or nil when it carries none.
	Tags map[string]string

	// Context holds condition keys that describe this resource rather than the
	// request — ec2:Subnet and ec2:Vpc on a launch's network interface, which the
	// Service Authorization Reference lists on that resource and not on the action
	// (#692). It is nil for a resource with none, which is every resource outside
	// EC2's launch path.
	//
	// Per-resource rather than merged into the request context for the same reason
	// Tags is: a key describing one resource must not satisfy a condition written
	// about another. AWS's own subnet guardrail depends on that — it denies
	// RunInstances on network-interface/* unless ec2:Subnet names one subnet, and
	// the launch's other four resources must not carry the key at all.
	Context map[string]string
}

// buildResourceARNs returns every resource the request names, each paired with
// its own tags. All of them must be allowed for the request to be allowed.
//
// Almost every AWS operation names one resource, and for those this returns
// exactly one pair — [buildResourceARN] plus the tags [AuthController.addResourceTags]
// reads. Three operations are exceptions:
//
//   - `organizations:MoveAccount` names an account, a source parent and a
//     destination parent, all three marked required by the Service Authorization
//     Reference, and authorizing against one of them over-permitted a policy
//     scoped to the other two (#660).
//   - `ec2:RunInstances` names an AMI, a subnet, security groups and network
//     interfaces alongside the instance it creates — five required resource
//     types, the most of any EC2 action — and was decided against a single ARN
//     built from InstanceId.1, which a launch never carries (#662).
//   - `ec2:CreateTags` and `ec2:DeleteTags` name up to 1000 resources of mixed
//     types in ResourceId.N, and were decided against that same absent
//     InstanceId.1 — so against "*" (#674).
//   - Every other EC2 operation naming resources by ID — `TerminateInstances` with
//     three InstanceId.N, and the three other plural parameters in
//     [ec2AuthzIDParams] — was decided against the first ID alone, leaving the
//     rest of the batch unauthorized (#744).
//
// The IAM arm is a different shape of exception: it still names one resource, but the
// one [buildResourceARN] answers for IAM is a flat `arn:aws:iam::<acct>:*` that a
// statement scoped to a role path cannot match. [iamAuthzResources] answers the three
// service-linked-role operations properly, and only those — the general per-operation
// IAM resource is #770.
//
// Each exception is gated on len(multi) > 0, which is what keeps every other
// operation of those services on the single-resource path below.
//
// The list is never empty. An empty list would skip the decision loop entirely
// and allow the request, which is the one direction a privilege boundary must
// never fail in.
func (a *AuthController) buildResourceARNs(reqCtx *RequestContext, req *AWSRequest) []authzResource {
	if req.Service == organizationsNamespace {
		if multi := orgAuthzMoveAccountResources(a.state, reqCtx, req); len(multi) > 0 {
			return multi
		}
	}
	if req.Service == "ec2" {
		if multi := ec2AuthzRunInstancesResources(a.state, reqCtx, req); len(multi) > 0 {
			return multi
		}
		if multi := ec2AuthzTagResources(a.state, reqCtx, req); len(multi) > 0 {
			return multi
		}
		if multi := ec2AuthzNamedResources(a.state, reqCtx, req); len(multi) > 0 {
			return multi
		}
	}
	if req.Service == "iam" {
		if multi := iamAuthzResources(a.state, reqCtx, req); len(multi) > 0 {
			return multi
		}
	}
	if req.Service == elbServiceName {
		if multi := elbAuthzResources(a.state, reqCtx, req); len(multi) > 0 {
			return multi
		}
	}
	return []authzResource{{
		ARN:  a.buildResourceARN(reqCtx, req),
		Tags: a.resourceTagsFor(reqCtx, req),
	}}
}

// buildResourceARN constructs a best-effort IAM resource ARN for the request.
//
// It is a method rather than a free function because some services cannot name
// their resources without reading them: an Organizations ARN embeds the
// organization's own ID, which only the stored record knows.
//
// It answers for one resource. An operation that names several is resolved by
// [AuthController.buildResourceARNs], which calls this for the single-resource
// case every other operation falls into.
func (a *AuthController) buildResourceARN(reqCtx *RequestContext, req *AWSRequest) string {
	acct := reqCtx.AccountID
	region := reqCtx.Region
	switch req.Service {
	case "s3":
		return buildS3ARN(req)
	case "iam":
		// Every IAM operation but the three service-linked-role ones, which
		// [iamAuthzResources] answers before this is reached. This is a flat `*` in the
		// resource position, so a statement scoped to a user, a role or a path matches
		// nothing here — which is why `${aws:username}` in IAMUserChangePassword's
		// Resource still grants nothing even with #745's substitution in place. Deriving
		// a per-operation resource for the rest of IAM is #770.
		return "arn:aws:iam::" + acct + ":*"
	case "ec2":
		// No EC2 request that names a resource substrate can resolve reaches here. A
		// launch names five and is answered by [ec2AuthzRunInstancesResources] (#662);
		// a tagging call's up-to-1000 by [ec2AuthzTagResources] (#674); and every ID
		// named under one of [ec2AuthzIDParams] by [ec2AuthzNamedResources] (#730,
		// #744), which returns a non-empty list whenever any of them resolves. What is
		// left is an operation naming none of them, whose request resource is "*".
		return "*"
	case "lambda":
		name := lambdaNameFromPath(req.Path)
		if name != "" {
			return "arn:aws:lambda:" + region + ":" + acct + ":function:" + name
		}
		return "*"
	case "dynamodb":
		if tbl := req.Params["TableName"]; tbl != "" {
			return "arn:aws:dynamodb:" + region + ":" + acct + ":table/" + tbl
		}
		return "*"
	case "sqs":
		if qurl := req.Params["QueueUrl"]; qurl != "" {
			// Extract queue name from URL: .../accountID/queueName
			parts := strings.Split(strings.TrimRight(qurl, "/"), "/")
			if len(parts) >= 1 {
				name := parts[len(parts)-1]
				return "arn:aws:sqs:" + region + ":" + acct + ":" + name
			}
		}
		return "*"
	case "apigateway":
		// /restapis/{apiId}/...
		parts := strings.SplitN(strings.TrimPrefix(req.Path, "/restapis/"), "/", 2)
		if parts[0] != "" {
			return "arn:aws:apigateway:" + region + "::/restapis/" + parts[0]
		}
		return "arn:aws:apigateway:" + region + "::/*"
	case "apigatewayv2":
		// /v2/apis/{apiId}/...
		trimmed := strings.TrimPrefix(req.Path, "/v2/apis/")
		if trimmed != req.Path {
			parts := strings.SplitN(trimmed, "/", 2)
			if parts[0] != "" {
				return "arn:aws:apigateway:" + region + "::/apis/" + parts[0]
			}
		}
		return "arn:aws:apigateway:" + region + "::/*"
	case "acm":
		if arn := req.Params["CertificateArn"]; arn != "" {
			return arn
		}
		return "*"
	case "states":
		if name := req.Params["name"]; name != "" {
			return "arn:aws:states:" + region + ":" + acct + ":stateMachine:" + name
		}
		return "*"
	case "ecr":
		if name := req.Params["repositoryName"]; name != "" {
			return "arn:aws:ecr:" + region + ":" + acct + ":repository/" + name
		}
		return "*"
	case "ecs":
		if cluster := req.Params["cluster"]; cluster != "" {
			return "arn:aws:ecs:" + region + ":" + acct + ":cluster/" + cluster
		}
		return "*"
	case "cognito-idp":
		if poolID := req.Params["UserPoolId"]; poolID != "" {
			return "arn:aws:cognito-idp:" + region + ":" + acct + ":userpool/" + poolID
		}
		return "*"
	case "cognito-identity":
		if poolID := req.Params["IdentityPoolId"]; poolID != "" {
			return "arn:aws:cognito-identity:" + region + ":" + acct + ":identitypool/" + poolID
		}
		return "*"
	case "kinesis":
		if name := req.Params["StreamName"]; name != "" {
			return "arn:aws:kinesis:" + region + ":" + acct + ":stream/" + name
		}
		return "*"
	case "rds":
		if id := req.Params["DBInstanceIdentifier"]; id != "" {
			return "arn:aws:rds:" + region + ":" + acct + ":db:" + id
		}
		return "*"
	case "elasticache":
		if id := req.Params["CacheClusterId"]; id != "" {
			return "arn:aws:elasticache:" + region + ":" + acct + ":cluster:" + id
		}
		return "*"
	case "elasticfilesystem":
		// path-based REST: /2015-02-01/file-systems/{id}
		pathParts := strings.SplitN(req.Path, "/", 5)
		if len(pathParts) >= 4 && pathParts[3] != "" {
			return "arn:aws:elasticfilesystem:" + region + ":" + acct + ":file-system/" + pathParts[3]
		}
		return "*"
	case "glue":
		if name := req.Params["Name"]; name != "" {
			return "arn:aws:glue:" + region + ":" + acct + ":database/" + name
		}
		return "*"
	case organizationsNamespace:
		// Organizations ARNs carry the organization's own ID as a segment, so the
		// real ARN is read from the stored record rather than reassembled here; see
		// orgAuthzResourceARN. Falling through to "*" would make a policy scoped to
		// one OU or policy apply to every one of them.
		return orgAuthzResourceARN(a.state, reqCtx, req)
	case configServiceNamespace:
		// A Config request's resource is resolved from the operation, because the
		// member naming it differs per operation and is nested for the two Puts. See
		// cfgsvcAuthzResourceARN for why an operation naming a *list* of resources
		// resolves to "*" rather than to one arbitrary member of the list.
		return cfgsvcAuthzResourceARN(reqCtx, req)
	default:
		return "*"
	}
}

// lambdaNameFromPath extracts the function name from a Lambda REST path
// like /2015-03-31/functions/{name}[/...].
func lambdaNameFromPath(path string) string {
	const prefix = "/2015-03-31/functions/"
	rest := strings.TrimPrefix(path, prefix)
	if rest == path {
		return ""
	}
	// Stop at the next '/'
	if idx := strings.Index(rest, "/"); idx >= 0 {
		return rest[:idx]
	}
	return rest
}

// buildS3ARN constructs an S3 resource ARN from the request path.
func buildS3ARN(req *AWSRequest) string {
	path := strings.TrimPrefix(req.Path, "/")
	if path == "" {
		return "arn:aws:s3:::*"
	}
	return "arn:aws:s3:::" + path
}

// authzAWSResourceTagPrefix is the global condition-key prefix every service reports a
// resource's tags under.
const authzAWSResourceTagPrefix = "aws:ResourceTag/"

// authzResourceTagPrefixes returns the condition-key prefixes a service reports a
// resource's tags under, in the order they are written.
//
// AWS documents aws:ResourceTag/tag-key as global — "the tag attached to the resource
// that you specify in the policy" — and some services additionally publish a
// service-specific duplicate of it. EC2 is one: two of the AWS-authored managed policies
// substrate bundles condition on ec2:ResourceTag/<key> rather than the global form
// (ManagedCloudformationResourcesCleanupPolicy on aws:cloudformation:stack-name,
// AmazonEKSClusterPolicy on eks:eni:owner), so a policy written the way AWS writes it
// sees nothing unless the tags are reported under that prefix too.
//
// The list is per-service and deliberately not a blanket second prefix: reporting an S3
// bucket's tags under s3:ResourceTag/ would invent a condition key AWS does not publish,
// and a policy that happened to use it would then be honored here and ignored by AWS —
// a divergence in the more dangerous direction, since it grants.
func authzResourceTagPrefixes(service string) []string {
	switch service {
	case "ec2":
		return []string{authzAWSResourceTagPrefix, "ec2:ResourceTag/"}
	case elbServiceName:
		// ELB is the other: "The elasticloadbalancing:ResourceTag/{{key}} condition key is
		// specific to Elastic Load Balancing. All mutating actions support this condition
		// key" (#748). Substrate reports it on every ELB action rather than only the
		// mutating ones, because a read reporting fewer keys than a write could only make a
		// condition on a describe unsatisfiable, and AWS's own list of what counts as
		// mutating is not published operation by operation.
		return []string{authzAWSResourceTagPrefix, elbResourceTagPrefix}
	}
	return []string{authzAWSResourceTagPrefix}
}

// resourceTagsFor loads the tags on the resource a request names, keyed by the
// bare tag key. [AuthController.CheckAccess] adds the aws:ResourceTag/ prefix —
// and any service-specific duplicate of it, see [authzResourceTagPrefixes] —
// once per resource in the list, so an operation naming several resources cannot
// mix one resource's tags into another's condition context.
func (a *AuthController) resourceTagsFor(reqCtx *RequestContext, req *AWSRequest) map[string]string {
	goCtx := context.Background()
	acct := reqCtx.AccountID
	var tags map[string]string

	switch req.Service {
	case "s3":
		bucket := bucketFromPath(req.Path)
		if bucket == "" {
			return nil
		}
		raw, err := a.state.Get(goCtx, s3Namespace, "bucket:"+bucket)
		if err != nil || raw == nil {
			return nil
		}
		var b S3Bucket
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil
		}
		tags = b.Tags

	case "lambda":
		name := lambdaNameFromPath(req.Path)
		if name == "" {
			return nil
		}
		raw, err := a.state.Get(goCtx, lambdaNamespace, "function:"+name)
		if err != nil || raw == nil {
			return nil
		}
		var fn LambdaFunction
		if err := json.Unmarshal(raw, &fn); err != nil {
			return nil
		}
		tags = fn.Tags

	case "sqs":
		qurl := strings.TrimRight(req.Params["QueueUrl"], "/")
		if qurl == "" {
			return nil
		}
		parts := strings.Split(qurl, "/")
		name := parts[len(parts)-1]
		if name == "" {
			return nil
		}
		raw, err := a.state.Get(goCtx, sqsNamespace, "queue:"+name)
		if err != nil || raw == nil {
			return nil
		}
		var q SQSQueue
		if err := json.Unmarshal(raw, &q); err != nil {
			return nil
		}
		tags = q.Tags

	case "iam":
		entityType, entityName := parsePrincipalARN(reqCtx.Principal.ARN)
		// The entity's own account, which is the one its ARN names rather than the one
		// the request resolved to — the two agree for every call substrate routes today,
		// and the ARN is the authority when they do not (#737).
		entityAccount := arnAccountID(reqCtx.Principal.ARN)
		switch entityType {
		case "user":
			raw, err := a.state.Get(goCtx, iamNamespace, iamUserKey(entityAccount, entityName))
			if err != nil || raw == nil {
				return nil
			}
			var u IAMUser
			if err := json.Unmarshal(raw, &u); err != nil {
				return nil
			}
			tags = iamTagsToMap(u.Tags)
		case "role":
			raw, err := a.state.Get(goCtx, iamNamespace, iamRoleKey(entityAccount, entityName))
			if err != nil || raw == nil {
				return nil
			}
			var r IAMRole
			if err := json.Unmarshal(raw, &r); err != nil {
				return nil
			}
			tags = iamTagsToMap(r.Tags)
		default:
			return nil
		}

	// EC2 has no arm here, and its absence is deliberate rather than a gap. Every EC2
	// request naming a resource substrate can resolve is answered by one of
	// buildResourceARNs' multi-resource arms, each of which reads that resource's own tags
	// through [ec2AuthzTagsFor] beside its ARN — so the tags always belong to the ARN the
	// decision is made about, which one arm here could not guarantee for a request naming
	// several resources. An ec2 arm did exist for the single-resource path until
	// [ec2AuthzNamedResources] made that path unreachable for any request with a resolvable
	// ID (#744); leaving it would have been a second reading of the same request that no
	// call could reach.

	case "dynamodb":
		tbl := req.Params["TableName"]
		if tbl == "" {
			return nil
		}
		raw, err := a.state.Get(goCtx, dynamodbNamespace, "table:"+acct+"/"+tbl)
		if err != nil || raw == nil {
			return nil
		}
		var t DynamoDBTable
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil
		}
		tags = t.Tags

	case organizationsNamespace:
		// The request names one Organizations resource — an account, root, OU, or
		// policy — under whichever member the operation uses. Only that one is read;
		// see orgAuthzResourceID for why merging the tags of two named resources
		// would be a false allow rather than a convenience.
		tags = orgAuthzResourceTags(a.state, req)

	case configServiceNamespace:
		// Config tags are keyed by the resource's ARN, so the same resolution
		// buildResourceARN performs decides whose tags these are — the two cannot
		// disagree about which resource a request names.
		tags = cfgsvcAuthzResourceTags(a.state, reqCtx, req)

	default:
		return nil
	}

	return tags
}

// addRequestTags parses tags being set on the resource in this request and
// injects them as aws:RequestTag/{Key} entries into condCtx.
func addRequestTags(condCtx map[string]string, req *AWSRequest) {
	switch req.Service {
	case "iam":
		// IAM tags arrive in the JSON body as "Tags": [{"Key":...,"Value":...}]
		var body struct {
			Tags []IAMTag `json:"Tags"`
		}
		if err := json.Unmarshal(req.Body, &body); err == nil {
			for _, t := range body.Tags {
				condCtx["aws:RequestTag/"+t.Key] = t.Value
			}
		}

	case "ec2":
		// EC2 tags arrive as query params: TagSpecification.1.Tag.N.Key / .Value
		//
		// This walk is deliberately not ec2LaunchTagsForResource: it collects every
		// tag in the request regardless of which resource the specification is scoped
		// to, because a policy condition on aws:RequestTag is about what the request
		// asked for, not about what state a handler ends up writing. Filtering by
		// resourceType here would silently narrow policy evaluation (#468).
		for i := 1; ; i++ {
			prefix := fmt.Sprintf("TagSpecification.%d.Tag.", i)
			found := false
			for j := 1; ; j++ {
				k := req.Params[fmt.Sprintf("%s%d.Key", prefix, j)]
				v := req.Params[fmt.Sprintf("%s%d.Value", prefix, j)]
				if k == "" {
					break
				}
				condCtx["aws:RequestTag/"+k] = v
				found = true
			}
			if !found {
				break
			}
		}
		// A direct CreateTags or DeleteTags carries no TagSpecification: its tags are
		// Tag.N.Key / Tag.N.Value, the spelling [extractEC2Tags] reads. The walk above
		// found none of them, so aws:RequestTag/* was empty for every tagging call, and
		// AWS's own documented CreateTags policies — which condition on exactly that key
		// — could not be satisfied. Resolving the resource ARNs those policies name
		// (#674) without this would have turned their false allow into a false deny: the
		// statement would finally match the resource and then fail on the missing key.
		//
		// DeleteTags treats a tag's value as optional, so a request that names only a key
		// records an empty value here. That is not a new denial: [conditionMatches] reads
		// an absent key as the empty string too, so the two are indistinguishable to
		// every operator, including Null.
		for i := 1; ; i++ {
			k := req.Params[fmt.Sprintf("Tag.%d.Key", i)]
			if k == "" {
				break
			}
			condCtx["aws:RequestTag/"+k] = req.Params[fmt.Sprintf("Tag.%d.Value", i)]
		}

	case "lambda":
		// Lambda tags arrive in the JSON body as "Tags": {"key": "value"}
		var body struct {
			Tags map[string]string `json:"Tags"`
		}
		if err := json.Unmarshal(req.Body, &body); err == nil {
			for k, v := range body.Tags {
				condCtx["aws:RequestTag/"+k] = v
			}
		}

	case organizationsNamespace:
		// Organizations tags arrive in the JSON body as "Tags": [{"Key":…,"Value":…}],
		// the same shape IAM uses. Reading them here is what lets a policy gate
		// TagResource or a tagged CreatePolicy on the tag the caller is trying to
		// apply, rather than only on tags already stored.
		for k, v := range orgAuthzRequestTags(req) {
			condCtx["aws:RequestTag/"+k] = v
		}

	case configServiceNamespace:
		// Config tags arrive as "Tags": [{"Key":…,"Value":…}] on TagResource and on all
		// four creating Puts, so one reader serves them. aws:RequestTag/${TagKey} is a
		// condition key on TagResource specifically, per the Service Authorization
		// Reference, which is what makes a "may only apply approved tags" policy
		// expressible here.
		for k, v := range cfgsvcAuthzRequestTags(req) {
			condCtx["aws:RequestTag/"+k] = v
		}

	case elbServiceName:
		// ELB tags arrive as query params — Tags.member.N.Key / .Value on AddTags and on
		// each of the four creates, TagKeys.member.N on RemoveTags — so one reader serves
		// all six (#748). Without this, aws:RequestTag/* and aws:TagKeys were empty for
		// every ELB call, so the tag-on-create pass below could not evaluate the key set
		// that is the whole point of the second decision.
		for k, v := range elbAuthzRequestTags(req) {
			condCtx["aws:RequestTag/"+k] = v
		}

	case "s3":
		// S3 tags arrive in the x-amz-tagging header as URL-encoded key=value pairs.
		tagging := req.Headers["x-amz-tagging"]
		if tagging == "" {
			return
		}
		vals, err := url.ParseQuery(tagging)
		if err != nil {
			return
		}
		for k, vs := range vals {
			if len(vs) > 0 {
				condCtx["aws:RequestTag/"+k] = vs[0]
			}
		}
	}
}

// requestTagKeys returns the tag keys a request context carries, in sorted order —
// the value of AWS's aws:TagKeys condition key.
//
// It reads the aws:RequestTag/ entries [addRequestTags] just produced rather than
// re-walking the request, so the two can never disagree about what the request asked
// for, and a service arm added to that function populates this key for free. AWS
// documents the pair the same way: aws:RequestTag/${TagKey} is "the tag key-value pair
// in a request" while aws:TagKeys "defines what tag-keys are allowed in a request but
// does not include the tag-key values".
//
// **The sort is load-bearing.** Three of addRequestTags' arms iterate a Go map, whose
// order is randomized per run, and a ForAllValues decision quantifies over this slice.
// An unsorted value would make a *denial* depend on map iteration order — the one
// thing an event log that must replay identically cannot tolerate. It also keeps the
// recorded context byte-stable, so two runs of the same request produce the same
// event.
func requestTagKeys(condCtx map[string]string) []string {
	const prefix = "aws:RequestTag/"
	keys := make([]string, 0, len(condCtx))
	for k := range condCtx {
		if after, ok := strings.CutPrefix(k, prefix); ok && after != "" {
			keys = append(keys, after)
		}
	}
	sort.Strings(keys)
	return keys
}

// iamTagsToMap converts []IAMTag to a map[string]string.
func iamTagsToMap(tags []IAMTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

// ec2TagsToMap converts []EC2Tag to a map[string]string.
func ec2TagsToMap(tags []EC2Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

// bucketFromPath extracts the bucket name from an S3 request path like /bucket[/key].
func bucketFromPath(path string) string {
	p := strings.TrimPrefix(path, "/")
	if p == "" {
		return ""
	}
	if idx := strings.Index(p, "/"); idx >= 0 {
		return p[:idx]
	}
	return p
}
