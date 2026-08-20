package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// AuthController enforces IAM policy decisions for all AWS service requests.
// It is wired into the server pipeline as ServerOptions.Auth and called before
// quota and consistency checks. A nil Principal (bootstrap / test mode) always
// passes.
type AuthController struct {
	state  StateManager
	logger Logger
}

// NewAuthController creates an AuthController backed by state and logger.
func NewAuthController(state StateManager, logger Logger) *AuthController {
	return &AuthController{state: state, logger: logger}
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

	// Every resource the request names must be allowed. A request naming several —
	// organizations:MoveAccount and ec2:RunInstances — is denied unless the caller's
	// policies admit all of them, which is how AWS evaluates a statement against
	// "every resource that is required" for the action (#660, #662).
	for _, res := range resources {
		condCtx := make(map[string]string, len(requestTags)+len(res.Tags))
		for k, v := range requestTags {
			condCtx[k] = v
		}
		// The tags travel with the ARN they belong to: one merged map across several
		// resources would let a tag on one satisfy a condition written about another,
		// which is the false allow orgAuthzResourceID's comment exists to prevent.
		for k, v := range res.Tags {
			condCtx["aws:ResourceTag/"+k] = v
		}

		result := Evaluate(docs, EvaluationRequest{
			Principal: reqCtx.Principal.ARN,
			Action:    action,
			Resource:  res.ARN,
			Context:   condCtx,
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
				Principal: reqCtx.Principal.ARN,
				Action:    action,
				Resource:  res.ARN,
				Context:   condCtx,
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

	return nil
}

// iamEntity is the IAM entity a principal ARN names, in the terms the policy
// keys are written under.
//
// Kind is "user" or "role" — never "assumed-role", because a session holds no
// policies of its own: IAM resolves an assumed-role's permissions against the
// role it assumed, so a session's Kind is "role" and its Name is the role's.
type iamEntity struct {
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

	switch entityType {
	case "user", "role":
		entity = iamEntity{Kind: entityType, Name: entityName}
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
		entity = iamEntity{Kind: "role", Name: roleName}
	default:
		// A principal substrate does not model as a policy-holding entity — a
		// service principal, a federated user, the account root. Not enforced,
		// rather than denied: refusing a caller whose policies cannot be looked up
		// would deny requests that pass today for a reason no test could fix.
		return iamEntity{}, false, nil
	}

	raw, err := state.Get(ctx, iamNamespace, entity.Kind+":"+entity.Name)
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
		groups, err := a.loadAuthzStringList(goCtx, iamUserGroupsKey(entity.Name))
		if err != nil {
			return nil, fmt.Errorf("load group memberships: %w", err)
		}
		for _, name := range groups {
			sources = append(sources, iamEntity{Kind: "group", Name: name})
		}
	}

	arnsBySource := make([][]string, 0, len(sources))
	for _, source := range sources {
		arns, err := a.loadAuthzStringList(goCtx, source.Kind+"_policies:"+source.Name)
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
	polRaw, err := a.state.Get(goCtx, iamNamespace, "policy:"+arn)
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
	namesRaw, err := a.state.Get(goCtx, iamNamespace, entity.Kind+"_inline_names:"+entity.Name)
	if err != nil || namesRaw == nil {
		return nil
	}
	var names []string
	if err := json.Unmarshal(namesRaw, &names); err != nil {
		return nil
	}
	var docs []PolicyDocument
	for _, name := range names {
		docRaw, err := a.state.Get(goCtx, iamNamespace, entity.Kind+"_inline:"+entity.Name+":"+name)
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

	entityRaw, err := a.state.Get(goCtx, iamNamespace, entity.Kind+":"+entity.Name)
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
	polRaw, err := a.state.Get(goCtx, iamNamespace, "policy:"+arn)
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
}

// buildResourceARNs returns every resource the request names, each paired with
// its own tags. All of them must be allowed for the request to be allowed.
//
// Almost every AWS operation names one resource, and for those this returns
// exactly one pair — [buildResourceARN] plus the tags [AuthController.addResourceTags]
// reads. Two operations are exceptions:
//
//   - `organizations:MoveAccount` names an account, a source parent and a
//     destination parent, all three marked required by the Service Authorization
//     Reference, and authorizing against one of them over-permitted a policy
//     scoped to the other two (#660).
//   - `ec2:RunInstances` names an AMI, a subnet, security groups and network
//     interfaces alongside the instance it creates — five required resource
//     types, the most of any EC2 action — and was decided against a single ARN
//     built from InstanceId.1, which a launch never carries (#662).
//
// Each exception is gated on len(multi) > 0, which is what keeps every other
// operation of those two services on the single-resource path below.
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
		return "arn:aws:iam::" + acct + ":*"
	case "ec2":
		// RunInstances does not reach here: it names five resources and is resolved
		// by [ec2AuthzRunInstancesResources] (#662). What is left is the operations
		// that name an instance by ID — and everything else, including a CreateTags
		// naming several ResourceId.N, which still resolves to "*".
		if id := req.Params["InstanceId.1"]; id != "" {
			return "arn:aws:ec2:" + region + ":" + acct + ":instance/" + id
		}
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

// resourceTagsFor loads the tags on the resource a request names, keyed by the
// bare tag key. [AuthController.CheckAccess] adds the aws:ResourceTag/ prefix,
// once per resource in the list, so an operation naming several resources cannot
// mix one resource's tags into another's condition context.
func (a *AuthController) resourceTagsFor(reqCtx *RequestContext, req *AWSRequest) map[string]string {
	goCtx := context.Background()
	acct := reqCtx.AccountID
	region := reqCtx.Region
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
		switch entityType {
		case "user":
			raw, err := a.state.Get(goCtx, iamNamespace, "user:"+entityName)
			if err != nil || raw == nil {
				return nil
			}
			var u IAMUser
			if err := json.Unmarshal(raw, &u); err != nil {
				return nil
			}
			tags = iamTagsToMap(u.Tags)
		case "role":
			raw, err := a.state.Get(goCtx, iamNamespace, "role:"+entityName)
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

	case "ec2":
		id := req.Params["InstanceId.1"]
		if id == "" {
			return nil
		}
		raw, err := a.state.Get(goCtx, ec2Namespace, "instance:"+acct+"/"+region+"/"+id)
		if err != nil || raw == nil {
			return nil
		}
		var inst EC2Instance
		if err := json.Unmarshal(raw, &inst); err != nil {
			return nil
		}
		tags = ec2TagsToMap(inst.Tags)

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
