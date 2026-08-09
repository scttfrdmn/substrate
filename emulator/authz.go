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
	resource := buildResourceARN(reqCtx, req)

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

	condCtx := a.buildConditionContext(reqCtx, req)

	result := Evaluate(docs, EvaluationRequest{
		Principal: reqCtx.Principal.ARN,
		Action:    action,
		Resource:  resource,
		Context:   condCtx,
	})

	// Both denial arms take their code from the same helper, so an identity-policy
	// refusal and a boundary refusal cannot drift apart — and neither can drift from
	// the trust-policy gate #593 added, which resolves the same way for STS.
	deniedCode := accessDeniedCodeFor(req.Service, "")

	if result.Decision != DecisionAllow {
		return &AWSError{
			Code: deniedCode,
			Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
				reqCtx.Principal.ARN, action, resource),
			HTTPStatus: http.StatusForbidden,
		}
	}

	// If a permission boundary is set it must also allow the action.
	boundary, err := a.loadPermissionBoundary(entity)
	if err != nil {
		a.logger.Warn("authz: failed to load permission boundary", "principal", reqCtx.Principal.ARN, "err", err)
	}
	if boundary != nil {
		boundaryResult := Evaluate([]PolicyDocument{*boundary}, EvaluationRequest{
			Principal: reqCtx.Principal.ARN,
			Action:    action,
			Resource:  resource,
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

// loadPoliciesForPrincipal loads attached managed + inline policies for the
// entity resolved from reqCtx.Principal.ARN.
func (a *AuthController) loadPoliciesForPrincipal(entity iamEntity) ([]PolicyDocument, error) {
	entityType, entityName := entity.Kind, entity.Name
	listKey := entityType + "_policies:" + entityName

	goCtx := context.Background()

	// Load attached managed policy ARNs.
	raw, err := a.state.Get(goCtx, iamNamespace, listKey)
	if err != nil {
		return nil, fmt.Errorf("load policy list: %w", err)
	}

	var arns []string
	if raw != nil {
		if err := json.Unmarshal(raw, &arns); err != nil {
			return nil, fmt.Errorf("unmarshal policy list: %w", err)
		}
	}

	// Fast path: AdministratorAccess grants all actions.
	for _, arn := range arns {
		if arn == "arn:aws:iam::aws:policy/AdministratorAccess" {
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

	var docs []PolicyDocument

	// Load managed policy documents.
	for _, arn := range arns {
		if mp, ok := GetManagedPolicy(arn); ok {
			docs = append(docs, mp.Document)
			continue
		}
		polRaw, err := a.state.Get(goCtx, iamNamespace, "policy:"+arn)
		if err != nil || polRaw == nil {
			continue
		}
		var pol IAMPolicy
		if err := json.Unmarshal(polRaw, &pol); err != nil {
			continue
		}
		docs = append(docs, pol.Document)
	}

	// Load inline policies.
	namesRaw, err := a.state.Get(goCtx, iamNamespace, entityType+"_inline_names:"+entityName)
	if err == nil && namesRaw != nil {
		var names []string
		if err := json.Unmarshal(namesRaw, &names); err == nil {
			for _, name := range names {
				docRaw, err := a.state.Get(goCtx, iamNamespace, entityType+"_inline:"+entityName+":"+name)
				if err != nil || docRaw == nil {
					continue
				}
				var doc PolicyDocument
				if err := json.Unmarshal(docRaw, &doc); err != nil {
					continue
				}
				docs = append(docs, doc)
			}
		}
	}

	return docs, nil
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

// buildResourceARN constructs a best-effort IAM resource ARN for the request.
func buildResourceARN(reqCtx *RequestContext, req *AWSRequest) string {
	acct := reqCtx.AccountID
	region := reqCtx.Region
	switch req.Service {
	case "s3":
		return buildS3ARN(req)
	case "iam":
		return "arn:aws:iam::" + acct + ":*"
	case "ec2":
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

// buildConditionContext assembles the IAM condition context map for a request,
// populating aws:ResourceTag/* and aws:RequestTag/* keys.
func (a *AuthController) buildConditionContext(reqCtx *RequestContext, req *AWSRequest) map[string]string {
	ctx := make(map[string]string)
	a.addResourceTags(ctx, reqCtx, req)
	addRequestTags(ctx, req)
	return ctx
}

// addResourceTags loads existing resource tags from state and injects them as
// aws:ResourceTag/{Key} entries into condCtx.
func (a *AuthController) addResourceTags(condCtx map[string]string, reqCtx *RequestContext, req *AWSRequest) {
	goCtx := context.Background()
	acct := reqCtx.AccountID
	region := reqCtx.Region
	var tags map[string]string

	switch req.Service {
	case "s3":
		bucket := bucketFromPath(req.Path)
		if bucket == "" {
			return
		}
		raw, err := a.state.Get(goCtx, s3Namespace, "bucket:"+bucket)
		if err != nil || raw == nil {
			return
		}
		var b S3Bucket
		if err := json.Unmarshal(raw, &b); err != nil {
			return
		}
		tags = b.Tags

	case "lambda":
		name := lambdaNameFromPath(req.Path)
		if name == "" {
			return
		}
		raw, err := a.state.Get(goCtx, lambdaNamespace, "function:"+name)
		if err != nil || raw == nil {
			return
		}
		var fn LambdaFunction
		if err := json.Unmarshal(raw, &fn); err != nil {
			return
		}
		tags = fn.Tags

	case "sqs":
		qurl := strings.TrimRight(req.Params["QueueUrl"], "/")
		if qurl == "" {
			return
		}
		parts := strings.Split(qurl, "/")
		name := parts[len(parts)-1]
		if name == "" {
			return
		}
		raw, err := a.state.Get(goCtx, sqsNamespace, "queue:"+name)
		if err != nil || raw == nil {
			return
		}
		var q SQSQueue
		if err := json.Unmarshal(raw, &q); err != nil {
			return
		}
		tags = q.Tags

	case "iam":
		entityType, entityName := parsePrincipalARN(reqCtx.Principal.ARN)
		switch entityType {
		case "user":
			raw, err := a.state.Get(goCtx, iamNamespace, "user:"+entityName)
			if err != nil || raw == nil {
				return
			}
			var u IAMUser
			if err := json.Unmarshal(raw, &u); err != nil {
				return
			}
			tags = iamTagsToMap(u.Tags)
		case "role":
			raw, err := a.state.Get(goCtx, iamNamespace, "role:"+entityName)
			if err != nil || raw == nil {
				return
			}
			var r IAMRole
			if err := json.Unmarshal(raw, &r); err != nil {
				return
			}
			tags = iamTagsToMap(r.Tags)
		default:
			return
		}

	case "ec2":
		id := req.Params["InstanceId.1"]
		if id == "" {
			return
		}
		raw, err := a.state.Get(goCtx, ec2Namespace, "instance:"+acct+"/"+region+"/"+id)
		if err != nil || raw == nil {
			return
		}
		var inst EC2Instance
		if err := json.Unmarshal(raw, &inst); err != nil {
			return
		}
		tags = ec2TagsToMap(inst.Tags)

	case "dynamodb":
		tbl := req.Params["TableName"]
		if tbl == "" {
			return
		}
		raw, err := a.state.Get(goCtx, dynamodbNamespace, "table:"+acct+"/"+tbl)
		if err != nil || raw == nil {
			return
		}
		var t DynamoDBTable
		if err := json.Unmarshal(raw, &t); err != nil {
			return
		}
		tags = t.Tags

	default:
		return
	}

	for k, v := range tags {
		condCtx["aws:ResourceTag/"+k] = v
	}
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
