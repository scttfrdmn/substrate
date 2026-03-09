package substrate

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
// is nil. Returns an *AWSError with code "AccessDeniedException" (HTTP 403)
// when access is denied.
func (a *AuthController) CheckAccess(reqCtx *RequestContext, req *AWSRequest) error {
	if reqCtx.Principal == nil {
		return nil
	}

	action := serviceToAction(req.Service, req.Operation)
	resource := buildResourceARN(reqCtx, req)

	docs, err := a.loadPoliciesForPrincipal(reqCtx)
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

	if result.Decision != DecisionAllow {
		return &AWSError{
			Code: "AccessDeniedException",
			Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
				reqCtx.Principal.ARN, action, resource),
			HTTPStatus: http.StatusForbidden,
		}
	}

	// If a permission boundary is set it must also allow the action.
	boundary, err := a.loadPermissionBoundary(reqCtx)
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
				Code: "AccessDeniedException",
				Message: fmt.Sprintf("User: %s is not authorized to perform: %s (blocked by permission boundary)",
					reqCtx.Principal.ARN, action),
				HTTPStatus: http.StatusForbidden,
			}
		}
	}

	return nil
}

// loadPoliciesForPrincipal loads attached managed + inline policies for the
// principal identified by reqCtx.Principal.ARN.
func (a *AuthController) loadPoliciesForPrincipal(reqCtx *RequestContext) ([]PolicyDocument, error) {
	entityType, entityName := parsePrincipalARN(reqCtx.Principal.ARN)
	if entityName == "" {
		return nil, fmt.Errorf("cannot parse principal ARN %q", reqCtx.Principal.ARN)
	}

	var listKey string
	switch entityType {
	case "user":
		listKey = "user_policies:" + entityName
	case "role":
		listKey = "role_policies:" + entityName
	default:
		return nil, fmt.Errorf("unsupported entity type %q in ARN", entityType)
	}

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
// principal, or nil if none is set.
func (a *AuthController) loadPermissionBoundary(reqCtx *RequestContext) (*PolicyDocument, error) {
	entityType, entityName := parsePrincipalARN(reqCtx.Principal.ARN)
	if entityName == "" {
		return nil, nil
	}

	goCtx := context.Background()

	var entityRaw []byte
	var err error
	switch entityType {
	case "user":
		entityRaw, err = a.state.Get(goCtx, iamNamespace, "user:"+entityName)
	case "role":
		entityRaw, err = a.state.Get(goCtx, iamNamespace, "role:"+entityName)
	default:
		return nil, nil
	}
	if err != nil || entityRaw == nil {
		return nil, err
	}

	var entity struct {
		PermissionsBoundary *IAMAttachedPolicy `json:"PermissionsBoundary,omitempty"`
	}
	if err := json.Unmarshal(entityRaw, &entity); err != nil {
		return nil, fmt.Errorf("unmarshal entity for boundary: %w", err)
	}
	if entity.PermissionsBoundary == nil {
		return nil, nil
	}

	// Resolve the boundary policy document.
	arn := entity.PermissionsBoundary.PolicyARN
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

// serviceToAction maps (service, operation) to an IAM action string,
// e.g. ("s3", "PutObject") → "s3:PutObject".
func serviceToAction(service, operation string) string {
	return service + ":" + operation
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
		qurl := req.Params["QueueUrl"]
		if qurl == "" {
			return
		}
		parts := strings.Split(strings.TrimRight(qurl, "/"), "/")
		name := parts[len(parts)-1]
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
