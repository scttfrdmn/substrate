package substrate

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// testAccountID is the well-known AWS account ID used for test access keys.
const testAccountID = "123456789012"

// fallbackAccountID is used when no account can be determined.
const fallbackAccountID = "000000000000"

// defaultRegion is used when no region can be extracted from the request.
const defaultRegion = "us-east-1"

// ParseAWSRequest extracts service, operation, region, and account from r and
// returns a populated [AWSRequest] and [RequestContext]. It is a pure function
// that does not perform SigV4 signature verification (deferred to a later release).
func ParseAWSRequest(r *http.Request) (*AWSRequest, *RequestContext, error) {
	if r == nil {
		return nil, nil, fmt.Errorf("request must not be nil")
	}

	// Build flat headers map.
	headers := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	// Build flat params map from query string and form values.
	params := make(map[string]string)
	if err := r.ParseForm(); err == nil {
		for k, vs := range r.Form {
			v := ""
			if len(vs) > 0 {
				v = vs[0]
			}
			if v == "" {
				v = "1" // bare key (e.g. ?uploads, ?versions)
			}
			params[k] = v
		}
	}

	host := r.Host
	if host == "" {
		host = r.Header.Get("Host")
	}

	target := r.Header.Get("X-Amz-Target")
	authHeader := r.Header.Get("Authorization")

	// S3 virtual-hosted-style URL normalisation.
	// mybucket.s3[.<region>].amazonaws.com → service="s3", path="/mybucket/..."
	effectivePath := r.URL.Path
	service := extractService(target, host, r.URL.Path)
	if _, normPath, ok := normalizeS3VirtualHost(host, r.URL.Path); ok {
		effectivePath = normPath
		service = "s3"
	}

	operation := extractOperation(target, params, r.Method)
	region := extractRegion(host, authHeader)
	account := extractAccount(authHeader)

	req := &AWSRequest{
		Service:   service,
		Operation: operation,
		Headers:   headers,
		Params:    params,
		Path:      effectivePath,
	}

	reqCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: account,
		Region:    region,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	return req, reqCtx, nil
}

// extractService determines the AWS service name from available signals, in
// priority order: X-Amz-Target header, Host header, URL path prefix.
func extractService(target, host, urlPath string) string {
	// 1. X-Amz-Target: "AmazonDynamoDB.GetItem" → "dynamodb"
	//    or "DynamoDB_20120810.GetItem" → "dynamodb"
	if target != "" {
		if svc := extractServiceFromTarget(target); svc != "" {
			return svc
		}
	}

	// 2. Host: "<service>.<region>.amazonaws.com" or "<service>.amazonaws.com"
	if host != "" {
		if svc := extractServiceFromHost(host); svc != "" {
			return svc
		}
	}

	// 3. URL path prefix: "/service/..." — emulator-local routing fallback.
	if svc := extractServiceFromPath(urlPath); svc != "" {
		return svc
	}

	return "unknown"
}

// targetServiceAliases maps derived lowercase service names to canonical AWS
// service names. It is consulted by both extractServiceFromTarget (for
// X-Amz-Target namespace names) and extractServiceFromHost (for subdomain
// names that do not match the emulator's canonical service name).
var targetServiceAliases = map[string]string{
	// "AmazonIdentityManagementService" → strip "Amazon" → "identitymanagementservice"
	"identitymanagementservice": "iam",
	// "AWSSecurityTokenService" → no strip → "awssecuritytokenservice"
	"awssecuritytokenservice": "sts",
	// "AWSCognitoIdentityService" → no strip → "awscognitoidentityservice"
	"awscognitoidentityservice": "cognito-identity",
	// "AWSCognitoIdentityProviderService" → no strip → "awscognitoidentityproviderservice"
	"awscognitoidentityproviderservice": "cognito-idp",
	// "ResourceGroupsTaggingAPI_20170126" → strip version → "resourcegroupstaggingapi"
	"resourcegroupstaggingapi": "tagging",
	// "TrentService" is the internal code-name for KMS.
	"trentservice": "kms",
	// "AmazonEventBridge" → strip "Amazon" → "eventbridge" (already correct), but
	// the host "events.*" produces "events" which must alias to "eventbridge".
	"events": "eventbridge",
	// "CertificateManager" is the ACM target namespace prefix.
	"certificatemanager": "acm",
	// "AmazonEC2ContainerServiceV20141113" → strip "Amazon" → "ec2containerservicev20141113"
	"ec2containerservicev20141113": "ecs",
	// "AmazonEC2ContainerRegistry_V1_1_0" → strip "Amazon" → strip after "_" → "ec2containerregistry"
	"ec2containerregistry": "ecr",
	// "elasticfilesystem" is the subdomain name for Amazon EFS.
	"elasticfilesystem": "efs",
	// "AWSGlue" → strip "AWS" → "glue".
	"awsglue": "glue",
	// "AWSInsightsIndexService" → strip "AWS" → "insightsindexservice" → "ce".
	"awsinsightsindexservice": "ce",
	// "AmazonBudgetServiceGateway" → strip "Amazon" → "budgetservicegateway" → "budgets".
	"budgetservicegateway": "budgets",
	// "AmazonHealthService" → strip "Amazon" → "healthservice" → "health".
	"healthservice": "health",
	// "email" is the subdomain name for Amazon SES v2.
	"email": "sesv2",
}

// extractServiceFromTarget parses an X-Amz-Target value such as
// "AmazonDynamoDB.GetItem" or "DynamoDB_20120810.GetItem" into a lowercase
// service name.
func extractServiceFromTarget(target string) string {
	// Split on "." to get the namespace part.
	dot := strings.IndexByte(target, '.')
	if dot < 0 {
		return ""
	}
	ns := target[:dot]

	// Strip known prefixes: "Amazon" prefix (AmazonDynamoDB → dynamodb).
	ns = strings.TrimPrefix(ns, "Amazon")

	// Strip version suffix: "DynamoDB_20120810" → "DynamoDB".
	if under := strings.IndexByte(ns, '_'); under > 0 {
		ns = ns[:under]
	}

	svc := strings.ToLower(ns)
	if canonical, ok := targetServiceAliases[svc]; ok {
		return canonical
	}
	return svc
}

// extractServiceFromHost parses a Host header value such as
// "dynamodb.us-east-1.amazonaws.com" or "iam.amazonaws.com".
func extractServiceFromHost(host string) string {
	// Strip port if present.
	if colon := strings.LastIndexByte(host, ':'); colon > 0 {
		host = host[:colon]
	}

	// Must end with ".amazonaws.com".
	if !strings.HasSuffix(host, ".amazonaws.com") {
		return ""
	}
	host = strings.TrimSuffix(host, ".amazonaws.com")

	// "<service>.<region>" or just "<service>".
	parts := strings.SplitN(host, ".", 2)
	svc := strings.ToLower(parts[0])
	if canonical, ok := targetServiceAliases[svc]; ok {
		return canonical
	}
	return svc
}

// extractServiceFromPath returns the first path segment when the URL path
// begins with "/service/<name>/".
func extractServiceFromPath(urlPath string) string {
	// Expected pattern: /service/<name>/...
	const prefix = "/service/"
	if !strings.HasPrefix(urlPath, prefix) {
		return ""
	}
	rest := urlPath[len(prefix):]
	if slash := strings.IndexByte(rest, '/'); slash > 0 {
		return strings.ToLower(rest[:slash])
	}
	return strings.ToLower(rest)
}

// extractOperation determines the API operation from available signals, in
// priority order: X-Amz-Target suffix, Action query/form parameter, HTTP method.
func extractOperation(target string, params map[string]string, method string) string {
	// 1. X-Amz-Target: "AmazonDynamoDB.GetItem" → "GetItem"
	if target != "" {
		if dot := strings.LastIndexByte(target, '.'); dot >= 0 && dot < len(target)-1 {
			return target[dot+1:]
		}
	}

	// 2. Action query/form parameter (EC2, SQS, SNS, …).
	if action := params["Action"]; action != "" {
		return action
	}

	// 3. Fallback: HTTP method.
	return method
}

// extractRegion determines the AWS region from available signals.
func extractRegion(host, authHeader string) string {
	// 1. Host: "<service>.<region>.amazonaws.com"
	if region := extractRegionFromHost(host); region != "" {
		return region
	}

	// 2. Authorization SigV4 credential scope.
	if region := extractRegionFromAuth(authHeader); region != "" {
		return region
	}

	// 3. Default region.
	return defaultRegion
}

// extractRegionFromHost parses the region segment from a Host header value
// such as "s3.us-west-2.amazonaws.com" or
// "mybucket.s3.us-east-1.amazonaws.com" (virtual-hosted S3).
func extractRegionFromHost(host string) string {
	// Strip port if present.
	if colon := strings.LastIndexByte(host, ':'); colon > 0 {
		host = host[:colon]
	}

	if !strings.HasSuffix(host, ".amazonaws.com") {
		return ""
	}
	trimmed := strings.TrimSuffix(host, ".amazonaws.com")

	parts := strings.Split(trimmed, ".")

	// For S3 virtual-hosted and path-style hosts the region (if present) is the
	// segment immediately after the literal "s3" token.
	for i, p := range parts {
		if p == "s3" {
			if i+1 < len(parts) {
				return parts[i+1]
			}
			return "" // s3 at the end → global, no region
		}
	}

	// Non-S3: "<service>.<region>" or just "<service>".
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

// normalizeS3VirtualHost detects an S3 virtual-hosted-style request
// (e.g., mybucket.s3[.<region>].amazonaws.com) and returns the bucket name,
// the normalised path (with bucket prepended), and ok=true when detected.
// For path-style S3 (s3[.<region>].amazonaws.com) and non-S3 hosts ok is false.
func normalizeS3VirtualHost(host, urlPath string) (bucket, normPath string, ok bool) {
	// Strip port if present.
	if colon := strings.LastIndexByte(host, ':'); colon > 0 {
		host = host[:colon]
	}

	if !strings.HasSuffix(host, ".amazonaws.com") {
		return "", "", false
	}
	trimmed := strings.TrimSuffix(host, ".amazonaws.com")

	// Find the "s3" segment.
	parts := strings.Split(trimmed, ".")
	s3Idx := -1
	for i, p := range parts {
		if p == "s3" {
			s3Idx = i
			break
		}
	}

	// s3Idx == 0 means path-style (s3.amazonaws.com or s3.<region>.amazonaws.com).
	// s3Idx < 0 means not an S3 host at all.
	if s3Idx <= 0 {
		return "", "", false
	}

	// Bucket is everything before the "s3" token.
	bucket = strings.Join(parts[:s3Idx], ".")
	// Normalised path: /<bucket><urlPath>.
	normPath = "/" + bucket + urlPath
	return bucket, normPath, true
}

// extractRegionFromAuth parses the credential scope embedded in a SigV4
// Authorization header, e.g.:
//
//	AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, …
func extractRegionFromAuth(authHeader string) string {
	const credPrefix = "Credential="
	idx := strings.Index(authHeader, credPrefix)
	if idx < 0 {
		return ""
	}
	cred := authHeader[idx+len(credPrefix):]
	// Trim trailing comma or space.
	if end := strings.IndexAny(cred, ", "); end > 0 {
		cred = cred[:end]
	}
	// cred is now: "<access-key>/<date>/<region>/<service>/aws4_request"
	parts := strings.Split(cred, "/")
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// extractAccount determines the AWS account ID from the Authorization header.
// Fake test access keys (starting with "AKIA") map to the well-known test
// account ID; everything else falls back to the zero account.
func extractAccount(authHeader string) string {
	const credPrefix = "Credential="
	idx := strings.Index(authHeader, credPrefix)
	if idx < 0 {
		return fallbackAccountID
	}
	cred := authHeader[idx+len(credPrefix):]
	if end := strings.IndexAny(cred, "/, "); end > 0 {
		accessKey := cred[:end]
		if strings.HasPrefix(accessKey, "AKIA") {
			return testAccountID
		}
	}
	return fallbackAccountID
}

// generateRequestID produces a unique request ID string.
func generateRequestID() string {
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}
