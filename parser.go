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
	// Bare keys (e.g. ?uploads, ?versions) have no "=" in the raw query and
	// are stored as "1" so callers can detect their presence with a map lookup.
	// Keys with an explicit empty value (e.g. ?prefix=) must be preserved as ""
	// to avoid corrupting parameters such as ListObjectsV2 Prefix.
	params := make(map[string]string)
	// Build a set of keys that appear with an explicit "=" in the raw query so
	// we can distinguish them from true bare keys.
	rawQuery := r.URL.RawQuery
	explicitEmpty := make(map[string]bool)
	for _, part := range strings.Split(rawQuery, "&") {
		if idx := strings.IndexByte(part, '='); idx >= 0 {
			key := part[:idx]
			if val := part[idx+1:]; val == "" {
				explicitEmpty[key] = true
			}
		}
	}
	if err := r.ParseForm(); err == nil {
		for k, vs := range r.Form {
			v := ""
			if len(vs) > 0 {
				v = vs[0]
			}
			if v == "" && !explicitEmpty[k] {
				v = "1" // bare key (e.g. ?uploads, ?versions)
			}
			params[k] = v
		}
	}

	host := r.Host
	if host == "" {
		host = r.Header.Get("Host")
	}
	// Ensure the Host is always present in the headers map so that plugins
	// such as APIGatewayProxyPlugin can read it without special-casing r.Host.
	if _, ok := headers["Host"]; !ok && host != "" {
		headers["Host"] = host
	}

	target := r.Header.Get("X-Amz-Target")
	authHeader := r.Header.Get("Authorization")
	// Presigned requests carry auth in query params instead of an Authorization
	// header.  Synthesise a minimal credential string so all header-based
	// extraction helpers (service, region, account) work transparently.
	if authHeader == "" {
		if credParam := params["X-Amz-Credential"]; credParam != "" {
			authHeader = "AWS4-HMAC-SHA256 Credential=" + credParam + ","
		}
	}

	// S3 virtual-hosted-style URL normalisation.
	// mybucket.s3[.<region>].amazonaws.com → service="s3", path="/mybucket/..."
	effectivePath := r.URL.Path
	service := extractService(target, host, r.URL.Path, authHeader)
	if _, normPath, ok := normalizeS3VirtualHost(host, r.URL.Path); ok {
		effectivePath = normPath
		service = "s3"
	} else if service == "s3" {
		// Custom-endpoint virtual-hosted style: <bucket>.<host>:<port>
		// AWS SDK v2 prepends the bucket name to the base-endpoint host even for
		// non-amazonaws.com endpoints (e.g. my-bucket.localhost:4566).
		// normalizeS3VirtualHost did not fire, but we already know service=="s3"
		// from the SigV4 credential scope, so normalise the path here.
		if _, normPath, ok := normalizeS3CustomEndpointVirtualHost(host, r.URL.Path); ok {
			effectivePath = normPath
		}
	}

	operation := extractOperation(target, params, r.Method, r.URL.Path)
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
// priority order: X-Amz-Target header, Host header, URL path prefix, SigV4
// credential scope.
func extractService(target, host, urlPath, authHeader string) string {
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

	// 4. SigV4 Authorization credential scope: the scope encodes the service
	// name explicitly (e.g. "…/us-east-1/sts/aws4_request"). This covers the
	// common pattern where callers use a single base-endpoint URL (e.g.
	// config.WithBaseEndpoint("http://localhost:4566")) so the Host is the
	// emulator address rather than the service-specific amazonaws.com host.
	if authHeader != "" {
		if svc := extractServiceFromAuth(authHeader); svc != "" {
			return svc
		}
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
	// "AWSBudgetServiceGateway" → strip "AWS" → "budgetservicegateway" → "budgets".
	"budgetservicegateway": "budgets",
	"awsbudgetservicegateway": "budgets",
	// "AmazonHealthService" → strip "Amazon" → "healthservice" → "health".
	"healthservice": "health",
	// "email" is the subdomain name for Amazon SES v2.
	"email": "sesv2",
	// "Kafka_20181101" → strip version → "Kafka" → lowercase → "kafka" → "msk".
	"kafka": "msk",
	// SigV4 service name for Amazon SES v2 is "ses".
	"ses": "sesv2",
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

	// AppSync execution endpoint: {apiId}.appsync-api.{region}
	if strings.Contains(host, ".appsync-api.") {
		return "appsync"
	}

	// API Gateway runtime endpoint: {apiId}.execute-api.{region}
	if strings.Contains(host, ".execute-api.") {
		return "execute-api"
	}

	// "<service>.<region>" or just "<service>".
	parts := strings.SplitN(host, ".", 2)
	svc := strings.ToLower(parts[0])
	if canonical, ok := targetServiceAliases[svc]; ok {
		return canonical
	}
	return svc
}

// smithyServiceAliases maps Smithy internal service IDs (lower-cased) found in
// Smithy RPC v2 CBOR URLs (/service/<ServiceId>/operation/<Op>) to Substrate's
// canonical service names.  Only services that use the rpc-v2-cbor protocol
// for at least one operation need entries here.
var smithyServiceAliases = map[string]string{
	// CloudWatch uses Smithy ID "GraniteServiceVersion20100801" for
	// operations that have migrated to the rpc-v2-cbor transport (e.g.
	// GetMetricData in cloudwatch SDK v1.55+).
	"graniteserviceversion20100801": "monitoring",
}

// extractServiceFromPath returns the first path segment when the URL path
// begins with "/service/<name>/".  The segment is looked up in
// smithyServiceAliases so that Smithy internal service IDs (e.g.
// "GraniteServiceVersion20100801") are resolved to canonical names.
func extractServiceFromPath(urlPath string) string {
	// Expected pattern: /service/<name>/...
	const prefix = "/service/"
	if !strings.HasPrefix(urlPath, prefix) {
		return ""
	}
	rest := urlPath[len(prefix):]
	var raw string
	if slash := strings.IndexByte(rest, '/'); slash > 0 {
		raw = strings.ToLower(rest[:slash])
	} else {
		raw = strings.ToLower(rest)
	}
	if canonical, ok := smithyServiceAliases[raw]; ok {
		return canonical
	}
	return raw
}

// extractOperationFromPath extracts the operation name from a Smithy RPC v2
// URL of the form /service/<ServiceId>/operation/<OperationName>.
func extractOperationFromPath(urlPath string) string {
	const opToken = "/operation/"
	idx := strings.Index(urlPath, opToken)
	if idx < 0 {
		return ""
	}
	op := urlPath[idx+len(opToken):]
	if slash := strings.IndexByte(op, '/'); slash >= 0 {
		op = op[:slash]
	}
	return op
}

// extractOperation determines the API operation from available signals, in
// priority order: X-Amz-Target suffix, Action query/form parameter, Smithy
// RPC v2 URL path, HTTP method.
func extractOperation(target string, params map[string]string, method, urlPath string) string {
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

	// 3. Smithy RPC v2 URL: /service/<ServiceId>/operation/<OperationName>
	if op := extractOperationFromPath(urlPath); op != "" {
		return op
	}

	// 4. Fallback: HTTP method.
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

	// execute-api runtime: "{apiId}.execute-api.{region}".
	if len(parts) >= 3 && parts[1] == "execute-api" {
		return parts[2]
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

// normalizeS3CustomEndpointVirtualHost detects virtual-hosted-style S3 requests
// sent to a non-amazonaws.com base endpoint (e.g. my-bucket.localhost:4566).
// AWS SDK v2 always prepends the bucket name to the configured base-endpoint
// host, so when callers use config.WithBaseEndpoint the Host header looks like
// "<bucket>.<emulator-host>:<port>" rather than the usual
// "<bucket>.s3.<region>.amazonaws.com".
//
// This function must only be called after the service has already been
// identified as "s3" via another signal (SigV4 credential scope); it should
// not be used as a standalone service detector.
func normalizeS3CustomEndpointVirtualHost(host, urlPath string) (bucket, normPath string, ok bool) {
	// Strip port.
	if colon := strings.LastIndexByte(host, ':'); colon > 0 {
		host = host[:colon]
	}

	// Standard amazonaws.com virtual-hosted paths are handled by
	// normalizeS3VirtualHost; skip them here.
	if strings.HasSuffix(host, ".amazonaws.com") {
		return "", "", false
	}

	// The bucket is the first DNS label when the host has at least one dot.
	// A bare hostname (e.g. "localhost") has no dot and is path-style.
	dot := strings.IndexByte(host, '.')
	if dot <= 0 {
		return "", "", false
	}

	bucket = host[:dot]
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

// extractServiceFromAuth parses the AWS service name from the SigV4
// Authorization header credential scope, e.g.:
//
//	AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20130524/us-east-1/sts/aws4_request, …
//
// This is the authoritative fallback for query-protocol requests that arrive at
// a single base endpoint (e.g. localhost:4566) where the Host header does not
// identify the service.
func extractServiceFromAuth(authHeader string) string {
	const credPrefix = "Credential="
	idx := strings.Index(authHeader, credPrefix)
	if idx < 0 {
		return ""
	}
	cred := authHeader[idx+len(credPrefix):]
	if end := strings.IndexAny(cred, ", "); end > 0 {
		cred = cred[:end]
	}
	// cred is now: "<access-key>/<date>/<region>/<service>/aws4_request"
	parts := strings.Split(cred, "/")
	if len(parts) < 4 {
		return ""
	}
	svc := strings.ToLower(parts[3])
	if canonical, ok := targetServiceAliases[svc]; ok {
		return canonical
	}
	return svc
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
