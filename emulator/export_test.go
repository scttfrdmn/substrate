package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/afero"
)

// This file exports internal symbols for use in external test packages.
// It is compiled only when running tests.

// AwaitTestServerForTest wraps awaitTestServer, the health probe [StartTestServer] waits on.
//
// Exported so the give-up path can be tested at all: it is unreachable through
// StartTestServer, whose server is in-process and bound before the probe runs, and the
// point of #798 is that this probe reports why it gave up rather than discarding the
// answer.
func AwaitTestServerForTest(baseURL string, deadline time.Duration) error {
	return awaitTestServer(baseURL, deadline)
}

// GenerateIAMIDForTest wraps generateIAMID for external tests.
func GenerateIAMIDForTest(prefix string) string { return generateIAMID(prefix) }

// IAMUserARNForTest wraps iamUserARN for external tests.
func IAMUserARNForTest(accountID, path, name string) string { return iamUserARN(accountID, path, name) }

// IAMRoleARNForTest wraps iamRoleARN for external tests.
func IAMRoleARNForTest(accountID, path, name string) string { return iamRoleARN(accountID, path, name) }

// IAMGroupARNForTest wraps iamGroupARN for external tests.
func IAMGroupARNForTest(accountID, path, name string) string {
	return iamGroupARN(accountID, path, name)
}

// IAMPolicyARNForTest wraps iamPolicyARN for external tests.
func IAMPolicyARNForTest(accountID, path, name string) string {
	return iamPolicyARN(accountID, path, name)
}

// NormalizeS3VirtualHostForTest wraps normalizeS3VirtualHost for external tests.
func NormalizeS3VirtualHostForTest(host, urlPath string) (bucket, normPath string, ok bool) {
	return normalizeS3VirtualHost(host, urlPath)
}

// S3PersistedContentEncodingForTest wraps s3PersistedContentEncoding for external
// tests. Header casing cannot be varied through the HTTP test helpers — net/http
// canonicalizes on the way in — so the case-insensitive resolution is only
// observable from here.
func S3PersistedContentEncodingForTest(headers map[string]string) string {
	return s3PersistedContentEncoding(headers)
}

// ExtractRegionFromHostForTest wraps extractRegionFromHost for external tests.
func ExtractRegionFromHostForTest(host string) string { return extractRegionFromHost(host) }

// ResolveOperationNameForTest wraps resolveOperationName for external tests.
func ResolveOperationNameForTest(req *AWSRequest) { resolveOperationName(req) }

// ExtractAccessKeyFromAuthForTest wraps extractAccessKeyFromAuth for external tests.
func ExtractAccessKeyFromAuthForTest(authHeader string) string {
	return extractAccessKeyFromAuth(authHeader)
}

// BuildCallerARNForTest wraps buildCallerARN for external tests.
func BuildCallerARNForTest(accountID, accessKeyID string) string {
	return buildCallerARN(accountID, accessKeyID)
}

// ResolvePrincipalForTest wraps resolvePrincipal for external tests.
//
// The HTTP path can only reach the records IAM and STS themselves write, so the
// arms that matter most are unreachable from it: a nil state manager, a stored
// record that fails to unmarshal, and a session record carrying no PrincipalArn.
// Each must resolve to no principal rather than to a partly-built one — an ARN
// assembled from a corrupt record would name some *other* entity's policies.
func ResolvePrincipalForTest(state StateManager, accountID, accessKeyID string) (*Principal, string) {
	return resolvePrincipal(context.Background(), state, accountID, accessKeyID)
}

// VerifySigV4ForTest wraps VerifySigV4 for external tests.
func VerifySigV4ForTest(r *http.Request, body []byte, reg *CredentialRegistry) error {
	return VerifySigV4(r, body, reg)
}

// IAMAuthorizeForTest exercises the unexported IAMPlugin.authorize method so
// coverage tools can reach the inline-policy and boundary loading helpers.
//
// It takes the resource as an ARN, which is what a caller wanting to pin one string wants.
// The door itself takes an authzResource, so that it can publish the resource's tags as
// aws:ResourceTag/<key> (#804); a test about those drives a real request through the server,
// where the resolver supplies the tags.
func IAMAuthorizeForTest(p *IAMPlugin, ctx *RequestContext, action, resource string) error {
	return p.authorize(context.Background(), ctx, action, authzResource{ARN: resource})
}

// IAMAuthorizeWithForTest exercises IAMPlugin.authorizeWith, the door that publishes a
// request-specific condition key alongside the caller's own (#747).
func IAMAuthorizeWithForTest(p *IAMPlugin, ctx *RequestContext, action, resource string,
	extra map[string]string) error {
	return p.authorizeWith(context.Background(), ctx, action, authzResource{ARN: resource}, extra)
}

// IAMSLRRoleNameForTest wraps iamSLRRoleName for external tests.
func IAMSLRRoleNameForTest(serviceName, customSuffix string) string {
	return iamSLRRoleName(serviceName, customSuffix)
}

// IAMSLRDerivedRoleNameForTest wraps iamSLRDerivedRoleName for external tests, which is
// what asserts that substrate's convention for an untabled principal stays stable.
func IAMSLRDerivedRoleNameForTest(serviceName string) string {
	return iamSLRDerivedRoleName(serviceName)
}

// IAMSLRPathForTest wraps iamSLRPath for external tests.
func IAMSLRPathForTest(serviceName string) string { return iamSLRPath(serviceName) }

// IAMSLRServiceFromRoleNameForTest wraps iamSLRServiceFromRoleName for external tests.
func IAMSLRServiceFromRoleNameForTest(roleName string) string {
	return iamSLRServiceFromRoleName(roleName)
}

// IAMSLRRoleFromDeletionTaskIDForTest wraps iamSLRRoleFromDeletionTaskID for external
// tests.
func IAMSLRRoleFromDeletionTaskIDForTest(taskID string) (string, string, bool) {
	return iamSLRRoleFromDeletionTaskID(taskID)
}

// IAMAuthzRequestContextForTest wraps iamAuthzRequestContext for external tests, which
// is what pins which operations publish iam:AWSServiceName.
func IAMAuthzRequestContextForTest(req *AWSRequest) map[string]string {
	ctx := make(map[string]string, 1)
	iamAuthzRequestContext(ctx, req)
	return ctx
}

// IAMSLRResourceARNForTest wraps iamAuthzSLRResourceARN for external tests.
func IAMSLRResourceARNForTest(state StateManager, ctx *RequestContext, req *AWSRequest) string {
	return iamAuthzSLRResourceARN(state, ctx, req)
}

// RecordEventAtTimeForTest records a pre-built Event into store, allowing
// tests to inject events with arbitrary Timestamp values for time-series
// coverage of forecast helpers.
func RecordEventAtTimeForTest(store *EventStore, ev *Event) error {
	return store.RecordEvent(context.Background(), ev)
}

// LinearRegressionForTest wraps the unexported linearRegression for direct
// unit testing.
func LinearRegressionForTest(xs, ys []float64) (slope, intercept float64) {
	return linearRegression(xs, ys)
}

// MeanFloatForTest wraps the unexported meanFloat for direct unit testing.
func MeanFloatForTest(vals []float64) float64 { return meanFloat(vals) }

// StddevFloatForTest wraps the unexported stddevFloat for direct unit testing.
func StddevFloatForTest(vals []float64, mean float64) float64 { return stddevFloat(vals, mean) }

// RuntimeToImage wraps runtimeToImage for external tests.
func RuntimeToImage(runtime string) string { return runtimeToImage(runtime) }

// FindFreePort wraps findFreePort for external tests.
func FindFreePort() (int, error) { return findFreePort() }

// NewLambdaExecutorForTest creates a LambdaExecutor with a forced Docker
// availability value, allowing tests to run without a Docker daemon.
func NewLambdaExecutorForTest(cfg LambdaExecCfg, logger Logger, dockerAvail bool) *LambdaExecutor {
	e := NewLambdaExecutor(cfg, logger)
	// Override the availability probe result.
	e.availOnce.Do(func() { e.available = dockerAvail })
	return e
}

// NewLambdaPluginForTest constructs a bare LambdaPlugin wired to the given
// state manager. It is used to test replay-cache helpers without a full server.
func NewLambdaPluginForTest(state StateManager, tc *TimeController) *LambdaPlugin {
	return &LambdaPlugin{
		state:   state,
		logger:  NewDefaultLogger(-4, false),
		tc:      tc,
		esmStop: make(map[string]chan struct{}),
	}
}

// SaveReplayForTest exposes saveReplay for external tests.
func (p *LambdaPlugin) SaveReplayForTest(functionARN string, payload, response []byte) {
	p.saveReplay(functionARN, payload, response)
}

// LoadReplayForTest exposes loadReplay for external tests.
func (p *LambdaPlugin) LoadReplayForTest(functionARN string, payload []byte) ([]byte, bool) {
	return p.loadReplay(functionARN, payload)
}

// BuildV1ProxyEventForTest wraps buildV1ProxyEvent for external tests.
func BuildV1ProxyEventForTest(req *AWSRequest, apiID, stage, resourcePath string) ([]byte, error) {
	return buildV1ProxyEvent(req, apiID, stage, resourcePath)
}

// BuildV2ProxyEventForTest wraps buildV2ProxyEvent for external tests.
func BuildV2ProxyEventForTest(req *AWSRequest, apiID, stage, resourcePath string) ([]byte, error) {
	return buildV2ProxyEvent(req, apiID, stage, resourcePath)
}

// ParseProxyResponseForTest wraps parseProxyResponse for external tests.
func ParseProxyResponseForTest(body []byte) (*AWSResponse, error) {
	return parseProxyResponse(body)
}

// ExtractLambdaARNFromURIForTest wraps extractLambdaARNFromURI for external tests.
func ExtractLambdaARNFromURIForTest(uri string) string {
	return extractLambdaARNFromURI(uri)
}

// NewRDSExecutorForTest wraps NewRDSExecutor for external tests.
func NewRDSExecutorForTest(logger Logger) *RDSExecutor {
	return NewRDSExecutor(logger)
}

// InjectRDSHandleForTest inserts a fake RDSContainerHandle into the executor's
// active map to exercise code paths that iterate over active containers.
func InjectRDSHandleForTest(e *RDSExecutor, instanceID, containerID string) {
	e.mu.Lock()
	e.active[instanceID] = &RDSContainerHandle{ContainerID: containerID, HostPort: 5432}
	e.mu.Unlock()
}

// RDSStopContainerForTest calls StopContainer for external tests.
func RDSStopContainerForTest(e *RDSExecutor, ctx context.Context, containerID string) error {
	return e.StopContainer(ctx, containerID)
}

// EvictStaleForTest exposes LambdaExecutor.evictStale for external tests.
func (e *LambdaExecutor) EvictStaleForTest() { e.evictStale() }

// InjectPoolEntryForTest inserts a fake containerHandle into the executor's
// warm pool to exercise code paths that iterate over the pool (e.g., StopAll).
func InjectPoolEntryForTest(e *LambdaExecutor, arn, containerID string) {
	e.mu.Lock()
	e.pool[arn] = &containerHandle{containerID: containerID}
	e.mu.Unlock()
}

// IsDockerAvailableForTest exposes LambdaExecutor.isDockerAvailable for tests.
func (e *LambdaExecutor) IsDockerAvailableForTest() bool { return e.isDockerAvailable() }

// ShutdownLambdaPluginForTest calls LambdaPlugin.Shutdown for coverage.
func ShutdownLambdaPluginForTest(p *LambdaPlugin, ctx context.Context) error {
	return p.Shutdown(ctx)
}

// LambdaPluginSetExecutorForTest injects a LambdaExecutor into the plugin for
// tests that need to exercise Docker execution or replay paths.
func LambdaPluginSetExecutorForTest(p *LambdaPlugin, exec *LambdaExecutor) {
	p.executor = exec
}

// LambdaPluginCreateFunctionForTest writes a minimal LambdaFunction to state
// so that invoke can load it without a real HTTP CreateFunction request.
//
// The account and Region the record is keyed under come from the function's own ARN, which is
// the pair a caller's invoke has to present to load it back (#943), so a caller supplying a
// consistent ARN needs no second parameter to say the same thing twice.
func LambdaPluginCreateFunctionForTest(p *LambdaPlugin, fn LambdaFunction) {
	acct, region := lambdaARNScope(fn.FunctionArn)
	_, _ = p.saveFunctionAndRespond(acct, region, fn, 200)
}

// InvokeLambdaForTest calls the unexported invoke method directly.
func InvokeLambdaForTest(p *LambdaPlugin, ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	return p.invoke(ctx, req, name)
}

// InvokePOSTForTest calls invokePOST against a container handle pointing at the
// given localhost port, letting a test stand in a fake Lambda runtime interface
// instead of a real Docker container.
func InvokePOSTForTest(e *LambdaExecutor, port int, payload []byte) (body []byte, functionError string, err error) {
	return e.invokePOST(context.Background(), &containerHandle{port: port}, payload)
}

// SQSQueueNameFromURLForTest wraps sqsQueueNameFromURL for external tests.
func SQSQueueNameFromURLForTest(queueURL string) string { return sqsQueueNameFromURL(queueURL) }

// SQSSeedForTest writes an SQS consistency seed directly into state, bypassing
// the control-plane handler. It lets a test arrange a seed behind a state manager
// whose Put or Delete is rigged to fail.
func SQSSeedForTest(state StateManager, queueName string, getURLMisses, getAttributesMisses int) error {
	data, err := json.Marshal(sqsConsistencySeed{
		QueueName:           queueName,
		GetURLMisses:        getURLMisses,
		GetAttributesMisses: getAttributesMisses,
	})
	if err != nil {
		return fmt.Errorf("marshal sqs seed: %w", err)
	}
	if err := state.Put(context.Background(), sqsCtrlNamespace, sqsCtrlKey(queueName), data); err != nil {
		return fmt.Errorf("put sqs seed: %w", err)
	}
	return nil
}

// SQSPutRawSeedForTest stores raw bytes at a consistency seed's key, so a test
// can exercise the unmarshal-failure path with a corrupt stored value.
func SQSPutRawSeedForTest(state StateManager, queueName string, raw []byte) error {
	if err := state.Put(context.Background(), sqsCtrlNamespace, sqsCtrlKey(queueName), raw); err != nil {
		return fmt.Errorf("put raw sqs seed: %w", err)
	}
	return nil
}

// CheckPresignedExpiryForTest exposes checkPresignedExpiry for white-box tests.
func CheckPresignedExpiryForTest(q url.Values, now time.Time) bool {
	return checkPresignedExpiry(q, now)
}

// Error-protocol names exposed so external tests can assert the classification
// without depending on the unexported enum's numeric values.
const (
	ErrProtoQueryXMLForTest  = "query-xml"
	ErrProtoJSONRPCForTest   = "json-rpc"
	ErrProtoRESTJSONForTest  = "rest-json"
	ErrProtoS3XMLForTest     = "s3-xml"
	ErrProtoEC2XMLForTest    = "ec2-xml"
	ErrProtoRPCV2CBORForTest = "rpc-v2-cbor"
	ErrProtoUnknownForTest   = "unknown"
)

// errProtoNameForTest is the shared translation from the internal constant to the
// ErrProto*ForTest names, so the two wrappers below cannot drift apart.
func errProtoNameForTest(proto awsErrorProtocol) string {
	switch proto {
	case errProtoQueryXML:
		return ErrProtoQueryXMLForTest
	case errProtoJSONRPC:
		return ErrProtoJSONRPCForTest
	case errProtoRESTJSON:
		return ErrProtoRESTJSONForTest
	case errProtoS3XML:
		return ErrProtoS3XMLForTest
	case errProtoEC2XML:
		return ErrProtoEC2XMLForTest
	case errProtoRPCV2CBOR:
		return ErrProtoRPCV2CBORForTest
	default:
		return ErrProtoUnknownForTest
	}
}

// ErrorProtocolForTest wraps errorProtocolFor, returning one of the
// ErrProto*ForTest names.
func ErrorProtocolForTest(service, contentType string) string {
	return errProtoNameForTest(errorProtocolFor(service, contentType))
}

// ErrorProtocolForRequestForTest wraps errorProtocolForRequest, returning one of the
// ErrProto*ForTest names. This is the classification the emulator actually uses, so a
// test asserting that a multi-protocol service follows its caller — and that a
// single-protocol one does not — has to go through this rather than through
// ErrorProtocolForTest.
func ErrorProtocolForRequestForTest(service string, r *http.Request) string {
	return errProtoNameForTest(errorProtocolForRequest(service, r))
}

// MultiProtocolServicesForTest returns every service that lets the request decide its
// error protocol, so a test can assert the set is the intended one rather than sampling
// it. Keeping the set small is the property under test: #392's fix depends on a
// single-protocol service never being reclassified by its Content-Type.
func MultiProtocolServicesForTest() []string {
	names := make([]string, 0, len(multiProtocolServices))
	for svc := range multiProtocolServices {
		names = append(names, svc)
	}
	sort.Strings(names)
	return names
}

// ErrorShapeForTest wraps errorShapeFor, returning the shape ID a "__type" member would
// carry, the message member's spelling, whether the model calls the fault a server one,
// and the shape's modeled HTTP status.
func ErrorShapeForTest(service, code string, status int) (shapeID, messageMember string, serverFault bool, httpStatus int) {
	shape := errorShapeFor(service, code, status)
	return shape.ShapeID(), shape.MessageMember, shape.ServerFault, shape.HTTPStatus
}

// CloudWatchErrorCodesForTest returns every Query code CloudWatch's error table
// translates, so a test can assert a property over the whole table instead of a sample.
func CloudWatchErrorCodesForTest() []string {
	codes := make([]string, 0, len(cloudWatchErrorShapes))
	for code := range cloudWatchErrorShapes {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// DetectWireProtocolForTest wraps detectWireProtocol so a test can pin the
// classification directly, which is where the protocol's own signals are asserted.
func DetectWireProtocolForTest(r *http.Request) WireProtocol { return detectWireProtocol(r) }

// DetectQueryModeForTest wraps detectQueryMode.
func DetectQueryModeForTest(r *http.Request) bool { return detectQueryMode(r) }

// RPCV2CBORTargetConflictForTest wraps rpcV2CBORTargetConflict, the malformed-request
// check the specification requires a server to make.
func RPCV2CBORTargetConflictForTest(r *http.Request) bool { return rpcV2CBORTargetConflict(r) }

// AccessDeniedCodeForTest wraps accessDeniedCodeFor so an external test can pin
// the protocol-to-code mapping directly, rather than only through the two
// AuthController denial arms that consume it.
func AccessDeniedCodeForTest(service, contentType string) string {
	return accessDeniedCodeFor(service, contentType)
}

// RegisteredErrorProtocolServicesForTest returns every service name in
// serviceErrorProtocols, so a test can assert a property over the whole table
// rather than over a sample that goes stale as plugins are added.
func RegisteredErrorProtocolServicesForTest() []string {
	names := make([]string, 0, len(serviceErrorProtocols))
	for svc := range serviceErrorProtocols {
		names = append(names, svc)
	}
	sort.Strings(names)
	return names
}

// IAMAccessDeniedCodeForTest is the code the IAM plugin's own gate reports, so a
// test can assert it agrees with what accessDeniedCodeFor derives for "iam"
// instead of restating the literal.
const IAMAccessDeniedCodeForTest = iamAccessDeniedCode

// PricingAccessDeniedCodeForTest is the code the Price List plugin's own gate
// reports, exposed for the same reason IAMAccessDeniedCodeForTest is: so a test
// asserts the plugin and the generic gate agree rather than restating either
// literal.
const PricingAccessDeniedCodeForTest = pricingErrAccessDenied

// MarshalAWSErrorForTest wraps marshalAWSError, selecting the protocol by one of
// the ErrProto*ForTest names. status is the HTTP status the error carries, which
// the S3 arm needs because it builds a whole response rather than a body alone.
//
// It names no service and no query mode, which is the right context for the five arms
// that do not consult either. Use MarshalAWSErrorWireForTest for the two that do.
func MarshalAWSErrorForTest(code, message, proto, jsonContentType string, status int) (body []byte, contentType string, headers map[string]string) {
	return MarshalAWSErrorWireForTest(code, message, proto, jsonContentType, "", false, status)
}

// MarshalAWSErrorWireForTest wraps marshalAWSError with the full serializer context, so
// a test can pin the shape-ID translation and the query-compatibility header — the two
// things that depend on which service raised the error and on what the caller asked for.
func MarshalAWSErrorWireForTest(code, message, proto, jsonContentType, service string, queryMode bool, status int) (body []byte, contentType string, headers map[string]string) {
	p := errProtoQueryXML
	switch proto {
	case ErrProtoJSONRPCForTest:
		p = errProtoJSONRPC
	case ErrProtoRESTJSONForTest:
		p = errProtoRESTJSON
	case ErrProtoS3XMLForTest:
		p = errProtoS3XML
	case ErrProtoEC2XMLForTest:
		p = errProtoEC2XML
	case ErrProtoRPCV2CBORForTest:
		p = errProtoRPCV2CBOR
	}
	return marshalAWSError(&AWSError{Code: code, Message: message, HTTPStatus: status}, errorWireContext{
		Protocol:        p,
		JSONContentType: jsonContentType,
		Service:         service,
		QueryMode:       queryMode,
	})
}

// S3ErrorResponseForTest wraps s3ErrorResponseWith so a test can compare an
// error the S3 plugin builds against one the pipeline builds, byte for byte.
func S3ErrorResponseForTest(code, message string, status int) []byte {
	return s3ErrorResponseWith(s3Error{Code: code, Message: message, Status: status}).Body
}

// CFNDispatchErrorForTest wraps cfnDispatchError so its precedence and its
// degraded reason shapes can be asserted directly.
//
// Going through the function rather than a deployed stack is deliberate: every
// response-style plugin a stack can reach (S3, IAM) writes both a <Code> and a
// <Message>, so the body-less and message-less arms are unreachable from any
// template. They still have to be right — a 4xx that produced an empty reason
// would be recorded as CREATE_COMPLETE, which is the whole defect — so they are
// pinned here at the unit the plan extracted them into.
func CFNDispatchErrorForTest(status int, body string, routeErr error) error {
	var resp *AWSResponse
	if status != 0 {
		resp = &AWSResponse{StatusCode: status, Body: []byte(body)}
	}
	return cfnDispatchError(resp, routeErr)
}

// CFNMapDeployerErrorForTest wraps cfnMapDeployerError so the classification it
// derives can be asserted against an error built by hand, rather than only
// against whichever deployer path a test happens to be able to provoke.
func CFNMapDeployerErrorForTest(err error) *AWSError { return cfnMapDeployerError(err) }

// CFNClassifiedErrorForTest builds a classified deployer error carrying class and
// an arbitrary message.
//
// The message being arbitrary is the point: it is what proves the wire code no
// longer depends on the wording. A test can hand this a message that says nothing
// a substring match would have recognized and assert the code still resolves,
// which is the regression #502 removed.
func CFNClassifiedErrorForTest(class error, message string) error {
	return cfnErrf(class, "%s", message)
}

// ResolveValueListForTest wraps resolveValueList so its conventions can be
// asserted at the seam rather than only through a deployer.
//
// The distinctions it draws are invisible downstream: resolveStringList drops
// empty members because a query API numbers its list parameters, so "AWS::NoValue
// contributes no element" and "an empty member is preserved" — two rules
// CloudFormation states and that a nested Fn::Split depends on — produce the same
// observable through every current call site. Pinning them here is what makes
// them regressions rather than accidents.
//
// listParams names the parameters declared with a list type, which is the only
// thing that makes a Ref list-valued.
func ResolveValueListForTest(v interface{}, params map[string]string, listParams map[string]bool, conditions map[string]bool) []string {
	cctx := &cfnContext{
		params:     params,
		listParams: listParams,
		conditions: conditions,
		resources:  map[string]DeployedResource{},
		evaluating: map[string]bool{},
	}
	return resolveValueList(v, cctx)
}

// CFNListParameterTypeForTest wraps cfnListParameterType so the set of declared
// types that make a Ref list-valued can be asserted directly.
func CFNListParameterTypeForTest(t string) bool { return cfnListParameterType(t) }

// ResolveNestedForTest wraps resolveNested so its four rules can be asserted at
// the seam rather than only through a deployer.
//
// Most of them are invisible downstream: a plugin that stores an untyped property
// cannot report that a key was rewritten, and a multi-key map resolving to
// whichever key Go's map iteration reached first is a race a single deploy passes
// by luck.
func ResolveNestedForTest(v interface{}, params map[string]string, listParams map[string]bool, conditions map[string]bool) interface{} {
	cctx := &cfnContext{
		params:     params,
		listParams: listParams,
		conditions: conditions,
		resources:  map[string]DeployedResource{},
		evaluating: map[string]bool{},
		region:     "us-east-1",
		accountID:  "123456789012",
	}
	return resolveNested(v, cctx)
}

// ECSRewriteContainerKeysForTest wraps the ECS container-definition key rewrite,
// so the members it must refuse to descend into can be asserted without a deploy.
func ECSRewriteContainerKeysForTest(v interface{}) interface{} {
	return ecsRewriteKeys(v, ecsContainerDefinitionKeys)
}

// ECSContainerDefinitionKeysForTest returns the ECS container-definition key
// mapping, so a typo in a hand-written table is caught by asserting the whole
// table against the rule its entries follow.
func ECSContainerDefinitionKeysForTest() map[string]string { return ecsContainerDefinitionKeys }

// CFNCidrBlocksForTest wraps cfnCidrBlocks so the documented examples — and the
// requests it must refuse rather than answer with a short list — can be asserted
// without a template.
func CFNCidrBlocksForTest(ipBlock string, count, cidrBits int) ([]string, error) {
	return cfnCidrBlocks(ipBlock, count, cidrBits)
}

// CFNPartitionForTest wraps cfnPartition.
func CFNPartitionForTest(region string) string { return cfnPartition(region) }

// CFNURLSuffixForTest wraps cfnURLSuffix.
func CFNURLSuffixForTest(region string) string { return cfnURLSuffix(region) }

// CFNResolveWithMappingsForTest resolves v against a template's Mappings section,
// returning the resolved value and any failure the resolution recorded.
//
// The failures are the point: a resolver returns a string, so "no such mapping
// key" and "the key held an empty string" are the same observable downstream, and
// the whole of #522's silent-literal defect lives in that gap. Reading them here
// pins which lookups fail and which fall back to a DefaultValue.
func CFNResolveWithMappingsForTest(
	v interface{},
	mappings map[string]map[string]map[string]interface{},
	params map[string]string,
	region string,
) (string, []string) {
	cctx := &cfnContext{
		params:     params,
		listParams: map[string]bool{},
		conditions: map[string]bool{},
		resources:  map[string]DeployedResource{},
		evaluating: map[string]bool{},
		mappings:   mappings,
		region:     region,
		accountID:  defaultAccountID,
		stackName:  "teststack",
	}
	return resolveValue(v, cctx), cctx.takeFailures()
}

// CFNResolveListWithMappingsForTest is CFNResolveWithMappingsForTest for a
// list-valued context, which is where Fn::GetAZs, Fn::Cidr and a list-valued
// mapping leaf keep their elements rather than being rejoined.
func CFNResolveListWithMappingsForTest(
	v interface{},
	mappings map[string]map[string]map[string]interface{},
	params map[string]string,
	region string,
) ([]string, []string) {
	cctx := &cfnContext{
		params:     params,
		listParams: map[string]bool{},
		conditions: map[string]bool{},
		resources:  map[string]DeployedResource{},
		evaluating: map[string]bool{},
		mappings:   mappings,
		region:     region,
		accountID:  defaultAccountID,
		stackName:  "teststack",
	}
	return resolveValueList(v, cctx), cctx.takeFailures()
}

// CFNResolveNestedWithMappingsForTest resolves v as a *structured property* rather
// than as a whole value, which is the only context where an intrinsic's resolved
// **shape** is observable.
//
// A scalar context rejoins a list, so "resolved to a list" and "resolved to a
// comma-joined string" are the same observable there. Inside a structured property
// they are not: a member holding a list and a member holding one string are
// different JSON, and different again from a member holding "". That distinction is
// what decides whether a list-valued mapping leaf or AWS::NotificationARNs reaches
// a plugin as an array.
func CFNResolveNestedWithMappingsForTest(
	v interface{},
	mappings map[string]map[string]map[string]interface{},
	params map[string]string,
	region string,
) (interface{}, []string) {
	cctx := &cfnContext{
		params:     params,
		listParams: map[string]bool{},
		conditions: map[string]bool{},
		resources:  map[string]DeployedResource{},
		evaluating: map[string]bool{},
		mappings:   mappings,
		region:     region,
		accountID:  defaultAccountID,
		stackName:  "teststack",
	}
	return resolveNested(v, cctx), cctx.takeFailures()
}

// CFNResolveImportForTest resolves v against a set of exports, returning the
// resolved value, the export names the resolution recorded as imports, and any
// failure.
//
// The imports are the half that has no other observable: whether an
// Fn::ImportValue counted as an import is what decides if the exporting stack can
// be deleted, and it is decided at resolution time — an import inside a false
// Fn::If branch never happened. Reading it here pins that without having to deploy
// two stacks and then attempt a delete.
func CFNResolveImportForTest(
	v interface{},
	exports map[string]string,
	params map[string]string,
) (value string, imports []string, failures []string) {
	cctx := &cfnContext{
		params:     params,
		listParams: map[string]bool{},
		conditions: map[string]bool{},
		resources:  map[string]DeployedResource{},
		evaluating: map[string]bool{},
		imports:    map[string]bool{},
		exports:    exports,
		region:     defaultRegion,
		accountID:  defaultAccountID,
		stackName:  "teststack",
	}
	value = resolveValue(v, cctx)
	return value, cctx.importedNames(), cctx.takeFailures()
}

// CFNStackExportsForTest returns a persisted stack's export name → value map, the
// join of ExportNames against Outputs that the registry reads.
func CFNStackExportsForTest(s CFNStackState) map[string]string { return s.exports() }

// CWLogGroupPolicyARNForTest wraps cwLogGroupPolicyARN for external tests.
//
// Exported because two of its cases are not reachable over HTTP: a created group
// always has substrate's own unsuffixed ARN, so the empty-ARN input (a record a
// snapshot restored before the field existed) and the already-suffixed input
// (idempotence over its own output) can only be exercised directly.
func CWLogGroupPolicyARNForTest(arn string) string { return cwLogGroupPolicyARN(arn) }

// CWPutLogGroupStateForTest writes a log group record into state under the key
// layout the plugin reads, without going through CreateLogGroup.
//
// This is how a test reaches a stored group whose fields the API path would never
// produce — an ARN-less record from an older snapshot, say — so the wire
// projection can be asserted over it.
func CWPutLogGroupStateForTest(ctx context.Context, state StateManager, accountID, region string, lg CWLogGroup) error {
	data, err := json.Marshal(lg)
	if err != nil {
		return fmt.Errorf("CWPutLogGroupStateForTest marshal: %w", err)
	}
	key := cwLogGroupKey(accountID, region, lg.LogGroupName)
	if putErr := state.Put(ctx, cloudwatchLogsNamespace, key, data); putErr != nil {
		return fmt.Errorf("CWPutLogGroupStateForTest state.Put: %w", putErr)
	}
	updateStringIndex(ctx, state, cloudwatchLogsNamespace, cwLogGroupNamesKey(accountID, region), lg.LogGroupName)
	return nil
}

// CFNSeededAZsForTest returns the Availability Zone names substrate reports for a
// region, derived from the same list EC2's DescribeAvailabilityZones uses.
//
// Exported so a test can assert Fn::GetAZs against EC2's own answer without
// hard-coding a zone list in a second place — which is the disagreement the
// resolver exists to avoid.
func CFNSeededAZsForTest(region string) []string {
	out := make([]string, 0, len(ec2SeededAZSuffixes))
	for _, suffix := range ec2SeededAZSuffixes {
		out = append(out, region+suffix)
	}
	return out
}

// DispatchForTest routes req through the deployer's own registry, so a test can
// observe a resource the way the sweep does.
//
// It exists because the delete sweep's whole claim is about state a caller can
// observe through an API call: asserting a bucket is gone by reading the state
// manager would assert the implementation rather than the observable, and a test
// server of its own would be a second registry that need not agree with the one the
// sweep dispatched into.
func (d *StackDeployer) DispatchForTest(
	ctx context.Context, req *AWSRequest, streamID string,
) (*AWSResponse, error) {
	resp, _, err := d.dispatch(ctx, req, streamID)
	return resp, err
}

// DeleteStackResourcesForTest sweeps a persisted stack's resources and returns the
// per-resource outcome, without touching the stack record.
//
// DeleteStack reports the per-resource detail only on failure — a successful sweep
// removes the whole record — so this is the only way to assert what a *successful*
// sweep decided: that a Snapshot policy deleted without a snapshot, that an
// unmodeled type was skipped with a reason naming it, or that an RDS instance
// resolved its default policy as Snapshot.
func (d *StackDeployer) DeleteStackResourcesForTest(
	ctx context.Context, stackName string,
) []CFNResourceDeletion {
	stack, err := d.loadStack(ctx, stackName)
	if err != nil || stack == nil {
		return nil
	}
	return d.deleteStackResources(ctx, stack, stackName, cfnDeleteStackOp)
}

// CFNStackEventForTest is the derived stack event, aliased so an external test can
// read its members. The type stays unexported in the package proper: it is a wire
// shape, and the only supported way to reach one is DescribeStackEvents.
type CFNStackEventForTest = cfnStackEvent

// CFNDeriveStackEventsForTest wraps cfnDeriveStackEvents (#501).
//
// The derivation is a pure function of a stack record, and exercising it directly
// is what pins the parts the wire cannot vary: a rollback whose sweep left some
// resources deleted and others failed, an UPDATE_ROLLBACK_FAILED stack, and a
// record restored from a snapshot that has no Status at all. Reaching those
// through CreateStack would mean engineering a failure for each.
func CFNDeriveStackEventsForTest(s CFNStackState, stackID string) []CFNStackEventForTest {
	return cfnDeriveStackEvents(s, stackID)
}

// CFNPaginateEventsForTest wraps cfnPaginateEvents, so the page boundary can be
// asserted without deploying a template of more than CFNStackEventsPageSizeForTest
// resources.
func CFNPaginateEventsForTest(events []CFNStackEventForTest, token string) ([]CFNStackEventForTest, string) {
	return cfnPaginateEvents(events, token)
}

// CFNStackEventsPageSizeForTest is the DescribeStackEvents page size, so a test
// builds a page boundary from the value the code uses rather than a second copy of
// it.
const CFNStackEventsPageSizeForTest = cfnStackEventsPageSize

// CFNGeneratedNameSuffixLenForTest is the width of the derived suffix on a
// generated physical name, so an external test can split a name into its
// {stack}-{logical} prefix and its suffix without hard-coding the width.
const CFNGeneratedNameSuffixLenForTest = cfnGeneratedNameSuffixLen

// CFNGeneratedNameForTest exposes cfnGeneratedName so the constraint arithmetic
// (truncation, charset, lowercasing) can be asserted directly, rather than only
// through the resource types that happen to reach it from a template.
func CFNGeneratedNameForTest(accountID, region, stackName, resType, logicalID string) string {
	return cfnGeneratedName(&cfnContext{
		accountID: accountID,
		region:    region,
		stackName: stackName,
	}, resType, logicalID)
}

// CFGSvcCFNRecordingGroupForTest and CFGSvcCFNRecordingModeForTest expose the two
// CloudFormation-to-API translations for AWS Config's configuration recorder, so a
// test can assert the *emitted* wire keys.
//
// Asserting through the service instead would not do it: substrate decodes the
// request with encoding/json, which matches field names case-insensitively, so an
// UpperCamel body decodes into the same struct as a lowerCamel one and the two are
// indistinguishable downstream. Real Config is case-sensitive, and the request body
// is what an exported event log replays against AWS, so the keys have to be pinned
// where they are produced.
func CFGSvcCFNRecordingGroupForTest(v interface{}) map[string]interface{} {
	return cfgsvcCFNRecordingGroup(v, nil)
}

// CFGSvcCFNRecordingModeForTest exposes cfgsvcCFNRecordingMode — see
// CFGSvcCFNRecordingGroupForTest for why the emitted keys are asserted directly.
func CFGSvcCFNRecordingModeForTest(v interface{}) map[string]interface{} {
	return cfgsvcCFNRecordingMode(v, nil)
}

// IAMMemberListForTest wraps iamMemberList for external tests.
func IAMMemberListForTest(params map[string]string, prefix string) []string {
	return iamMemberList(params, prefix)
}

// IAMMemberStructsForTest wraps iamMemberStructs for external tests.
func IAMMemberStructsForTest(params map[string]string, prefix string) []map[string]string {
	return iamMemberStructs(params, prefix)
}

// IAMMemberTagsForTest wraps iamMemberTags for external tests.
func IAMMemberTagsForTest(params map[string]string) []IAMTag { return iamMemberTags(params) }

// IAMScalarParamsForTest is a struct holding one of each string-tolerant IAM scalar,
// for external tests of the query-protocol scalar decoding (#642).
type IAMScalarParamsForTest struct {
	// Count is an integer parameter, standing in for MaxItems.
	Count iamInt `json:"Count"`

	// Flag is a boolean parameter, standing in for OnlyAttached.
	Flag iamBool `json:"Flag"`

	// Nested is a list of structures holding a scalar, standing in for a shape whose
	// scalar is not a top-level member. It exists to pin how far iamParamField
	// resolves a name (#787).
	Nested []struct {
		// Count is a scalar one level down from the body's top level.
		Count iamInt `json:"Count"`
	} `json:"Nested"`
}

// IAMScalarValuesForTest returns the decoded scalars as plain Go values.
func (p IAMScalarParamsForTest) IAMScalarValuesForTest() (int, bool) {
	return p.Count.Int(), p.Flag.Bool()
}

// ParseIAMBodyForTest wraps parseIAMBody for external tests, so the whole
// body-decoding path — including the parameter-naming error message — is testable
// from package emulator_test.
func ParseIAMBodyForTest(body []byte, dst any) error { return parseIAMBody(body, dst) }

// ParseBlockDeviceMappingsForTest wraps ec2ParseBlockDeviceMappings so the
// BlockDeviceMapping.N reader is testable in isolation, including the prefix that
// lets one parser serve both RunInstances and a launch template (#666).
func ParseBlockDeviceMappingsForTest(params map[string]string, prefix string) []EC2BlockDeviceMapping {
	return ec2ParseBlockDeviceMappings(params, prefix)
}

// LaunchVolumesForTest wraps ec2LaunchVolumesFor, taking the two instance fields the
// resolution actually reads rather than a whole EC2Instance, so a test states only
// what it is asserting about.
func LaunchVolumesForTest(instanceID, availabilityZone string, mappings []EC2BlockDeviceMapping) []EC2Volume {
	inst := &EC2Instance{
		InstanceID:       instanceID,
		AvailabilityZone: availabilityZone,
		AccountID:        "123456789012",
		Region:           "us-east-1",
	}
	return ec2LaunchVolumesFor(inst, mappings, nil, "2026-01-01T00:00:00Z")
}

// RequestTagKeysForTest wraps requestTagKeys for external tests.
//
// The sorted order it produces cannot be observed through any API surface: every
// operator that reads aws:TagKeys is set-semantic, so reordering the slice changes no
// decision. What it does change is whether the same request produces the same
// recorded context twice, since three of addRequestTags' arms iterate a Go map — so
// this is the only place the ordering can be asserted at all, the same reason
// S3PersistedContentEncodingForTest exists (#690).
func RequestTagKeysForTest(condCtx map[string]string) []string { return requestTagKeys(condCtx) }

// AddRequestTagsForTest wraps addRequestTags, returning the aws:RequestTag entries it
// reads out of a request together with the aws:TagKeys value derived from them. It
// pairs the two halves the way CheckAccess does, so a test can assert that a service
// arm feeds both without going through a whole authorized request.
func AddRequestTagsForTest(req *AWSRequest) (map[string]string, []string) {
	tags := make(map[string]string)
	addRequestTags(tags, req)
	return tags, requestTagKeys(tags)
}

// --- IAM state keys (#737) -------------------------------------------------
//
// A test that seeds IAM state directly needs to spell the same key the plugin
// does, and since #737 that key carries an account. Exporting the builders is
// what keeps a test from hardcoding the layout: when the layout changes again,
// the tests move with the production code instead of drifting into addressing
// keys nothing reads.

// IAMUserKeyForTest wraps iamUserKey for external tests.
func IAMUserKeyForTest(accountID, userName string) string { return iamUserKey(accountID, userName) }

// IAMRoleKeyForTest wraps iamRoleKey for external tests.
func IAMRoleKeyForTest(accountID, roleName string) string { return iamRoleKey(accountID, roleName) }

// IAMGroupKeyForTest wraps iamGroupKey for external tests.
func IAMGroupKeyForTest(accountID, groupName string) string {
	return iamGroupKey(accountID, groupName)
}

// IAMPolicyKeyForTest wraps iamPolicyKey for external tests. The account comes from
// the ARN, as it does in production.
func IAMPolicyKeyForTest(arn string) string { return iamPolicyKey(arn) }

// IAMPolicyPrefixForTest wraps iamPolicyPrefix for external tests.
func IAMPolicyPrefixForTest(accountID string) string { return iamPolicyPrefix(accountID) }

// IAMAttachedPoliciesKeyForTest wraps iamAttachedPoliciesKey for external tests.
func IAMAttachedPoliciesKeyForTest(accountID, kind, name string) string {
	return iamAttachedPoliciesKey(accountID, kind, name)
}

// IAMAttachedPoliciesPrefixForTest wraps iamAttachedPoliciesPrefix for external tests.
func IAMAttachedPoliciesPrefixForTest(accountID, kind string) string {
	return iamAttachedPoliciesPrefix(accountID, kind)
}

// IAMInlinePolicyKeyForTest wraps iamInlinePolicyKey for external tests.
func IAMInlinePolicyKeyForTest(accountID, kind, entityName, policyName string) string {
	return iamInlinePolicyKey(accountID, kind, entityName, policyName)
}

// IAMInlinePolicyNamesKeyForTest wraps iamInlinePolicyNamesKey for external tests.
func IAMInlinePolicyNamesKeyForTest(accountID, kind, entityName string) string {
	return iamInlinePolicyNamesKey(accountID, kind, entityName)
}

// IAMGroupUsersKeyForTest wraps iamGroupUsersKey for external tests.
func IAMGroupUsersKeyForTest(accountID, groupName string) string {
	return iamGroupUsersKey(accountID, groupName)
}

// IAMUserGroupsKeyForTest wraps iamUserGroupsKey for external tests.
func IAMUserGroupsKeyForTest(accountID, userName string) string {
	return iamUserGroupsKey(accountID, userName)
}

// IAMAccessKeyKeyForTest wraps iamAccessKeyKey for external tests. It takes no
// account: an access key ID determines one, so the record carries it instead.
func IAMAccessKeyKeyForTest(accessKeyID string) string { return iamAccessKeyKey(accessKeyID) }

// IAMInstanceProfileKeyForTest wraps iamInstanceProfileKey for external tests.
func IAMInstanceProfileKeyForTest(accountID, name string) string {
	return iamInstanceProfileKey(accountID, name)
}

// IAMUserPrefixForTest wraps iamUserPrefix for external tests.
func IAMUserPrefixForTest(accountID string) string { return iamUserPrefix(accountID) }

// IAMRolePrefixForTest wraps iamRolePrefix for external tests.
func IAMRolePrefixForTest(accountID string) string { return iamRolePrefix(accountID) }

// IAMGroupPrefixForTest wraps iamGroupPrefix for external tests.
func IAMGroupPrefixForTest(accountID string) string { return iamGroupPrefix(accountID) }

// IAMInstanceProfilePrefixForTest wraps iamInstanceProfilePrefix for external tests.
func IAMInstanceProfilePrefixForTest(accountID string) string {
	return iamInstanceProfilePrefix(accountID)
}

// ELBTagsByARNForTest wraps elbLoadTagsByARN for external tests, so a test can assert what
// a create or a tagging call actually persisted rather than re-reading it through
// DescribeTags — which would let a bug in the reader hide a bug in the writer.
func ELBTagsByARNForTest(state StateManager, accountID, region, arn string) ([]ELBTag, error) {
	return elbLoadTagsByARN(state, accountID+"/"+region, arn)
}

// CBORPairForTest is one member of a map built by [CBORMapForTest].
type CBORPairForTest struct {
	// Key is the member name.
	Key string

	// Value is the member's value.
	Value any
}

// CBORMapForTest builds the codec's ordered map type from pairs, so an external test can
// pin the bytes of a structure without cborMap itself being exported. Order is preserved,
// which is the property under test.
func CBORMapForTest(pairs ...CBORPairForTest) any {
	m := make(cborMap, 0, len(pairs))
	for _, p := range pairs {
		m = append(m, cborEntry(p))
	}
	return m
}

// CBOREncodeForTest wraps cborEncode for external tests, which is where the wire bytes are
// asserted.
func CBOREncodeForTest(v any) ([]byte, error) { return cborEncode(v) }

// CBORDecodeForTest wraps cborDecode for external tests.
func CBORDecodeForTest(data []byte) (any, error) { return cborDecode(data) }

// CWNormalizeInputForTest wraps cwNormalizeInput, returning the flattened parameter map.
//
// The flattening is where a JSON or CBOR request becomes something a handler can read, so
// a test asserts the query-form keys it produces rather than the handler's behavior over
// them (#785).
func CWNormalizeInputForTest(protocol WireProtocol, body []byte) (map[string]string, error) {
	req := &AWSRequest{Protocol: protocol, Body: body}
	if err := cwNormalizeInput(req); err != nil {
		return nil, err
	}
	return req.Params, nil
}

// CWRespondForTest renders a DescribeAlarms-shaped response over alarms in the given
// protocol, returning the response's status, body and headers.
//
// Going through [CWAlarm] rather than exposing the neutral document keeps cwDoc internal
// while still exercising every part of it: nested structures, lists of structures, lists
// of strings, a double, two integers, a boolean, and members that are absent rather than
// empty.
func CWRespondForTest(protocol WireProtocol, operation, requestID string, alarms []CWAlarm) (int, []byte, map[string]string, error) {
	req := &AWSRequest{Protocol: protocol}
	resp, err := cwRespond(req, operation, requestID, cwDoc{}.with("MetricAlarms", cwAlarmList(alarms)))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Body, resp.Headers, nil
}

// CWUnitResponseForTest wraps cwUnitResponse, the answer for the six CloudWatch
// operations whose modeled output is smithy.api#Unit.
func CWUnitResponseForTest(protocol WireProtocol, operation, requestID string) (int, []byte, map[string]string, error) {
	req := &AWSRequest{Protocol: protocol}
	resp, err := cwUnitResponse(req, operation, requestID)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Body, resp.Headers, nil
}

// CWXMLTextForTest wraps cwXMLText, which renders a scalar as query-protocol character
// data. Exported separately because timestamps and blobs appear in CloudWatch's model but
// not in any response substrate currently builds.
func CWXMLTextForTest(v any) (string, error) { return cwXMLText(v) }

// CWParamTextForTest wraps cwParamText, the same scalars in the query form's request
// spelling.
func CWParamTextForTest(v any) (string, error) { return cwParamText(v) }

// AuthzActionResourceTypesForTest wraps authzActionResourceTypes, the reader for the
// generated Service Reference Information table (authz_reference_gen.go).
func AuthzActionResourceTypesForTest(service, operation string) ([]string, bool) {
	return authzActionResourceTypes(service, operation)
}

// AuthzActionSupportsResourceTypeForTest wraps authzActionSupportsResourceType, the gate
// that decides whether an operation may be authorized against a resource at all.
func AuthzActionSupportsResourceTypeForTest(service, operation, resourceType string) bool {
	return authzActionSupportsResourceType(service, operation, resourceType)
}

// AuthzResourceARNFormatsForTest wraps authzResourceARNFormats, AWS's documented ARN
// shapes for a resource type.
func AuthzResourceARNFormatsForTest(service, resourceType string) ([]string, bool) {
	return authzResourceARNFormats(service, resourceType)
}

// ResourceTypeMatchesForTest wraps resourceTypeMatches, the ResourceTypeFilters comparison
// GetResources applies to every scanned ARN.
//
// It is exported because the property under test is about ARN *shapes*, not about resources: the
// filter has to read a type out of a slash-delimited portion, a colon-delimited one, one behind a
// leading slash, and one that embeds no type at all. Reaching every shape through the wire would mean
// creating a resource in six services to assert one string comparison, and two of the shapes — an S3
// bucket whose name begins with "bucket", a malformed ARN — cannot be created at all. #936 is
// precisely a defect in that comparison, so it is pinned where it lives.
func ResourceTypeMatchesForTest(arn string, filters []string) bool {
	return resourceTypeMatches(arn, filters)
}

// TaggingResolveARNForTest wraps (*TaggingPlugin).resolveARN, the tagging API's ARN-to-state-key
// resolver.
//
// It is exported so a guard table can assert *where* an ARN is refused rather than only that it
// is. Both halves of the request path — a resolver that has no arm for the type, and a merge that
// finds no record at the key — answer a FailedResourcesMap entry over the wire, so a row that was
// refused by the resolver and is now refused by the merge, because its type gained an arm, stays
// green while no longer guarding the thing it is named for. That is #939, and three rows had gone
// stale that way before it was caught by reading a log line. The resolver's body never touches its
// receiver, so a zero-value plugin is enough to call it.
func TaggingResolveARNForTest(arn string) (ns, key string, err error) {
	return (&TaggingPlugin{}).resolveARN(arn)
}

// ELBResourceKindFromARNForTest wraps elbResourceKindFromARN, which classifies an ELBv2 ARN as
// one of the four taggable kinds.
//
// It is exported so a test can assert that the pre-#774 nested listener and rule ARNs an earlier
// version recorded are still recognized alongside the flat ones AWS publishes — a property no
// request-level assertion can reach, because nothing mints the old shape any more.
func ELBResourceKindFromARNForTest(arn string) string {
	return elbResourceKindFromARN(arn)
}

// EC2TaggableARNTypeForTest resolves an EC2 resource ID to the ARN type
// [ec2TaggableResource] gives it, or "" for a prefix it does not recognize.
//
// The type is what the authorization reference is consulted with, so a test can sweep
// every prefix substrate resolves and assert AWS publishes the type it names.
func EC2TaggableARNTypeForTest(id string) string {
	target, ok := ec2TaggableResource(NewMemoryStateManager(),
		&RequestContext{AccountID: "123456789012", Region: "us-east-1"}, id)
	if !ok {
		return ""
	}
	return target.arnType
}

// EC2AuthzNamedResourceARNsForTest returns the ARNs [ec2AuthzNamedResources] resolves for an
// operation and its parameters, in the order the decision walks them.
//
// It exists for the one case the plugin cannot reach: an operation AWS's reference does not
// publish cannot be dispatched, since substrate does not route it, yet the resolver's answer
// for such an operation is exactly what stops a stale snapshot from widening a grant (#762).
func EC2AuthzNamedResourceARNsForTest(state StateManager, operation string, params map[string]string) []string {
	reqCtx := &RequestContext{AccountID: "123456789012", Region: "us-east-1"}
	resources := ec2AuthzNamedResources(state, reqCtx, &AWSRequest{
		Service:   "ec2",
		Operation: operation,
		Params:    params,
	})
	arns := make([]string, 0, len(resources))
	for _, r := range resources {
		arns = append(arns, r.ARN)
	}
	return arns
}

// AuthzPrincipalContextForTest returns the condition keys [authzPrincipalContext] publishes
// for a principal, in a fresh map.
//
// It is how a test distinguishes an *absent* key from an empty one, which is the whole of
// the fallback contract for `aws:userid`: a policy testing it with `Null` inverts on the
// difference, and a decision test that only asserted allow/deny would pass either way (#771).
func AuthzPrincipalContextForTest(principal *Principal) map[string]string {
	ctx := make(map[string]string)
	authzPrincipalContext(ctx, principal)
	return ctx
}

// IAMPrincipalTagsForTest wraps iamPrincipalTags, the per-request read of the tags on the
// entity a principal ARN names.
func IAMPrincipalTagsForTest(state StateManager, principalARN string) map[string]string {
	return iamPrincipalTags(context.Background(), state, principalARN)
}

// IAMEntityForPrincipalARNForTest wraps iamEntityForPrincipalARN, returning the account, kind
// and friendly name a principal ARN resolves to and whether it names an entity at all.
//
// Exported because the two arms disagree deliberately — a user or role ARN's name is its
// *last* segment while an assumed-role ARN's is its *first*, since that form carries no path
// (#801) — and a decision test cannot tell "resolved to nothing" from "resolved and was
// permitted": both allow.
func IAMEntityForPrincipalARNForTest(principalARN string) (account, kind, name string, ok bool) {
	entity, found := iamEntityForPrincipalARN(principalARN)
	return entity.Account, entity.Kind, entity.Name, found
}

// IAMCallerUserNameForTest wraps iamCallerUserName, the user an operation acts on when its
// UserName parameter is absent.
func IAMCallerUserNameForTest(principal *Principal) string {
	return iamCallerUserName(&RequestContext{Principal: principal})
}

// AuthzServiceActionResourcesForTest returns every action AWS's reference publishes for a
// service, mapped to the resource types it publishes for that action.
//
// It reads the generated map directly, which [authzActionResourceTypes]' own file forbids
// production code from doing — the point of that rule is that the *meaning* of an absent key
// stays in one place, and enumeration asks nothing about absence. A test needs the enumeration
// to check the other direction of #770's claim: that every IAM action AWS publishes no resource
// types for is absent from [iamAuthzOperationResource]. Asserting only the rows that exist
// would pass a table that had quietly acquired a row for ListUsers.
func AuthzServiceActionResourcesForTest(service string) map[string][]string {
	prefix := service + ":"
	actions := make(map[string][]string)
	for key, types := range authzActionResources {
		if operation, ok := strings.CutPrefix(key, prefix); ok {
			actions[operation] = types
		}
	}
	return actions
}

// AuthzResourceARNsForTest returns the resource ARNs [AuthController.buildResourceARNs]
// decides a request against.
//
// It is the generic gate's half of #770's both-doors invariant: the same request put to
// [IAMAuthzResourceForTest] must produce the same string, or one policy gets two answers
// depending on which door the caller arrived at.
func AuthzResourceARNsForTest(a *AuthController, reqCtx *RequestContext, req *AWSRequest) []string {
	resources := a.buildResourceARNs(reqCtx, req)
	arns := make([]string, 0, len(resources))
	for _, res := range resources {
		arns = append(arns, res.ARN)
	}
	return arns
}

// IAMAuthzResourceForTest wraps [IAMPlugin.authzResource], the resource every gate inside the
// IAM plugin passes — the plugin door's half of the same invariant. It answers the ARN, which
// is what that invariant is about; [IAMAuthzResourceTagsForTest] answers the tags beside it.
func IAMAuthzResourceForTest(p *IAMPlugin, reqCtx *RequestContext, req *AWSRequest) string {
	return p.authzResource(reqCtx, req).ARN
}

// IAMAuthzResourceTagsForTest answers the tags the plugin door publishes for a request, and
// [AuthzResourceTagsForTest] the ones the generic gate does.
//
// They are the same invariant as the ARN, one condition key over: a tag read at one door and
// not the other means one policy gets two answers depending on which door the caller arrived
// at, which is what #804 was on the resource side of (#770).
func IAMAuthzResourceTagsForTest(p *IAMPlugin, reqCtx *RequestContext, req *AWSRequest) map[string]string {
	return p.authzResource(reqCtx, req).Tags
}

// AuthzResourceTagsForTest returns the tags the generic gate pairs with each resource a
// request names, in the same order as [AuthzResourceARNsForTest].
func AuthzResourceTagsForTest(a *AuthController, reqCtx *RequestContext, req *AWSRequest) []map[string]string {
	resources := a.buildResourceARNs(reqCtx, req)
	tags := make([]map[string]string, 0, len(resources))
	for _, res := range resources {
		tags = append(tags, res.Tags)
	}
	return tags
}

// IAMAuthzAccountResourceARNForTest wraps iamAuthzAccountResourceARN, the resource an IAM
// request whose own resource substrate cannot resolve is decided against.
func IAMAuthzAccountResourceARNForTest(accountID string) string {
	return iamAuthzAccountResourceARN(accountID)
}

// IAMInstanceProfileARNForTest wraps iamInstanceProfileARN, which joined the other four
// minters with #770.
func IAMInstanceProfileARNForTest(accountID, path, name string) string {
	return iamInstanceProfileARN(accountID, path, name)
}

// IAMAuthzResourceRowForTest is one row of [iamAuthzOperationResource], flattened into
// exported fields so a test outside the package can sweep the whole table.
type IAMAuthzResourceRowForTest struct {
	// Operation is the row's key.
	Operation string
	// Type is the resource type AWS publishes for the action, or "" for a row whose parameter
	// carries a finished ARN of varying type.
	Type string
	// NameParams are the request parameters that may carry the resource's name.
	NameParams []string
	// CallerIsResource reports that an absent name means the calling user.
	CallerIsResource bool
}

// IAMAuthzOperationResourceRowsForTest returns every row of [iamAuthzOperationResource].
//
// The order is unspecified, matching the map it comes from: every assertion written against it
// is per row or a set membership, so nothing may depend on the runtime's iteration order.
func IAMAuthzOperationResourceRowsForTest() []IAMAuthzResourceRowForTest {
	rows := make([]IAMAuthzResourceRowForTest, 0, len(iamAuthzOperationResource))
	for operation, ref := range iamAuthzOperationResource {
		rows = append(rows, IAMAuthzResourceRowForTest{
			Operation:        operation,
			Type:             ref.Type,
			NameParams:       ref.NameParams,
			CallerIsResource: ref.CallerIsResource,
		})
	}
	return rows
}

// PayloadPathsForTest returns every file path in the S3 plugin's payload filesystem,
// sorted. It reports the paths rather than the bytes, which is all an assertion about
// a reset needs.
//
// Exported because the payloads are unobservable through the wire by construction:
// every read of the filesystem is gated on object metadata the state manager holds,
// so after a reset no request can tell an emptied filesystem from one still holding
// the previous run's bytes. That is why the leak in #902 went unnoticed, and it is
// why asserting on it needs a way in from outside. The payloads a test asserts on are
// still written by a real PutObject, so #765's rule — that a helper writing state
// directly cannot prove anything about the owning service — is not in play.
func (p *S3Plugin) PayloadPathsForTest() ([]string, error) {
	var paths []string
	err := afero.Walk(p.fs, "/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk s3 payload filesystem: %w", err)
	}
	sort.Strings(paths)
	return paths, nil
}

// OwnsFilesystemForTest reports whether the S3 plugin created its own payload
// filesystem rather than being handed one as Options["filesystem"] — the condition
// [S3Plugin.ResetForRun] empties it on.
func (p *S3Plugin) OwnsFilesystemForTest() bool { return p.ownsFS }

// ESMPollerCountForTest returns the number of event-source-mapping pollers the Lambda
// plugin is holding a stop channel for.
//
// Exported because a poller is otherwise unobservable: it acts only on a one-second
// wall-clock ticker, so asserting that a stopped poller stops polling would make the
// test depend on real elapsed time, which the house rules forbid. The count is the
// deterministic form of the same assertion — the channel is closed and forgotten, and
// [LambdaPlugin.sqsPollerLoop] returns on that channel unconditionally.
func (p *LambdaPlugin) ESMPollerCountForTest() int {
	p.esmMu.Lock()
	defer p.esmMu.Unlock()
	return len(p.esmStop)
}

// EvictionStoppedForTest reports whether the executor's idle-eviction goroutine has
// been told to exit — the one difference between [LambdaExecutor.StopAll] and
// [LambdaExecutor.DrainPool], and the reason a state reset calls the second (#903).
//
// The read is non-blocking, so it reports the channel's state rather than waiting on
// it, and it does not depend on whether the goroutine has noticed yet.
func (e *LambdaExecutor) EvictionStoppedForTest() bool {
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

// RDSActiveContainerCountForTest returns the number of Postgres containers the
// executor still believes it is holding. Until #903 nothing ever dropped an entry, so
// the map grew for the life of the process and StopAll re-stopped containers it had
// already stopped.
func RDSActiveContainerCountForTest(e *RDSExecutor) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.active)
}

// CheckReplayGatesForTest runs the four pre-plugin gates a [ReplayPipeline] holds and
// reports the step that refused ("" when none did), the refusal, and any latency a
// fault rule asked for.
//
// Exported because the gate is what #833 added and what it decides is otherwise only
// observable through a whole replay: a quota or consistency refusal is *expected to be
// absent* on replay, and a test that asserted its absence by replaying could not tell
// "the controller was consulted and exempted the request" from "the controller was
// never reached" — which is the exact confusion that left three isReplaying guards
// unreachable for as long as they existed.
func CheckReplayGatesForTest(p ReplayPipeline, reqCtx *RequestContext, req *AWSRequest) (string, time.Duration, error) {
	out := p.gates().check(reqCtx, req)
	return string(out.step), out.latency, out.err
}

// RewindFaultsForTest returns the controller to its armed state, as a replay does at
// its start. It is exported so a test can assert the rewind's effect on
// [FaultController.GetConfig] directly, rather than only through the replay that calls
// it (#833).
func RewindFaultsForTest(f *FaultController) { f.rewindForReplay() }

// ReplayBodyDifference is one difference from a response-body comparison, in the
// shape a test asserts on: the [EventDifference.Field] the replay engine would
// record and the two values.
type ReplayBodyDifference struct {
	// Field is the value bodyDifferenceField renders for the difference's path.
	Field string

	// Expected is the value the recorded body carries.
	Expected interface{}

	// Actual is the value the replayed body carries.
	Actual interface{}
}

// CompareResponseBodiesForTest compares two response bodies the way replay
// verification does and returns the differences.
//
// Exported because the comparison is worth testing on a pair of bodies rather than
// only through a whole replay: the normalisation policy #817 asks to have stated —
// JSON member order ignored, XML indentation ignored, collection order and every
// minted identifier reported — is a claim about specific pairs of documents, and a
// recording cannot be made to contain most of them on demand.
func CompareResponseBodiesForTest(recorded, replayed []byte) []ReplayBodyDifference {
	diffs := responseBodyDifferences(&AWSResponse{Body: recorded}, &AWSResponse{Body: replayed})
	out := make([]ReplayBodyDifference, 0, len(diffs))
	for _, d := range diffs {
		out = append(out, ReplayBodyDifference{
			Field:    bodyDifferenceField(d.path),
			Expected: d.expected,
			Actual:   d.actual,
		})
	}
	return out
}

// ReplayBodyDiffLimitForTest is the per-body cap on reported differences, so a test
// asserting the truncation marker does not restate the constant.
func ReplayBodyDiffLimitForTest() int { return replayBodyDiffLimit }

// ReplayBodyDiffTruncatedForTest is the Actual value of the difference appended when
// a comparison stops at the cap.
func ReplayBodyDiffTruncatedForTest() string { return replayBodyDiffTruncated }

// ReplayBodyMemberAbsentForTest is the value reported for a side that has no member,
// element or attribute at a path.
func ReplayBodyMemberAbsentForTest() string { return bodyMemberAbsent }

// S3AccountingPrefixForTest returns the S3 bucket and the accounting prefix a
// request's request-rate counts against.
//
// Exported because the rule is substrate's own choice (#818) and worth asserting
// directly: the key-to-prefix mapping is a claim about paths, and reaching every
// case through the gate would take one server call per case to assert something the
// derivation states in one line.
func S3AccountingPrefixForTest(req *AWSRequest) (bucket, prefix string) {
	return s3AccountingPrefix(req)
}

// S3PrefixRateRuleKeysForTest returns the two [QuotaConfig.Rules] keys that carry
// S3's published per-prefix ceilings, read and write in that order.
func S3PrefixRateRuleKeysForTest() (read, write string) {
	return s3RateRuleKey(s3RateRead), s3RateRuleKey(s3RateWrite)
}

// S3PrefixRateDefaultsForTest returns the published per-prefix request-rate
// ceilings, so a test asserting the built-in rules does not restate the figures.
func S3PrefixRateDefaultsForTest() (read, write float64) {
	return s3PrefixReadRequestsPerSecond, s3PrefixWriteRequestsPerSecond
}

// DefaultQuotaRulesForTest returns the built-in rate limits.
func DefaultQuotaRulesForTest() map[string]RateRule { return defaultQuotaRules() }
