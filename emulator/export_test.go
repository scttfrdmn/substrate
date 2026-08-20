package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// This file exports internal symbols for use in external test packages.
// It is compiled only when running tests.

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
func IAMAuthorizeForTest(p *IAMPlugin, ctx *RequestContext, action, resource string) error {
	return p.authorize(context.Background(), ctx, action, resource)
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
func LambdaPluginCreateFunctionForTest(p *LambdaPlugin, fn LambdaFunction) {
	_, _ = p.saveFunctionAndRespond(fn, 200)
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
	ErrProtoQueryXMLForTest = "query-xml"
	ErrProtoJSONRPCForTest  = "json-rpc"
	ErrProtoRESTJSONForTest = "rest-json"
	ErrProtoS3XMLForTest    = "s3-xml"
	ErrProtoEC2XMLForTest   = "ec2-xml"
	ErrProtoUnknownForTest  = "unknown"
)

// ErrorProtocolForTest wraps errorProtocolFor, returning one of the
// ErrProto*ForTest names.
func ErrorProtocolForTest(service, contentType string) string {
	switch errorProtocolFor(service, contentType) {
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
	default:
		return ErrProtoUnknownForTest
	}
}

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
func MarshalAWSErrorForTest(code, message, proto, jsonContentType string, status int) (body []byte, contentType string, headers map[string]string) {
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
	}
	return marshalAWSError(&AWSError{Code: code, Message: message, HTTPStatus: status}, p, jsonContentType)
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
		accountID:  testAccountID,
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
		accountID:  testAccountID,
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
		accountID:  testAccountID,
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
		accountID:  testAccountID,
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
