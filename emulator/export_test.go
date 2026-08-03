package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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

// ExtractAccessKeyFromAuthForTest wraps extractAccessKeyFromAuth for external tests.
func ExtractAccessKeyFromAuthForTest(authHeader string) string {
	return extractAccessKeyFromAuth(authHeader)
}

// BuildCallerARNForTest wraps buildCallerARN for external tests.
func BuildCallerARNForTest(accountID, accessKeyID string) string {
	return buildCallerARN(accountID, accessKeyID)
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
	default:
		return ErrProtoUnknownForTest
	}
}

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
