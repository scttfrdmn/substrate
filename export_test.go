package substrate

import (
	"context"
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

// CheckPresignedExpiryForTest exposes checkPresignedExpiry for white-box tests.
func CheckPresignedExpiryForTest(q url.Values, now time.Time) bool {
	return checkPresignedExpiry(q, now)
}
