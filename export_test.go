package substrate

import (
	"context"
	"net/http"
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
