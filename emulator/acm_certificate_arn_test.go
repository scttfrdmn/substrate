package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #921: every code ACM publishes on the five operations that take a CertificateArn carries HTTP
// 400, and substrate answered ResourceNotFoundException at 404 — a status ACM publishes nowhere.
// A malformed ARN also reached the state lookup and was reported absent, which answers a question
// the caller did not ask: the string they sent could not name a certificate, so retrying against a
// real one will not help.
//
// Everything here goes over the wire and asserts the **status alongside the code**, because that
// is the whole of the issue. A decoded error struct carries the code and not the status, so a test
// reading one could not have caught the 404 and none did — the existing ACM tests assert the code
// after a delete and stop there.
//
// The five operations are covered as one table rather than three (the tag operations) plus two,
// because they share one validator and one not-found helper. A test that exercised only the tag
// operations would leave the other two resting on nothing, which is how the SSM helper's status was
// left unpinned until #933.

// acmCertificateOperations are the five ACM operations that take a CertificateArn and therefore
// share [acmValidateCertificateARN] and the not-found helper.
//
// The body is the minimum each operation needs beyond the ARN: the two tag-writing operations
// publish Tags as required, and the other three take the ARN alone. It is a function of the ARN
// rather than a fixed map so one table row can be sent to all five.
var acmCertificateOperations = []struct {
	name string
	body func(arn string) map[string]any
}{
	{"DescribeCertificate", func(arn string) map[string]any {
		return map[string]any{"CertificateArn": arn}
	}},
	{"DeleteCertificate", func(arn string) map[string]any {
		return map[string]any{"CertificateArn": arn}
	}},
	{"AddTagsToCertificate", func(arn string) map[string]any {
		return map[string]any{"CertificateArn": arn, "Tags": []map[string]string{{"Key": "k", "Value": "v"}}}
	}},
	{"RemoveTagsFromCertificate", func(arn string) map[string]any {
		return map[string]any{"CertificateArn": arn, "Tags": []map[string]string{{"Key": "k"}}}
	}},
	{"ListTagsForCertificate", func(arn string) map[string]any {
		return map[string]any{"CertificateArn": arn}
	}},
}

// acmCall posts one ACM operation for one ARN and returns the status and error code.
func acmCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, string) {
	t.Helper()
	return decodeAWSResponse(t,
		signedRequest(t, ts, acmTarget, taggingTestAccount, op, body), nil)
}

// TestACMCertificateARN_AnAbsentCertificateIsReportedAt400 pins the status of the not-found answer
// on every operation that can give it.
//
// The ARN is well-formed and names the caller's own account and Region, so nothing but the missing
// record can be refusing it — which is what makes the code assertion mean the thing the issue is
// about. AWS gives ResourceNotFoundException HTTP 400 on all five pages, describing it as "The
// specified certificate cannot be found in the caller's account or the caller's account cannot be
// found".
func TestACMCertificateARN_AnAbsentCertificateIsReportedAt400(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	const arn = "arn:aws:acm:us-east-1:" + taggingTestAccount +
		":certificate/12345678-1234-1234-1234-123456789012"

	for _, op := range acmCertificateOperations {
		t.Run(op.name, func(t *testing.T) {
			status, code := acmCall(t, ts, op.name, op.body(arn))
			assert.Equal(t, "ResourceNotFoundException", code, "%s on an absent certificate", op.name)
			assert.Equal(t, http.StatusBadRequest, status, "%s on an absent certificate", op.name)
		})
	}
}

// TestACMCertificateARN_AnUnusableARNIsRefusedBeforeTheLookup covers both refusal tiers and every
// reason each is reached, on every operation.
//
// The two codes are asserted separately because they tell a caller different things and the split
// is the point of the issue: ValidationException means the value breaks a constraint AWS publishes
// on the member, InvalidArnException means it satisfies the pattern and still cannot name a
// certificate. Neither is ResourceNotFoundException, which is what all of these answered before.
//
// The rows whose want is InvalidArnException are the ones that carry substrate's reading rather
// than AWS's — see acm_certificate_arn.go's preamble — so they are the rows to revisit if AWS ever
// publishes what it answers for them.
func TestACMCertificateARN_AnUnusableARNIsRefusedBeforeTheLookup(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))

	for _, tc := range []struct {
		name string
		arn  string
		want string
	}{
		// Tier one: a documented constraint on the member is broken. AWS's own code.
		{"absent", "", "ValidationException"},
		{"shorter than the published minimum", "arn:aws:acm:x", "ValidationException"},
		{"longer than the published maximum",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":certificate/" + strings.Repeat("a", 2048),
			"ValidationException"},
		{"another service", "arn:aws:s3:::a-bucket-named-long-enough-to-pass", "ValidationException"},
		{"an account that is not digits",
			"arn:aws:acm:us-east-1:not-an-account:certificate/abc-123", "ValidationException"},
		{"not an ARN at all", "12345678-1234-1234-1234-123456789012", "ValidationException"},

		// Tier two: the pattern is satisfied and the ARN still names no certificate. Substrate's
		// reading of InvalidArnException, whose published description speaks of non-existence.
		{"no Region", "arn:aws:acm::" + taggingTestAccount + ":certificate/abc-123", "InvalidArnException"},
		{"another ACM resource type",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":acme-endpoint/abc-123", "InvalidArnException"},
		{"a resource type with nothing after it",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":certificate", "InvalidArnException"},
		{"something nested under a certificate",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":certificate/abc-123/renewal", "InvalidArnException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range acmCertificateOperations {
				status, code := acmCall(t, ts, op.name, op.body(tc.arn))
				assert.Equal(t, tc.want, code, "%s with %s", op.name, tc.name)
				assert.Equal(t, http.StatusBadRequest, status, "%s with %s", op.name, tc.name)
			}
		})
	}
}

// TestACMCertificateARN_TheARNACMMintsIsAccepted is the guard against the validator being too
// strict, which is the failure mode a table of refusals cannot see.
//
// The ARN comes from RequestCertificate rather than being written here, so the test cannot pass by
// agreeing with a shape substrate happens to build in two places. All five operations are exercised
// in an order that leaves the delete last, since it removes the record the others read.
func TestACMCertificateARN_TheARNACMMintsIsAccepted(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	arn := requestACMCertificate(t, ts, "accepted.example.com")

	for _, op := range []string{
		"DescribeCertificate", "AddTagsToCertificate", "ListTagsForCertificate",
		"RemoveTagsFromCertificate", "DeleteCertificate",
	} {
		var body map[string]any
		for _, candidate := range acmCertificateOperations {
			if candidate.name == op {
				body = candidate.body(arn)
			}
		}
		require.NotNil(t, body, "a body for %s", op)

		status, code := acmCall(t, ts, op, body)
		assert.Empty(t, code, "%s on the ARN ACM minted", op)
		assert.Equal(t, http.StatusOK, status, "%s on the ARN ACM minted", op)
	}
}

// TestACMCertificateARN_TheTaggingAPIRefusesWhatACMRefuses asserts the two arms agree, which is
// what routing both through one validator buys.
//
// The tagging API renders its refusal into a FailedResourcesMap entry rather than an error
// response, so the shapes differ and only the decision can be compared. An ARN naming something
// nested under a certificate is the case where the two used to be able to drift: ACM's own
// operations built a key nothing was stored at and said "not found", while the resolver refused it
// by name.
func TestACMCertificateARN_TheTaggingAPIRefusesWhatACMRefuses(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	const nested = "arn:aws:acm:us-east-1:" + taggingTestAccount + ":certificate/abc-123/renewal"

	status, code := acmCall(t, ts, "ListTagsForCertificate", map[string]any{"CertificateArn": nested})
	require.Equal(t, "InvalidArnException", code, "ACM's own operation refuses a nested ARN")
	require.Equal(t, http.StatusBadRequest, status, "ACM's own operation refuses a nested ARN")

	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	tagStatus, tagCode := decodeAWSResponse(t,
		signedRequest(t, ts, taggingTarget, taggingTestAccount, "TagResources",
			map[string]any{"ResourceARNList": []string{nested}, "Tags": map[string]string{"k": "v"}}), &out)
	require.Empty(t, tagCode, "TagResources answers a FailedResourcesMap, not an error")
	require.Equal(t, http.StatusOK, tagStatus, "TagResources answers a FailedResourcesMap, not an error")
	assert.Contains(t, out.FailedResourcesMap, nested, "the tagging API refuses the same ARN")
}
