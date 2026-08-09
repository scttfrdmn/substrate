package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #595: the generic authorization gate returned AccessDeniedException for every
// service, but AWS only uses the suffixed form on the JSON protocols. On the XML
// ones it sends the bare AccessDenied — which is what #593's trust-policy gate
// already emitted, so substrate reported *two different codes for one conceptual
// outcome* depending on which gate refused. A consumer matching on the code saw a
// denial it could not identify.

// TestAccessDeniedCodeFor_TracksTheWireProtocol pins the mapping the fix rests
// on. Audited across every botocore model, deduplicated by service and restricted
// to shapes carrying "exception": true:
//
//	family                                  bare AccessDenied  AccessDeniedException
//	XML   (query 17, rest-xml 4, ec2 1)     1 (cloudfront)     0
//	JSON  (json 149, rest-json 242, cbor 2) 0                  233
//
// The JSON arm is settled by the models: all 233 that declare the shape use the
// suffixed form. The XML arm is not — only CloudFront declares it at all, so what
// decides it is that no query-protocol service models an access-denied shape
// (all 17, STS and IAM and CloudFormation among them), nor do S3 or EC2. Every
// service where this code is observable in substrate is in that set, so the value
// is observed AWS behavior rather than modeled. See accessDeniedCodeFor.
func TestAccessDeniedCodeFor_TracksTheWireProtocol(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		service     string
		contentType string
		want        string
	}{
		// The services #595 names. STS is the one whose real code is quoted in the
		// issue from AWS's own CLI output.
		{"sts is query xml", "sts", "", "AccessDenied"},
		{"iam is query xml", "iam", "application/x-amz-json-1.1", "AccessDenied"},
		{"cloudformation is query xml", "cloudformation", "", "AccessDenied"},
		{"sns is query xml", "sns", "", "AccessDenied"},

		// S3 and EC2 have their own error documents but are still XML-family, and
		// s3_publicaccess.go already documents the bare code for S3.
		{"s3 is bare", "s3", "", "AccessDenied"},
		{"ec2 is bare", "ec2", "", "AccessDenied"},

		// The JSON side, where the models are unanimous.
		{"dynamodb is json rpc", "dynamodb", "application/x-amz-json-1.0", "AccessDeniedException"},
		{"ssm is json rpc", "ssm", "application/x-amz-json-1.1", "AccessDeniedException"},
		{"sqs is json rpc", "sqs", "", "AccessDeniedException"},
		{"lambda is rest json", "lambda", "application/json", "AccessDeniedException"},
		{"apigateway is rest json", "apigateway", "application/json", "AccessDeniedException"},

		// An unregistered service falls through errorProtocolFor's Content-Type
		// sniff, so it inherits that fallback rather than a code of its own.
		{"unknown with a json body", "notaservice", "application/x-amz-json-1.1", "AccessDeniedException"},
		{"unknown with no content type", "notaservice", "", "AccessDenied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want,
				emulator.AccessDeniedCodeForTest(tt.service, tt.contentType))
		})
	}
}

// TestAccessDeniedCode_MatchesTheServiceErrorProtocol is the property behind the
// table above, asserted over every registered service rather than a sample: a
// service's denial code is the suffixed form exactly when its error protocol is
// one of the JSON ones. A new plugin added to serviceErrorProtocols is covered by
// this without anyone remembering to extend the table.
func TestAccessDeniedCode_MatchesTheServiceErrorProtocol(t *testing.T) {
	t.Parallel()
	for _, svc := range emulator.RegisteredErrorProtocolServicesForTest() {
		proto := emulator.ErrorProtocolForTest(svc, "")
		wantSuffixed := proto == emulator.ErrProtoJSONRPCForTest ||
			proto == emulator.ErrProtoRESTJSONForTest

		want := "AccessDenied"
		if wantSuffixed {
			want = "AccessDeniedException"
		}
		assert.Equal(t, want, emulator.AccessDeniedCodeForTest(svc, ""),
			"service %q classified as %s", svc, proto)
	}
}

// TestAccessDenied_IAMPluginAgreesWithTheGenericGate closes the other half of the
// drift. IAM's plugin runs its own authorize() and built its denials from a
// hardcoded literal, so fixing only AuthController would have left substrate
// reporting two codes for the *same service* — which is #595's complaint restated
// one level down.
func TestAccessDenied_IAMPluginAgreesWithTheGenericGate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, emulator.AccessDeniedCodeForTest("iam", ""),
		emulator.IAMAccessDeniedCodeForTest,
		"the IAM plugin's denial code must be the one its protocol implies")
}

// TestAccessDenied_BothGatesAgree is the test #595 asks for by name: an identity
// denial and a trust-policy denial on the same role must report the same code, so
// the two cannot drift apart again.
//
// It is a live-server test rather than a unit assertion because the two codes are
// produced in different places — AuthController.CheckAccess for the identity gate
// and the STS plugin's trust-policy evaluation for the other — and only a request
// through the whole pipeline shows what a caller actually receives from each.
func TestAccessDenied_BothGatesAgree(t *testing.T) {
	const roleARN = "arn:aws:iam::123456789012:role/broker"
	trustPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}`

	// The identity gate: a caller with no sts:AssumeRole grant. The trust policy
	// here *would* allow it, so the refusal can only come from the identity gate.
	identitySrv := newTrustPolicyTestServer(t)
	identityKey := trustSetupCallerWithoutAssume(t, identitySrv, "ungranted")
	trustCreateRole(t, identitySrv, "broker", trustPolicy)

	identityDenied := trustAssumeRole(t, identitySrv, identityKey, roleARN, "session", "")
	require.Equal(t, http.StatusForbidden, identityDenied.StatusCode)
	identityCode, _ := trustErrorFrom(t, identityDenied)

	// The trust-policy gate: a caller that *is* granted sts:AssumeRole, refused by
	// a role that trusts only another account.
	trustSrv := newTrustPolicyTestServer(t)
	trustKey := trustSetupCaller(t, trustSrv, "granted")
	trustCreateRole(t, trustSrv, "broker", `{"Version":"2012-10-17","Statement":[`+
		`{"Effect":"Allow","Principal":{"AWS":"999999999999"},"Action":"sts:AssumeRole"}]}`)

	trustDenied := trustAssumeRole(t, trustSrv, trustKey, roleARN, "session", "")
	require.Equal(t, http.StatusForbidden, trustDenied.StatusCode)
	trustCode, _ := trustErrorFrom(t, trustDenied)

	assert.Equal(t, trustCode, identityCode,
		"the two gates must report one code for one outcome")
	// And that shared code is the one AWS sends for STS, so agreeing on the wrong
	// value would not satisfy this test.
	assert.Equal(t, "AccessDenied", identityCode)
}

// trustSetupCallerWithoutAssume creates an IAM user with a policy that grants
// something unrelated, and returns its access key ID.
//
// A policy is still attached, deliberately. A user with no policy at all is an
// implicit deny too, but resolveIAMEntity's not-enforced arm and an empty policy
// list are different paths; granting an unrelated action puts the request on the
// evaluated-and-refused path, which is the one that produces a denial code.
func trustSetupCallerWithoutAssume(t *testing.T, srv *emulator.Server, userName string) string {
	t.Helper()
	require.Equal(t, http.StatusOK,
		trustIAMCall(t, srv, "CreateUser", map[string]any{"UserName": userName}).StatusCode)
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   userName,
		"PolicyName": "unrelated",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
	}).StatusCode)

	resp := trustIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": userName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	var parsed struct {
		AccessKey struct {
			AccessKeyID string `xml:"AccessKeyId"`
		} `xml:"CreateAccessKeyResult>AccessKey"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed))
	require.NotEmpty(t, parsed.AccessKey.AccessKeyID)
	return parsed.AccessKey.AccessKeyID
}
