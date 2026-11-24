package emulator_test

// The plugins' own checks that request validation duplicates (#1612): they answer what the gate
// answers, whoever builds the request and whether or not the gate is on.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestWireValidation_APluginCalledWithoutAServiceStillRefuses pins that a plugin's own check
// names its service itself: called directly with an AWSRequest whose Service is unset, as an
// embedder or a unit test may build one, it still answers a code and a status.
func TestWireValidation_APluginCalledWithoutAServiceStillRefuses(t *testing.T) {
	ts := emulator.StartTestServer(t)
	reqCtx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "direct-1"}
	for _, tc := range []struct {
		plugin, code string
		req          *emulator.AWSRequest
	}{
		{"elasticache", "InvalidParameterValue", &emulator.AWSRequest{Operation: "CreateCacheCluster",
			Params: map[string]string{"Action": "CreateCacheCluster"}}},
		{"ec2", "MissingParameter", &emulator.AWSRequest{Operation: "CreateKeyPair",
			Params: map[string]string{"Action": "CreateKeyPair"}}},
		{"sesv2", "BadRequestException", &emulator.AWSRequest{Operation: "CreateEmailIdentity", HTTPMethod: "POST",
			Path: "/v2/email/identities", Body: []byte(`{}`), Params: map[string]string{}}},
		{"route53", "InvalidInput", &emulator.AWSRequest{Operation: "CreateHostedZone", HTTPMethod: "POST",
			Path: "/2013-04-01/hostedzone", Params: map[string]string{},
			Body: []byte(`<CreateHostedZoneRequest><CallerReference>r</CallerReference></CreateHostedZoneRequest>`)}},
		{"acm", "ValidationException", &emulator.AWSRequest{Operation: "RequestCertificate", Body: []byte(`{}`),
			Headers: map[string]string{}, Params: map[string]string{}}},
	} {
		t.Run(tc.plugin, func(t *testing.T) {
			p, ok := ts.Registry().Plugin(tc.plugin)
			require.True(t, ok)
			_, err := p.HandleRequest(reqCtx, tc.req)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, tc.code, awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
			assert.NotEmpty(t, awsErr.Message)
		})
	}
}

// TestWireValidation_Route53BatchChecksAnswerInTheGatesWords pins that ChangeResourceRecordSets'
// own check of a batch, which a request the gate does not see reaches, reports what the gate
// reports, every failure included, and that a blank Name is not reported as null.
func TestWireValidation_Route53BatchChecksAnswerInTheGatesWords(t *testing.T) {
	on := emulator.StartTestServer(t)
	off := emulator.StartTestServer(t, emulator.WithoutWireValidation())
	for _, tc := range []struct{ name, rrType, ttl, want string }{
		{"a bad Type", "a", "300", "1 validation error detected"},
		{"a bad Type and a bad TTL", "a", "3600000000000", "2 validation errors detected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var answers [2]string
			for i, server := range []*emulator.TestServer{on, off} {
				zone := wireHostedZone(t, server, "type"+string(rune('a'+i))+".example")
				status, body := route53Call(http.MethodPost, "/2013-04-01/hostedzone/"+zone+"/rrset/",
					wireChangeBatch(tc.rrType, tc.ttl)).do(t, server)
				require.Equal(t, http.StatusBadRequest, status, string(body))
				code, message := wireErrorOf(t, body)
				assert.Equal(t, "InvalidInput", code)
				assert.True(t, strings.HasPrefix(message, tc.want), message)
				answers[i] = message
			}
			assert.Equal(t, answers[0], answers[1], "gate on and gate off answer alike")
		})
	}

	status, body := route53Call(http.MethodPost, "/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest><Name></Name><CallerReference>r</CallerReference></CreateHostedZoneRequest>`).do(t, on)
	require.Equal(t, http.StatusBadRequest, status, string(body))
	_, message := wireErrorOf(t, body)
	assert.Equal(t, "1 validation error detected: Value '' at 'name' failed to satisfy constraint: Member must not be blank",
		message)
}

// TestWireValidation_AnUnknownMemberKeepsThePluginsRefusal pins the fallback: a check whose
// member the tables do not hold answers the plugin's own refusal rather than an empty code.
func TestWireValidation_AnUnknownMemberKeepsThePluginsRefusal(t *testing.T) {
	err := emulator.WireRequireMemberFallbackForTest()
	require.NotNil(t, err)
	assert.Equal(t, "Fallback", err.Code)
}

// TestWireValidation_ASentenceNamesTheFirstMissingMember pins that a service with a sentence of
// its own for a missing member answers it even when another failure comes first.
func TestWireValidation_ASentenceNamesTheFirstMissingMember(t *testing.T) {
	assert.Equal(t, "The parameter B must be provided and must not be blank.", emulator.WireMissingAfterInvalidForTest())
}
