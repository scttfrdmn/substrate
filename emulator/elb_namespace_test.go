package emulator_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// elbV2Namespace is the document namespace an ELBv2 (2015-12-01) response must carry.
//
// Asserted rather than assumed, for the reason #1147 records: substrate spelled it `https://` for
// every routed ELBv2 operation, and no test noticed because botocore resolves output shapes by
// element name and ignores the namespace entirely. Only a consumer that compares the attribute as
// a string — an XSD step, a prefix-bound XPath, a golden file recorded from AWS — sees it, so the
// wire is where it has to be pinned.
const elbV2Namespace = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"

// elbReadBody sends one ELBv2 request and returns the raw body, asserting a 200.
func elbReadBody(t *testing.T, baseURL string, params map[string]string) string {
	t.Helper()
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read elb response body")
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	return string(body)
}

// TestELB_ResponsesCarryThePublishedDocumentNamespace pins the `xmlns` attribute of both
// generations on the wire.
//
// Three shapes, because they reach the attribute by three different routes: a result-bearing
// response builds it from [elbXMLNS] in the handler, an empty-result response builds it in
// elbEmptyOKResponse, and a classic response builds it from a second constant. A one-character
// regression in either constant would leave two of the three passing.
//
// The `https://` assertions are negative rather than absent: that spelling is the defect #1147
// fixed, and it is the one a plausible edit reintroduces.
func TestELB_ResponsesCarryThePublishedDocumentNamespace(t *testing.T) {
	ts := newELBTestServer(t)

	created := elbReadBody(t, ts.URL, map[string]string{
		"Action":           "CreateLoadBalancer",
		"Name":             "ns-alb",
		"Type":             "application",
		"Subnets.member.1": "subnet-abc123",
	})
	assert.Contains(t, created, `<CreateLoadBalancerResponse xmlns="`+elbV2Namespace+`">`,
		"a result-bearing ELBv2 response carries the 2015-12-01 document namespace")

	// An operation whose output shape declares no members still carries the namespace, and it is
	// built in a different place — elbEmptyOKResponse rather than the handler.
	deleted := elbReadBody(t, ts.URL, map[string]string{
		"Action":          "DeleteLoadBalancer",
		"LoadBalancerArn": elbARNFromCreateResponse(t, created),
	})
	assert.Contains(t, deleted, `<DeleteLoadBalancerResponse xmlns="`+elbV2Namespace+`">`,
		"an empty-result ELBv2 response carries the same namespace")

	classic := elbReadBody(t, ts.URL, map[string]string{
		"Action":                              "CreateLoadBalancer",
		"Version":                             "2012-06-01",
		"LoadBalancerName":                    "ns-classic",
		"Listeners.member.1.Protocol":         "HTTP",
		"Listeners.member.1.LoadBalancerPort": "80",
		"Listeners.member.1.InstancePort":     "8080",
		"AvailabilityZones.member.1":          "us-east-1a",
	})
	assert.Contains(t, classic, elbClassicNamespace,
		"a classic response carries the 2012-06-01 document namespace")

	for name, body := range map[string]string{
		"ELBv2 create": created, "ELBv2 delete": deleted, "classic create": classic,
	} {
		assert.NotContains(t, body, "https://elasticloadbalancing.amazonaws.com/doc/",
			"%s must not spell the document namespace https:// (#1147)", name)
	}
}

// elbARNFromCreateResponse pulls the minted ARN out of a CreateLoadBalancer body.
//
// A substring walk rather than an XML decode, because the point of the surrounding test is the raw
// bytes: decoding into a struct would make the test depend on the namespace it is asserting.
func elbARNFromCreateResponse(t *testing.T, body string) string {
	t.Helper()
	const openTag, closeTag = "<LoadBalancerArn>", "</LoadBalancerArn>"
	start := strings.Index(body, openTag)
	require.GreaterOrEqual(t, start, 0, "no LoadBalancerArn in %s", body)
	rest := body[start+len(openTag):]
	end := strings.Index(rest, closeTag)
	require.GreaterOrEqual(t, end, 0, "unterminated LoadBalancerArn in %s", body)
	return rest[:end]
}
