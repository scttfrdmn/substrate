package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for #844 Tier 1a: the Classic Load Balancer API is routed by the `Version` a Query request
// carries.
//
// Every assertion here is over the wire, because the defect was a routing decision and the routing
// is made from the request body. And every one of the three operations is asserted in **both**
// directions — the classic call answers the classic shape, and the ELBv2 call of the same action
// name answers exactly what it answered before — because a discriminator that gets either direction
// wrong is worse than none: the ELBv2 half is what substrate's own fixtures and recorded event logs
// replay through.

// elbClassicVersion is the Query `Version` that routes the classic API.
const elbClassicVersion = "2012-06-01"

// elbClassicListenerParams is one `Listeners.member.N` in the query-protocol shape a classic create
// carries, so the tests below spell the nesting once.
func elbClassicListenerParams(index int, protocol string, lbPort, instancePort int) map[string]string {
	prefix := "Listeners.member." + strconv.Itoa(index) + "."
	return map[string]string{
		prefix + "Protocol":         protocol,
		prefix + "LoadBalancerPort": strconv.Itoa(lbPort),
		prefix + "InstancePort":     strconv.Itoa(instancePort),
	}
}

// elbClassicCreateParams is a well-formed classic `CreateLoadBalancer`, with one HTTP listener.
func elbClassicCreateParams(name string, extra map[string]string) map[string]string {
	params := map[string]string{
		"Action":           "CreateLoadBalancer",
		"Version":          elbClassicVersion,
		"LoadBalancerName": name,
	}
	for k, v := range elbClassicListenerParams(1, "HTTP", 80, 8080) {
		params[k] = v
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// elbClassicCreateResult is the whole of the operation's published Response Elements.
type elbClassicCreateResult struct {
	Result struct {
		DNSName string `xml:"DNSName"`
	} `xml:"CreateLoadBalancerResult"`
}

// elbClassicDescribeResult is the published `DescribeLoadBalancers` output, including the
// `ListenerDescriptions` wrapper each listener is carried inside.
type elbClassicDescribeResult struct {
	Result struct {
		Descriptions []struct {
			LoadBalancerName     string `xml:"LoadBalancerName"`
			DNSName              string `xml:"DNSName"`
			Scheme               string `xml:"Scheme"`
			VPCId                string `xml:"VPCId"`
			CreatedTime          string `xml:"CreatedTime"`
			ListenerDescriptions []struct {
				Listener struct {
					Protocol         string `xml:"Protocol"`
					LoadBalancerPort int    `xml:"LoadBalancerPort"`
					InstanceProtocol string `xml:"InstanceProtocol"`
					InstancePort     int    `xml:"InstancePort"`
					SSLCertificateID string `xml:"SSLCertificateId"`
				} `xml:"Listener"`
			} `xml:"ListenerDescriptions>member"`
			AvailabilityZones []string `xml:"AvailabilityZones>member"`
			Subnets           []string `xml:"Subnets>member"`
			SecurityGroups    []string `xml:"SecurityGroups>member"`
		} `xml:"LoadBalancerDescriptions>member"`
		NextMarker string `xml:"NextMarker"`
	} `xml:"DescribeLoadBalancersResult"`
}

// elbClassicCreate creates a classic load balancer and returns its DNS name.
func elbClassicCreate(t *testing.T, baseURL, name string, extra map[string]string) string {
	t.Helper()
	resp := elbRequest(t, baseURL, elbClassicCreateParams(name, extra))
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "classic CreateLoadBalancer %s", name)
	var out elbClassicCreateResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.Result.DNSName, "classic CreateLoadBalancer answers a DNSName")
	return out.Result.DNSName
}

// elbClassicDescribe runs a classic DescribeLoadBalancers and decodes it.
func elbClassicDescribe(t *testing.T, baseURL string, extra map[string]string) elbClassicDescribeResult {
	t.Helper()
	params := map[string]string{"Action": "DescribeLoadBalancers", "Version": elbClassicVersion}
	for k, v := range extra {
		params[k] = v
	}
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "classic DescribeLoadBalancers")
	var out elbClassicDescribeResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	return out
}

// elbClassicRawBody returns a response body verbatim, for the assertions about which elements are
// *absent*: a decode can only see the members its struct declares, and half of what the
// discriminator has to get right is that the other generation's members are not there.
func elbClassicRawBody(t *testing.T, baseURL string, params map[string]string) (int, string) {
	t.Helper()
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// TestELBClassic_CreateAnswersTheDNSNameAndNothingElse is the first of the three operations, and the
// shape is the assertion: `CreateLoadBalancer`'s published Response Elements are `DNSName` alone,
// where the ELBv2 operation of the same name answers the whole load balancer.
func TestELBClassic_CreateAnswersTheDNSNameAndNothingElse(t *testing.T) {
	ts := newELBTestServer(t)

	status, body := elbClassicRawBody(t, ts.URL, elbClassicCreateParams("classic-web", nil))
	require.Equal(t, http.StatusOK, status)

	assert.Contains(t, body, "<CreateLoadBalancerResult>")
	assert.Contains(t, body, "<DNSName>classic-web-")
	assert.Contains(t, body, elbClassicNamespace,
		"a classic response carries the 2012-06-01 document namespace")
	// The ELBv2 members, which are what a classic caller got before the discriminator.
	assert.NotContains(t, body, "<LoadBalancers>")
	assert.NotContains(t, body, "<LoadBalancerArn>")
	assert.NotContains(t, body, "<Type>")
}

// elbClassicNamespace is the namespace a classic response must carry, asserted rather than assumed
// because a consumer's XML parser reads it and because ELBv2's own constant spells the scheme
// differently.
const elbClassicNamespace = `xmlns="http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/"`

// TestELBClassic_DescribeReportsTheLoadBalancerDescriptionShape is the operation whose ELBv2 answer
// was the silent wrong one.
//
// Both generations wrap the result in `DescribeLoadBalancersResult`, so botocore found the wrapper it
// wanted and decoded an **empty list** of `LoadBalancerDescriptions` — no error anywhere, just a
// consumer told it owns no classic load balancers. So the members are asserted individually: the
// result member's name, the `ListenerDescriptions` wrapper around each listener, and the absence of
// ELBv2's `LoadBalancers`.
func TestELBClassic_DescribeReportsTheLoadBalancerDescriptionShape(t *testing.T) {
	ts := newELBTestServer(t)
	dnsName := elbClassicCreate(t, ts.URL, "classic-web", map[string]string{
		"AvailabilityZones.member.1":          "us-east-1a",
		"AvailabilityZones.member.2":          "us-east-1b",
		"SecurityGroups.member.1":             "sg-0123456789abcdef0",
		"Listeners.member.2.Protocol":         "HTTPS",
		"Listeners.member.2.LoadBalancerPort": "443",
		"Listeners.member.2.InstancePort":     "8443",
		"Listeners.member.2.InstanceProtocol": "HTTP",
		"Listeners.member.2.SSLCertificateId": "arn:aws:iam::123456789012:server-certificate/c",
		"Tags.member.1.Key":                   "env",
		"Tags.member.1.Value":                 "prod",
	})

	out := elbClassicDescribe(t, ts.URL, nil)
	require.Len(t, out.Result.Descriptions, 1)
	got := out.Result.Descriptions[0]

	assert.Equal(t, "classic-web", got.LoadBalancerName)
	assert.Equal(t, dnsName, got.DNSName, "the create's DNSName is the one the describe reports")
	assert.Equal(t, "internet-facing", got.Scheme, "the published default")
	assert.Equal(t, []string{"us-east-1a", "us-east-1b"}, got.AvailabilityZones)
	assert.Equal(t, []string{"sg-0123456789abcdef0"}, got.SecurityGroups)
	assert.NotEmpty(t, got.CreatedTime)
	// No subnets were named, so there is no VPC to report and the member is absent rather than
	// carrying a guess.
	assert.Empty(t, got.VPCId)
	assert.Empty(t, got.Subnets)

	require.Len(t, got.ListenerDescriptions, 2)
	first := got.ListenerDescriptions[0].Listener
	assert.Equal(t, "HTTP", first.Protocol)
	assert.Equal(t, 80, first.LoadBalancerPort)
	assert.Equal(t, 8080, first.InstancePort)
	// InstanceProtocol was not sent for this listener, so it is not reported: the value on the wire
	// is the value the request set.
	assert.Empty(t, first.InstanceProtocol)

	second := got.ListenerDescriptions[1].Listener
	assert.Equal(t, "HTTPS", second.Protocol)
	assert.Equal(t, 443, second.LoadBalancerPort)
	assert.Equal(t, 8443, second.InstancePort)
	assert.Equal(t, "HTTP", second.InstanceProtocol)
	assert.Equal(t, "arn:aws:iam::123456789012:server-certificate/c", second.SSLCertificateID)

	_, body := elbClassicRawBody(t, ts.URL,
		map[string]string{"Action": "DescribeLoadBalancers", "Version": elbClassicVersion})
	assert.Contains(t, body, "<LoadBalancerDescriptions>")
	assert.Contains(t, body, "<ListenerDescriptions>")
	assert.Contains(t, body, "<PolicyNames></PolicyNames>",
		"AWS publishes an empty list rather than an absent member: "+
			`"The policies. If there are no policies enabled, the list is empty."`)
	assert.NotContains(t, body, "<LoadBalancers>")
	// Substrate's own bookkeeping, and the ARN no classic response publishes (#756).
	assert.NotContains(t, body, "<ARN>")
	assert.NotContains(t, body, "<AccountID>")
	assert.NotContains(t, body, "<Tags>")
}

// TestELBClassic_TheInternalSchemePrefixesTheDNSName pins a published detail a consumer reads the
// scheme out of.
//
// AWS's sample for an internal load balancer is
// `internal-my-internal-loadbalancer-1234567890.us-east-1.elb.amazonaws.com` — the `internal-`
// prefix is published, not inferred.
func TestELBClassic_TheInternalSchemePrefixesTheDNSName(t *testing.T) {
	ts := newELBTestServer(t)
	dnsName := elbClassicCreate(t, ts.URL, "classic-internal", map[string]string{
		"Scheme":           "internal",
		"Subnets.member.1": "subnet-0123456789abcdef0",
	})
	assert.True(t, strings.HasPrefix(dnsName, "internal-classic-internal-"),
		"an internal load balancer's DNS name carries the published internal- prefix, got %q", dnsName)

	out := elbClassicDescribe(t, ts.URL, nil)
	require.Len(t, out.Result.Descriptions, 1)
	assert.Equal(t, "internal", out.Result.Descriptions[0].Scheme)
	assert.Equal(t, []string{"subnet-0123456789abcdef0"}, out.Result.Descriptions[0].Subnets)
}

// TestELBClassic_DeleteIsIdempotentAndNamesItsLoadBalancer is the third operation, and the one whose
// ELBv2 answer refused a well-formed request.
//
// AWS publishes **no** operation-specific errors for classic `DeleteLoadBalancer` and states the rule
// outright — "If the load balancer does not exist or has already been deleted, the call to
// DeleteLoadBalancer still succeeds." A cleanup path that deletes twice met `ValidationError` on a
// missing `LoadBalancerArn` instead, because ELBv2's handler was answering.
func TestELBClassic_DeleteIsIdempotentAndNamesItsLoadBalancer(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "classic-doomed", nil)

	del := map[string]string{
		"Action": "DeleteLoadBalancer", "Version": elbClassicVersion,
		"LoadBalancerName": "classic-doomed",
	}
	for _, attempt := range []string{"first", "second"} {
		status, body := elbClassicRawBody(t, ts.URL, del)
		assert.Equal(t, http.StatusOK, status, "%s delete", attempt)
		// The empty result element, for the reason [elbEmptyOKResponse] documents: botocore looks
		// the wrapper up by name and raises KeyError when it is absent.
		assert.Contains(t, body, "<DeleteLoadBalancerResult></DeleteLoadBalancerResult>", attempt)
	}

	assert.Empty(t, elbClassicDescribe(t, ts.URL, nil).Result.Descriptions,
		"the delete removed the record")

	// A load balancer that never existed is the same success.
	status, _ := elbClassicRawBody(t, ts.URL, map[string]string{
		"Action": "DeleteLoadBalancer", "Version": elbClassicVersion,
		"LoadBalancerName": "never-existed",
	})
	assert.Equal(t, http.StatusOK, status,
		"deleting a load balancer that does not exist still succeeds")

	// The one refusal the operation has, and it is not from its own Errors section — that section is
	// empty. An absent `LoadBalancerName` names nothing at all, so the idempotent success has nothing
	// to be idempotent about, and the code is the Common Errors one [elbClassicValidationError]
	// documents.
	missing := elbRequest(t, ts.URL, map[string]string{
		"Action": "DeleteLoadBalancer", "Version": elbClassicVersion,
	})
	defer missing.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, missing.StatusCode)
	assert.Equal(t, "ValidationError", elbErrorCode(t, missing))
}

// TestELBClassic_TheELBv2FormOfTheSameActionIsUntouched is the other direction of the
// discriminator, and the one that matters for everything substrate already answered.
//
// Substrate's own fixtures and its recorded event logs are full of hand-built ELBv2 Query bodies —
// some carrying `Version=2015-12-01`, most carrying no version at all — and a replay of one must
// decode exactly as it did before #844. So both spellings are asserted, and the classic members must
// be absent from both.
func TestELBClassic_TheELBv2FormOfTheSameActionIsUntouched(t *testing.T) {
	for _, version := range []string{"", "2015-12-01"} {
		name := "explicit-version"
		if version == "" {
			name = "no-version"
		}
		t.Run(name, func(t *testing.T) {
			ts := newELBTestServer(t)
			create := map[string]string{
				"Action": "CreateLoadBalancer", "Name": "v2-web", "Type": "application",
			}
			describe := map[string]string{"Action": "DescribeLoadBalancers"}
			if version != "" {
				create["Version"] = version
				describe["Version"] = version
			}

			status, body := elbClassicRawBody(t, ts.URL, create)
			require.Equal(t, http.StatusOK, status)
			assert.Contains(t, body, "<LoadBalancers>",
				"an ELBv2 create answers the load balancer it made")
			assert.Contains(t, body, ":loadbalancer/app/v2-web/",
				"the ARN keeps ELBv2's three segments")
			assert.NotContains(t, body, "2012-06-01")

			status, body = elbClassicRawBody(t, ts.URL, describe)
			require.Equal(t, http.StatusOK, status)
			assert.Contains(t, body, "<LoadBalancers>")
			assert.NotContains(t, body, "<LoadBalancerDescriptions>")

			// And the ELBv2 delete still takes an ARN, which is the member the classic handler does
			// not read.
			status, _ = elbClassicRawBody(t, ts.URL, map[string]string{
				"Action": "DeleteLoadBalancer", "LoadBalancerName": "v2-web",
			})
			assert.Equal(t, http.StatusBadRequest, status,
				"ELBv2's DeleteLoadBalancer requires LoadBalancerArn, whichever version is absent")
		})
	}
}

// TestELBClassic_BothGenerationsCanHoldOneName is why the classic records live under a state-key
// prefix of their own.
//
// AWS scopes a classic name and an ELBv2 name to separate namespaces — each generation's
// `CreateLoadBalancer` publishes its duplicate-name refusal against its own generation only — so
// `web` can be both, and each generation's describe must report only its own.
func TestELBClassic_BothGenerationsCanHoldOneName(t *testing.T) {
	ts := newELBTestServer(t)
	v2ARN := elbCreateLB(t, ts.URL, "web", nil)
	elbClassicCreate(t, ts.URL, "web", nil)

	classic := elbClassicDescribe(t, ts.URL, nil)
	require.Len(t, classic.Result.Descriptions, 1,
		"the classic describe reports the classic load balancer and not the ELBv2 one")
	assert.Equal(t, "web", classic.Result.Descriptions[0].LoadBalancerName)

	_, body := elbClassicRawBody(t, ts.URL, map[string]string{"Action": "DescribeLoadBalancers"})
	assert.Contains(t, body, v2ARN, "the ELBv2 describe reports the ELBv2 load balancer")
	assert.Equal(t, 1, strings.Count(body, "<member>"),
		"and reports only it: the classic record is in another key space")
}

// TestELBClassic_DescribeRefusesANameThatNamesNothing is a published code the ELBv2 handler of the
// same action does not answer.
//
// Classic `DescribeLoadBalancers` publishes `LoadBalancerNotFound` at 400 for a name in
// `LoadBalancerNames.member.N` that names nothing. ELBv2's publishes the code for its own `Names`
// and substrate's handler still filters silently there; this one refuses, because the classic page is
// the one being implemented.
func TestELBClassic_DescribeRefusesANameThatNamesNothing(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "classic-web", nil)

	out := elbClassicDescribe(t, ts.URL, map[string]string{"LoadBalancerNames.member.1": "classic-web"})
	require.Len(t, out.Result.Descriptions, 1, "a name that names something is reported")

	resp := elbRequest(t, ts.URL, map[string]string{
		"Action": "DescribeLoadBalancers", "Version": elbClassicVersion,
		"LoadBalancerNames.member.1": "classic-web",
		"LoadBalancerNames.member.2": "not-a-load-balancer",
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "LoadBalancerNotFound", elbErrorCode(t, resp))
}

// TestELBClassic_DescribePagesByItsPublishedPageSize covers the cursor the operation publishes.
//
// `PageSize` is "a number from 1 to 400. The default is 400" and the response carries `NextMarker`,
// so a consumer's paging loop is a real loop. Without the cursor a `PageSize` of 1 returned
// everything, which is a wrong answer rather than a missing feature.
func TestELBClassic_DescribePagesByItsPublishedPageSize(t *testing.T) {
	ts := newELBTestServer(t)
	for _, name := range []string{"classic-a", "classic-b", "classic-c"} {
		elbClassicCreate(t, ts.URL, name, nil)
	}

	var seen []string
	marker := ""
	for page := 0; page < 4; page++ {
		extra := map[string]string{"PageSize": "2"}
		if marker != "" {
			extra["Marker"] = marker
		}
		out := elbClassicDescribe(t, ts.URL, extra)
		for _, d := range out.Result.Descriptions {
			seen = append(seen, d.LoadBalancerName)
		}
		marker = out.Result.NextMarker
		if marker == "" {
			break
		}
	}
	assert.Equal(t, []string{"classic-a", "classic-b", "classic-c"}, seen,
		"the whole listing comes back across the pages, in key order, once each")
	assert.Empty(t, marker, "the last page carries no marker")
}

// TestELBClassic_Refusals is every refusal the three operations answer, each with the code and status
// its own page publishes.
//
// A table because the codes are the point: eleven of the twelve errors `CreateLoadBalancer` publishes
// are 400 and exactly one — `InvalidConfigurationRequest` — is 409, and a handler that answered one
// status for everything would be the defect #1072 records in Step Functions.
func TestELBClassic_Refusals(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string]string
		wantCode   string
		wantStatus int
	}{{
		name:       "no name",
		params:     map[string]string{"LoadBalancerName": ""},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		// "must have a maximum of 32 characters"
		name:       "a name over 32 characters",
		params:     map[string]string{"LoadBalancerName": strings.Repeat("a", 33)},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		// "cannot begin or end with a hyphen"
		name:       "a name beginning with a hyphen",
		params:     map[string]string{"LoadBalancerName": "-web"},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		name:       "a name ending with a hyphen",
		params:     map[string]string{"LoadBalancerName": "web-"},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		// "must contain only alphanumeric characters or hyphens"
		name:       "a name carrying an underscore",
		params:     map[string]string{"LoadBalancerName": "web_1"},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		name: "no listeners",
		params: map[string]string{
			"Listeners.member.1.Protocol":         "",
			"Listeners.member.1.LoadBalancerPort": "",
			"Listeners.member.1.InstancePort":     "",
		},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		// An index carrying some member but not all of them is validated rather than read as the
		// terminator of the list.
		name:       "a listener with no protocol",
		params:     map[string]string{"Listeners.member.1.Protocol": ""},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		name:       "a listener with no instance port",
		params:     map[string]string{"Listeners.member.1.InstancePort": ""},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		// "Valid Range: Minimum value of 1. Maximum value of 65535."
		name:       "an instance port above the published range",
		params:     map[string]string{"Listeners.member.1.InstancePort": "65536"},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		name:       "an instance port below the published range",
		params:     map[string]string{"Listeners.member.1.InstancePort": "0"},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		name:       "a port that is not a number",
		params:     map[string]string{"Listeners.member.1.LoadBalancerPort": "eighty"},
		wantCode:   "ValidationError",
		wantStatus: http.StatusBadRequest,
	}, {
		// The four protocols the Listener type publishes are "HTTP, HTTPS, TCP, or SSL".
		name:       "a protocol outside the published four",
		params:     map[string]string{"Listeners.member.1.Protocol": "UDP"},
		wantCode:   "UnsupportedProtocol",
		wantStatus: http.StatusBadRequest,
	}, {
		name:       "an instance protocol outside the published four",
		params:     map[string]string{"Listeners.member.1.InstanceProtocol": "GENEVE"},
		wantCode:   "UnsupportedProtocol",
		wantStatus: http.StatusBadRequest,
	}, {
		name:       "a scheme outside the published two",
		params:     map[string]string{"Scheme": "public"},
		wantCode:   "InvalidScheme",
		wantStatus: http.StatusBadRequest,
	}, {
		// The one non-400 of the twelve, and the only published code a same-port pair can answer:
		// DuplicateListener is on CreateLoadBalancerListeners and on no page this operation has.
		name: "two listeners on one load balancer port",
		params: map[string]string{
			"Listeners.member.2.Protocol":         "TCP",
			"Listeners.member.2.LoadBalancerPort": "80",
			"Listeners.member.2.InstancePort":     "9090",
		},
		wantCode:   "InvalidConfigurationRequest",
		wantStatus: http.StatusConflict,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServer(t)
			resp := elbRequest(t, ts.URL, elbClassicCreateParams("classic-web", tt.params))
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Equal(t, tt.wantCode, elbErrorCode(t, resp))

			// Nothing was written: a refused create must leave no load balancer behind.
			assert.Empty(t, elbClassicDescribe(t, ts.URL, nil).Result.Descriptions)
		})
	}
}

// TestELBClassic_ARepeatedNameIsRefused is the uniqueness half of the name rule, which needs state
// and has its own published code.
func TestELBClassic_ARepeatedNameIsRefused(t *testing.T) {
	ts := newELBTestServer(t)
	dnsName := elbClassicCreate(t, ts.URL, "classic-web", nil)

	resp := elbRequest(t, ts.URL, elbClassicCreateParams("classic-web", nil))
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "DuplicateLoadBalancerName", elbErrorCode(t, resp))

	// And the first one is untouched, which is what makes the refusal a refusal rather than a
	// silent replace.
	out := elbClassicDescribe(t, ts.URL, nil)
	require.Len(t, out.Result.Descriptions, 1)
	assert.Equal(t, dnsName, out.Result.Descriptions[0].DNSName)
}

// TestELBClassic_DescribeRefusesABadCursor covers the two cursor members' own refusals.
func TestELBClassic_DescribeRefusesABadCursor(t *testing.T) {
	tests := []struct {
		name  string
		extra map[string]string
	}{
		{name: "a page size above the published maximum", extra: map[string]string{"PageSize": "401"}},
		{name: "a page size below the published minimum", extra: map[string]string{"PageSize": "0"}},
		{name: "a page size that is not a number", extra: map[string]string{"PageSize": "all"}},
		{name: "a marker the service never issued", extra: map[string]string{"Marker": "page-two"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServer(t)
			elbClassicCreate(t, ts.URL, "classic-web", nil)

			params := map[string]string{
				"Action": "DescribeLoadBalancers", "Version": elbClassicVersion,
			}
			for k, v := range tt.extra {
				params[k] = v
			}
			resp := elbRequest(t, ts.URL, params)
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
		})
	}
}

// TestELBClassic_AnActionOnlyTheClassicAPIPublishesIsStillUnrouted states Tier 1a's boundary, so
// that the three routed operations are not read as the whole API.
//
// The version discriminates three shared action names. An action only the classic API publishes is
// an action substrate does not route, and it answers as one — the honest answer, and the one that
// keeps a consumer from reading a 200 as "substrate models classic instance registration".
func TestELBClassic_AnActionOnlyTheClassicAPIPublishesIsStillUnrouted(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "classic-web", nil)

	for _, action := range []string{
		"RegisterInstancesWithLoadBalancer",
		"CreateLoadBalancerListeners",
		"ConfigureHealthCheck",
		"DescribeLoadBalancerPolicies",
	} {
		resp := elbRequest(t, ts.URL, map[string]string{
			"Action": action, "Version": elbClassicVersion, "LoadBalancerName": "classic-web",
		})
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, action)
		assert.Equal(t, "InvalidAction", elbErrorCode(t, resp), action)
		require.NoError(t, resp.Body.Close())
	}
}

// The classic records' state namespace and the two key prefixes they live under, duplicated from
// `elb_classic.go` so [elbClassicFaultState] can fault one classic write and nothing else. Faulting
// the whole `elb` namespace would break the bookkeeping every ELB request does, and the request
// would then fail for a reason other than the one under test — the scoping
// `elbLimitFaultState` does for the account-limit seed, for the same reason. The record prefix ends
// in a colon and the index prefix does not, which is what keeps the record write and the name-index
// write separately faultable.
const (
	elbClassicStateNamespace = "elb"
	elbClassicRecordPrefix   = "classic_lb:"
	elbClassicIndexPrefix    = "classic_lb_names"
)

// elbClassicFaultState wraps a working StateManager and injects one store failure on the classic
// records.
//
// Every field is set before the server starts and never written afterwards: each request is served
// on its own goroutine, so a field armed between two requests is a race the detector flags. That is
// why "a record the listing cannot read" is spelled as a *name* rather than as a flag armed after
// the create — a create for `vanishedName` sees exactly the absent record every create sees, so the
// fault can be in place from the start and still only bite the listing.
type elbClassicFaultState struct {
	emulator.StateManager
	recordGetErr error
	recordPutErr error
	indexPutErr  error
	deleteErr    error
	listErr      error
	vanishedName string
}

// isClassicRecord reports whether a key names a classic load balancer's own record, as against the
// name index or any of the four ELBv2 kinds.
func (m *elbClassicFaultState) isClassicRecord(namespace, key string) bool {
	return namespace == elbClassicStateNamespace && strings.HasPrefix(key, elbClassicRecordPrefix)
}

func (m *elbClassicFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.isClassicRecord(namespace, key) {
		if m.recordGetErr != nil {
			return nil, m.recordGetErr
		}
		if m.vanishedName != "" && strings.HasSuffix(key, "/"+m.vanishedName) {
			return nil, nil
		}
	}
	return m.StateManager.Get(ctx, namespace, key)
}

func (m *elbClassicFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if namespace == elbClassicStateNamespace {
		if m.indexPutErr != nil && strings.HasPrefix(key, elbClassicIndexPrefix) {
			return m.indexPutErr
		}
		if m.recordPutErr != nil && strings.HasPrefix(key, elbClassicRecordPrefix) {
			return m.recordPutErr
		}
	}
	return m.StateManager.Put(ctx, namespace, key, value)
}

func (m *elbClassicFaultState) Delete(ctx context.Context, namespace, key string) error {
	if m.deleteErr != nil && m.isClassicRecord(namespace, key) {
		return m.deleteErr
	}
	return m.StateManager.Delete(ctx, namespace, key)
}

func (m *elbClassicFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if m.listErr != nil && namespace == elbClassicStateNamespace &&
		strings.HasPrefix(prefix, elbClassicRecordPrefix) {
		return nil, m.listErr
	}
	return m.StateManager.List(ctx, namespace, prefix)
}

// TestELBClassic_AStoreFailureIsAnsweredAsOne asserts that each of the three operations' store
// reads and writes propagates a failure rather than answering a plausible 200.
//
// This matters more for a newly routed operation than for an established one, because the answer a
// consumer would otherwise get is indistinguishable from success: a `CreateLoadBalancer` whose
// record could not be written still has a DNS name to report, and a `DescribeLoadBalancers` whose
// listing could not be read still has an empty list to report — which is exactly the silent wrong
// answer #844 exists to remove, arriving by a different route. A 5xx is a signal a consumer's retry
// loop can act on; a wrong-but-plausible 200 is not.
func TestELBClassic_AStoreFailureIsAnsweredAsOne(t *testing.T) {
	boom := errors.New("state store unavailable")

	tests := []struct {
		name  string
		state *elbClassicFaultState
		// forbidden is the result element the failed operation must not have answered.
		forbidden string
		do        func(t *testing.T, baseURL string) (int, string)
	}{
		{
			name:      "create cannot read the name it would take",
			state:     &elbClassicFaultState{recordGetErr: boom},
			forbidden: "<CreateLoadBalancerResult>",
			do: func(t *testing.T, baseURL string) (int, string) {
				t.Helper()
				return elbClassicRawBody(t, baseURL, elbClassicCreateParams("classic-web", nil))
			},
		},
		{
			name:      "create cannot write the record",
			state:     &elbClassicFaultState{recordPutErr: boom},
			forbidden: "<DNSName>",
			do: func(t *testing.T, baseURL string) (int, string) {
				t.Helper()
				return elbClassicRawBody(t, baseURL, elbClassicCreateParams("classic-web", nil))
			},
		},
		{
			// The record is written and the name index is not, so this is the one row where the
			// refusal is not the whole story — but a DNS name answered over a half-written create
			// would be worse, because the caller would have no reason to look.
			name:      "create cannot write the name index",
			state:     &elbClassicFaultState{indexPutErr: boom},
			forbidden: "<DNSName>",
			do: func(t *testing.T, baseURL string) (int, string) {
				t.Helper()
				return elbClassicRawBody(t, baseURL, elbClassicCreateParams("classic-web", nil))
			},
		},
		{
			name:      "describe cannot list the records",
			state:     &elbClassicFaultState{listErr: boom},
			forbidden: "<DescribeLoadBalancersResult>",
			do: func(t *testing.T, baseURL string) (int, string) {
				t.Helper()
				return elbClassicRawBody(t, baseURL, map[string]string{
					"Action": "DescribeLoadBalancers", "Version": elbClassicVersion,
				})
			},
		},
		{
			// A delete that cannot read the record cannot know whether it was there, and the
			// published success for an absent load balancer must not absorb that: "it was already
			// gone" and "I could not look" are different facts.
			name:      "delete cannot read the record",
			state:     &elbClassicFaultState{recordGetErr: boom},
			forbidden: "<DeleteLoadBalancerResult>",
			do: func(t *testing.T, baseURL string) (int, string) {
				t.Helper()
				return elbClassicRawBody(t, baseURL, map[string]string{
					"Action": "DeleteLoadBalancer", "Version": elbClassicVersion,
					"LoadBalancerName": "classic-web",
				})
			},
		},
		{
			name:      "delete cannot remove the record",
			state:     &elbClassicFaultState{deleteErr: boom},
			forbidden: "<DeleteLoadBalancerResult>",
			do: func(t *testing.T, baseURL string) (int, string) {
				t.Helper()
				elbClassicCreate(t, baseURL, "classic-web", nil)
				return elbClassicRawBody(t, baseURL, map[string]string{
					"Action": "DeleteLoadBalancer", "Version": elbClassicVersion,
					"LoadBalancerName": "classic-web",
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.StateManager = emulator.NewMemoryStateManager()
			ts := newELBTestServerWithState(t, tt.state)

			status, body := tt.do(t, ts.URL)
			assert.GreaterOrEqual(t, status, http.StatusInternalServerError,
				"a store failure must not be answered as a success; body was %s", body)
			assert.NotContains(t, body, tt.forbidden,
				"no result may be reported by an operation whose store call failed")
		})
	}
}

// TestELBClassic_ARecordTheListingCannotUseIsOmittedNotFatal asserts the other half of the store
// rule: a *listing* skips the record it cannot use and reports the rest.
//
// The two are not in tension. A failure of the `List` itself is a failure of the whole question the
// caller asked, so it propagates (asserted above); one unusable record among several is not, and
// failing the call would hide every healthy load balancer behind one bad key — which is worse than
// the omission, and is why [ELBPlugin.loadClassicLoadBalancers] continues past both cases. The
// cases are distinct on purpose: a key the store lists and then cannot produce, and a record whose
// bytes are not a classic load balancer — state written by hand, or by an older substrate.
func TestELBClassic_ARecordTheListingCannotUseIsOmittedNotFatal(t *testing.T) {
	t.Run("a key the store lists and cannot produce", func(t *testing.T) {
		state := &elbClassicFaultState{
			StateManager: emulator.NewMemoryStateManager(), vanishedName: "vanished",
		}
		ts := newELBTestServerWithState(t, state)
		elbClassicCreate(t, ts.URL, "intact", nil)
		elbClassicCreate(t, ts.URL, "vanished", nil)

		out := elbClassicDescribe(t, ts.URL, nil)
		require.Len(t, out.Result.Descriptions, 1, "the readable record is still reported")
		assert.Equal(t, "intact", out.Result.Descriptions[0].LoadBalancerName)
	})

	t.Run("a record whose bytes are not a classic load balancer", func(t *testing.T) {
		state := emulator.NewMemoryStateManager()
		ts := newELBTestServerWithState(t, state)
		elbClassicCreate(t, ts.URL, "intact", nil)

		// The scope is read back off the record the create wrote rather than assumed, so the key
		// this seeds is the one the listing walks whatever account the server attributes to.
		keys, err := state.List(t.Context(), elbClassicStateNamespace, elbClassicRecordPrefix)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		scope := strings.TrimSuffix(keys[0], "/intact")
		require.NoError(t, state.Put(t.Context(), elbClassicStateNamespace,
			scope+"/undecodable", []byte("{not-json")))

		out := elbClassicDescribe(t, ts.URL, nil)
		require.Len(t, out.Result.Descriptions, 1, "one unreadable record does not hide the others")
		assert.Equal(t, "intact", out.Result.Descriptions[0].LoadBalancerName)
	})
}

// TestELBClassic_CreateTimeTagsAreKeptAndTheirDuplicateKeyRefused asserts that the tags a classic
// create carries reach the record, and that the operation's published `DuplicateTagKeys` is answered
// before anything is written.
//
// The first half is the defect #1087 records for two other services — a create-time tag set parsed
// and dropped — asserted here at the operation's birth rather than left to be found later. The
// second is why the validation runs before the write: a create carrying a tag it cannot legally
// apply must leave no load balancer behind, so the refusal and the absence are one assertion.
func TestELBClassic_CreateTimeTagsAreKeptAndTheirDuplicateKeyRefused(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	elbClassicCreate(t, ts.URL, "tagged", map[string]string{
		"Tags.member.1.Key": "department", "Tags.member.1.Value": "digital-media",
	})
	tagged := "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount + ":loadbalancer/tagged"
	assert.Equal(t, map[string]string{"department": "digital-media"}, getResourcesTags(t, ts, tagged),
		"a tag supplied to the create is the tag the resource carries")

	resp := elbRequest(t, ts.URL, elbClassicCreateParams("twice-keyed", map[string]string{
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
		"Tags.member.2.Key": "env", "Tags.member.2.Value": "staging",
	}))
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "DuplicateTagKeys", elbErrorCode(t, resp))

	// And nothing was written: the name the refused create asked for names nothing, which the
	// operation's own `LoadBalancerNotFound` is how a caller can see.
	absent := elbRequest(t, ts.URL, map[string]string{
		"Action": "DescribeLoadBalancers", "Version": elbClassicVersion,
		"LoadBalancerNames.member.1": "twice-keyed",
	})
	defer absent.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, absent.StatusCode,
		"a refused create left no load balancer behind")
	assert.Equal(t, "LoadBalancerNotFound", elbErrorCode(t, absent))
}

// TestELBClassic_ARecordTheTaggingAPICannotDecodeReadsAsAbsent asserts that the classic arm of the
// tagging resolvers answers "not there" for a record whose bytes it cannot decode.
//
// That is the same reading [TestELBClassic_ARecordTheListingCannotUseIsOmittedNotFatal] pins for the
// listing, arriving through the other caller: the resolver's scan skips a record it cannot decode
// and finishes without a match, so the answer is the absent-resource code rather than a 5xx. A store
// that genuinely fails is still an error — `elbResolveTaggedResourceOfKind` says so and answers one
// — and these two must not be conflated, because a broken backend is not an absent resource.
func TestELBClassic_ARecordTheTaggingAPICannotDecodeReadsAsAbsent(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	elbClassicCreate(t, ts.URL, "undecodable", nil)

	key := elbClassicRecordPrefix + taggingTestAccount + "/us-east-1/undecodable"
	require.NoError(t, ts.StateManager().Put(t.Context(), elbClassicStateNamespace, key,
		[]byte("{not-json")))

	arn := "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount +
		":loadbalancer/undecodable"
	got, ok := tagResourcesFailures(t, ts, "TagResources", arn)[arn]
	require.True(t, ok, "TagResources reported no failure for an undecodable classic record")
	assert.Equal(t, "InvalidParameterException", got.ErrorCode)
	assert.Equal(t, 400, got.StatusCode)
}
