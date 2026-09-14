package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #755 and #858, both in createIPSet.
//
// #755: all four of Addresses, IPAddressVersion, Name and Scope are documented
// Required: Yes, and substrate defaulted three of them — so a request AWS rejects
// created an IP set whose scope and address family the caller never chose, then
// reported them back as though it had asked.
//
// The two error codes are AWS's, not chosen: an omitted required member answers
// ValidationError/400 from WAFv2's Common Error Types ("Check that all required
// parameters are included and that values are valid"), and a present-but-invalid
// value answers WAFInvalidParameterException/400, which CreateIPSet's own Errors
// section lists.
//
// The empty-Addresses case is the one that needed care rather than a check: AWS
// lists `"Addresses": []` among CreateIPSet's valid example specifications while
// marking `"Addresses": [""]` INVALID, so absent and empty are distinct and the
// nil-to-empty normalisation had to be removed rather than kept beside the
// refusal.
//
// #858: the plugin hardcoded the ARN's scope segment as "regional" where
// CloudFormation derived it, so one logical web ACL reported two different ARNs.

// wafv2CreateIPSetError sends a CreateIPSet body and returns the AWSError it was
// refused with, failing the test if the request succeeded instead.
func wafv2CreateIPSetError(
	t *testing.T, p *emulator.WAFv2Plugin, ctx *emulator.RequestContext, body map[string]any,
) *emulator.AWSError {
	t.Helper()
	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateIPSet", body))
	require.Error(t, err, "CreateIPSet must refuse %v; it answered %+v", body, resp)
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr, "the refusal must be an AWSError, not %v", err)
	return awsErr
}

// wafv2ValidIPSetBody is a request AWS accepts, so each test below changes exactly
// one thing about it and every refusal is attributable to that change.
func wafv2ValidIPSetBody() map[string]any {
	return map[string]any{
		"Name":             "allow-list",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{"192.0.2.44/32"},
	}
}

// TestWAFv2CreateIPSet_AnOmittedRequiredMemberIsRefused covers all four required
// members, each omitted on its own. IPAddressVersion is #755's title case, and the
// other three were either defaulted the same way or already refused.
func TestWAFv2CreateIPSet_AnOmittedRequiredMemberIsRefused(t *testing.T) {
	for _, member := range []string{"Name", "Scope", "IPAddressVersion", "Addresses"} {
		t.Run(member, func(t *testing.T) {
			p, ctx := setupWAFv2Plugin(t)
			body := wafv2ValidIPSetBody()
			delete(body, member)

			awsErr := wafv2CreateIPSetError(t, p, ctx, body)
			assert.Equal(t, "ValidationError", awsErr.Code,
				"an omitted required member is WAFv2's ValidationError, not an invalid value")
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
			assert.Contains(t, awsErr.Message, member,
				"the refusal must name the member that was missing")
		})
	}
}

// TestWAFv2CreateIPSet_AnEmptyBodyIsRefused is the degenerate case of the above:
// a request carrying nothing used to create an IP set named "" — no, it was
// refused for the name alone, and every other member was invented. It must now
// report a missing required member.
func TestWAFv2CreateIPSet_AnEmptyBodyIsRefused(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)
	awsErr := wafv2CreateIPSetError(t, p, ctx, map[string]any{})
	assert.Equal(t, "ValidationError", awsErr.Code)
	assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
}

// TestWAFv2CreateIPSet_AnInvalidValueIsRefused is the other code. Every case here
// supplies the member, so none of them can be reported as missing.
func TestWAFv2CreateIPSet_AnInvalidValueIsRefused(t *testing.T) {
	tests := []struct {
		name   string
		member string
		value  any
	}{
		// Valid Values: IPV4 | IPV6. "ipv4" is the case a caller most plausibly
		// sends, and the enum is documented uppercase.
		{"lowercase IP version", "IPAddressVersion", "ipv4"},
		{"unknown IP version", "IPAddressVersion", "IPV5"},
		// Valid Values: CLOUDFRONT | REGIONAL.
		{"lowercase scope", "Scope", "regional"},
		{"unknown scope", "Scope", "GLOBAL"},
		// Pattern: ^[\w\-]+$, length 1-128.
		{"name with a space", "Name", "allow list"},
		{"name with a slash", "Name", "allow/list"},
		{"name over 128 characters", "Name", strings.Repeat("a", 129)},
		// Addresses "must be specified using Classless Inter-Domain Routing (CIDR)
		// notation", and AWS "supports all IPv4 and IPv6 CIDR ranges except for /0".
		{"address with no prefix", "Addresses", []string{"192.0.2.44"}},
		{"address that is not an address", "Addresses", []string{"not-an-ip/32"}},
		{"empty address string", "Addresses", []string{""}},
		{"IPv4 /0", "Addresses", []string{"0.0.0.0/0"}},
		{"IPv6 /0", "Addresses", []string{"::/0"}},
		// One good address does not excuse a bad one.
		{"one valid and one invalid", "Addresses", []string{"192.0.2.44/32", "10.0.0.0/0"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, ctx := setupWAFv2Plugin(t)
			body := wafv2ValidIPSetBody()
			body[tc.member] = tc.value

			awsErr := wafv2CreateIPSetError(t, p, ctx, body)
			assert.Equal(t, "WAFInvalidParameterException", awsErr.Code,
				"a present-but-invalid value is WAFInvalidParameterException, not ValidationError")
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
			assert.Contains(t, awsErr.Message, tc.member,
				"the refusal must name the member whose value was invalid")
		})
	}
}

// TestWAFv2CreateIPSet_AnEmptyAddressArrayIsAccepted is the distinction that made
// the nil-to-empty default a defect rather than a convenience. AWS lists
// `"Addresses": []` among its valid example specifications, so an empty array is a
// caller asking for an IP set that matches nothing — a different request from one
// that omits the member, and one substrate must still accept.
func TestWAFv2CreateIPSet_AnEmptyAddressArrayIsAccepted(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)
	body := wafv2ValidIPSetBody()
	body["Addresses"] = []string{}

	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateIPSet", body))
	require.NoError(t, err, `"Addresses": [] is a documented valid specification`)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Summary struct {
			ID  string `json:"Id"`
			ARN string `json:"ARN"`
		} `json:"Summary"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	require.NotEmpty(t, out.Summary.ID)

	// And the set reads back with no addresses rather than with an invented one.
	getResp, err := p.HandleRequest(ctx, wafv2Request(t, "GetIPSet", map[string]any{
		"Id": out.Summary.ID, "Name": "allow-list", "Scope": "REGIONAL",
	}))
	require.NoError(t, err)
	var got struct {
		IPSet struct {
			Addresses []string `json:"Addresses"`
		} `json:"IPSet"`
	}
	require.NoError(t, json.Unmarshal(getResp.Body, &got))
	assert.Empty(t, got.IPSet.Addresses)
}

// TestWAFv2CreateIPSet_AValidRequestKeepsWhatItAskedFor is the negative twin of
// the refusal tests: the validation must not have narrowed what AWS accepts. It
// also asserts the members are stored as sent rather than as defaulted, which is
// the original symptom — an IPV6 request came back IPV4.
func TestWAFv2CreateIPSet_AValidRequestKeepsWhatItAskedFor(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)
	body := wafv2ValidIPSetBody()
	body["Name"] = "v6-set"
	body["IPAddressVersion"] = "IPV6"
	body["Addresses"] = []string{"2001:db8::/32", "2001:db8:1::/48"}

	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateIPSet", body))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Summary struct {
			ID string `json:"Id"`
		} `json:"Summary"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))

	getResp, err := p.HandleRequest(ctx, wafv2Request(t, "GetIPSet", map[string]any{
		"Id": out.Summary.ID, "Name": "v6-set", "Scope": "REGIONAL",
	}))
	require.NoError(t, err)
	var got struct {
		IPSet struct {
			IPAddressVersion string   `json:"IPAddressVersion"`
			Addresses        []string `json:"Addresses"`
		} `json:"IPSet"`
	}
	require.NoError(t, json.Unmarshal(getResp.Body, &got))
	assert.Equal(t, "IPV6", got.IPSet.IPAddressVersion)
	assert.Equal(t, []string{"2001:db8::/32", "2001:db8:1::/48"}, got.IPSet.Addresses)
}

// TestWAFv2ARN_TheScopeSegmentFollowsTheScope is #858 at the plugin. A CLOUDFRONT
// resource reported ":regional/" because the segment was a literal.
func TestWAFv2ARN_TheScopeSegmentFollowsTheScope(t *testing.T) {
	tests := []struct {
		scope   string
		segment string
	}{
		{"REGIONAL", ":regional/"},
		{"CLOUDFRONT", ":cloudfront/"},
	}

	for _, tc := range tests {
		t.Run(tc.scope, func(t *testing.T) {
			p, ctx := setupWAFv2Plugin(t)

			aclResp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateWebACL", map[string]any{
				"Name":          "scoped-acl",
				"Scope":         tc.scope,
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
			}))
			require.NoError(t, err)

			body := wafv2ValidIPSetBody()
			body["Scope"] = tc.scope
			ipResp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateIPSet", body))
			require.NoError(t, err)

			for name, resp := range map[string]*emulator.AWSResponse{
				"web ACL": aclResp, "IP set": ipResp,
			} {
				var out struct {
					Summary struct {
						ARN string `json:"ARN"`
					} `json:"Summary"`
				}
				require.NoError(t, json.Unmarshal(resp.Body, &out))
				assert.Contains(t, out.Summary.ARN, tc.segment,
					"%s: the scope segment must follow the Scope, not a literal", name)
			}
		})
	}
}

// TestWAFv2ARN_TheAPIAndCloudFormationAgree is #858's actual symptom. Both paths
// create a CLOUDFRONT web ACL through the same deployer, so the two ARNs are
// produced by the two builders that used to disagree — CloudFormation derived the
// scope segment, the plugin hardcoded it.
//
// The ARNs are not equal and are not meant to be: the CloudFormation stub uses the
// logical ID as the ARN's identifier segment where the API mints one. What must
// agree is everything up to that segment.
func TestWAFv2ARN_TheAPIAndCloudFormationAgree(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"CFNAcl": {
				"Type": "AWS::WAFv2::WebACL",
				"Properties": {
					"Name": "cfn-acl",
					"Scope": "CLOUDFRONT",
					"DefaultAction": {"Allow": {}},
					"VisibilityConfig": {
						"SampledRequestsEnabled": true,
						"CloudWatchMetricsEnabled": true,
						"MetricName": "cfn-acl"
					}
				}
			}
		}
	}`

	result, err := d.Deploy(t.Context(), tmpl, "wafv2-arn", nil)
	require.NoError(t, err)
	cfnARN := resourceByLogicalID(t, result, "CFNAcl").ARN
	require.NotEmpty(t, cfnARN)

	apiBody, err := json.Marshal(map[string]any{
		"Name":          "api-acl",
		"Scope":         "CLOUDFRONT",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
	})
	require.NoError(t, err)
	apiResp, err := d.DispatchForTest(t.Context(), &emulator.AWSRequest{
		Service:   "wafv2",
		Operation: "CreateWebACL",
		Headers:   map[string]string{"X-Amz-Target": "AWSWAF_20190729.CreateWebACL"},
		Body:      apiBody,
		Params:    map[string]string{},
	}, "wafv2-arn")
	require.NoError(t, err)

	var out struct {
		Summary struct {
			ARN string `json:"ARN"`
		} `json:"Summary"`
	}
	require.NoError(t, json.Unmarshal(apiResp.Body, &out))

	// Compare everything through the scope segment and the resource type. Before
	// #858 the CloudFormation ARN read ":cloudfront/webacl/" and the API's
	// ":regional/webacl/" for the same scope.
	const prefix = "arn:aws:wafv2:us-east-1:123456789012:cloudfront/webacl/"
	assert.True(t, strings.HasPrefix(cfnARN, prefix),
		"CloudFormation ARN %q must lead with %q", cfnARN, prefix)
	assert.True(t, strings.HasPrefix(out.Summary.ARN, prefix),
		"API ARN %q must lead with %q", out.Summary.ARN, prefix)
}
