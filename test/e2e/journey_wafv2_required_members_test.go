package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafv2types "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// #755 and #858 at the wire tier, where three things can be proved that the plugin
// tier cannot.
//
// First, that substrate's refusal decodes as WAFv2's *typed* exception rather than
// as a generic error — the failure mode #738 established, where a consumer's
// errors.As branch never runs because the code went out under a name the SDK does
// not model.
//
// Second, that `"Addresses": []` reaches substrate as an empty list and not as an
// absent member. AWS lists that specification as valid while marking
// `"Addresses": [""]` INVALID, and the generated SDK keys its own required-member
// check on `v.Addresses == nil` — so the distinction substrate had erased with
// `if input.Addresses == nil { input.Addresses = []string{} }` is one the SDK
// itself draws, and only a real client can carry it across the wire.
//
// Third, that a CLOUDFRONT web ACL's ARN names the scope it has. Which literal a
// CLOUDFRONT scope renders is substrate's reading — the four sources are named in
// wafv2ARN's doc comment — but that both creation paths agree on one answer is not.

// TestJourney_WAFv2CreateIPSetOmittedMemberIsRefused sends the request the typed
// SDK will not: its generated validator refuses an absent Name, Scope,
// IPAddressVersion or Addresses client-side, so a Go consumer never reached
// substrate's defaults at all. A CLI caller, another language's client, or an
// older SDK does, which is why this is a raw POST rather than a client call, and
// why the defect survived every tier that used a client.
func TestJourney_WAFv2CreateIPSetOmittedMemberIsRefused(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, member := range []string{"Name", "Scope", "IPAddressVersion", "Addresses"} {
		t.Run(member, func(t *testing.T) {
			body := map[string]any{
				"Name":             "raw-set",
				"Scope":            "REGIONAL",
				"IPAddressVersion": "IPV4",
				"Addresses":        []string{"192.0.2.44/32"},
			}
			delete(body, member)

			status, errBody := wafv2RawCall(t, ts, "CreateIPSet", body)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %v", status, errBody)
			}
			if got := errBody["Code"]; got != "ValidationError" {
				t.Errorf("Code = %q, want ValidationError — WAFv2's CommonErrors code for "+
					"an input that omits a required parameter", got)
			}
			// The __type is what an SDK matches on, so a code in Code but not in
			// __type would still decode as an unmodelled error.
			if !strings.Contains(errBody["__type"], "ValidationError") {
				t.Errorf("__type = %q, want it to name ValidationError", errBody["__type"])
			}
			if !strings.Contains(errBody["Message"], member) {
				t.Errorf("Message = %q, want it to name the missing member %q",
					errBody["Message"], member)
			}
		})
	}
}

// TestJourney_WAFv2CreateIPSetInvalidValueDecodesAsTheTypedError uses the typed
// client, which can send an invalid *value* even though it cannot send an absent
// member. The assertion is errors.As against WAFv2's own exception type: a
// consumer's error branch keys on that, so a 400 carrying the right code under a
// name the SDK does not model would still leave the branch dead.
func TestJourney_WAFv2CreateIPSetInvalidValueDecodesAsTheTypedError(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := wafv2.NewFromConfig(cfg, func(o *wafv2.Options) { o.RetryMaxAttempts = 1 })

	tests := []struct {
		name  string
		input *wafv2.CreateIPSetInput
	}{{
		// Valid Values: CLOUDFRONT | REGIONAL. The enum is a string type, so a
		// caller can send an unmodelled value and the SDK will serialise it.
		name: "lowercase scope",
		input: &wafv2.CreateIPSetInput{
			Name:             aws.String("bad-scope"),
			Scope:            wafv2types.Scope("regional"),
			IPAddressVersion: wafv2types.IPAddressVersionIpv4,
			Addresses:        []string{"192.0.2.44/32"},
		},
	}, {
		name: "unknown IP version",
		input: &wafv2.CreateIPSetInput{
			Name:             aws.String("bad-version"),
			Scope:            wafv2types.ScopeRegional,
			IPAddressVersion: wafv2types.IPAddressVersion("IPV5"),
			Addresses:        []string{"192.0.2.44/32"},
		},
	}, {
		// Name Pattern: ^[\w\-]+$.
		name: "name with a space",
		input: &wafv2.CreateIPSetInput{
			Name:             aws.String("bad name"),
			Scope:            wafv2types.ScopeRegional,
			IPAddressVersion: wafv2types.IPAddressVersionIpv4,
			Addresses:        []string{"192.0.2.44/32"},
		},
	}, {
		// "supports all IPv4 and IPv6 CIDR ranges except for /0".
		name: "slash-zero address",
		input: &wafv2.CreateIPSetInput{
			Name:             aws.String("zero-prefix"),
			Scope:            wafv2types.ScopeRegional,
			IPAddressVersion: wafv2types.IPAddressVersionIpv4,
			Addresses:        []string{"0.0.0.0/0"},
		},
	}, {
		name: "address in no CIDR notation",
		input: &wafv2.CreateIPSetInput{
			Name:             aws.String("no-prefix"),
			Scope:            wafv2types.ScopeRegional,
			IPAddressVersion: wafv2types.IPAddressVersionIpv4,
			Addresses:        []string{"192.0.2.44"},
		},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := client.CreateIPSet(ctx, tc.input)
			if err == nil {
				t.Fatalf("CreateIPSet succeeded, returning %+v", out)
			}

			var typed *wafv2types.WAFInvalidParameterException
			if !errors.As(err, &typed) {
				t.Fatalf("error does not decode as WAFInvalidParameterException: %v", err)
			}
			var api smithy.APIError
			if errors.As(err, &api) && api.ErrorCode() != "WAFInvalidParameterException" {
				t.Errorf("ErrorCode = %q, want WAFInvalidParameterException", api.ErrorCode())
			}
		})
	}
}

// TestJourney_WAFv2CreateIPSetAcceptsAnEmptyAddressList is the empty-versus-absent
// distinction carried by a real client. The SDK's own validator refuses
// `Addresses: nil` and permits `Addresses: []string{}`, which is the same line AWS
// draws in its examples, so substrate must accept this and store no addresses
// rather than reject it alongside the omission.
func TestJourney_WAFv2CreateIPSetAcceptsAnEmptyAddressList(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := wafv2.NewFromConfig(cfg, func(o *wafv2.Options) { o.RetryMaxAttempts = 1 })

	create, err := client.CreateIPSet(ctx, &wafv2.CreateIPSetInput{
		Name:             aws.String("matches-nothing"),
		Scope:            wafv2types.ScopeRegional,
		IPAddressVersion: wafv2types.IPAddressVersionIpv4,
		Addresses:        []string{},
	})
	if err != nil {
		t.Fatalf(`CreateIPSet with "Addresses": []: %v`, err)
	}
	if create.Summary == nil || create.Summary.Id == nil {
		t.Fatal("CreateIPSet returned no summary Id")
	}

	got, err := client.GetIPSet(ctx, &wafv2.GetIPSetInput{
		Id:    create.Summary.Id,
		Name:  aws.String("matches-nothing"),
		Scope: wafv2types.ScopeRegional,
	})
	if err != nil {
		t.Fatalf("GetIPSet: %v", err)
	}
	if got.IPSet == nil {
		t.Fatal("GetIPSet returned no IPSet")
	}
	if len(got.IPSet.Addresses) != 0 {
		t.Errorf("Addresses = %v, want none — an empty list must not gain an entry",
			got.IPSet.Addresses)
	}
}

// TestJourney_WAFv2CloudFrontARNNamesItsScope is #858 through a client. The plugin
// hardcoded "regional" where CloudFormation derived the segment, so a CLOUDFRONT
// resource created through the API reported an ARN naming a scope it did not have
// — and an ARN is what IPSetReferenceStatement and AssociateWebACL take, so it is
// an identifier another operation is expected to accept.
func TestJourney_WAFv2CloudFrontARNNamesItsScope(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := wafv2.NewFromConfig(cfg, func(o *wafv2.Options) { o.RetryMaxAttempts = 1 })

	acl, err := client.CreateWebACL(ctx, &wafv2.CreateWebACLInput{
		Name:          aws.String("edge-acl"),
		Scope:         wafv2types.ScopeCloudfront,
		DefaultAction: &wafv2types.DefaultAction{Allow: &wafv2types.AllowAction{}},
		VisibilityConfig: &wafv2types.VisibilityConfig{
			MetricName:               aws.String("edgeAcl"),
			SampledRequestsEnabled:   true,
			CloudWatchMetricsEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("CreateWebACL: %v", err)
	}
	if acl.Summary == nil || acl.Summary.ARN == nil {
		t.Fatal("CreateWebACL returned no summary ARN")
	}

	ipSet, err := client.CreateIPSet(ctx, &wafv2.CreateIPSetInput{
		Name:             aws.String("edge-ips"),
		Scope:            wafv2types.ScopeCloudfront,
		IPAddressVersion: wafv2types.IPAddressVersionIpv4,
		Addresses:        []string{"192.0.2.44/32"},
	})
	if err != nil {
		t.Fatalf("CreateIPSet: %v", err)
	}
	if ipSet.Summary == nil || ipSet.Summary.ARN == nil {
		t.Fatal("CreateIPSet returned no summary ARN")
	}

	for _, tc := range []struct {
		what string
		arn  string
		want string
	}{
		{"web ACL", *acl.Summary.ARN, ":cloudfront/webacl/"},
		{"IP set", *ipSet.Summary.ARN, ":cloudfront/ipset/"},
	} {
		if !strings.Contains(tc.arn, tc.want) {
			t.Errorf("%s ARN = %q, want it to contain %q — the scope segment follows the "+
				"Scope, not a literal", tc.what, tc.arn, tc.want)
		}
	}
}

// wafv2RawCall posts a WAFv2 request built by hand, returning the status and the
// decoded error body. It exists because the generated SDK refuses a request with an
// omitted required member before it leaves the process, so the only way to send one
// is to skip the client.
func wafv2RawCall(
	t *testing.T, ts *emulator.TestServer, op string, body map[string]any,
) (int, map[string]string) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s body: %v", op, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build %s request: %v", op, err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSWAF_20190729."+op)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var decoded map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode %s response: %v", op, err)
	}
	return resp.StatusCode, decoded
}
