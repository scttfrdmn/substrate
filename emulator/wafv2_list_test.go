package emulator_test

// ListWebACLs and ListIPSets page by Limit and NextMarker, and GetWebACLForResource answers the whole
// WebACL (#1161). Every assertion goes over the wire.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

const (
	wafv2ListHost   = "wafv2.us-east-1.amazonaws.com"
	wafv2ListTarget = "AWSWAF_20190729"
)

// wafv2ListCall sends one WAFv2 operation and decodes a successful body into out.
func wafv2ListCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any, out any) {
	t.Helper()
	status, raw := cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, op, body)
	require.Equal(t, http.StatusOK, status, "%s: %s", op, raw)
	if out != nil {
		require.NoError(t, json.Unmarshal(raw, out), "%s", raw)
	}
}

// wafv2ListCreateWebACL creates a REGIONAL web ACL and returns its ARN and ID.
func wafv2ListCreateWebACL(t *testing.T, ts *emulator.TestServer, name string) (arn, id, lockToken string) {
	t.Helper()
	var out struct {
		Summary struct {
			ARN       string `json:"ARN"`
			ID        string `json:"Id"`
			LockToken string `json:"LockToken"`
		} `json:"Summary"`
	}
	wafv2ListCall(t, ts, "CreateWebACL", map[string]any{
		"Name":          name,
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Block": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": name,
		},
	}, &out)
	return out.Summary.ARN, out.Summary.ID, out.Summary.LockToken
}

func TestWAFv2List_TwoPageWalks(t *testing.T) {
	cases := []struct {
		op, member string
		create     func(t *testing.T, ts *emulator.TestServer, i int)
	}{
		{op: "ListWebACLs", member: "WebACLs", create: func(t *testing.T, ts *emulator.TestServer, i int) {
			wafv2ListCreateWebACL(t, ts, fmt.Sprintf("acl-%d", i))
		}},
		{op: "ListIPSets", member: "IPSets", create: func(t *testing.T, ts *emulator.TestServer, i int) {
			wafv2ListCall(t, ts, "CreateIPSet", map[string]any{
				"Name": fmt.Sprintf("ipset-%d", i), "Scope": "REGIONAL",
				"IPAddressVersion": "IPV4", "Addresses": []string{"10.0.0.0/24"},
			}, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			for i := range 3 {
				tc.create(t, ts, i)
			}
			page := func(body map[string]any) ([]string, string) {
				var doc map[string]json.RawMessage
				wafv2ListCall(t, ts, tc.op, body, &doc)
				var items []struct {
					ID string `json:"Id"`
				}
				require.NoError(t, json.Unmarshal(doc[tc.member], &items))
				ids := make([]string, 0, len(items))
				for _, it := range items {
					ids = append(ids, it.ID)
				}
				var marker string
				if raw, ok := doc["NextMarker"]; ok {
					require.NoError(t, json.Unmarshal(raw, &marker))
					require.NotEmpty(t, marker, "a marker is never emitted empty")
				}
				return ids, marker
			}

			first, marker := page(map[string]any{"Scope": "REGIONAL", "Limit": 2})
			require.Len(t, first, 2, "Limit bounds the page")
			require.NotEmpty(t, marker, "a truncated page carries NextMarker")
			second, last := page(map[string]any{"Scope": "REGIONAL", "Limit": 2, "NextMarker": marker})
			require.Len(t, second, 1)
			assert.Empty(t, last, "the final page omits NextMarker")
			assert.NotContains(t, first, second[0], "the second page resumes after the first")

			all, none := page(map[string]any{"Scope": "REGIONAL"})
			assert.Len(t, all, 3, "an absent Limit answers the whole of a small listing")
			assert.Empty(t, none)

			for _, body := range []map[string]any{
				{"Scope": "REGIONAL", "Limit": 0},
				{"Scope": "REGIONAL", "Limit": 101},
				{"Scope": "REGIONAL", "Limit": -1},
				{"Scope": "REGIONAL", "NextMarker": "not-a-marker"},
			} {
				status, raw := cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, tc.op, body)
				assert.Equal(t, http.StatusBadRequest, status, "%v: %s", body, raw)
				assert.Equal(t, "WAFInvalidParameterException", cicdCode(t, raw), "%v: %s", body, raw)
			}
		})
	}
}

func TestWAFv2List_GetWebACLForResourceAnswersTheWholeWebACL(t *testing.T) {
	ts := emulator.StartTestServer(t)
	arn, id, lockToken := wafv2ListCreateWebACL(t, ts, "for-resource")
	resource := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/alb/0123456789abcdef"
	wafv2ListCall(t, ts, "AssociateWebACL", map[string]any{"WebACLArn": arn, "ResourceArn": resource}, nil)

	var forResource, direct struct {
		WebACL map[string]any `json:"WebACL"`
	}
	wafv2ListCall(t, ts, "GetWebACLForResource", map[string]any{"ResourceArn": resource}, &forResource)
	wafv2ListCall(t, ts, "GetWebACL", map[string]any{"Name": "for-resource", "Scope": "REGIONAL", "Id": id}, &direct)

	acl := forResource.WebACL
	assert.Equal(t, arn, acl["ARN"])
	assert.Equal(t, id, acl["Id"])
	assert.Equal(t, "for-resource", acl["Name"])
	assert.Equal(t, map[string]any{"Block": map[string]any{}}, acl["DefaultAction"], "DefaultAction is Required: Yes")
	assert.Equal(t, map[string]any{
		"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "for-resource",
	}, acl["VisibilityConfig"], "VisibilityConfig is Required: Yes")
	assert.Equal(t, direct.WebACL, acl, "the same record GetWebACL reports")

	// An association whose web ACL has been deleted names nothing.
	wafv2ListCall(t, ts, "DeleteWebACL", map[string]any{
		"Name": "for-resource", "Scope": "REGIONAL", "Id": id, "LockToken": lockToken,
	}, nil)
	status, raw := cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, "GetWebACLForResource", map[string]any{"ResourceArn": resource})
	assert.Equal(t, http.StatusBadRequest, status, "%s", raw)
	assert.Equal(t, "WAFNonexistentItemException", cicdCode(t, raw), "%s", raw)
}

// GetWebACL resolves its ARN member through the path GetWebACLForResource uses, for both scopes, so
// the ARN the builder mints is the ARN the resolver accepts.
func TestWAFv2List_GetWebACLByARN(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for _, scope := range []string{"REGIONAL", "CLOUDFRONT"} {
		var created struct {
			Summary struct {
				ARN string `json:"ARN"`
				ID  string `json:"Id"`
			} `json:"Summary"`
		}
		wafv2ListCall(t, ts, "CreateWebACL", map[string]any{
			"Name": "by-arn-" + scope, "Scope": scope, "DefaultAction": map[string]any{"Allow": map[string]any{}},
			"VisibilityConfig": map[string]any{"SampledRequestsEnabled": false, "CloudWatchMetricsEnabled": false, "MetricName": "m"},
		}, &created)
		var got struct {
			WebACL map[string]any `json:"WebACL"`
		}
		wafv2ListCall(t, ts, "GetWebACL", map[string]any{"ARN": created.Summary.ARN}, &got)
		assert.Equal(t, created.Summary.ID, got.WebACL["Id"], scope)
		assert.Equal(t, created.Summary.ARN, got.WebACL["ARN"], scope)
	}
	for name, arn := range map[string]string{
		"another account": "arn:aws:wafv2:us-east-1:999999999999:regional/webacl/x/00000000-0000-0000-0000-000000000000",
		"not a web ACL":   "arn:aws:wafv2:us-east-1:123456789012:regional/ipset/x/00000000-0000-0000-0000-000000000000",
		"no such ACL":     "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/x/00000000-0000-0000-0000-000000000000",
	} {
		status, raw := cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, "GetWebACL", map[string]any{"ARN": arn})
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", name, raw)
		assert.Equal(t, "WAFNonexistentItemException", cicdCode(t, raw), "%s: %s", name, raw)
	}
}
