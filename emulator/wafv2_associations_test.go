package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// DeleteWebACL refuses an ACL a resource is still associated with, AssociateWebACL refuses a WebACLArn
// naming no ACL, and GetWebACLForResource answers a null web ACL for a resource with none (#1541).

func TestWAFv2Associations_DeleteWebACLRefusesAnAssociatedACL(t *testing.T) {
	ts := emulator.StartTestServer(t)
	arn, id, lockToken := wafv2ListCreateWebACL(t, ts, "in-use")
	resources := []string{
		"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/a/0123456789abcdef",
		"arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc",
	}
	for _, r := range resources {
		wafv2ListCall(t, ts, "AssociateWebACL", map[string]any{"WebACLArn": arn, "ResourceArn": r}, nil)
	}
	del := func(token string) (int, []byte) {
		return cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, "DeleteWebACL",
			map[string]any{"Name": "in-use", "Scope": "REGIONAL", "Id": id, "LockToken": token})
	}

	for i, r := range resources {
		for _, token := range []string{lockToken, "00000000-0000-0000-0000-000000000000"} {
			// The association is checked before the lock token, so a stale token learns the same thing.
			status, raw := del(token)
			require.Equal(t, http.StatusBadRequest, status, "%s", raw)
			assert.Equal(t, "WAFAssociatedItemException", cicdCode(t, raw), "%s", raw)
		}
		var got struct {
			WebACL map[string]any `json:"WebACL"`
		}
		wafv2ListCall(t, ts, "GetWebACL", map[string]any{"Name": "in-use", "Scope": "REGIONAL", "Id": id}, &got)
		assert.Equal(t, arn, got.WebACL["ARN"], "a refused delete leaves the ACL in place")
		wafv2ListCall(t, ts, "DisassociateWebACL", map[string]any{"ResourceArn": r}, nil)
		if i < len(resources)-1 {
			continue
		}
		status, raw := del(lockToken)
		require.Equal(t, http.StatusOK, status, "an ACL associated with nothing is deleted: %s", raw)
	}
}

func TestWAFv2Associations_AssociateWebACLRefusesAnAbsentACL(t *testing.T) {
	ts := emulator.StartTestServer(t)
	resource := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/a/0123456789abcdef"
	for name, tc := range map[string]struct {
		webACL string
		code   string
	}{
		"no WebACLArn":       {"", "ValidationError"},
		"an ARN naming none": {"arn:aws:wafv2:us-east-1:123456789012:regional/webacl/x/00000000-0000-0000-0000-000000000000", "WAFNonexistentItemException"},
		"not a web ACL ARN":  {"arn:aws:wafv2:us-east-1:123456789012:regional/ipset/x/00000000-0000-0000-0000-000000000000", "WAFNonexistentItemException"},
	} {
		t.Run(name, func(t *testing.T) {
			body := map[string]any{"ResourceArn": resource}
			if tc.webACL != "" {
				body["WebACLArn"] = tc.webACL
			}
			status, raw := cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, "AssociateWebACL", body)
			require.Equal(t, http.StatusBadRequest, status, "%s", raw)
			assert.Equal(t, tc.code, cicdCode(t, raw), "%s", raw)

			status, raw = cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, "GetWebACLForResource", map[string]any{"ResourceArn": resource})
			require.Equal(t, http.StatusOK, status, "%s", raw)
			assert.JSONEq(t, `{}`, string(raw), "the refused association stored nothing")
		})
	}
}

func TestWAFv2Associations_AnUnassociatedResourceAnswersANullWebACL(t *testing.T) {
	ts := emulator.StartTestServer(t)
	status, raw := cicdCall(t, ts, wafv2ListHost, wafv2ListTarget, "GetWebACLForResource",
		map[string]any{"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/none/1"})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	assert.JSONEq(t, `{}`, string(raw), "no WebACL member, which an SDK reads as a nil WebACL")
}
