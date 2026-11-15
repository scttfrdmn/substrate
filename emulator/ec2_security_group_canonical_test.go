package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestEC2_SecurityGroupRules_CIDRsAreCanonicalised is #1553's first half: every rule CIDR is
// stored, described and compared in the canonical form API_AuthorizeSecurityGroupIngress
// publishes — "if you specify 100.68.0.18/18 for the CIDR block, AWS canonicalizes the CIDR block
// to 100.68.0.0/18 ... if you attempt to add another rule with the non-canonical form of the CIDR
// ... the API throws an duplicate rule error." The page's example is pinned below.
func TestEC2_SecurityGroupRules_CIDRsAreCanonicalised(t *testing.T) {
	t.Parallel()
	ingress := func(sg string, extra map[string]string) map[string]string {
		p := map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg,
			"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}
	t.Run("the page's example is stored and described canonically", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.71.0.0/16"))
		ec2OK(t, ts, ingress(sg, map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "100.68.0.18/18"}))
		assert.Equal(t, []string{"100.68.0.0/18"}, ec2GroupRuleSources(t, ts, sg, false))
	})

	// legacy builds the top-level form, IpProtocol/FromPort/ToPort/CidrIp.
	legacy := func(action, sg, cidr string) map[string]string {
		return map[string]string{
			"Action": action, "GroupId": sg, "IpProtocol": "tcp", "FromPort": "22", "ToPort": "22", "CidrIp": cidr,
		}
	}
	tests := []struct {
		name   string
		stored map[string]string
		// again builds the second request, naming the stored rule in another spelling.
		again func(action, sg string) map[string]string
	}{
		{"IPv4, non-canonical after canonical",
			map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "100.68.0.0/18"},
			func(action, sg string) map[string]string {
				p := ingress(sg, map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "100.68.0.18/18"})
				p["Action"] = action
				return p
			}},
		{"IPv4, canonical after non-canonical",
			map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "100.68.0.18/18"},
			func(action, sg string) map[string]string {
				p := ingress(sg, map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "100.68.0.0/18"})
				p["Action"] = action
				return p
			}},
		{"IPv6, host bits and an uncompressed spelling",
			map[string]string{"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "2001:db8::/32"},
			func(action, sg string) map[string]string {
				p := ingress(sg, map[string]string{"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "2001:0DB8:0000:0000::1/32"})
				p["Action"] = action
				return p
			}},
		{"the legacy top-level CidrIp",
			map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "192.0.2.0/24"},
			func(action, sg string) map[string]string { return legacy(action, sg, "192.0.2.77/24") }},
	}
	for _, tc := range tests {
		t.Run("duplicate: "+tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.72.0.0/16"))
			ec2OK(t, ts, ingress(sg, tc.stored))
			ec2RequireRefusal(t, ts, tc.again("AuthorizeSecurityGroupIngress", sg), "InvalidPermission.Duplicate")
		})
		t.Run("revoke: "+tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.73.0.0/16"))
			ec2OK(t, ts, ingress(sg, tc.stored))
			ec2OK(t, ts, tc.again("RevokeSecurityGroupIngress", sg))
			var doc struct {
				Ingress []string `xml:"securityGroupInfo>item>ipPermissions>item>ipProtocol"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": sg}, &doc)
			assert.Empty(t, doc.Ingress, "the revoke removed the rule its other spelling named")
		})
	}

	t.Run("IPv6 is described in its canonical text form", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.74.0.0/16"))
		ec2OK(t, ts, ingress(sg, map[string]string{"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "2001:0DB8:00AB::1/48"}))
		var doc struct {
			V6 []string `xml:"securityGroupInfo>item>ipPermissions>item>ipv6Ranges>item>cidrIpv6"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": sg}, &doc)
		assert.Equal(t, []string{"2001:db8:ab::/48"}, doc.V6)
	})

	for _, tc := range []struct {
		name  string
		extra map[string]string
	}{
		{"no prefix length", map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "1.2.3.4"}},
		{"a prefix past 32", map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/33"}},
		{"an IPv6 range as CidrIp", map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "::/0"}},
		{"an IPv4 range as CidrIpv6", map[string]string{"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "10.0.0.0/8"}},
		{"a group ID as CidrIp", map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "sg-0123456789abcdef0"}},
	} {
		t.Run("malformed: "+tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.75.0.0/16"))
			params := ingress(sg, tc.extra)
			status, code, msg := ec2ErrorDetail(t, ts, params)
			require.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			cidr := tc.extra["IpPermissions.1.IpRanges.1.CidrIp"] + tc.extra["IpPermissions.1.Ipv6Ranges.1.CidrIpv6"]
			assert.Equal(t, "CIDR block "+cidr+" is malformed", msg)
			assert.Empty(t, ec2GroupRuleSources(t, ts, sg, false), "a refused authorize writes nothing")
		})
	}
}

// TestEC2_SecurityGroupRules_RecordFromBeforeCanonicalisationStillRevokes pins #1553's third
// criterion: a rule an earlier substrate stored as sent — 100.68.0.18/18 — is still matched by a
// revoke naming it, in either spelling, and still counts as a duplicate.
func TestEC2_SecurityGroupRules_RecordFromBeforeCanonicalisationStillRevokes(t *testing.T) {
	t.Parallel()
	for _, spelling := range []string{"100.68.0.18/18", "100.68.0.0/18", "100.68.63.255/18"} {
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()
			state := emulator.NewMemoryStateManager()
			ts := newEC2TestServerWithState(t, state)
			sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.76.0.0/16"))
			ec2WriteLegacyIngress(t, state, sg, "100.68.0.18/18")

			rule := map[string]string{
				"GroupId": sg, "IpPermissions.1.IpProtocol": "tcp",
				"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
				"IpPermissions.1.IpRanges.1.CidrIp": spelling,
			}
			rule["Action"] = "AuthorizeSecurityGroupIngress"
			ec2RequireRefusal(t, ts, rule, "InvalidPermission.Duplicate")
			rule["Action"] = "RevokeSecurityGroupIngress"
			ec2OK(t, ts, rule)
			assert.Empty(t, ec2GroupRuleSources(t, ts, sg, false))
		})
	}
}

// ec2WriteLegacyIngress rewrites a stored group's ingress rules to one tcp/22 rule holding cidr
// exactly as given, the shape a substrate before #1553 stored a non-canonical CIDR in.
func ec2WriteLegacyIngress(t *testing.T, state emulator.StateManager, sgID, cidr string) {
	t.Helper()
	ec2RewriteRecord(t, state, "sg:", sgID, func(rec map[string]any) {
		rec["ingress_rules"] = []map[string]any{{
			"ip_protocol": "tcp", "from_port": 22, "to_port": 22, "ip_ranges": []string{cidr},
		}}
	})
}

// TestEC2_SecurityGroupRules_GroupNameResolvesInTheDefaultVPC is #1553's second half: Authorize
// and Revoke read GroupName as both pages publish it — "[Default VPC] The name of the security
// group" — resolving it among the default VPC's groups, with GroupId winning when both are sent.
func TestEC2_SecurityGroupRules_GroupNameResolvesInTheDefaultVPC(t *testing.T) {
	t.Parallel()
	rule := func(action string, extra map[string]string) map[string]string {
		p := map[string]string{
			"Action": action, "IpPermissions.1.IpProtocol": "tcp",
			"IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
			"IpPermissions.1.IpRanges.1.CidrIp": "203.0.113.0/24",
		}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}

	t.Run("authorize and revoke by name, ingress and egress", func(t *testing.T) {
		t.Parallel()
		for _, dir := range []struct{ authorize, revoke string }{
			{"AuthorizeSecurityGroupIngress", "RevokeSecurityGroupIngress"},
			{"AuthorizeSecurityGroupEgress", "RevokeSecurityGroupEgress"},
		} {
			ts := newEC2TestServer(t)
			sg := ec2CreateSG(t, ts, "Web-Tier", "web", "")
			ec2OK(t, ts, rule(dir.authorize, map[string]string{"GroupName": "web-tier"}))
			egress := strings.HasSuffix(dir.authorize, "Egress")
			assert.Contains(t, ec2GroupRuleSources(t, ts, sg, egress), "203.0.113.0/24", dir.authorize)
			ec2OK(t, ts, rule(dir.revoke, map[string]string{"GroupName": "Web-Tier"}))
			assert.NotContains(t, ec2GroupRuleSources(t, ts, sg, egress), "203.0.113.0/24", dir.revoke)
		}
	})

	t.Run("GroupId wins when both are sent", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		byName := ec2CreateSG(t, ts, "named", "named", "")
		byID := ec2CreateSG(t, ts, "other", "other", ec2CreateVPC(t, ts, "10.77.0.0/16"))
		ec2OK(t, ts, rule("AuthorizeSecurityGroupIngress", map[string]string{"GroupId": byID, "GroupName": "named"}))
		assert.Equal(t, []string{"203.0.113.0/24"}, ec2GroupRuleSources(t, ts, byID, false))
		assert.Empty(t, ec2GroupRuleSources(t, ts, byName, false))
	})

	t.Run("a name outside the default VPC is not found", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2CreateSG(t, ts, "elsewhere", "elsewhere", ec2CreateVPC(t, ts, "10.78.0.0/16"))
		ec2RequireRefusal(t, ts, rule("AuthorizeSecurityGroupIngress", map[string]string{"GroupName": "elsewhere"}),
			"InvalidGroup.NotFound")
	})

	t.Run("neither is MissingParameter", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2RequireRefusal(t, ts, rule("AuthorizeSecurityGroupIngress", nil), "MissingParameter")
	})
}
