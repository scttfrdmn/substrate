package emulator_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RevokeSecurityGroupIngress/Egress removes the sources it names, not the permission they were
// authorized in (#1453).
//
// EC2 stores one rule per source. Substrate stores the IpPermission an authorize carried, and
// through v0.125.0 a revoke naming one of its sources dropped the whole permission — so revoking
// 0.0.0.0/0 from a permission authorized with 0.0.0.0/0 and 10.1.0.0/16 left no SSH rule at all.

func ec2AuthorizeSSH(t *testing.T, ts *httptest.Server, groupID string, cidrs ...string) {
	t.Helper()
	params := map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": groupID,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
	}
	for i, c := range cidrs {
		params["IpPermissions.1.IpRanges."+strconv.Itoa(i+1)+".CidrIp"] = c
	}
	ec2FleetXML(t, ts, params, nil)
}

func ec2RevokeSSH(t *testing.T, ts *httptest.Server, groupID string, cidrs ...string) *http.Response {
	t.Helper()
	params := map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": groupID,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
	}
	for i, c := range cidrs {
		params["IpPermissions.1.IpRanges."+strconv.Itoa(i+1)+".CidrIp"] = c
	}
	return ec2Request(t, ts, params)
}

// ec2SSHSources returns every CIDR the group's tcp/22 ingress rules admit.
func ec2SSHSources(t *testing.T, ts *httptest.Server, groupID string) []string {
	t.Helper()
	groups := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": groupID})
	require.Len(t, groups, 1)
	var out []string
	for _, p := range groups[0].Ingress {
		if p.Protocol == "tcp" && p.FromPort == 22 && p.ToPort == 22 {
			out = append(out, p.CIDRs...)
		}
	}
	return out
}

// TestEC2_RevokeSecurityGroupIngress_OneRangeOfAMultiRangePermission is the issue's repro, plus
// the two-call shape it reports already worked.
func TestEC2_RevokeSecurityGroupIngress_OneRangeOfAMultiRangePermission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		authorize func(t *testing.T, ts *httptest.Server, groupID string)
	}{
		{"authorized together", func(t *testing.T, ts *httptest.Server, g string) {
			ec2AuthorizeSSH(t, ts, g, "0.0.0.0/0", "10.1.0.0/16")
		}},
		{"authorized in two calls", func(t *testing.T, ts *httptest.Server, g string) {
			ec2AuthorizeSSH(t, ts, g, "0.0.0.0/0")
			ec2AuthorizeSSH(t, ts, g, "10.1.0.0/16")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			groupID := ec2CreateSG(t, ts, "ssh", "ssh", ec2CreateVPC(t, ts, "10.0.0.0/16"))
			tc.authorize(t, ts, groupID)

			resp := ec2RevokeSSH(t, ts, groupID, "0.0.0.0/0")
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, []string{"10.1.0.0/16"}, ec2SSHSources(t, ts, groupID),
				"only the revoked range is removed")

			resp = ec2RevokeSSH(t, ts, groupID, "10.1.0.0/16")
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Empty(t, ec2SSHSources(t, ts, groupID), "the permission goes once it holds no source")
		})
	}
}

// TestEC2_RevokeSecurityGroupIngress_UnmatchedRule pins the published answer for a revoke that
// names a rule the group does not hold, which depends on the group's VPC.
func TestEC2_RevokeSecurityGroupIngress_UnmatchedRule(t *testing.T) {
	t.Parallel()

	t.Run("a non-default VPC refuses and revokes nothing", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		groupID := ec2CreateSG(t, ts, "ssh", "ssh", ec2CreateVPC(t, ts, "10.0.0.0/16"))
		ec2AuthorizeSSH(t, ts, groupID, "0.0.0.0/0", "10.1.0.0/16")

		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "RevokeSecurityGroupIngress", "GroupId": groupID,
			"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
			"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
			"IpPermissions.1.IpRanges.2.CidrIp": "192.168.0.0/16",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidPermission.NotFound", code)
		assert.ElementsMatch(t, []string{"0.0.0.0/0", "10.1.0.0/16"}, ec2SSHSources(t, ts, groupID),
			"no rules are revoked, including the one that did match")
	})

	t.Run("the default VPC revokes what matched and reports the rest", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		groupID := ec2CreateSG(t, ts, "ssh", "ssh", "") // the default VPC
		ec2AuthorizeSSH(t, ts, groupID, "0.0.0.0/0", "10.1.0.0/16")

		resp := ec2Request(t, ts, map[string]string{
			"Action": "RevokeSecurityGroupIngress", "GroupId": groupID,
			"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
			"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
			"IpPermissions.1.IpRanges.2.CidrIp": "192.168.0.0/16",
		})
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
		assert.Contains(t, string(body), "<return>true</return>")
		assert.Contains(t, string(body), "<unknownIpPermissionSet><item><ipProtocol>tcp</ipProtocol>")
		assert.Contains(t, string(body), "<cidrIp>192.168.0.0/16</cidrIp>")
		assert.NotContains(t, string(body), "<cidrIp>0.0.0.0/0</cidrIp>", "a revoked range is not unknown")
		assert.Equal(t, []string{"10.1.0.0/16"}, ec2SSHSources(t, ts, groupID))
	})
}

// TestEC2_RevokeSecurityGroupIngress_SourcesStayDistinct pins that a source of one kind does not
// revoke a source of another on the same port, across all four kinds.
func TestEC2_RevokeSecurityGroupIngress_SourcesStayDistinct(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	peer := ec2CreateSG(t, ts, "peer", "peer", vpcID)
	groupID := ec2CreateSG(t, ts, "mixed", "mixed", vpcID)
	perm := map[string]string{
		"GroupId":                    groupID,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
	}
	with := func(action string, extra map[string]string) map[string]string {
		out := map[string]string{"Action": action}
		for k, v := range perm {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	ec2FleetXML(t, ts, with("AuthorizeSecurityGroupIngress", map[string]string{
		"IpPermissions.1.IpRanges.1.CidrIp":            "10.0.0.0/8",
		"IpPermissions.1.Ipv6Ranges.1.CidrIpv6":        "::/0",
		"IpPermissions.1.PrefixListIds.1.PrefixListId": "pl-0123456789abcdef0",
		"IpPermissions.1.Groups.1.GroupId":             peer,
	}), nil)

	ec2FleetXML(t, ts, with("RevokeSecurityGroupIngress", map[string]string{
		"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "::/0",
	}), nil)
	ec2FleetXML(t, ts, with("RevokeSecurityGroupIngress", map[string]string{
		"IpPermissions.1.Groups.1.GroupId": peer,
	}), nil)

	groups := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": groupID})
	require.Len(t, groups[0].Ingress, 1)
	rule := groups[0].Ingress[0]
	assert.Equal(t, []string{"10.0.0.0/8"}, rule.CIDRs)
	assert.Equal(t, []string{"pl-0123456789abcdef0"}, rule.Prefixes)
	assert.Empty(t, rule.IPv6, "the IPv6 range alone was revoked")
	assert.Empty(t, rule.Groups, "the group reference alone was revoked")
}
