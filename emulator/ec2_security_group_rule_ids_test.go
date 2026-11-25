package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Security group rule IDs (#1490): one sgr- rule per source, revocable and modifiable by ID,
// and described by DescribeSecurityGroupRules.

// ec2SGRuleDoc is one SecurityGroupRule as a response renders it.
type ec2SGRuleDoc struct {
	ID          string `xml:"securityGroupRuleId"`
	GroupID     string `xml:"groupId"`
	OwnerID     string `xml:"groupOwnerId"`
	IsEgress    bool   `xml:"isEgress"`
	Protocol    string `xml:"ipProtocol"`
	FromPort    int    `xml:"fromPort"`
	ToPort      int    `xml:"toPort"`
	CidrIPv4    string `xml:"cidrIpv4"`
	CidrIPv6    string `xml:"cidrIpv6"`
	PrefixList  string `xml:"prefixListId"`
	RefGroupID  string `xml:"referencedGroupInfo>groupId"`
	Description string `xml:"description"`
	ARN         string `xml:"securityGroupRuleArn"`
	Tags        []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

// ec2SGRuleIDPattern is the sgr- form AWS publishes: "sgr-xxxxxxxxxxxxxxxxx".
var ec2SGRuleIDPattern = regexp.MustCompile(`^sgr-[0-9a-f]{17}$`)

// ec2Authorize sends an authorize and returns the rules it reports adding.
func ec2Authorize(t *testing.T, ts *httptest.Server, action string, params map[string]string) []ec2SGRuleDoc {
	t.Helper()
	params["Action"] = action
	var doc struct {
		Return bool           `xml:"return"`
		Rules  []ec2SGRuleDoc `xml:"securityGroupRuleSet>item"`
	}
	ec2FleetXML(t, ts, params, &doc)
	require.True(t, doc.Return)
	return doc.Rules
}

// ec2DescribeSGRules sends DescribeSecurityGroupRules and returns the rules and next token.
func ec2DescribeSGRules(t *testing.T, ts *httptest.Server, params map[string]string) ([]ec2SGRuleDoc, string) {
	t.Helper()
	params["Action"] = "DescribeSecurityGroupRules"
	var doc struct {
		Rules []ec2SGRuleDoc `xml:"securityGroupRuleSet>item"`
		Next  string         `xml:"nextToken"`
	}
	ec2FleetXML(t, ts, params, &doc)
	return doc.Rules, doc.Next
}

// ec2SGRulesOf returns the rules of one group, by DescribeSecurityGroupRules' group-id filter.
func ec2SGRulesOf(t *testing.T, ts *httptest.Server, groupID string) []ec2SGRuleDoc {
	t.Helper()
	rules, _ := ec2DescribeSGRules(t, ts, ec2Filter("group-id", groupID))
	return rules
}

// ec2RevokeResponse is a revoke's response body.
type ec2RevokeResponse struct {
	Return  bool `xml:"return"`
	Revoked []struct {
		ID         string `xml:"securityGroupRuleId"`
		GroupID    string `xml:"groupId"`
		IsEgress   bool   `xml:"isEgress"`
		CidrIPv4   string `xml:"cidrIpv4"`
		CidrIPv6   string `xml:"cidrIpv6"`
		RefGroupID string `xml:"referencedGroupId"`
	} `xml:"revokedSecurityGroupRuleSet>item"`
	Unknown []struct {
		Protocol string `xml:"ipProtocol"`
	} `xml:"unknownIpPermissionSet>item"`
}

func TestEC2_SGRules_AuthorizeSplitsSourcesIntoRules(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.0.0.0/16")
	group := ec2CreateSG(t, ts, "web", "web", vpc)
	peer := ec2CreateSG(t, ts, "peer", "peer", vpc)

	added := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.IpRanges.1.CidrIp":            "10.1.0.5/16",
		"IpPermissions.1.IpRanges.1.Description":       "office",
		"IpPermissions.1.IpRanges.2.CidrIp":            "192.168.0.0/24",
		"IpPermissions.1.Ipv6Ranges.1.CidrIpv6":        "2001:db8::/32",
		"IpPermissions.1.PrefixListIds.1.PrefixListId": "pl-0123456789abcdef0",
		"IpPermissions.1.Groups.1.GroupId":             peer,
		"IpPermissions.1.Groups.1.Description":         "from peer",
	})
	require.Len(t, added, 5, "one rule per source")
	seen := map[string]bool{}
	for _, r := range added {
		assert.Regexp(t, ec2SGRuleIDPattern, r.ID)
		assert.False(t, seen[r.ID], "rule IDs are distinct")
		seen[r.ID] = true
		assert.Equal(t, group, r.GroupID)
		assert.Equal(t, "123456789012", r.OwnerID)
		assert.False(t, r.IsEgress)
		assert.Equal(t, "tcp", r.Protocol)
		assert.Equal(t, 443, r.FromPort)
		assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:security-group-rule/"+r.ID, r.ARN)
	}
	assert.Equal(t, "10.1.0.0/16", added[0].CidrIPv4, "the CIDR is canonical")
	assert.Equal(t, "office", added[0].Description)
	assert.Equal(t, "192.168.0.0/24", added[1].CidrIPv4)
	assert.Empty(t, added[1].Description)
	assert.Equal(t, "2001:db8::/32", added[2].CidrIPv6)
	assert.Equal(t, "pl-0123456789abcdef0", added[3].PrefixList)
	assert.Equal(t, peer, added[4].RefGroupID)
	assert.Equal(t, "from peer", added[4].Description)

	// Describe reports the same rules under the same IDs, plus the group's default egress rule.
	described := ec2SGRulesOf(t, ts, group)
	byID := map[string]ec2SGRuleDoc{}
	for _, r := range described {
		byID[r.ID] = r
	}
	for _, r := range added {
		assert.Equal(t, r, byID[r.ID], "rule %s reads back as authorized", r.ID)
	}
	egress := 0
	for _, r := range described {
		if r.IsEgress {
			egress++
			assert.Equal(t, "-1", r.Protocol)
			assert.Equal(t, "0.0.0.0/0", r.CidrIPv4)
		}
	}
	assert.Equal(t, 1, egress, "a new group's default egress rule is a rule too")

	// DescribeSecurityGroups renders the IPv4 description where the IpRange shape has it.
	var sgDoc struct {
		Ranges []struct {
			CIDR        string `xml:"cidrIp"`
			Description string `xml:"description"`
		} `xml:"securityGroupInfo>item>ipPermissions>item>ipRanges>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": group}, &sgDoc)
	require.Len(t, sgDoc.Ranges, 2)
	assert.Equal(t, "office", sgDoc.Ranges[0].Description)
}

func TestEC2_SGRules_IDsAreStableAndDerivedForOlderState(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	// A group as an older build stored it: rules, and no rule metadata at all.
	sg := emulator.EC2SecurityGroup{
		GroupID: "sg-0123456789abcdef0", GroupName: "old", VPCID: "vpc-0123456789abcdef0",
		AccountID: "123456789012", Region: "us-east-1",
		IngressRules: []emulator.EC2IPPermission{{
			IPProtocol: "tcp", FromPort: 22, ToPort: 22, IPRanges: []string{"10.0.0.0/8", "172.16.0.0/12"},
		}},
	}
	data, err := json.Marshal(sg)
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "ec2", "sg:123456789012/us-east-1/"+sg.GroupID, data))

	ids := func() []string {
		ts := newEC2TestServerWithState(t, state)
		var out []string
		for _, r := range ec2SGRulesOf(t, ts, sg.GroupID) {
			out = append(out, r.ID)
		}
		return out
	}
	first := ids()
	require.Len(t, first, 2)
	assert.Equal(t, first, ids(), "the IDs are derived from content, so every read agrees")
	assert.NotEqual(t, first[0], first[1])
}

func TestEC2_SGRules_DescribeSelectors(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.0.0.0/16")
	a := ec2CreateSG(t, ts, "a", "a", vpc)
	b := ec2CreateSG(t, ts, "b", "b", vpc)
	var cidrs []map[string]string
	for i := 1; i <= 6; i++ {
		cidrs = append(cidrs, map[string]string{
			"GroupId": a, "IpPermissions.1.IpProtocol": "tcp",
			"IpPermissions.1.FromPort": strconv.Itoa(i), "IpPermissions.1.ToPort": strconv.Itoa(i),
			"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
		})
	}
	var aRules []ec2SGRuleDoc
	for _, p := range cidrs {
		aRules = append(aRules, ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", p)...)
	}
	tagged := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": b, "IpPermissions.1.IpProtocol": "udp", "IpPermissions.1.FromPort": "53",
		"IpPermissions.1.ToPort": "53", "IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
		"TagSpecification.1.ResourceType": "security-group-rule",
		"TagSpecification.1.Tag.1.Key":    "Name", "TagSpecification.1.Tag.1.Value": "dns",
	})
	require.Len(t, tagged, 1)
	require.Len(t, tagged[0].Tags, 1)
	assert.Equal(t, "dns", tagged[0].Tags[0].Value)

	t.Run("by ID", func(t *testing.T) {
		got, _ := ec2DescribeSGRules(t, ts, map[string]string{
			"SecurityGroupRuleId.1": aRules[2].ID, "SecurityGroupRuleId.2": tagged[0].ID,
		})
		require.Len(t, got, 2)
		assert.ElementsMatch(t, []string{aRules[2].ID, tagged[0].ID}, []string{got[0].ID, got[1].ID})
	})
	t.Run("filters", func(t *testing.T) {
		assert.Len(t, ec2SGRulesOf(t, ts, a), 7, "six ingress and the default egress")
		got, _ := ec2DescribeSGRules(t, ts, ec2Filter("security-group-rule-id", aRules[0].ID))
		require.Len(t, got, 1)
		assert.Equal(t, aRules[0].ID, got[0].ID)
		got, _ = ec2DescribeSGRules(t, ts, ec2Filter("tag:Name", "dns"))
		require.Len(t, got, 1)
		assert.Equal(t, tagged[0].ID, got[0].ID)
	})
	t.Run("paging walks every rule once", func(t *testing.T) {
		all, next := ec2DescribeSGRules(t, ts, map[string]string{})
		assert.Empty(t, next)
		var walked []string
		token := ""
		for {
			params := map[string]string{"MaxResults": "5"}
			if token != "" {
				params["NextToken"] = token
			}
			page, nextToken := ec2DescribeSGRules(t, ts, params)
			assert.LessOrEqual(t, len(page), 5)
			for _, r := range page {
				walked = append(walked, r.ID)
			}
			if nextToken == "" {
				break
			}
			token = nextToken
		}
		require.Len(t, walked, len(all))
		for i := range all {
			assert.Equal(t, all[i].ID, walked[i])
		}
	})
	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"unknown ID", map[string]string{"SecurityGroupRuleId.1": "sgr-00000000000000000"}, "InvalidSecurityGroupRuleId.NotFound"},
		{"malformed ID", map[string]string{"SecurityGroupRuleId.1": "rule-1"}, "InvalidSecurityGroupRuleId.Malformed"},
		{"IDs with MaxResults", map[string]string{"SecurityGroupRuleId.1": aRules[0].ID, "MaxResults": "5"}, "InvalidParameterCombination"},
		{"MaxResults below range", map[string]string{"MaxResults": "4"}, "InvalidParameterValue"},
		{"a token not issued", map[string]string{"NextToken": "bogus"}, "InvalidParameterValue"},
		{"an undocumented filter", ec2Filter("tag-key", "Name"), "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "DescribeSecurityGroupRules"
			status, code := ec2ErrorCode(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

func TestEC2_SGRules_RevokeByID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.0.0.0/16")
	group := ec2CreateSG(t, ts, "web", "web", vpc)
	added := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.1.0.0/16", "IpPermissions.1.IpRanges.2.CidrIp": "10.2.0.0/16",
	})
	require.Len(t, added, 2)
	var egressID string
	for _, r := range ec2SGRulesOf(t, ts, group) {
		if r.IsEgress {
			egressID = r.ID
		}
	}
	require.NotEmpty(t, egressID)

	var revoked ec2RevokeResponse
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": group, "SecurityGroupRuleId.1": added[0].ID,
	}, &revoked)
	assert.True(t, revoked.Return)
	require.Len(t, revoked.Revoked, 1)
	assert.Equal(t, added[0].ID, revoked.Revoked[0].ID)
	assert.Equal(t, "10.1.0.0/16", revoked.Revoked[0].CidrIPv4)
	assert.Equal(t, group, revoked.Revoked[0].GroupID)
	assert.Equal(t, []string{"10.2.0.0/16"}, ec2SSHSources(t, ts, group), "only the named rule is revoked")

	for _, tc := range []struct {
		name, action, id, code string
	}{
		{"a revoked rule", "RevokeSecurityGroupIngress", added[0].ID, "InvalidSecurityGroupRuleId.NotFound"},
		{"an egress rule named on ingress", "RevokeSecurityGroupIngress", egressID, "InvalidSecurityGroupRuleId.NotFound"},
		{"a malformed ID", "RevokeSecurityGroupIngress", "sgr-XYZ", "InvalidSecurityGroupRuleId.Malformed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ec2ErrorCode(t, ts, map[string]string{
				"Action": tc.action, "GroupId": group, "SecurityGroupRuleId.1": tc.id,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}

	// The egress rule revokes through its own operation.
	var egress ec2RevokeResponse
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupEgress", "GroupId": group, "SecurityGroupRuleId.1": egressID,
	}, &egress)
	require.Len(t, egress.Revoked, 1)
	assert.True(t, egress.Revoked[0].IsEgress)

	// A revoke by permission reports the rules it removed too.
	var byPermission ec2RevokeResponse
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.2.0.0/16",
	}, &byPermission)
	require.Len(t, byPermission.Revoked, 1)
	assert.Equal(t, added[1].ID, byPermission.Revoked[0].ID)
	assert.Empty(t, ec2SGRulesOf(t, ts, group))
}

// TestEC2_SGRules_RevokeAbsentEgress pins #1658's EC2 half: revoking an egress rule the group does
// not hold is InvalidPermission.NotFound outside the default VPC, and a success naming the rule in
// unknownIpPermissionSet inside it.
func TestEC2_SGRules_RevokeAbsentEgress(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ipv6AllEgress := func(group string) map[string]string {
		return map[string]string{
			"Action": "RevokeSecurityGroupEgress", "GroupId": group, "IpPermissions.1.IpProtocol": "-1",
			"IpPermissions.1.FromPort": "0", "IpPermissions.1.ToPort": "0",
			"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "::/0",
		}
	}

	nondefault := ec2CreateSG(t, ts, "tf", "tf", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	status, code := ec2ErrorCode(t, ts, ipv6AllEgress(nondefault))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPermission.NotFound", code, "the code the Terraform provider tolerates")
	assert.Len(t, ec2SGRulesOf(t, ts, nondefault), 1, "nothing is revoked")

	inDefault := ec2CreateSG(t, ts, "tf", "tf", "")
	var resp ec2RevokeResponse
	ec2FleetXML(t, ts, ipv6AllEgress(inDefault), &resp)
	assert.True(t, resp.Return)
	require.Len(t, resp.Unknown, 1)
	assert.Equal(t, "-1", resp.Unknown[0].Protocol)
	assert.Empty(t, resp.Revoked)

	// The IPv4 default egress rule the provider revokes first is held, and revokes.
	ipv4 := ipv6AllEgress(nondefault)
	delete(ipv4, "IpPermissions.1.Ipv6Ranges.1.CidrIpv6")
	ipv4["IpPermissions.1.IpRanges.1.CidrIp"] = "0.0.0.0/0"
	var held ec2RevokeResponse
	ec2FleetXML(t, ts, ipv4, &held)
	require.Len(t, held.Revoked, 1)
	assert.Equal(t, "0.0.0.0/0", held.Revoked[0].CidrIPv4)
}

func TestEC2_SGRules_Modify(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.0.0.0/16")
	group := ec2CreateSG(t, ts, "web", "web", vpc)
	peer := ec2CreateSG(t, ts, "peer", "peer", vpc)
	authorize := func(cidr string) ec2SGRuleDoc {
		rules := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
			"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
			"IpPermissions.1.FromPort": "80", "IpPermissions.1.ToPort": "80",
			"IpPermissions.1.IpRanges.1.CidrIp": cidr,
		})
		require.Len(t, rules, 1)
		return rules[0]
	}
	rule := authorize("10.1.0.0/16")
	other := authorize("10.9.0.0/16")
	ec2TagResource(t, ts, rule.ID, "Env", "prod")

	modify := func(id string, body map[string]string) map[string]string {
		params := map[string]string{
			"Action": "ModifySecurityGroupRules", "GroupId": group,
			"SecurityGroupRule.1.SecurityGroupRuleId": id,
		}
		for k, v := range body {
			params["SecurityGroupRule.1.SecurityGroupRule."+k] = v
		}
		return params
	}
	var ok struct {
		Return bool `xml:"return"`
	}
	ec2FleetXML(t, ts, modify(rule.ID, map[string]string{
		"IpProtocol": "tcp", "FromPort": "8080", "ToPort": "8080",
		"CidrIpv4": "10.2.0.9/16", "Description": "moved",
	}), &ok)
	assert.True(t, ok.Return)

	got, _ := ec2DescribeSGRules(t, ts, map[string]string{"SecurityGroupRuleId.1": rule.ID})
	require.Len(t, got, 1, "the rule keeps its ID")
	assert.Equal(t, 8080, got[0].FromPort)
	assert.Equal(t, "10.2.0.0/16", got[0].CidrIPv4)
	assert.Equal(t, "moved", got[0].Description)
	require.Len(t, got[0].Tags, 1, "the rule keeps its tags")
	assert.Equal(t, "prod", got[0].Tags[0].Value)

	// The old content is free again, and a rule authorized with it is another rule.
	again := authorize("10.1.0.0/16")
	assert.NotEqual(t, rule.ID, again.ID)
	got, _ = ec2DescribeSGRules(t, ts, map[string]string{"SecurityGroupRuleId.1": rule.ID})
	require.Len(t, got, 1)
	assert.Equal(t, "10.2.0.0/16", got[0].CidrIPv4, "and the modified rule is still the one its ID names")

	// A referenced group is a source as well.
	ec2FleetXML(t, ts, modify(rule.ID, map[string]string{
		"IpProtocol": "tcp", "FromPort": "8080", "ToPort": "8080", "ReferencedGroupId": peer,
	}), &ok)
	got, _ = ec2DescribeSGRules(t, ts, map[string]string{"SecurityGroupRuleId.1": rule.ID})
	require.Len(t, got, 1)
	assert.Equal(t, peer, got[0].RefGroupID)

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"onto another rule's content", modify(rule.ID, map[string]string{
			"IpProtocol": "tcp", "FromPort": "80", "ToPort": "80", "CidrIpv4": "10.9.0.0/16",
		}), "InvalidPermission.Duplicate"},
		{"an unknown rule", modify("sgr-00000000000000000", map[string]string{
			"IpProtocol": "tcp", "CidrIpv4": "10.3.0.0/16",
		}), "InvalidSecurityGroupRuleId.NotFound"},
		{"a malformed rule ID", modify("nope", map[string]string{
			"IpProtocol": "tcp", "CidrIpv4": "10.3.0.0/16",
		}), "InvalidSecurityGroupRuleId.Malformed"},
		{"two sources", modify(rule.ID, map[string]string{
			"IpProtocol": "tcp", "CidrIpv4": "10.3.0.0/16", "CidrIpv6": "2001:db8::/32",
		}), "InvalidParameterCombination"},
		{"no source", modify(rule.ID, map[string]string{"IpProtocol": "tcp"}), "MissingParameter"},
		{"no protocol", modify(rule.ID, map[string]string{"CidrIpv4": "10.3.0.0/16"}), "MissingParameter"},
		{"a malformed CIDR", modify(rule.ID, map[string]string{
			"IpProtocol": "tcp", "CidrIpv4": "10.3.0.0",
		}), "InvalidParameterValue"},
		{"no rules", map[string]string{"Action": "ModifySecurityGroupRules", "GroupId": group}, "MissingParameter"},
		{"no group", map[string]string{"Action": "ModifySecurityGroupRules"}, "MissingParameter"},
		{"an unknown group", map[string]string{
			"Action": "ModifySecurityGroupRules", "GroupId": "sg-00000000000000000",
		}, "InvalidGroup.NotFound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ec2ErrorCode(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
	got, _ = ec2DescribeSGRules(t, ts, map[string]string{"SecurityGroupRuleId.1": other.ID})
	require.Len(t, got, 1, "a refused modify writes nothing")
	assert.Equal(t, "10.9.0.0/16", got[0].CidrIPv4)
}

func TestEC2_SGRules_Tags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	rules := ec2SGRulesOf(t, ts, group)
	require.Len(t, rules, 1)
	id := rules[0].ID

	ec2TagResource(t, ts, id, "Name", "egress")
	ec2TagResource(t, ts, id, "Team", "net")
	got, _ := ec2DescribeSGRules(t, ts, ec2Filter("tag:Team", "net"))
	require.Len(t, got, 1)
	assert.Len(t, got[0].Tags, 2)

	resp := ec2Request(t, ts, map[string]string{
		"Action": "DeleteTags", "ResourceId.1": id, "Tag.1.Key": "Team",
	})
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, _ = ec2DescribeSGRules(t, ts, map[string]string{"SecurityGroupRuleId.1": id})
	require.Len(t, got, 1)
	require.Len(t, got[0].Tags, 1)
	assert.Equal(t, "Name", got[0].Tags[0].Key)

	// A tag on an sgr- ID naming no rule is a no-op, as for any absent resource.
	resp = ec2Request(t, ts, map[string]string{
		"Action": "CreateTags", "ResourceId.1": "sgr-00000000000000000", "Tag.1.Key": "k", "Tag.1.Value": "v",
	})
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Revoking the rule takes its tags with it: re-authorizing the content starts untagged.
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupEgress", "GroupId": group, "SecurityGroupRuleId.1": id,
	}, nil)
	readded := ec2Authorize(t, ts, "AuthorizeSecurityGroupEgress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "-1", "IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})
	require.Len(t, readded, 1)
	assert.Empty(t, readded[0].Tags)
}

// TestEC2_SGRules_TerraformRuleResource mirrors aws_vpc_security_group_ingress_rule (provider
// v5) over raw HTTP: create by AuthorizeSecurityGroupIngress with tags, read by
// DescribeSecurityGroupRules SecurityGroupRuleId.1, update by ModifySecurityGroupRules, and delete
// by RevokeSecurityGroupIngress SecurityGroupRuleId.1.
func TestEC2_SGRules_TerraformRuleResource(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "tf", "tf", ec2CreateVPC(t, ts, "10.0.0.0/16"))

	created := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.IpRanges.1.CidrIp":      "10.0.0.0/8",
		"IpPermissions.1.IpRanges.1.Description": "https",
		"TagSpecification.1.ResourceType":        "security-group-rule",
		"TagSpecification.1.Tag.1.Key":           "Name", "TagSpecification.1.Tag.1.Value": "https",
	})
	require.Len(t, created, 1)
	id := created[0].ID

	read := func() ec2SGRuleDoc {
		got, _ := ec2DescribeSGRules(t, ts, map[string]string{"SecurityGroupRuleId.1": id})
		require.Len(t, got, 1)
		return got[0]
	}
	r := read()
	assert.Equal(t, "https", r.Description)
	assert.Equal(t, "10.0.0.0/8", r.CidrIPv4)

	ec2FleetXML(t, ts, map[string]string{
		"Action": "ModifySecurityGroupRules", "GroupId": group,
		"SecurityGroupRule.1.SecurityGroupRuleId":           id,
		"SecurityGroupRule.1.SecurityGroupRule.IpProtocol":  "tcp",
		"SecurityGroupRule.1.SecurityGroupRule.FromPort":    "443",
		"SecurityGroupRule.1.SecurityGroupRule.ToPort":      "443",
		"SecurityGroupRule.1.SecurityGroupRule.CidrIpv4":    "10.0.0.0/8",
		"SecurityGroupRule.1.SecurityGroupRule.Description": "https, edited",
	}, nil)
	assert.Equal(t, "https, edited", read().Description)

	resp := ec2Request(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": group, "SecurityGroupRuleId.1": id,
	})
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	var revoked ec2RevokeResponse
	require.NoError(t, xml.Unmarshal(body, &revoked))
	require.Len(t, revoked.Revoked, 1)

	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action": "DescribeSecurityGroupRules", "SecurityGroupRuleId.1": id,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidSecurityGroupRuleId.NotFound", code, "the provider's read-after-delete answer")
}

// TestEC2Authz_SGRuleTagsAreTheRulesOwn pins that CreateTags on an sgr- ID is decided against
// the rule's tags, not those of the group whose record holds it, in both directions: a grant
// keyed on the rule's tag allows, and one keyed on the group's tag does not.
func TestEC2Authz_SGRuleTagsAreTheRulesOwn(t *testing.T) {
	t.Parallel()
	const group = "sg-0aaa1111bbbb2222c"
	sg := emulator.EC2SecurityGroup{
		GroupID: group, AccountID: ec2AuthzAccount, Region: ec2AuthzRegion,
		Tags: []emulator.EC2Tag{{Key: "Env", Value: "prod"}},
		IngressRules: []emulator.EC2IPPermission{{
			IPProtocol: "tcp", FromPort: 22, ToPort: 22, IPRanges: []string{"10.0.0.0/8"},
		}},
	}
	emulator.EC2TagSecurityGroupRuleForTest(&sg, 0, []emulator.EC2Tag{{Key: "Env", Value: "dev"}})
	ruleID := emulator.EC2SecurityGroupRuleIDsForTest(sg)[0]
	params := map[string]string{"ResourceId.1": ruleID, "Tag.1.Key": "Owner", "Tag.1.Value": "me"}

	for _, tc := range []struct {
		name, env string
		allowed   bool
	}{
		{"a grant on the rule's tag allows", "dev", true},
		{"a grant on the group's tag does not", "prod", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newEC2AuthzFixture(t, "rule-tagger", emulator.PolicyDocument{})
			f.put(t, "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+group, sg)
			f.setPolicy(t, ec2AuthzConditionStatement("ec2:CreateTags",
				ec2AuthzTagCondition("StringEquals", "aws:ResourceTag/Env", tc.env)))
			err := f.call(t, "CreateTags", params)
			if tc.allowed {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "security-group-rule/"+ruleID)
		})
	}
}
