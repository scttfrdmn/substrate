package emulator_test

import (
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Edge cases of security group rule IDs (#1490): a rule named twice, a rule naming no source,
// refused modify inputs, store faults, and tag writes racing rule writes.

// TestEC2_SGRules_RevokeNamedTwice pins that a revoke naming one rule by IpPermissions.N and by
// SecurityGroupRuleId.N revokes it once, rather than reporting the second removal as unknown,
// which outside the default VPC refuses the whole call.
func TestEC2_SGRules_RevokeNamedTwice(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	added := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.1.0.0/16",
	})
	require.Len(t, added, 1)
	var resp ec2RevokeResponse
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.1.0.0/16", "SecurityGroupRuleId.1": added[0].ID,
	}, &resp)
	require.Len(t, resp.Revoked, 1)
	assert.Empty(t, resp.Unknown)
	assert.Empty(t, ec2SSHSources(t, ts, group))

	// The same for a rule naming no source, beside a sourced rule on its ports: naming it both
	// ways removes it, and only it.
	ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
	})
	sourceless := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
	})
	require.Len(t, sourceless, 1)
	var both ec2RevokeResponse
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"SecurityGroupRuleId.1": sourceless[0].ID,
	}, &both)
	require.Len(t, both.Revoked, 1)
	assert.Equal(t, sourceless[0].ID, both.Revoked[0].ID)
	assert.Equal(t, []string{"10.0.0.0/8"}, ec2SSHSources(t, ts, group))
}

// TestEC2_SGRules_ReferencedGroupMustExist pins that a rule naming a group of the account that
// does not exist is InvalidGroup.NotFound on authorize and on modify alike, and that a modify's
// rule ID is checked before its referenced group.
func TestEC2_SGRules_ReferencedGroupMustExist(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	const missing = "sg-00000000000000000"
	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.Groups.1.GroupId": missing,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidGroup.NotFound", code)

	modify := func(id string) map[string]string {
		return map[string]string{
			"Action": "ModifySecurityGroupRules", "GroupId": group,
			"SecurityGroupRule.1.SecurityGroupRuleId":                 id,
			"SecurityGroupRule.1.SecurityGroupRule.IpProtocol":        "tcp",
			"SecurityGroupRule.1.SecurityGroupRule.ReferencedGroupId": missing,
		}
	}
	_, code = ec2ErrorCode(t, ts, modify("bogus"))
	assert.Equal(t, "InvalidSecurityGroupRuleId.Malformed", code, "the rule ID is checked first")
	_, code = ec2ErrorCode(t, ts, modify("sgr-00000000000000000"))
	assert.Equal(t, "InvalidSecurityGroupRuleId.NotFound", code)
	_, code = ec2ErrorCode(t, ts, modify(ec2SGRulesOf(t, ts, group)[0].ID))
	assert.Equal(t, "InvalidGroup.NotFound", code)
}

// TestEC2Authz_SGRuleTagReadFaultRefuses pins that CreateTags on an sgr- ID is refused, not
// decided against an empty tag set, when the rule cannot be resolved: a tag-conditioned Deny
// must not fail open.
func TestEC2Authz_SGRuleTagReadFaultRefuses(t *testing.T) {
	t.Parallel()
	f := newEC2AuthzFixture(t, "rule-tagger", emulator.PolicyDocument{})
	f.setPolicy(t,
		emulator.PolicyStatement{Effect: "Allow", Action: emulator.StringOrSlice{"ec2:CreateTags"}, Resource: emulator.StringOrSlice{"*"}},
		emulator.PolicyStatement{
			Effect: "Deny", Action: emulator.StringOrSlice{"ec2:CreateTags"}, Resource: emulator.StringOrSlice{"*"},
			Condition: ec2AuthzTagCondition("StringEquals", "aws:ResourceTag/Env", "prod"),
		})
	faulty := &ecrListFaultState{StateManager: f.state, failList: "sg:"}
	auth := emulator.NewAuthController(faulty, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := newAuthTestReqCtx("arn:aws:iam::" + ec2AuthzAccount + ":user/rule-tagger")
	err := auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service: "ec2", Operation: "CreateTags", Path: "/",
		Params: map[string]string{"ResourceId.1": "sgr-0123456789abcdef0", "Tag.1.Key": "k", "Tag.1.Value": "v"},
	})
	require.Error(t, err, "an unreadable rule refuses the call rather than allowing it")
}

// TestEC2_SGRules_SourcelessRuleByID pins that revoking or modifying a rule naming no source, by
// its ID, touches that rule and not a sourced rule on the same protocol and ports stored first.
func TestEC2_SGRules_SourcelessRuleByID(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"revoke", "modify"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
			ssh := func(extra map[string]string) map[string]string {
				p := map[string]string{
					"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
					"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
				}
				for k, v := range extra {
					p[k] = v
				}
				return p
			}
			ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", ssh(map[string]string{
				"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
			}))
			sourceless := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", ssh(nil))
			require.Len(t, sourceless, 1)
			require.Empty(t, sourceless[0].CidrIPv4)

			if op == "revoke" {
				ec2FleetXML(t, ts, map[string]string{
					"Action": "RevokeSecurityGroupIngress", "GroupId": group,
					"SecurityGroupRuleId.1": sourceless[0].ID,
				}, nil)
			} else {
				ec2FleetXML(t, ts, map[string]string{
					"Action": "ModifySecurityGroupRules", "GroupId": group,
					"SecurityGroupRule.1.SecurityGroupRuleId":          sourceless[0].ID,
					"SecurityGroupRule.1.SecurityGroupRule.IpProtocol": "tcp",
					"SecurityGroupRule.1.SecurityGroupRule.FromPort":   "22",
					"SecurityGroupRule.1.SecurityGroupRule.ToPort":     "22",
					"SecurityGroupRule.1.SecurityGroupRule.CidrIpv4":   "192.168.0.0/16",
				}, nil)
			}
			sources := ec2SSHSources(t, ts, group)
			assert.Contains(t, sources, "10.0.0.0/8", "the sourced rule stored first is untouched")
			if op == "modify" {
				assert.Contains(t, sources, "192.168.0.0/16")
			}
			for _, r := range ec2SGRulesOf(t, ts, group) {
				if !r.IsEgress {
					assert.NotEmpty(t, r.CidrIPv4, "no sourceless ingress rule remains")
				}
			}
		})
	}
}

// TestEC2_SGRules_ModifyRefusesBadPortsAndGroups covers the two modify inputs that name nothing
// a rule can hold: a port that is not an integer, and a referenced group that does not exist.
func TestEC2_SGRules_ModifyRefusesBadPortsAndGroups(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	id := ec2SGRulesOf(t, ts, group)[0].ID
	for _, tc := range []struct {
		name string
		body map[string]string
		code string
	}{
		{"a port that is not an integer", map[string]string{"FromPort": "ssh", "CidrIpv4": "10.0.0.0/8"}, "InvalidParameterValue"},
		{"a referenced group that does not exist", map[string]string{"ReferencedGroupId": "sg-00000000000000000"}, "InvalidGroup.NotFound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{
				"Action": "ModifySecurityGroupRules", "GroupId": group,
				"SecurityGroupRule.1.SecurityGroupRuleId":          id,
				"SecurityGroupRule.1.SecurityGroupRule.IpProtocol": "tcp",
			}
			for k, v := range tc.body {
				params["SecurityGroupRule.1.SecurityGroupRule."+k] = v
			}
			status, code := ec2ErrorCode(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

// TestEC2_SGRules_TagListFaultIsAnError pins that a store fault while resolving an sgr- ID fails
// CreateTags rather than answering success having written nothing.
func TestEC2_SGRules_TagListFaultIsAnError(t *testing.T) {
	t.Parallel()
	fault := &ecrListFaultState{StateManager: emulator.NewMemoryStateManager()}
	ts := newEC2TestServerWithState(t, fault)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	id := ec2SGRulesOf(t, ts, group)[0].ID
	fault.failList = "sg:"
	resp := ec2Request(t, ts, map[string]string{
		"Action": "CreateTags", "ResourceId.1": id, "Tag.1.Key": "k", "Tag.1.Value": "v",
	})
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestEC2_SecurityGroupTagsAndRulesDoNotLoseWrites runs CreateTags on a group alongside
// authorizes on it. Both rewrite the group's record, so each must see the other's write: every
// rule and every tag survives.
func TestEC2_SecurityGroupTagsAndRulesDoNotLoseWrites(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	const n = 12
	var wg sync.WaitGroup
	statuses := make(chan string, 2*n)
	send := func(label string, params map[string]string) {
		defer wg.Done()
		resp := ec2Request(t, ts, params)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			statuses <- label + ": " + resp.Status
		}
	}
	for i := range n {
		port := strconv.Itoa(1000 + i)
		key := "k" + strconv.Itoa(i)
		wg.Add(2)
		go send("authorize "+port, map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
			"IpPermissions.1.FromPort": port, "IpPermissions.1.ToPort": port,
			"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
		})
		go send("tag "+key, map[string]string{
			"Action": "CreateTags", "ResourceId.1": group, "Tag.1.Key": key, "Tag.1.Value": "v",
		})
	}
	wg.Wait()
	close(statuses)
	for s := range statuses {
		t.Error(s)
	}
	rules := 0
	for _, r := range ec2SGRulesOf(t, ts, group) {
		if !r.IsEgress {
			rules++
		}
	}
	assert.Equal(t, n, rules, "no authorize was lost to a tag write")
	var doc struct {
		Tags []struct {
			Key string `xml:"key"`
		} `xml:"securityGroupInfo>item>tagSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": group}, &doc)
	assert.Len(t, doc.Tags, n, "no tag was lost to an authorize")
}
