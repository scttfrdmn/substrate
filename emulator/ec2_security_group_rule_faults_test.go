package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2GetFaultState faults Get for keys under failGet.
type ec2GetFaultState struct {
	emulator.StateManager
	failGet string
}

func (m *ec2GetFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.failGet != "" && strings.HasPrefix(key, m.failGet) {
		return nil, errors.New("ec2 test store fault")
	}
	return m.StateManager.Get(ctx, namespace, key)
}

// TestEC2_SGRules_TagGetFaultRefuses pins that a group record that cannot be read while an sgr-
// ID is resolved fails CreateTags, and that authorization refuses rather than deciding a
// tag-conditioned Deny against no tags.
func TestEC2_SGRules_TagGetFaultRefuses(t *testing.T) {
	t.Parallel()

	t.Run("handler", func(t *testing.T) {
		t.Parallel()
		fault := &ec2GetFaultState{StateManager: emulator.NewMemoryStateManager()}
		ts := newEC2TestServerWithState(t, fault)
		group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
		id := ec2SGRulesOf(t, ts, group)[0].ID
		fault.failGet = "sg:"
		resp := ec2Request(t, ts, map[string]string{
			"Action": "CreateTags", "ResourceId.1": id, "Tag.1.Key": "k", "Tag.1.Value": "v",
		})
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("authorization", func(t *testing.T) {
		t.Parallel()
		f := newEC2AuthzFixture(t, "rule-tagger", emulator.PolicyDocument{})
		sg := emulator.EC2SecurityGroup{
			GroupID: "sg-0aaa1111bbbb2222c", AccountID: ec2AuthzAccount, Region: ec2AuthzRegion,
			IngressRules: []emulator.EC2IPPermission{{
				IPProtocol: "tcp", FromPort: 22, ToPort: 22, IPRanges: []string{"10.0.0.0/8"},
			}},
		}
		emulator.EC2TagSecurityGroupRuleForTest(&sg, 0, []emulator.EC2Tag{{Key: "Env", Value: "prod"}})
		raw, err := json.Marshal(sg)
		require.NoError(t, err)
		require.NoError(t, f.state.Put(t.Context(), "ec2", "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+sg.GroupID, raw))
		f.setPolicy(t,
			emulator.PolicyStatement{Effect: "Allow", Action: emulator.StringOrSlice{"ec2:CreateTags"}, Resource: emulator.StringOrSlice{"*"}},
			emulator.PolicyStatement{
				Effect: "Deny", Action: emulator.StringOrSlice{"ec2:CreateTags"}, Resource: emulator.StringOrSlice{"*"},
				Condition: ec2AuthzTagCondition("StringEquals", "aws:ResourceTag/Env", "prod"),
			})
		faulty := &ec2GetFaultState{StateManager: f.state, failGet: "sg:"}
		auth := emulator.NewAuthController(faulty, emulator.NewDefaultLogger(slog.LevelError, false))
		err = auth.CheckAccess(newAuthTestReqCtx("arn:aws:iam::"+ec2AuthzAccount+":user/rule-tagger"), &emulator.AWSRequest{
			Service: "ec2", Operation: "CreateTags", Path: "/",
			Params: map[string]string{
				"ResourceId.1": emulator.EC2SecurityGroupRuleIDsForTest(sg)[0], "Tag.1.Key": "k", "Tag.1.Value": "v",
			},
		})
		require.Error(t, err, "an unreadable group refuses the call rather than allowing it")
	})
}

// TestEC2_SGRules_SourcelessPermissionBesideAnID is the review's case: tcp/22 holding 10.0.0.0/8
// and 0.0.0.0/0, revoked with a sourceless tcp/22 permission and the 0.0.0.0/0 rule's ID. The rule
// the ID names is revoked first, so it is never reported unknown.
func TestEC2_SGRules_SourcelessPermissionBesideAnID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.0.0.0/16"))
	added := ec2Authorize(t, ts, "AuthorizeSecurityGroupIngress", map[string]string{
		"GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8", "IpPermissions.1.IpRanges.2.CidrIp": "0.0.0.0/0",
	})
	require.Len(t, added, 2)
	var resp ec2RevokeResponse
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": group, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
		"SecurityGroupRuleId.1": added[1].ID,
	}, &resp)
	assert.Empty(t, resp.Unknown)
	ids := map[string]bool{}
	for _, r := range resp.Revoked {
		ids[r.ID] = true
	}
	assert.True(t, ids[added[1].ID], "the rule the ID names is revoked")
}

// TestEC2_SGRules_UnresolvableGroupName pins that a same-account referenced group named by a
// GroupName that resolves to no group is InvalidGroup.NotFound on authorize, in both the
// IpPermissions and the legacy SourceSecurityGroupName forms.
func TestEC2_SGRules_UnresolvableGroupName(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	group := ec2CreateSG(t, ts, "web", "web", "")
	for name, params := range map[string]map[string]string{
		"IpPermissions": {
			"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
			"IpPermissions.1.Groups.1.GroupName": "nope",
		},
		"SourceSecurityGroupName": {"SourceSecurityGroupName": "nope"},
	} {
		t.Run(name, func(t *testing.T) {
			params["Action"] = "AuthorizeSecurityGroupIngress"
			params["GroupId"] = group
			status, code := ec2ErrorCode(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidGroup.NotFound", code)
		})
	}
}
