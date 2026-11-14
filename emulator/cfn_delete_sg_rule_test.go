package emulator_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfnSweepSGRuleTemplate is a group in a VPC of the stack's own with one standalone ingress
// rule, so the rule's revoke happens outside the default VPC — where an absent rule is
// InvalidPermission.NotFound (#1453).
const cfnSweepSGRuleTemplate = `{
  "Resources": {
    "Net": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.40.0.0/16"}},
    "Group": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
      "GroupDescription": "sweep", "VpcId": {"Ref": "Net"}}},
    "Ssh": {"Type": "AWS::EC2::SecurityGroupIngress", "Properties": {
      "GroupId": {"Ref": "Group"}, "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
      "CidrIp": "10.0.0.0/8"}}
  }
}`

// TestCFN_SweepToleratesARevokedSecurityGroupRule pins that a stack whose standalone ingress
// rule was revoked out of band still deletes: the sweep's revoke answers
// InvalidPermission.NotFound, which [cfnDeleteAbsentCodes] reads as "already gone".
func TestCFN_SweepToleratesARevokedSecurityGroupRule(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, cfnSweepSGRuleTemplate, "sgrule", nil)
	require.NoError(t, err)
	groupID := ""
	for _, r := range result.Resources {
		if r.LogicalID == "Group" {
			groupID = r.PhysicalID
		}
	}
	require.NotEmpty(t, groupID)

	revoke := &emulator.AWSRequest{
		Service: "ec2", Operation: "RevokeSecurityGroupIngress", Headers: map[string]string{},
		Params: map[string]string{
			"Action": "RevokeSecurityGroupIngress", "GroupId": groupID,
			"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
			"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
		},
	}
	resp, err := d.DispatchForTest(ctx, revoke, "sgrule")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, err = d.DispatchForTest(ctx, revoke, "sgrule")
	require.Error(t, err, "a second revoke of the same rule is refused")
	require.Contains(t, err.Error(), "InvalidPermission.NotFound")

	assert.NoError(t, d.DeleteStack(ctx, "sgrule"), "an already-revoked rule is not a failed delete")
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks)
}

// TestCFN_SecurityGroupIngressCarriesBothCidrForms pins that CidrIpv6 reaches the group as an
// IPv6 range rather than overwriting CidrIp, so a rule declaring both carries both — and the
// sweep's revoke, built from the same properties, matches the rule it authorized.
func TestCFN_SecurityGroupIngressCarriesBothCidrForms(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	template := `{
  "Resources": {
    "Net": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.41.0.0/16"}},
    "Group": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
      "GroupDescription": "dual", "VpcId": {"Ref": "Net"}}},
    "Https": {"Type": "AWS::EC2::SecurityGroupIngress", "Properties": {
      "GroupId": {"Ref": "Group"}, "IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
      "CidrIp": "10.0.0.0/8", "CidrIpv6": "::/0"}}
  }
}`
	result, err := d.Deploy(ctx, template, "dualstack", nil)
	require.NoError(t, err)
	groupID := ""
	for _, r := range result.Resources {
		if r.LogicalID == "Group" {
			groupID = r.PhysicalID
		}
	}
	require.NotEmpty(t, groupID)

	describe := &emulator.AWSRequest{
		Service: "ec2", Operation: "DescribeSecurityGroups", Headers: map[string]string{},
		Params: map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": groupID},
	}
	resp, err := d.DispatchForTest(ctx, describe, "dualstack")
	require.NoError(t, err)
	body := string(resp.Body)
	assert.Contains(t, body, "<cidrIp>10.0.0.0/8</cidrIp>")
	assert.Contains(t, body, "<cidrIpv6>::/0</cidrIpv6>")

	assert.NoError(t, d.DeleteStack(ctx, "dualstack"))
}
