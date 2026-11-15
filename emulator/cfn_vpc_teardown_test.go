package emulator_test

import (
	"context"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfnPublicVPCTemplate is the shape every public-subnet VPC template has: a VPC, a subnet, an
// internet gateway attached through AWS::EC2::VPCGatewayAttachment, a route table, and a security
// group declaring its own egress — the all-traffic rule CDK emits for allowAllOutbound, beside a
// narrower one.
const cfnPublicVPCTemplate = `{
  "Resources": {
    "Net": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.90.0.0/16"}},
    "Public": {"Type": "AWS::EC2::Subnet", "Properties": {"VpcId": {"Ref": "Net"}, "CidrBlock": "10.90.1.0/24"}},
    "Gateway": {"Type": "AWS::EC2::InternetGateway"},
    "Attach": {"Type": "AWS::EC2::VPCGatewayAttachment", "Properties": {
      "VpcId": {"Ref": "Net"}, "InternetGatewayId": {"Ref": "Gateway"}}},
    "Routes": {"Type": "AWS::EC2::RouteTable", "Properties": {"VpcId": {"Ref": "Net"}}},
    "AllOut": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
      "GroupDescription": "all out", "VpcId": {"Ref": "Net"},
      "SecurityGroupEgress": [{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0"}]}},
    "Narrow": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
      "GroupDescription": "narrow", "VpcId": {"Ref": "Net"},
      "SecurityGroupEgress": [{"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "CidrIp": "10.0.0.0/8"}]}}
  }
}`

// cfnDescribeEC2 routes an EC2 request through the deployer and decodes its XML into v.
func cfnDescribeEC2(t *testing.T, d *emulator.StackDeployer, params map[string]string, v any) {
	t.Helper()
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "ec2", Operation: params["Action"], Params: params, Headers: map[string]string{},
	}, "probe")
	require.NoError(t, err)
	require.NoError(t, xml.Unmarshal(resp.Body, v))
}

// TestCFN_PublicVPCDeploysAndTearsDown pins the two CloudFormation behaviors #1491 and #1492
// depend on: a VPCGatewayAttachment really attaches its gateway, and a group declaring egress loses
// the default all-traffic rule rather than duplicating it — and that a stack built in that order
// still deletes cleanly now that DeleteVpc and DeleteInternetGateway check dependents.
func TestCFN_PublicVPCDeploysAndTearsDown(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, cfnPublicVPCTemplate, "public", nil)
	require.NoError(t, err)
	ids := map[string]string{}
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, r.LogicalID)
		ids[r.LogicalID] = r.PhysicalID
	}

	var igws struct {
		VPCs []string `xml:"internetGatewaySet>item>attachmentSet>item>vpcId"`
	}
	cfnDescribeEC2(t, d, map[string]string{"Action": "DescribeInternetGateways", "InternetGatewayId.1": ids["Gateway"]}, &igws)
	assert.Equal(t, []string{ids["Net"]}, igws.VPCs, "the attachment is real")

	egress := func(groupID string) []string {
		var doc struct {
			Rules []struct {
				Protocol string `xml:"ipProtocol"`
				CIDR     string `xml:"ipRanges>item>cidrIp"`
			} `xml:"securityGroupInfo>item>ipPermissionsEgress>item"`
		}
		cfnDescribeEC2(t, d, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": groupID}, &doc)
		var out []string
		for _, r := range doc.Rules {
			out = append(out, r.Protocol+" "+r.CIDR)
		}
		return out
	}
	assert.Equal(t, []string{"-1 0.0.0.0/0"}, egress(ids["AllOut"]), "the declared rule replaces the default, once")
	assert.Equal(t, []string{"tcp 10.0.0.0/8"}, egress(ids["Narrow"]), "declared egress removes the default")

	require.NoError(t, d.DeleteStack(ctx, "public"))
	var vpcs struct {
		IDs []string `xml:"vpcSet>item>vpcId"`
	}
	cfnDescribeEC2(t, d, map[string]string{
		"Action": "DescribeVpcs", "Filter.1.Name": "vpc-id", "Filter.1.Value.1": ids["Net"],
	}, &vpcs)
	assert.Empty(t, vpcs.IDs, "the sweep detached the gateway, so the VPC deleted")
	var gone struct {
		IDs []string `xml:"internetGatewaySet>item>internetGatewayId"`
	}
	cfnDescribeEC2(t, d, map[string]string{
		"Action": "DescribeInternetGateways", "Filter.1.Name": "internet-gateway-id", "Filter.1.Value.1": ids["Gateway"],
	}, &gone)
	assert.Empty(t, gone.IDs)
}
