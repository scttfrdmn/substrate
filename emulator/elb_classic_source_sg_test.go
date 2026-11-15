package emulator_test

import (
	"context"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCFN_ClassicELBSourceSecurityGroupResolves pins #1421: a classic load balancer in a VPC
// records a SourceSecurityGroup — its first security group, or the VPC's default group when the
// create names none — and reports it through DescribeLoadBalancers and Fn::GetAtt.
func TestCFN_ClassicELBSourceSecurityGroupResolves(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const tmpl = `{
	"Resources": {
		"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.40.0.0/16"}},
		"Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {"VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.40.1.0/24"}},
		"Web": {"Type": "AWS::EC2::SecurityGroup", "Properties": {"GroupName": "web-elb",
			"GroupDescription": "the load balancer's group", "VpcId": {"Ref": "Vpc"}}},
		"Named": {"Type": "AWS::ElasticLoadBalancing::LoadBalancer", "Properties": {
			"LoadBalancerName": "with-group", "Subnets": [{"Ref": "Subnet"}],
			"SecurityGroups": [{"Ref": "Web"}],
			"Listeners": [{"Protocol": "HTTP", "LoadBalancerPort": "80", "InstancePort": "80"}]}},
		"Defaulted": {"Type": "AWS::ElasticLoadBalancing::LoadBalancer", "Properties": {
			"LoadBalancerName": "without-group", "Subnets": [{"Ref": "Subnet"}],
			"Listeners": [{"Protocol": "HTTP", "LoadBalancerPort": "80", "InstancePort": "80"}]}}
	},
	"Outputs": {
		"NamedGroup": {"Value": {"Fn::GetAtt": ["Named", "SourceSecurityGroup.GroupName"]}},
		"NamedOwner": {"Value": {"Fn::GetAtt": ["Named", "SourceSecurityGroup.OwnerAlias"]}},
		"DefaultGroup": {"Value": {"Fn::GetAtt": ["Defaulted", "SourceSecurityGroup.GroupName"]}}
	}
}`
	result, err := d.Deploy(context.Background(), tmpl, "classic-ssg", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	assert.Equal(t, "web-elb", result.Outputs["NamedGroup"])
	assert.Equal(t, "123456789012", result.Outputs["NamedOwner"], "a VPC load balancer's owner alias is the account")
	assert.Equal(t, "default", result.Outputs["DefaultGroup"], "a VPC load balancer created without a group gets the VPC's default")

	resp := cfnClassicELBQuery(t, d, "DescribeLoadBalancers", nil)
	var doc struct {
		Members []struct {
			Name           string   `xml:"LoadBalancerName"`
			VPCId          string   `xml:"VPCId"`
			SecurityGroups []string `xml:"SecurityGroups>member"`
			Source         *struct {
				GroupName  string `xml:"GroupName"`
				OwnerAlias string `xml:"OwnerAlias"`
			} `xml:"SourceSecurityGroup"`
		} `xml:"DescribeLoadBalancersResult>LoadBalancerDescriptions>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "%s", resp.Body)
	require.Len(t, doc.Members, 2)
	vpcID := resourceByLogicalID(t, result, "Vpc").PhysicalID
	for _, m := range doc.Members {
		assert.Equal(t, vpcID, m.VPCId, "%s reports the subnet's VPC", m.Name)
		require.NotNil(t, m.Source, "%s reports a SourceSecurityGroup", m.Name)
		assert.Equal(t, "123456789012", m.Source.OwnerAlias)
		require.Len(t, m.SecurityGroups, 1)
		switch m.Name {
		case "with-group":
			assert.Equal(t, resourceByLogicalID(t, result, "Web").PhysicalID, m.SecurityGroups[0])
			assert.Equal(t, "web-elb", m.Source.GroupName)
		case "without-group":
			assert.Equal(t, "default", m.Source.GroupName)
		}
	}
}
