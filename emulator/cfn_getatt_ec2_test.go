package emulator_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCFN_StandaloneIngressReadsGroupIDThroughGetAtt is the form CDK emits for a security group's
// rule: a standalone AWS::EC2::SecurityGroupIngress whose GroupId is `!GetAtt SG.GroupId`. The
// attribute resolved to "", so the rule's AuthorizeSecurityGroupIngress was refused with
// MissingParameter. The group is in the default VPC, where Ref answers the group's name and the
// attribute is the only way to the ID.
func TestCFN_StandaloneIngressReadsGroupIDThroughGetAtt(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), `{"Resources":{
		"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web"}},
		"Rule":{"Type":"AWS::EC2::SecurityGroupIngress","Properties":{
			"GroupId":{"Fn::GetAtt":["SG","GroupId"]},
			"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIp":"0.0.0.0/0"}}}}`, "getatt-ingress", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status, result.StatusReason)
	ingress, _ := v129SGRules(t, d, resourceByLogicalID(t, result, "SG").PhysicalID)
	assert.Equal(t, []string{"tcp 443-443 0.0.0.0/0"}, ingress)
}

// TestCFN_EC2AttributesResolve pins each EC2 attribute #1545's sweep added against the value EC2
// reports, so Fn::GetAtt answers what the service does rather than "".
func TestCFN_EC2AttributesResolve(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), `{"Resources":{
		"Net":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.0.0.0/16"}},
		"Sub":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Net"},"CidrBlock":"10.0.1.0/24",
			"AvailabilityZone":"us-east-1b"}},
		"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"x","VpcId":{"Ref":"Net"}}},
		"DefaultVPCGroup":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"x"}},
		"IGW":{"Type":"AWS::EC2::InternetGateway"},
		"RT":{"Type":"AWS::EC2::RouteTable","Properties":{"VpcId":{"Ref":"Net"}}},
		"Inst":{"Type":"AWS::EC2::Instance","Properties":{"InstanceType":"t3.micro","SubnetId":{"Ref":"Sub"},
			"ImageId":"resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"}}},
	"Outputs":{
		"VpcId":{"Value":{"Fn::GetAtt":["Net","VpcId"]}},
		"VpcCidr":{"Value":{"Fn::GetAtt":["Net","CidrBlock"]}},
		"VpcDefaultSG":{"Value":{"Fn::GetAtt":["Net","DefaultSecurityGroup"]}},
		"SubnetId":{"Value":{"Fn::GetAtt":["Sub","SubnetId"]}},
		"SubnetVpc":{"Value":{"Fn::GetAtt":["Sub","VpcId"]}},
		"SubnetCidr":{"Value":{"Fn::GetAtt":["Sub","CidrBlock"]}},
		"SubnetAZ":{"Value":{"Fn::GetAtt":["Sub","AvailabilityZone"]}},
		"SubnetAZId":{"Value":{"Fn::GetAtt":["Sub","AvailabilityZoneId"]}},
		"GroupId":{"Value":{"Fn::GetAtt":["SG","GroupId"]}},
		"GroupVpc":{"Value":{"Fn::GetAtt":["SG","VpcId"]}},
		"DefaultVPCGroupVpc":{"Value":{"Fn::GetAtt":["DefaultVPCGroup","VpcId"]}},
		"GatewayId":{"Value":{"Fn::GetAtt":["IGW","InternetGatewayId"]}},
		"RouteTableId":{"Value":{"Fn::GetAtt":["RT","RouteTableId"]}},
		"InstanceId":{"Value":{"Fn::GetAtt":["Inst","InstanceId"]}},
		"InstanceVpc":{"Value":{"Fn::GetAtt":["Inst","VpcId"]}},
		"InstanceIp":{"Value":{"Fn::GetAtt":["Inst","PrivateIp"]}},
		"InstanceDns":{"Value":{"Fn::GetAtt":["Inst","PrivateDnsName"]}}}}`, "getatt-ec2", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status, result.StatusReason)
	out := result.Outputs
	vpc := resourceByLogicalID(t, result, "Net").PhysicalID
	for name, want := range map[string]string{
		"VpcId": vpc, "VpcCidr": "10.0.0.0/16",
		"SubnetId": resourceByLogicalID(t, result, "Sub").PhysicalID, "SubnetVpc": vpc,
		"SubnetCidr": "10.0.1.0/24", "SubnetAZ": "us-east-1b", "SubnetAZId": "use1-az2",
		"GroupId": resourceByLogicalID(t, result, "SG").PhysicalID, "GroupVpc": vpc,
		"GatewayId":    resourceByLogicalID(t, result, "IGW").PhysicalID,
		"RouteTableId": resourceByLogicalID(t, result, "RT").PhysicalID,
		"InstanceId":   resourceByLogicalID(t, result, "Inst").PhysicalID, "InstanceVpc": vpc,
	} {
		assert.Equal(t, want, out[name], name)
	}
	assert.Regexp(t, regexp.MustCompile(`^sg-[0-9a-f]+$`), out["VpcDefaultSG"])
	assert.NotEqual(t, out["GroupId"], out["VpcDefaultSG"], "the VPC's own default group, not the stack's")
	assert.Regexp(t, regexp.MustCompile(`^vpc-[0-9a-f]+$`), out["DefaultVPCGroupVpc"])
	assert.NotEqual(t, vpc, out["DefaultVPCGroupVpc"], "a group declared with no VpcId is in the default VPC")
	assert.Regexp(t, regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`), out["InstanceIp"])
	assert.NotEmpty(t, out["InstanceDns"])
}
