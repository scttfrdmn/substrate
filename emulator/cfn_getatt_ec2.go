package emulator

import (
	"context"
	"encoding/json"
)

// cfn_getatt_ec2.go answers the Fn::GetAtt attributes of the EC2 types CDK templates read most:
// the security group, VPC, subnet and instance attributes that only EC2's record holds. Each
// attribute and its meaning is from the type's "Return values" section in the Template Reference.
//
// An attribute that is the resource's own EC2 ID is answered from the physical ID in
// [cfnGetAttPerType] (cfnEC2IDAttribute). The rest are read from EC2's state once the resource has
// deployed and recorded in [DeployedResource.Metadata] under the published attribute name, where
// the generic metadata step answers them — the record is the deployer's view of the resource, and a
// value is fixed when the resource is created, as CloudFormation resolves an attribute when the
// resource reaches CREATE_COMPLETE. A stack recorded before these were added has no such entries,
// and its attributes resolve to "" as they did until the resource is next updated.
//
// Refused rather than answered (#1591), each listed in cfnGetAttUnmodelled or
// cfnGetAttUnmodelledPrefixes with its reason: a VPC's DefaultNetworkAcl, CidrBlockAssociations,
// Ipv6CidrBlocks and VpcEncryptionControl.*, a subnet's Ipv6CidrBlocks, NetworkAclAssociationId,
// OutpostArn and BlockPublicAccessStates.*, and a NAT gateway's EniId, RouteTableId,
// AutoProvisionZones and AutoScalingIps — network ACLs, IPv6 blocks, CIDR associations, encryption
// control, Outposts, Block Public Access and regional NAT gateways are not modeled.

// cfnEC2IDAttribute names, by type, the attribute whose value is the resource's EC2 ID.
var cfnEC2IDAttribute = map[string]string{
	"AWS::EC2::SecurityGroup":   "GroupId",
	"AWS::EC2::VPC":             "VpcId",
	"AWS::EC2::Subnet":          "SubnetId",
	"AWS::EC2::Instance":        "InstanceId",
	"AWS::EC2::InternetGateway": "InternetGatewayId",
	"AWS::EC2::RouteTable":      "RouteTableId",
	"AWS::EC2::NatGateway":      "NatGatewayId",
}

// recordEC2Attributes records on dr the published attributes of an EC2 resource that only EC2's
// record holds. A type it does not cover, a record it cannot read, and a value the record does not
// carry all record nothing, so the attribute keeps resolving as it did.
//
// The metadata map is copied before it is written, because an updater returns the record the
// previous deployment holds, which a rollback still compares against.
func (d *StackDeployer) recordEC2Attributes(ctx context.Context, dr *DeployedResource) {
	if d.state == nil || dr.PhysicalID == "" {
		return
	}
	acct, region := d.identity.accountID, d.identity.region
	var attrs map[string]string
	switch dr.Type {
	case "AWS::EC2::SecurityGroup":
		var sg EC2SecurityGroup
		if d.readEC2Record(ctx, "sg:"+acct+"/"+region+"/"+dr.PhysicalID, &sg) {
			// "The ID of the VPC", which for a group declared with no VpcId is the default VPC's.
			attrs = map[string]string{"VpcId": sg.VPCID}
		}
	case "AWS::EC2::VPC":
		var vpc EC2VPC
		if d.readEC2Record(ctx, "vpc:"+acct+"/"+region+"/"+dr.PhysicalID, &vpc) {
			attrs = map[string]string{
				"CidrBlock": vpc.CIDRBlock,
				"DefaultSecurityGroup": ec2LookupDefaultSecurityGroup(ctx, d.state,
					&RequestContext{AccountID: acct, Region: region}, dr.PhysicalID),
			}
		}
	case "AWS::EC2::Subnet":
		var subnet EC2Subnet
		if d.readEC2Record(ctx, "subnet:"+acct+"/"+region+"/"+dr.PhysicalID, &subnet) {
			attrs = map[string]string{
				"VpcId":              subnet.VPCID,
				"CidrBlock":          subnet.CIDRBlock,
				"AvailabilityZone":   subnet.AvailabilityZone,
				"AvailabilityZoneId": cfnAvailabilityZoneID(region, subnet.AvailabilityZone),
			}
		}
	case "AWS::EC2::Instance":
		var inst EC2Instance
		if d.readEC2Record(ctx, "instance:"+acct+"/"+region+"/"+dr.PhysicalID, &inst) {
			attrs = map[string]string{
				"VpcId":          inst.VPCID,
				"PrivateIp":      inst.PrivateIPAddress,
				"PrivateDnsName": inst.PrivateDNSName,
				"PublicIp":       inst.PublicIPAddress,
				"PublicDnsName":  inst.PublicDNSName,
			}
		}
	}
	if len(attrs) == 0 {
		return
	}
	dr.Metadata = cfnCopyMetadata(dr.Metadata)
	for k, v := range attrs {
		if v != "" {
			dr.Metadata[k] = v
		}
	}
}

// readEC2Record decodes one EC2 state record into v, reporting whether there was one to decode.
func (d *StackDeployer) readEC2Record(ctx context.Context, key string, v interface{}) bool {
	data, err := d.state.Get(ctx, ec2Namespace, key)
	if err != nil || data == nil {
		return false
	}
	return json.Unmarshal(data, v) == nil
}

// cfnAvailabilityZoneID maps a zone name to its ID through the zones EC2 itself reports, so the
// subnet attribute and DescribeAvailabilityZones cannot disagree.
func cfnAvailabilityZoneID(region, zone string) string {
	for _, z := range ec2SeededZones(region) {
		if z.ZoneName == zone {
			return z.ZoneID
		}
	}
	return ""
}
