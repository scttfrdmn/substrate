package emulator_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests below cover #1522: network interfaces as resources of their own, with the
// lifecycle Terraform's aws_network_interface and aws_network_interface_attachment drive.

// eniItem is the part of a NetworkInterface element these tests read.
type eniItem struct {
	ID              string `xml:"networkInterfaceId"`
	SubnetID        string `xml:"subnetId"`
	VpcID           string `xml:"vpcId"`
	AZ              string `xml:"availabilityZone"`
	Description     string `xml:"description"`
	Status          string `xml:"status"`
	PrivateIP       string `xml:"privateIpAddress"`
	SourceDestCheck bool   `xml:"sourceDestCheck"`
	Groups          []struct {
		GroupID string `xml:"groupId"`
	} `xml:"groupSet>item"`
	Tags []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
	Attachment *struct {
		AttachmentID        string `xml:"attachmentId"`
		InstanceID          string `xml:"instanceId"`
		DeviceIndex         int    `xml:"deviceIndex"`
		DeleteOnTermination bool   `xml:"deleteOnTermination"`
		Status              string `xml:"status"`
	} `xml:"attachment"`
	Addresses []struct {
		IP      string `xml:"privateIpAddress"`
		Primary bool   `xml:"primary"`
	} `xml:"privateIpAddressesSet>item"`
}

// eniFixture is a VPC with one /24 subnet and a security group.
type eniFixture struct {
	ts     *httptest.Server
	vpc    string
	subnet string
	sg     string
}

func newENIFixture(t *testing.T) eniFixture {
	t.Helper()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.20.0.0/16")
	return eniFixture{
		ts: ts, vpc: vpc,
		subnet: ec2CreateSubnetIn(t, ts, vpc, "10.20.1.0/24"),
		sg:     ec2CreateSG(t, ts, "eni-sg", "eni", vpc),
	}
}

// create sends CreateNetworkInterface in the fixture's subnet.
func (f eniFixture) create(t *testing.T, extra map[string]string) eniItem {
	t.Helper()
	params := map[string]string{"Action": "CreateNetworkInterface", "SubnetId": f.subnet}
	for k, v := range extra {
		params[k] = v
	}
	var doc struct {
		ENI eniItem `xml:"networkInterface"`
	}
	ec2FleetXML(t, f.ts, params, &doc)
	require.NotEmpty(t, doc.ENI.ID)
	return doc.ENI
}

// describe sends DescribeNetworkInterfaces with params.
func (f eniFixture) describe(t *testing.T, params map[string]string) []eniItem {
	t.Helper()
	params["Action"] = "DescribeNetworkInterfaces"
	var doc struct {
		Items []eniItem `xml:"networkInterfaceSet>item"`
	}
	ec2FleetXML(t, f.ts, params, &doc)
	return doc.Items
}

// one describes a single interface by ID.
func (f eniFixture) one(t *testing.T, id string) eniItem {
	t.Helper()
	items := f.describe(t, map[string]string{"NetworkInterfaceId.1": id})
	require.Len(t, items, 1)
	return items[0]
}

// attach attaches eni to instance at index and returns the attachment ID.
func (f eniFixture) attach(t *testing.T, eni, instance, index string) string {
	t.Helper()
	var doc struct {
		AttachmentID string `xml:"attachmentId"`
	}
	ec2FleetXML(t, f.ts, map[string]string{
		"Action": "AttachNetworkInterface", "NetworkInterfaceId": eni, "InstanceId": instance, "DeviceIndex": index,
	}, &doc)
	require.Regexp(t, `^eni-attach-[0-9a-f]{16}$`, doc.AttachmentID)
	return doc.AttachmentID
}

func TestEC2NetworkInterface_CreateDescribeAndFilter(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)

	plain := f.create(t, nil)
	assert.Equal(t, "available", plain.Status)
	assert.Equal(t, "10.20.1.4", plain.PrivateIP)
	assert.Equal(t, f.vpc, plain.VpcID)
	assert.Equal(t, "us-east-1a", plain.AZ)
	assert.True(t, plain.SourceDestCheck)
	assert.Nil(t, plain.Attachment, "an available interface has no attachment")
	require.Len(t, plain.Groups, 1, "no group named means the VPC's default group")
	assert.NotEqual(t, f.sg, plain.Groups[0].GroupID)

	tagged := f.create(t, map[string]string{
		"Description": "web", "SecurityGroupId.1": f.sg,
		"PrivateIpAddresses.1.PrivateIpAddress": "10.20.1.40", "PrivateIpAddresses.1.Primary": "true",
		"PrivateIpAddresses.2.PrivateIpAddress": "10.20.1.41",
		"SecondaryPrivateIpAddressCount":        "1",
		"TagSpecification.1.ResourceType":       "network-interface",
		"TagSpecification.1.Tag.1.Key":          "aegc:deployment", "TagSpecification.1.Tag.1.Value": "dev",
	})
	assert.Equal(t, "10.20.1.40", tagged.PrivateIP)
	require.Len(t, tagged.Addresses, 3)
	assert.True(t, tagged.Addresses[0].Primary)
	assert.Equal(t, "10.20.1.41", tagged.Addresses[1].IP)
	assert.Equal(t, "10.20.1.5", tagged.Addresses[2].IP, "the counted secondary is the next free address")
	require.Len(t, tagged.Tags, 1)

	selections := []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{"tag", map[string]string{"Filter.1.Name": "tag:aegc:deployment", "Filter.1.Value.1": "dev"}, []string{tagged.ID}},
		{"tag-key", map[string]string{"Filter.1.Name": "tag-key", "Filter.1.Value.1": "aegc:deployment"}, []string{tagged.ID}},
		{"description", map[string]string{"Filter.1.Name": "description", "Filter.1.Value.1": "web"}, []string{tagged.ID}},
		{"group-id", map[string]string{"Filter.1.Name": "group-id", "Filter.1.Value.1": f.sg}, []string{tagged.ID}},
		{"group-name", map[string]string{"Filter.1.Name": "group-name", "Filter.1.Value.1": "eni-sg"}, []string{tagged.ID}},
		{"secondary address", map[string]string{"Filter.1.Name": "addresses.private-ip-address", "Filter.1.Value.1": "10.20.1.41"}, []string{tagged.ID}},
		{"addresses.primary false", map[string]string{"Filter.1.Name": "addresses.primary", "Filter.1.Value.1": "false"}, []string{tagged.ID}},
		{"private-ip-address", map[string]string{"Filter.1.Name": "private-ip-address", "Filter.1.Value.1": "10.20.1.4"}, []string{plain.ID}},
		{"status", map[string]string{"Filter.1.Name": "status", "Filter.1.Value.1": "available", "Filter.2.Name": "subnet-id", "Filter.2.Value.1": f.subnet}, []string{plain.ID, tagged.ID}},
		{"vpc-id", map[string]string{"Filter.1.Name": "vpc-id", "Filter.1.Value.1": f.vpc}, []string{plain.ID, tagged.ID}},
		{"an attachment filter selects no available interface", map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": "i-0123456789abcdef0"}, nil},
		{"source-dest-check", map[string]string{"Filter.1.Name": "source-dest-check", "Filter.1.Value.1": "true", "Filter.2.Name": "vpc-id", "Filter.2.Value.1": f.vpc}, []string{plain.ID, tagged.ID}},
		{"requester-managed", map[string]string{"Filter.1.Name": "requester-managed", "Filter.1.Value.1": "true"}, nil},
		{"an inert filter", map[string]string{"Filter.1.Name": "mac-address", "Filter.1.Value.1": "x", "Filter.2.Name": "vpc-id", "Filter.2.Value.1": f.vpc}, []string{plain.ID, tagged.ID}},
	}
	for _, tc := range selections {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, item := range f.describe(t, tc.params) {
				got = append(got, item.ID)
			}
			assert.ElementsMatch(t, tc.want, got)
		})
	}

	refusals := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"no subnet", map[string]string{}, "MissingParameter"},
		{"an unknown subnet", map[string]string{"SubnetId": "subnet-0123456789abcdef0"}, "InvalidSubnetID.NotFound"},
		{"an unknown interface type", map[string]string{"SubnetId": f.subnet, "InterfaceType": "bogus"}, "InvalidParameterValue"},
		{"a negative secondary count", map[string]string{"SubnetId": f.subnet, "SecondaryPrivateIpAddressCount": "-1"}, "InvalidParameterValue"},
		{"an address in use", map[string]string{"SubnetId": f.subnet, "PrivateIpAddress": "10.20.1.4"}, "InvalidIPAddress.InUse"},
		{"a group in another VPC", map[string]string{"SubnetId": f.subnet, "SecurityGroupId.1": "sg-0123456789abcdef0"}, "InvalidGroup.NotFound"},
		{"a reserved tag key", map[string]string{"SubnetId": f.subnet, "TagSpecification.1.ResourceType": "network-interface", "TagSpecification.1.Tag.1.Key": "aws:x"}, "InvalidParameterValue"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "CreateNetworkInterface"
			ec2RequireRefusal(t, f.ts, tc.params, tc.code)
		})
	}

	t.Run("an efa-only interface has no address", func(t *testing.T) {
		efa := f.create(t, map[string]string{"InterfaceType": "efa-only"})
		assert.Empty(t, efa.PrivateIP)
		assert.Empty(t, efa.Addresses)
	})
}

func TestEC2NetworkInterface_AttachDetachDeleteLifecycle(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
	})
	instance := ids[0]
	eni := f.create(t, map[string]string{"Description": "extra"})

	attachmentID := f.attach(t, eni.ID, instance, "1")
	attached := f.one(t, eni.ID)
	assert.Equal(t, "in-use", attached.Status)
	require.NotNil(t, attached.Attachment)
	assert.Equal(t, attachmentID, attached.Attachment.AttachmentID)
	assert.Equal(t, instance, attached.Attachment.InstanceID)
	assert.Equal(t, 1, attached.Attachment.DeviceIndex)
	assert.False(t, attached.Attachment.DeleteOnTermination, "an interface attached after launch is kept by default")
	assert.Len(t, f.describe(t, map[string]string{"Filter.1.Name": "attachment.attachment-id", "Filter.1.Value.1": attachmentID}), 1)
	assert.Len(t, f.describe(t, map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": instance}), 2)
	assert.Len(t, niDescribe(t, f.ts, instance).Interfaces, 2, "the instance reports both")

	other := f.create(t, nil)
	refusals := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"a device index the instance uses", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": instance, "DeviceIndex": "1"}, "InvalidParameterValue"},
		{"an interface already attached", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": eni.ID, "InstanceId": instance, "DeviceIndex": "2"}, "InvalidNetworkInterface.InUse"},
		{"an unknown instance", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": "i-0123456789abcdef0", "DeviceIndex": "2"}, "InvalidInstanceID.NotFound"},
		{"an unknown interface", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": "eni-0123456789abcdef0", "InstanceId": instance, "DeviceIndex": "2"}, "InvalidNetworkInterfaceID.NotFound"},
		{"a malformed interface", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": "nope", "InstanceId": instance, "DeviceIndex": "2"}, "InvalidNetworkInterfaceID.Malformed"},
		{"no device index", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": instance}, "MissingParameter"},
		{"a bad device index", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": instance, "DeviceIndex": "x"}, "InvalidParameterValue"},
		{"a bad network card", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": instance, "DeviceIndex": "2", "NetworkCardIndex": "x"}, "InvalidParameterValue"},
		{"no instance", map[string]string{"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "DeviceIndex": "2"}, "MissingParameter"},
		{"deleting an attached interface", map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": eni.ID}, "InvalidNetworkInterface.InUse"},
		{"deleting with no ID", map[string]string{"Action": "DeleteNetworkInterface"}, "MissingParameter"},
		{"detaching an unknown attachment", map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-0123456789abcdef0"}, "InvalidAttachmentID.NotFound"},
		{"detaching a malformed attachment", map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": eni.ID}, "InvalidNetworkInterfaceAttachmentId.Malformed"},
		{"detaching with no attachment", map[string]string{"Action": "DetachNetworkInterface"}, "MissingParameter"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusal(t, f.ts, tc.params, tc.code)
		})
	}
	ec2RequireRefusalMessage(t, f.ts, map[string]string{
		"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": instance, "DeviceIndex": "0",
	}, "InvalidParameterValue", "Instance '"+instance+"' already has an interface attached at device index '0'.")

	t.Run("the primary interface cannot be detached", func(t *testing.T) {
		primary := f.describe(t, map[string]string{
			"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": instance,
			"Filter.2.Name": "attachment.device-index", "Filter.2.Value.1": "0",
		})
		require.Len(t, primary, 1)
		ec2RequireRefusalMessage(t, f.ts, map[string]string{
			"Action": "DetachNetworkInterface", "AttachmentId": primary[0].Attachment.AttachmentID,
		}, "OperationNotPermitted", "The network interface at device index 0 and networkCard index 0 cannot be detached.")
	})

	ec2OK(t, f.ts, map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": attachmentID, "Force": "true"})
	detached := f.one(t, eni.ID)
	assert.Equal(t, "available", detached.Status)
	require.NotNil(t, detached.Attachment, "the ended attachment is still reported")
	assert.Equal(t, "detached", detached.Attachment.Status,
		"the status the Terraform AWS provider waits for after a detach")
	assert.Len(t, f.describe(t, map[string]string{
		"Filter.1.Name": "attachment.status", "Filter.1.Value.1": "detached",
	}), 1)
	assert.Len(t, niDescribe(t, f.ts, instance).Interfaces, 1, "the instance no longer reports it")
	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "DetachNetworkInterface", "AttachmentId": attachmentID,
	}, "InvalidAttachmentID.NotFound")
	assert.Equal(t, eni.PrivateIP, detached.PrivateIP, "a detached interface keeps its address")

	available := ipaAvailable(t, f.ts, f.subnet)
	ec2OK(t, f.ts, map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": eni.ID})
	assert.Equal(t, available+1, ipaAvailable(t, f.ts, f.subnet), "a deleted interface releases its address")
	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "DescribeNetworkInterfaces", "NetworkInterfaceId.1": eni.ID,
	}, "InvalidNetworkInterfaceID.NotFound")
	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "DeleteNetworkInterface", "NetworkInterfaceId": eni.ID,
	}, "InvalidNetworkInterfaceID.NotFound")

	t.Run("a terminated instance cannot take an interface", func(t *testing.T) {
		ec2OK(t, f.ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": instance})
		ec2RequireRefusal(t, f.ts, map[string]string{
			"Action": "AttachNetworkInterface", "NetworkInterfaceId": other.ID, "InstanceId": instance, "DeviceIndex": "3",
		}, "IncorrectInstanceState")
	})
}

func TestEC2NetworkInterface_TerminateSettlesEachInterface(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	kept := f.create(t, nil)
	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.SubnetId": f.subnet,
		"NetworkInterface.2.DeleteOnTermination": "false",
		"TagSpecification.1.ResourceType":        "network-interface",
		"TagSpecification.1.Tag.1.Key":           "aegc:deployment", "TagSpecification.1.Tag.1.Value": "dev",
	})
	instance := ids[0]
	f.attach(t, kept.ID, instance, "2")

	launched := f.describe(t, map[string]string{"Filter.1.Name": "tag:aegc:deployment", "Filter.1.Value.1": "dev"})
	require.Len(t, launched, 2, "the launch's network-interface tags land on both interfaces it created")
	for _, item := range launched {
		assert.Equal(t, "in-use", item.Status)
	}

	ec2OK(t, f.ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": instance})
	remaining := f.describe(t, map[string]string{"Filter.1.Name": "subnet-id", "Filter.1.Value.1": f.subnet})
	require.Len(t, remaining, 2, "the primary is deleted; the two not deleted on termination remain")
	for _, item := range remaining {
		assert.Equal(t, "available", item.Status)
		if item.Attachment != nil {
			assert.Equal(t, "detached", item.Attachment.Status)
		}
	}
	assert.Empty(t, niDescribe(t, f.ts, instance).Interfaces, "a terminated instance reports no interface")

	t.Run("an interface still in the subnet holds it", func(t *testing.T) {
		ec2RequireRefusal(t, f.ts, map[string]string{"Action": "DeleteSubnet", "SubnetId": f.subnet}, "DependencyViolation")
	})
	t.Run("an interface still using a group holds it", func(t *testing.T) {
		g := ec2CreateSG(t, f.ts, "held", "held", f.vpc)
		eni := f.create(t, map[string]string{"SecurityGroupId.1": g})
		ec2RequireRefusal(t, f.ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": g}, "DependencyViolation")
		ec2OK(t, f.ts, map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": eni.ID})
		ec2OK(t, f.ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": g})
	})
	t.Run("an interface still in the VPC holds it", func(t *testing.T) {
		ec2RequireRefusal(t, f.ts, map[string]string{"Action": "DeleteVpc", "VpcId": f.vpc}, "DependencyViolation")
	})
	for _, item := range remaining {
		ec2OK(t, f.ts, map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": item.ID})
	}
	ec2OK(t, f.ts, map[string]string{"Action": "DeleteSubnet", "SubnetId": f.subnet})
}

func TestEC2NetworkInterface_ModifyAttribute(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
	})
	primary := f.describe(t, map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0]})
	require.Len(t, primary, 1)
	eniID := primary[0].ID

	ec2OK(t, f.ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID, "Description.Value": "renamed",
	})
	ec2OK(t, f.ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID, "SourceDestCheck.Value": "false",
	})
	ec2OK(t, f.ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID, "SecurityGroupId.1": f.sg,
	})
	ec2OK(t, f.ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID,
		"Attachment.AttachmentId": primary[0].Attachment.AttachmentID, "Attachment.DeleteOnTermination": "false",
	})
	got := f.one(t, eniID)
	assert.Equal(t, "renamed", got.Description)
	assert.False(t, got.SourceDestCheck)
	require.Len(t, got.Groups, 1)
	assert.Equal(t, f.sg, got.Groups[0].GroupID)
	assert.False(t, got.Attachment.DeleteOnTermination)
	described := niDescribe(t, f.ts, ids[0])
	require.Len(t, described.Groups, 1)
	assert.Equal(t, f.sg, described.Groups[0].GroupID, "the primary interface's groups are the instance's")

	refusals := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"an unknown attachment", map[string]string{"NetworkInterfaceId": eniID, "Attachment.AttachmentId": "eni-attach-0123456789abcdef0"}, "InvalidAttachmentID.NotFound"},
		{"a malformed attachment", map[string]string{"NetworkInterfaceId": eniID, "Attachment.AttachmentId": "bogus"}, "InvalidNetworkInterfaceAttachmentId.Malformed"},
		{"an unknown group", map[string]string{"NetworkInterfaceId": eniID, "SecurityGroupId.1": "sg-0123456789abcdef0"}, "InvalidGroup.NotFound"},
		{"an unknown interface", map[string]string{"NetworkInterfaceId": "eni-0123456789abcdef0"}, "InvalidNetworkInterfaceID.NotFound"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "ModifyNetworkInterfaceAttribute"
			ec2RequireRefusal(t, f.ts, tc.params, tc.code)
		})
	}

	t.Run("a primary that is not deleted on termination survives it", func(t *testing.T) {
		ec2OK(t, f.ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0]})
		assert.Equal(t, "available", f.one(t, eniID).Status)
	})
}

func TestEC2NetworkInterface_Tagging(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	eni := f.create(t, nil)
	ec2OK(t, f.ts, map[string]string{
		"Action": "CreateTags", "ResourceId.1": eni.ID, "Tag.1.Key": "Name", "Tag.1.Value": "nic",
	})
	assert.Len(t, f.describe(t, map[string]string{"Filter.1.Name": "tag:Name", "Filter.1.Value.1": "nic"}), 1)

	var tags struct {
		Items []struct {
			ID   string `xml:"resourceId"`
			Type string `xml:"resourceType"`
		} `xml:"tagSet>item"`
	}
	ec2FleetXML(t, f.ts, map[string]string{
		"Action": "DescribeTags", "Filter.1.Name": "resource-type", "Filter.1.Value.1": "network-interface",
	}, &tags)
	require.Len(t, tags.Items, 1)
	assert.Equal(t, eni.ID, tags.Items[0].ID)

	ec2OK(t, f.ts, map[string]string{"Action": "DeleteTags", "ResourceId.1": eni.ID, "Tag.1.Key": "Name"})
	assert.Empty(t, f.describe(t, map[string]string{"Filter.1.Name": "tag:Name", "Filter.1.Value.1": "nic"}))
}

func TestEC2NetworkInterface_LaunchTemplateInterfaceTags(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	ec2OK(t, f.ts, map[string]string{
		"Action": "CreateLaunchTemplate", "LaunchTemplateName": "nic-tags",
		"LaunchTemplateData.ImageId":                         ec2TestImage,
		"LaunchTemplateData.TagSpecification.1.ResourceType": "network-interface",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":    "team",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":  "infra",
	})
	var versions struct {
		Types []string `xml:"launchTemplateVersionSet>item>launchTemplateData>tagSpecificationSet>item>resourceType"`
	}
	ec2FleetXML(t, f.ts, map[string]string{
		"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateName": "nic-tags",
	}, &versions)
	assert.Equal(t, []string{"network-interface"}, versions.Types, "the scope reads back")

	ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
		"LaunchTemplate.LaunchTemplateName": "nic-tags",
	})
	tagged := f.describe(t, map[string]string{"Filter.1.Name": "tag:team", "Filter.1.Value.1": "infra"})
	require.Len(t, tagged, 1)
	assert.Equal(t, "in-use", tagged[0].Status)

	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "CreateLaunchTemplate", "LaunchTemplateName": "bad-nic-tags",
		"LaunchTemplateData.ImageId":                         ec2TestImage,
		"LaunchTemplateData.TagSpecification.1.ResourceType": "network-interface",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":    "aws:reserved",
	}, "InvalidParameterValue")
}

func TestEC2NetworkInterface_LaunchAttachesAnExistingInterface(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	eni := f.create(t, nil)

	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "2", "MaxCount": "2",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.NetworkInterfaceId": eni.ID,
	}, "InvalidNetworkInterface.InUse")
	assert.Equal(t, "available", f.one(t, eni.ID).Status, "the refused launch attached nothing")

	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.NetworkInterfaceId": "eni-0123456789abcdef0",
	}, "InvalidNetworkInterfaceID.NotFound")
	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.NetworkInterfaceId": "eni-0123456789abcdef0",
	}, "InvalidNetworkInterfaceID.NotFound")
	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.SubnetId": "subnet-0123456789abcdef0",
	}, "InvalidSubnetID.NotFound")

	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.NetworkInterfaceId": eni.ID,
	})
	got := f.one(t, eni.ID)
	assert.Equal(t, "in-use", got.Status)
	assert.Equal(t, ids[0], got.Attachment.InstanceID)
	assert.False(t, got.Attachment.DeleteOnTermination)

	ec2OK(t, f.ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0]})
	assert.Equal(t, "available", f.one(t, eni.ID).Status, "an interface the launch did not create outlives it")
}
