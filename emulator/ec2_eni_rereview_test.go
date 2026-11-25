package emulator_test

import (
	"encoding/xml"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2NetworkInterface_MultiCardIDsKeepTheirDrawOrder pins the IDs of a launch's unnamed
// declared interfaces on two network cards to the request mint's ordinals in the order the
// declarations sort by device index alone — c0d0, c1d0, c0d1 — which is the order event logs
// recorded them in, though the interfaces are reported network card first.
func TestEC2NetworkInterface_MultiCardIDsKeepTheirDrawOrder(t *testing.T) {
	t.Parallel()
	p, setup, _ := setupEC2WirePlugin(t)
	subnet := ec2WireSubnet(t, p, setup, ec2WireVPC(t, p, setup))

	const seed = "req-two-cards"
	ctx := &emulator.RequestContext{AccountID: ec2WireAccount, Region: ec2WireRegion, RequestID: seed, IDs: emulator.NewIDMint(seed)}
	ec2Wire(t, p, ctx, "RunInstances", map[string]string{
		"ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": subnet,
		"NetworkInterface.2.DeviceIndex": "0", "NetworkInterface.2.NetworkCardIndex": "1", "NetworkInterface.2.SubnetId": subnet,
		"NetworkInterface.3.DeviceIndex": "1", "NetworkInterface.3.SubnetId": subnet,
	})

	mint := emulator.NewIDMint(seed)
	mint.Hex(8) // the reservation
	mint.Hex(8) // the instance
	want := map[[2]int]string{
		{0, 0}: "eni-" + mint.Hex(8),
		{1, 0}: "eni-" + mint.Hex(8),
		{0, 1}: "eni-" + mint.Hex(8),
	}

	read := &emulator.RequestContext{AccountID: ec2WireAccount, Region: ec2WireRegion, RequestID: "req-read", IDs: emulator.NewIDMint("req-read")}
	var doc struct {
		Items []struct {
			ID     string `xml:"networkInterfaceId"`
			Card   int    `xml:"attachment>networkCardIndex"`
			Device int    `xml:"attachment>deviceIndex"`
		} `xml:"networkInterfaceSet>item"`
	}
	require.NoError(t, xml.Unmarshal(ec2Wire(t, p, read, "DescribeNetworkInterfaces", nil), &doc))
	require.Len(t, doc.Items, 3)
	for _, item := range doc.Items {
		assert.Equal(t, want[[2]int{item.Card, item.Device}], item.ID, "card %d device %d", item.Card, item.Device)
	}
}

// TestEC2NetworkInterface_SecondaryGroupsAreCheckedAndDefaulted covers a launch's secondary
// interface groups: named ones must exist in the interface's VPC, and none means its default.
func TestEC2NetworkInterface_SecondaryGroupsAreCheckedAndDefaulted(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	ec2RequireRefusal(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.SubnetId": f.subnet,
		"NetworkInterface.2.SecurityGroupId.1": "sg-0123456789abcdef0",
	}, "InvalidGroup.NotFound")

	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.1.SecurityGroupId.1": f.sg,
		"NetworkInterface.2.DeviceIndex":       "1", "NetworkInterface.2.SubnetId": f.subnet,
	})
	secondary := f.describe(t, map[string]string{
		"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0],
		"Filter.2.Name": "attachment.device-index", "Filter.2.Value.1": "1",
	})
	require.Len(t, secondary, 1)
	require.Len(t, secondary[0].Groups, 1)
	assert.NotEqual(t, f.sg, secondary[0].Groups[0].GroupID, "a secondary naming no group takes the VPC's default")
}

// TestEC2NetworkInterface_OlderInterfacesStayDeletedOnceStored covers an older instance with a
// kept secondary interface: its derived attachment ID detaches it with no prior write, and once
// deleted it is not synthesized again.
func TestEC2NetworkInterface_OlderInterfacesStayDeletedOnceStored(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.90.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.90.1.0/24")
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-0000000000000000f", SubnetID: subnet, VPCID: vpc, AvailabilityZone: "us-east-1a",
		PrivateIPAddress: "10.90.1.4",
		NetworkInterfaces: []emulator.EC2NetworkInterface{
			{NetworkInterfaceID: "eni-000000000000000f1", DeviceIndex: 0, SubnetID: subnet, PrivateIPAddress: "10.90.1.4", DeleteOnTermination: true},
			{NetworkInterfaceID: "eni-000000000000000f2", DeviceIndex: 1, SubnetID: subnet, PrivateIPAddress: "10.90.1.9"},
		},
	})
	f := eniFixture{ts: ts, vpc: vpc, subnet: subnet}

	secondary := f.one(t, "eni-000000000000000f2")
	require.NotNil(t, secondary.Attachment)
	assert.Equal(t, "eni-attach-000000000000000f2", secondary.Attachment.AttachmentID,
		"the synthesized attachment carries a usable ID before any write")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-000000000000000f1",
	}, "OperationNotPermitted")
	ec2OK(t, ts, map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": secondary.Attachment.AttachmentID})
	assert.Equal(t, "available", f.one(t, "eni-000000000000000f2").Status)

	ec2OK(t, ts, map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": "eni-000000000000000f2"})
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "DescribeNetworkInterfaces", "NetworkInterfaceId.1": "eni-000000000000000f2",
	}, "InvalidNetworkInterfaceID.NotFound")
	assert.Len(t, niDescribe(t, ts, "i-0000000000000000f").Interfaces, 1, "the instance no longer reports it")
	assert.Equal(t, 251-1, ipaAvailable(t, ts, subnet), "its address is free")
}

// TestEC2NetworkInterface_TagsLandOnAnOlderInterface covers CreateTags naming an interface an older
// instance reports: the instance's interfaces are stored and the tag is on the record.
func TestEC2NetworkInterface_TagsLandOnAnOlderInterface(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.91.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.91.1.0/24")
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-00000000000000010", SubnetID: subnet, VPCID: vpc, PrivateIPAddress: "10.91.1.4",
	})
	ec2OK(t, ts, map[string]string{
		"Action": "CreateTags", "ResourceId.1": "eni-00000000000000010", "Tag.1.Key": "Name", "Tag.1.Value": "older",
	})
	f := eniFixture{ts: ts, vpc: vpc, subnet: subnet}
	tagged := f.describe(t, map[string]string{"Filter.1.Name": "tag:Name", "Filter.1.Value.1": "older"})
	require.Len(t, tagged, 1)
	assert.Equal(t, "eni-00000000000000010", tagged[0].ID)
}

// TestEC2NetworkInterface_SharedOlderAddressSurvivesAStoredReporter covers two older instances
// recorded at one address: storing B's interfaces and terminating A leaves the address held by B's
// stored interface, so no new allocation is given it.
func TestEC2NetworkInterface_SharedOlderAddressSurvivesAStoredReporter(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "172.31.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "172.31.1.0/24")
	for _, id := range []string{"i-00000000000000011", "i-00000000000000012"} {
		ipaPutLegacyInstance(t, state, emulator.EC2Instance{
			InstanceID: id, SubnetID: subnet, VPCID: vpc, PrivateIPAddress: "172.31.1.10",
		})
	}
	ec2OK(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": "eni-00000000000000012", "Description.Value": "b",
	})
	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000011"})
	assert.Equal(t, 250, ipaAvailable(t, ts, subnet), "B's stored interface still holds the address")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNetworkInterface", "SubnetId": subnet, "PrivateIpAddress": "172.31.1.10",
	}, "InvalidIPAddress.InUse")
	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000012"})
	assert.Equal(t, 251, ipaAvailable(t, ts, subnet))
}
