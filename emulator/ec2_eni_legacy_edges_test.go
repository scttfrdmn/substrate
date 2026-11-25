package emulator_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestEC2NetworkInterface_OlderSharedInterfaceIDHasOneOwner covers an older two-instance launch
// that named one interface by NetworkInterfaceId, which the older build embedded on both
// instances: the interface is reported once, on the lower instance ID, and storing the other
// instance's interfaces leaves it as it is.
func TestEC2NetworkInterface_OlderSharedInterfaceIDHasOneOwner(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.96.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.96.1.0/24")
	const shared = "eni-0000000000000aaa1"
	for _, id := range []string{"i-00000000000000031", "i-00000000000000030"} {
		ipaPutLegacyInstance(t, state, emulator.EC2Instance{
			InstanceID: id, SubnetID: subnet, VPCID: vpc, PrivateIPAddress: "10.96.1.20",
			NetworkInterfaces: []emulator.EC2NetworkInterface{
				{NetworkInterfaceID: shared, DeviceIndex: 0, SubnetID: subnet, PrivateIPAddress: "10.96.1.20"},
			},
		})
	}
	f := eniFixture{ts: ts, vpc: vpc, subnet: subnet}
	all := f.describe(t, map[string]string{"NetworkInterfaceId.1": shared})
	require.Len(t, all, 1, "the shared interface is reported once")
	assert.Equal(t, "i-00000000000000030", all[0].Attachment.InstanceID, "on the lower instance ID")
	assert.Empty(t, niDescribe(t, ts, "i-00000000000000031").Interfaces)

	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000031"})
	got := f.one(t, shared)
	require.NotNil(t, got.Attachment)
	assert.Equal(t, "i-00000000000000030", got.Attachment.InstanceID, "the other instance's termination leaves it")

	ec2OK(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": shared, "Description.Value": "owner",
	})
	stored := f.one(t, shared)
	assert.Equal(t, "owner", stored.Description)
	assert.Equal(t, "i-00000000000000030", stored.Attachment.InstanceID)
}

// TestEC2NetworkInterface_OlderInterfaceIsInItsSubnetsVPC covers an older instance whose declared
// secondary interface is in a subnet of another VPC: the interface reports that subnet's VPC.
func TestEC2NetworkInterface_OlderInterfaceIsInItsSubnetsVPC(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpcA := ec2CreateVPC(t, ts, "10.97.0.0/16")
	vpcB := ec2CreateVPC(t, ts, "10.98.0.0/16")
	subnetA := ec2CreateSubnetIn(t, ts, vpcA, "10.97.1.0/24")
	subnetB := ec2CreateSubnetIn(t, ts, vpcB, "10.98.1.0/24")
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-00000000000000040", SubnetID: subnetA, VPCID: vpcA, PrivateIPAddress: "10.97.1.4",
		NetworkInterfaces: []emulator.EC2NetworkInterface{
			{NetworkInterfaceID: "eni-00000000000000041", DeviceIndex: 0, SubnetID: subnetA, PrivateIPAddress: "10.97.1.4"},
			{NetworkInterfaceID: "eni-00000000000000042", DeviceIndex: 1, SubnetID: subnetB, PrivateIPAddress: "10.98.1.4"},
		},
	})
	f := eniFixture{ts: ts, vpc: vpcB, subnet: subnetB}
	assert.Equal(t, vpcB, f.one(t, "eni-00000000000000042").VpcID)
	assert.Equal(t, vpcA, f.one(t, "eni-00000000000000041").VpcID)
}

// TestEC2NetworkInterface_OlderPrimaryIsTheLowestDeviceIndex covers an older instance whose launch
// declared nothing at device index 0: the older build took the lowest device index as primary,
// so that interface's groups are the instance's and it cannot be detached.
func TestEC2NetworkInterface_OlderPrimaryIsTheLowestDeviceIndex(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.99.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.99.1.0/24")
	group := ec2CreateSG(t, ts, "lowest", "lowest", vpc)
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-00000000000000050", SubnetID: subnet, VPCID: vpc, PrivateIPAddress: "10.99.1.4",
		NetworkInterfaces: []emulator.EC2NetworkInterface{
			{NetworkInterfaceID: "eni-00000000000000051", DeviceIndex: 1, SubnetID: subnet, PrivateIPAddress: "10.99.1.4", SecurityGroupIDs: []string{group}},
			{NetworkInterfaceID: "eni-00000000000000052", DeviceIndex: 2, SubnetID: subnet, PrivateIPAddress: "10.99.1.5"},
		},
	})
	described := niDescribe(t, ts, "i-00000000000000050")
	require.Len(t, described.Groups, 1)
	assert.Equal(t, group, described.Groups[0].GroupID)
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-00000000000000051",
	}, "OperationNotPermitted")
	ec2OK(t, ts, map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-00000000000000052"})
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "DetachNetworkInterface", "AttachmentId": "eni-attach-00000000000000051",
	}, "OperationNotPermitted")
}

// TestEC2AddressAllocation_ANatGatewayAtAnOlderAddressKeepsIt covers an address a NAT gateway and
// an older instance were both recorded at: the instance's termination leaves it held by the
// gateway.
func TestEC2AddressAllocation_ANatGatewayAtAnOlderAddressKeepsIt(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.100.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.100.1.0/24")
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-00000000000000060", SubnetID: subnet, VPCID: vpc, PrivateIPAddress: "10.100.1.9",
	})
	gw := emulator.EC2NATGateway{
		NatGatewayID: "nat-00000000000000060", SubnetID: subnet, VPCID: vpc, PrivateIP: "10.100.1.9",
		State: "available", ConnectivityType: "private", AccountID: "123456789012", Region: "us-east-1",
	}
	raw, err := json.Marshal(gw)
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "ec2", "nat:123456789012/us-east-1/"+gw.NatGatewayID, raw))

	assert.Equal(t, 250, ipaAvailable(t, ts, subnet))
	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000060"})
	assert.Equal(t, 250, ipaAvailable(t, ts, subnet), "the gateway still holds the address")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNetworkInterface", "SubnetId": subnet, "PrivateIpAddress": "10.100.1.9",
	}, "InvalidIPAddress.InUse")
}
