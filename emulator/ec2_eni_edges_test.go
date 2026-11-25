package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2NetworkInterface_AttachmentFilters covers the attachment.* filters a describe evaluates
// beyond the instance and attachment IDs.
func TestEC2NetworkInterface_AttachmentFilters(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	vpc := ec2CreateVPC(t, ts, "10.80.0.0/16")
	f := eniFixture{ts: ts, vpc: vpc, subnet: ec2CreateSubnetIn(t, ts, vpc, "10.80.1.0/24")}
	ids := ec2RunInstanceIDs(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
	})
	extra := f.create(t, nil)
	f.attach(t, extra.ID, ids[0], "1")

	var times struct {
		AttachTime []string `xml:"networkInterfaceSet>item>attachment>attachTime"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeNetworkInterfaces", "NetworkInterfaceId.1": extra.ID}, &times)
	require.Len(t, times.AttachTime, 1)

	for name, tc := range map[string]struct {
		filter, value string
		want          int
	}{
		"deleted on termination":     {"attachment.delete-on-termination", "true", 1},
		"kept on termination":        {"attachment.delete-on-termination", "false", 1},
		"attached at a time":         {"attachment.attach-time", times.AttachTime[0], 2},
		"owned by the account":       {"attachment.instance-owner-id", "123456789012", 2},
		"attached now":               {"attachment.status", "attached", 2},
		"at device index 1":          {"attachment.device-index", "1", 1},
		"an interface type":          {"interface-type", "interface", 2},
		"a primary address is there": {"addresses.primary", "true", 2},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Len(t, f.describe(t, map[string]string{
				"Filter.1.Name": tc.filter, "Filter.1.Value.1": tc.value,
				"Filter.2.Name": "vpc-id", "Filter.2.Value.1": vpc,
			}), tc.want)
		})
	}
}

// TestEC2AddressAllocation_ASubnetTooSmallHasNoAddress covers a block with no usable address: a /30
// is four addresses, all of them reserved.
func TestEC2AddressAllocation_ASubnetTooSmallHasNoAddress(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.81.0.0/16"), "10.81.0.0/30")
	assert.Equal(t, 0, ipaAvailable(t, ts, subnet))
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnet,
	}, "InsufficientFreeAddressesInSubnet")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNetworkInterface", "SubnetId": subnet, "PrivateIpAddress": "10.81.0.2",
	}, "InvalidParameterValue")
}

// TestEC2NetworkInterface_CreateAddressRefusals covers CreateNetworkInterface's secondary
// addresses: an unmarked list takes its first entry as primary, and a secondary address or count
// the subnet cannot give is refused with nothing created.
func TestEC2NetworkInterface_CreateAddressRefusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.82.0.0/16")
	f := eniFixture{ts: ts, vpc: vpc, subnet: ec2CreateSubnetIn(t, ts, vpc, "10.82.0.0/28")}

	unmarked := f.create(t, map[string]string{
		"PrivateIpAddresses.1.PrivateIpAddress": "10.82.0.6",
		"PrivateIpAddresses.2.PrivateIpAddress": "10.82.0.7",
	})
	assert.Equal(t, "10.82.0.6", unmarked.PrivateIP, "the first listed address is the primary")
	require.Len(t, unmarked.Addresses, 2)

	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNetworkInterface", "SubnetId": f.subnet,
		"PrivateIpAddresses.1.PrivateIpAddress": "10.82.0.8",
		"PrivateIpAddresses.2.PrivateIpAddress": "10.82.0.7",
	}, "InvalidIPAddress.InUse")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNetworkInterface", "SubnetId": f.subnet, "SecondaryPrivateIpAddressCount": "20",
	}, "InsufficientFreeAddressesInSubnet")
	assert.Equal(t, 9, ipaAvailable(t, ts, f.subnet), "neither refused create held an address")
}
