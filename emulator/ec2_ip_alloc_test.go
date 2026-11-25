package emulator_test

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests below cover #1592: every private address comes from its subnet's range, skips the
// five reserved addresses, is unique within the subnet, and is reflected in the subnet's
// availableIpAddressCount.

// ipaRun launches count instances into subnet, with extra params, and returns their IDs and
// private addresses.
func ipaRun(t *testing.T, ts *httptest.Server, subnet string, count int, extra map[string]string) (ids, addrs []string) {
	t.Helper()
	params := map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage,
		"MinCount": strconv.Itoa(count), "MaxCount": strconv.Itoa(count),
	}
	if subnet != "" {
		params["SubnetId"] = subnet
	}
	for k, v := range extra {
		params[k] = v
	}
	var doc struct {
		Instances []struct {
			ID  string `xml:"instanceId"`
			IP  string `xml:"privateIpAddress"`
			DNS string `xml:"privateDnsName"`
		} `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, params, &doc)
	for _, inst := range doc.Instances {
		ids = append(ids, inst.ID)
		addrs = append(addrs, inst.IP)
	}
	return ids, addrs
}

// ipaAvailable reads a subnet's availableIpAddressCount through DescribeSubnets.
func ipaAvailable(t *testing.T, ts *httptest.Server, subnet string) int {
	t.Helper()
	var doc struct {
		Count []int `xml:"subnetSet>item>availableIpAddressCount"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSubnets", "SubnetId.1": subnet}, &doc)
	require.Len(t, doc.Count, 1)
	return doc.Count[0]
}

func TestEC2AddressAllocation_SeparateLaunchesGetDistinctAddressesInTheSubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.0.0.0/16"), "10.0.1.0/24")

	_, first := ipaRun(t, ts, subnet, 1, nil)
	_, second := ipaRun(t, ts, subnet, 2, nil)
	assert.Equal(t, []string{"10.0.1.4"}, first, "the first four addresses are reserved")
	assert.Equal(t, []string{"10.0.1.5", "10.0.1.6"}, second, "a second launch does not collide with the first")

	ids, _ := ipaRun(t, ts, subnet, 1, nil)
	described := niDescribe(t, ts, ids[0])
	assert.Equal(t, "10.0.1.7", described.PrivateIPAddress)
	require.Len(t, described.Interfaces, 1)
	assert.Equal(t, "ip-10-0-1-7.us-east-1.compute.internal", described.Interfaces[0].PrivateDNSName)
	assert.Equal(t, 251-4, ipaAvailable(t, ts, subnet))
}

func TestEC2AddressAllocation_DefaultVPCLaunchStaysInItsDefaultSubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, first := ipaRun(t, ts, "", 1, nil)
	_, second := ipaRun(t, ts, "", 1, nil)
	assert.Equal(t, "172.31.0.4", first[0], "the default subnet in the first zone is 172.31.0.0/20")
	assert.Equal(t, "172.31.0.5", second[0])
}

func TestEC2AddressAllocation_ExplicitAddresses(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.2.0.0/16"), "10.2.1.0/24")

	_, addrs := ipaRun(t, ts, subnet, 1, map[string]string{"PrivateIpAddress": "10.2.1.50"})
	assert.Equal(t, []string{"10.2.1.50"}, addrs)
	_, addrs = ipaRun(t, ts, subnet, 1, map[string]string{
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": subnet,
		"NetworkInterface.1.PrivateIpAddress": "10.2.1.60",
	})
	assert.Equal(t, []string{"10.2.1.60"}, addrs, "the primary interface's address is the instance's")

	refusals := []struct {
		name, addr, code, message string
	}{
		{"an address another instance holds", "10.2.1.50", "InvalidIPAddress.InUse", "Address 10.2.1.50 is in use."},
		{"an address outside the subnet", "10.2.2.9", "InvalidParameterValue", "Address does not fall within the subnet's address range"},
		{"the network address", "10.2.1.0", "InvalidParameterValue", "Address 10.2.1.0 is in subnet's reserved address range"},
		{"the router's address", "10.2.1.1", "InvalidParameterValue", "Address 10.2.1.1 is in subnet's reserved address range"},
		{"the broadcast address", "10.2.1.255", "InvalidParameterValue", "Address 10.2.1.255 is in subnet's reserved address range"},
		{"not an IPv4 address", "fe80::1", "InvalidParameterValue", "Value (fe80::1) for parameter PrivateIpAddress is invalid. It is not an IPv4 address."},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusalMessage(t, ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
				"SubnetId": subnet, "PrivateIpAddress": tc.addr,
			}, tc.code, tc.message)
		})
	}

	t.Run("a two-instance launch naming one address writes nothing", func(t *testing.T) {
		before := len(ec2DescribeInstanceIDs(t, ts))
		ec2RequireRefusal(t, ts, map[string]string{
			"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "2", "MaxCount": "2",
			"SubnetId": subnet, "PrivateIpAddress": "10.2.1.70",
		}, "InvalidIPAddress.InUse")
		assert.Len(t, ec2DescribeInstanceIDs(t, ts), before)
		_, addrs := ipaRun(t, ts, subnet, 1, map[string]string{"PrivateIpAddress": "10.2.1.70"})
		assert.Equal(t, []string{"10.2.1.70"}, addrs, "the refused launch held no address")
	})
}

func TestEC2AddressAllocation_ExhaustionReleaseAndReuse(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.3.0.0/16"), "10.3.0.0/28")
	assert.Equal(t, 11, ipaAvailable(t, ts, subnet), "a /28 has sixteen addresses, five reserved")

	ids, addrs := ipaRun(t, ts, subnet, 11, nil)
	assert.Equal(t, "10.3.0.4", addrs[0])
	assert.Equal(t, "10.3.0.14", addrs[10], "the last usable address is the one before the broadcast address")
	assert.Equal(t, 0, ipaAvailable(t, ts, subnet))
	ec2RequireRefusalMessage(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnet,
	}, "InsufficientFreeAddressesInSubnet",
		"The specified subnet does not contain enough free private IP addresses to fulfill your request.")

	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[3]})
	assert.Equal(t, 1, ipaAvailable(t, ts, subnet), "a terminated instance releases its address")
	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[3]})
	assert.Equal(t, 1, ipaAvailable(t, ts, subnet), "terminating it again releases nothing more")

	_, again := ipaRun(t, ts, subnet, 1, nil)
	assert.Equal(t, []string{"10.3.0.7"}, again, "the cursor wraps round to the freed address")
}

func TestEC2AddressAllocation_NatGatewaysTakeAndReleaseSubnetAddresses(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.4.0.0/16"), "10.4.1.0/24")
	_, addrs := ipaRun(t, ts, subnet, 1, nil)
	require.Equal(t, []string{"10.4.1.4"}, addrs)

	var created struct {
		ID string `xml:"natGateway>natGatewayId"`
		IP string `xml:"natGateway>natGatewayAddressSet>item>privateIp"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateNatGateway", "SubnetId": subnet, "ConnectivityType": "private"}, &created)
	assert.Equal(t, "10.4.1.5", created.IP, "the gateway's address is the subnet's next free one")
	assert.Equal(t, 251-2, ipaAvailable(t, ts, subnet))

	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNatGateway", "SubnetId": subnet, "ConnectivityType": "private", "PrivateIpAddress": "10.4.1.4",
	}, "InvalidIPAddress.InUse")

	ec2OK(t, ts, map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": created.ID})
	assert.Equal(t, 251-1, ipaAvailable(t, ts, subnet), "a deleted gateway releases its address")
	ec2OK(t, ts, map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": created.ID})
	assert.Equal(t, 251-1, ipaAvailable(t, ts, subnet))
}

// TestEC2AddressAllocation_OlderStateKeepsItsAddresses covers state written before the
// allocator: an instance recorded with an address in the subnet keeps it, and the first launch
// after the upgrade does not hand it out again.
func TestEC2AddressAllocation_OlderStateKeepsItsAddresses(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.5.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.5.1.0/24")

	for i, ip := range []string{"10.5.1.4", "10.5.1.5"} {
		inst := emulator.EC2Instance{
			InstanceID: "i-0000000000000000" + strconv.Itoa(i+1), ReservationID: "r-00000000000000001",
			ImageID: ec2TestImage, InstanceType: "t3.micro", SubnetID: subnet, VPCID: vpc,
			State:            emulator.EC2InstanceState{Code: 16, Name: "running"},
			PrivateIPAddress: ip, AccountID: "123456789012", Region: "us-east-1",
		}
		data, err := json.Marshal(inst)
		require.NoError(t, err)
		require.NoError(t, state.Put(t.Context(), "ec2", "instance:123456789012/us-east-1/"+inst.InstanceID, data))
	}
	assert.Equal(t, 251-2, ipaAvailable(t, ts, subnet), "the record built from older state counts them")

	_, addrs := ipaRun(t, ts, subnet, 1, nil)
	assert.Equal(t, []string{"10.5.1.6"}, addrs)

	// The older instance reports a primary interface derived from its own ID, with no ID drawn.
	described := niDescribe(t, ts, "i-00000000000000001")
	require.Len(t, described.Interfaces, 1)
	assert.Equal(t, "eni-00000000000000001", described.Interfaces[0].NetworkInterfaceID)
	assert.Equal(t, "10.5.1.4", described.Interfaces[0].PrivateIPAddress)

	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-00000000000000001"})
	assert.Equal(t, 251-2, ipaAvailable(t, ts, subnet), "terminating it releases its address")
}

// TestEC2AddressAllocation_OlderInstanceWithDeclaredInterfaces covers an older instance whose
// launch declared interfaces, embedded on the instance record: they are described as they were
// recorded, an interface attached to it afterwards is a record of its own beside them, and
// terminating it releases the embedded addresses and leaves the attached interface available.
func TestEC2AddressAllocation_OlderInstanceWithDeclaredInterfaces(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.8.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.8.1.0/24")
	inst := emulator.EC2Instance{
		InstanceID: "i-0000000000000000a", ReservationID: "r-00000000000000001",
		ImageID: ec2TestImage, InstanceType: "t3.micro", SubnetID: subnet, VPCID: vpc,
		State:            emulator.EC2InstanceState{Code: 16, Name: "running"},
		PrivateIPAddress: "10.8.1.4", AccountID: "123456789012", Region: "us-east-1",
		NetworkInterfaces: []emulator.EC2NetworkInterface{
			{NetworkInterfaceID: "eni-000000000000000a1", DeviceIndex: 0, SubnetID: subnet, PrivateIPAddress: "10.8.1.4", DeleteOnTermination: true},
			{NetworkInterfaceID: "eni-000000000000000a2", DeviceIndex: 1, SubnetID: subnet, PrivateIPAddress: "10.8.1.9", DeleteOnTermination: true},
		},
	}
	data, err := json.Marshal(inst)
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "ec2", "instance:123456789012/us-east-1/"+inst.InstanceID, data))
	f := eniFixture{ts: ts, vpc: vpc, subnet: subnet}

	assert.Len(t, f.describe(t, map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": inst.InstanceID}), 2)
	extra := f.create(t, nil)
	assert.Equal(t, "10.8.1.5", extra.PrivateIP, "the embedded addresses are held")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "AttachNetworkInterface", "NetworkInterfaceId": extra.ID, "InstanceId": inst.InstanceID, "DeviceIndex": "1",
	}, "InvalidParameterValue")
	f.attach(t, extra.ID, inst.InstanceID, "2")
	assert.Len(t, niDescribe(t, ts, inst.InstanceID).Interfaces, 3)

	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": inst.InstanceID})
	assert.Equal(t, "available", f.one(t, extra.ID).Status)
	assert.Equal(t, 251-1, ipaAvailable(t, ts, subnet), "only the attached interface's address is still held")
}

func TestEC2AddressAllocation_SubnetWithNoCIDRStillAllocates(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var doc struct {
		ID string `xml:"subnet>subnetId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": ec2CreateVPC(t, ts, "10.6.0.0/16")}, &doc)
	_, addrs := ipaRun(t, ts, doc.ID, 2, nil)
	assert.Equal(t, []string{"172.31.0.4", "172.31.0.5"}, addrs)
	_, addrs = ipaRun(t, ts, doc.ID, 1, map[string]string{"PrivateIpAddress": "192.168.9.9"})
	assert.Equal(t, []string{"192.168.9.9"}, addrs, "a subnet stating no range holds a named address to none")
}

func TestEC2AddressAllocation_CreateSubnetReportsItsUsableCount(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var doc struct {
		Count int `xml:"subnet>availableIpAddressCount"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": ec2CreateVPC(t, ts, "10.7.0.0/16"), "CidrBlock": "10.7.0.0/20",
	}, &doc)
	assert.Equal(t, 4091, doc.Count)
}
