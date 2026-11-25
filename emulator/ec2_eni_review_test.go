package emulator_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2NetworkInterface_FleetLaunchesKeepTheirMintSequence pins a two-pool CreateFleet's IDs to
// the request mint's ordinals: the fleet ID, then each pool's reservation, instance and volume IDs
// and nothing else. The interface and attachment IDs a launch also draws come from the request's
// "eni" side stream, so they shift none of the IDs a recording of the same request holds.
func TestEC2NetworkInterface_FleetLaunchesKeepTheirMintSequence(t *testing.T) {
	t.Parallel()
	p, setup, _ := setupEC2WirePlugin(t)
	ltBody := ec2Wire(t, p, setup, "CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName": "replay-pools", "LaunchTemplateData.ImageId": ec2TestImage,
	})
	ltID := ec2WireElement(t, "CreateLaunchTemplate", ltBody, "launchTemplateId")

	const seed = "req-fleet-replay"
	fleetCtx := &emulator.RequestContext{
		AccountID: ec2WireAccount, Region: ec2WireRegion, RequestID: seed, IDs: emulator.NewIDMint(seed),
	}
	created := ec2Wire(t, p, fleetCtx, "CreateFleet", map[string]string{
		"Type": "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "t3.micro",
		"LaunchTemplateConfigs.1.Overrides.2.InstanceType":                     "t3.small",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "4",
		"TargetCapacitySpecification.DefaultTargetCapacityType":                "on-demand",
	})
	fleetID := ec2WireElement(t, "CreateFleet", created, "fleetId")

	read := &emulator.RequestContext{AccountID: ec2WireAccount, Region: ec2WireRegion, RequestID: "req-read", IDs: emulator.NewIDMint("req-read")}
	var got []string
	hexOf := regexp.MustCompile(`<(?:reservationId|instanceId|volumeId)>(?:r|i|vol)-([0-9a-f]{16})</`)
	for _, m := range hexOf.FindAllStringSubmatch(string(ec2Wire(t, p, read, "DescribeInstances", nil)), -1) {
		got = append(got, m[1])
	}
	for _, m := range hexOf.FindAllStringSubmatch(string(ec2Wire(t, p, read, "DescribeVolumes", nil)), -1) {
		got = append(got, m[1])
	}
	got = uniqueStrings(got)
	require.Len(t, got, 2+4+4, "two reservations, four instances, four root volumes")

	mint := emulator.NewIDMint(seed)
	assert.Equal(t, strings.ReplaceAll(strings.TrimPrefix(fleetID, "fleet-"), "-", "")[:16], mint.Hex(8),
		"the fleet ID is the request's first draw")
	var want []string
	for range got {
		want = append(want, mint.Hex(8))
	}
	assert.ElementsMatch(t, want, got,
		"the fleet's other IDs are exactly the mint's next draws, with no interface or attachment draw among them")
}

// uniqueStrings returns ss without repeats, in first-seen order.
func uniqueStrings(ss []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// TestEC2NetworkInterface_ModifyGroupsDoesNotReviveATerminatedInstance races a primary
// interface's group change against the instance's termination. The change writes the interface
// alone, so the instance ends terminated whichever lands first.
func TestEC2NetworkInterface_ModifyGroupsDoesNotReviveATerminatedInstance(t *testing.T) {
	t.Parallel()
	for range 10 {
		f := newENIFixture(t)
		ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
			"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
		})
		primary := f.describe(t, map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0]})
		require.Len(t, primary, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			resp := ec2Request(t, f.ts, map[string]string{
				"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": primary[0].ID, "SecurityGroupId.1": f.sg,
			})
			_ = resp.Body.Close()
		}()
		go func() {
			defer wg.Done()
			resp := ec2Request(t, f.ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0]})
			_ = resp.Body.Close()
		}()
		wg.Wait()
		var doc struct {
			States []string `xml:"reservationSet>item>instancesSet>item>instanceState>name"`
		}
		ec2FleetXML(t, f.ts, map[string]string{"Action": "DescribeInstances", "InstanceId.1": ids[0]}, &doc)
		require.Len(t, doc.States, 1)
		assert.Contains(t, []string{"shutting-down", "terminated"}, doc.States[0])
	}
}

func TestEC2NetworkInterface_ZoneMismatchIsRefused(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	var other struct {
		ID string `xml:"subnet>subnetId"`
	}
	ec2FleetXML(t, f.ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": f.vpc, "CidrBlock": "10.20.9.0/24", "AvailabilityZone": "us-east-1b",
	}, &other)
	elsewhere := f.create(t, map[string]string{"SubnetId": other.ID})
	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
	})

	msg := "You may not attach a network interface to an instance if they are not in the same availability zone"
	ec2RequireRefusalMessage(t, f.ts, map[string]string{
		"Action": "AttachNetworkInterface", "NetworkInterfaceId": elsewhere.ID, "InstanceId": ids[0], "DeviceIndex": "1",
	}, "InvalidParameterCombination", msg)
	ec2RequireRefusalMessage(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.SubnetId": other.ID,
	}, "InvalidParameterCombination", msg)
	ec2RequireRefusalMessage(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.NetworkInterfaceId": elsewhere.ID,
	}, "InvalidParameterCombination", msg)

	t.Run("another VPC in the same zone attaches", func(t *testing.T) {
		vpc := ec2CreateVPC(t, f.ts, "10.21.0.0/16")
		sameZone := ec2CreateSubnetIn(t, f.ts, vpc, "10.21.1.0/24")
		eni := f.create(t, map[string]string{"SubnetId": sameZone})
		f.attach(t, eni.ID, ids[0], "1")
	})
}

func TestEC2NetworkInterface_CreateWithTheSamePrimaryTwice(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	eni := f.create(t, map[string]string{
		"PrivateIpAddress":                      "10.20.1.30",
		"PrivateIpAddresses.1.PrivateIpAddress": "10.20.1.30", "PrivateIpAddresses.1.Primary": "true",
		"PrivateIpAddresses.2.PrivateIpAddress": "10.20.1.31",
	})
	assert.Equal(t, "10.20.1.30", eni.PrivateIP)
	require.Len(t, eni.Addresses, 2, "the repeated primary is one address")
	assert.Equal(t, "10.20.1.31", eni.Addresses[1].IP)

	for name, params := range map[string]map[string]string{
		"a different address marked primary": {
			"PrivateIpAddress":                      "10.20.1.40",
			"PrivateIpAddresses.1.PrivateIpAddress": "10.20.1.41", "PrivateIpAddresses.1.Primary": "true",
		},
		"two entries marked primary": {
			"PrivateIpAddresses.1.PrivateIpAddress": "10.20.1.42", "PrivateIpAddresses.1.Primary": "true",
			"PrivateIpAddresses.2.PrivateIpAddress": "10.20.1.43", "PrivateIpAddresses.2.Primary": "true",
		},
	} {
		t.Run(name, func(t *testing.T) {
			params["Action"] = "CreateNetworkInterface"
			params["SubnetId"] = f.subnet
			ec2RequireRefusal(t, f.ts, params, "InvalidParameterValue")
		})
	}
}

func TestEC2NetworkInterface_PrimaryIsDeviceZeroOnCardZero(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	for name, params := range map[string]map[string]string{
		"no interface at device index 0": {
			"NetworkInterface.1.DeviceIndex": "1", "NetworkInterface.1.SubnetId": f.subnet,
		},
		"device index 0 only on another card": {
			"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.NetworkCardIndex": "1",
			"NetworkInterface.1.SubnetId": f.subnet,
		},
		"one position twice": {
			"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
			"NetworkInterface.2.DeviceIndex": "0", "NetworkInterface.2.SubnetId": f.subnet,
		},
	} {
		t.Run(name, func(t *testing.T) {
			params["Action"] = "RunInstances"
			params["ImageId"] = ec2TestImage
			params["MinCount"], params["MaxCount"] = "1", "1"
			params["SubnetId"] = f.subnet
			ec2RequireRefusal(t, f.ts, params, "InvalidParameterValue")
		})
	}

	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": f.subnet,
	})
	eni := f.create(t, nil)
	var doc struct {
		AttachmentID string `xml:"attachmentId"`
	}
	ec2FleetXML(t, f.ts, map[string]string{
		"Action": "AttachNetworkInterface", "NetworkInterfaceId": eni.ID, "InstanceId": ids[0],
		"DeviceIndex": "0", "NetworkCardIndex": "1",
	}, &doc)
	ec2OK(t, f.ts, map[string]string{"Action": "DetachNetworkInterface", "AttachmentId": doc.AttachmentID})
	assert.Equal(t, "available", f.one(t, eni.ID).Status,
		"device index 0 on network card 1 is not the primary, so it detaches")
}

func TestEC2NetworkInterface_EfaOnlyAtLaunchHasNoAddress(t *testing.T) {
	t.Parallel()
	f := newENIFixture(t)
	before := ipaAvailable(t, f.ts, f.subnet)
	ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
		"NetworkInterface.2.DeviceIndex": "1", "NetworkInterface.2.SubnetId": f.subnet,
		"NetworkInterface.2.InterfaceType": "efa-only",
	})
	assert.Equal(t, before-1, ipaAvailable(t, f.ts, f.subnet), "only the primary takes an address")
	efa := f.describe(t, map[string]string{
		"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0],
		"Filter.2.Name": "interface-type", "Filter.2.Value.1": "efa-only",
	})
	require.Len(t, efa, 1)
	assert.Empty(t, efa[0].PrivateIP)
}

// ipaPutLegacyInstance writes an instance record shaped as an older build wrote it.
func ipaPutLegacyInstance(t *testing.T, state emulator.StateManager, inst emulator.EC2Instance) {
	t.Helper()
	inst.ReservationID = "r-00000000000000001"
	inst.ImageID = ec2TestImage
	inst.InstanceType = "t3.micro"
	inst.State = emulator.EC2InstanceState{Code: 16, Name: "running"}
	inst.AccountID, inst.Region = "123456789012", "us-east-1"
	data, err := json.Marshal(inst)
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "ec2", "instance:123456789012/us-east-1/"+inst.InstanceID, data))
}

func TestEC2NetworkInterface_OlderInterfacesAreStoredOnFirstModify(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.50.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.50.1.0/24")
	oldGroup := ec2CreateSG(t, ts, "old", "old", vpc)
	newGroup := ec2CreateSG(t, ts, "new", "new", vpc)
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-0000000000000000b", SubnetID: subnet, VPCID: vpc, AvailabilityZone: "us-east-1a",
		PrivateIPAddress: "10.50.1.4", SecurityGroupIDs: []string{oldGroup},
	})
	f := eniFixture{ts: ts, vpc: vpc, subnet: subnet}
	const eniID = "eni-0000000000000000b"

	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteNetworkInterface", "NetworkInterfaceId": eniID},
		"InvalidNetworkInterface.InUse")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID, "SecurityGroupId.1": "sg-0123456789abcdef0",
	}, "InvalidGroup.NotFound")
	ec2OK(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID, "SecurityGroupId.1": newGroup,
	})

	stored := f.one(t, eniID)
	require.Len(t, stored.Groups, 1)
	assert.Equal(t, newGroup, stored.Groups[0].GroupID)
	require.NotNil(t, stored.Attachment)
	assert.Regexp(t, `^eni-attach-`, stored.Attachment.AttachmentID, "the stored interface has an attachment ID")
	assert.Len(t, f.describe(t, map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": "i-0000000000000000b"}), 1,
		"the stored interface replaces the synthesized one")
	described := niDescribe(t, ts, "i-0000000000000000b")
	require.Len(t, described.Groups, 1)
	assert.Equal(t, newGroup, described.Groups[0].GroupID, "the instance's groupSet is its primary interface's")
	ec2OK(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": oldGroup})

	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-0000000000000000b"})
	assert.Equal(t, 251, ipaAvailable(t, ts, subnet), "the stored primary is deleted with the instance and frees its address")
}

func TestEC2NetworkInterface_OlderSharedAddressStaysHeld(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.60.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.60.1.0/24")
	for _, id := range []string{"i-0000000000000000c", "i-0000000000000000d"} {
		ipaPutLegacyInstance(t, state, emulator.EC2Instance{
			InstanceID: id, SubnetID: subnet, VPCID: vpc, PrivateIPAddress: "10.60.1.10",
		})
	}
	assert.Equal(t, 250, ipaAvailable(t, ts, subnet))
	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-0000000000000000c"})
	assert.Equal(t, 250, ipaAvailable(t, ts, subnet), "another live instance still reports the address")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"SubnetId": subnet, "PrivateIpAddress": "10.60.1.10",
	}, "InvalidIPAddress.InUse")
	ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-0000000000000000d"})
	assert.Equal(t, 251, ipaAvailable(t, ts, subnet), "the last instance reporting it frees it")
}

// TestEC2NetworkInterface_OlderInstanceRefusals covers what an older instance's unstored
// interfaces hold and refuse: its group, attaching or modifying an attachment it never recorded,
// and IDs that name nothing.
func TestEC2NetworkInterface_OlderInstanceRefusals(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	vpc := ec2CreateVPC(t, ts, "10.70.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpc, "10.70.1.0/24")
	group := ec2CreateSG(t, ts, "older", "older", vpc)
	ipaPutLegacyInstance(t, state, emulator.EC2Instance{
		InstanceID: "i-0000000000000000e", SubnetID: subnet, VPCID: vpc, AvailabilityZone: "us-east-1a",
		PrivateIPAddress: "10.70.1.4", SecurityGroupIDs: []string{group},
	})
	const eniID = "eni-0000000000000000e"

	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": group}, "DependencyViolation")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "AttachNetworkInterface", "NetworkInterfaceId": eniID, "InstanceId": "i-0000000000000000e", "DeviceIndex": "1",
	}, "InvalidNetworkInterface.InUse")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID,
		"Attachment.AttachmentId": "eni-attach-0123456789abcdef0", "Attachment.DeleteOnTermination": "false",
	}, "InvalidAttachmentID.NotFound")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": "eni-0123456789abcdef0", "Description.Value": "x",
	}, "InvalidNetworkInterfaceID.NotFound")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": "nope",
	}, "InvalidNetworkInterfaceID.Malformed")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "ModifyNetworkInterfaceAttribute"}, "MissingParameter")

	ec2OK(t, ts, map[string]string{
		"Action": "ModifyNetworkInterfaceAttribute", "NetworkInterfaceId": eniID, "Description.Value": "stored",
	})
	f := eniFixture{ts: ts, vpc: vpc, subnet: subnet}
	assert.Equal(t, "stored", f.one(t, eniID).Description)
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": group}, "DependencyViolation")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.NetworkInterfaceId": eniID,
	}, "InvalidNetworkInterface.InUse")
}
