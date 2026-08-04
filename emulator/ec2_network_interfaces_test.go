package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests below cover #455: every NetworkInterface.N a launch declares is parsed,
// not only index 1.
//
// They assert on the wire XML rather than on the stored record. An interface substrate
// parses and stores but does not report is a write with no observable, which is the
// defect shape this release is about — and `networkInterfaceSet` is the observation
// AWS's own RunInstances sample response carries.

// niInstance is the part of an instance item these tests read.
type niInstance struct {
	InstanceID       string `xml:"instanceId"`
	SubnetID         string `xml:"subnetId"`
	PrivateIPAddress string `xml:"privateIpAddress"`
	PublicIPAddress  string `xml:"publicIpAddress"`
	Groups           []struct {
		GroupID string `xml:"groupId"`
	} `xml:"groupSet>item"`
	Interfaces []niInterface `xml:"networkInterfaceSet>item"`
}

// niInterface is one networkInterfaceSet>item.
type niInterface struct {
	NetworkInterfaceID string `xml:"networkInterfaceId"`
	SubnetID           string `xml:"subnetId"`
	VpcID              string `xml:"vpcId"`
	Description        string `xml:"description"`
	OwnerID            string `xml:"ownerId"`
	Status             string `xml:"status"`
	PrivateIPAddress   string `xml:"privateIpAddress"`
	PrivateDNSName     string `xml:"privateDnsName"`
	InterfaceType      string `xml:"interfaceType"`
	Groups             []struct {
		GroupID string `xml:"groupId"`
	} `xml:"groupSet>item"`
	Attachment struct {
		DeviceIndex         int    `xml:"deviceIndex"`
		Status              string `xml:"status"`
		DeleteOnTermination bool   `xml:"deleteOnTermination"`
		NetworkCardIndex    int    `xml:"networkCardIndex"`
	} `xml:"attachment"`
	PrivateIPAddresses []struct {
		PrivateIPAddress string `xml:"privateIpAddress"`
		Primary          bool   `xml:"primary"`
	} `xml:"privateIpAddressesSet>item"`
}

// niRun launches instances and returns the RunInstances response's instance items.
func niRun(t *testing.T, ts *httptest.Server, params map[string]string) []niInstance {
	t.Helper()
	full := map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-12345678",
		"MinCount": "1",
		"MaxCount": "1",
	}
	for k, v := range params {
		full[k] = v
	}
	resp := ec2Request(t, ts, full)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "RunInstances")
	var out struct {
		XMLName   xml.Name     `xml:"RunInstancesResponse"`
		Instances []niInstance `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	return out.Instances
}

// niDescribe reads one instance back through DescribeInstances.
func niDescribe(t *testing.T, ts *httptest.Server, instanceID string) niInstance {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instanceID,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "DescribeInstances")
	var out struct {
		XMLName      xml.Name `xml:"DescribeInstancesResponse"`
		Reservations []struct {
			Instances []niInstance `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Reservations, 1)
	require.Len(t, out.Reservations[0].Instances, 1)
	return out.Reservations[0].Instances[0]
}

// niSubnets creates a VPC with two subnets and a security group in each, so a
// two-interface launch has somewhere real to attach.
//
// Security groups are validated against the target subnet's VPC, so both subnets
// share one VPC: a launch whose interfaces name subnets in different VPCs is not
// something AWS allows either.
func niSubnets(t *testing.T, ts *httptest.Server) (subnetA, subnetB, groupA, groupB string) {
	t.Helper()
	vpc := niCreate(t, ts, map[string]string{
		"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16",
	}, "CreateVpcResponse", "vpc>vpcId")
	subnetA = niCreate(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpc, "CidrBlock": "10.0.1.0/24",
	}, "CreateSubnetResponse", "subnet>subnetId")
	subnetB = niCreate(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpc, "CidrBlock": "10.0.2.0/24",
	}, "CreateSubnetResponse", "subnet>subnetId")
	groupA = niCreate(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "VpcId": vpc,
		"GroupName": "sg-a", "GroupDescription": "a",
	}, "CreateSecurityGroupResponse", "groupId")
	groupB = niCreate(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "VpcId": vpc,
		"GroupName": "sg-b", "GroupDescription": "b",
	}, "CreateSecurityGroupResponse", "groupId")
	return subnetA, subnetB, groupA, groupB
}

// niCreate sends a create request and pulls one element out of its response.
func niCreate(t *testing.T, ts *httptest.Server, params map[string]string, root, path string) string {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, params["Action"])
	var id string
	require.NoError(t, niDecodePath(xml.NewDecoder(resp.Body), root, path, &id))
	require.NotEmpty(t, id, "%s returned no id at %s", params["Action"], path)
	return id
}

// niDecodePath walks an XML document to a slash-separated element path and returns
// that element's character data.
//
// Hand-written rather than done with struct tags because the path differs per action
// and a tag cannot be built at run time. It is deliberately shallow: it matches the
// first element at each level, which is all these single-resource create responses
// contain.
func niDecodePath(dec *xml.Decoder, root, path string, out *string) error {
	want := append([]string{root}, niSplit(path)...)
	depth := 0
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if depth < len(want) && start.Name.Local == want[depth] {
			depth++
			if depth == len(want) {
				return dec.DecodeElement(out, &start)
			}
		}
	}
}

// niSplit splits a slash-separated element path.
func niSplit(path string) []string {
	var out []string
	current := ""
	for _, r := range path {
		if r == '>' {
			out = append(out, current)
			current = ""
			continue
		}
		current += string(r)
	}
	return append(out, current)
}

// TestEC2NetworkInterfaces_TwoInterfacesBothReported is #455's headline: a launch
// declaring two interfaces reports two, where it reported one.
func TestEC2NetworkInterfaces_TwoInterfacesBothReported(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, groupA, groupB := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":         "0",
		"NetworkInterface.1.SubnetId":            subnetA,
		"NetworkInterface.1.SecurityGroupId.1":   groupA,
		"NetworkInterface.2.DeviceIndex":         "1",
		"NetworkInterface.2.SubnetId":            subnetB,
		"NetworkInterface.2.SecurityGroupId.1":   groupB,
		"NetworkInterface.2.Description":         "secondary",
		"NetworkInterface.2.DeleteOnTermination": "false",
	})
	require.Len(t, instances, 1)
	inst := instances[0]

	require.Len(t, inst.Interfaces, 2,
		"both declared interfaces are reported; one is the pre-fix behavior")

	// The primary's values are the instance's top-level ones, which is what real EC2
	// puts there — the secondary must not displace them.
	assert.Equal(t, subnetA, inst.SubnetID,
		"the instance's subnetId is the primary interface's")
	require.Len(t, inst.Groups, 1)
	assert.Equal(t, groupA, inst.Groups[0].GroupID,
		"the instance's groupSet is the primary interface's")

	primary, secondary := inst.Interfaces[0], inst.Interfaces[1]
	assert.Equal(t, 0, primary.Attachment.DeviceIndex)
	assert.Equal(t, subnetA, primary.SubnetID)
	assert.Equal(t, inst.PrivateIPAddress, primary.PrivateIPAddress,
		"the primary interface's address is the instance's, not a second one")
	require.Len(t, primary.Groups, 1)
	assert.Equal(t, groupA, primary.Groups[0].GroupID)
	assert.True(t, primary.Attachment.DeleteOnTermination,
		"an interface the launch creates defaults to delete-on-termination")

	assert.Equal(t, 1, secondary.Attachment.DeviceIndex)
	assert.Equal(t, subnetB, secondary.SubnetID)
	require.Len(t, secondary.Groups, 1)
	assert.Equal(t, groupB, secondary.Groups[0].GroupID,
		"the secondary keeps its own groups rather than inheriting the primary's")
	assert.Equal(t, "secondary", secondary.Description)
	assert.False(t, secondary.Attachment.DeleteOnTermination,
		"an explicit DeleteOnTermination=false wins over the default")
	assert.NotEqual(t, primary.PrivateIPAddress, secondary.PrivateIPAddress,
		"two interfaces on one instance have distinct addresses")
	assert.NotEqual(t, primary.NetworkInterfaceID, secondary.NetworkInterfaceID,
		"two interfaces have distinct IDs")

	for _, ifc := range inst.Interfaces {
		assert.Regexp(t, `^eni-[0-9a-f]+$`, ifc.NetworkInterfaceID)
		assert.Equal(t, "in-use", ifc.Status)
		assert.Equal(t, "attached", ifc.Attachment.Status)
		assert.Equal(t, "interface", ifc.InterfaceType,
			"an absent InterfaceType reads back as the documented default")
		assert.NotEmpty(t, ifc.VpcID, "an interface reports the VPC it is in")
		assert.NotEmpty(t, ifc.OwnerID)
		assert.NotEmpty(t, ifc.PrivateDNSName)
		require.Len(t, ifc.PrivateIPAddresses, 1)
		assert.Equal(t, ifc.PrivateIPAddress, ifc.PrivateIPAddresses[0].PrivateIPAddress)
		assert.True(t, ifc.PrivateIPAddresses[0].Primary)
	}
}

// TestEC2NetworkInterfaces_DescribeInstancesReportsThem checks the interfaces survive
// the state round-trip, since RunInstances answers from the in-memory instance while
// DescribeInstances answers from what was persisted.
func TestEC2NetworkInterfaces_DescribeInstancesReportsThem(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, groupA, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":       "0",
		"NetworkInterface.1.SubnetId":          subnetA,
		"NetworkInterface.1.SecurityGroupId.1": groupA,
		"NetworkInterface.2.DeviceIndex":       "1",
		"NetworkInterface.2.SubnetId":          subnetB,
	})
	require.Len(t, instances, 1)

	described := niDescribe(t, ts, instances[0].InstanceID)
	require.Len(t, described.Interfaces, 2,
		"the interfaces were persisted, not only echoed")
	assert.Equal(t, instances[0].Interfaces[0].NetworkInterfaceID,
		described.Interfaces[0].NetworkInterfaceID,
		"the same interface IDs read back; a describe does not mint new ones")
	assert.Equal(t, subnetB, described.Interfaces[1].SubnetID)
}

// TestEC2NetworkInterfaces_DeviceIndexKeysIdentity is the plan's named mutant made a
// test: identity is DeviceIndex, not the parameter index.
//
// The interfaces are declared in the reverse of their attachment order, so a
// parameter-index implementation names subnetB the primary and puts the instance's
// top-level subnetId in the wrong place. AWS documents DeviceIndex as "the position of
// the network interface in the attachment order", which the parameter index does not
// have to agree with.
func TestEC2NetworkInterfaces_DeviceIndexKeysIdentity(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, groupA, groupB := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":       "3",
		"NetworkInterface.1.SubnetId":          subnetB,
		"NetworkInterface.1.SecurityGroupId.1": groupB,
		"NetworkInterface.2.DeviceIndex":       "0",
		"NetworkInterface.2.SubnetId":          subnetA,
		"NetworkInterface.2.SecurityGroupId.1": groupA,
	})
	require.Len(t, instances, 1)
	inst := instances[0]

	assert.Equal(t, subnetA, inst.SubnetID,
		"the primary is DeviceIndex 0, whichever parameter index declared it")
	require.Len(t, inst.Groups, 1)
	assert.Equal(t, groupA, inst.Groups[0].GroupID,
		"the instance's groups are the primary's, keyed on DeviceIndex")

	require.Len(t, inst.Interfaces, 2)
	assert.Equal(t, 0, inst.Interfaces[0].Attachment.DeviceIndex,
		"interfaces are reported in attachment order, primary first")
	assert.Equal(t, 3, inst.Interfaces[1].Attachment.DeviceIndex,
		"a device index need not be contiguous; 3 is reported as 3")
	assert.Equal(t, subnetB, inst.Interfaces[1].SubnetID)
}

// TestEC2NetworkInterfaces_GapStopsTheParse checks the contiguous-from-1 rule
// [indexedParams] already used for string lists: index 3 without index 2 is not read.
//
// This is not pedantry about a malformed request. Stopping at the gap is what bounds
// the loop — an implementation that scanned for some maximum index instead would have
// to choose one, and would silently drop anything above it.
func TestEC2NetworkInterfaces_GapStopsTheParse(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex": "0",
		"NetworkInterface.1.SubnetId":    subnetA,
		"NetworkInterface.3.DeviceIndex": "2",
		"NetworkInterface.3.SubnetId":    subnetB,
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 1,
		"the parse stops at the missing index 2, so index 3 is not read")
	assert.Equal(t, subnetA, instances[0].Interfaces[0].SubnetID)
}

// TestEC2NetworkInterfaces_SingleInterfaceUnchanged is the no-regression guard: the
// index-1-only launch that worked before must behave identically.
//
// It is the case every existing launch-template and spawn test exercises, and the one
// a parse-all-N change is most likely to break.
func TestEC2NetworkInterfaces_SingleInterfaceUnchanged(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, _, groupA, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":              "0",
		"NetworkInterface.1.SubnetId":                 subnetA,
		"NetworkInterface.1.SecurityGroupId.1":        groupA,
		"NetworkInterface.1.AssociatePublicIpAddress": "true",
	})
	require.Len(t, instances, 1)
	inst := instances[0]

	assert.Equal(t, subnetA, inst.SubnetID)
	require.Len(t, inst.Groups, 1)
	assert.Equal(t, groupA, inst.Groups[0].GroupID)
	require.Len(t, inst.Interfaces, 1)
	assert.Equal(t, 0, inst.Interfaces[0].Attachment.DeviceIndex)

	described := niDescribe(t, ts, inst.InstanceID)
	assert.NotEmpty(t, described.PrivateIPAddress)
}

// TestEC2NetworkInterfaces_NoInterfaceDeclared checks that a launch naming no
// interface reports no interface set rather than a phantom one.
//
// Substrate does not create an ENI record for the interface every real instance has,
// because it does not model ENIs as resources; reporting one here would be an
// observation with nothing behind it.
func TestEC2NetworkInterfaces_NoInterfaceDeclared(t *testing.T) {
	ts := newEC2TestServer(t)

	instances := niRun(t, ts, map[string]string{"InstanceType": "t3.micro"})
	require.Len(t, instances, 1)
	assert.Empty(t, instances[0].Interfaces,
		"a launch declaring no interface reports none")
	assert.NotEmpty(t, instances[0].SubnetID,
		"the default-VPC subnet still applies")
}

// TestEC2NetworkInterfaces_MultiCountDistinctAddresses checks that two instances from
// one launch do not report the same secondary address.
func TestEC2NetworkInterfaces_MultiCountDistinctAddresses(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"MaxCount":                       "2",
		"NetworkInterface.1.DeviceIndex": "0",
		"NetworkInterface.1.SubnetId":    subnetA,
		"NetworkInterface.2.DeviceIndex": "1",
		"NetworkInterface.2.SubnetId":    subnetB,
	})
	require.Len(t, instances, 2)

	seen := map[string]string{}
	for _, inst := range instances {
		require.Len(t, inst.Interfaces, 2)
		for _, ifc := range inst.Interfaces {
			require.NotEmpty(t, ifc.PrivateIPAddress)
			prior, dup := seen[ifc.PrivateIPAddress]
			assert.False(t, dup,
				"address %s is on both %s and %s", ifc.PrivateIPAddress, prior, inst.InstanceID)
			seen[ifc.PrivateIPAddress] = inst.InstanceID
		}
	}
}

// TestEC2NetworkInterfaces_ExistingInterfaceAttached checks the two things that change
// when a launch attaches an interface it did not create: the ID is the caller's, and
// DeleteOnTermination defaults to false rather than true.
//
// The default is not arbitrary. Deleting an interface the caller brought would destroy
// something the launch did not make, which is why AWS defaults it the other way for an
// attached interface than for a created one.
func TestEC2NetworkInterfaces_ExistingInterfaceAttached(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, _, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":        "0",
		"NetworkInterface.1.NetworkInterfaceId": "eni-0abcdef01234567",
		"NetworkInterface.2.DeviceIndex":        "1",
		"NetworkInterface.2.SubnetId":           subnetA,
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 2)

	attached := instances[0].Interfaces[0]
	assert.Equal(t, "eni-0abcdef01234567", attached.NetworkInterfaceID,
		"an attached interface keeps the ID the caller named")
	assert.False(t, attached.Attachment.DeleteOnTermination,
		"an interface the launch did not create is not deleted with the instance")
	assert.True(t, instances[0].Interfaces[1].Attachment.DeleteOnTermination,
		"one the launch did create is")
}

// TestEC2NetworkInterfaces_InterfaceTypeAndNetworkCard checks two members that only
// matter for reading back what was declared: an EFA interface and a non-zero network
// card.
func TestEC2NetworkInterfaces_InterfaceTypeAndNetworkCard(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":      "0",
		"NetworkInterface.1.SubnetId":         subnetA,
		"NetworkInterface.2.DeviceIndex":      "1",
		"NetworkInterface.2.SubnetId":         subnetB,
		"NetworkInterface.2.InterfaceType":    "efa",
		"NetworkInterface.2.NetworkCardIndex": "1",
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 2)

	assert.Equal(t, 0, instances[0].Interfaces[0].Attachment.NetworkCardIndex,
		"the primary must be on network card 0")
	assert.Equal(t, "efa", instances[0].Interfaces[1].InterfaceType)
	assert.Equal(t, 1, instances[0].Interfaces[1].Attachment.NetworkCardIndex)
}

// TestEC2NetworkInterfaces_PrivateIPAddressHonored checks that an address the request
// names on a secondary interface is the one reported, rather than a substrate-assigned
// one.
func TestEC2NetworkInterfaces_PrivateIPAddressHonored(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":      "0",
		"NetworkInterface.1.SubnetId":         subnetA,
		"NetworkInterface.2.DeviceIndex":      "1",
		"NetworkInterface.2.SubnetId":         subnetB,
		"NetworkInterface.2.PrivateIpAddress": "10.0.2.77",
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 2)

	secondary := instances[0].Interfaces[1]
	assert.Equal(t, "10.0.2.77", secondary.PrivateIPAddress)
	assert.Equal(t, "ip-10-0-2-77.us-east-1.compute.internal", secondary.PrivateDNSName,
		"the DNS name is derived from the address the caller named")
}

// TestEC2NetworkInterfaces_LaunchTemplateAllInterfaces is the launch-template half:
// LaunchTemplateData.NetworkInterface.N is parsed for all N too.
func TestEC2NetworkInterfaces_LaunchTemplateAllInterfaces(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, groupA, groupB := niSubnets(t, ts)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "two-nics",
		"LaunchTemplateData.ImageId": "ami-12345678",
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":       "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":          subnetA,
		"LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1": groupA,
		"LaunchTemplateData.NetworkInterface.2.DeviceIndex":       "1",
		"LaunchTemplateData.NetworkInterface.2.SubnetId":          subnetB,
		"LaunchTemplateData.NetworkInterface.2.SecurityGroupId.1": groupB,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// The template reads back with both interfaces.
	versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateName": "two-nics"})
	require.Len(t, versions, 1)
	require.Len(t, versions[0].Data.Interfaces, 2,
		"a template's second interface reads back; losing it was the pre-fix behavior")
	assert.Equal(t, subnetA, versions[0].Data.Interfaces[0].SubnetID)
	assert.Equal(t, subnetB, versions[0].Data.Interfaces[1].SubnetID)

	// And a launch from it gets both.
	instances := niRun(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateName": "two-nics",
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 2,
		"a launch from the template attaches both interfaces")
	assert.Equal(t, subnetA, instances[0].SubnetID,
		"the instance's subnet is the template's primary interface's")
	assert.Equal(t, subnetB, instances[0].Interfaces[1].SubnetID)
}

// TestEC2NetworkInterfaces_RequestInterfacesBeatTemplate checks the precedence rule:
// a request declaring its own interfaces does not inherit the template's.
//
// All-or-nothing rather than per-device-index, because the reference's rule is that
// "any additional parameters that you specify for the new instance overwrite the
// corresponding parameters included in the launch template" — merging per index would
// let a request naming one interface silently acquire a second.
func TestEC2NetworkInterfaces_RequestInterfacesBeatTemplate(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, _, _ := niSubnets(t, ts)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "tmpl-two",
		"LaunchTemplateData.ImageId": "ami-12345678",
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":    subnetB,
		"LaunchTemplateData.NetworkInterface.2.DeviceIndex": "1",
		"LaunchTemplateData.NetworkInterface.2.SubnetId":    subnetB,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	instances := niRun(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateName": "tmpl-two",
		"NetworkInterface.1.DeviceIndex":    "0",
		"NetworkInterface.1.SubnetId":       subnetA,
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 1,
		"the request's one interface replaces the template's two rather than merging")
	assert.Equal(t, subnetA, instances[0].Interfaces[0].SubnetID)
}

// TestEC2NetworkInterfaces_TemplateFlatFieldsStillLaunch checks that a template
// whose one interface names no DeviceIndex is still read — the shape every template
// created before #455 was created as.
func TestEC2NetworkInterfaces_TemplateFlatFieldsStillLaunch(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, _, groupA, _ := niSubnets(t, ts)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "flat",
		"LaunchTemplateData.ImageId": "ami-12345678",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":          subnetA,
		"LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1": groupA,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	instances := niRun(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateName": "flat",
	})
	require.Len(t, instances, 1)
	assert.Equal(t, subnetA, instances[0].SubnetID,
		"an interface naming a subnet but no DeviceIndex is still read")
	require.Len(t, instances[0].Groups, 1)
	assert.Equal(t, groupA, instances[0].Groups[0].GroupID)
}

// TestEC2NetworkInterfaces_PreSliceTemplateStateStillWorks is the replay gate: a
// template stored before the interface slice existed carries its networking in the
// flat fields alone, and must still launch and still read back.
//
// The stored JSON is written by hand because CreateLaunchTemplate can no longer
// produce it — it now always writes the slice too — and that is exactly what makes
// this a migration rather than a round-trip. Without the flat-field fallback in
// ec2LTVersionData, such a template would describe with an empty
// networkInterfaceSet, so a replayed event log would report a template with no
// networking at all while the record plainly holds a subnet.
func TestEC2NetworkInterfaces_PreSliceTemplateStateStillWorks(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	subnetA, _, groupA, _ := niSubnets(t, ts)

	// The shape createLaunchTemplate wrote before #455: subnetId,
	// associatePublicIpAddress and networkInterfaceGroups, no networkInterfaces key.
	const acct, region, ltID = "000000000000", "us-east-1", "lt-0preslice00001"
	stored := map[string]any{
		"launchTemplateId":     ltID,
		"launchTemplateName":   "preslice",
		"defaultVersionNumber": 1,
		"latestVersionNumber":  1,
		"createdBy":            acct,
		"createTime":           "2026-01-01T00:00:00Z",
		"latestData": map[string]any{
			"imageId":                  "ami-12345678",
			"instanceType":             "t3.small",
			"subnetId":                 subnetA,
			"associatePublicIpAddress": "true",
			"networkInterfaceGroups":   []string{groupA},
		},
		"accountID": acct,
		"region":    region,
	}
	raw, err := json.Marshal(stored)
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, state.Put(ctx, "ec2", "lt:"+acct+"/"+region+"/"+ltID, raw))
	require.NoError(t, state.Put(ctx, "ec2", "lt_by_name:"+acct+"/"+region+"/preslice", []byte(ltID)))
	idsRaw, err := json.Marshal([]string{ltID})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "ec2", "lt_ids:"+acct+"/"+region, idsRaw))

	instances := niRun(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
	require.Len(t, instances, 1)
	assert.Equal(t, subnetA, instances[0].SubnetID, "the stored subnet still launches")
	require.Len(t, instances[0].Groups, 1)
	assert.Equal(t, groupA, instances[0].Groups[0].GroupID)
	assert.NotEmpty(t, niDescribe(t, ts, instances[0].InstanceID).PublicIPAddress,
		"the stored public-IP preference still applies")

	// And it describes as the one interface it holds, synthesized from those fields.
	versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
	require.Len(t, versions, 1)
	require.Len(t, versions[0].Data.Interfaces, 1,
		"a pre-slice template describes its interface, not an empty set")
	assert.Equal(t, subnetA, versions[0].Data.Interfaces[0].SubnetID)
	assert.Equal(t, "true", versions[0].Data.Interfaces[0].AssociatePublicIPAddress)
	assert.Equal(t, []string{groupA}, versions[0].Data.Interfaces[0].Groups)
}

// TestEC2NetworkInterfaces_PublicIPFromPrimaryOnly checks that the public-IP
// preference is read from the primary interface alone.
//
// AWS documents the public IP as assignable "to a network interface for eth0" only, so
// a secondary interface asking for one is not a statement about the instance's public
// IP. Reading it from any interface would let a secondary silently turn on a public
// address the caller asked for somewhere it does not apply.
func TestEC2NetworkInterfaces_PublicIPFromPrimaryOnly(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, subnetB, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":              "0",
		"NetworkInterface.1.SubnetId":                 subnetA,
		"NetworkInterface.2.DeviceIndex":              "1",
		"NetworkInterface.2.SubnetId":                 subnetB,
		"NetworkInterface.2.AssociatePublicIpAddress": "true",
	})
	require.Len(t, instances, 1)

	described := niDescribe(t, ts, instances[0].InstanceID)
	assert.Empty(t, described.PublicIPAddress,
		"a secondary interface's public-IP preference does not apply to the instance")

	// The same preference on the primary does apply, which is what makes the
	// assertion above about *which* interface rather than about the feature.
	onPrimary := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex":              "0",
		"NetworkInterface.1.SubnetId":                 subnetA,
		"NetworkInterface.1.AssociatePublicIpAddress": "true",
	})
	require.Len(t, onPrimary, 1)
	assert.NotEmpty(t, niDescribe(t, ts, onPrimary[0].InstanceID).PublicIPAddress)
}

// TestEC2NetworkInterfaces_DeviceIndexUnparseable checks that a non-numeric device
// index does not fail the launch or make the interface vanish.
//
// It reads as 0, the documented primary. A launch is not rejected over a member
// substrate can infer, and the interface's subnet is unambiguous whatever the index
// says.
func TestEC2NetworkInterfaces_DeviceIndexUnparseable(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, _, _, _ := niSubnets(t, ts)

	instances := niRun(t, ts, map[string]string{
		"NetworkInterface.1.DeviceIndex": "primary",
		"NetworkInterface.1.SubnetId":    subnetA,
	})
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, 1,
		"an unparseable device index does not drop the interface")
	assert.Equal(t, 0, instances[0].Interfaces[0].Attachment.DeviceIndex)
	assert.Equal(t, subnetA, instances[0].SubnetID)
}

// TestEC2NetworkInterfaces_ManyInterfaces checks the loop is bounded by the request
// rather than by a hard-coded ceiling, which is the failure mode a "scan up to N"
// implementation has.
func TestEC2NetworkInterfaces_ManyInterfaces(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetA, _, _, _ := niSubnets(t, ts)

	params := map[string]string{}
	const count = 8
	for n := 1; n <= count; n++ {
		params["NetworkInterface."+strconv.Itoa(n)+".DeviceIndex"] = strconv.Itoa(n - 1)
		params["NetworkInterface."+strconv.Itoa(n)+".SubnetId"] = subnetA
	}
	instances := niRun(t, ts, params)
	require.Len(t, instances, 1)
	require.Len(t, instances[0].Interfaces, count)
	for n, ifc := range instances[0].Interfaces {
		assert.Equal(t, n, ifc.Attachment.DeviceIndex)
	}
}
