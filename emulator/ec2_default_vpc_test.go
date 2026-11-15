package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The default VPC every account starts with in each Region (#1464).
//
// Through v0.125.0 a fresh substrate held no VPC at all — DescribeVpcs and DescribeSubnets
// answered nothing, with or without is-default and default-for-az — and one was created only
// by a RunInstances that named no subnet, with a single subnet. These tests pin what a real
// account has after reset: one default VPC (172.31.0.0/16), a default subnet per zone, the
// default security group, an attached internet gateway, and a main route table routing
// 0.0.0.0/0 to it.

// ec2DefaultVPCView is the default VPC and its resources, decoded out of the five describes a
// consumer reads them through.
type ec2DefaultVPCView struct {
	vpcID   string
	subnets []describedSubnet
	group   ec2DefaultSG
	igwID   string
	routes  map[string]string
	main    bool
}

type ec2DefaultSG struct {
	GroupID   string `xml:"groupId"`
	GroupName string `xml:"groupName"`
	VpcID     string `xml:"vpcId"`
	OwnerID   string `xml:"ownerId"`
	Ingress   []struct {
		Protocol string `xml:"ipProtocol"`
		Groups   []struct {
			GroupID string `xml:"groupId"`
			UserID  string `xml:"userId"`
		} `xml:"groups>item"`
	} `xml:"ipPermissions>item"`
	Egress []struct {
		Protocol string   `xml:"ipProtocol"`
		CIDRs    []string `xml:"ipRanges>item>cidrIp"`
	} `xml:"ipPermissionsEgress>item"`
}

func ec2ReadDefaultVPC(t *testing.T, call func(map[string]string) []byte) ec2DefaultVPCView {
	t.Helper()
	var view ec2DefaultVPCView

	var vpcs struct {
		Items []ec2TestVPC `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeVpcs", "is-default", "true")), &vpcs))
	require.Len(t, vpcs.Items, 1, "is-default=true selects exactly one VPC")
	assert.True(t, vpcs.Items[0].IsDefault)
	assert.Equal(t, "172.31.0.0/16", vpcs.Items[0].CIDRBlock)
	assert.Equal(t, "available", vpcs.Items[0].State)
	view.vpcID = vpcs.Items[0].VpcID

	var subnets struct {
		Items []describedSubnet `xml:"subnetSet>item"`
	}
	// vpc-id as well, because DeleteVpc does not cascade: a deleted default VPC's subnets are
	// still listed, and a recreated one's are additional to them.
	require.NoError(t, xml.Unmarshal(call(map[string]string{
		"Action":        "DescribeSubnets",
		"Filter.1.Name": "default-for-az", "Filter.1.Value.1": "true",
		"Filter.2.Name": "vpc-id", "Filter.2.Value.1": view.vpcID,
	}), &subnets))
	view.subnets = subnets.Items
	sort.Slice(view.subnets, func(i, j int) bool {
		return view.subnets[i].AvailabilityZone < view.subnets[j].AvailabilityZone
	})

	var groups struct {
		Items []ec2DefaultSG `xml:"securityGroupInfo>item"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{
		"Action":        "DescribeSecurityGroups",
		"Filter.1.Name": "vpc-id", "Filter.1.Value.1": view.vpcID,
		"Filter.2.Name": "group-name", "Filter.2.Value.1": "default",
	}), &groups))
	require.Len(t, groups.Items, 1, "the default VPC has one group named default")
	view.group = groups.Items[0]

	var igws struct {
		Items []ec2TestIGW `xml:"internetGatewaySet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeInternetGateways", "attachment.vpc-id", view.vpcID)), &igws))
	require.Len(t, igws.Items, 1, "one internet gateway is attached to the default VPC")
	view.igwID = igws.Items[0].InternetGatewayID

	var rtbs struct {
		Items []struct {
			Routes []struct {
				Dest    string `xml:"destinationCidrBlock"`
				Gateway string `xml:"gatewayId"`
			} `xml:"routeSet>item"`
			Associations []struct {
				Main bool `xml:"main"`
			} `xml:"associationSet>item"`
		} `xml:"routeTableSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeRouteTables", "vpc-id", view.vpcID)), &rtbs))
	require.Len(t, rtbs.Items, 1, "the default VPC has one route table, its main one")
	view.routes = map[string]string{}
	for _, r := range rtbs.Items[0].Routes {
		view.routes[r.Dest] = r.Gateway
	}
	for _, a := range rtbs.Items[0].Associations {
		view.main = view.main || a.Main
	}
	return view
}

// ec2CallHTTP returns a request function over an httptest server, failing on a non-200.
func ec2CallHTTP(t *testing.T, call func(map[string]string) *http.Response) func(map[string]string) []byte {
	return func(params map[string]string) []byte {
		t.Helper()
		resp := call(params)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
		return body
	}
}

// TestEC2_DefaultVPC_SeededBeforeTheFirstRead is the issue's repro: on a fresh server, the
// very first call is a describe, and it already sees the default VPC and everything AWS
// creates with one.
func TestEC2_DefaultVPC_SeededBeforeTheFirstRead(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	view := ec2ReadDefaultVPC(t, ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) }))

	require.Len(t, view.subnets, 3, "one default subnet per seeded Availability Zone")
	for i, want := range []struct{ zone, cidr string }{
		{"us-east-1a", "172.31.0.0/20"},
		{"us-east-1b", "172.31.16.0/20"},
		{"us-east-1c", "172.31.32.0/20"},
	} {
		s := view.subnets[i]
		assert.Equal(t, want.zone, s.AvailabilityZone)
		assert.Equal(t, want.cidr, s.CIDRBlock)
		assert.Equal(t, view.vpcID, s.VpcID)
		assert.True(t, s.DefaultForAz, "%s is the zone's default subnet", s.SubnetID)
		assert.True(t, s.MapPublicIPOnLaunch, "a default subnet is public without a NAT gateway")
	}

	assert.Equal(t, view.vpcID, view.group.VpcID)
	assert.NotEmpty(t, view.group.OwnerID)
	require.Len(t, view.group.Ingress, 1, "the default group's one ingress rule")
	assert.Equal(t, "-1", view.group.Ingress[0].Protocol)
	require.Len(t, view.group.Ingress[0].Groups, 1)
	assert.Equal(t, view.group.GroupID, view.group.Ingress[0].Groups[0].GroupID,
		"the ingress rule admits the group itself, as AWS's default group does")
	require.Len(t, view.group.Egress, 1)
	assert.Equal(t, []string{"0.0.0.0/0"}, view.group.Egress[0].CIDRs)

	assert.True(t, view.main, "the route table is the VPC's main one")
	assert.Equal(t, "local", view.routes["172.31.0.0/16"])
	assert.Equal(t, view.igwID, view.routes["0.0.0.0/0"], "the default route goes to the attached gateway")
}

// TestEC2_DefaultVPC_IsStablePerAccountAndRegion pins that the seed's identifiers derive from the
// account and Region rather than from whichever request triggered it: two servers seed the same
// default VPC ID, and a second Region gets a different default VPC of its own.
func TestEC2_DefaultVPC_IsStablePerAccountAndRegion(t *testing.T) {
	t.Parallel()
	first := ec2DescribeVPCs(t, newEC2TestServer(t), ec2OneFilter("DescribeVpcs", "is-default", "true"))
	ts := newEC2TestServer(t)
	ec2CreateVPC(t, ts, "10.0.0.0/16") // a different first request
	second := ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "is-default", "true"))
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	assert.Equal(t, first[0].VpcID, second[0].VpcID, "the default VPC ID does not depend on the first request")

	srv := emulator.StartTestServer(t)
	call := func(region string) string {
		form := url.Values{"Action": {"DescribeVpcs"}, "Filter.1.Name": {"is-default"}, "Filter.1.Value.1": {"true"}}
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/", strings.NewReader(form.Encode()))
		require.NoError(t, err)
		req.Host = "ec2." + region + ".amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		var doc struct {
			Items []ec2TestVPC `xml:"vpcSet>item"`
		}
		require.NoError(t, xml.NewDecoder(resp.Body).Decode(&doc))
		require.Len(t, doc.Items, 1, "region %s has its own default VPC", region)
		return doc.Items[0].VpcID
	}
	assert.NotEqual(t, call("us-east-1"), call("eu-west-1"))
}

// TestEC2_DefaultVPC_LaunchLandsInTheZoneASubnet pins that a launch naming no subnet uses the
// default VPC, in its zone-a subnet, with the default group and a public address.
func TestEC2_DefaultVPC_LaunchLandsInTheZoneASubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	view := ec2ReadDefaultVPC(t, ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) }))

	var run struct {
		Instances []struct {
			SubnetID string   `xml:"subnetId"`
			VpcID    string   `xml:"vpcId"`
			Groups   []string `xml:"groupSet>item>groupId"`
			Zone     string   `xml:"placement>availabilityZone"`
		} `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
	}, &run)
	require.Len(t, run.Instances, 1)
	inst := run.Instances[0]
	assert.Equal(t, view.subnets[0].SubnetID, inst.SubnetID)
	assert.Equal(t, view.vpcID, inst.VpcID)
	assert.Equal(t, "us-east-1a", inst.Zone)
	assert.Equal(t, []string{view.group.GroupID}, inst.Groups)

	assert.Len(t, ec2DescribeVPCs(t, ts, map[string]string{"Action": "DescribeVpcs"}), 1,
		"the launch created no second default VPC")
}

// TestEC2_DefaultVPC_SecurityGroupWithoutVpcIdLandsInIt pins CreateSecurityGroup's "VpcId ...
// Required for a nondefault VPC": a group created without one is in the default VPC.
func TestEC2_DefaultVPC_SecurityGroupWithoutVpcIdLandsInIt(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	defaults := ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "is-default", "true"))
	require.Len(t, defaults, 1)
	ec2CreateSG(t, ts, "workers", "no ingress", "")
	assert.ElementsMatch(t, []string{"default", "workers"},
		ec2SecurityGroupNames(t, ts, ec2Filter("vpc-id", defaults[0].VpcID)))
}

// TestEC2_DefaultVPC_DeletedStaysDeleted pins the tombstone and the two operations that act on
// it: a deleted default VPC is not re-seeded, a launch naming no subnet is then refused with
// VPCIdNotSpecified, and CreateDefaultVpc brings one back — with a new ID and every resource.
func TestEC2_DefaultVPC_DeletedStaysDeleted(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	seeded := ec2ReadDefaultVPC(t, call)

	// A default VPC is deleted the way any VPC is: its dependents first (#1492).
	ec2TearDownVPC(t, call, seeded.vpcID)
	assert.Empty(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "is-default", "true")),
		"a deleted default VPC is not seeded again by the next request")

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "VPCIdNotSpecified", code)

	status, code, _ = ec2ErrorDetail(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "g", "GroupDescription": "g",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "VPCIdNotSpecified", code, "a group naming no VPC has no default VPC to land in")

	var created struct {
		VpcID     string `xml:"vpc>vpcId"`
		IsDefault bool   `xml:"vpc>isDefault"`
		CIDR      string `xml:"vpc>cidrBlock"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "CreateDefaultVpc"}), &created))
	assert.True(t, created.IsDefault)
	assert.Equal(t, "172.31.0.0/16", created.CIDR)
	assert.NotEqual(t, seeded.vpcID, created.VpcID, "a recreated default VPC has a new ID")

	again := ec2ReadDefaultVPC(t, call)
	assert.Equal(t, created.VpcID, again.vpcID)
	assert.Len(t, again.subnets, 3)
	assert.Equal(t, again.igwID, again.routes["0.0.0.0/0"])

	status, code, _ = ec2ErrorDetail(t, ts, map[string]string{"Action": "CreateDefaultVpc"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "DefaultVpcAlreadyExists", code)
}

// TestEC2_CreateDefaultSubnet covers the published refusals and the one success path: a deleted
// default subnet can be recreated in its zone, taking a free /20.
func TestEC2_CreateDefaultSubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	view := ec2ReadDefaultVPC(t, call)

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"a zone that already has one", map[string]string{"AvailabilityZone": "us-east-1b"},
			"DefaultSubnetAlreadyExistsInAvailabilityZone"},
		{"neither zone parameter", map[string]string{}, "MissingParameter"},
		{"both zone parameters", map[string]string{"AvailabilityZone": "us-east-1b", "AvailabilityZoneId": "use1-az2"},
			"InvalidParameterCombination"},
		{"a zone the Region does not have", map[string]string{"AvailabilityZone": "us-east-1z"},
			"InvalidAvailabilityZone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"Action": "CreateDefaultSubnet"}
			for k, v := range tc.params {
				params[k] = v
			}
			status, code, _ := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}

	call(map[string]string{"Action": "DeleteSubnet", "SubnetId": view.subnets[1].SubnetID})
	var created struct {
		Subnet describedSubnet `xml:"subnet"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{
		"Action": "CreateDefaultSubnet", "AvailabilityZoneId": "use1-az2",
	}), &created))
	assert.Equal(t, "us-east-1b", created.Subnet.AvailabilityZone, "the zone ID resolves to its zone")
	assert.Equal(t, "172.31.16.0/20", created.Subnet.CIDRBlock, "the freed /20 is reused")
	assert.True(t, created.Subnet.DefaultForAz)
	assert.True(t, created.Subnet.MapPublicIPOnLaunch)
	assert.Equal(t, view.vpcID, created.Subnet.VpcID)

	ec2TearDownVPC(t, call, view.vpcID)
	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "CreateDefaultSubnet", "AvailabilityZone": "us-east-1a",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "DefaultVpcDoesNotExist", code)
}

// TestEC2_DefaultVPC_WithoutDefaultVPCOption pins the server option: a server started without a
// default VPC holds none, refuses a subnet-less launch, and still creates one on request.
func TestEC2_DefaultVPC_WithoutDefaultVPCOption(t *testing.T) {
	t.Parallel()
	srv := emulator.StartTestServer(t, emulator.WithoutDefaultVPC())
	post := func(params map[string]string) *http.Response {
		form := url.Values{}
		for k, v := range params {
			form.Set(k, v)
		}
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/", strings.NewReader(form.Encode()))
		require.NoError(t, err)
		req.Host = "ec2.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}
	call := ec2CallHTTP(t, post)

	var vpcs struct {
		Items []ec2TestVPC `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "DescribeVpcs"}), &vpcs))
	assert.Empty(t, vpcs.Items)

	resp := post(map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1"})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "<Code>VPCIdNotSpecified</Code>")

	call(map[string]string{"Action": "CreateDefaultVpc"})
	view := ec2ReadDefaultVPC(t, call)
	assert.Len(t, view.subnets, 3)
}

// TestEC2_DefaultVPC_ConfigSwitch pins the substrate.yaml key and its environment override.
func TestEC2_DefaultVPC_ConfigSwitch(t *testing.T) {
	assert.True(t, emulator.DefaultConfig().EC2.DefaultVPC, "seeded by default")

	cfg, err := emulator.LoadConfig("")
	require.NoError(t, err)
	assert.True(t, cfg.EC2.DefaultVPC)

	t.Setenv("SUBSTRATE_EC2_DEFAULT_VPC", "false")
	cfg, err = emulator.LoadConfig("")
	require.NoError(t, err)
	assert.False(t, cfg.EC2.DefaultVPC)
}
