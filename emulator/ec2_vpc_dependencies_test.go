package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2CreateSubnetIn creates a subnet in vpcID and returns its ID.
func ec2CreateSubnetIn(t *testing.T, ts *httptest.Server, vpcID, cidr string) string {
	t.Helper()
	var doc struct {
		ID string `xml:"subnet>subnetId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": cidr}, &doc)
	require.NotEmpty(t, doc.ID)
	return doc.ID
}

// ec2OK sends params and requires a 200.
func ec2OK(t *testing.T, ts *httptest.Server, params map[string]string) {
	t.Helper()
	ec2FleetXML(t, ts, params, nil)
}

// ec2RequireRefusal sends params and requires a 400 carrying code.
func ec2RequireRefusal(t *testing.T, ts *httptest.Server, params map[string]string, code string) {
	t.Helper()
	status, got, msg := ec2ErrorDetail(t, ts, params)
	require.Equal(t, http.StatusBadRequest, status, "%v: %s %s", params, got, msg)
	require.Equal(t, code, got, "%v: %s", params, msg)
}

// ec2TearDownVPC deletes a VPC the way AWS requires (#1492): its subnets, its non-default
// security groups and non-main route tables, and its internet gateways — detached first — and
// then the VPC itself.
func ec2TearDownVPC(t *testing.T, call func(map[string]string) []byte, vpcID string) {
	t.Helper()
	var subnets struct {
		IDs []string `xml:"subnetSet>item>subnetId"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeSubnets", "vpc-id", vpcID)), &subnets))
	for _, id := range subnets.IDs {
		call(map[string]string{"Action": "DeleteSubnet", "SubnetId": id})
	}
	var igws struct {
		IDs []string `xml:"internetGatewaySet>item>internetGatewayId"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeInternetGateways", "attachment.vpc-id", vpcID)), &igws))
	for _, id := range igws.IDs {
		call(map[string]string{"Action": "DetachInternetGateway", "InternetGatewayId": id, "VpcId": vpcID})
		call(map[string]string{"Action": "DeleteInternetGateway", "InternetGatewayId": id})
	}
	var groups struct {
		Items []struct {
			ID   string `xml:"groupId"`
			Name string `xml:"groupName"`
		} `xml:"securityGroupInfo>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeSecurityGroups", "vpc-id", vpcID)), &groups))
	for _, g := range groups.Items {
		if g.Name != "default" {
			call(map[string]string{"Action": "DeleteSecurityGroup", "GroupId": g.ID})
		}
	}
	var rtbs struct {
		Items []struct {
			ID    string `xml:"routeTableId"`
			Mains []bool `xml:"associationSet>item>main"`
		} `xml:"routeTableSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeRouteTables", "vpc-id", vpcID)), &rtbs))
	for _, r := range rtbs.Items {
		main := false
		for _, m := range r.Mains {
			main = main || m
		}
		if !main {
			call(map[string]string{"Action": "DeleteRouteTable", "RouteTableId": r.ID})
		}
	}
	call(map[string]string{"Action": "DeleteVpc", "VpcId": vpcID})
}

// TestEC2_InternetGateway_AttachDetachContract pins #1194: both operations require VpcId, a
// gateway attaches to one VPC at a time, and a detach naming the wrong VPC is refused rather than
// answered return=true.
func TestEC2_InternetGateway_AttachDetachContract(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcA := ec2CreateVPC(t, ts, "10.10.0.0/16")
	vpcB := ec2CreateVPC(t, ts, "10.11.0.0/16")
	igw := ec2CreateIGW(t, ts)

	attach := func(vpc string) map[string]string {
		p := map[string]string{"Action": "AttachInternetGateway", "InternetGatewayId": igw}
		if vpc != "" {
			p["VpcId"] = vpc
		}
		return p
	}
	detach := func(vpc string) map[string]string {
		p := map[string]string{"Action": "DetachInternetGateway", "InternetGatewayId": igw}
		if vpc != "" {
			p["VpcId"] = vpc
		}
		return p
	}

	ec2RequireRefusal(t, ts, attach(""), "MissingParameter")
	ec2RequireRefusal(t, ts, attach("vpc-0000000000000dead"), "InvalidVpcID.NotFound")
	ec2RequireRefusal(t, ts, detach(vpcA), "Gateway.NotAttached")

	// A well-formed attach answers exactly what it always has, apart from the requestId every EC2
	// success document leads with since #1189.
	resp := ec2Request(t, ts, attach(vpcA))
	body := readBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, ec2StripRequestID(body), `<AttachInternetGatewayResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><return>true</return></AttachInternetGatewayResponse>`)

	ec2RequireRefusal(t, ts, attach(vpcA), "Resource.AlreadyAssociated")
	ec2RequireRefusal(t, ts, attach(vpcB), "Resource.AlreadyAssociated")
	ec2RequireRefusal(t, ts, detach(""), "MissingParameter")
	ec2RequireRefusal(t, ts, detach(vpcB), "Gateway.NotAttached")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteInternetGateway", "InternetGatewayId": igw},
		"DependencyViolation")

	var described struct {
		VPCs []string `xml:"internetGatewaySet>item>attachmentSet>item>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInternetGateways", "InternetGatewayId.1": igw}, &described)
	assert.Equal(t, []string{vpcA}, described.VPCs, "the refused requests changed nothing")

	resp = ec2Request(t, ts, detach(vpcA))
	body = readBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, ec2StripRequestID(body), `<DetachInternetGatewayResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><return>true</return></DetachInternetGatewayResponse>`)
	ec2RequireRefusal(t, ts, detach(vpcA), "Gateway.NotAttached")
	ec2OK(t, ts, map[string]string{"Action": "DeleteInternetGateway", "InternetGatewayId": igw})
}

// TestEC2_CreateVpc_MintsTheDefaultSecurityGroup pins #1492's other half: every VPC is created
// with a group named default, carrying the self-referencing ingress rule and the all-traffic
// egress rule the VPC User Guide lists.
func TestEC2_CreateVpc_MintsTheDefaultSecurityGroup(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.20.0.0/16")

	var doc struct {
		Groups []struct {
			ID          string   `xml:"groupId"`
			Name        string   `xml:"groupName"`
			Description string   `xml:"groupDescription"`
			IngressFrom []string `xml:"ipPermissions>item>groups>item>groupId"`
			IngressProt []string `xml:"ipPermissions>item>ipProtocol"`
			EgressCIDR  []string `xml:"ipPermissionsEgress>item>ipRanges>item>cidrIp"`
		} `xml:"securityGroupInfo>item"`
	}
	ec2FleetXML(t, ts, ec2OneFilter("DescribeSecurityGroups", "vpc-id", vpcID), &doc)
	require.Len(t, doc.Groups, 1)
	g := doc.Groups[0]
	assert.Equal(t, "default", g.Name)
	assert.Equal(t, "default VPC security group", g.Description)
	assert.Equal(t, []string{g.ID}, g.IngressFrom, "the ingress source is the group itself")
	assert.Equal(t, []string{"-1"}, g.IngressProt)
	assert.Equal(t, []string{"0.0.0.0/0"}, g.EgressCIDR)

	// Its name is reserved in that VPC like in every other.
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "default", "GroupDescription": "d", "VpcId": vpcID,
	}, "InvalidParameterValue")
}

// TestEC2_DeleteVpc_RefusesWhileDependentsRemain is #1492's table: each dependent AWS's
// DeleteVpc page names, that substrate stores, refuses the delete with DependencyViolation and
// leaves the VPC listed; once the dependent is gone the delete succeeds and takes the default
// security group and main route table with it.
func TestEC2_DeleteVpc_RefusesWhileDependentsRemain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// add creates the dependent and returns the request that removes it.
		add func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string
	}{
		{"a subnet", func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string {
			id := ec2CreateSubnetIn(t, ts, vpcID, "10.30.1.0/24")
			return []map[string]string{{"Action": "DeleteSubnet", "SubnetId": id}}
		}},
		{"an attached internet gateway", func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string {
			igw := ec2CreateIGW(t, ts)
			ec2OK(t, ts, map[string]string{"Action": "AttachInternetGateway", "InternetGatewayId": igw, "VpcId": vpcID})
			return []map[string]string{{"Action": "DetachInternetGateway", "InternetGatewayId": igw, "VpcId": vpcID}}
		}},
		{"a non-default security group", func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string {
			id := ec2CreateSG(t, ts, "app", "app", vpcID)
			return []map[string]string{{"Action": "DeleteSecurityGroup", "GroupId": id}}
		}},
		{"a non-main route table", func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string {
			var doc struct {
				ID string `xml:"routeTable>routeTableId"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID}, &doc)
			return []map[string]string{{"Action": "DeleteRouteTable", "RouteTableId": doc.ID}}
		}},
		{"a NAT gateway not yet deleted", func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string {
			subnet := ec2CreateSubnetIn(t, ts, vpcID, "10.30.2.0/24")
			alloc, _ := ec2AllocateAddress(t, ts)
			var nat struct {
				ID string `xml:"natGateway>natGatewayId"`
			}
			ec2FleetXML(t, ts, map[string]string{
				"Action": "CreateNatGateway", "SubnetId": subnet, "AllocationId": alloc,
			}, &nat)
			// The gateway holds its subnet too (#1551), so the subnet can only go after it.
			return []map[string]string{
				{"Action": "DeleteNatGateway", "NatGatewayId": nat.ID},
				{"Action": "DeleteSubnet", "SubnetId": subnet},
			}
		}},
		{"an instance not yet terminated", func(t *testing.T, ts *httptest.Server, vpcID string) []map[string]string {
			subnet := ec2CreateSubnetIn(t, ts, vpcID, "10.30.3.0/24")
			ids := ec2RunInstanceIDs(t, ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnet,
			})
			require.Len(t, ids, 1)
			// The instance holds its subnet too (#1551), so the subnet can only go after it.
			return []map[string]string{
				{"Action": "TerminateInstances", "InstanceId.1": ids[0]},
				{"Action": "DeleteSubnet", "SubnetId": subnet},
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			vpcID := ec2CreateVPC(t, ts, "10.30.0.0/16")
			removes := tc.add(t, ts, vpcID)

			ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID}, "DependencyViolation")
			require.Len(t, ec2DescribeVPCs(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID}), 1,
				"a refused delete writes nothing")
			assert.Contains(t, ec2SecurityGroupNames(t, ts, ec2Filter("vpc-id", vpcID)), "default")

			for _, r := range removes {
				ec2OK(t, ts, r)
			}
			ec2OK(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID})
			assert.Empty(t, ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "vpc-id", vpcID)))
			assert.Empty(t, ec2SecurityGroupNames(t, ts, ec2Filter("vpc-id", vpcID)),
				"the default security group goes with the VPC")
			var rtbs struct {
				IDs []string `xml:"routeTableSet>item>routeTableId"`
			}
			ec2FleetXML(t, ts, ec2OneFilter("DescribeRouteTables", "vpc-id", vpcID), &rtbs)
			assert.Empty(t, rtbs.IDs, "the main route table goes with the VPC")
		})
	}
}
