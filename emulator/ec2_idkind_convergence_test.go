package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #713: eighteen mutating EC2 handlers hand-wrote an Invalid*.NotFound whose ID family
// already had an entry in ec2_resourceid.go's kind table. The code, message and status now
// come from the kind, through ec2RequireNamedResource.
//
// The tables below are the guard on that. Every row names an operation, the parameter it
// reads the ID from, and the exact code and message the kind produces — so a hand-written
// copy reappearing anywhere, or a kind's wording changing without the table changing,
// turns a row red. They pin the wire body rather than the internal call, because the wire
// body is what an SDK consumer's error branch matches on.
//
// Four of the eighteen used a wording of their own that this replaced ("Security group not
// found", "Internet gateway not found", "Route table not found", and the volume family's
// trailing-period form). Those are behavior changes, recorded in the CHANGELOG.

// ec2ErrorBody sends params and returns the status, code and message from the "ec2"
// protocol's error document.
func ec2ErrorBody(t *testing.T, ts *httptest.Server, params map[string]string) (int, string, string) {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var doc struct {
		XMLName xml.Name `xml:"Response"`
		Code    string   `xml:"Errors>Error>Code"`
		Message string   `xml:"Errors>Error>Message"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), string(body))
	return resp.StatusCode, doc.Code, doc.Message
}

// ec2KindCase is one mutating operation reached with a bogus resource ID.
type ec2KindCase struct {
	// name is the subtest name, "<Action> <param>".
	name string
	// params are the request parameters with the ID left as a placeholder: the tests
	// below fill idParam with the ID under test.
	params map[string]string
	// idParam is the parameter the handler reads the resource ID from.
	idParam string
	// notFound and malformed are the kind's two codes.
	notFound, malformed string
	// noun is the kind's noun, as it appears in the NotFound message.
	noun string
}

// ec2KindCases covers every handler #713 converged, one row per (operation, ID family).
//
// ReplaceRouteTableAssociation is deliberately absent: it resolves the AssociationId
// before the target route table, so a request carrying a bogus ID for both is answered by
// InvalidAssociationID.NotFound — a code with no kind entry. Its route-table half is
// covered by TestEC2_IDKind_ReplaceRouteTableAssociation below, which supplies a real
// association first.
func ec2KindCases() []ec2KindCase {
	return []ec2KindCase{
		{
			name:    "AuthorizeSecurityGroupIngress GroupId",
			params:  map[string]string{"Action": "AuthorizeSecurityGroupIngress", "IpProtocol": "tcp", "FromPort": "22", "ToPort": "22", "CidrIp": "0.0.0.0/0"},
			idParam: "GroupId",
			// AWS lowercases the "d" in the Malformed code but not in the NotFound one.
			notFound: "InvalidGroup.NotFound", malformed: "InvalidGroupId.Malformed",
			noun: "security group",
		},
		{
			name:    "RevokeSecurityGroupEgress GroupId",
			params:  map[string]string{"Action": "RevokeSecurityGroupEgress", "IpProtocol": "tcp", "FromPort": "22", "ToPort": "22", "CidrIp": "0.0.0.0/0"},
			idParam: "GroupId",
			// AWS lowercases the "d" in the Malformed code but not in the NotFound one.
			notFound: "InvalidGroup.NotFound", malformed: "InvalidGroupId.Malformed",
			noun: "security group",
		},
		{
			name:     "AttachInternetGateway InternetGatewayId",
			params:   map[string]string{"Action": "AttachInternetGateway", "VpcId": "vpc-0000000000000001"},
			idParam:  "InternetGatewayId",
			notFound: "InvalidInternetGatewayID.NotFound", malformed: "InvalidInternetGatewayId.Malformed",
			noun: "internet gateway",
		},
		{
			name:     "DetachInternetGateway InternetGatewayId",
			params:   map[string]string{"Action": "DetachInternetGateway", "VpcId": "vpc-0000000000000001"},
			idParam:  "InternetGatewayId",
			notFound: "InvalidInternetGatewayID.NotFound", malformed: "InvalidInternetGatewayId.Malformed",
			noun: "internet gateway",
		},
		{
			name:     "AssociateRouteTable RouteTableId",
			params:   map[string]string{"Action": "AssociateRouteTable", "SubnetId": "subnet-0000000000000001"},
			idParam:  "RouteTableId",
			notFound: "InvalidRouteTableID.NotFound", malformed: "InvalidRouteTableId.Malformed",
			noun: "route table",
		},
		{
			name:     "CreateRoute RouteTableId",
			params:   map[string]string{"Action": "CreateRoute", "DestinationCidrBlock": "0.0.0.0/0", "GatewayId": "igw-0000000000000001"},
			idParam:  "RouteTableId",
			notFound: "InvalidRouteTableID.NotFound", malformed: "InvalidRouteTableId.Malformed",
			noun: "route table",
		},
		{
			name:     "ReplaceRoute RouteTableId",
			params:   map[string]string{"Action": "ReplaceRoute", "DestinationCidrBlock": "0.0.0.0/0", "GatewayId": "igw-0000000000000001"},
			idParam:  "RouteTableId",
			notFound: "InvalidRouteTableID.NotFound", malformed: "InvalidRouteTableId.Malformed",
			noun: "route table",
		},
		{
			name:     "DeleteRoute RouteTableId",
			params:   map[string]string{"Action": "DeleteRoute", "DestinationCidrBlock": "0.0.0.0/0"},
			idParam:  "RouteTableId",
			notFound: "InvalidRouteTableID.NotFound", malformed: "InvalidRouteTableId.Malformed",
			noun: "route table",
		},
		{
			name:     "ModifySubnetAttribute SubnetId",
			params:   map[string]string{"Action": "ModifySubnetAttribute", "MapPublicIpOnLaunch.Value": "true"},
			idParam:  "SubnetId",
			notFound: "InvalidSubnetID.NotFound", malformed: "InvalidSubnetID.Malformed",
			noun: "subnet",
		},
		{
			name:     "CreateNatGateway SubnetId",
			params:   map[string]string{"Action": "CreateNatGateway", "AllocationId": "eipalloc-0000000000000001"},
			idParam:  "SubnetId",
			notFound: "InvalidSubnetID.NotFound", malformed: "InvalidSubnetID.Malformed",
			noun: "subnet",
		},
		{
			name:     "ModifyVpcAttribute VpcId",
			params:   map[string]string{"Action": "ModifyVpcAttribute", "EnableDnsSupport.Value": "true"},
			idParam:  "VpcId",
			notFound: "InvalidVpcID.NotFound", malformed: "InvalidVpcID.Malformed",
			noun: "VPC",
		},
		{
			name:    "AssociateAddress AllocationId",
			params:  map[string]string{"Action": "AssociateAddress", "InstanceId": "i-0000000000000001"},
			idParam: "AllocationId",
			// EC2 publishes no InvalidAllocationID.Malformed, so the kind falls back.
			notFound: "InvalidAllocationID.NotFound", malformed: "InvalidAllocationID.NotFound",
			noun: "allocation",
		},
		{
			name:     "ReleaseAddress AllocationId",
			params:   map[string]string{"Action": "ReleaseAddress"},
			idParam:  "AllocationId",
			notFound: "InvalidAllocationID.NotFound", malformed: "InvalidAllocationID.NotFound",
			noun: "allocation",
		},
		{
			name:    "DeleteNatGateway NatGatewayId",
			params:  map[string]string{"Action": "DeleteNatGateway"},
			idParam: "NatGatewayId",
			// The reference spells the malformed code NatGatewayMalformed, outside the
			// Invalid*ID.Malformed family entirely. It also publishes a second absence
			// code, NatGatewayNotFound, which this handler used before #713.
			notFound: "InvalidNatGatewayID.NotFound", malformed: "NatGatewayMalformed",
			noun: "NAT gateway",
		},
		{
			name:     "DeleteVolume VolumeId",
			params:   map[string]string{"Action": "DeleteVolume"},
			idParam:  "VolumeId",
			notFound: "InvalidVolume.NotFound", malformed: "InvalidVolumeID.Malformed",
			noun: "volume",
		},
		{
			name:     "AttachVolume VolumeId",
			params:   map[string]string{"Action": "AttachVolume", "InstanceId": "i-0000000000000001", "Device": "/dev/xvdf"},
			idParam:  "VolumeId",
			notFound: "InvalidVolume.NotFound", malformed: "InvalidVolumeID.Malformed",
			noun: "volume",
		},
		{
			name:     "DetachVolume VolumeId",
			params:   map[string]string{"Action": "DetachVolume"},
			idParam:  "VolumeId",
			notFound: "InvalidVolume.NotFound", malformed: "InvalidVolumeID.Malformed",
			noun: "volume",
		},
	}
}

// ec2KindParams copies a case's parameters with the ID filled in, so the shared table can
// be reused across the three tests without one mutating another's map.
func ec2KindParams(c ec2KindCase, id string) map[string]string {
	params := make(map[string]string, len(c.params)+1)
	for k, v := range c.params {
		params[k] = v
	}
	if id != "" {
		params[c.idParam] = id
	}
	return params
}

// TestEC2_IDKind_MutateAbsentID pins the kind's NotFound code and its exact message on
// every converged operation. The message is the kind's — "The <noun> ID '<id>' does not
// exist", no trailing period — which is what four of these handlers did not say before.
func TestEC2_IDKind_MutateAbsentID(t *testing.T) {
	t.Parallel()
	for _, c := range ec2KindCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			// The prefix is derived from the case's own expectation rather than
			// hardcoded per row: an ID's prefix is the kind's, and reading it off the
			// parameter name would not survive AllocationId/eipalloc-.
			id := ec2KindPrefixFor(t, c.idParam) + "0000000000000dead"
			status, code, msg := ec2ErrorBody(t, ts, ec2KindParams(c, id))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, c.notFound, code)
			assert.Equal(t, "The "+c.noun+" ID '"+id+"' does not exist", msg)
		})
	}
}

// TestEC2_IDKind_MutateMalformedID pins the second half: syntax is checked before
// existence, so a bogus prefix answers the kind's Malformed code — or its NotFound, for
// the two families where AWS publishes no Malformed variant.
//
// Before #713 every one of these answered NotFound, telling a caller who typo'd an ID that
// a well-formed resource was missing.
func TestEC2_IDKind_MutateMalformedID(t *testing.T) {
	t.Parallel()
	for _, c := range ec2KindCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code, msg := ec2ErrorBody(t, ts, ec2KindParams(c, "not-an-id"))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, c.malformed, code)
			if c.malformed == c.notFound {
				assert.Equal(t, "The "+c.noun+" ID 'not-an-id' does not exist", msg)
				return
			}
			assert.Equal(t, `Invalid id: "not-an-id"`, msg)
		})
	}
}

// TestEC2_IDKind_MutateMissingID pins the third: an omitted ID is a MissingParameter
// naming the parameter the caller left out, not a Malformed reporting an invalid value for
// something never sent.
//
// Eleven of these handlers read the ID straight out of req.Params with no presence check
// at all, so an omitted ID reached the state lookup as the empty string and came back as
// the resource being absent.
func TestEC2_IDKind_MutateMissingID(t *testing.T) {
	t.Parallel()
	for _, c := range ec2KindCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code, msg := ec2ErrorBody(t, ts, ec2KindParams(c, ""))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "MissingParameter", code)
			assert.Equal(t, "The request must contain the parameter "+c.idParam, msg)
		})
	}
}

// ec2KindPrefixFor maps a request parameter to the wire prefix of the IDs it carries.
func ec2KindPrefixFor(t *testing.T, idParam string) string {
	t.Helper()
	prefixes := map[string]string{
		"GroupId":           "sg-",
		"InternetGatewayId": "igw-",
		"RouteTableId":      "rtb-",
		"SubnetId":          "subnet-",
		"VpcId":             "vpc-",
		"AllocationId":      "eipalloc-",
		"NatGatewayId":      "nat-",
		"VolumeId":          "vol-",
	}
	prefix, ok := prefixes[idParam]
	require.True(t, ok, "no ID prefix known for parameter %q", idParam)
	return prefix
}

// TestEC2_IDKind_ReplaceRouteTableAssociation covers the one converged site the shared
// table cannot reach: ReplaceRouteTableAssociation resolves the AssociationId first, so
// its route-table check needs a real association in front of it.
func TestEC2_IDKind_ReplaceRouteTableAssociation(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	vpcID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateVpc", "CidrBlock": "10.9.0.0/16",
	}, "vpcId")
	subnetID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": "10.9.1.0/24",
	}, "subnetId")
	rtbID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateRouteTable", "VpcId": vpcID,
	}, "routeTableId")
	assocID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "AssociateRouteTable", "RouteTableId": rtbID, "SubnetId": subnetID,
	}, "associationId")

	absent := "rtb-0000000000000dead"
	status, code, msg := ec2ErrorBody(t, ts, map[string]string{
		"Action": "ReplaceRouteTableAssociation", "AssociationId": assocID, "RouteTableId": absent,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidRouteTableID.NotFound", code)
	assert.Equal(t, "The route table ID '"+absent+"' does not exist", msg)

	status, code, msg = ec2ErrorBody(t, ts, map[string]string{
		"Action": "ReplaceRouteTableAssociation", "AssociationId": assocID, "RouteTableId": "not-an-id",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidRouteTableId.Malformed", code)
	assert.Equal(t, `Invalid id: "not-an-id"`, msg)

	// Both refusals above must leave the association they were asked to move intact.
	// Adding the target check found this: it used to sit after the source association had
	// been removed and committed, so the two calls above destroyed the association and
	// then reported a failure — and this test's own second call reached
	// InvalidAssociationID.NotFound rather than the route-table refusal it was asserting.
	assert.Equal(t, []string{assocID}, ec2RouteTableAssociationIDs(t, ts, rtbID),
		"a refused ReplaceRouteTableAssociation writes nothing")

	// The happy path still moves it, and to a second table rather than duplicating.
	otherID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateRouteTable", "VpcId": vpcID,
	}, "routeTableId")
	moved := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "ReplaceRouteTableAssociation", "AssociationId": assocID, "RouteTableId": otherID,
	}, "newAssociationId")
	assert.Empty(t, ec2RouteTableAssociationIDs(t, ts, rtbID), "the source association is gone")
	assert.Equal(t, []string{moved}, ec2RouteTableAssociationIDs(t, ts, otherID),
		"and exactly one new one exists on the target")

	// Replacing an association with one on its own route table replaces rather than
	// duplicates: the target was read before the detach, so the append has to build on the
	// post-detach list.
	sameTable := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "ReplaceRouteTableAssociation", "AssociationId": moved, "RouteTableId": otherID,
	}, "newAssociationId")
	assert.Equal(t, []string{sameTable}, ec2RouteTableAssociationIDs(t, ts, otherID),
		"a same-table replace leaves one association, not two")
}

// ec2RouteTableAssociationIDs returns the association IDs DescribeRouteTables reports for
// one route table, sorted — state.List documents no ordering.
func ec2RouteTableAssociationIDs(t *testing.T, ts *httptest.Server, rtbID string) []string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeRouteTables", "RouteTableId.1": rtbID})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var doc struct {
		Items []struct {
			Associations []struct {
				AssociationID string `xml:"routeTableAssociationId"`
			} `xml:"associationSet>item"`
		} `xml:"routeTableSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &doc), string(body))
	ids := []string{}
	for _, item := range doc.Items {
		for _, a := range item.Associations {
			ids = append(ids, a.AssociationID)
		}
	}
	sort.Strings(ids)
	return ids
}

// TestEC2_IDKind_ConvergedOperationsStillSucceed is the other half of the guard: routing a
// refusal through the kind must not refuse a request that should work. Every operation the
// table above reaches with a bogus ID is reached here with a real one.
//
// Without this, a kind check placed before the state lookup — or a wellFormed rule
// tightened past what substrate's own 16-hex generators emit — would pass the tables above
// while breaking every caller.
func TestEC2_IDKind_ConvergedOperationsStillSucceed(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	vpcID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateVpc", "CidrBlock": "10.8.0.0/16",
	}, "vpcId")
	subnetID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": "10.8.1.0/24",
	}, "subnetId")
	sgID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "kind-guard",
		"GroupDescription": "converged operations", "VpcId": vpcID,
	}, "groupId")
	igwID := ec2CreateAndExtract(t, ts, map[string]string{"Action": "CreateInternetGateway"}, "internetGatewayId")
	rtbID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateRouteTable", "VpcId": vpcID,
	}, "routeTableId")
	allocID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "AllocateAddress", "Domain": "vpc",
	}, "allocationId")
	instanceID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1", "SubnetId": subnetID,
	}, "instanceId")
	volID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateVolume", "AvailabilityZone": "us-east-1a", "Size": "8",
	}, "volumeId")
	natID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateNatGateway", "SubnetId": subnetID, "AllocationId": allocID,
	}, "natGatewayId")

	// AssociateAddress runs here rather than in the table below because the table is a
	// literal: its parameters are evaluated before the first step runs, so the
	// association ID DisassociateAddress needs would not exist yet.
	eipAssocID := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "AssociateAddress", "AllocationId": allocID, "InstanceId": instanceID,
	}, "associationId")

	// Ordered, because several of these depend on each other: the address must be
	// associated before it can be released, and the volume attached before detached.
	for _, step := range []struct {
		name   string
		params map[string]string
	}{
		{"AuthorizeSecurityGroupIngress", map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": sgID,
			"IpProtocol": "tcp", "FromPort": "22", "ToPort": "22", "CidrIp": "0.0.0.0/0",
		}},
		{"RevokeSecurityGroupEgress", map[string]string{
			"Action": "RevokeSecurityGroupEgress", "GroupId": sgID,
			"IpProtocol": "tcp", "FromPort": "22", "ToPort": "22", "CidrIp": "0.0.0.0/0",
		}},
		{"AttachInternetGateway", map[string]string{
			"Action": "AttachInternetGateway", "InternetGatewayId": igwID, "VpcId": vpcID,
		}},
		{"DetachInternetGateway", map[string]string{
			"Action": "DetachInternetGateway", "InternetGatewayId": igwID, "VpcId": vpcID,
		}},
		{"CreateRoute", map[string]string{
			"Action": "CreateRoute", "RouteTableId": rtbID,
			"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igwID,
		}},
		{"ReplaceRoute", map[string]string{
			"Action": "ReplaceRoute", "RouteTableId": rtbID,
			"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igwID,
		}},
		{"DeleteRoute", map[string]string{
			"Action": "DeleteRoute", "RouteTableId": rtbID, "DestinationCidrBlock": "0.0.0.0/0",
		}},
		{"AssociateRouteTable", map[string]string{
			"Action": "AssociateRouteTable", "RouteTableId": rtbID, "SubnetId": subnetID,
		}},
		{"ModifySubnetAttribute", map[string]string{
			"Action": "ModifySubnetAttribute", "SubnetId": subnetID, "MapPublicIpOnLaunch.Value": "true",
		}},
		{"ModifyVpcAttribute", map[string]string{
			"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDnsSupport.Value": "true",
		}},
		// An associated address cannot be released — AWS answers
		// InvalidIPAddress.InUse — so the disassociate is part of the sequence, not
		// bookkeeping.
		{"DisassociateAddress", map[string]string{
			"Action": "DisassociateAddress", "AssociationId": eipAssocID,
		}},
		{"ReleaseAddress", map[string]string{"Action": "ReleaseAddress", "AllocationId": allocID}},
		{"AttachVolume", map[string]string{
			"Action": "AttachVolume", "VolumeId": volID, "InstanceId": instanceID, "Device": "/dev/xvdf",
		}},
		{"DetachVolume", map[string]string{"Action": "DetachVolume", "VolumeId": volID}},
		{"DeleteVolume", map[string]string{"Action": "DeleteVolume", "VolumeId": volID}},
		{"DeleteNatGateway", map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": natID}},
	} {
		resp := ec2Request(t, ts, step.params)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", step.name, string(body))
	}
}

// ec2CreateAndExtract sends a create and returns the named element of its response,
// failing the test if the create was refused or the element is absent.
func ec2CreateAndExtract(t *testing.T, ts *httptest.Server, params map[string]string, tag string) string {
	t.Helper()
	resp := ec2Request(t, ts, params)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", params["Action"], string(body))
	id := ec2ExtractXML(string(body), tag)
	require.NotEmpty(t, id, "no <%s> in %s response: %s", tag, params["Action"], string(body))
	return id
}
