package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2ErrorCode sends params and returns the HTTP status plus the error code from
// the "ec2" protocol's error document.
//
// The XPath is the SDK's own — ec2query.ErrorComponents reads
// "Errors>Error>Code", with the plural <Errors> wrapper — so a regression in the
// wire shape empties the code here for the same reason it does in a real SDK
// caller (#591). It is not the Query protocol's Error>Code, which is what this
// helper read while EC2 was misclassified.
func ec2ErrorCode(t *testing.T, ts *httptest.Server, params map[string]string) (int, string) {
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
	// An empty code on a body that is not an error document is the contract callers
	// rely on: a table pinning refusals usually carries an accepted row too, whose
	// success document this cannot and should not decode.
	if xml.Unmarshal(body, &doc) != nil {
		return resp.StatusCode, ""
	}
	return resp.StatusCode, doc.Code
}

// TestEC2_DescribeByExplicitID_NotFound covers #391: naming a resource ID
// explicitly on a Describe* call asserts the ID exists, so a well-formed ID that
// resolves to nothing must fail with the resource's Invalid*.NotFound code
// rather than returning 200 with an empty set. A consumer's error branch is
// otherwise unreachable and its test passes while verifying nothing.
//
// Codes and their inconsistent casing come from the EC2 error reference:
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
func TestEC2_DescribeByExplicitID_NotFound(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name:   "instances",
			params: map[string]string{"Action": "DescribeInstances", "InstanceId.1": "i-0000000000000dead"},
			code:   "InvalidInstanceID.NotFound",
		},
		{
			name:   "instance status",
			params: map[string]string{"Action": "DescribeInstanceStatus", "InstanceId.1": "i-0000000000000dead"},
			code:   "InvalidInstanceID.NotFound",
		},
		{
			name:   "vpcs",
			params: map[string]string{"Action": "DescribeVpcs", "VpcId.1": "vpc-0000000000000dead"},
			code:   "InvalidVpcID.NotFound",
		},
		{
			name:   "subnets",
			params: map[string]string{"Action": "DescribeSubnets", "SubnetId.1": "subnet-0000000000000dead"},
			code:   "InvalidSubnetID.NotFound",
		},
		{
			name:   "security groups",
			params: map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": "sg-0000000000000dead"},
			code:   "InvalidGroup.NotFound",
		},
		{
			name:   "internet gateways",
			params: map[string]string{"Action": "DescribeInternetGateways", "InternetGatewayId.1": "igw-0000000000000dead"},
			code:   "InvalidInternetGatewayID.NotFound",
		},
		{
			name:   "route tables",
			params: map[string]string{"Action": "DescribeRouteTables", "RouteTableId.1": "rtb-0000000000000dead"},
			code:   "InvalidRouteTableID.NotFound",
		},
		{
			name:   "snapshots",
			params: map[string]string{"Action": "DescribeSnapshots", "SnapshotId.1": "snap-0000000000000dead"},
			code:   "InvalidSnapshot.NotFound",
		},
		{
			name:   "addresses",
			params: map[string]string{"Action": "DescribeAddresses", "AllocationId.1": "eipalloc-0000000000000dead"},
			code:   "InvalidAllocationID.NotFound",
		},
		{
			name:   "nat gateways",
			params: map[string]string{"Action": "DescribeNatGateways", "NatGatewayId.1": "nat-0000000000000dead"},
			code:   "InvalidNatGatewayID.NotFound",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, tt.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.code, code)
		})
	}
}

// TestEC2_DescribeByExplicitID_Malformed covers the second half of #391: a
// syntactically invalid ID is rejected before any lookup, with the resource's
// Malformed code. AWS spells several of these differently from the matching
// NotFound code (InvalidGroupId.Malformed vs InvalidGroup.NotFound), and SDK
// callers match the literal string, so each is asserted individually.
func TestEC2_DescribeByExplicitID_Malformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name:   "instance wrong prefix",
			params: map[string]string{"Action": "DescribeInstances", "InstanceId.1": "not-an-id"},
			code:   "InvalidInstanceID.Malformed",
		},
		{
			name:   "vpc wrong prefix",
			params: map[string]string{"Action": "DescribeVpcs", "VpcId.1": "not-an-id"},
			code:   "InvalidVpcID.Malformed",
		},
		{
			name:   "vpc non-hex suffix",
			params: map[string]string{"Action": "DescribeVpcs", "VpcId.1": "vpc-zzzz"},
			code:   "InvalidVpcID.Malformed",
		},
		{
			name:   "vpc empty suffix",
			params: map[string]string{"Action": "DescribeVpcs", "VpcId.1": "vpc-"},
			code:   "InvalidVpcID.Malformed",
		},
		{
			name:   "subnet wrong prefix",
			params: map[string]string{"Action": "DescribeSubnets", "SubnetId.1": "not-an-id"},
			code:   "InvalidSubnetID.Malformed",
		},
		{
			// AWS lowercases the "d" here but not in InvalidGroup.NotFound.
			name:   "security group wrong prefix",
			params: map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": "not-an-id"},
			code:   "InvalidGroupId.Malformed",
		},
		{
			name:   "internet gateway wrong prefix",
			params: map[string]string{"Action": "DescribeInternetGateways", "InternetGatewayId.1": "not-an-id"},
			code:   "InvalidInternetGatewayId.Malformed",
		},
		{
			name:   "route table wrong prefix",
			params: map[string]string{"Action": "DescribeRouteTables", "RouteTableId.1": "not-an-id"},
			code:   "InvalidRouteTableId.Malformed",
		},
		{
			name:   "snapshot wrong prefix",
			params: map[string]string{"Action": "DescribeSnapshots", "SnapshotId.1": "not-an-id"},
			code:   "InvalidSnapshotID.Malformed",
		},
		{
			// EC2 publishes no InvalidAllocationID.Malformed, so a malformed
			// allocation ID surfaces as NotFound.
			name:   "allocation wrong prefix falls back to NotFound",
			params: map[string]string{"Action": "DescribeAddresses", "AllocationId.1": "not-an-id"},
			code:   "InvalidAllocationID.NotFound",
		},
		{
			// NAT gateways are the one family whose Malformed code sits outside the
			// Invalid*ID.Malformed naming entirely: the reference publishes it as
			// NatGatewayMalformed (#713).
			name:   "nat gateway wrong prefix",
			params: map[string]string{"Action": "DescribeNatGateways", "NatGatewayId.1": "not-an-id"},
			code:   "NatGatewayMalformed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, tt.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.code, code)
		})
	}
}

// TestEC2_MalformedBeatsNotFound asserts syntax is checked before existence: a
// request naming both a malformed and an absent ID reports Malformed, matching
// EC2's ordering.
func TestEC2_MalformedBeatsNotFound(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action":   "DescribeVpcs",
		"VpcId.1":  "vpc-0000000000000dead",
		"VpcId.2":  "bogus",
		"MaxItems": "10",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidVpcID.Malformed", code)
}

// TestEC2_DescribeWithNoIDs_ReturnsEmpty guards the "absent vs. filtered"
// distinction #391 turns on: with no explicit ID, an empty account legitimately
// describes to 200 and an empty set. Only an explicit ID is an existence
// assertion.
func TestEC2_DescribeWithNoIDs_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	for _, action := range []string{
		"DescribeInstances", "DescribeInstanceStatus", "DescribeVpcs", "DescribeSubnets",
		"DescribeSecurityGroups", "DescribeInternetGateways", "DescribeRouteTables",
		"DescribeSnapshots", "DescribeAddresses", "DescribeNatGateways",
	} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			resp := ec2Request(t, ts, map[string]string{"Action": action})
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}

// TestEC2_DescribeExistingID_Succeeds asserts the happy path still works: an ID
// that was just created resolves, and mixing it with an absent ID fails the
// whole call — EC2 does not return the partial set.
func TestEC2_DescribeExistingID_Succeeds(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close() //nolint:errcheck
	vpcID := ec2ExtractXML(string(body), "vpcId")
	require.NotEmpty(t, vpcID)

	// The created ID resolves.
	ok := ec2Request(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID})
	defer ok.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, ok.StatusCode)

	// One present + one absent ID fails the whole call, naming the absent one.
	status, code := ec2ErrorCode(t, ts, map[string]string{
		"Action":  "DescribeVpcs",
		"VpcId.1": vpcID,
		"VpcId.2": "vpc-0000000000000dead",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidVpcID.NotFound", code)
}

// TestEC2_DescribeIDWithNonMatchingFilter_ReturnsEmpty asserts an ID excluded by
// a filter rather than by absence still counts as resolved: EC2 answers an
// existing ID plus a non-matching filter with an empty set, not NotFound.
func TestEC2_DescribeIDWithNonMatchingFilter_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close() //nolint:errcheck
	vpcID := ec2ExtractXML(string(body), "vpcId")
	require.NotEmpty(t, vpcID)

	rtb := ec2Request(t, ts, map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID})
	require.Equal(t, http.StatusOK, rtb.StatusCode)
	rtbBody, err := io.ReadAll(rtb.Body)
	require.NoError(t, err)
	rtb.Body.Close() //nolint:errcheck
	rtbID := ec2ExtractXML(string(rtbBody), "routeTableId")
	require.NotEmpty(t, rtbID)

	desc := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeRouteTables",
		"RouteTableId.1":   rtbID,
		"Filter.1.Name":    "vpc-id",
		"Filter.1.Value.1": "vpc-0000000000000000",
	})
	defer desc.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, desc.StatusCode)
	descBody, err := io.ReadAll(desc.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(descBody), rtbID)
}

// TestEC2_MutateMissingID_NotFound covers the mutating counterparts: naming an
// absent ID on a Terminate/Stop/Start or a Delete* is the same existence
// assertion, and silently returning success hides a caller's mistake.
func TestEC2_MutateMissingID_NotFound(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{
			name:   "terminate",
			params: map[string]string{"Action": "TerminateInstances", "InstanceId.1": "i-0000000000000dead"},
			code:   "InvalidInstanceID.NotFound",
		},
		{
			name:   "stop",
			params: map[string]string{"Action": "StopInstances", "InstanceId.1": "i-0000000000000dead"},
			code:   "InvalidInstanceID.NotFound",
		},
		{
			name:   "start",
			params: map[string]string{"Action": "StartInstances", "InstanceId.1": "i-0000000000000dead"},
			code:   "InvalidInstanceID.NotFound",
		},
		{
			name:   "terminate malformed",
			params: map[string]string{"Action": "TerminateInstances", "InstanceId.1": "bogus"},
			code:   "InvalidInstanceID.Malformed",
		},
		{
			name:   "delete vpc",
			params: map[string]string{"Action": "DeleteVpc", "VpcId": "vpc-0000000000000dead"},
			code:   "InvalidVpcID.NotFound",
		},
		{
			name:   "delete subnet",
			params: map[string]string{"Action": "DeleteSubnet", "SubnetId": "subnet-0000000000000dead"},
			code:   "InvalidSubnetID.NotFound",
		},
		{
			name:   "delete security group",
			params: map[string]string{"Action": "DeleteSecurityGroup", "GroupId": "sg-0000000000000dead"},
			code:   "InvalidGroup.NotFound",
		},
		{
			name:   "delete internet gateway",
			params: map[string]string{"Action": "DeleteInternetGateway", "InternetGatewayId": "igw-0000000000000dead"},
			code:   "InvalidInternetGatewayID.NotFound",
		},
		{
			name:   "delete route table",
			params: map[string]string{"Action": "DeleteRouteTable", "RouteTableId": "rtb-0000000000000dead"},
			code:   "InvalidRouteTableID.NotFound",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, tt.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.code, code)
		})
	}
}

// TestEC2_MutateWithoutID_MissingParameter asserts an omitted resource ID is
// reported as MissingParameter rather than being read as an empty-string ID and
// falling through to the Malformed path, which would name a parameter the caller
// never sent.
func TestEC2_MutateWithoutID_MissingParameter(t *testing.T) {
	t.Parallel()
	for _, action := range []string{
		"DeleteSubnet", "DeleteSecurityGroup", "DeleteInternetGateway", "DeleteRouteTable",
	} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code := ec2ErrorCode(t, ts, map[string]string{"Action": action})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "MissingParameter", code)
		})
	}
}

// TestEC2_DuplicateRequestedID_Resolves asserts the same ID named twice resolves
// once rather than leaving a second copy permanently unmatched, which would turn
// a legitimate request into a spurious NotFound.
func TestEC2_DuplicateRequestedID_Resolves(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close() //nolint:errcheck
	vpcID := ec2ExtractXML(string(body), "vpcId")
	require.NotEmpty(t, vpcID)

	desc := ec2Request(t, ts, map[string]string{
		"Action":  "DescribeVpcs",
		"VpcId.1": vpcID,
		"VpcId.2": vpcID,
	})
	defer desc.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, desc.StatusCode)
}

// TestEC2_SubstrateMintedIDsAreWellFormed guards a self-inflicted regression:
// substrate's generators emit 16 hex characters where AWS emits 8 or 17, so a
// Malformed check that pinned the length would reject substrate's own IDs. Every
// ID a create returns must describe back cleanly.
func TestEC2_SubstrateMintedIDsAreWellFormed(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	create := func(params map[string]string, tag string) string {
		t.Helper()
		resp := ec2Request(t, ts, params)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		id := ec2ExtractXML(string(body), tag)
		require.NotEmpty(t, id, "no <%s> in %s response", tag, params["Action"])
		return id
	}

	vpcID := create(map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"}, "vpcId")
	subnetID := create(map[string]string{
		"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": "10.0.1.0/24",
	}, "subnetId")
	sgID := create(map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "g", "GroupDescription": "d", "VpcId": vpcID,
	}, "groupId")
	igwID := create(map[string]string{"Action": "CreateInternetGateway"}, "internetGatewayId")
	rtbID := create(map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID}, "routeTableId")
	instanceID := create(map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1",
	}, "instanceId")
	allocID := create(map[string]string{"Action": "AllocateAddress", "Domain": "vpc"}, "allocationId")
	natID := create(map[string]string{
		"Action": "CreateNatGateway", "SubnetId": subnetID, "AllocationId": allocID,
	}, "natGatewayId")

	for _, tc := range []struct{ action, param, id string }{
		{"DescribeVpcs", "VpcId.1", vpcID},
		{"DescribeSubnets", "SubnetId.1", subnetID},
		{"DescribeSecurityGroups", "GroupId.1", sgID},
		{"DescribeInternetGateways", "InternetGatewayId.1", igwID},
		{"DescribeRouteTables", "RouteTableId.1", rtbID},
		{"DescribeInstances", "InstanceId.1", instanceID},
		{"DescribeInstanceStatus", "InstanceId.1", instanceID},
		{"DescribeAddresses", "AllocationId.1", allocID},
		{"DescribeNatGateways", "NatGatewayId.1", natID},
	} {
		t.Run(tc.action, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{"Action": tc.action, tc.param: tc.id})
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, http.StatusOK, resp.StatusCode, "id %q rejected", tc.id)
		})
	}
}
