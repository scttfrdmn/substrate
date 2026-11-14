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

// #673's second half: a launch refused for a bad security group leaves nothing behind.
//
// runInstancesWithTags validated security-group *membership*, which needs the target
// subnet's VPC — so the check sat below the default-VPC branch, and a launch that omitted
// SubnetId committed a VPC, a subnet, a security group, an internet gateway, a route table
// and four index mutations before being refused.
//
// The fix split the check in two: existence needs nothing and runs before the first write,
// membership keeps its place. As of #1464 the default VPC no longer comes from the launch
// at all — it is seeded on the account's first EC2 request — so the assertion is that a
// refused launch changes no listing, with the seeded default VPC counted on both sides.
func TestEC2_DefaultVPC_BadSecurityGroupRefusalWritesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	before := ec2ListingSizes(t, ts)
	require.Equal(t, map[string]int{"vpcs": 1, "subnets": 3, "groups": 1, "instances": 0}, before,
		"a fresh account holds its default VPC, three default subnets and the default group")

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action":            "RunInstances",
		"ImageId":           ec2TestImage,
		"MinCount":          "1",
		"MaxCount":          "1",
		"SecurityGroupId.1": "sg-0000000000000dead",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidGroup.NotFound", code)
	assert.Equal(t, "The security group ID 'sg-0000000000000dead' does not exist", msg,
		"the kind's wording since #713 routed this refusal through ec2SecurityGroupIDKind")

	assert.Equal(t, before, ec2ListingSizes(t, ts), "a refused launch changes no listing")
}

// ec2ListingSizes counts the VPCs, subnets, security groups and instances the account holds,
// for a test asserting that a refused request wrote nothing.
func ec2ListingSizes(t *testing.T, ts *httptest.Server) map[string]int {
	t.Helper()
	sizes := map[string]int{}
	for action, key := range map[string]string{
		"DescribeVpcs": "vpcs", "DescribeSubnets": "subnets",
		"DescribeSecurityGroups": "groups", "DescribeInstances": "instances",
	} {
		resp := ec2Request(t, ts, map[string]string{"Action": action})
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
		var counted struct {
			VPCs      []struct{} `xml:"vpcSet>item"`
			Subnets   []struct{} `xml:"subnetSet>item"`
			Groups    []struct{} `xml:"securityGroupInfo>item"`
			Instances []struct{} `xml:"reservationSet>item>instancesSet>item"`
		}
		require.NoError(t, xml.Unmarshal(body, &counted), string(body))
		sizes[key] = len(counted.VPCs) + len(counted.Subnets) + len(counted.Groups) + len(counted.Instances)
	}
	return sizes
}

// TestEC2_DefaultVPC_MembershipStillRefusedAfterResolution pins the half of the check
// that could not move: a group that exists but lives in another VPC.
//
// Membership needs the target subnet's VPC, so it can only be decided once the subnet
// is known. Splitting existence out must not weaken it — a launch into the default VPC
// naming a group from a VPC of the caller's own is still refused, just later, and by
// then the default VPC legitimately exists because the launch was going to use it.
func TestEC2_DefaultVPC_MembershipStillRefusedAfterResolution(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// A VPC of the caller's own, with a group inside it. This is not the default VPC:
	// CreateVpc does not set isDefault.
	resp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var created struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	require.NoError(t, xml.Unmarshal(body, &created), string(body))
	require.NotEmpty(t, created.VPCID)

	resp = ec2Request(t, ts, map[string]string{
		"Action":      "CreateSecurityGroup",
		"GroupName":   "elsewhere",
		"Description": "a group in a VPC no launch below uses",
		"VpcId":       created.VPCID,
	})
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var group struct {
		GroupID string `xml:"groupId"`
	}
	require.NoError(t, xml.Unmarshal(body, &group), string(body))
	require.NotEmpty(t, group.GroupID)

	_, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action":            "RunInstances",
		"ImageId":           ec2TestImage,
		"MinCount":          "1",
		"MaxCount":          "1",
		"SecurityGroupId.1": group.GroupID,
	})
	assert.Equal(t, "InvalidGroup.NotFound", code)
	assert.Contains(t, msg, group.GroupID)
	assert.Contains(t, msg, "does not belong to VPC",
		"the membership message is the second one, unchanged by the split")
}
