package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #673's second half: a launch refused for a bad security group leaves no default VPC
// behind.
//
// runInstancesWithTags validated security-group *membership*, which needs the target
// subnet's VPC — so the check sat below the ensureDefaultVPC branch, and a launch that
// omitted SubnetId committed a VPC, a subnet, a security group, an internet gateway, a
// route table and four index mutations before being refused. Two of those writes
// swallow their own errors, so the state a refused request left behind was not even
// guaranteed to be complete.
//
// The fix splits the check in two: existence needs nothing and runs before the first
// write, membership keeps its place. This is reachable with no IAM configured at all,
// so it stands on its own regardless of the authorization change beside it.
func TestEC2_DefaultVPC_BadSecurityGroupRefusalWritesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

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

	// Nothing the launch would have created exists. DescribeSecurityGroups is included
	// because ensureDefaultVPC's third write is the default group itself.
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeVpcs"})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var vpcs struct {
		Items []struct{} `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &vpcs), string(body))
	assert.Empty(t, vpcs.Items, "a refused launch creates no default VPC")

	resp = ec2Request(t, ts, map[string]string{"Action": "DescribeSubnets"})
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var subnets struct {
		Items []struct{} `xml:"subnetSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &subnets), string(body))
	assert.Empty(t, subnets.Items, "and no default subnet")

	resp = ec2Request(t, ts, map[string]string{"Action": "DescribeSecurityGroups"})
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var groups struct {
		Items []struct{} `xml:"securityGroupInfo>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &groups), string(body))
	assert.Empty(t, groups.Items, "and no default security group")

	resp = ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var instances struct {
		Items []struct{} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &instances), string(body))
	assert.Empty(t, instances.Items, "and no instance")
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
