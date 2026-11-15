package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2RequireRefusalMessage sends params and requires a 400 carrying code and message.
func ec2RequireRefusalMessage(t *testing.T, ts *httptest.Server, params map[string]string, code, message string) {
	t.Helper()
	status, got, msg := ec2ErrorDetail(t, ts, params)
	require.Equal(t, http.StatusBadRequest, status, "%v: %s %s", params, got, msg)
	require.Equal(t, code, got, "%v: %s", params, msg)
	require.Equal(t, message, msg, "%v", params)
}

// ec2GroupExists reports whether DescribeSecurityGroups still lists sgID.
func ec2GroupExists(t *testing.T, ts *httptest.Server, sgID string) bool {
	t.Helper()
	return len(ec2SecurityGroupNames(t, ts, ec2Filter("group-id", sgID))) == 1
}

// ec2DefaultGroupOf returns the ID of vpcID's default security group.
func ec2DefaultGroupOf(t *testing.T, ts *httptest.Server, vpcID string) string {
	t.Helper()
	var doc struct {
		IDs []string `xml:"securityGroupInfo>item>groupId"`
	}
	params := ec2OneFilter("DescribeSecurityGroups", "vpc-id", vpcID)
	params["Filter.2.Name"] = "group-name"
	params["Filter.2.Value.1"] = "default"
	ec2FleetXML(t, ts, params, &doc)
	require.Len(t, doc.IDs, 1)
	return doc.IDs[0]
}

// TestEC2_DeleteSecurityGroup_DefaultGroupIsRefused is #1551's first criterion: "You can't delete
// a default security group. If you try to delete a default security group, we return the
// following error code: Client.CannotDelete" (VPC User Guide), with the client-error table's
// gloss as the message.
func TestEC2_DeleteSecurityGroup_DefaultGroupIsRefused(t *testing.T) {
	t.Parallel()
	const msg = "You cannot delete the 'default' security group in your VPC, but you can change its rules."

	t.Run("a created VPC's default group, by ID", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		sg := ec2DefaultGroupOf(t, ts, ec2CreateVPC(t, ts, "10.80.0.0/16"))
		ec2RequireRefusalMessage(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": sg}, "CannotDelete", msg)
		assert.True(t, ec2GroupExists(t, ts, sg), "a refused delete writes nothing")
	})

	t.Run("the default VPC's default group, by name", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2RequireRefusalMessage(t, ts,
			map[string]string{"Action": "DeleteSecurityGroup", "GroupName": "default"}, "CannotDelete", msg)
	})

	t.Run("a default group stored before the flag is recognized by its name", func(t *testing.T) {
		t.Parallel()
		state := emulator.NewMemoryStateManager()
		ts := newEC2TestServerWithState(t, state)
		sg := ec2DefaultGroupOf(t, ts, ec2CreateVPC(t, ts, "10.81.0.0/16"))
		ec2RewriteRecord(t, state, "sg:", sg, func(rec map[string]any) {
			require.Equal(t, true, rec["is_default"], "the group is flagged when minted")
			delete(rec, "is_default")
		})
		ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": sg}, "CannotDelete")
	})

	t.Run("its rules can still be changed", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		sg := ec2DefaultGroupOf(t, ts, ec2CreateVPC(t, ts, "10.82.0.0/16"))
		ec2OK(t, ts, map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg, "IpPermissions.1.IpProtocol": "tcp",
			"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22", "IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
		})
	})
}

// TestEC2_DeleteSecurityGroup_InUseIsRefused is #1551's second criterion, per
// API_DeleteSecurityGroup: "If you attempt to delete a security group that is associated with an
// instance or network interface, is referenced by another security group in the same VPC ... the
// operation fails with DependencyViolation." Each row creates the dependent, requires the refusal
// and that the group is still listed, then removes the dependent and requires the delete to pass.
func TestEC2_DeleteSecurityGroup_InUseIsRefused(t *testing.T) {
	t.Parallel()
	type fixture struct {
		ts     *httptest.Server
		vpcID  string
		subnet string
		target string
	}
	refer := func(action string) func(t *testing.T, f fixture) []map[string]string {
		return func(t *testing.T, f fixture) []map[string]string {
			other := ec2CreateSG(t, f.ts, "other", "other", f.vpcID)
			rule := map[string]string{
				"Action": action, "GroupId": other, "IpPermissions.1.IpProtocol": "tcp",
				"IpPermissions.1.FromPort": "5432", "IpPermissions.1.ToPort": "5432",
				"IpPermissions.1.Groups.1.GroupId": f.target,
			}
			ec2OK(t, f.ts, rule)
			revoke := map[string]string{}
			for k, v := range rule {
				revoke[k] = v
			}
			revoke["Action"] = strings.Replace(action, "Authorize", "Revoke", 1)
			return []map[string]string{revoke}
		}
	}
	tests := []struct {
		name string
		add  func(t *testing.T, f fixture) []map[string]string
	}{
		{"another group's ingress rule names it", refer("AuthorizeSecurityGroupIngress")},
		{"another group's egress rule names it", refer("AuthorizeSecurityGroupEgress")},
		{"a running instance carries it", func(t *testing.T, f fixture) []map[string]string {
			ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
				"SubnetId": f.subnet, "SecurityGroupId.1": f.target,
			})
			require.Len(t, ids, 1)
			return []map[string]string{{"Action": "TerminateInstances", "InstanceId.1": ids[0]}}
		}},
		{"a launched network interface carries it", func(t *testing.T, f fixture) []map[string]string {
			ids := ec2RunInstanceIDs(t, f.ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
				"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": f.subnet,
				"NetworkInterface.1.SecurityGroupId.1": f.target,
			})
			require.Len(t, ids, 1)
			return []map[string]string{{"Action": "TerminateInstances", "InstanceId.1": ids[0]}}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			vpcID := ec2CreateVPC(t, ts, "10.83.0.0/16")
			f := fixture{ts: ts, vpcID: vpcID, subnet: ec2CreateSubnetIn(t, ts, vpcID, "10.83.1.0/24")}
			f.target = ec2CreateSG(t, ts, "target", "target", vpcID)
			removes := tc.add(t, f)

			del := map[string]string{"Action": "DeleteSecurityGroup", "GroupId": f.target}
			ec2RequireRefusalMessage(t, ts, del, "DependencyViolation", "resource "+f.target+" has a dependent object")
			require.True(t, ec2GroupExists(t, ts, f.target), "a refused delete writes nothing")

			for _, r := range removes {
				ec2OK(t, ts, r)
			}
			ec2OK(t, ts, del)
			assert.False(t, ec2GroupExists(t, ts, f.target))
		})
	}

	t.Run("a rule naming the group itself does not hold it", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		sg := ec2CreateSG(t, ts, "self", "self", ec2CreateVPC(t, ts, "10.84.0.0/16"))
		ec2OK(t, ts, map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg, "IpPermissions.1.IpProtocol": "-1",
			"IpPermissions.1.Groups.1.GroupId": sg,
		})
		ec2OK(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": sg})
	})
}

// TestEC2_DeleteSubnet_RefusesWhileOccupied is #1551's third criterion: API_DeleteSubnet's "You
// must terminate all running instances in the subnet before you can delete the subnet", and the
// same for a NAT gateway not yet deleted, answered with DependencyViolation and EC2's observed
// message.
func TestEC2_DeleteSubnet_RefusesWhileOccupied(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		add  func(t *testing.T, ts *httptest.Server, subnet string) map[string]string
	}{
		{"a running instance", func(t *testing.T, ts *httptest.Server, subnet string) map[string]string {
			ids := ec2RunInstanceIDs(t, ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnet,
			})
			require.Len(t, ids, 1)
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0]}
		}},
		{"a stopped instance", func(t *testing.T, ts *httptest.Server, subnet string) map[string]string {
			ids := ec2RunInstanceIDs(t, ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnet,
			})
			require.Len(t, ids, 1)
			ec2OK(t, ts, map[string]string{"Action": "StopInstances", "InstanceId.1": ids[0]})
			return map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0]}
		}},
		{"a NAT gateway", func(t *testing.T, ts *httptest.Server, subnet string) map[string]string {
			alloc, _ := ec2AllocateAddress(t, ts)
			var nat struct {
				ID string `xml:"natGateway>natGatewayId"`
			}
			ec2FleetXML(t, ts, map[string]string{
				"Action": "CreateNatGateway", "SubnetId": subnet, "AllocationId": alloc,
			}, &nat)
			return map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": nat.ID}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.85.0.0/16"), "10.85.1.0/24")
			remove := tc.add(t, ts, subnet)

			del := map[string]string{"Action": "DeleteSubnet", "SubnetId": subnet}
			ec2RequireRefusalMessage(t, ts, del, "DependencyViolation",
				"The subnet '"+subnet+"' has dependencies and cannot be deleted.")
			var doc struct {
				IDs []string `xml:"subnetSet>item>subnetId"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSubnets", "SubnetId.1": subnet}, &doc)
			require.Equal(t, []string{subnet}, doc.IDs, "a refused delete writes nothing")

			ec2OK(t, ts, remove)
			ec2OK(t, ts, del)
		})
	}
}
