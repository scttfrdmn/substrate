package emulator_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2InstanceIDsFiltered returns the instance IDs DescribeInstances answers for one filter.
func ec2InstanceIDsFiltered(t *testing.T, ts *httptest.Server, name, value string) []string {
	t.Helper()
	var doc struct {
		IDs []string `xml:"reservationSet>item>instancesSet>item>instanceId"`
	}
	ec2FleetXML(t, ts, ec2OneFilter("DescribeInstances", name, value), &doc)
	return doc.IDs
}

// TestEC2_DescribeInstances_ArchitectureAndPlacementGroupFilters pins #750's rule for the two
// members #1525 and #1193 made readable: once `architecture` and `placement.groupName` are
// rendered, the filters over them narrow rather than being inert. DescribeInstances publishes
// `architecture` and `placement-group-name`; it publishes no placement-group-id filter.
func TestEC2_DescribeInstances_ArchitectureAndPlacementGroupFilters(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2CreatePlacementGroup(t, ts, "workers")
	arm := ec2RunOne(t, ts, map[string]string{
		"ImageId": ec2TestImageArm, "InstanceType": "c7g.large", "Placement.GroupName": "workers",
	})
	x86 := ec2RunOne(t, ts, map[string]string{"InstanceType": "c7i.xlarge"})

	assert.Equal(t, []string{arm.ID}, ec2InstanceIDsFiltered(t, ts, "architecture", "arm64"))
	assert.Equal(t, []string{x86.ID}, ec2InstanceIDsFiltered(t, ts, "architecture", "x86_64"))
	assert.Empty(t, ec2InstanceIDsFiltered(t, ts, "architecture", "i386"))
	assert.Equal(t, []string{arm.ID}, ec2InstanceIDsFiltered(t, ts, "placement-group-name", "workers"))
	assert.Empty(t, ec2InstanceIDsFiltered(t, ts, "placement-group-name", "others"),
		"an instance in no placement group matches no name")
}

// TestEC2_DeleteSecurityGroup_AReferenceFromAnotherVPCDoesNotHoldIt pins the scope
// API_DeleteSecurityGroup gives a reference — "is referenced by another security group in the
// same VPC". A peered VPC's group naming it in a rule does not stop the delete.
func TestEC2_DeleteSecurityGroup_AReferenceFromAnotherVPCDoesNotHoldIt(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	target := ec2CreateSG(t, ts, "target", "target", ec2CreateVPC(t, ts, "10.86.0.0/16"))
	peer := ec2CreateSG(t, ts, "peer", "peer", ec2CreateVPC(t, ts, "10.87.0.0/16"))
	ec2OK(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": peer, "IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.Groups.1.GroupId": target,
	})
	ec2OK(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": target})
}

// TestEC2_SecurityGroupRules_IPv4MappedIPv6IsRefused pins that a CIDR's family is decided by its
// text. `::ffff:0:0/96` would unmap to 0.0.0.0/0 if accepted as CidrIp, opening the rule to every
// IPv4 address, so it is malformed there — and malformed as CidrIpv6 too, where it would name IPv4
// space through the IPv6 parameter.
func TestEC2_SecurityGroupRules_IPv4MappedIPv6IsRefused(t *testing.T) {
	t.Parallel()
	for _, param := range []string{"IpPermissions.1.IpRanges.1.CidrIp", "IpPermissions.1.Ipv6Ranges.1.CidrIpv6"} {
		for _, cidr := range []string{"::ffff:0:0/96", "::ffff:10.0.0.0/104"} {
			t.Run(param+"="+cidr, func(t *testing.T) {
				t.Parallel()
				ts := newEC2TestServer(t)
				sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.88.0.0/16"))
				ec2RequireRefusalMessage(t, ts, map[string]string{
					"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg, "IpPermissions.1.IpProtocol": "tcp",
					"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22", param: cidr,
				}, "InvalidParameterValue", "CIDR block "+cidr+" is malformed")
				assert.Empty(t, ec2GroupRuleSources(t, ts, sg, false))
			})
		}
	}
}

// TestEC2_SecurityGroupRules_AStoredMalformedRangeCanStillBeRevoked pins that a range an earlier
// substrate stored verbatim, malformed, is revoked by a request naming its exact text — the
// request CloudFormation's sweep rebuilds from the template that authorized it — while the same
// text is still refused on authorize, and an unheld malformed range still refused on revoke.
func TestEC2_SecurityGroupRules_AStoredMalformedRangeCanStillBeRevoked(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	sg := ec2CreateSG(t, ts, "web", "web", ec2CreateVPC(t, ts, "10.89.0.0/16"))
	ec2WriteLegacyIngress(t, state, sg, "10.0.0/8")

	rule := func(action, cidr string) map[string]string {
		return map[string]string{
			"Action": action, "GroupId": sg, "IpPermissions.1.IpProtocol": "tcp",
			"IpPermissions.1.FromPort": "22", "IpPermissions.1.ToPort": "22",
			"IpPermissions.1.IpRanges.1.CidrIp": cidr,
		}
	}
	ec2RequireRefusal(t, ts, rule("AuthorizeSecurityGroupIngress", "10.0.0/8"), "InvalidParameterValue")
	ec2RequireRefusal(t, ts, rule("RevokeSecurityGroupIngress", "10.0.1/8"), "InvalidParameterValue")
	ec2OK(t, ts, rule("RevokeSecurityGroupIngress", "10.0.0/8"))
	assert.Empty(t, ec2GroupRuleSources(t, ts, sg, false))
}

// TestEC2_ImportKeyPair_RSALengthIsOneOfThePublished pins the User Guide's "Supported lengths:
// 1024, 2048, and 4096" for an imported RSA key; another length is InvalidKeyPair.Format.
func TestEC2_ImportKeyPair_RSALengthIsOneOfThePublished(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		bits int
		ok   bool
	}{{1024, true}, {2048, true}, {1536, false}} {
		key, err := rsa.GenerateKey(rand.Reader, tc.bits)
		require.NoError(t, err)
		line := "ssh-rsa " + base64.StdEncoding.EncodeToString(ec2RSABlob(&key.PublicKey))
		ts := newEC2TestServer(t)
		params := map[string]string{"Action": "ImportKeyPair", "KeyName": "k", "PublicKeyMaterial": line}
		if tc.ok {
			ec2OK(t, ts, params)
			continue
		}
		ec2RequireRefusal(t, ts, params, "InvalidKeyPair.Format")
	}
}

// TestEC2_DeleteSubnet_ConcurrentLaunchCannotLandInADeletedSubnet races launches into a subnet
// against its delete (#1551). Each run ends one of two ways — the delete wins and every launch
// that follows is refused, or a launch wins and the delete is refused — and never with an instance
// recorded in a subnet that no longer exists. The assertion is on that invariant, not on timing.
func TestEC2_DeleteSubnet_ConcurrentLaunchCannotLandInADeletedSubnet(t *testing.T) {
	t.Parallel()
	for range 20 {
		ts := newEC2TestServer(t)
		subnet := ec2CreateSubnetIn(t, ts, ec2CreateVPC(t, ts, "10.92.0.0/16"), "10.92.1.0/24")
		var wg sync.WaitGroup
		wg.Add(2)
		var deleted bool
		go func() {
			defer wg.Done()
			resp := ec2Request(t, ts, map[string]string{"Action": "DeleteSubnet", "SubnetId": subnet})
			deleted = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		}()
		go func() {
			defer wg.Done()
			resp := ec2Request(t, ts, map[string]string{
				"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "SubnetId": subnet,
			})
			_ = resp.Body.Close()
		}()
		wg.Wait()
		if deleted {
			assert.Empty(t, ec2InstanceIDsFiltered(t, ts, "subnet-id", subnet),
				"no instance is recorded in a subnet that was deleted")
		}
	}
}

// TestEC2_AttachInternetGateway_ConcurrentDeleteVpcLeavesNoAttachment races an attach against
// the VPC's delete (#1554): either the delete wins and the attach is refused, or the attach wins
// and the delete is refused; a gateway is never left attached to a VPC that is gone.
func TestEC2_AttachInternetGateway_ConcurrentDeleteVpcLeavesNoAttachment(t *testing.T) {
	t.Parallel()
	for range 20 {
		ts := newEC2TestServer(t)
		vpcID := ec2CreateVPC(t, ts, "10.93.0.0/16")
		igw := ec2CreateIGW(t, ts)
		var wg sync.WaitGroup
		wg.Add(2)
		var deleted bool
		go func() {
			defer wg.Done()
			resp := ec2Request(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID})
			deleted = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		}()
		go func() {
			defer wg.Done()
			resp := ec2Request(t, ts, map[string]string{
				"Action": "AttachInternetGateway", "InternetGatewayId": igw, "VpcId": vpcID,
			})
			_ = resp.Body.Close()
		}()
		wg.Wait()
		if deleted {
			assert.Empty(t, ec2GatewaysAttachedTo(t, ts, vpcID), "no gateway is attached to a deleted VPC")
		}
	}
}
