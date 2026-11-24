package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2GroupRuleSources returns every IPv4 source of a group's rules in one direction, one entry per
// stored source, so a duplicate written twice is visible as a repeated entry.
func ec2GroupRuleSources(t *testing.T, ts *httptest.Server, groupID string, egress bool) []string {
	t.Helper()
	var doc struct {
		Ingress []string `xml:"securityGroupInfo>item>ipPermissions>item>ipRanges>item>cidrIp"`
		Egress  []string `xml:"securityGroupInfo>item>ipPermissionsEgress>item>ipRanges>item>cidrIp"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": groupID}, &doc)
	if egress {
		return doc.Egress
	}
	return doc.Ingress
}

// TestEC2_AuthorizeSecurityGroup_DuplicateIsRefused is #1491's first half: an authorize naming a
// source the group already allows for the same protocol and ports is InvalidPermission.Duplicate,
// checked per source, and nothing is written.
func TestEC2_AuthorizeSecurityGroup_DuplicateIsRefused(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.70.0.0/16")
	sg := ec2CreateSG(t, ts, "web", "web", vpcID)
	peer := ec2CreateSG(t, ts, "peer", "peer", vpcID)

	ingress := func(extra map[string]string) map[string]string {
		p := map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg,
			"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}
	ec2OK(t, ts, ingress(map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0"}))
	ec2OK(t, ts, ingress(map[string]string{"IpPermissions.1.Groups.1.GroupId": peer}))

	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{"the same CIDR", ingress(map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0"})},
		{"a new CIDR beside a held one", ingress(map[string]string{
			"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8", "IpPermissions.1.IpRanges.2.CidrIp": "0.0.0.0/0",
		})},
		{"the same source group", ingress(map[string]string{"IpPermissions.1.Groups.1.GroupId": peer})},
		{"one source twice in one request", ingress(map[string]string{
			"IpPermissions.1.IpRanges.1.CidrIp": "192.0.2.0/24", "IpPermissions.1.IpRanges.2.CidrIp": "192.0.2.0/24",
		})},
		{"the legacy top-level form", map[string]string{
			"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg,
			"IpProtocol": "tcp", "FromPort": "443", "ToPort": "443", "CidrIp": "0.0.0.0/0",
		}},
		{"the group's default egress rule", map[string]string{
			"Action": "AuthorizeSecurityGroupEgress", "GroupId": sg,
			"IpPermissions.1.IpProtocol": "-1", "IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
		}},
		{"the default egress rule with -1 ports, which the all-traffic protocol ignores", map[string]string{
			"Action": "AuthorizeSecurityGroupEgress", "GroupId": sg,
			"IpPermissions.1.IpProtocol": "-1", "IpPermissions.1.FromPort": "-1", "IpPermissions.1.ToPort": "-1",
			"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
		}},
		{"the protocol in capitals", ingress(map[string]string{
			"IpPermissions.1.IpProtocol": "TCP", "IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
		})},
		{"the protocol by number", ingress(map[string]string{
			"IpPermissions.1.IpProtocol": "6", "IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusal(t, ts, tc.params, "InvalidPermission.Duplicate")
		})
	}
	assert.Equal(t, []string{"0.0.0.0/0"}, ec2GroupRuleSources(t, ts, sg, false), "no refused authorize wrote a source")
	assert.Equal(t, []string{"0.0.0.0/0"}, ec2GroupRuleSources(t, ts, sg, true))

	// The same source on another port, or in the other direction, is a different rule.
	ec2OK(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "80", "IpPermissions.1.ToPort": "80",
		"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})
	ec2OK(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupEgress", "GroupId": sg,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})

	// A revoke, spelling the protocol by number, matches the same canonical rule, and frees the
	// source for a fresh authorize.
	ec2OK(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupIngress", "GroupId": sg,
		"IpPermissions.1.IpProtocol": "6", "IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})
	ec2OK(t, ts, ingress(map[string]string{"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0"}))

	// The default egress rule, stored with no ports, is revoked by the -1/-1 spelling.
	ec2OK(t, ts, map[string]string{
		"Action": "RevokeSecurityGroupEgress", "GroupId": sg,
		"IpPermissions.1.IpProtocol": "-1", "IpPermissions.1.FromPort": "-1", "IpPermissions.1.ToPort": "-1",
		"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})
	assert.Equal(t, []string{"0.0.0.0/0"}, ec2GroupRuleSources(t, ts, sg, true), "only the tcp/443 egress rule is left")
}

// TestEC2_DeleteVpc_ConcurrentGroupCreateIsNotOrphaned pins the lock DeleteVpc holds across its
// dependency check: a CreateSecurityGroup racing a DeleteVpc either lands before the check, and the
// delete is refused, or after the delete, and the create is refused. A group is never left in a VPC
// that no longer exists.
func TestEC2_DeleteVpc_ConcurrentGroupCreateIsNotOrphaned(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	for i := 0; i < 10; i++ {
		vpcID := ec2CreateVPC(t, ts, "10.75.0.0/16")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = ec2Request(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID}).Body.Close()
		}()
		go func() {
			defer wg.Done()
			_ = ec2Request(t, ts, map[string]string{
				"Action": "CreateSecurityGroup", "GroupName": "racer", "GroupDescription": "d", "VpcId": vpcID,
			}).Body.Close()
		}()
		wg.Wait()
		if len(ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "vpc-id", vpcID))) == 0 {
			assert.Empty(t, ec2SecurityGroupNames(t, ts, ec2Filter("vpc-id", vpcID)), "no group outlives its VPC")
		}
	}
}

// TestEC2_CreateSecurityGroup_NameIsUniquePerVPC is #1491's second half: a name another group in
// the same VPC holds, in any case, is InvalidGroup.Duplicate, while the same name in another VPC
// is a different group.
func TestEC2_CreateSecurityGroup_NameIsUniquePerVPC(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcA := ec2CreateVPC(t, ts, "10.71.0.0/16")
	vpcB := ec2CreateVPC(t, ts, "10.72.0.0/16")
	ec2CreateSG(t, ts, "app", "app", vpcA)

	create := func(name, vpc string) map[string]string {
		p := map[string]string{"Action": "CreateSecurityGroup", "GroupName": name, "GroupDescription": "d"}
		if vpc != "" {
			p["VpcId"] = vpc
		}
		return p
	}
	ec2RequireRefusal(t, ts, create("app", vpcA), "InvalidGroup.Duplicate")
	ec2RequireRefusal(t, ts, create("APP", vpcA), "InvalidGroup.Duplicate")
	ec2CreateSG(t, ts, "app", "app", vpcB)
	ec2CreateSG(t, ts, "app", "in the default VPC", "")
	ec2RequireRefusal(t, ts, create("App", ""), "InvalidGroup.Duplicate")
	ec2RequireRefusal(t, ts, create("orphan", "vpc-0000000000000dead"), "InvalidVpcID.NotFound")
	assert.ElementsMatch(t, []string{"default", "app"}, ec2SecurityGroupNames(t, ts, ec2Filter("vpc-id", vpcA)))
}

// TestEC2_CreateSecurityGroup_ConcurrentSameNameCreatesOne pins the lock across the name check
// and the write: of many concurrent creates of one name in one VPC, exactly one succeeds.
func TestEC2_CreateSecurityGroup_ConcurrentSameNameCreatesOne(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.73.0.0/16")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := ec2Request(t, ts, map[string]string{
				"Action": "CreateSecurityGroup", "GroupName": "race", "GroupDescription": "d", "VpcId": vpcID,
			})
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
	assert.ElementsMatch(t, []string{"default", "race"}, ec2SecurityGroupNames(t, ts, ec2Filter("vpc-id", vpcID)))
}

// TestEC2_DeleteSecurityGroup_ByNameInTheDefaultVPC pins GroupName on DeleteSecurityGroup, which
// AWS publishes for the default VPC only.
func TestEC2_DeleteSecurityGroup_ByNameInTheDefaultVPC(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.74.0.0/16")
	ec2CreateSG(t, ts, "batch", "in the default VPC", "")
	elsewhere := ec2CreateSG(t, ts, "elsewhere", "in another VPC", vpcID)

	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupName": "elsewhere"},
		"InvalidGroup.NotFound")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteSecurityGroup"}, "MissingParameter")
	ec2OK(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupName": "Batch"})
	assert.Empty(t, ec2DescribeSGNames(t, ts, map[string]string{"GroupName.1": "batch"}))
	require.Equal(t, []string{"elsewhere"}, ec2DescribeSGNames(t, ts, map[string]string{"GroupId.1": elsewhere}))
}

// TestEC2_CreateSecurityGroup_RefusesTheReservedName pins EC2's observed refusal of "default",
// in any case.
func TestEC2_CreateSecurityGroup_RefusesTheReservedName(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	for _, name := range []string{"default", "Default"} {
		status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "CreateSecurityGroup", "GroupName": name, "GroupDescription": "x", "VpcId": vpcID,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Equal(t, "Cannot use reserved security group name: "+name, msg)
	}
}
