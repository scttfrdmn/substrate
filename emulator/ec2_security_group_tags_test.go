package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Security groups keep their tags, and DescribeSecurityGroups evaluates every filter it accepts
// (#1440).
//
// Through v0.125.0 CreateSecurityGroup discarded TagSpecification.N, DescribeSecurityGroups
// rendered no tagSet — so a tag CreateTags had stored was unreadable too — and tag:<key>,
// tag-key, description, owner-id and the twenty rule filters were accepted and inert, so a
// `tag:spawn:managed=true` sweep selected every group in the account.

// ec2DescribedSG is a securityGroupInfo item as a caller decodes it.
type ec2DescribedSG struct {
	GroupID string `xml:"groupId"`
	OwnerID string `xml:"ownerId"`
	Tags    []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
	Ingress []struct {
		Protocol string   `xml:"ipProtocol"`
		FromPort int      `xml:"fromPort"`
		ToPort   int      `xml:"toPort"`
		CIDRs    []string `xml:"ipRanges>item>cidrIp"`
		IPv6     []string `xml:"ipv6Ranges>item>cidrIpv6"`
		Prefixes []string `xml:"prefixListIds>item>prefixListId"`
		Groups   []struct {
			GroupID string `xml:"groupId"`
		} `xml:"groups>item"`
	} `xml:"ipPermissions>item"`
}

func ec2DescribeSGs(t *testing.T, ts *httptest.Server, params map[string]string) []ec2DescribedSG {
	t.Helper()
	full := map[string]string{"Action": "DescribeSecurityGroups"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Groups []ec2DescribedSG `xml:"securityGroupInfo>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Groups
}

func ec2SGTags(g ec2DescribedSG) map[string]string {
	out := map[string]string{}
	for _, tag := range g.Tags {
		out[tag.Key] = tag.Value
	}
	return out
}

// TestEC2_SecurityGroupTags_Repro is the issue's four steps.
func TestEC2_SecurityGroupTags_Repro(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")

	var created struct {
		GroupID string `xml:"groupId"`
		Tags    []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"tagSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "tagged", "GroupDescription": "tagged", "VpcId": vpcID,
		"TagSpecification.1.ResourceType": "security-group",
		"TagSpecification.1.Tag.1.Key":    "a", "TagSpecification.1.Tag.1.Value": "b",
	}, &created)
	require.NotEmpty(t, created.GroupID)
	require.Len(t, created.Tags, 1, "CreateSecurityGroup answers the tags it applied")

	groups := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": created.GroupID})
	require.Len(t, groups, 1)
	assert.Equal(t, map[string]string{"a": "b"}, ec2SGTags(groups[0]))

	ec2TagResource(t, ts, created.GroupID, "c", "d")
	groups = ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": created.GroupID})
	assert.Equal(t, map[string]string{"a": "b", "c": "d"}, ec2SGTags(groups[0]))

	assert.Len(t, ec2DescribeSGs(t, ts, ec2Filter("tag:c", "d")), 1)
	assert.Empty(t, ec2DescribeSGs(t, ts, ec2Filter("tag:c", "zz")), "a tag: filter matching nothing selects nothing")
}

// TestEC2_SecurityGroupTags_CreateRefusesWhatEveryCreateRefuses pins the shared tag rules on
// this create path, and that a refused create leaves no group behind.
func TestEC2_SecurityGroupTags_CreateRefusesWhatEveryCreateRefuses(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	before := len(ec2DescribeSGs(t, ts, nil))

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "reserved", "GroupDescription": "reserved",
		"TagSpecification.1.ResourceType": "security-group",
		"TagSpecification.1.Tag.1.Key":    "aws:owner", "TagSpecification.1.Tag.1.Value": "x",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Len(t, ec2DescribeSGs(t, ts, nil), before, "a refused create writes no group")

	// A specification scoped to another type is skipped, not applied and not refused.
	id := ec2CreateAndExtract(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "other-scope", "GroupDescription": "x",
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "k", "TagSpecification.1.Tag.1.Value": "v",
	}, "groupId")
	groups := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": id})
	require.Len(t, groups, 1)
	assert.Empty(t, groups[0].Tags)
}

// TestEC2_SecurityGroupFilters_EveryNameIsEvaluated is the table over the names that were inert.
func TestEC2_SecurityGroupFilters_EveryNameIsEvaluated(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	web := ec2CreateSG(t, ts, "web", "web tier", vpcID)
	db := ec2CreateSG(t, ts, "db", "db tier", vpcID)
	ec2TagResource(t, ts, web, "spawn:managed", "true")
	ec2TagResource(t, ts, db, "spawn:managed", "false")
	ec2TagResource(t, ts, db, "Owner", "team")

	ec2FleetXML(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": web,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.IpRanges.1.CidrIp":     "0.0.0.0/0",
		"IpPermissions.1.Ipv6Ranges.1.CidrIpv6": "::/0",
	}, nil)
	ec2FleetXML(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": db,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "5432", "IpPermissions.1.ToPort": "5432",
		"IpPermissions.1.Groups.1.GroupId":             web,
		"IpPermissions.1.PrefixListIds.1.PrefixListId": "pl-0123456789abcdef0",
	}, nil)
	ec2FleetXML(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupEgress", "GroupId": db,
		"IpPermissions.1.IpProtocol": "udp", "IpPermissions.1.FromPort": "53", "IpPermissions.1.ToPort": "53",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.2/32",
	}, nil)
	owner := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": web})[0].OwnerID
	require.NotEmpty(t, owner, "ownerId is rendered")

	ids := func(params map[string]string) []string {
		var out []string
		for _, g := range ec2DescribeSGs(t, ts, params) {
			if g.GroupID == web || g.GroupID == db {
				out = append(out, g.GroupID)
			}
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		filter map[string]string
		want   []string
	}{
		{"tag: selects by value", ec2Filter("tag:spawn:managed", "true"), []string{web}},
		{"tag: values OR", ec2Filter("tag:spawn:managed", "true", "false"), []string{web, db}},
		{"tag: matching nothing", ec2Filter("tag:spawn:managed", "maybe"), nil},
		{"tag-key", ec2Filter("tag-key", "Owner"), []string{db}},
		{"description", ec2Filter("description", "web tier"), []string{web}},
		{"description wildcard", ec2Filter("description", "* tier"), []string{web, db}},
		{"owner-id", ec2Filter("owner-id", owner), []string{web, db}},
		{"owner-id another account", ec2Filter("owner-id", "999999999999"), nil},
		{"ip-permission.cidr", ec2Filter("ip-permission.cidr", "0.0.0.0/0"), []string{web}},
		{"ip-permission.ipv6-cidr", ec2Filter("ip-permission.ipv6-cidr", "::/0"), []string{web}},
		{"ip-permission.prefix-list-id", ec2Filter("ip-permission.prefix-list-id", "pl-0123456789abcdef0"), []string{db}},
		{"ip-permission.from-port", ec2Filter("ip-permission.from-port", "5432"), []string{db}},
		{"ip-permission.to-port", ec2Filter("ip-permission.to-port", "443"), []string{web}},
		{"ip-permission.protocol", ec2Filter("ip-permission.protocol", "tcp"), []string{web, db}},
		{"ip-permission.group-id", ec2Filter("ip-permission.group-id", web), []string{db}},
		{"ip-permission.user-id", ec2Filter("ip-permission.user-id", owner), []string{db}},
		{"egress.ip-permission.cidr", ec2Filter("egress.ip-permission.cidr", "10.0.0.2/32"), []string{db}},
		{"egress.ip-permission.protocol", ec2Filter("egress.ip-permission.protocol", "udp"), []string{db}},
		{"egress.ip-permission.from-port", ec2Filter("egress.ip-permission.from-port", "53"), []string{db}},
		{"egress.ip-permission.cidr shared by both", ec2Filter("egress.ip-permission.cidr", "0.0.0.0/0"), []string{web, db}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, ids(tc.filter))
		})
	}

	// "If using multiple filters for rules, the results include security groups for which any
	// combination of rules - not necessarily a single rule - match all filters": port 5432 and
	// CIDR 10.0.0.2/32 never share a rule in db, but each matches one of db's rules.
	assert.Equal(t, []string{db}, ids(map[string]string{
		"Filter.1.Name": "ip-permission.from-port", "Filter.1.Value.1": "5432",
		"Filter.2.Name": "egress.ip-permission.cidr", "Filter.2.Value.1": "10.0.0.2/32",
	}))

	groups := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": web})
	require.Len(t, groups[0].Ingress, 1)
	assert.Equal(t, []string{"::/0"}, groups[0].Ingress[0].IPv6, "an IPv6 range is stored and rendered")
}
