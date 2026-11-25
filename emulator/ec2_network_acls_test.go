package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Network ACLs (#1614): every VPC gets a default ACL, every subnet an association to it, and
// the seven operations record and render rules without applying them.

// naclTestEntry is a NetworkAclEntry as a caller decodes it.
type naclTestEntry struct {
	RuleNumber int    `xml:"ruleNumber"`
	Protocol   string `xml:"protocol"`
	RuleAction string `xml:"ruleAction"`
	Egress     bool   `xml:"egress"`
	CIDRBlock  string `xml:"cidrBlock"`
	IPv6CIDR   string `xml:"ipv6CidrBlock"`
	PortRange  *struct {
		From int `xml:"from"`
		To   int `xml:"to"`
	} `xml:"portRange"`
	ICMP *struct {
		Code int `xml:"code"`
		Type int `xml:"type"`
	} `xml:"icmpTypeCode"`
}

// naclTestACL is a NetworkAcl as a caller decodes it.
type naclTestACL struct {
	ID           string          `xml:"networkAclId"`
	VpcID        string          `xml:"vpcId"`
	OwnerID      string          `xml:"ownerId"`
	IsDefault    bool            `xml:"default"`
	Entries      []naclTestEntry `xml:"entrySet>item"`
	Associations []struct {
		ID       string `xml:"networkAclAssociationId"`
		ACLID    string `xml:"networkAclId"`
		SubnetID string `xml:"subnetId"`
	} `xml:"associationSet>item"`
	Tags []describedTag `xml:"tagSet>item"`
}

// naclList runs DescribeNetworkAcls and decodes its answer.
func naclList(t *testing.T, call func(map[string]string) []byte, params map[string]string) ([]naclTestACL, string) {
	t.Helper()
	p := map[string]string{"Action": "DescribeNetworkAcls"}
	for k, v := range params {
		p[k] = v
	}
	var doc struct {
		XMLName xml.Name      `xml:"DescribeNetworkAclsResponse"`
		Items   []naclTestACL `xml:"networkAclSet>item"`
		Next    string        `xml:"nextToken"`
	}
	require.NoError(t, xml.Unmarshal(call(p), &doc))
	return doc.Items, doc.Next
}

// naclVPCDefault returns vpcID's default ACL, found as the Terraform AWS provider finds it: by
// vpc-id and default=true, requiring exactly one.
func naclVPCDefault(t *testing.T, call func(map[string]string) []byte, vpcID string) naclTestACL {
	t.Helper()
	acls, _ := naclList(t, call, map[string]string{
		"Filter.1.Name": "vpc-id", "Filter.1.Value.1": vpcID,
		"Filter.2.Name": "default", "Filter.2.Value.1": "true",
	})
	require.Len(t, acls, 1, "exactly one default ACL for %s", vpcID)
	return acls[0]
}

// naclCreate creates a custom ACL in vpcID and returns it.
func naclCreate(t *testing.T, call func(map[string]string) []byte, vpcID string, extra map[string]string) naclTestACL {
	t.Helper()
	p := map[string]string{"Action": "CreateNetworkAcl", "VpcId": vpcID}
	for k, v := range extra {
		p[k] = v
	}
	var doc struct {
		ACL naclTestACL `xml:"networkAcl"`
	}
	require.NoError(t, xml.Unmarshal(call(p), &doc))
	require.True(t, strings.HasPrefix(doc.ACL.ID, "acl-"), doc.ACL.ID)
	return doc.ACL
}

// naclEntryParams is a CreateNetworkAclEntry request for an ingress TCP rule.
func naclEntryParams(action, aclID, rule string, extra map[string]string) map[string]string {
	p := map[string]string{
		"Action": action, "NetworkAclId": aclID, "RuleNumber": rule, "Protocol": "6",
		"RuleAction": "allow", "Egress": "false", "CidrBlock": "10.0.0.0/16",
		"PortRange.From": "443", "PortRange.To": "443",
	}
	for k, v := range extra {
		if v == "" {
			delete(p, k)
			continue
		}
		p[k] = v
	}
	return p
}

func TestEC2NetworkACL_EveryVPCHasADefaultACLAndEverySubnetAnAssociation(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.60.0.0/16")
	subnetID := ec2CreateSubnetIn(t, ts, vpcID, "10.60.1.0/24")

	acl := naclVPCDefault(t, call, vpcID)
	assert.True(t, acl.IsDefault)
	assert.Equal(t, "123456789012", acl.OwnerID)
	require.Len(t, acl.Entries, 4)
	// Egress first, each direction by rule number — AWS's DescribeNetworkAcls sample.
	want := []naclTestEntry{
		{RuleNumber: 100, Protocol: "-1", RuleAction: "allow", Egress: true, CIDRBlock: "0.0.0.0/0"},
		{RuleNumber: 32767, Protocol: "-1", RuleAction: "deny", Egress: true, CIDRBlock: "0.0.0.0/0"},
		{RuleNumber: 100, Protocol: "-1", RuleAction: "allow", Egress: false, CIDRBlock: "0.0.0.0/0"},
		{RuleNumber: 32767, Protocol: "-1", RuleAction: "deny", Egress: false, CIDRBlock: "0.0.0.0/0"},
	}
	assert.Equal(t, want, acl.Entries)
	require.Len(t, acl.Associations, 1)
	assert.Equal(t, subnetID, acl.Associations[0].SubnetID)
	assert.Equal(t, acl.ID, acl.Associations[0].ACLID)
	assert.True(t, strings.HasPrefix(acl.Associations[0].ID, "aclassoc-"))

	// The empty sets render as present elements, as AWS's sample does.
	raw := string(call(map[string]string{"Action": "CreateNetworkAcl", "VpcId": vpcID}))
	assert.Contains(t, raw, "<associationSet></associationSet>")
	assert.Contains(t, raw, "<tagSet></tagSet>")

	// The default VPC has one too, associated with each of its default subnets.
	var vpcs struct {
		Items []struct {
			ID string `xml:"vpcId"`
		} `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeVpcs", "is-default", "true")), &vpcs))
	require.Len(t, vpcs.Items, 1)
	def := naclVPCDefault(t, call, vpcs.Items[0].ID)
	assert.Len(t, def.Associations, 3, "one per seeded zone's default subnet")

	// CreateDefaultSubnet associates its subnet: delete one default subnet, then recreate it.
	subnetsOf := func() map[string]string {
		var doc struct {
			Items []struct {
				ID   string `xml:"subnetId"`
				Zone string `xml:"availabilityZone"`
			} `xml:"subnetSet>item"`
		}
		require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeSubnets", "vpc-id", vpcs.Items[0].ID)), &doc))
		out := map[string]string{}
		for _, s := range doc.Items {
			out[s.Zone] = s.ID
		}
		return out
	}
	call(map[string]string{"Action": "DeleteSubnet", "SubnetId": subnetsOf()["us-east-1b"]})
	assert.Len(t, naclVPCDefault(t, call, vpcs.Items[0].ID).Associations, 2, "DeleteSubnet removes the association")
	var created struct {
		ID string `xml:"subnet>subnetId"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "CreateDefaultSubnet", "AvailabilityZone": "us-east-1b"}), &created))
	got, _ := naclList(t, call, ec2OneFilter("DescribeNetworkAcls", "association.subnet-id", created.ID))
	require.Len(t, got, 1)
	assert.Equal(t, def.ID, got[0].ID)
}

func TestEC2NetworkACL_EntryLifecycle(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.61.0.0/16")
	acl := naclCreate(t, call, vpcID, map[string]string{
		"TagSpecification.1.ResourceType": "network-acl",
		"TagSpecification.1.Tag.1.Key":    "Name", "TagSpecification.1.Tag.1.Value": "web",
	})
	assert.False(t, acl.IsDefault)
	require.Len(t, acl.Entries, 2, "a custom ACL holds only the catch-all denies")
	require.Len(t, acl.Tags, 1)
	assert.Equal(t, "web", acl.Tags[0].Value)

	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "100", nil))
	// An identical rule succeeds unchanged; a different one under the same number does not.
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "100", nil))
	ec2RequireRefusal(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "100", map[string]string{"RuleAction": "deny"}),
		"NetworkAclEntryAlreadyExists")
	// The same number in the other direction is a different rule.
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "100", map[string]string{"Egress": "true"}))
	// Names are stored as numbers, the CIDR canonicalised, and ports dropped for -1.
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "110", map[string]string{
		"Protocol": "all", "CidrBlock": "100.68.0.18/18", "PortRange.From": "1", "PortRange.To": "2",
	}))
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "120", map[string]string{
		"Protocol": "icmp", "PortRange.From": "", "PortRange.To": "", "Icmp.Type": "8", "Icmp.Code": "-1",
	}))
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "130", map[string]string{
		"Protocol": "udp", "CidrBlock": "", "Ipv6CidrBlock": "2001:DB8::/32", "PortRange.From": "53", "PortRange.To": "53",
	}))

	got, _ := naclList(t, call, map[string]string{"NetworkAclId.1": acl.ID})
	require.Len(t, got, 1)
	byRule := map[int]naclTestEntry{}
	for _, e := range got[0].Entries {
		if !e.Egress {
			byRule[e.RuleNumber] = e
		}
	}
	require.NotNil(t, byRule[100].PortRange)
	assert.Equal(t, 443, byRule[100].PortRange.From)
	assert.Equal(t, "-1", byRule[110].Protocol)
	assert.Equal(t, "100.68.0.0/18", byRule[110].CIDRBlock)
	assert.Nil(t, byRule[110].PortRange, "a -1 rule stores no ports")
	assert.Equal(t, "1", byRule[120].Protocol)
	require.NotNil(t, byRule[120].ICMP)
	assert.Equal(t, 8, byRule[120].ICMP.Type)
	assert.Nil(t, byRule[120].PortRange)
	assert.Equal(t, "17", byRule[130].Protocol)
	assert.Equal(t, "2001:db8::/32", byRule[130].IPv6CIDR)

	ec2OK(t, ts, naclEntryParams("ReplaceNetworkAclEntry", acl.ID, "100", map[string]string{"RuleAction": "deny"}))
	got, _ = naclList(t, call, map[string]string{"NetworkAclId.1": acl.ID})
	for _, e := range got[0].Entries {
		if e.RuleNumber == 100 && !e.Egress {
			assert.Equal(t, "deny", e.RuleAction)
		}
	}
	ec2OK(t, ts, map[string]string{"Action": "DeleteNetworkAclEntry", "NetworkAclId": acl.ID, "RuleNumber": "100", "Egress": "false"})
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteNetworkAclEntry", "NetworkAclId": acl.ID, "RuleNumber": "100", "Egress": "false"},
		"InvalidNetworkAclEntry.NotFound")
	ec2RequireRefusal(t, ts, naclEntryParams("ReplaceNetworkAclEntry", acl.ID, "999", nil), "InvalidNetworkAclEntry.NotFound")
}

func TestEC2NetworkACL_EntryRefusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.62.0.0/16")
	acl := naclCreate(t, call, vpcID, nil)

	for _, tc := range []struct {
		name  string
		extra map[string]string
		code  string
	}{
		{"rule number zero", map[string]string{"RuleNumber": "0"}, "InvalidParameterValue"},
		{"the reserved catch-all number", map[string]string{"RuleNumber": "32767"}, "InvalidParameterValue"},
		{"no rule number", map[string]string{"RuleNumber": ""}, "MissingParameter"},
		{"no egress", map[string]string{"Egress": ""}, "MissingParameter"},
		{"an egress that is not a boolean", map[string]string{"Egress": "sideways"}, "InvalidParameterValue"},
		{"no protocol", map[string]string{"Protocol": ""}, "MissingParameter"},
		{"an unknown protocol", map[string]string{"Protocol": "sctp"}, "InvalidParameterValue"},
		{"a protocol out of range", map[string]string{"Protocol": "256"}, "InvalidParameterValue"},
		{"no rule action", map[string]string{"RuleAction": ""}, "MissingParameter"},
		{"an unknown rule action", map[string]string{"RuleAction": "maybe"}, "InvalidParameterValue"},
		{"no CIDR", map[string]string{"CidrBlock": ""}, "MissingParameter"},
		{"both CIDRs", map[string]string{"Ipv6CidrBlock": "::/0"}, "InvalidParameterCombination"},
		{"a malformed CIDR", map[string]string{"CidrBlock": "10.0.0.0/33"}, "InvalidParameterValue"},
		{"a malformed IPv6 CIDR", map[string]string{"CidrBlock": "", "Ipv6CidrBlock": "10.0.0.0/8"}, "InvalidParameterValue"},
		{"TCP with no port range", map[string]string{"PortRange.From": "", "PortRange.To": ""}, "MissingParameter"},
		{"half a port range", map[string]string{"PortRange.To": ""}, "MissingParameter"},
		{"half a port range, the other half", map[string]string{"PortRange.From": ""}, "MissingParameter"},
		{"a port out of range", map[string]string{"PortRange.To": "70000"}, "InvalidParameterValue"},
		{"a port that is not a number", map[string]string{"PortRange.From": "http"}, "InvalidParameterValue"},
		{"a reversed port range", map[string]string{"PortRange.From": "500", "PortRange.To": "400"}, "InvalidParameterValue"},
		{"ICMP with no type", map[string]string{"Protocol": "1"}, "MissingParameter"},
		{"ICMPv6 over IPv6 with no type", map[string]string{"Protocol": "58", "CidrBlock": "", "Ipv6CidrBlock": "::/0"}, "MissingParameter"},
		{"an unknown ACL", map[string]string{"NetworkAclId": "acl-0000000000000000"}, "InvalidNetworkAclID.NotFound"},
		{"a malformed ACL", map[string]string{"NetworkAclId": "nacl-1"}, "InvalidNetworkAclId.Malformed"},
		{"no ACL", map[string]string{"NetworkAclId": ""}, "MissingParameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusal(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "200", tc.extra), tc.code)
		})
	}
	// ICMPv6 over IPv4 needs no type: "traffic for all ICMP types and codes allowed".
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "210", map[string]string{"Protocol": "58", "PortRange.From": "", "PortRange.To": ""}))

	// The twenty-first rule in one direction is over the quota; the other direction is not.
	for n := 2; n <= 20; n++ {
		ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, strconv.Itoa(300+n), nil))
	}
	ec2RequireRefusal(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "400", nil), "NetworkAclEntryLimitExceeded")
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "400", map[string]string{"Egress": "true"}))
}

func TestEC2NetworkACL_AssociationsAndDeletes(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.63.0.0/16")
	subnetID := ec2CreateSubnetIn(t, ts, vpcID, "10.63.1.0/24")
	def := naclVPCDefault(t, call, vpcID)
	custom := naclCreate(t, call, vpcID, nil)
	otherVPC := ec2CreateVPC(t, ts, "10.64.0.0/16")
	foreign := naclCreate(t, call, otherVPC, nil)

	oldAssoc := def.Associations[0].ID
	ec2RequireRefusal(t, ts, map[string]string{"Action": "ReplaceNetworkAclAssociation", "AssociationId": oldAssoc, "NetworkAclId": foreign.ID},
		"InvalidParameterValue")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "ReplaceNetworkAclAssociation", "AssociationId": "aclassoc-0000000000000000", "NetworkAclId": custom.ID},
		"InvalidAssociationID.NotFound")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "ReplaceNetworkAclAssociation", "AssociationId": oldAssoc, "NetworkAclId": "acl-0000000000000000"},
		"InvalidNetworkAclID.NotFound")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "ReplaceNetworkAclAssociation", "NetworkAclId": custom.ID}, "MissingParameter")

	var replaced struct {
		ID string `xml:"newAssociationId"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{
		"Action": "ReplaceNetworkAclAssociation", "AssociationId": oldAssoc, "NetworkAclId": custom.ID,
	}), &replaced))
	assert.True(t, strings.HasPrefix(replaced.ID, "aclassoc-"))
	assert.NotEqual(t, oldAssoc, replaced.ID)
	got, _ := naclList(t, call, ec2OneFilter("DescribeNetworkAcls", "association.association-id", replaced.ID))
	require.Len(t, got, 1)
	assert.Equal(t, custom.ID, got[0].ID)
	assert.Equal(t, subnetID, got[0].Associations[0].SubnetID)
	assert.Empty(t, naclVPCDefault(t, call, vpcID).Associations)

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{"Action": "DeleteNetworkAcl", "NetworkAclId": custom.ID})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "DependencyViolation", code)
	assert.Equal(t, "The networkAcl '"+custom.ID+"' has dependencies and cannot be deleted.", msg)
	status, code, msg = ec2ErrorDetail(t, ts, map[string]string{"Action": "DeleteNetworkAcl", "NetworkAclId": def.ID})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, "cannot delete default network ACL "+def.ID, msg)
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteNetworkAcl", "NetworkAclId": "acl-0000000000000000"}, "InvalidNetworkAclID.NotFound")

	// A custom ACL holds the VPC; the subnet does too until it is deleted.
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID}, "DependencyViolation")
	ec2OK(t, ts, map[string]string{"Action": "DeleteSubnet", "SubnetId": subnetID})
	ec2OK(t, ts, map[string]string{"Action": "DeleteNetworkAcl", "NetworkAclId": custom.ID})
	ec2OK(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID})
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeNetworkAcls", "NetworkAclId.1": def.ID}, "InvalidNetworkAclID.NotFound")
}

func TestEC2NetworkACL_DescribeFiltersAndPages(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.65.0.0/16")
	subnetID := ec2CreateSubnetIn(t, ts, vpcID, "10.65.1.0/24")
	acl := naclCreate(t, call, vpcID, map[string]string{
		"TagSpecification.1.ResourceType": "network-acl",
		"TagSpecification.1.Tag.1.Key":    "Tier", "TagSpecification.1.Tag.1.Value": "db",
	})
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "150", map[string]string{"Protocol": "tcp", "CidrBlock": "192.168.0.0/24"}))
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "160", map[string]string{
		"Protocol": "1", "PortRange.From": "", "PortRange.To": "", "Icmp.Type": "3", "Icmp.Code": "4",
	}))
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", acl.ID, "170", map[string]string{"CidrBlock": "", "Ipv6CidrBlock": "2001:db8::/48"}))
	def := naclVPCDefault(t, call, vpcID)

	ids := func(name, value string) []string {
		acls, _ := naclList(t, call, map[string]string{"Filter.1.Name": name, "Filter.1.Value.1": value,
			"Filter.2.Name": "vpc-id", "Filter.2.Value.1": vpcID})
		var out []string
		for _, a := range acls {
			out = append(out, a.ID)
		}
		return out
	}
	for _, tc := range []struct {
		name, value string
		want        []string
	}{
		{"network-acl-id", acl.ID, []string{acl.ID}},
		{"default", "false", []string{acl.ID}},
		{"owner-id", "123456789012", nil},
		{"tag:Tier", "db", []string{acl.ID}},
		{"tag-key", "Tier", []string{acl.ID}},
		{"association.subnet-id", subnetID, []string{def.ID}},
		{"association.network-acl-id", def.ID, []string{def.ID}},
		{"association.association-id", def.Associations[0].ID, []string{def.ID}},
		{"entry.cidr", "192.168.0.0/24", []string{acl.ID}},
		{"entry.ipv6-cidr", "2001:db8::/48", []string{acl.ID}},
		{"entry.protocol", "tcp", []string{acl.ID}},
		{"entry.protocol", "6", []string{acl.ID}},
		{"entry.icmp.type", "3", []string{acl.ID}},
		{"entry.icmp.code", "4", []string{acl.ID}},
		{"entry.port-range.from", "443", []string{acl.ID}},
		{"entry.port-range.to", "443", []string{acl.ID}},
		{"entry.rule-number", "150", []string{acl.ID}},
		{"entry.rule-action", "allow", nil},
		{"entry.egress", "true", nil},
		{"entry.rule-number", "100", []string{def.ID}},
	} {
		got := ids(tc.name, tc.value)
		if tc.want == nil {
			assert.ElementsMatch(t, []string{acl.ID, def.ID}, got, "%s=%s matches both", tc.name, tc.value)
			continue
		}
		assert.ElementsMatch(t, tc.want, got, "%s=%s", tc.name, tc.value)
	}
	assert.Empty(t, ids("entry.icmp.type", "99"))
	ec2RequireRefusal(t, ts, ec2OneFilter("DescribeNetworkAcls", "entry.port", "1"), "InvalidParameterValue")

	// Pages, over the published 5–1000 range.
	for i := 0; i < 5; i++ {
		naclCreate(t, call, vpcID, nil)
	}
	page, next := naclList(t, call, map[string]string{"MaxResults": "5"})
	assert.Len(t, page, 5)
	require.NotEmpty(t, next)
	rest, last := naclList(t, call, map[string]string{"MaxResults": "5", "NextToken": next})
	assert.NotEmpty(t, rest)
	assert.NotEqual(t, page[0].ID, rest[0].ID)
	_ = last
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeNetworkAcls", "MaxResults": "4"}, "InvalidParameterValue")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeNetworkAcls", "MaxResults": "5", "NetworkAclId.1": acl.ID},
		"InvalidParameterCombination")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeNetworkAcls", "NetworkAclId.1": "acl-zz"}, "InvalidNetworkAclId.Malformed")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DescribeNetworkAcls", "NextToken": "x"}, "InvalidParameterValue")
}

func TestEC2NetworkACL_CreateAndTagRefusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	ec2RequireRefusal(t, ts, map[string]string{"Action": "CreateNetworkAcl", "VpcId": "vpc-0000000000000000"}, "InvalidVpcID.NotFound")
	ec2RequireRefusal(t, ts, map[string]string{"Action": "CreateNetworkAcl"}, "MissingParameter")
	vpcID := ec2CreateVPC(t, ts, "10.66.0.0/16")
	ec2RequireRefusal(t, ts, map[string]string{
		"Action": "CreateNetworkAcl", "VpcId": vpcID,
		"TagSpecification.1.ResourceType": "network-acl", "TagSpecification.1.Tag.1.Key": "aws:reserved",
		"TagSpecification.1.Tag.1.Value": "x",
	}, "InvalidParameterValue")

	// CreateTags reaches an ACL, and DescribeTags reports it as a network-acl.
	acl := naclCreate(t, call, vpcID, nil)
	ec2TagResource(t, ts, acl.ID, "Owner", "net")
	var tags struct {
		Items []struct {
			ResourceID   string `xml:"resourceId"`
			ResourceType string `xml:"resourceType"`
		} `xml:"tagSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeTags", "resource-id", acl.ID)), &tags))
	require.Len(t, tags.Items, 1)
	assert.Equal(t, "network-acl", tags.Items[0].ResourceType)
	ec2OK(t, ts, map[string]string{"Action": "DeleteTags", "ResourceId.1": acl.ID, "Tag.1.Key": "Owner"})
	got, _ := naclList(t, call, map[string]string{"NetworkAclId.1": acl.ID})
	assert.Empty(t, got[0].Tags)
}

// TestEC2NetworkACL_DrawnAfterTheVPCsExistingDraws pins the replay rule: CreateVpc draws its
// VPC, main route table, association and default security group IDs first, as it did before
// #1614, and the default ACL's after them, so a recorded CreateVpc replays to the same IDs.
func TestEC2NetworkACL_DrawnAfterTheVPCsExistingDraws(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p := &emulator.EC2Plugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State: state, Logger: emulator.NewDefaultLogger(0, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))},
	}))
	reqCtx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", IDs: emulator.NewIDMint("req-replay-1")}
	_, err := p.HandleRequest(reqCtx, &emulator.AWSRequest{Operation: "CreateVpc", Params: map[string]string{"CidrBlock": "10.70.0.0/16"}})
	require.NoError(t, err)

	mint := emulator.NewIDMint("req-replay-1")
	vpcID := "vpc-" + mint.Hex(8)
	_ = mint.Hex(8) // main route table
	_ = mint.Hex(8) // its association
	_ = mint.Hex(8) // default security group
	aclID := "acl-" + mint.Hex(8)

	resp, err := p.HandleRequest(&emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", IDs: emulator.NewIDMint("r2")},
		&emulator.AWSRequest{Operation: "DescribeNetworkAcls", Params: map[string]string{"Filter.1.Name": "vpc-id", "Filter.1.Value.1": vpcID}})
	require.NoError(t, err)
	var doc struct {
		Items []naclTestACL `xml:"networkAclSet>item"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc))
	require.Len(t, doc.Items, 1)
	assert.Equal(t, aclID, doc.Items[0].ID)
}

// TestEC2NetworkACL_RecordsAnOlderBuildWroteAreSynthesized covers a VPC and subnet with no ACL
// records: their default ACL and association read back under derived IDs, the same on every
// read, and a write persists them under those IDs.
func TestEC2NetworkACL_RecordsAnOlderBuildWroteAreSynthesized(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	ec2OK(t, ts, map[string]string{"Action": "DescribeVpcs"}) // seed the default VPC first

	put := func(key string, v any) {
		data, err := json.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, state.Put(context.Background(), "ec2", key, data))
	}
	const vpcID, subnetID = "vpc-0123456789abcdef", "subnet-0123456789abcdef"
	put("vpc:123456789012/us-east-1/"+vpcID, emulator.EC2VPC{VPCID: vpcID, CIDRBlock: "10.80.0.0/16", State: "available",
		AccountID: "123456789012", Region: "us-east-1"})
	put("subnet:123456789012/us-east-1/"+subnetID, emulator.EC2Subnet{SubnetID: subnetID, VPCID: vpcID, CIDRBlock: "10.80.1.0/24",
		State: "available", AccountID: "123456789012", Region: "us-east-1"})

	first := naclVPCDefault(t, call, vpcID)
	second := naclVPCDefault(t, call, vpcID)
	assert.Equal(t, first.ID, second.ID, "the derived ID is stable across reads")
	require.Len(t, first.Associations, 1)
	assert.Equal(t, first.Associations[0].ID, second.Associations[0].ID)
	assert.Len(t, first.Entries, 4)

	// The VPC renders the Region's default DHCP option set, which DescribeDhcpOptions lists.
	var vpcs struct {
		Items []struct {
			DHCP string `xml:"dhcpOptionsId"`
		} `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID}), &vpcs))
	require.Len(t, vpcs.Items, 1)
	assert.True(t, strings.HasPrefix(vpcs.Items[0].DHCP, "dopt-"))

	// A tag, an entry and a replaced association each persist the synthesized ACL.
	ec2TagResource(t, ts, first.ID, "Name", "legacy")
	raw, err := state.Get(context.Background(), "ec2", "acl:123456789012/us-east-1/"+first.ID)
	require.NoError(t, err)
	require.NotNil(t, raw, "CreateTags persisted the ACL under its derived ID")
	ec2OK(t, ts, naclEntryParams("CreateNetworkAclEntry", first.ID, "200", nil))
	after := naclVPCDefault(t, call, vpcID)
	assert.Equal(t, first.ID, after.ID)
	assert.Len(t, after.Entries, 5)
	require.Len(t, after.Tags, 1)

	custom := naclCreate(t, call, vpcID, nil)
	ec2OK(t, ts, map[string]string{"Action": "ReplaceNetworkAclAssociation", "AssociationId": first.Associations[0].ID, "NetworkAclId": custom.ID})
	moved, _ := naclList(t, call, ec2OneFilter("DescribeNetworkAcls", "association.subnet-id", subnetID))
	require.Len(t, moved, 1)
	assert.Equal(t, custom.ID, moved[0].ID)

	// A second legacy VPC, whose ACL is persisted by being the target of a replace.
	const vpc2, subnet2 = "vpc-0aaaaaaaaaaaaaaa", "subnet-0aaaaaaaaaaaaaaa"
	put("vpc:123456789012/us-east-1/"+vpc2, emulator.EC2VPC{VPCID: vpc2, CIDRBlock: "10.81.0.0/16", State: "available",
		AccountID: "123456789012", Region: "us-east-1"})
	put("subnet:123456789012/us-east-1/"+subnet2, emulator.EC2Subnet{SubnetID: subnet2, VPCID: vpc2, CIDRBlock: "10.81.1.0/24",
		State: "available", AccountID: "123456789012", Region: "us-east-1"})
	legacy2 := naclVPCDefault(t, call, vpc2)
	custom2 := naclCreate(t, call, vpc2, nil)
	var r struct {
		ID string `xml:"newAssociationId"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "ReplaceNetworkAclAssociation",
		"AssociationId": legacy2.Associations[0].ID, "NetworkAclId": custom2.ID}), &r))
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "ReplaceNetworkAclAssociation",
		"AssociationId": r.ID, "NetworkAclId": legacy2.ID}), &r))
	raw, err = state.Get(context.Background(), "ec2", "acl:123456789012/us-east-1/"+legacy2.ID)
	require.NoError(t, err)
	assert.NotNil(t, raw, "a synthesized ACL named as a replace's target is persisted")

	// A subnet created in a legacy VPC is associated with its derived default ACL.
	const vpc3 = "vpc-0bbbbbbbbbbbbbbb"
	put("vpc:123456789012/us-east-1/"+vpc3, emulator.EC2VPC{VPCID: vpc3, CIDRBlock: "10.82.0.0/16", State: "available",
		AccountID: "123456789012", Region: "us-east-1"})
	legacy3 := naclVPCDefault(t, call, vpc3)
	sub3 := ec2CreateSubnetIn(t, ts, vpc3, "10.82.1.0/24")
	got, _ := naclList(t, call, ec2OneFilter("DescribeNetworkAcls", "association.subnet-id", sub3))
	require.Len(t, got, 1)
	assert.Equal(t, legacy3.ID, got[0].ID)
	// A subnet in no VPC at all has no association.
	orphan := ec2CreateSubnetIn(t, ts, "vpc-0ccccccccccccccc", "10.83.1.0/24")
	got, _ = naclList(t, call, ec2OneFilter("DescribeNetworkAcls", "association.subnet-id", orphan))
	assert.Empty(t, got)
}
