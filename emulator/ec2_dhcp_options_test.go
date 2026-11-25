package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// DHCP option sets (#1614): the Region's default set, the caller's own, and the VPC's
// dhcpOptionsId that names one.

// doptTestSet is a DhcpOptions as a caller decodes it.
type doptTestSet struct {
	ID      string `xml:"dhcpOptionsId"`
	OwnerID string `xml:"ownerId"`
	Configs []struct {
		Key    string   `xml:"key"`
		Values []string `xml:"valueSet>item>value"`
	} `xml:"dhcpConfigurationSet>item"`
	Tags []describedTag `xml:"tagSet>item"`
}

// options returns the set's options as a map.
func (s doptTestSet) options() map[string][]string {
	out := map[string][]string{}
	for _, c := range s.Configs {
		out[c.Key] = c.Values
	}
	return out
}

// doptList runs DescribeDhcpOptions with params.
func doptList(t *testing.T, call func(map[string]string) []byte, params map[string]string) ([]doptTestSet, string) {
	t.Helper()
	p := map[string]string{"Action": "DescribeDhcpOptions"}
	for k, v := range params {
		p[k] = v
	}
	var doc struct {
		XMLName xml.Name      `xml:"DescribeDhcpOptionsResponse"`
		Items   []doptTestSet `xml:"dhcpOptionsSet>item"`
		Next    string        `xml:"nextToken"`
	}
	require.NoError(t, xml.Unmarshal(call(p), &doc))
	return doc.Items, doc.Next
}

// doptVPCSet returns the dhcpOptionsId DescribeVpcs renders for vpcID.
func doptVPCSet(t *testing.T, call func(map[string]string) []byte, vpcID string) string {
	t.Helper()
	var doc struct {
		Items []struct {
			DHCP string `xml:"dhcpOptionsId"`
		} `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID}), &doc))
	require.Len(t, doc.Items, 1)
	return doc.Items[0].DHCP
}

// doptCreate creates a set with a domain name and two name servers.
func doptCreate(t *testing.T, call func(map[string]string) []byte, extra map[string]string) doptTestSet {
	t.Helper()
	p := map[string]string{
		"Action":                      "CreateDhcpOptions",
		"DhcpConfiguration.1.Key":     "domain-name",
		"DhcpConfiguration.1.Value.1": "corp.example",
		"DhcpConfiguration.2.Key":     "domain-name-servers",
		"DhcpConfiguration.2.Value.1": "10.0.0.2",
		"DhcpConfiguration.2.Value.2": "10.0.0.3",
	}
	for k, v := range extra {
		p[k] = v
	}
	var doc struct {
		Set doptTestSet `xml:"dhcpOptions"`
	}
	require.NoError(t, xml.Unmarshal(call(p), &doc))
	require.True(t, strings.HasPrefix(doc.Set.ID, "dopt-"), doc.Set.ID)
	return doc.Set
}

func TestEC2DHCPOptions_DefaultSetAndAssociation(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.71.0.0/16")

	sets, _ := doptList(t, call, nil)
	require.Len(t, sets, 1, "the Region's default set")
	def := sets[0]
	assert.Equal(t, "123456789012", def.OwnerID)
	assert.Equal(t, map[string][]string{"domain-name": {"ec2.internal"}, "domain-name-servers": {"AmazonProvidedDNS"}}, def.options())
	assert.Equal(t, def.ID, doptVPCSet(t, call, vpcID), "a new VPC uses the default set")
	// The lookup the Terraform AWS provider's aws_default_vpc_dhcp_options makes.
	found, _ := doptList(t, call, map[string]string{
		"Filter.1.Name": "key", "Filter.1.Value.1": "domain-name",
		"Filter.2.Name": "value", "Filter.2.Value.1": "ec2.internal",
		"Filter.3.Name": "key", "Filter.3.Value.1": "domain-name-servers",
		"Filter.4.Name": "value", "Filter.4.Value.1": "AmazonProvidedDNS",
	})
	require.Len(t, found, 1)
	assert.Equal(t, def.ID, found[0].ID)

	custom := doptCreate(t, call, map[string]string{
		"TagSpecification.1.ResourceType": "dhcp-options",
		"TagSpecification.1.Tag.1.Key":    "Name", "TagSpecification.1.Tag.1.Value": "corp",
	})
	assert.Equal(t, []string{"10.0.0.2", "10.0.0.3"}, custom.options()["domain-name-servers"])
	require.Len(t, custom.Tags, 1)

	ec2OK(t, ts, map[string]string{"Action": "AssociateDhcpOptions", "DhcpOptionsId": custom.ID, "VpcId": vpcID})
	assert.Equal(t, custom.ID, doptVPCSet(t, call, vpcID))
	var vpcs struct {
		Items []struct {
			ID string `xml:"vpcId"`
		} `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(call(ec2OneFilter("DescribeVpcs", "dhcp-options-id", custom.ID)), &vpcs))
	require.Len(t, vpcs.Items, 1)
	assert.Equal(t, vpcID, vpcs.Items[0].ID)

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{"Action": "DeleteDhcpOptions", "DhcpOptionsId": custom.ID})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "DependencyViolation", code)
	assert.Equal(t, "The dhcpOptions '"+custom.ID+"' has dependencies and cannot be deleted.", msg)

	ec2OK(t, ts, map[string]string{"Action": "AssociateDhcpOptions", "DhcpOptionsId": "default", "VpcId": vpcID})
	assert.Equal(t, "default", doptVPCSet(t, call, vpcID), "default associates no set")
	ec2OK(t, ts, map[string]string{"Action": "DeleteDhcpOptions", "DhcpOptionsId": custom.ID})
	sets, _ = doptList(t, call, nil)
	assert.Len(t, sets, 1)

	// The default set is still in use by the default VPC.
	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteDhcpOptions", "DhcpOptionsId": def.ID}, "DependencyViolation")
}

func TestEC2DHCPOptions_Refusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.72.0.0/16")
	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"no options", map[string]string{"Action": "CreateDhcpOptions"}, "MissingParameter"},
		{"an unknown option", map[string]string{"Action": "CreateDhcpOptions",
			"DhcpConfiguration.1.Key": "router", "DhcpConfiguration.1.Value.1": "10.0.0.1"}, "InvalidParameterValue"},
		{"an option given twice", map[string]string{"Action": "CreateDhcpOptions",
			"DhcpConfiguration.1.Key": "domain-name", "DhcpConfiguration.1.Value.1": "a",
			"DhcpConfiguration.2.Key": "domain-name", "DhcpConfiguration.2.Value.1": "b"}, "InvalidParameterValue"},
		{"an option with no value", map[string]string{"Action": "CreateDhcpOptions", "DhcpConfiguration.1.Key": "domain-name"},
			"InvalidParameterValue"},
		{"five name servers", map[string]string{"Action": "CreateDhcpOptions", "DhcpConfiguration.1.Key": "domain-name-servers",
			"DhcpConfiguration.1.Value.1": "1.1.1.1", "DhcpConfiguration.1.Value.2": "1.1.1.2", "DhcpConfiguration.1.Value.3": "1.1.1.3",
			"DhcpConfiguration.1.Value.4": "1.1.1.4", "DhcpConfiguration.1.Value.5": "1.1.1.5"}, "InvalidParameterValue"},
		{"a NetBIOS node type of 3", map[string]string{"Action": "CreateDhcpOptions",
			"DhcpConfiguration.1.Key": "netbios-node-type", "DhcpConfiguration.1.Value.1": "3"}, "InvalidParameterValue"},
		{"a lease time under 140", map[string]string{"Action": "CreateDhcpOptions",
			"DhcpConfiguration.1.Key": "ipv6-address-preferred-lease-time", "DhcpConfiguration.1.Value.1": "60"}, "InvalidParameterValue"},
		{"a reserved tag", map[string]string{"Action": "CreateDhcpOptions",
			"DhcpConfiguration.1.Key": "domain-name", "DhcpConfiguration.1.Value.1": "a",
			"TagSpecification.1.ResourceType": "dhcp-options", "TagSpecification.1.Tag.1.Key": "aws:x",
			"TagSpecification.1.Tag.1.Value": "y"}, "InvalidParameterValue"},
		{"describe an unknown set", map[string]string{"Action": "DescribeDhcpOptions", "DhcpOptionsId.1": "dopt-00000000"},
			"InvalidDhcpOptionID.NotFound"},
		{"describe a malformed set", map[string]string{"Action": "DescribeDhcpOptions", "DhcpOptionsId.1": "opt-1"},
			"InvalidDhcpOptionsId.Malformed"},
		{"describe IDs with MaxResults", map[string]string{"Action": "DescribeDhcpOptions", "DhcpOptionsId.1": "dopt-00000000",
			"MaxResults": "5"}, "InvalidParameterCombination"},
		{"describe a page of four", map[string]string{"Action": "DescribeDhcpOptions", "MaxResults": "4"}, "InvalidParameterValue"},
		{"describe a bad token", map[string]string{"Action": "DescribeDhcpOptions", "NextToken": "?"}, "InvalidParameterValue"},
		{"describe an undocumented filter", ec2OneFilter("DescribeDhcpOptions", "domain-name", "x"), "InvalidParameterValue"},
		{"delete an unknown set", map[string]string{"Action": "DeleteDhcpOptions", "DhcpOptionsId": "dopt-00000000"},
			"InvalidDhcpOptionsID.NotFound"},
		{"delete with no set", map[string]string{"Action": "DeleteDhcpOptions"}, "MissingParameter"},
		{"associate an unknown set", map[string]string{"Action": "AssociateDhcpOptions", "DhcpOptionsId": "dopt-00000000", "VpcId": vpcID},
			"InvalidDhcpOptionsID.NotFound"},
		{"associate with an unknown VPC", map[string]string{"Action": "AssociateDhcpOptions", "DhcpOptionsId": "default",
			"VpcId": "vpc-0000000000000000"}, "InvalidVpcID.NotFound"},
		{"associate with no set", map[string]string{"Action": "AssociateDhcpOptions", "VpcId": vpcID}, "MissingParameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusal(t, ts, tc.params, tc.code)
		})
	}

	// Every option the page documents is accepted.
	set := doptCreate(t, call, map[string]string{
		"DhcpConfiguration.3.Key": "ntp-servers", "DhcpConfiguration.3.Value.1": "169.254.169.123",
		"DhcpConfiguration.4.Key": "netbios-name-servers", "DhcpConfiguration.4.Value.1": "10.0.0.4",
		"DhcpConfiguration.5.Key": "netbios-node-type", "DhcpConfiguration.5.Value.1": "2",
		"DhcpConfiguration.6.Key": "ipv6-address-preferred-lease-time", "DhcpConfiguration.6.Value.1": "3600",
	})
	assert.Len(t, set.Configs, 6)

	for _, tc := range []struct{ name, value string }{
		{"dhcp-options-id", set.ID}, {"key", "ntp-servers"}, {"value", "169.254.*"}, {"owner-id", "123456789012"},
	} {
		got, _ := doptList(t, call, map[string]string{"Filter.1.Name": tc.name, "Filter.1.Value.1": tc.value,
			"Filter.2.Name": "dhcp-options-id", "Filter.2.Value.1": set.ID})
		assert.Len(t, got, 1, tc.name)
	}
	ec2TagResource(t, ts, set.ID, "Team", "net")
	got, _ := doptList(t, call, ec2OneFilter("DescribeDhcpOptions", "tag:Team", "net"))
	require.Len(t, got, 1)
	got, _ = doptList(t, call, ec2OneFilter("DescribeDhcpOptions", "tag-key", "Team"))
	require.Len(t, got, 1)
	got, _ = doptList(t, call, ec2OneFilter("DescribeDhcpOptions", "key", "no-such-key"))
	assert.Empty(t, got)

	// Pages over the published 5–1000 range.
	for i := 0; i < 5; i++ {
		doptCreate(t, call, nil)
	}
	page, next := doptList(t, call, map[string]string{"MaxResults": "5"})
	assert.Len(t, page, 5)
	require.NotEmpty(t, next)
	rest, _ := doptList(t, call, map[string]string{"MaxResults": "5", "NextToken": next})
	assert.Len(t, rest, 2, "the default set, the six-option set and five more")
}

// TestEC2DHCPOptions_TheDefaultSetCanBeTaggedAndDeleted covers the default set's two writes: a tag
// persists it under its derived ID, and once no VPC uses it a delete leaves later VPCs with none.
func TestEC2DHCPOptions_TheDefaultSetCanBeTaggedAndDeleted(t *testing.T) {
	t.Parallel()
	ts := ec2ServerWithoutDefaultVPC(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2RegionRequest(t, ts, "us-west-2", p) })

	sets, _ := doptList(t, call, nil)
	require.Len(t, sets, 1)
	def := sets[0]
	assert.Equal(t, []string{"us-west-2.compute.internal"}, def.options()["domain-name"])

	call(map[string]string{"Action": "CreateTags", "ResourceId.1": def.ID, "Tag.1.Key": "Name", "Tag.1.Value": "default"})
	sets, _ = doptList(t, call, nil)
	require.Len(t, sets, 1)
	require.Len(t, sets[0].Tags, 1, "the tag landed on the default set")
	assert.Equal(t, def.ID, sets[0].ID)

	call(map[string]string{"Action": "DeleteDhcpOptions", "DhcpOptionsId": def.ID})
	sets, _ = doptList(t, call, nil)
	assert.Empty(t, sets)
	var created struct {
		ID   string `xml:"vpc>vpcId"`
		DHCP string `xml:"vpc>dhcpOptionsId"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "CreateVpc", "CidrBlock": "10.73.0.0/16"}), &created))
	assert.Equal(t, "default", created.DHCP, "with the default set deleted, a new VPC has none")
	// Tagging the deleted default set is a no-op, not a resurrection.
	call(map[string]string{"Action": "CreateTags", "ResourceId.1": def.ID, "Tag.1.Key": "Name", "Tag.1.Value": "again"})
	sets, _ = doptList(t, call, nil)
	assert.Empty(t, sets)
	// The default VPC created after the delete has none either.
	var dv struct {
		DHCP string `xml:"vpc>dhcpOptionsId"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "CreateDefaultVpc"}), &dv))
	assert.Equal(t, "default", dv.DHCP)
}

// ec2ServerWithoutDefaultVPC is an EC2 server whose accounts start with no default VPC, for a
// test that must reach a state no VPC uses the default DHCP set in.
func ec2ServerWithoutDefaultVPC(t *testing.T) *httptest.Server {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	tc := emulator.NewTimeController(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)
	p := &emulator.EC2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc, "default_vpc": false},
	}))
	registry.Register(p)
	srv := emulator.NewServer(*emulator.DefaultConfig(), registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// ec2RegionRequest is [ec2Request] addressed to region's endpoint.
func ec2RegionRequest(t *testing.T, ts *httptest.Server, region string, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "ec2." + region + ".amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}
