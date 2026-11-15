package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2_AllocateAddress_TagSpecificationIsFindable is #1191's headline: an address allocated
// with tags carries them on DescribeAddresses, is selected by a tag filter, and is found by the
// Resource Groups Tagging API — and a tag written through that API lands on the address.
func TestEC2_AllocateAddress_TagSpecificationIsFindable(t *testing.T) {
	t.Parallel()
	ts, _ := newTaggingTestServer(t)

	var alloc struct {
		AllocationID string `xml:"allocationId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "AllocateAddress", "Domain": "vpc",
		"TagSpecification.1.ResourceType": "elastic-ip",
		"TagSpecification.1.Tag.1.Key":    "CostCenter",
		"TagSpecification.1.Tag.1.Value":  "networking",
	}, &alloc)
	require.NotEmpty(t, alloc.AllocationID)
	ec2AllocateAddress(t, ts) // an untagged neighbor, which no tag query may report

	var described struct {
		IDs []string `xml:"addressesSet>item>allocationId"`
	}
	ec2FleetXML(t, ts, ec2OneFilter("DescribeAddresses", "tag:CostCenter", "networking"), &described)
	assert.Equal(t, []string{alloc.AllocationID}, described.IDs)

	arn := "arn:aws:ec2:us-east-1:123456789012:elastic-ip/" + alloc.AllocationID
	resp := taggingRequest(t, ts, "GetResources", map[string]any{"ResourceTypeFilters": []string{"ec2:elastic-ip"}})
	var got struct {
		ResourceTagMappingList []struct {
			ResourceARN string
			Tags        []struct{ Key, Value string }
		}
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.NoError(t, resp.Body.Close())
	require.Len(t, got.ResourceTagMappingList, 1, "only the tagged address is reported")
	assert.Equal(t, arn, got.ResourceTagMappingList[0].ResourceARN)
	assert.Equal(t, []struct{ Key, Value string }{{"CostCenter", "networking"}}, got.ResourceTagMappingList[0].Tags)

	resp = taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{arn}, "Tags": map[string]string{"Team": "core"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	described.IDs = nil
	ec2FleetXML(t, ts, ec2OneFilter("DescribeAddresses", "tag:Team", "core"), &described)
	assert.Equal(t, []string{alloc.AllocationID}, described.IDs, "a tagging-API write lands on the address")

	var domain struct {
		Domain []string `xml:"addressesSet>item>domain"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeAddresses", "AllocationId.1": alloc.AllocationID}, &domain)
	assert.Equal(t, []string{"vpc"}, domain.Domain, "the tag write kept the address's own members")
}

// TestEC2_AllocateAddress_Parameters pins the rest of #1191: Domain's published values, the
// border group echoed, and the refusals for recovery and for address pools substrate does not
// model.
func TestEC2_AllocateAddress_Parameters(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	_, held := ec2AllocateAddress(t, ts)

	// The no-parameter form answers as before, the Region as its border group.
	resp := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress"})
	body := readBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, "<domain>vpc</domain><networkBorderGroup>us-east-1</networkBorderGroup></AllocateAddressResponse>")

	var echoed struct {
		Domain      string `xml:"domain"`
		BorderGroup string `xml:"networkBorderGroup"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "AllocateAddress", "Domain": "standard", "NetworkBorderGroup": "us-east-1-bos-1",
	}, &echoed)
	assert.Equal(t, "standard", echoed.Domain)
	assert.Equal(t, "us-east-1-bos-1", echoed.BorderGroup)

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"a Domain outside vpc | standard", map[string]string{"Domain": "foo"}, "InvalidParameterValue"},
		{"a malformed Address", map[string]string{"Address": "not-an-ip"}, "InvalidAddress.Malformed"},
		{"an Address the account holds", map[string]string{"Address": held}, "InvalidIPAddress.InUse"},
		{"an Address the account does not hold", map[string]string{"Address": "203.0.113.3"}, "InvalidAddress.NotFound"},
		{"PublicIpv4Pool", map[string]string{"PublicIpv4Pool": "ipv4pool-ec2-012345"}, "InvalidParameterValue"},
		{"CustomerOwnedIpv4Pool", map[string]string{"CustomerOwnedIpv4Pool": "ipv4pool-coip-012345"}, "InvalidParameterValue"},
		{"IpamPoolId", map[string]string{"IpamPoolId": "ipam-pool-012345"}, "InvalidParameterValue"},
		{"a reserved tag key", map[string]string{
			"TagSpecification.1.ResourceType": "elastic-ip", "TagSpecification.1.Tag.1.Key": "aws:owner",
			"TagSpecification.1.Tag.1.Value": "x",
		}, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"Action": "AllocateAddress"}
			for k, v := range tc.params {
				params[k] = v
			}
			ec2RequireRefusal(t, ts, params, tc.code)
		})
	}
	var all struct {
		IDs []string `xml:"addressesSet>item>allocationId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeAddresses"}, &all)
	assert.Len(t, all.IDs, 3, "no refused request allocated an address")
}
