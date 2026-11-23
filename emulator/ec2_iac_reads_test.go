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

// ec2ReadOK sends params, requires a 200 and returns the body.
func ec2ReadOK(t *testing.T, ts *httptest.Server, params map[string]string) []byte {
	t.Helper()
	resp := ec2Request(t, ts, params)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%v: %s", params, body)
	return body
}

// TestEC2DescribeNetworkInterfaces_EveryEvaluatedFilter selects the one recorded interface by
// each evaluated filter, with a value that matches and one that does not (#1519).
func TestEC2DescribeNetworkInterfaces_EveryEvaluatedFilter(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.92.0.0/16")
	subnet := ec2CreateSubnetIn(t, ts, vpcID, "10.92.1.0/24")
	ids := ec2RunInstanceIDs(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": subnet,
		"NetworkInterface.1.Description": "eth0",
	})
	require.Len(t, ids, 1)

	var out struct {
		Items []struct {
			ID     string `xml:"networkInterfaceId"`
			Owner  string `xml:"ownerId"`
			IP     string `xml:"privateIpAddress"`
			DNS    string `xml:"privateDnsName"`
			Type   string `xml:"interfaceType"`
			Attach struct {
				Owner string `xml:"instanceOwnerId"`
			} `xml:"attachment"`
		} `xml:"networkInterfaceSet>item"`
	}
	require.NoError(t, xml.Unmarshal(ec2ReadOK(t, ts, map[string]string{"Action": "DescribeNetworkInterfaces"}), &out))
	require.Len(t, out.Items, 1)
	eni := out.Items[0]

	count := func(name, value string) int {
		body := ec2ReadOK(t, ts, map[string]string{
			"Action": "DescribeNetworkInterfaces", "Filter.1.Name": name, "Filter.1.Value.1": value,
		})
		var page struct {
			Items []struct{} `xml:"networkInterfaceSet>item"`
		}
		require.NoError(t, xml.Unmarshal(body, &page))
		return len(page.Items)
	}
	filters := []struct{ name, value string }{
		{"attachment.device-index", "0"},
		{"attachment.instance-owner-id", eni.Attach.Owner},
		{"attachment.status", "attached"},
		{"description", "eth0"},
		{"interface-type", eni.Type},
		{"network-interface-id", eni.ID},
		{"owner-id", eni.Owner},
		{"private-dns-name", eni.DNS},
		{"private-ip-address", eni.IP},
		{"status", "in-use"},
	}
	for _, f := range filters {
		t.Run(f.name, func(t *testing.T) {
			assert.Equal(t, 1, count(f.name, f.value), "matches")
			assert.Equal(t, 0, count(f.name, "no-such-value"), "does not match")
		})
	}
	t.Run("availability-zone", func(t *testing.T) {
		assert.Equal(t, 0, count("availability-zone", "no-such-zone"))
	})
	t.Run("tag-key", func(t *testing.T) {
		assert.Equal(t, 0, count("tag-key", "Name"), "interfaces carry no tags")
	})
}

// TestEC2CreateVpc_TagsAndTenancy covers CreateVpc's TagSpecification and InstanceTenancy, which
// Terraform reads back after an apply (#1519).
func TestEC2CreateVpc_TagsAndTenancy(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	type vpcItem struct {
		ID      string `xml:"vpcId"`
		Tenancy string `xml:"instanceTenancy"`
		Tags    []struct {
			Key string `xml:"key"`
		} `xml:"tagSet>item"`
	}
	var created struct {
		Vpc vpcItem `xml:"vpc"`
	}
	require.NoError(t, xml.Unmarshal(ec2ReadOK(t, ts, map[string]string{
		"Action": "CreateVpc", "CidrBlock": "10.93.0.0/16", "InstanceTenancy": "dedicated",
		"TagSpecification.1.ResourceType": "vpc",
		"TagSpecification.1.Tag.1.Key":    "Name", "TagSpecification.1.Tag.1.Value": "tagged",
	}), &created))
	assert.Equal(t, "dedicated", created.Vpc.Tenancy)
	require.Len(t, created.Vpc.Tags, 1)
	assert.Equal(t, "Name", created.Vpc.Tags[0].Key)

	var described struct {
		Vpcs []vpcItem `xml:"vpcSet>item"`
	}
	require.NoError(t, xml.Unmarshal(ec2ReadOK(t, ts, map[string]string{
		"Action": "DescribeVpcs", "VpcId.1": created.Vpc.ID,
	}), &described))
	require.Len(t, described.Vpcs, 1)
	assert.Equal(t, "dedicated", described.Vpcs[0].Tenancy)
	assert.Len(t, described.Vpcs[0].Tags, 1, "DescribeVpcs reports the creation tags")

	var plain struct {
		Vpc vpcItem `xml:"vpc"`
	}
	require.NoError(t, xml.Unmarshal(ec2ReadOK(t, ts, map[string]string{
		"Action": "CreateVpc", "CidrBlock": "10.94.0.0/16",
	}), &plain))
	assert.Equal(t, "default", plain.Vpc.Tenancy, "no InstanceTenancy is the default tenancy")

	refusals := []struct {
		name   string
		params map[string]string
	}{
		{"an unknown tenancy", map[string]string{"InstanceTenancy": "shared"}},
		{"host tenancy, which CreateVpc does not take", map[string]string{"InstanceTenancy": "host"}},
		{"a reserved tag key", map[string]string{"TagSpecification.1.ResourceType": "vpc",
			"TagSpecification.1.Tag.1.Key": "aws:reserved", "TagSpecification.1.Tag.1.Value": "x"}},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "CreateVpc"
			tc.params["CidrBlock"] = "10.95.0.0/16"
			resp := ec2Request(t, ts, tc.params)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", body)
		})
	}
}
