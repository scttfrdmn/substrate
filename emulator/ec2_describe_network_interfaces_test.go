package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2DescribeNetworkInterfaces covers the call the Terraform AWS provider makes before it
// deletes a subnet or a security group (#1519): the interfaces instances hold, selected by ID and
// by filter, and an empty set for a subnet nothing is attached in.
func TestEC2DescribeNetworkInterfaces(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.91.0.0/16")
	used := ec2CreateSubnetIn(t, ts, vpcID, "10.91.1.0/24")
	empty := ec2CreateSubnetIn(t, ts, vpcID, "10.91.2.0/24")
	sg := ec2CreateSG(t, ts, "eni", "eni", vpcID)
	ids := ec2RunInstanceIDs(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"NetworkInterface.1.DeviceIndex": "0", "NetworkInterface.1.SubnetId": used,
		"NetworkInterface.1.SecurityGroupId.1": sg, "NetworkInterface.1.Description": "primary",
	})
	require.Len(t, ids, 1)

	type item struct {
		ID          string `xml:"networkInterfaceId"`
		SubnetID    string `xml:"subnetId"`
		VpcID       string `xml:"vpcId"`
		Status      string `xml:"status"`
		Description string `xml:"description"`
		Groups      []struct {
			GroupID string `xml:"groupId"`
		} `xml:"groupSet>item"`
		Attachment struct {
			InstanceID  string `xml:"instanceId"`
			DeviceIndex int    `xml:"deviceIndex"`
			Status      string `xml:"status"`
		} `xml:"attachment"`
	}
	describe := func(t *testing.T, params map[string]string) []item {
		t.Helper()
		params["Action"] = "DescribeNetworkInterfaces"
		resp := ec2Request(t, ts, params)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
		var out struct {
			Items []item `xml:"networkInterfaceSet>item"`
		}
		require.NoError(t, xml.Unmarshal(body, &out))
		return out.Items
	}

	all := describe(t, map[string]string{})
	require.Len(t, all, 1)
	eni := all[0]
	assert.Regexp(t, `^eni-[0-9a-f]+$`, eni.ID)
	assert.Equal(t, used, eni.SubnetID)
	assert.Equal(t, vpcID, eni.VpcID)
	assert.Equal(t, "in-use", eni.Status)
	assert.Equal(t, "primary", eni.Description)
	require.Len(t, eni.Groups, 1)
	assert.Equal(t, sg, eni.Groups[0].GroupID)
	assert.Equal(t, ids[0], eni.Attachment.InstanceID)
	assert.Equal(t, "attached", eni.Attachment.Status)

	selections := []struct {
		name   string
		params map[string]string
		want   int
	}{
		{"by its subnet", map[string]string{"Filter.1.Name": "subnet-id", "Filter.1.Value.1": used}, 1},
		{"a subnet with nothing attached", map[string]string{"Filter.1.Name": "subnet-id", "Filter.1.Value.1": empty}, 0},
		{"by its group", map[string]string{"Filter.1.Name": "group-id", "Filter.1.Value.1": sg}, 1},
		{"by its instance", map[string]string{"Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": ids[0]}, 1},
		{"by its VPC", map[string]string{"Filter.1.Name": "vpc-id", "Filter.1.Value.1": vpcID}, 1},
		{"a tag filter selects nothing", map[string]string{"Filter.1.Name": "tag:Name", "Filter.1.Value.1": "x"}, 0},
		{"an inert filter selects everything", map[string]string{"Filter.1.Name": "requester-managed", "Filter.1.Value.1": "false"}, 1},
		{"by its ID", map[string]string{"NetworkInterfaceId.1": eni.ID}, 1},
	}
	for _, tc := range selections {
		t.Run(tc.name, func(t *testing.T) {
			assert.Len(t, describe(t, tc.params), tc.want)
		})
	}

	refusals := []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"an ID no instance holds", map[string]string{"NetworkInterfaceId.1": "eni-0123456789abcdef0"}, "InvalidNetworkInterfaceID.NotFound"},
		{"a malformed ID", map[string]string{"NetworkInterfaceId.1": "not-an-eni"}, "InvalidNetworkInterfaceID.Malformed"},
		{"an undocumented filter", map[string]string{"Filter.1.Name": "no-such-filter", "Filter.1.Value.1": "x"}, "InvalidParameterValue"},
		{"MaxResults below the range", map[string]string{"MaxResults": "4"}, "InvalidParameterValue"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "DescribeNetworkInterfaces"
			ec2RequireRefusal(t, ts, tc.params, tc.code)
		})
	}

	t.Run("a terminated instance's interface is gone", func(t *testing.T) {
		ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": ids[0]})
		assert.Empty(t, describe(t, map[string]string{}))
	})
}
