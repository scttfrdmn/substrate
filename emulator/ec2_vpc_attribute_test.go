package emulator_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2VPCAttribute reads one attribute back through DescribeVpcAttribute.
func ec2VPCAttribute(t *testing.T, ts *httptest.Server, vpcID, attribute string) bool {
	t.Helper()
	var doc struct {
		VpcID        string `xml:"vpcId"`
		DNSSupport   *bool  `xml:"enableDnsSupport>value"`
		DNSHostnames *bool  `xml:"enableDnsHostnames>value"`
		UsageMetrics *bool  `xml:"enableNetworkAddressUsageMetrics>value"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "DescribeVpcAttribute", "VpcId": vpcID, "Attribute": attribute,
	}, &doc)
	require.Equal(t, vpcID, doc.VpcID)
	var got *bool
	present := 0
	for name, v := range map[string]*bool{
		"enableDnsSupport": doc.DNSSupport, "enableDnsHostnames": doc.DNSHostnames,
		"enableNetworkAddressUsageMetrics": doc.UsageMetrics,
	} {
		if v != nil {
			present++
			if name == attribute {
				got = v
			}
		}
	}
	require.Equal(t, 1, present, "the response carries the one attribute asked for")
	require.NotNil(t, got, "the response carries %s", attribute)
	return *got
}

// TestEC2_VPCAttributes_PublishedSpellingsRoundTrip is #1151: each modify is sent under the
// parameter name its page publishes and read back through DescribeVpcAttribute or, for the subnet,
// DescribeSubnets.
func TestEC2_VPCAttributes_PublishedSpellingsRoundTrip(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.40.0.0/16")

	// A new VPC's defaults.
	assert.True(t, ec2VPCAttribute(t, ts, vpcID, "enableDnsSupport"))
	assert.False(t, ec2VPCAttribute(t, ts, vpcID, "enableDnsHostnames"))
	assert.False(t, ec2VPCAttribute(t, ts, vpcID, "enableNetworkAddressUsageMetrics"))

	for _, tc := range []struct {
		param, attribute string
		value            bool
	}{
		{"EnableDnsHostnames.Value", "enableDnsHostnames", true},
		{"EnableDnsSupport.Value", "enableDnsSupport", false},
		{"EnableNetworkAddressUsageMetrics.Value", "enableNetworkAddressUsageMetrics", true},
	} {
		value := "false"
		if tc.value {
			value = "true"
		}
		ec2OK(t, ts, map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, tc.param: value})
		assert.Equal(t, tc.value, ec2VPCAttribute(t, ts, vpcID, tc.attribute), tc.param)
	}

	// The Go-initialism spellings are not AWS's, and change nothing: a request carrying only one
	// names no attribute, which is refused (#1554).
	ec2RequireRefusal(t, ts, map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDNSHostnames.Value": "false"},
		"InvalidParameterCombination")
	assert.True(t, ec2VPCAttribute(t, ts, vpcID, "enableDnsHostnames"))

	subnetID := ec2CreateSubnetIn(t, ts, vpcID, "10.40.1.0/24")
	ec2OK(t, ts, map[string]string{"Action": "ModifySubnetAttribute", "SubnetId": subnetID, "MapPublicIpOnLaunch.Value": "true"})
	var subnets struct {
		Map []bool `xml:"subnetSet>item>mapPublicIpOnLaunch"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSubnets", "SubnetId.1": subnetID}, &subnets)
	assert.Equal(t, []bool{true}, subnets.Map)
}

// TestEC2_DescribeVpcAttribute_Refusals pins the request contract: VpcId and Attribute are
// required, VpcId resolves, and Attribute is one of the three published values.
func TestEC2_DescribeVpcAttribute_Refusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.41.0.0/16")
	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"no VpcId", map[string]string{"Attribute": "enableDnsSupport"}, "MissingParameter"},
		{"an unknown VPC", map[string]string{"VpcId": "vpc-0000000000000dead", "Attribute": "enableDnsSupport"}, "InvalidVpcID.NotFound"},
		{"no Attribute", map[string]string{"VpcId": vpcID}, "MissingParameter"},
		{"an unpublished Attribute", map[string]string{"VpcId": vpcID, "Attribute": "enableDNSSupport"}, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"Action": "DescribeVpcAttribute"}
			for k, v := range tc.params {
				params[k] = v
			}
			ec2RequireRefusal(t, ts, params, tc.code)
		})
	}
}
