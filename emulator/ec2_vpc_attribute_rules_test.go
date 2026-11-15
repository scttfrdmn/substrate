package emulator_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2_ModifyVpcAttribute_CombinationRules is #1554's first criterion: API_ModifyVpcAttribute's
// "You cannot modify the DNS resolution and DNS hostnames attributes in the same request" and "You
// can only enable DNS hostnames if you've enabled DNS support", each refused before anything is
// written.
func TestEC2_ModifyVpcAttribute_CombinationRules(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// setup runs first on a fresh VPC, which has DNS support on and hostnames off.
		setup  []map[string]string
		params map[string]string
		code   string
		msg    string
	}{
		{"both DNS attributes", nil,
			map[string]string{"EnableDnsSupport.Value": "true", "EnableDnsHostnames.Value": "true"},
			"InvalidParameterCombination",
			"Fields for multiple attribute types specified: enableDnsSupport, enableDnsHostnames"},
		{"a DNS attribute beside usage metrics", nil,
			map[string]string{"EnableDnsHostnames.Value": "true", "EnableNetworkAddressUsageMetrics.Value": "true"},
			"InvalidParameterCombination",
			"Fields for multiple attribute types specified: enableDnsHostnames, enableNetworkAddressUsageMetrics"},
		{"no attribute at all", nil, map[string]string{},
			"InvalidParameterCombination",
			"The request must contain exactly one of the parameters EnableDnsSupport, EnableDnsHostnames or " +
				"EnableNetworkAddressUsageMetrics"},
		{"hostnames on while DNS support is off",
			[]map[string]string{{"EnableDnsSupport.Value": "false"}},
			map[string]string{"EnableDnsHostnames.Value": "true"},
			"InvalidParameterValue", "You can only enable DNS hostnames if you've enabled DNS support."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			vpcID := ec2CreateVPC(t, ts, "10.42.0.0/16")
			modify := func(attrs map[string]string) map[string]string {
				p := map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID}
				for k, v := range attrs {
					p[k] = v
				}
				return p
			}
			for _, s := range tc.setup {
				ec2OK(t, ts, modify(s))
			}
			support := ec2VPCAttribute(t, ts, vpcID, "enableDnsSupport")
			ec2RequireRefusalMessage(t, ts, modify(tc.params), tc.code, tc.msg)
			assert.Equal(t, support, ec2VPCAttribute(t, ts, vpcID, "enableDnsSupport"), "nothing is written")
			assert.False(t, ec2VPCAttribute(t, ts, vpcID, "enableDnsHostnames"), "nothing is written")
			assert.False(t, ec2VPCAttribute(t, ts, vpcID, "enableNetworkAddressUsageMetrics"), "nothing is written")
		})
	}

	t.Run("the published order: support, then hostnames, in separate requests", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		vpcID := ec2CreateVPC(t, ts, "10.43.0.0/16")
		ec2OK(t, ts, map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDnsSupport.Value": "false"})
		ec2OK(t, ts, map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDnsHostnames.Value": "false"})
		ec2OK(t, ts, map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDnsSupport.Value": "true"})
		ec2OK(t, ts, map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDnsHostnames.Value": "true"})
		assert.True(t, ec2VPCAttribute(t, ts, vpcID, "enableDnsHostnames"))
	})
}

// ec2GatewaysAttachedTo returns the internet gateways DescribeInternetGateways reports attached
// to vpcID.
func ec2GatewaysAttachedTo(t *testing.T, ts *httptest.Server, vpcID string) []string {
	t.Helper()
	var doc struct {
		IDs []string `xml:"internetGatewaySet>item>internetGatewayId"`
	}
	ec2FleetXML(t, ts, ec2OneFilter("DescribeInternetGateways", "attachment.vpc-id", vpcID), &doc)
	return doc.IDs
}

// TestEC2_AttachInternetGateway_OnePerVPC is #1554's second criterion: "You can attach only one
// internet gateway to a VPC at a time" (Amazon VPC quotas), answered with EC2's observed
// InvalidParameterValue / "Network vpc-… already has an internet gateway attached".
func TestEC2_AttachInternetGateway_OnePerVPC(t *testing.T) {
	t.Parallel()
	attach := func(igw, vpc string) map[string]string {
		return map[string]string{"Action": "AttachInternetGateway", "InternetGatewayId": igw, "VpcId": vpc}
	}

	t.Run("a created VPC", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		vpcID := ec2CreateVPC(t, ts, "10.44.0.0/16")
		first, second := ec2CreateIGW(t, ts), ec2CreateIGW(t, ts)
		ec2OK(t, ts, attach(first, vpcID))
		ec2RequireRefusalMessage(t, ts, attach(second, vpcID), "InvalidParameterValue",
			"Network "+vpcID+" already has an internet gateway attached")
		assert.Equal(t, []string{first}, ec2GatewaysAttachedTo(t, ts, vpcID), "a refused attach writes nothing")

		ec2OK(t, ts, map[string]string{"Action": "DetachInternetGateway", "InternetGatewayId": first, "VpcId": vpcID})
		ec2OK(t, ts, attach(second, vpcID))
		assert.Equal(t, []string{second}, ec2GatewaysAttachedTo(t, ts, vpcID))
	})

	t.Run("the default VPC, which is seeded with one", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		vpcs := ec2DescribeVPCs(t, ts, ec2OneFilter("DescribeVpcs", "is-default", "true"))
		require.Len(t, vpcs, 1)
		defaultVPC := vpcs[0].VpcID
		seeded := ec2GatewaysAttachedTo(t, ts, defaultVPC)
		require.Len(t, seeded, 1)

		mine := ec2CreateIGW(t, ts)
		ec2RequireRefusal(t, ts, attach(mine, defaultVPC), "InvalidParameterValue")
		assert.Equal(t, seeded, ec2GatewaysAttachedTo(t, ts, defaultVPC))
	})
}
