package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2RouteTableFixture is a VPC with a fresh, non-main route table to put routes in.
type ec2RouteTableFixture struct {
	vpcID, rtbID string
}

func newEC2RouteTableFixture(t *testing.T, ts *httptest.Server) ec2RouteTableFixture {
	t.Helper()
	vpcID := ec2CreateVPC(t, ts, "10.50.0.0/16")
	var doc struct {
		ID string `xml:"routeTable>routeTableId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID}, &doc)
	require.NotEmpty(t, doc.ID)
	return ec2RouteTableFixture{vpcID: vpcID, rtbID: doc.ID}
}

// routeItems returns the raw routeSet>item elements of the fixture's table, in order.
func (f ec2RouteTableFixture) routeItems(t *testing.T, ts *httptest.Server) []string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeRouteTables", "RouteTableId.1": f.rtbID})
	body := readBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	_, rest, ok := strings.Cut(body, "<routeSet>")
	if !ok {
		return nil
	}
	set, _, _ := strings.Cut(rest, "</routeSet>")
	var out []string
	for _, part := range strings.Split(set, "<item>") {
		if item, _, found := strings.Cut(part, "</item>"); found {
			out = append(out, "<item>"+item+"</item>")
		}
	}
	return out
}

func (f ec2RouteTableFixture) create(params map[string]string) map[string]string {
	out := map[string]string{"Action": "CreateRoute", "RouteTableId": f.rtbID}
	for k, v := range params {
		out[k] = v
	}
	return out
}

// TestEC2_CreateRoute_EveryTargetUnderItsOwnElement is #1187's headline: a NAT-gateway default
// route stores the NAT gateway, and each target renders under the element API_Route publishes for
// it, alone.
func TestEC2_CreateRoute_EveryTargetUnderItsOwnElement(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newEC2RouteTableFixture(t, ts)
	igw := ec2CreateIGW(t, ts)
	nat := ec2CreateNatGatewayID(t, ts)
	instance := ec2RunInstanceIDs(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
	})[0]

	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"an internet gateway, byte-identical to before",
			map[string]string{"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igw},
			"<item><destinationCidrBlock>0.0.0.0/0</destinationCidrBlock><gatewayId>" + igw +
				"</gatewayId><state>active</state></item>"},
		{"a NAT gateway",
			map[string]string{"DestinationCidrBlock": "10.60.0.0/16", "NatGatewayId": nat},
			"<item><destinationCidrBlock>10.60.0.0/16</destinationCidrBlock><natGatewayId>" + nat +
				"</natGatewayId><state>active</state></item>"},
		{"a NAT instance, with its owner",
			map[string]string{"DestinationCidrBlock": "10.61.0.0/16", "InstanceId": instance},
			"<item><destinationCidrBlock>10.61.0.0/16</destinationCidrBlock><instanceId>" + instance +
				"</instanceId><instanceOwnerId>123456789012</instanceOwnerId><state>active</state></item>"},
		{"a network interface, stored unresolved",
			map[string]string{"DestinationCidrBlock": "10.62.0.0/16", "NetworkInterfaceId": "eni-0123456789abcdef0"},
			"<item><destinationCidrBlock>10.62.0.0/16</destinationCidrBlock><networkInterfaceId>eni-0123456789abcdef0" +
				"</networkInterfaceId><state>active</state></item>"},
		{"a transit gateway",
			map[string]string{"DestinationCidrBlock": "10.63.0.0/16", "TransitGatewayId": "tgw-0123456789abcdef0"},
			"<item><destinationCidrBlock>10.63.0.0/16</destinationCidrBlock><transitGatewayId>tgw-0123456789abcdef0" +
				"</transitGatewayId><state>active</state></item>"},
		{"a VPC peering connection",
			map[string]string{"DestinationCidrBlock": "10.64.0.0/16", "VpcPeeringConnectionId": "pcx-111aaa22"},
			"<item><destinationCidrBlock>10.64.0.0/16</destinationCidrBlock><vpcPeeringConnectionId>pcx-111aaa22" +
				"</vpcPeeringConnectionId><state>active</state></item>"},
		{"a Gateway Load Balancer endpoint renders as gatewayId, the element API_Route has for it",
			map[string]string{"DestinationCidrBlock": "10.65.0.0/16", "VpcEndpointId": "vpce-0123456789abcdef0"},
			"<item><destinationCidrBlock>10.65.0.0/16</destinationCidrBlock><gatewayId>vpce-0123456789abcdef0" +
				"</gatewayId><state>active</state></item>"},
		{"an IPv6 destination to an egress-only gateway",
			map[string]string{"DestinationIpv6CidrBlock": "::/0", "EgressOnlyInternetGatewayId": "eigw-1234567890abc1234"},
			"<item><destinationIpv6CidrBlock>::/0</destinationIpv6CidrBlock><egressOnlyInternetGatewayId>" +
				"eigw-1234567890abc1234</egressOnlyInternetGatewayId><state>active</state></item>"},
		{"a prefix-list destination to a core network",
			map[string]string{"DestinationPrefixListId": "pl-63a5400a", "CoreNetworkArn": "arn:aws:networkmanager::123456789012:core-network/core-network-1"},
			"<item><destinationPrefixListId>pl-63a5400a</destinationPrefixListId><coreNetworkArn>" +
				"arn:aws:networkmanager::123456789012:core-network/core-network-1</coreNetworkArn><state>active</state></item>"},
		{"the page's canonicalisation example",
			map[string]string{"DestinationCidrBlock": "100.68.0.18/18", "CarrierGatewayId": "cagw-0123456789abcdef0"},
			"<item><destinationCidrBlock>100.68.0.0/18</destinationCidrBlock><carrierGatewayId>cagw-0123456789abcdef0" +
				"</carrierGatewayId><state>active</state></item>"},
	}
	for _, tc := range tests {
		ec2OK(t, ts, f.create(tc.params))
	}
	items := f.routeItems(t, ts)
	require.Len(t, items, len(tests))
	for i, tc := range tests {
		assert.Equal(t, tc.want, items[i], tc.name)
	}
}

// TestEC2_CreateRoute_CompositionRules pins the two sentences API_CreateRoute opens with, the
// NotFound codes of a target substrate stores, and RouteAlreadyExists. Each refusal writes nothing.
func TestEC2_CreateRoute_CompositionRules(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newEC2RouteTableFixture(t, ts)
	igw := ec2CreateIGW(t, ts)
	ec2OK(t, ts, f.create(map[string]string{"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igw}))

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"no destination", map[string]string{"GatewayId": igw}, "MissingParameter"},
		{"two destinations", map[string]string{
			"DestinationCidrBlock": "10.0.0.0/8", "DestinationIpv6CidrBlock": "::/0", "GatewayId": igw,
		}, "InvalidParameterCombination"},
		{"no target", map[string]string{"DestinationCidrBlock": "10.0.0.0/8"}, "MissingParameter"},
		{"two targets", map[string]string{
			"DestinationCidrBlock": "10.0.0.0/8", "GatewayId": igw, "NatGatewayId": "nat-0123456789abcdef0",
		}, "InvalidParameterCombination"},
		{"an internet gateway that does not exist", map[string]string{
			"DestinationCidrBlock": "10.0.0.0/8", "GatewayId": "igw-0000000000000dead",
		}, "InvalidGatewayID.NotFound"},
		{"a NAT gateway that does not exist", map[string]string{
			"DestinationCidrBlock": "10.0.0.0/8", "NatGatewayId": "nat-0000000000000dead",
		}, "InvalidNatGatewayID.NotFound"},
		{"an instance that does not exist", map[string]string{
			"DestinationCidrBlock": "10.0.0.0/8", "InstanceId": "i-0000000000000dead",
		}, "InvalidInstanceID.NotFound"},
		{"a destination the table already routes", map[string]string{
			"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igw,
		}, "RouteAlreadyExists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ec2RequireRefusal(t, ts, f.create(tc.params), tc.code)
		})
	}
	assert.Len(t, f.routeItems(t, ts), 1, "no refused request stored a route")
}

// TestEC2_ReplaceRoute_SharesTheParser pins that ReplaceRoute reads the same twelve targets as
// CreateRoute, clears the previous one, and accepts its own LocalTarget.
func TestEC2_ReplaceRoute_SharesTheParser(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newEC2RouteTableFixture(t, ts)
	igw := ec2CreateIGW(t, ts)
	nat := ec2CreateNatGatewayID(t, ts)
	ec2OK(t, ts, f.create(map[string]string{"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igw}))

	replace := func(params map[string]string) map[string]string {
		out := map[string]string{"Action": "ReplaceRoute", "RouteTableId": f.rtbID}
		for k, v := range params {
			out[k] = v
		}
		return out
	}
	ec2OK(t, ts, replace(map[string]string{"DestinationCidrBlock": "0.0.0.0/0", "NatGatewayId": nat}))
	assert.Equal(t, []string{"<item><destinationCidrBlock>0.0.0.0/0</destinationCidrBlock><natGatewayId>" + nat +
		"</natGatewayId><state>active</state></item>"}, f.routeItems(t, ts), "the gateway target is cleared")

	ec2OK(t, ts, replace(map[string]string{"DestinationCidrBlock": "0.0.0.0/0", "LocalTarget": "true"}))
	assert.Equal(t, []string{"<item><destinationCidrBlock>0.0.0.0/0</destinationCidrBlock><gatewayId>local" +
		"</gatewayId><state>active</state></item>"}, f.routeItems(t, ts))

	ec2RequireRefusal(t, ts, replace(map[string]string{"DestinationCidrBlock": "0.0.0.0/0"}), "MissingParameter")
	ec2RequireRefusal(t, ts, replace(map[string]string{
		"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igw, "LocalTarget": "true",
	}), "InvalidParameterCombination")
	ec2RequireRefusal(t, ts, replace(map[string]string{
		"DestinationCidrBlock": "0.0.0.0/0", "NatGatewayId": "nat-0000000000000dead",
	}), "InvalidNatGatewayID.NotFound")
	ec2RequireRefusal(t, ts, replace(map[string]string{
		"DestinationCidrBlock": "10.9.0.0/16", "GatewayId": igw,
	}), "InvalidRoute.NotFound")
}

// TestEC2_DeleteRoute_MatchesEveryDestinationForm pins DeleteRoute reading the IPv6 and
// prefix-list destinations its page publishes, and requiring one.
func TestEC2_DeleteRoute_MatchesEveryDestinationForm(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newEC2RouteTableFixture(t, ts)
	ec2OK(t, ts, f.create(map[string]string{"DestinationIpv6CidrBlock": "::/0", "EgressOnlyInternetGatewayId": "eigw-1234567890abc1234"}))
	ec2OK(t, ts, f.create(map[string]string{"DestinationPrefixListId": "pl-63a5400a", "TransitGatewayId": "tgw-0123456789abcdef0"}))
	require.Len(t, f.routeItems(t, ts), 2)

	ec2RequireRefusal(t, ts, map[string]string{"Action": "DeleteRoute", "RouteTableId": f.rtbID}, "MissingParameter")
	ec2OK(t, ts, map[string]string{"Action": "DeleteRoute", "RouteTableId": f.rtbID, "DestinationIpv6CidrBlock": "::/0"})
	ec2OK(t, ts, map[string]string{"Action": "DeleteRoute", "RouteTableId": f.rtbID, "DestinationPrefixListId": "pl-63a5400a"})
	assert.Empty(t, f.routeItems(t, ts))
}

// TestEC2_Route_StoredBeforeTargetFieldsReadsAsGatewayID pins the additive schema: a route
// record written before #1187, whatever target it folded into gateway_id, still renders as
// gatewayId.
func TestEC2_Route_StoredBeforeTargetFieldsReadsAsGatewayID(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	f := newEC2RouteTableFixture(t, ts)

	key := "rtb:123456789012/us-east-1/" + f.rtbID
	raw, err := state.Get(context.Background(), "ec2", key)
	require.NoError(t, err)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &record))
	record["routes"] = json.RawMessage(`[{"destination_cidr":"0.0.0.0/0","gateway_id":"nat-0123456789abcdef0","state":"active"}]`)
	patched, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "ec2", key, patched))

	assert.Equal(t, []string{"<item><destinationCidrBlock>0.0.0.0/0</destinationCidrBlock><gatewayId>nat-0123456789abcdef0" +
		"</gatewayId><state>active</state></item>"}, f.routeItems(t, ts))
}
