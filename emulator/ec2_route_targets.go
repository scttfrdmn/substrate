package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// Route destinations and targets (#1187).
//
// API_CreateRoute and API_ReplaceRoute both open with the same two composition rules: "You must
// specify either a destination CIDR block or a prefix list ID. You must also specify exactly one
// of the resources from the parameter list." Through v0.127.0 CreateRoute read GatewayId alone,
// so the private-subnet default route every three-tier VPC template emits — NatGatewayId to
// 0.0.0.0/0 — was stored with an empty target, and ReplaceRoute read six of the twelve targets
// and folded all of them onto one gateway field. Both handlers now read every published target
// through [ec2ParseRouteTarget], and a route stores one field per target type, rendered under
// the element API_Route publishes for it.

// ec2RouteTargetParam is one published route-target request parameter and the field of an
// [EC2Route] it is stored in.
type ec2RouteTargetParam struct {
	// Param is the request parameter's published name.
	Param string

	// Field returns the route field the target is stored in.
	Field func(*EC2Route) *string
}

// ec2RouteTargetParams is the twelve target parameters API_CreateRoute and API_ReplaceRoute
// publish, in the pages' order.
//
// VpcEndpointId has no member of its own on API_Route, whose contents list gatewayId — "The ID
// of a gateway attached to your VPC" — and no vpcEndpointId, so a Gateway Load Balancer
// endpoint target is stored in, and rendered as, gatewayId.
var ec2RouteTargetParams = []ec2RouteTargetParam{
	{"CarrierGatewayId", func(r *EC2Route) *string { return &r.CarrierGatewayID }},
	{"CoreNetworkArn", func(r *EC2Route) *string { return &r.CoreNetworkARN }},
	{"EgressOnlyInternetGatewayId", func(r *EC2Route) *string { return &r.EgressOnlyInternetGatewayID }},
	{"GatewayId", func(r *EC2Route) *string { return &r.GatewayID }},
	{"InstanceId", func(r *EC2Route) *string { return &r.InstanceID }},
	{"LocalGatewayId", func(r *EC2Route) *string { return &r.LocalGatewayID }},
	{"NatGatewayId", func(r *EC2Route) *string { return &r.NatGatewayID }},
	{"NetworkInterfaceId", func(r *EC2Route) *string { return &r.NetworkInterfaceID }},
	{"OdbNetworkArn", func(r *EC2Route) *string { return &r.OdbNetworkARN }},
	{"TransitGatewayId", func(r *EC2Route) *string { return &r.TransitGatewayID }},
	{"VpcEndpointId", func(r *EC2Route) *string { return &r.GatewayID }},
	{"VpcPeeringConnectionId", func(r *EC2Route) *string { return &r.VpcPeeringConnectionID }},
}

// ec2RouteGatewayIDKind is the kind a GatewayId naming an internet gateway resolves through.
//
// It is not [ec2InternetGatewayIDKind]: a route's GatewayId is "The ID of an internet gateway or
// virtual private gateway", and the client-error table's code for one that names nothing is
// InvalidGatewayID.NotFound, "The specified gateway does not exist", not the internet-gateway
// family's InvalidInternetGatewayID.NotFound. The table publishes no InvalidGatewayID.Malformed,
// so a malformed ID is NotFound too.
var ec2RouteGatewayIDKind = ec2IDKind{
	Prefix: "igw-", NotFound: "InvalidGatewayID.NotFound", Noun: "gateway",
}

// ec2RouteDestination is the destination a route request names: exactly one of the three
// published destination parameters, under the parameter name it came from.
type ec2RouteDestination struct {
	// IPv4 is DestinationCidrBlock.
	IPv4 string

	// IPv6 is DestinationIpv6CidrBlock.
	IPv6 string

	// PrefixList is DestinationPrefixListId.
	PrefixList string
}

// matches reports whether a stored route has this destination, compared exactly. API_DeleteRoute
// and API_ReplaceRoute both say the value "must match the CIDR for the route exactly"; the
// request's IPv4 block has been canonicalised by then, as the stored one was.
func (d ec2RouteDestination) matches(r EC2Route) bool {
	switch {
	case d.IPv4 != "":
		return r.DestinationCIDR == d.IPv4
	case d.IPv6 != "":
		return r.DestinationIPv6CIDR == d.IPv6
	default:
		return d.PrefixList != "" && r.DestinationPrefixListID == d.PrefixList
	}
}

// ec2ParseRouteDestination reads a route request's destination.
//
// No destination at all is MissingParameter; more than one is InvalidParameterCombination,
// substrate's reading of "either a destination CIDR block or a prefix list ID" — the pages
// publish no code for either, so both come from the client-error table, and the messages are
// substrate's own.
//
// The IPv4 block is canonicalised for every caller, per CreateRoute's "We modify the specified
// CIDR block to its canonical form; for example, if you specify 100.68.0.18/18, we modify it to
// 100.68.0.0/18." CreateRoute stores the canonical form, so ReplaceRoute and DeleteRoute
// canonicalise theirs to find it. A block that does not parse as a CIDR is kept verbatim, as
// every route was before #1187.
func ec2ParseRouteDestination(params map[string]string) (ec2RouteDestination, error) {
	d := ec2RouteDestination{
		IPv4:       params["DestinationCidrBlock"],
		IPv6:       params["DestinationIpv6CidrBlock"],
		PrefixList: params["DestinationPrefixListId"],
	}
	named := 0
	for _, v := range []string{d.IPv4, d.IPv6, d.PrefixList} {
		if v != "" {
			named++
		}
	}
	switch {
	case named == 0:
		return d, &AWSError{
			Code: "MissingParameter",
			Message: "The request must contain one of the parameters DestinationCidrBlock, " +
				"DestinationIpv6CidrBlock or DestinationPrefixListId",
			HTTPStatus: http.StatusBadRequest,
		}
	case named > 1:
		return d, &AWSError{
			Code: "InvalidParameterCombination",
			Message: "Only one of DestinationCidrBlock, DestinationIpv6CidrBlock and " +
				"DestinationPrefixListId may be specified",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if d.IPv4 != "" {
		if _, block, err := net.ParseCIDR(d.IPv4); err == nil && block.IP.To4() != nil {
			d.IPv4 = block.String()
		}
	}
	return d, nil
}

// ec2ParseRouteTarget reads a route request's target into a route carrying exactly one target
// field.
//
// No target is MissingParameter and two or more are InvalidParameterCombination, enforcing
// "You must also specify exactly one of the resources from the parameter list". As for the
// destination, the pages publish no code, so both come from the client-error table, and the
// messages are substrate's own.
//
// localTarget admits ReplaceRoute's LocalTarget, "whether to reset the local route to its
// default target (local)", as one more way to name the target; CreateRoute publishes no such
// parameter.
func ec2ParseRouteTarget(params map[string]string, localTarget bool) (EC2Route, error) {
	var route EC2Route
	var named []string
	for _, t := range ec2RouteTargetParams {
		if v := params[t.Param]; v != "" {
			*t.Field(&route) = v
			named = append(named, t.Param)
		}
	}
	if localTarget && strings.EqualFold(params["LocalTarget"], "true") {
		route.GatewayID = "local"
		named = append(named, "LocalTarget")
	}
	switch {
	case len(named) == 0:
		return route, &AWSError{
			Code:       "MissingParameter",
			Message:    "The request must contain exactly one route target, such as GatewayId or NatGatewayId",
			HTTPStatus: http.StatusBadRequest,
		}
	case len(named) > 1:
		return route, &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "Only one route target may be specified, but the request names " + strings.Join(named, ", "),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return route, nil
}

// resolveRouteTarget refuses a target naming a resource substrate stores that does not exist,
// with that resource's NotFound code: an internet gateway (InvalidGatewayID.NotFound, see
// [ec2RouteGatewayIDKind]), a NAT gateway or an instance.
//
// The other targets are stored unresolved, because substrate holds no record of their type:
// a virtual private gateway (a GatewayId that is not igw-), a transit gateway, a VPC peering
// connection, an egress-only internet gateway, a VPC endpoint, a carrier or local gateway, a core
// network or an ODB network. NetworkInterfaceId is in that list too until substrate models
// network interfaces as their own records (#1522).
//
// It returns the state the route is stored in. A target that resolves but is not available is
// stored `blackhole`, which is what API_Route defines the state as: "The blackhole state indicates
// that the route's target isn't available (for example, the specified gateway isn't attached to
// the VPC, or the specified NAT instance has been terminated)." A NAT gateway counts as
// unavailable once it is `deleting`, `deleted` or `failed` (the record keeps the settled state), and
// an instance once it is `shutting-down` or `terminated`. Every other target is `active`. A target
// that does not exist at all is refused rather than stored `blackhole`: the page publishes the
// NotFound codes for that, and blackhole describes a target that exists but cannot carry traffic.
//
// The state is decided when the route is written. A route whose target goes away later keeps the
// state it was written with (#1552).
func (p *EC2Plugin) resolveRouteTarget(reqCtx *RequestContext, route EC2Route) (string, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region + "/"
	ctx := context.Background()
	switch {
	case strings.HasPrefix(route.GatewayID, "igw-"):
		data, err := p.state.Get(ctx, ec2Namespace, "igw:"+scope+route.GatewayID)
		if err != nil {
			return "", fmt.Errorf("ec2 resolve route target %s: %w", route.GatewayID, err)
		}
		return ec2RouteActive, ec2RequireResource(ec2RouteGatewayIDKind, route.GatewayID, data != nil)
	case route.NatGatewayID != "":
		data, err := p.state.Get(ctx, ec2Namespace, "nat:"+scope+route.NatGatewayID)
		if err != nil {
			return "", fmt.Errorf("ec2 resolve route target %s: %w", route.NatGatewayID, err)
		}
		if reqErr := ec2RequireResource(ec2NatGatewayIDKind, route.NatGatewayID, data != nil); reqErr != nil {
			return "", reqErr
		}
		var gw EC2NATGateway
		if err := json.Unmarshal(data, &gw); err != nil {
			return "", fmt.Errorf("ec2 resolve route target %s: %w", route.NatGatewayID, err)
		}
		switch gw.State {
		case "deleting", "deleted", "failed":
			return ec2RouteBlackhole, nil
		}
		return ec2RouteActive, nil
	case route.InstanceID != "":
		data, err := p.state.Get(ctx, ec2Namespace, "instance:"+scope+route.InstanceID)
		if err != nil {
			return "", fmt.Errorf("ec2 resolve route target %s: %w", route.InstanceID, err)
		}
		if reqErr := ec2RequireResource(ec2InstanceIDKind, route.InstanceID, data != nil); reqErr != nil {
			return "", reqErr
		}
		var inst EC2Instance
		if err := json.Unmarshal(data, &inst); err != nil {
			return "", fmt.Errorf("ec2 resolve route target %s: %w", route.InstanceID, err)
		}
		switch inst.State.Name {
		case "shutting-down", "terminated":
			return ec2RouteBlackhole, nil
		}
		return ec2RouteActive, nil
	default:
		return ec2RouteActive, nil
	}
}

// API_Route's two published states.
const (
	ec2RouteActive    = "active"
	ec2RouteBlackhole = "blackhole"
)

// ec2RouteNotFoundError is InvalidRoute.NotFound, with the first sentence of the client-error
// table's gloss as its message.
func ec2RouteNotFoundError() *AWSError {
	return &AWSError{
		Code:       "InvalidRoute.NotFound",
		Message:    "The specified route does not exist in the specified route table.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2RouteAlreadyExistsError is RouteAlreadyExists, with the client-error table's gloss as its
// message.
func ec2RouteAlreadyExistsError() *AWSError {
	return &AWSError{
		Code:       "RouteAlreadyExists",
		Message:    "A route for the specified CIDR block already exists in this route table.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2RouteItem is one routeSet>item, AWS's Route.
//
// Each target renders under its own published element, and only when set. gatewayId follows
// destinationCidrBlock directly, where it has always rendered, so a route to an internet
// gateway is byte-identical to before #1187. Through v0.127.0 gatewayId was rendered on every
// route, empty included; it is now omitted when the route names another target. instanceOwnerId
// is the route table's account: substrate's instances are all the caller's own.
type ec2RouteItem struct {
	DestCIDR                    string `xml:"destinationCidrBlock,omitempty"`
	GatewayID                   string `xml:"gatewayId,omitempty"`
	DestIPv6CIDR                string `xml:"destinationIpv6CidrBlock,omitempty"` //nolint:revive
	DestPrefixListID            string `xml:"destinationPrefixListId,omitempty"`
	CarrierGatewayID            string `xml:"carrierGatewayId,omitempty"`
	CoreNetworkARN              string `xml:"coreNetworkArn,omitempty"`
	EgressOnlyInternetGatewayID string `xml:"egressOnlyInternetGatewayId,omitempty"`
	InstanceID                  string `xml:"instanceId,omitempty"`
	InstanceOwnerID             string `xml:"instanceOwnerId,omitempty"`
	LocalGatewayID              string `xml:"localGatewayId,omitempty"`
	NatGatewayID                string `xml:"natGatewayId,omitempty"`
	NetworkInterfaceID          string `xml:"networkInterfaceId,omitempty"`
	OdbNetworkARN               string `xml:"odbNetworkArn,omitempty"`
	TransitGatewayID            string `xml:"transitGatewayId,omitempty"`
	VpcPeeringConnectionID      string `xml:"vpcPeeringConnectionId,omitempty"`
	State                       string `xml:"state"`
}

// ec2RouteXML renders one stored route. A route written before #1187 carries only GatewayID,
// whatever the target's type, and so still renders as gatewayId.
func ec2RouteXML(r EC2Route, ownerID string) ec2RouteItem {
	item := ec2RouteItem{
		DestCIDR:                    r.DestinationCIDR,
		GatewayID:                   r.GatewayID,
		DestIPv6CIDR:                r.DestinationIPv6CIDR,
		DestPrefixListID:            r.DestinationPrefixListID,
		CarrierGatewayID:            r.CarrierGatewayID,
		CoreNetworkARN:              r.CoreNetworkARN,
		EgressOnlyInternetGatewayID: r.EgressOnlyInternetGatewayID,
		InstanceID:                  r.InstanceID,
		LocalGatewayID:              r.LocalGatewayID,
		NatGatewayID:                r.NatGatewayID,
		NetworkInterfaceID:          r.NetworkInterfaceID,
		OdbNetworkARN:               r.OdbNetworkARN,
		TransitGatewayID:            r.TransitGatewayID,
		VpcPeeringConnectionID:      r.VpcPeeringConnectionID,
		State:                       r.State,
	}
	if r.InstanceID != "" {
		item.InstanceOwnerID = ownerID
	}
	return item
}

// ec2SetRouteTarget replaces a stored route's target with the one target carries, clearing every
// other target field so the route never names two.
func ec2SetRouteTarget(r *EC2Route, target EC2Route) {
	for _, t := range ec2RouteTargetParams {
		*t.Field(r) = *t.Field(&target)
	}
}
