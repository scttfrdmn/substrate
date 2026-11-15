package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"sort"
)

// The default VPC (#1464).
//
// Every AWS account has a default VPC in each Region, with "a default subnet in each
// Availability Zone", a default security group and an attached internet gateway
// (API_CreateDefaultVpc; the VPC User Guide's "Default VPCs" page). Through v0.125.0
// substrate created one only lazily, from a RunInstances that named no subnet, and with a
// single subnet — so a fresh DescribeVpcs answered nothing, and tooling that places
// resources in the default VPC (aws-elastic-globus-compute's workers, CDK's
// Vpc.fromLookup({isDefault: true})) found no VPC to place them in.
//
// It is now seeded on the first EC2 request an account makes in a Region, before that
// request is dispatched, so every read — DescribeVpcs, DescribeSubnets,
// DescribeSecurityGroups, DescribeInternetGateways, DescribeRouteTables — already sees it.
// The seed writes a marker record, so the check every later request makes is one Get.
//
// The seed's identifiers are derived from the account and Region, not from the request that
// happened to trigger it: the same account and Region always get the same default VPC ID,
// whichever request came first, and — the property that matters for replay — the triggering
// request's own mint is not advanced, so a recorded CreateVpc that was the first EC2 call in
// its run still mints the vpc- ID it recorded.
//
// Deleting the default VPC leaves a tombstone in the marker, so the seed does not bring it
// back; CreateDefaultVpc clears it. The seed can be switched off for the whole server with
// ec2.default_vpc (SUBSTRATE_EC2_DEFAULT_VPC=false), which starts every account and Region
// without one, as an account whose default VPC was deleted is.

// ec2DefaultVPCCIDR is the default VPC's IPv4 block, which AWS fixes: "172.31.0.0/16".
const ec2DefaultVPCCIDR = "172.31.0.0/16"

// ec2DefaultVPCMarker records whether an account and Region's default VPC has been seeded,
// and whether it has since been deleted.
type ec2DefaultVPCMarker struct {
	// VPCID is the default VPC the marker was written for.
	VPCID string `json:"vpc_id,omitempty"`

	// Deleted is the tombstone: the default VPC was deleted and must not be re-seeded.
	Deleted bool `json:"deleted,omitempty"`
}

// ec2DefaultVPCMarkerKey is the state key of an account and Region's default-VPC marker.
func ec2DefaultVPCMarkerKey(accountID, region string) string {
	return "default_vpc:" + accountID + "/" + region
}

// ec2DefaultSubnetCIDR returns the /20 a default VPC's index'th default subnet takes:
// 172.31.0.0/20, 172.31.16.0/20, 172.31.32.0/20, …, which is how AWS numbers them.
func ec2DefaultSubnetCIDR(index int) string {
	return fmt.Sprintf("172.31.%d.0/20", index*16)
}

// ec2NoDefaultVPCError is VPCIdNotSpecified, which the client-error table publishes for "You
// have no default VPC in which to carry out the request. Specify a VPC or subnet ID".
func ec2NoDefaultVPCError() *AWSError {
	return &AWSError{
		Code: "VPCIdNotSpecified",
		Message: "No default VPC for this user. Specify a VPC or subnet ID, or create a default VPC " +
			"with CreateDefaultVpc.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// writeDefaultVPCMarker records the default VPC's marker for the request's account and
// Region.
func (p *EC2Plugin) writeDefaultVPCMarker(ctx context.Context, reqCtx *RequestContext, marker ec2DefaultVPCMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("ec2 default VPC marker marshal: %w", err)
	}
	if err := p.state.Put(ctx, ec2Namespace, ec2DefaultVPCMarkerKey(reqCtx.AccountID, reqCtx.Region), data); err != nil {
		return fmt.Errorf("ec2 default VPC marker put: %w", err)
	}
	return nil
}

// seedDefaultVPC creates the request's account and Region's default VPC if it has never
// been seeded, and does nothing otherwise.
//
// The unlocked Get is the fast path every request after the first takes. The lock covers
// the slow path's check-then-create, so two concurrent first requests cannot both seed — the
// derived IDs would make the records identical, but the index lists would gain duplicates.
//
// A request context with no account or Region — a hand-built one, as some plugin tests use —
// has no scope to seed into and is left alone.
//
// A failure to *read* the marker skips the seed rather than failing the request: the seed is
// a precondition the request did not ask for, and the handler reports a state that cannot be
// read through its own reads, with its own ordering — a describe refuses a malformed token
// before it touches state (#887), and a seed refusing first would answer 500 in its place.
// A failure to *write* is returned, because by then the seed has started.
func (p *EC2Plugin) seedDefaultVPC(reqCtx *RequestContext) error {
	if p.defaultVPCDisabled || reqCtx == nil || reqCtx.AccountID == "" || reqCtx.Region == "" {
		return nil
	}
	ctx := context.Background()
	key := ec2DefaultVPCMarkerKey(reqCtx.AccountID, reqCtx.Region)
	if data, err := p.state.Get(ctx, ec2Namespace, key); err != nil || data != nil {
		return nil //nolint:nilerr // An unreadable marker skips the seed; see above.
	}
	p.defaultVPCMu.Lock()
	defer p.defaultVPCMu.Unlock()
	if data, err := p.state.Get(ctx, ec2Namespace, key); err != nil || data != nil {
		return nil //nolint:nilerr // An unreadable marker skips the seed; see above.
	}
	// State written before #1464 may already hold a default VPC, created lazily by a launch.
	// It is adopted rather than joined by a second one.
	existing, _, err := ec2LookupDefaultVPC(ctx, p.state, reqCtx, "")
	if err != nil {
		return nil //nolint:nilerr // An unreadable store skips the seed, as an unreadable marker does.
	}
	if existing != nil {
		return p.writeDefaultVPCMarker(ctx, reqCtx, ec2DefaultVPCMarker{VPCID: existing.VPCID})
	}
	seedCtx := *reqCtx
	seedCtx.IDs = NewIDMint("ec2-default-vpc/" + reqCtx.AccountID + "/" + reqCtx.Region)
	vpc, err := p.createDefaultVPCResources(ctx, &seedCtx)
	if err != nil {
		return err
	}
	return p.writeDefaultVPCMarker(ctx, reqCtx, ec2DefaultVPCMarker{VPCID: vpc.VPCID})
}

// createDefaultVPCResources writes a default VPC and everything AWS creates with one: a
// default subnet in each seeded Availability Zone, the default security group, an attached
// internet gateway, and a main route table routing 0.0.0.0/0 to it.
//
// Identifiers come from reqCtx.IDs, which the caller chooses: the seed passes a mint derived
// from the account and Region, CreateDefaultVpc the request's own.
func (p *EC2Plugin) createDefaultVPCResources(ctx context.Context, reqCtx *RequestContext) (*EC2VPC, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	vpcID := generateVPCID(reqCtx.IDs)
	vpc := EC2VPC{
		VPCID:              vpcID,
		CIDRBlock:          ec2DefaultVPCCIDR,
		IsDefault:          true,
		State:              "available",
		EnableDNSSupport:   true,
		EnableDNSHostnames: true,
		AccountID:          reqCtx.AccountID,
		Region:             reqCtx.Region,
	}
	if err := p.putEC2Record(ctx, "vpc:"+scope+"/"+vpcID, vpc); err != nil {
		return nil, fmt.Errorf("ec2 default VPC: %w", err)
	}
	if err := p.appendToList(scope, "vpc_ids", vpcID); err != nil {
		return nil, fmt.Errorf("ec2 default VPC index vpc: %w", err)
	}

	// The default security group: "A default security group includes a default rule that
	// grants instances unrestricted network access to each other" (API_CreateSecurityGroup)
	// — an all-traffic ingress rule from the group itself, as AWS's DescribeSecurityGroups
	// Example 4 renders it — and the all-traffic egress rule every new group carries.
	// The group is the one [ec2NewDefaultSecurityGroup] describes, shared with CreateVpc since
	// #1492.
	if err := p.createDefaultSecurityGroup(ctx, reqCtx, vpcID); err != nil {
		return nil, fmt.Errorf("ec2 default VPC: %w", err)
	}

	igwID := generateIGWID(reqCtx.IDs)
	igw := EC2InternetGateway{
		InternetGatewayID: igwID,
		Attachments:       []EC2IGWAttachment{{VPCID: vpcID, State: "available"}},
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}
	if err := p.putEC2Record(ctx, "igw:"+scope+"/"+igwID, igw); err != nil {
		return nil, fmt.Errorf("ec2 default VPC internet gateway: %w", err)
	}
	if err := p.appendToList(scope, "igw_ids", igwID); err != nil {
		return nil, fmt.Errorf("ec2 default VPC index internet gateway: %w", err)
	}

	if _, err := p.createRouteTableForVPC(reqCtx, vpcID, ec2DefaultVPCCIDR, true, igwID); err != nil {
		return nil, fmt.Errorf("ec2 default VPC main route table: %w", err)
	}

	for i, zone := range ec2SeededZones(reqCtx.Region) {
		if _, err := p.createDefaultSubnetIn(ctx, reqCtx, vpcID, zone.ZoneName, ec2DefaultSubnetCIDR(i)); err != nil {
			return nil, err
		}
	}
	return &vpc, nil
}

// createDefaultSubnetIn writes one default subnet: DefaultForAz and MapPublicIpOnLaunch both
// true, which is what makes a launch into it public without a NAT gateway.
func (p *EC2Plugin) createDefaultSubnetIn(ctx context.Context, reqCtx *RequestContext, vpcID, zone, cidr string) (*EC2Subnet, error) {
	subnetID := generateSubnetID(reqCtx.IDs)
	subnet := EC2Subnet{
		SubnetID:            subnetID,
		VPCID:               vpcID,
		CIDRBlock:           cidr,
		AvailabilityZone:    zone,
		IsDefault:           true,
		MapPublicIPOnLaunch: true,
		State:               "available",
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.putEC2Record(ctx, "subnet:"+scope+"/"+subnetID, subnet); err != nil {
		return nil, fmt.Errorf("ec2 default subnet: %w", err)
	}
	if err := p.appendToList(scope, "subnet_ids", subnetID); err != nil {
		return nil, fmt.Errorf("ec2 default subnet index: %w", err)
	}
	return &subnet, nil
}

// putEC2Record marshals v and writes it under key in the EC2 namespace.
func (p *EC2Plugin) putEC2Record(ctx context.Context, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	if err := p.state.Put(ctx, ec2Namespace, key, data); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// ec2LookupDefaultVPC returns the account and region's default VPC and the default subnet a
// launch naming no subnet lands in, reading state without writing any of it.
//
// It is a free function taking a [StateManager] for the same reason
// [ec2LookupLaunchTemplate] is one: the authorization decision needs it, and CheckAccess runs
// before the handler and has no plugin to call a method on. A launch that omits SubnetId is
// authorized against the subnet it will actually land in, which is only knowable by reading
// the default VPC first (#673).
//
// The default VPC has a default subnet per zone as of #1464. zone is the launch's
// Placement.AvailabilityZone: when it names one, the subnet returned is that zone's default
// subnet; when it is empty, the subnet in the lexicographically first zone — the Region's
// "a" zone while it exists. The choice has to be a rule rather than whichever record a scan
// meets first, because the handler and the authorizer both call this and must name the same
// subnet.
//
// A nil subnet with a non-nil VPC means the default VPC exists but has no default subnet
// (in zone, when one is named); both nil means there is no default VPC. A state error is
// returned rather than reported as "no default VPC".
func ec2LookupDefaultVPC(ctx context.Context, state StateManager, reqCtx *RequestContext, zone string) (*EC2VPC, *EC2Subnet, error) {
	vpcKeys, err := state.List(ctx, ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, nil, fmt.Errorf("ec2 lookupDefaultVPC list vpcs: %w", err)
	}
	for _, k := range vpcKeys {
		data, getErr := state.Get(ctx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var vpc EC2VPC
		if json.Unmarshal(data, &vpc) != nil || !vpc.IsDefault {
			continue
		}
		subnets, listErr := ec2DefaultSubnets(ctx, state, reqCtx, vpc.VPCID)
		if listErr != nil {
			return nil, nil, listErr
		}
		for _, s := range subnets {
			if zone == "" || s.AvailabilityZone == zone {
				return &vpc, &s, nil
			}
		}
		return &vpc, nil, nil
	}
	return nil, nil, nil
}

// ec2DefaultSubnets returns a default VPC's default subnets, ordered by zone.
func ec2DefaultSubnets(ctx context.Context, state StateManager, reqCtx *RequestContext, vpcID string) ([]EC2Subnet, error) {
	subnetKeys, err := state.List(ctx, ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 lookupDefaultVPC list subnets: %w", err)
	}
	var out []EC2Subnet
	for _, sk := range subnetKeys {
		sdata, sErr := state.Get(ctx, ec2Namespace, sk)
		if sErr != nil || sdata == nil {
			continue
		}
		var subnet EC2Subnet
		if json.Unmarshal(sdata, &subnet) == nil && subnet.VPCID == vpcID && subnet.IsDefault {
			out = append(out, subnet)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].AvailabilityZone < out[j].AvailabilityZone })
	return out, nil
}

// ec2LookupDefaultSecurityGroup returns the ID of the security group a launch that named
// none falls back to — the group named "default" in vpcID — or "" when none resolves.
//
// Shared between the launch handler and the authorization decision so the group the launch
// attaches and the group its policy is evaluated against cannot differ. Until #1464 the rule
// was "the first security-group key in the account and Region, if it belongs to vpcID",
// which was right only while the lazily created default group was usually the account's
// first; with the default VPC seeded eagerly beside groups a caller creates, it is the group's
// name that identifies it, which is what API_CreateSecurityGroup's "the instance is launched
// into the appropriate default security group" describes.
func ec2LookupDefaultSecurityGroup(ctx context.Context, state StateManager, reqCtx *RequestContext, vpcID string) string {
	sgKeys, err := state.List(ctx, ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return ""
	}
	for _, k := range sgKeys {
		data, getErr := state.Get(ctx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg EC2SecurityGroup
		if json.Unmarshal(data, &sg) == nil && sg.VPCID == vpcID && ec2IsDefaultSecurityGroup(sg) {
			return sg.GroupID
		}
	}
	return ""
}

// resolveLaunchDefaultSubnet returns the default VPC and subnet a launch naming no subnet
// lands in: the default subnet of the launch's Placement.AvailabilityZone, or of the
// Region's first zone when it names none.
//
// It no longer creates them (#1464): the default VPC is seeded before the request is
// dispatched, so a launch that finds none is in an account whose default VPC was deleted or
// was never seeded, and EC2 refuses that launch with VPCIdNotSpecified. A named zone with no
// default subnet is refused with InvalidInput / "No default subnet for availability zone:
// '…'", EC2's observed answer (RunInstances' Errors section is empty). A default VPC with
// no default subnet left is refused with MissingInput, the client-error table's "An input
// parameter is missing", worded as EC2 words it.
func (p *EC2Plugin) resolveLaunchDefaultSubnet(reqCtx *RequestContext, zone string) (*EC2VPC, *EC2Subnet, error) {
	vpc, subnet, err := ec2LookupDefaultVPC(context.Background(), p.state, reqCtx, zone)
	if err != nil {
		return nil, nil, err
	}
	if vpc == nil {
		return nil, nil, ec2NoDefaultVPCError()
	}
	if subnet == nil && zone != "" {
		return nil, nil, &AWSError{
			Code:       "InvalidInput",
			Message:    "No default subnet for availability zone: '" + zone + "'.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if subnet == nil {
		return nil, nil, &AWSError{
			Code:       "MissingInput",
			Message:    "No subnets found for the default VPC '" + vpc.VPCID + "'. Please specify a subnet.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return vpc, subnet, nil
}

// markDefaultVPCDeleted writes the tombstone when a deleted VPC was the default one.
func (p *EC2Plugin) markDefaultVPCDeleted(reqCtx *RequestContext, vpc EC2VPC) error {
	if !vpc.IsDefault {
		return nil
	}
	return p.writeDefaultVPCMarker(context.Background(), reqCtx, ec2DefaultVPCMarker{VPCID: vpc.VPCID, Deleted: true})
}

// createDefaultVpc handles CreateDefaultVpc: "Creates a default VPC with a size /16 IPv4
// CIDR block and a default subnet in each Availability Zone ... You cannot have more than one
// default VPC per Region."
//
// A second one is refused with DefaultVpcAlreadyExists, the client-error table's "A default
// VPC already exists in the AWS Region. You can only have one default VPC per Region." The
// new VPC's identifiers come from the request's own mint, so a VPC recreated after a delete
// does not reuse the seeded one's ID. The marker is rewritten, clearing a tombstone.
//
// The VPC is rendered available where AWS's sample shows pending: substrate mints every VPC
// available, and CreateVpc already answers that way.
func (p *EC2Plugin) createDefaultVpc(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	p.defaultVPCMu.Lock()
	defer p.defaultVPCMu.Unlock()
	existing, _, err := ec2LookupDefaultVPC(context.Background(), p.state, reqCtx, "")
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "DefaultVpcAlreadyExists",
			Message:    "A default VPC already exists for this account in this region.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	vpc, err := p.createDefaultVPCResources(context.Background(), reqCtx)
	if err != nil {
		return nil, err
	}
	if err := p.writeDefaultVPCMarker(context.Background(), reqCtx, ec2DefaultVPCMarker{VPCID: vpc.VPCID}); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name   `xml:"CreateDefaultVpcResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Vpc     ec2VPCItem `xml:"vpc"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Vpc: ec2VPCXML(*vpc)})
}

// createDefaultSubnet handles CreateDefaultSubnet: "Creates a default subnet with a size /20
// IPv4 CIDR block in the specified Availability Zone in your default VPC. You can have only
// one default subnet per Availability Zone."
//
// The zone is named by AvailabilityZone or AvailabilityZoneId — "Either ... must be
// specified, but not both" — and must be one of the Region's seeded zones. The published
// refusals are DefaultVpcDoesNotExist, DefaultSubnetAlreadyExistsInAvailabilityZone and
// InvalidAvailabilityZone, all from the client-error table. The block is the first
// 172.31.x.0/20 that overlaps no subnet of the default VPC — a caller's own CreateSubnet in it
// included. Ipv6Native is not modeled.
func (p *EC2Plugin) createDefaultSubnet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	zone, zoneID := req.Params["AvailabilityZone"], req.Params["AvailabilityZoneId"]
	if zone == "" && zoneID == "" {
		return nil, ec2MissingParameter("AvailabilityZone")
	}
	if zone != "" && zoneID != "" {
		return nil, &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "Either AvailabilityZone or AvailabilityZoneId must be specified, but not both.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	resolved := ""
	for _, z := range ec2SeededZones(reqCtx.Region) {
		if (zone != "" && z.ZoneName == zone) || (zoneID != "" && z.ZoneID == zoneID) {
			resolved = z.ZoneName
		}
	}
	if resolved == "" {
		named := zone
		if named == "" {
			named = zoneID
		}
		return nil, &AWSError{
			Code:       "InvalidAvailabilityZone",
			Message:    "The specified Availability Zone '" + named + "' is not valid.",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// Held across the check and the write, so two concurrent calls for one zone cannot both
	// pass the one-default-per-zone check, and cannot both take the same free block.
	p.defaultVPCMu.Lock()
	defer p.defaultVPCMu.Unlock()
	ctx := context.Background()
	vpc, _, err := ec2LookupDefaultVPC(ctx, p.state, reqCtx, "")
	if err != nil {
		return nil, err
	}
	if vpc == nil {
		return nil, &AWSError{
			Code:       "DefaultVpcDoesNotExist",
			Message:    "There is no default VPC in which to carry out the request. Create one with CreateDefaultVpc.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	defaults, err := ec2DefaultSubnets(ctx, p.state, reqCtx, vpc.VPCID)
	if err != nil {
		return nil, err
	}
	for _, s := range defaults {
		if s.AvailabilityZone == resolved {
			return nil, &AWSError{
				Code:       "DefaultSubnetAlreadyExistsInAvailabilityZone",
				Message:    "A default subnet already exists in Availability Zone '" + resolved + "'.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	taken, err := p.vpcSubnetCIDRs(ctx, reqCtx, vpc.VPCID)
	if err != nil {
		return nil, err
	}
	cidr := ""
	for i := 0; i < 16; i++ {
		if candidate := ec2DefaultSubnetCIDR(i); !ec2CIDROverlapsAny(candidate, taken) {
			cidr = candidate
			break
		}
	}
	if cidr == "" {
		return nil, &AWSError{
			Code:       "InsufficientFreeAddressesInSubnet",
			Message:    "The default VPC has no free /20 for a default subnet.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	subnet, err := p.createDefaultSubnetIn(ctx, reqCtx, vpc.VPCID, resolved, cidr)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name      `xml:"CreateDefaultSubnetResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Subnet  ec2SubnetItem `xml:"subnet"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Subnet: ec2SubnetXML(*subnet)})
}

// ec2CIDROverlapsAny reports whether candidate shares any address with a block in taken. A
// block that does not parse is skipped: substrate does not validate CreateSubnet's CidrBlock,
// so a stored one may not be a CIDR at all, and it cannot hold an address the candidate needs.
func ec2CIDROverlapsAny(candidate string, taken map[string]bool) bool {
	_, want, err := net.ParseCIDR(candidate)
	if err != nil {
		return true
	}
	for block := range taken {
		_, have, parseErr := net.ParseCIDR(block)
		if parseErr != nil {
			continue
		}
		if want.Contains(have.IP) || have.Contains(want.IP) {
			return true
		}
	}
	return false
}

// vpcSubnetCIDRs returns the IPv4 blocks every subnet of vpcID holds.
func (p *EC2Plugin) vpcSubnetCIDRs(ctx context.Context, reqCtx *RequestContext, vpcID string) (map[string]bool, error) {
	keys, err := p.state.List(ctx, ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 vpcSubnetCIDRs list: %w", err)
	}
	taken := map[string]bool{}
	for _, k := range keys {
		data, getErr := p.state.Get(ctx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var s EC2Subnet
		if json.Unmarshal(data, &s) == nil && s.VPCID == vpcID {
			taken[s.CIDRBlock] = true
		}
	}
	return taken, nil
}
