package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Network ACLs (#1614).
//
// The VPC User Guide's "Default network ACL for a VPC": "Your virtual private cloud (VPC)
// automatically comes with a default network ACL. A default network ACL is configured to allow
// all traffic to flow in and out of the subnets with which it is associated." Every VPC
// substrate creates — CreateVpc and the seeded default VPC — gets one, and every subnet gets an
// aclassoc- association to its VPC's default ACL ("By default when you create a subnet, it's
// automatically associated with the default network ACL", API_ReplaceNetworkAclAssociation).
//
// The rules are recorded and rendered, never applied: substrate carries no packets, so an ACL
// is observable only through DescribeNetworkAcls, which is the scope rule in CLAUDE.md.
//
// # State
//
// An ACL is stored under acl:{account}/{region}/{acl-id} as an [EC2NetworkACL]. A subnet's
// association is stored under aclassoc:{account}/{region}/{subnet-id} as an
// [EC2NetworkACLAssociation], keyed by the subnet because a subnet has exactly one at a time;
// ReplaceNetworkAclAssociation rewrites it, and DeleteSubnet removes it.
//
// # Records an older build wrote
//
// A VPC or subnet written before #1614 has no ACL record and no association record. They are
// synthesized when read, never by drawing from the request's mint: the default ACL's ID is
// derived from the account, Region and VPC ID ([ec2DerivedDefaultNetworkACLID]), and an
// association's from the account, Region and subnet ID ([ec2DerivedNetworkACLAssociationID]),
// each through a seeded [IDMint] of its own. The same VPC therefore always reads back the same
// acl- ID, and a write that touches a synthesized record — an entry change, a replaced
// association, a tag — persists it under that ID, after which it reads back from state like any
// other.
//
// # Locking
//
// Every write of an ACL or association record holds [EC2Plugin.sgMu], the lock DeleteVpc and
// DeleteSubnet hold across their dependency checks and deletes:
//
//   - the operations below take it themselves;
//   - CreateVpc and the default VPC's creation ([EC2Plugin.createDefaultVPCResources]) hold it
//     across the whole creation, and [EC2Plugin.createDefaultNetworkACL] runs under it;
//   - [EC2Plugin.associateSubnetWithDefaultNetworkACL], which CreateSubnet and
//     CreateDefaultSubnet call, takes it around the default-ACL lookup and the write;
//   - every tag write — CreateTags, DeleteTags, the tagging API and CloudFormation's stamps —
//     takes it before [EC2Plugin.tagMu] for an ID whose record other operations also rewrite
//     under it ([ec2TagWriteTakesSGMu]), see [EC2Plugin.lockTagWrite].
//
// The order is igwMu, then defaultVPCMu, then sgMu, then tagMu, and nothing holding a later lock
// takes an earlier one.

// ec2NetworkACLDefaultRuleNumber is the rule number of the IPv4 catch-all deny every ACL ends
// with — the rule the console shows as "*" and DescribeNetworkAcls renders as 32767 (AWS's
// DescribeNetworkAcls sample). CreateNetworkAclEntry's RuleNumber reserves it: "Positive integer
// from 1 to 32766. The range 32767 to 65535 is reserved for internal use".
const ec2NetworkACLDefaultRuleNumber = 32767

// ec2NetworkACLMaxRuleNumber is the largest rule number a caller may write.
const ec2NetworkACLMaxRuleNumber = 32766

// ec2NetworkACLRulesPerDirection is the "Inbound or outbound rules per network ACL" quota's
// default, 20, from the VPC User Guide's quotas page. The catch-all deny is not counted, since
// a caller cannot add or remove it.
const ec2NetworkACLRulesPerDirection = 20

// EC2NetworkACL is a stored network ACL.
type EC2NetworkACL struct {
	// NetworkACLID is the ACL's acl- ID.
	NetworkACLID string `json:"network_acl_id"`

	// VPCID is the VPC the ACL belongs to.
	VPCID string `json:"vpc_id"`

	// IsDefault reports whether this is the VPC's default ACL.
	IsDefault bool `json:"is_default,omitempty"`

	// Entries are the ACL's rules, both directions, including the catch-all denies.
	Entries []EC2NetworkACLEntry `json:"entries,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`
}

// EC2NetworkACLEntry is one rule of a network ACL.
type EC2NetworkACLEntry struct {
	// RuleNumber orders the rule within its direction.
	RuleNumber int `json:"rule_number"`

	// Egress is true for an outbound rule.
	Egress bool `json:"egress"`

	// Protocol is the IANA protocol number as a string, or "-1" for all protocols.
	Protocol string `json:"protocol"`

	// RuleAction is allow or deny.
	RuleAction string `json:"rule_action"`

	// CIDRBlock is the IPv4 range, canonicalised, or empty for an IPv6 rule.
	CIDRBlock string `json:"cidr_block,omitempty"`

	// IPv6CIDRBlock is the IPv6 range, canonicalised, or empty for an IPv4 rule.
	IPv6CIDRBlock string `json:"ipv6_cidr_block,omitempty"`

	// PortRange is the port range of a TCP or UDP rule, nil for any other protocol.
	PortRange *EC2NetworkACLPortRange `json:"port_range,omitempty"`

	// ICMPTypeCode is the ICMP type and code of an ICMP or ICMPv6 rule, nil otherwise.
	ICMPTypeCode *EC2NetworkACLICMPTypeCode `json:"icmp_type_code,omitempty"`
}

// EC2NetworkACLPortRange is a network ACL rule's port range.
type EC2NetworkACLPortRange struct {
	// From is the first port.
	From int `json:"from"`

	// To is the last port.
	To int `json:"to"`
}

// EC2NetworkACLICMPTypeCode is a network ACL rule's ICMP type and code.
type EC2NetworkACLICMPTypeCode struct {
	// Type is the ICMP type, -1 for all types.
	Type int `json:"type"`

	// Code is the ICMP code, -1 for all codes.
	Code int `json:"code"`
}

// EC2NetworkACLAssociation is a subnet's association with a network ACL.
type EC2NetworkACLAssociation struct {
	// AssociationID is the aclassoc- ID.
	AssociationID string `json:"association_id"`

	// NetworkACLID is the ACL the subnet is associated with.
	NetworkACLID string `json:"network_acl_id"`

	// SubnetID is the associated subnet.
	SubnetID string `json:"subnet_id"`
}

// ec2NetworkACLIDKind is the network ACL ID family, with both codes from the EC2 error
// reference: InvalidNetworkAclId.Malformed ("Ensure that you provide the ID in the form
// acl-xxxxxxxxxxxxxxxxx") and InvalidNetworkAclID.NotFound — AWS's casing differs between the
// two, as it does for VPCs.
var ec2NetworkACLIDKind = ec2IDKind{
	Prefix: "acl-", NotFound: "InvalidNetworkAclID.NotFound",
	Malformed: "InvalidNetworkAclId.Malformed", Noun: "network ACL",
}

// ec2NetworkACLAssociationIDKind is the network ACL association ID family. The error
// reference's InvalidAssociationID.NotFound covers it — "The specified association ID (for an
// Elastic IP address, a route table, or network ACL) does not exist" — and it publishes no
// Malformed variant, so a malformed ID is reported as NotFound.
var ec2NetworkACLAssociationIDKind = ec2IDKind{
	Prefix: "aclassoc-", NotFound: "InvalidAssociationID.NotFound", Noun: "association",
}

// generateNetworkACLID mints a network ACL ID: "acl-" and 16 hex characters.
func generateNetworkACLID(m *IDMint) string {
	return "acl-" + m.Hex(8)
}

// generateNetworkACLAssociationID mints a network ACL association ID: "aclassoc-" and 16 hex
// characters.
func generateNetworkACLAssociationID(m *IDMint) string {
	return "aclassoc-" + m.Hex(8)
}

// ec2DerivedDefaultNetworkACLID is the ID a VPC an older build wrote reads its default ACL
// under: the first draw of a mint seeded with the account, Region and VPC ID, so it is the same
// on every read and in every process.
func ec2DerivedDefaultNetworkACLID(accountID, region, vpcID string) string {
	return generateNetworkACLID(NewIDMint("ec2-default-network-acl/" + accountID + "/" + region + "/" + vpcID))
}

// ec2DerivedNetworkACLAssociationID is the ID a subnet an older build wrote reads its default
// ACL association under, derived as [ec2DerivedDefaultNetworkACLID] is, from the subnet ID.
func ec2DerivedNetworkACLAssociationID(accountID, region, subnetID string) string {
	return generateNetworkACLAssociationID(NewIDMint("ec2-network-acl-association/" + accountID + "/" + region + "/" + subnetID))
}

// ec2NetworkACLKey is the state key of a network ACL.
func ec2NetworkACLKey(accountID, region, aclID string) string {
	return ec2StatePrefix("acl", accountID, region) + aclID
}

// ec2NetworkACLAssociationKey is the state key of a subnet's network ACL association.
func ec2NetworkACLAssociationKey(accountID, region, subnetID string) string {
	return ec2StatePrefix("aclassoc", accountID, region) + subnetID
}

// ec2NetworkACLCatchAll returns the two catch-all deny rules every ACL carries, egress first.
func ec2NetworkACLCatchAll() []EC2NetworkACLEntry {
	return []EC2NetworkACLEntry{
		{RuleNumber: ec2NetworkACLDefaultRuleNumber, Egress: true, Protocol: "-1", RuleAction: "deny", CIDRBlock: "0.0.0.0/0"},
		{RuleNumber: ec2NetworkACLDefaultRuleNumber, Egress: false, Protocol: "-1", RuleAction: "deny", CIDRBlock: "0.0.0.0/0"},
	}
}

// ec2NewDefaultNetworkACL returns a VPC's default ACL: rule 100 allowing all IPv4 traffic and
// the catch-all deny, in each direction — the VPC User Guide's "Default inbound rules" and
// "Default outbound rules" tables. The IPv6 rules (101 and the ::/0 deny) are "added only if
// you create the VPC with an associated IPv6 CIDR block", which substrate does not model.
func ec2NewDefaultNetworkACL(aclID, vpcID string) EC2NetworkACL {
	allow := []EC2NetworkACLEntry{
		{RuleNumber: 100, Egress: true, Protocol: "-1", RuleAction: "allow", CIDRBlock: "0.0.0.0/0"},
		{RuleNumber: 100, Egress: false, Protocol: "-1", RuleAction: "allow", CIDRBlock: "0.0.0.0/0"},
	}
	return EC2NetworkACL{
		NetworkACLID: aclID,
		VPCID:        vpcID,
		IsDefault:    true,
		Entries:      append(allow, ec2NetworkACLCatchAll()...),
	}
}

// createDefaultNetworkACL draws the ID of vpcID's default ACL and an association ID for each of
// subnetIDs, in that order, and writes them. CreateVpc passes no subnets; the default VPC passes
// its default subnets. Both callers draw these IDs last, after every other resource they create,
// so the IDs of those resources do not depend on this one. The caller holds [EC2Plugin.sgMu].
func (p *EC2Plugin) createDefaultNetworkACL(ctx context.Context, reqCtx *RequestContext, vpcID string, subnetIDs []string) error {
	aclID := generateNetworkACLID(reqCtx.IDs)
	if err := p.putEC2Record(ctx, ec2NetworkACLKey(reqCtx.AccountID, reqCtx.Region, aclID), ec2NewDefaultNetworkACL(aclID, vpcID)); err != nil {
		return fmt.Errorf("ec2 default network ACL: %w", err)
	}
	for _, subnetID := range subnetIDs {
		assoc := EC2NetworkACLAssociation{
			AssociationID: generateNetworkACLAssociationID(reqCtx.IDs),
			NetworkACLID:  aclID,
			SubnetID:      subnetID,
		}
		if err := p.putEC2Record(ctx, ec2NetworkACLAssociationKey(reqCtx.AccountID, reqCtx.Region, subnetID), assoc); err != nil {
			return fmt.Errorf("ec2 default network ACL association: %w", err)
		}
	}
	return nil
}

// associateSubnetWithDefaultNetworkACL draws an association ID for a new subnet and associates
// it with its VPC's default ACL, under [EC2Plugin.sgMu]. It is called after the subnet's own
// draw. A subnet whose VPC does not exist — CreateSubnet does not check — is given no association
// and draws nothing, and so is one a concurrent DeleteSubnet removed after it was written, which
// the lock makes this check see.
func (p *EC2Plugin) associateSubnetWithDefaultNetworkACL(ctx context.Context, reqCtx *RequestContext, subnetID, vpcID string) error {
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	subnet, err := p.state.Get(ctx, ec2Namespace, ec2StatePrefix("subnet", reqCtx.AccountID, reqCtx.Region)+subnetID)
	if err != nil {
		return fmt.Errorf("ec2 subnet network ACL association subnet: %w", err)
	}
	if subnet == nil {
		return nil
	}
	aclID, err := p.vpcDefaultNetworkACLID(ctx, reqCtx, vpcID)
	if err != nil || aclID == "" {
		return err
	}
	assoc := EC2NetworkACLAssociation{
		AssociationID: generateNetworkACLAssociationID(reqCtx.IDs),
		NetworkACLID:  aclID,
		SubnetID:      subnetID,
	}
	if err := p.putEC2Record(ctx, ec2NetworkACLAssociationKey(reqCtx.AccountID, reqCtx.Region, subnetID), assoc); err != nil {
		return fmt.Errorf("ec2 subnet network ACL association: %w", err)
	}
	return nil
}

// vpcDefaultNetworkACLID returns vpcID's default ACL: the stored one, the derived ID for a VPC
// an older build wrote, or "" when the VPC does not exist.
func (p *EC2Plugin) vpcDefaultNetworkACLID(ctx context.Context, reqCtx *RequestContext, vpcID string) (string, error) {
	data, err := p.state.Get(ctx, ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+vpcID)
	if err != nil {
		return "", fmt.Errorf("ec2 default network ACL vpc: %w", err)
	}
	if data == nil {
		return "", nil
	}
	return ec2LookupDefaultNetworkACL(ctx, p.state, reqCtx.AccountID, reqCtx.Region, vpcID)
}

// ec2LookupDefaultNetworkACL returns vpcID's default ACL ID: the stored one, or the derived ID of
// a VPC an older build wrote. It does not check that the VPC exists. It is a free function so
// CloudFormation's VPC DefaultNetworkAcl attribute, which holds only the state, can read it.
func ec2LookupDefaultNetworkACL(ctx context.Context, state StateManager, accountID, region, vpcID string) (string, error) {
	found := ""
	if err := ec2ScanRecords(ctx, state, ec2StatePrefix("acl", accountID, region), func(_ string, acl EC2NetworkACL) {
		if acl.IsDefault && acl.VPCID == vpcID && found == "" {
			found = acl.NetworkACLID
		}
	}); err != nil {
		return "", fmt.Errorf("ec2 default network ACL: %w", err)
	}
	if found != "" {
		return found, nil
	}
	return ec2DerivedDefaultNetworkACLID(accountID, region, vpcID), nil
}

// ec2LookupSubnetNetworkACLAssociation returns the ID of subnetID's network ACL association: the
// stored one, the derived ID of a subnet an older build wrote in a VPC that exists — the
// association [EC2Plugin.loadNetworkACLScope] synthesizes for it — or "" for a subnet that has
// none. It is the value of CloudFormation's subnet NetworkAclAssociationId attribute.
func ec2LookupSubnetNetworkACLAssociation(ctx context.Context, state StateManager, accountID, region, subnetID string) (string, error) {
	data, err := state.Get(ctx, ec2Namespace, ec2NetworkACLAssociationKey(accountID, region, subnetID))
	if err != nil {
		return "", fmt.Errorf("ec2 network ACL association: %w", err)
	}
	if data == nil {
		return ec2DerivedSubnetAssociation(ctx, state, accountID, region, subnetID)
	}
	var a EC2NetworkACLAssociation
	if err := json.Unmarshal(data, &a); err != nil {
		return "", fmt.Errorf("ec2 network ACL association unmarshal: %w", err)
	}
	return a.AssociationID, nil
}

// ec2DerivedSubnetAssociation returns the derived association ID of a subnet with no stored
// association when the subnet and its VPC both exist, and "" otherwise.
func ec2DerivedSubnetAssociation(ctx context.Context, state StateManager, accountID, region, subnetID string) (string, error) {
	raw, err := state.Get(ctx, ec2Namespace, ec2StatePrefix("subnet", accountID, region)+subnetID)
	if err != nil {
		return "", fmt.Errorf("ec2 network ACL association subnet: %w", err)
	}
	if raw == nil {
		return "", nil
	}
	var subnet EC2Subnet
	if err := json.Unmarshal(raw, &subnet); err != nil {
		return "", fmt.Errorf("ec2 network ACL association subnet unmarshal: %w", err)
	}
	vpc, err := state.Get(ctx, ec2Namespace, ec2StatePrefix("vpc", accountID, region)+subnet.VPCID)
	if err != nil {
		return "", fmt.Errorf("ec2 network ACL association vpc: %w", err)
	}
	if vpc == nil {
		return "", nil
	}
	return ec2DerivedNetworkACLAssociationID(accountID, region, subnetID), nil
}

// deleteSubnetNetworkACLAssociation removes a deleted subnet's association.
func (p *EC2Plugin) deleteSubnetNetworkACLAssociation(ctx context.Context, reqCtx *RequestContext, subnetID string) error {
	if err := p.state.Delete(ctx, ec2Namespace, ec2NetworkACLAssociationKey(reqCtx.AccountID, reqCtx.Region, subnetID)); err != nil {
		return fmt.Errorf("ec2 deleteSubnet network ACL association: %w", err)
	}
	return nil
}

// vpcNetworkControlDependents reports whether vpcID holds a network ACL other than its default
// one, which DeleteVpc must refuse on, and returns the state keys of what the delete removes with
// the VPC: its default ACL, per "When you delete the VPC, it deletes the default security group,
// network ACL, and route table for the VPC", and its flow logs, per "flow logs for deleted VPCs
// are eventually automatically removed" (both API_DeleteVpc). Substrate removes them at once,
// since "eventually" names no observable delay. A VPC an older build wrote has no stored default
// ACL, so there is no ACL to remove.
//
// A subnet's flow logs are kept when the subnet is deleted: no AWS source states what happens
// to them.
func (p *EC2Plugin) vpcNetworkControlDependents(ctx context.Context, reqCtx *RequestContext, vpcID string) (bool, []string, error) {
	held := false
	var owned []string
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("acl", reqCtx.AccountID, reqCtx.Region), func(k string, acl EC2NetworkACL) {
		if acl.VPCID != vpcID {
			return
		}
		if acl.IsDefault {
			owned = append(owned, k)
			return
		}
		held = true
	}); err != nil {
		return false, nil, fmt.Errorf("ec2 deleteVpc network ACLs: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("flowlog", reqCtx.AccountID, reqCtx.Region), func(k string, fl EC2FlowLog) {
		if fl.ResourceID == vpcID {
			owned = append(owned, k)
		}
	}); err != nil {
		return false, nil, fmt.Errorf("ec2 deleteVpc flow logs: %w", err)
	}
	return held, owned, nil
}

// ec2NetworkACLScope is every network ACL and subnet association in one account and Region,
// stored and synthesized, as one read sees them.
type ec2NetworkACLScope struct {
	// acls are the ACLs, ordered by ID.
	acls []EC2NetworkACL
	// storedACL records which ACL IDs have a record in state.
	storedACL map[string]bool
	// assocs are the associations, ordered by subnet ID.
	assocs []EC2NetworkACLAssociation
	// storedAssoc records which subnets' associations have a record in state.
	storedAssoc map[string]bool
	// subnetVPC maps each subnet to its VPC.
	subnetVPC map[string]string
}

// loadNetworkACLScope reads the request's account and Region's ACLs and associations, and
// synthesizes the default ACL of each VPC that has none stored and the association of each
// subnet that has none, as the package comment describes. It makes no ID draw and writes
// nothing.
func (p *EC2Plugin) loadNetworkACLScope(ctx context.Context, reqCtx *RequestContext) (*ec2NetworkACLScope, error) {
	acct, region := reqCtx.AccountID, reqCtx.Region
	s := &ec2NetworkACLScope{storedACL: map[string]bool{}, storedAssoc: map[string]bool{}, subnetVPC: map[string]string{}}
	defaults := map[string]string{}
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("acl", acct, region), func(_ string, acl EC2NetworkACL) {
		s.acls = append(s.acls, acl)
		s.storedACL[acl.NetworkACLID] = true
		if acl.IsDefault {
			defaults[acl.VPCID] = acl.NetworkACLID
		}
	}); err != nil {
		return nil, fmt.Errorf("ec2 network ACLs: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("vpc", acct, region), func(_ string, vpc EC2VPC) {
		if _, ok := defaults[vpc.VPCID]; ok {
			return
		}
		id := ec2DerivedDefaultNetworkACLID(acct, region, vpc.VPCID)
		defaults[vpc.VPCID] = id
		s.acls = append(s.acls, ec2NewDefaultNetworkACL(id, vpc.VPCID))
	}); err != nil {
		return nil, fmt.Errorf("ec2 network ACL vpcs: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("aclassoc", acct, region), func(_ string, a EC2NetworkACLAssociation) {
		s.assocs = append(s.assocs, a)
		s.storedAssoc[a.SubnetID] = true
	}); err != nil {
		return nil, fmt.Errorf("ec2 network ACL associations: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("subnet", acct, region), func(_ string, subnet EC2Subnet) {
		s.subnetVPC[subnet.SubnetID] = subnet.VPCID
		if s.storedAssoc[subnet.SubnetID] {
			return
		}
		aclID, ok := defaults[subnet.VPCID]
		if !ok {
			return
		}
		s.assocs = append(s.assocs, EC2NetworkACLAssociation{
			AssociationID: ec2DerivedNetworkACLAssociationID(acct, region, subnet.SubnetID),
			NetworkACLID:  aclID,
			SubnetID:      subnet.SubnetID,
		})
	}); err != nil {
		return nil, fmt.Errorf("ec2 network ACL subnets: %w", err)
	}
	sort.Slice(s.acls, func(i, j int) bool { return s.acls[i].NetworkACLID < s.acls[j].NetworkACLID })
	sort.Slice(s.assocs, func(i, j int) bool { return s.assocs[i].SubnetID < s.assocs[j].SubnetID })
	return s, nil
}

// acl returns the ACL with the ID, or nil.
func (s *ec2NetworkACLScope) acl(id string) *EC2NetworkACL {
	for i := range s.acls {
		if s.acls[i].NetworkACLID == id {
			return &s.acls[i]
		}
	}
	return nil
}

// associationByID returns the association with the ID, or nil.
func (s *ec2NetworkACLScope) associationByID(id string) *EC2NetworkACLAssociation {
	for i := range s.assocs {
		if s.assocs[i].AssociationID == id {
			return &s.assocs[i]
		}
	}
	return nil
}

// associationsOf returns the associations of the ACL with the ID, ordered by subnet.
func (s *ec2NetworkACLScope) associationsOf(id string) []EC2NetworkACLAssociation {
	var out []EC2NetworkACLAssociation
	for _, a := range s.assocs {
		if a.NetworkACLID == id {
			out = append(out, a)
		}
	}
	return out
}

// putNetworkACL writes acl, which persists a synthesized ACL under its derived ID.
func (p *EC2Plugin) putNetworkACL(ctx context.Context, reqCtx *RequestContext, acl EC2NetworkACL) error {
	if err := p.putEC2Record(ctx, ec2NetworkACLKey(reqCtx.AccountID, reqCtx.Region, acl.NetworkACLID), acl); err != nil {
		return fmt.Errorf("ec2 network ACL: %w", err)
	}
	return nil
}

// syntheticNetworkACLs returns the synthesized ACLs among ids — those with no record in state —
// so CreateTags, whose writer changes only a stored record, can persist them first. The caller
// holds [EC2Plugin.sgMu].
func (p *EC2Plugin) syntheticNetworkACLs(reqCtx *RequestContext, ids []string) ([]EC2NetworkACL, error) {
	var named []string
	for _, id := range ids {
		if strings.HasPrefix(id, ec2NetworkACLIDKind.Prefix) {
			named = append(named, id)
		}
	}
	if len(named) == 0 {
		return nil, nil
	}
	s, err := p.loadNetworkACLScope(context.Background(), reqCtx)
	if err != nil {
		return nil, err
	}
	var out []EC2NetworkACL
	for _, id := range named {
		if acl := s.acl(id); acl != nil && !s.storedACL[id] {
			out = append(out, *acl)
		}
	}
	return out, nil
}

// ec2TagWriteTakesSGMu reports whether id names a record that operations other than tagging
// rewrite or delete under [EC2Plugin.sgMu]: a VPC (AssociateDhcpOptions, ModifyVpcAttribute,
// DeleteVpc), a subnet (DeleteSubnet), a security group (its rule changes and delete), a network
// ACL (its entries and delete), a DHCP option set and a flow log (their deletes), and a security
// group rule, whose tags live in its group's record (#1490). A tag write
// naming one takes sgMu as well, so neither write can overwrite the other's change with a stale
// copy, and a tag write cannot write back a record a delete has just removed.
func ec2TagWriteTakesSGMu(id string) bool {
	prefix, _, ok := strings.Cut(id, "-")
	if !ok {
		return false
	}
	switch prefix {
	case "vpc", "subnet", "sg", "sgr", "acl", "dopt", "fl":
		return true
	default:
		return false
	}
}

// lockTagWrite takes the locks a tag write naming ids needs — [EC2Plugin.sgMu] when any ID is
// one [ec2TagWriteTakesSGMu] names, then [EC2Plugin.tagMu] — and returns the function that
// releases them. sgMu is taken first, the order every holder of both follows.
func (p *EC2Plugin) lockTagWrite(ids []string) func() {
	needSG := false
	for _, id := range ids {
		needSG = needSG || ec2TagWriteTakesSGMu(id)
	}
	if needSG {
		p.sgMu.Lock()
	}
	p.tagMu.Lock()
	return func() {
		p.tagMu.Unlock()
		if needSG {
			p.sgMu.Unlock()
		}
	}
}

// --- Rendering ---

// ec2NetworkACLItem renders a network ACL as AWS's NetworkAcl element, in the member order
// AWS's DescribeNetworkAcls sample shows. associationSet and tagSet are present when empty, as
// that sample renders them (`<associationSet/>`, `<tagSet/>`).
type ec2NetworkACLItem struct {
	// NetworkACLID is the ACL's ID.
	NetworkACLID string `xml:"networkAclId"`

	// VpcID is the ACL's VPC.
	VpcID string `xml:"vpcId"`

	// OwnerID is the account that owns the ACL.
	OwnerID string `xml:"ownerId"`

	// IsDefault reports whether this is the VPC's default ACL.
	IsDefault bool `xml:"default"`

	// Entries are the rules, egress first, each direction by rule number.
	Entries []ec2NetworkACLEntryItem `xml:"entrySet>item"`

	// Associations are the subnet associations.
	Associations ec2NetworkACLAssociationSet `xml:"associationSet"`

	// Tags are the ACL's tags.
	Tags ec2TagSetXML `xml:"tagSet"`
}

// ec2NetworkACLAssociationSet is the present-even-when-empty associationSet element.
type ec2NetworkACLAssociationSet struct {
	// Items are the associations.
	Items []ec2NetworkACLAssociationItem `xml:"item"`
}

// ec2NetworkACLAssociationItem renders one NetworkAclAssociation. Its members mirror
// [EC2NetworkACLAssociation]'s, so one converts to the other.
type ec2NetworkACLAssociationItem struct {
	// AssociationID is the aclassoc- ID.
	AssociationID string `xml:"networkAclAssociationId"`

	// NetworkACLID is the ACL.
	NetworkACLID string `xml:"networkAclId"`

	// SubnetID is the subnet.
	SubnetID string `xml:"subnetId"`
}

// ec2NetworkACLEntryItem renders one NetworkAclEntry, in the member order AWS's sample shows.
type ec2NetworkACLEntryItem struct {
	// RuleNumber is the rule number.
	RuleNumber int `xml:"ruleNumber"`

	// Protocol is the protocol number.
	Protocol string `xml:"protocol"`

	// RuleAction is allow or deny.
	RuleAction string `xml:"ruleAction"`

	// Egress is true for an outbound rule.
	Egress bool `xml:"egress"`

	// CIDRBlock is the IPv4 range.
	CIDRBlock string `xml:"cidrBlock,omitempty"`

	// IPv6CIDRBlock is the IPv6 range.
	IPv6CIDRBlock string `xml:"ipv6CidrBlock,omitempty"`

	// ICMPTypeCode is the ICMP type and code, absent for a non-ICMP rule.
	ICMPTypeCode *ec2NetworkACLICMPItem `xml:"icmpTypeCode,omitempty"`

	// PortRange is the port range, absent for a rule that is not TCP or UDP.
	PortRange *ec2NetworkACLPortRangeItem `xml:"portRange,omitempty"`
}

// ec2NetworkACLICMPItem renders an IcmpTypeCode.
type ec2NetworkACLICMPItem struct {
	// Code is the ICMP code.
	Code int `xml:"code"`

	// Type is the ICMP type.
	Type int `xml:"type"`
}

// ec2NetworkACLPortRangeItem renders a PortRange.
type ec2NetworkACLPortRangeItem struct {
	// From is the first port.
	From int `xml:"from"`

	// To is the last port.
	To int `xml:"to"`
}

// ec2SortNetworkACLEntries orders entries as AWS's sample does: egress before ingress, and each
// direction by rule number.
func ec2SortNetworkACLEntries(entries []EC2NetworkACLEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Egress != entries[j].Egress {
			return entries[i].Egress
		}
		return entries[i].RuleNumber < entries[j].RuleNumber
	})
}

// ec2NetworkACLXML renders acl with its associations.
func ec2NetworkACLXML(acl EC2NetworkACL, owner string, assocs []EC2NetworkACLAssociation) ec2NetworkACLItem {
	item := ec2NetworkACLItem{
		NetworkACLID: acl.NetworkACLID,
		VpcID:        acl.VPCID,
		OwnerID:      owner,
		IsDefault:    acl.IsDefault,
		Tags:         ec2TagSetXML{Items: ec2TagItems(acl.Tags)},
	}
	entries := append([]EC2NetworkACLEntry(nil), acl.Entries...)
	ec2SortNetworkACLEntries(entries)
	for _, e := range entries {
		ei := ec2NetworkACLEntryItem{
			RuleNumber: e.RuleNumber, Protocol: e.Protocol, RuleAction: e.RuleAction, Egress: e.Egress,
			CIDRBlock: e.CIDRBlock, IPv6CIDRBlock: e.IPv6CIDRBlock,
		}
		if e.ICMPTypeCode != nil {
			ei.ICMPTypeCode = &ec2NetworkACLICMPItem{Code: e.ICMPTypeCode.Code, Type: e.ICMPTypeCode.Type}
		}
		if e.PortRange != nil {
			ei.PortRange = &ec2NetworkACLPortRangeItem{From: e.PortRange.From, To: e.PortRange.To}
		}
		item.Entries = append(item.Entries, ei)
	}
	for _, a := range assocs {
		item.Associations.Items = append(item.Associations.Items, ec2NetworkACLAssociationItem(a))
	}
	return item
}

// --- Operations ---

// createNetworkACL handles CreateNetworkAcl: a new ACL in VpcId holding only the catch-all
// denies — a custom ACL "denies all inbound and outbound traffic until you add rules" (VPC User
// Guide, "Custom network ACLs"). TagSpecification.N scoped to network-acl is recorded. A
// ClientToken is echoed, not used to deduplicate.
func (p *EC2Plugin) createNetworkACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	tags := ec2TagSpecificationTags(req.Params, "", "network-acl")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	data, err := p.state.Get(ctx, ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+vpcID)
	if err != nil {
		return nil, fmt.Errorf("ec2 createNetworkAcl vpc: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VPCIDKind, "VpcId", vpcID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	acl := EC2NetworkACL{
		NetworkACLID: generateNetworkACLID(reqCtx.IDs),
		VPCID:        vpcID,
		Entries:      ec2NetworkACLCatchAll(),
		Tags:         tags,
	}
	if err := p.putNetworkACL(ctx, reqCtx, acl); err != nil {
		return nil, err
	}
	type response struct {
		XMLName     xml.Name          `xml:"CreateNetworkAclResponse"`
		XMLNS       string            `xml:"xmlns,attr"`
		NetworkACL  ec2NetworkACLItem `xml:"networkAcl"`
		ClientToken string            `xml:"clientToken,omitempty"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:       ec2XMLNS,
		NetworkACL:  ec2NetworkACLXML(acl, reqCtx.AccountID, nil),
		ClientToken: req.Params["ClientToken"],
	})
}

// ec2XMLNS is the namespace every EC2 response document carries.
const ec2XMLNS = "http://ec2.amazonaws.com/doc/2016-11-15/"

// ec2ReturnTrue answers an EC2 operation whose response is `<return>true</return>`.
func ec2ReturnTrue(operation string) (*AWSResponse, error) {
	type response struct {
		XMLName xml.Name
		XMLNS   string `xml:"xmlns,attr"`
		Return  bool   `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLName: xml.Name{Local: operation + "Response"}, XMLNS: ec2XMLNS, Return: true})
}

// deleteNetworkACL handles DeleteNetworkAcl: "You can't delete the ACL if it's associated with
// any subnets. You can't delete the default network ACL."
//
// The default ACL is refused with InvalidParameterValue / "cannot delete default network ACL
// acl-…", EC2's observed answer (Stack Overflow 26340690, a DeleteNetworkAcl on a default VPC's
// ACL). An associated ACL is refused with DependencyViolation, worded as EC2 words the same
// refusal for its other resource types — "The <type> '<id>' has dependencies and cannot be
// deleted." (see [ec2SubnetDependencyViolationError]); no source shows the networkAcl form
// itself.
func (p *EC2Plugin) deleteNetworkACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	aclID := req.Params["NetworkAclId"]
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	s, err := p.loadNetworkACLScope(ctx, reqCtx)
	if err != nil {
		return nil, err
	}
	acl := s.acl(aclID)
	if reqErr := ec2RequireNamedResource(ec2NetworkACLIDKind, "NetworkAclId", aclID, acl != nil); reqErr != nil {
		return nil, reqErr
	}
	if acl.IsDefault {
		return nil, &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "cannot delete default network ACL " + aclID,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if len(s.associationsOf(aclID)) > 0 {
		return nil, &AWSError{
			Code:       "DependencyViolation",
			Message:    "The networkAcl '" + aclID + "' has dependencies and cannot be deleted.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err := p.state.Delete(ctx, ec2Namespace, ec2NetworkACLKey(reqCtx.AccountID, reqCtx.Region, aclID)); err != nil {
		return nil, fmt.Errorf("ec2 deleteNetworkAcl: %w", err)
	}
	return ec2ReturnTrue("DeleteNetworkAcl")
}

// ec2NetworkACLProtocolNames maps the protocol names a caller may send, and a filter may name
// ("tcp | udp | icmp or a protocol number"), to the numbers AWS stores and renders.
var ec2NetworkACLProtocolNames = map[string]string{
	"all": "-1", "tcp": "6", "udp": "17", "icmp": "1", "icmpv6": "58",
}

// ec2NetworkACLProtocol normalizes a Protocol parameter to its number: a name in
// [ec2NetworkACLProtocolNames], -1, or an IANA number from 0 to 255. Anything else is
// InvalidParameterValue; the message is substrate's own.
func ec2NetworkACLProtocol(raw string) (string, *AWSError) {
	if n, ok := ec2NetworkACLProtocolNames[strings.ToLower(raw)]; ok {
		return n, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < -1 || n > 255 {
		return "", ec2InvalidParameterValue("Protocol", raw, "Specify a protocol number from 0 to 255, or -1 for all protocols.")
	}
	return strconv.Itoa(n), nil
}

// ec2NetworkACLRuleKey reads RuleNumber and Egress, the two members that name an entry. Both are
// required; a rule number outside 1–32766 is refused, per "Positive integer from 1 to 32766".
// The messages are substrate's own.
func ec2NetworkACLRuleKey(params map[string]string) (int, bool, *AWSError) {
	rawRule, ok := params["RuleNumber"]
	if !ok || rawRule == "" {
		return 0, false, ec2MissingParameter("RuleNumber")
	}
	rule, err := strconv.Atoi(rawRule)
	if err != nil || rule < 1 || rule > ec2NetworkACLMaxRuleNumber {
		return 0, false, ec2InvalidParameterValue("RuleNumber", rawRule, "It must be an integer from 1 to 32766.")
	}
	rawEgress, ok := params["Egress"]
	if !ok || rawEgress == "" {
		return 0, false, ec2MissingParameter("Egress")
	}
	egress, err := strconv.ParseBool(rawEgress)
	if err != nil {
		return 0, false, ec2InvalidParameterValue("Egress", rawEgress, "It must be true or false.")
	}
	return rule, egress, nil
}

// ec2ParseNetworkACLEntry reads the entry CreateNetworkAclEntry and ReplaceNetworkAclEntry
// carry.
//
// What is stored follows the parameter documentation: a port range only for TCP (6) and UDP
// (17), where it is "Required", and an ICMP type and code only for ICMP (1) and ICMPv6 (58),
// where it is "Required if specifying protocol 1 (ICMP) or protocol 58 (ICMPv6) with an IPv6
// CIDR block". For any other protocol "traffic on all ports is allowed, regardless of any ports
// or ICMP types or codes that you specify", so a range or type sent with it is not kept — which
// is also what a reader sees on AWS, where a -1 rule "doesn't store ports" (the Terraform AWS
// provider's aws_network_acl). Exactly one of CidrBlock and Ipv6CidrBlock is required, and the
// block is canonicalised ("if you specify 100.68.0.18/18, we modify it to 100.68.0.0/18").
// Refusal messages are substrate's own.
func ec2ParseNetworkACLEntry(params map[string]string) (EC2NetworkACLEntry, *AWSError) {
	rule, egress, awsErr := ec2NetworkACLRuleKey(params)
	if awsErr != nil {
		return EC2NetworkACLEntry{}, awsErr
	}
	rawProtocol := params["Protocol"]
	if rawProtocol == "" {
		return EC2NetworkACLEntry{}, ec2MissingParameter("Protocol")
	}
	protocol, awsErr := ec2NetworkACLProtocol(rawProtocol)
	if awsErr != nil {
		return EC2NetworkACLEntry{}, awsErr
	}
	action := params["RuleAction"]
	switch action {
	case "allow", "deny":
	case "":
		return EC2NetworkACLEntry{}, ec2MissingParameter("RuleAction")
	default:
		return EC2NetworkACLEntry{}, ec2InvalidParameterValue("RuleAction", action, "It must be allow or deny.")
	}
	entry := EC2NetworkACLEntry{RuleNumber: rule, Egress: egress, Protocol: protocol, RuleAction: action}
	v4, v6 := params["CidrBlock"], params["Ipv6CidrBlock"]
	switch {
	case v4 == "" && v6 == "":
		return EC2NetworkACLEntry{}, ec2MissingParameter("CidrBlock")
	case v4 != "" && v6 != "":
		return EC2NetworkACLEntry{}, &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "Specify either CidrBlock or Ipv6CidrBlock, not both.",
			HTTPStatus: http.StatusBadRequest,
		}
	case v4 != "":
		canonical, err := ec2CanonicalRuleCIDR(v4, false)
		if err != nil {
			return EC2NetworkACLEntry{}, ec2MalformedCIDRError(v4)
		}
		entry.CIDRBlock = canonical
	default:
		canonical, err := ec2CanonicalRuleCIDR(v6, true)
		if err != nil {
			return EC2NetworkACLEntry{}, ec2MalformedCIDRError(v6)
		}
		entry.IPv6CIDRBlock = canonical
	}
	switch protocol {
	case "6", "17":
		from, to, ok, awsErr := ec2NetworkACLIntPair(params, "PortRange.From", "PortRange.To", 0, 65535)
		if awsErr != nil {
			return EC2NetworkACLEntry{}, awsErr
		}
		if !ok {
			return EC2NetworkACLEntry{}, ec2MissingParameter("PortRange")
		}
		if from > to {
			return EC2NetworkACLEntry{}, ec2InvalidParameterValue("PortRange.From", strconv.Itoa(from), "It must not be greater than PortRange.To.")
		}
		entry.PortRange = &EC2NetworkACLPortRange{From: from, To: to}
	case "1", "58":
		typ, code, ok, awsErr := ec2NetworkACLIntPair(params, "Icmp.Type", "Icmp.Code", -1, 255)
		if awsErr != nil {
			return EC2NetworkACLEntry{}, awsErr
		}
		switch {
		case ok:
			entry.ICMPTypeCode = &EC2NetworkACLICMPTypeCode{Type: typ, Code: code}
		case protocol == "1" || entry.IPv6CIDRBlock != "":
			return EC2NetworkACLEntry{}, ec2MissingParameter("Icmp")
		}
	}
	return entry, nil
}

// ec2NetworkACLIntPair reads two integer members, each within [lo, hi]. ok is false when neither
// is present; one without the other is refused.
func ec2NetworkACLIntPair(params map[string]string, first, second string, lo, hi int) (int, int, bool, *AWSError) {
	a, aOK := params[first]
	b, bOK := params[second]
	if !aOK && !bOK {
		return 0, 0, false, nil
	}
	if !aOK {
		return 0, 0, false, ec2MissingParameter(first)
	}
	if !bOK {
		return 0, 0, false, ec2MissingParameter(second)
	}
	x, errA := strconv.Atoi(a)
	if errA != nil || x < lo || x > hi {
		return 0, 0, false, ec2InvalidParameterValue(first, a, "It must be an integer from "+strconv.Itoa(lo)+" to "+strconv.Itoa(hi)+".")
	}
	y, errB := strconv.Atoi(b)
	if errB != nil || y < lo || y > hi {
		return 0, 0, false, ec2InvalidParameterValue(second, b, "It must be an integer from "+strconv.Itoa(lo)+" to "+strconv.Itoa(hi)+".")
	}
	return x, y, true, nil
}

// ec2SameNetworkACLEntry reports whether two entries carry identical attributes.
func ec2SameNetworkACLEntry(a, b EC2NetworkACLEntry) bool {
	if a.RuleNumber != b.RuleNumber || a.Egress != b.Egress || a.Protocol != b.Protocol ||
		a.RuleAction != b.RuleAction || a.CIDRBlock != b.CIDRBlock || a.IPv6CIDRBlock != b.IPv6CIDRBlock {
		return false
	}
	if (a.PortRange == nil) != (b.PortRange == nil) || (a.PortRange != nil && *a.PortRange != *b.PortRange) {
		return false
	}
	return (a.ICMPTypeCode == nil) == (b.ICMPTypeCode == nil) &&
		(a.ICMPTypeCode == nil || *a.ICMPTypeCode == *b.ICMPTypeCode)
}

// ec2NetworkACLEntryIndex returns the index of the entry with rule number and direction, or -1.
func ec2NetworkACLEntryIndex(entries []EC2NetworkACLEntry, rule int, egress bool) int {
	for i, e := range entries {
		if e.RuleNumber == rule && e.Egress == egress {
			return i
		}
	}
	return -1
}

// ec2NetworkACLEntryNotFound is InvalidNetworkAclEntry.NotFound, with the error reference's
// gloss for the code as its message.
func ec2NetworkACLEntryNotFound() *AWSError {
	return &AWSError{
		Code:       "InvalidNetworkAclEntry.NotFound",
		Message:    "The specified network ACL entry does not exist.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// changeNetworkACL loads the ACL a request's NetworkAclId names, under [EC2Plugin.sgMu], lets
// change rewrite its entries, and writes it back — persisting a synthesized default ACL under
// its derived ID. change returns write=false to leave the record as it is.
func (p *EC2Plugin) changeNetworkACL(
	reqCtx *RequestContext, req *AWSRequest, change func(acl *EC2NetworkACL) (write bool, awsErr *AWSError),
) error {
	aclID := req.Params["NetworkAclId"]
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	s, err := p.loadNetworkACLScope(ctx, reqCtx)
	if err != nil {
		return err
	}
	acl := s.acl(aclID)
	if reqErr := ec2RequireNamedResource(ec2NetworkACLIDKind, "NetworkAclId", aclID, acl != nil); reqErr != nil {
		return reqErr
	}
	write, awsErr := change(acl)
	if awsErr != nil {
		return awsErr
	}
	if !write {
		return nil
	}
	return p.putNetworkACL(ctx, reqCtx, *acl)
}

// createNetworkACLEntry handles CreateNetworkAclEntry.
//
// A rule number the direction already holds is NetworkAclEntryAlreadyExists, the error
// reference's code and gloss — unless the stored entry is identical, which succeeds without a
// change ("CreateNetworkAclEntry succeeds if there is an existing rule with identical
// attributes", the Terraform AWS provider's aws_network_acl_rule). A direction already holding
// [ec2NetworkACLRulesPerDirection] rules is NetworkAclEntryLimitExceeded, also the reference's.
func (p *EC2Plugin) createNetworkACLEntry(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	entry, awsErr := ec2ParseNetworkACLEntry(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	err := p.changeNetworkACL(reqCtx, req, func(acl *EC2NetworkACL) (bool, *AWSError) {
		if i := ec2NetworkACLEntryIndex(acl.Entries, entry.RuleNumber, entry.Egress); i >= 0 {
			if ec2SameNetworkACLEntry(acl.Entries[i], entry) {
				return false, nil
			}
			return false, &AWSError{
				Code:       "NetworkAclEntryAlreadyExists",
				Message:    "The specified rule number already exists in this network ACL.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		held := 0
		for _, e := range acl.Entries {
			if e.Egress == entry.Egress && e.RuleNumber <= ec2NetworkACLMaxRuleNumber {
				held++
			}
		}
		if held >= ec2NetworkACLRulesPerDirection {
			return false, &AWSError{
				Code:       "NetworkAclEntryLimitExceeded",
				Message:    "You've reached the limit on the number of rules that you can add to the network ACL.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		acl.Entries = append(acl.Entries, entry)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return ec2ReturnTrue("CreateNetworkAclEntry")
}

// replaceNetworkACLEntry handles ReplaceNetworkAclEntry: the entry with the rule number and
// direction is replaced whole, and a rule number the direction does not hold is
// InvalidNetworkAclEntry.NotFound.
func (p *EC2Plugin) replaceNetworkACLEntry(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	entry, awsErr := ec2ParseNetworkACLEntry(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	err := p.changeNetworkACL(reqCtx, req, func(acl *EC2NetworkACL) (bool, *AWSError) {
		i := ec2NetworkACLEntryIndex(acl.Entries, entry.RuleNumber, entry.Egress)
		if i < 0 {
			return false, ec2NetworkACLEntryNotFound()
		}
		acl.Entries[i] = entry
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return ec2ReturnTrue("ReplaceNetworkAclEntry")
}

// deleteNetworkACLEntry handles DeleteNetworkAclEntry: "Deletes the specified ingress or egress
// entry (rule)". A rule number the direction does not hold is InvalidNetworkAclEntry.NotFound,
// the code the Terraform AWS provider's aws_network_acl_rule treats as already deleted. The
// catch-all deny cannot be named, since 32767 is outside the range a caller may send.
func (p *EC2Plugin) deleteNetworkACLEntry(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rule, egress, awsErr := ec2NetworkACLRuleKey(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	err := p.changeNetworkACL(reqCtx, req, func(acl *EC2NetworkACL) (bool, *AWSError) {
		i := ec2NetworkACLEntryIndex(acl.Entries, rule, egress)
		if i < 0 {
			return false, ec2NetworkACLEntryNotFound()
		}
		acl.Entries = append(acl.Entries[:i], acl.Entries[i+1:]...)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return ec2ReturnTrue("DeleteNetworkAclEntry")
}

// replaceNetworkACLAssociation handles ReplaceNetworkAclAssociation: "Changes which network ACL
// a subnet is associated with." The subnet is named by its current AssociationId, and the
// response carries a newly minted one, as AWS's does (newAssociationId).
//
// An AssociationId naming nothing is InvalidAssociationID.NotFound and a NetworkAclId naming
// nothing is InvalidNetworkAclID.NotFound. An ACL in a different VPC from the subnet is refused
// with InvalidParameterValue; the message is substrate's own. A synthesized target ACL is
// persisted, so the association names a stored record.
func (p *EC2Plugin) replaceNetworkACLAssociation(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	assocID, aclID := req.Params["AssociationId"], req.Params["NetworkAclId"]
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	s, err := p.loadNetworkACLScope(ctx, reqCtx)
	if err != nil {
		return nil, err
	}
	assoc := s.associationByID(assocID)
	if reqErr := ec2RequireNamedResource(ec2NetworkACLAssociationIDKind, "AssociationId", assocID, assoc != nil); reqErr != nil {
		return nil, reqErr
	}
	acl := s.acl(aclID)
	if reqErr := ec2RequireNamedResource(ec2NetworkACLIDKind, "NetworkAclId", aclID, acl != nil); reqErr != nil {
		return nil, reqErr
	}
	if vpc := s.subnetVPC[assoc.SubnetID]; vpc != acl.VPCID {
		return nil, ec2InvalidParameterValue("NetworkAclId", aclID, "The network ACL and subnet "+assoc.SubnetID+" belong to different VPCs.")
	}
	if !s.storedACL[aclID] {
		if err := p.putNetworkACL(ctx, reqCtx, *acl); err != nil {
			return nil, err
		}
	}
	replaced := EC2NetworkACLAssociation{
		AssociationID: generateNetworkACLAssociationID(reqCtx.IDs),
		NetworkACLID:  aclID,
		SubnetID:      assoc.SubnetID,
	}
	if err := p.putEC2Record(ctx, ec2NetworkACLAssociationKey(reqCtx.AccountID, reqCtx.Region, assoc.SubnetID), replaced); err != nil {
		return nil, fmt.Errorf("ec2 replaceNetworkAclAssociation: %w", err)
	}
	type response struct {
		XMLName          xml.Name `xml:"ReplaceNetworkAclAssociationResponse"`
		XMLNS            string   `xml:"xmlns,attr"`
		NewAssociationID string   `xml:"newAssociationId"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: ec2XMLNS, NewAssociationID: replaced.AssociationID})
}

// ec2NetworkACLFilterSpec is DescribeNetworkAcls' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeNetworkAcls.html. Every
// documented name is evaluated.
func ec2NetworkACLFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"association.association-id", "association.network-acl-id", "association.subnet-id",
			"default", "entry.cidr", "entry.icmp.code", "entry.icmp.type", "entry.ipv6-cidr",
			"entry.port-range.from", "entry.port-range.to", "entry.protocol", "entry.rule-action",
			"entry.egress", "entry.rule-number", "network-acl-id", "owner-id", "tag-key", "vpc-id",
		},
	}
}

// ec2NetworkACLMatchesFilter evaluates one DescribeNetworkAcls filter.
//
// An association.* filter matches when any association does, and an entry.* filter when any
// entry does, each filter independently — so entry.egress=true and entry.rule-number=100 select
// an ACL holding an egress rule and a rule numbered 100, not necessarily one rule that is both,
// which is how a filter over a list member reads elsewhere in EC2. entry.protocol values may be
// names ("tcp | udp | icmp or a protocol number"), and are compared as the numbers stored.
func ec2NetworkACLMatchesFilter(acl EC2NetworkACL, owner string, assocs []EC2NetworkACLAssociation, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(acl.Tags, name, values); isTag {
		return matched
	}
	anyAssoc := func(get func(EC2NetworkACLAssociation) string) bool {
		for _, a := range assocs {
			if ec2FilterAccepts(values, get(a)) {
				return true
			}
		}
		return false
	}
	anyEntry := func(get func(EC2NetworkACLEntry) (string, bool)) bool {
		for _, e := range acl.Entries {
			if v, ok := get(e); ok && ec2FilterAccepts(values, v) {
				return true
			}
		}
		return false
	}
	switch name {
	case "association.association-id":
		return anyAssoc(func(a EC2NetworkACLAssociation) string { return a.AssociationID })
	case "association.network-acl-id":
		return anyAssoc(func(a EC2NetworkACLAssociation) string { return a.NetworkACLID })
	case "association.subnet-id":
		return anyAssoc(func(a EC2NetworkACLAssociation) string { return a.SubnetID })
	case "default":
		return ec2FilterAccepts(values, strconv.FormatBool(acl.IsDefault))
	case "entry.cidr":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) { return e.CIDRBlock, e.CIDRBlock != "" })
	case "entry.ipv6-cidr":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) { return e.IPv6CIDRBlock, e.IPv6CIDRBlock != "" })
	case "entry.icmp.code":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) {
			if e.ICMPTypeCode == nil {
				return "", false
			}
			return strconv.Itoa(e.ICMPTypeCode.Code), true
		})
	case "entry.icmp.type":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) {
			if e.ICMPTypeCode == nil {
				return "", false
			}
			return strconv.Itoa(e.ICMPTypeCode.Type), true
		})
	case "entry.port-range.from":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) {
			if e.PortRange == nil {
				return "", false
			}
			return strconv.Itoa(e.PortRange.From), true
		})
	case "entry.port-range.to":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) {
			if e.PortRange == nil {
				return "", false
			}
			return strconv.Itoa(e.PortRange.To), true
		})
	case "entry.protocol":
		normalized := make([]string, len(values))
		for i, v := range values {
			normalized[i] = v
			if n, ok := ec2NetworkACLProtocolNames[strings.ToLower(v)]; ok {
				normalized[i] = n
			}
		}
		for _, e := range acl.Entries {
			if ec2FilterAccepts(normalized, e.Protocol) {
				return true
			}
		}
		return false
	case "entry.rule-action":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) { return e.RuleAction, true })
	case "entry.egress":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) { return strconv.FormatBool(e.Egress), true })
	case "entry.rule-number":
		return anyEntry(func(e EC2NetworkACLEntry) (string, bool) { return strconv.Itoa(e.RuleNumber), true })
	case "network-acl-id":
		return ec2FilterAccepts(values, acl.NetworkACLID)
	case "owner-id":
		return ec2FilterAccepts(values, owner)
	case "vpc-id":
		return ec2FilterAccepts(values, acl.VPCID)
	default:
		return true
	}
}

// describeNetworkACLs handles DescribeNetworkAcls: the account's ACLs in the Region, stored and
// synthesized, narrowed by NetworkAclId.N and by the filters [ec2NetworkACLFilterSpec]
// evaluates, ordered by ID and paginated on the page's published 5–1000 range, through the
// helpers every converted describe shares (see [EC2Plugin.describeVPCs]).
func (p *EC2Plugin) describeNetworkACLs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	aclIDs := extractIndexedParams(req.Params, "NetworkAclId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "NetworkAclId", aclIDs); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2MinPublishedMaxResults, ec2MaxPublishedMaxResults1000)
	if awsErr != nil {
		return nil, awsErr
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	ids := newEC2IDFilter(aclIDs, ec2NetworkACLIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2NetworkACLFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	s, err := p.loadNetworkACLScope(context.Background(), reqCtx)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName     xml.Name            `xml:"DescribeNetworkAclsResponse"`
		XMLNS       string              `xml:"xmlns,attr"`
		NetworkACLs []ec2NetworkACLItem `xml:"networkAclSet>item"`
		NextToken   string              `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: ec2XMLNS}
	for _, acl := range s.acls {
		if !ids.match(acl.NetworkACLID) {
			continue
		}
		assocs := s.associationsOf(acl.NetworkACLID)
		selected := true
		for name, values := range filters {
			if !ec2NetworkACLMatchesFilter(acl, reqCtx.AccountID, assocs, name, values) {
				selected = false
				break
			}
		}
		if selected {
			resp.NetworkACLs = append(resp.NetworkACLs, ec2NetworkACLXML(acl, reqCtx.AccountID, assocs))
		}
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	resp.NetworkACLs, resp.NextToken = ec2Page(resp.NetworkACLs, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}
