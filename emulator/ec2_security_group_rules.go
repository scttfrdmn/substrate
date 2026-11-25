package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// ec2CanonicalRuleCIDR returns a rule CIDR in the canonical form AWS stores it in (#1553).
//
// API_AuthorizeSecurityGroupIngress, on CidrIp: "AWS canonicalizes IPv4 and IPv6 CIDRs. For
// example, if you specify 100.68.0.18/18 for the CIDR block, AWS canonicalizes the CIDR block to
// 100.68.0.0/18. Any subsequent DescribeSecurityGroups and DescribeSecurityGroupRules calls will
// return the canonicalized form of the CIDR block." The canonical form is the network address the
// prefix length leaves, rendered as [netip.Prefix.String] renders it — dotted quad for IPv4, the
// compressed RFC 5952 text for IPv6 — the step [ec2ParseRouteDestination] takes for a route's
// destination (#1187).
//
// v6 says which family the parameter carries, and the family is decided by the text: CidrIp must
// be an IPv4 dotted quad and CidrIpv6 an IPv6 address that is not IPv4-mapped. So `::ffff:0:0/96`
// is refused in both: as CidrIp it would otherwise unmap to 0.0.0.0/0 and open the rule to every
// IPv4 address, and as CidrIpv6 it names IPv4 space through the other family's parameter. A value
// of the wrong family is refused like any other unparseable one. The refusal is
// InvalidParameterValue with "CIDR block <value> is malformed", EC2's observed answer to an
// unparseable rule CIDR on both families (aws/aws-cli#2846 for an IPv6 range,
// hashicorp/terraform#9481 for an IPv4 address with no prefix length, NixOS/nixops-aws#63 for an
// IPv6 range sent as CidrIp); the operation's Errors section is the Common Error Types
// boilerplate. That the IPv4-mapped form is refused rather than unmapped is substrate's reading.
func ec2CanonicalRuleCIDR(cidr string, v6 bool) (string, error) {
	prefix, err := netip.ParsePrefix(cidr)
	family := err == nil && prefix.Addr().Is4()
	if v6 {
		family = err == nil && prefix.Addr().Is6() && !prefix.Addr().Is4In6()
	}
	if !family {
		return "", ec2MalformedCIDRError(cidr)
	}
	return prefix.Masked().String(), nil
}

// ec2MalformedCIDRError is the refusal [ec2CanonicalRuleCIDR] answers.
func ec2MalformedCIDRError(cidr string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "CIDR block " + cidr + " is malformed",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CanonicalizePermissions rewrites every IPv4 and IPv6 range in perms to its canonical form,
// in place, so that the duplicate check, the revoke match and the stored record all see the form
// AWS stores (#1553). It runs on authorize and revoke alike, so a revoke naming the
// non-canonical spelling of a stored rule removes it.
//
// held, when non-nil, is the group's stored rules in the request's direction, and is passed by a
// revoke. A value that does not canonicalise but is held verbatim is kept as sent rather than
// refused: an earlier substrate stored each range as it was sent, malformed or not, and without
// this a rule it stored could never be revoked — the revoke would be refused before it was
// matched. [ec2SameRuleCIDR] then matches it by its exact text. An authorize passes nil, so a
// malformed range is never written.
func ec2CanonicalizePermissions(perms []EC2IPPermission, held []EC2IPPermission) error {
	heldVerbatim := func(cidr string, v6 bool) bool {
		for _, r := range held {
			ranges := r.IPRanges
			if v6 {
				ranges = r.IPv6Ranges
			}
			if slices.Contains(ranges, cidr) {
				return true
			}
		}
		return false
	}
	canonicalize := func(ranges []string, v6 bool) error {
		for j, cidr := range ranges {
			canonical, err := ec2CanonicalRuleCIDR(cidr, v6)
			if err != nil {
				if heldVerbatim(cidr, v6) {
					continue
				}
				return err
			}
			ranges[j] = canonical
		}
		return nil
	}
	for i := range perms {
		if err := canonicalize(perms[i].IPRanges, false); err != nil {
			return err
		}
		if err := canonicalize(perms[i].IPv6Ranges, true); err != nil {
			return err
		}
	}
	return nil
}

// ec2SameRuleCIDR reports whether a stored rule CIDR names the range target does. target is
// canonical by the time it is compared, or a held malformed value kept verbatim; the stored side
// is canonicalised here, because a group written before #1553 holds each range as it was sent, and
// its rule must still match a revoke or a duplicate authorize naming it in either spelling. A
// stored value that does not parse — which only an earlier substrate could have accepted —
// matches only its own exact text. A stored IPv4-mapped value canonicalises to IPv6 text, so it
// never matches an IPv4 target.
func ec2SameRuleCIDR(stored, target string) bool {
	if stored == target {
		return true
	}
	if prefix, err := netip.ParsePrefix(stored); err == nil {
		return prefix.Masked().String() == target
	}
	return false
}

// ec2HoldsRuleCIDR reports whether a stored range list holds cidr, compared as
// [ec2SameRuleCIDR] compares.
func ec2HoldsRuleCIDR(stored []string, cidr string) bool {
	return slices.ContainsFunc(stored, func(s string) bool { return ec2SameRuleCIDR(s, cidr) })
}

// ec2PermHasSource reports whether a permission names any source at all: an IPv4 or IPv6
// range, a prefix list, or a referenced security group.
func ec2PermHasSource(perm EC2IPPermission) bool {
	return len(perm.IPRanges) > 0 || len(perm.IPv6Ranges) > 0 ||
		len(perm.PrefixListIDs) > 0 || len(perm.UserIDGroupPairs) > 0
}

// ec2SamePortRange reports whether two permissions share protocol and port range, which is
// the half of a rule's identity a revoke must match before its sources are compared, and an
// authorize must match before it can be a duplicate. Both sides are compared in the canonical
// form [ec2CanonicalPortRange] gives, so the two operations agree on what one rule is.
func ec2SamePortRange(a, b EC2IPPermission) bool {
	return ec2CanonicalPortRange(a) == ec2CanonicalPortRange(b)
}

// ec2RulePortRange is a rule's protocol and port range in canonical form.
type ec2RulePortRange struct {
	protocol string
	from, to int
}

// ec2CanonicalPortRange returns the protocol and port range a permission means, rather than how
// the request spelled them (#1491).
//
// API_IpPermission's IpProtocol is "The IP protocol name (tcp, udp, icmp, icmpv6) or number", so a
// name and its IANA number — tcp and 6, udp and 17, icmp and 1, icmpv6 and 58 — are one protocol,
// and so are tcp and TCP: the names are compared case-insensitively, which is substrate's reading,
// since the page lists them lower-case without saying how others are treated. The same gloss says
// "specifying -1 or a protocol number other than tcp, udp, icmp, or icmpv6 allows traffic on all
// ports, regardless of any port range you specify", so for every such protocol the ports are not
// part of the rule's identity: `-1` sent with -1/-1 and `-1` sent with no ports (stored 0/0) are one
// rule. The stored permission keeps the spelling it was authorized with; only comparison is
// canonical.
func ec2CanonicalPortRange(perm EC2IPPermission) ec2RulePortRange {
	protocol := strings.ToLower(strings.TrimSpace(perm.IPProtocol))
	switch protocol {
	case "6":
		protocol = "tcp"
	case "17":
		protocol = "udp"
	case "1":
		protocol = "icmp"
	case "58":
		protocol = "icmpv6"
	}
	switch protocol {
	case "tcp", "udp", "icmp", "icmpv6":
		return ec2RulePortRange{protocol: protocol, from: perm.FromPort, to: perm.ToPort}
	default:
		return ec2RulePortRange{protocol: protocol}
	}
}

// ec2GroupPairMatches reports whether a referenced-group source a revoke names is the one a
// stored rule holds. When both carry an ID the IDs decide; otherwise the names do.
//
// Both sides normally carry an ID: [EC2Plugin.resolveGroupPairNames] fills one in for a
// name-only reference — the default-VPC form, and the legacy SourceSecurityGroupName
// parameter — on authorize and on revoke alike, so a rule authorized by name is revoked by
// ID and the other way round. The name comparison is what is left for a name that names no
// group in the VPC.
func ec2GroupPairMatches(stored, target EC2UserIDGroupPair) bool {
	if target.GroupID != "" && stored.GroupID != "" {
		return stored.GroupID == target.GroupID
	}
	return target.GroupName != "" && stored.GroupName == target.GroupName
}

// resolveGroupPairNames fills in the group ID of every name-only referenced group in perms,
// looking the name up among the groups of vpcID — "The source security group must be in the
// same VPC" (API_RevokeSecurityGroupIngress). The name is kept alongside the ID it resolved
// to. A name that resolves to no group is left as it was.
func (p *EC2Plugin) resolveGroupPairNames(reqCtx *RequestContext, vpcID string, perms []EC2IPPermission) error {
	var byName map[string]string
	for i := range perms {
		for j := range perms[i].UserIDGroupPairs {
			pair := &perms[i].UserIDGroupPairs[j]
			if pair.GroupID != "" || pair.GroupName == "" {
				continue
			}
			if byName == nil {
				var err error
				if byName, err = p.securityGroupIDsByName(reqCtx, vpcID); err != nil {
					return err
				}
			}
			pair.GroupID = byName[strings.ToLower(pair.GroupName)]
		}
	}
	return nil
}

// securityGroupIDsByName maps the lower-cased name of each group in vpcID to its ID. Names are
// unique per VPC since #1491; for state an earlier substrate wrote, which may hold two groups of
// one name, the first in key order wins.
func (p *EC2Plugin) securityGroupIDsByName(reqCtx *RequestContext, vpcID string) (map[string]string, error) {
	keys, err := p.state.List(context.Background(), ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 securityGroupIDsByName list: %w", err)
	}
	out := map[string]string{}
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg EC2SecurityGroup
		if json.Unmarshal(data, &sg) != nil || sg.VPCID != vpcID {
			continue
		}
		if _, seen := out[strings.ToLower(sg.GroupName)]; !seen {
			out[strings.ToLower(sg.GroupName)] = sg.GroupID
		}
	}
	return out, nil
}

// ec2NoGroupName stands in for the name of a group a name list cannot select, so the name half
// of [ec2IDFilter.matchOrNamed] never matches it. It is a NUL, which no group name can contain:
// API_CreateSecurityGroup's valid characters are "a-z, A-Z, 0-9, spaces, and
// ._-:/()#,@[]+=&;{}!$*", and an empty string would not do, because a request may name one.
const ec2NoGroupName = "\x00"

// refuseDuplicateGroupName returns InvalidGroup.Duplicate when a group in vpcID already holds
// name, compared case-insensitively — API_CreateSecurityGroup: "Names are case-insensitive and
// must be unique within the VPC" (#1491). The message is the client-error table's gloss for the
// code. The caller holds [EC2Plugin.sgMu].
func (p *EC2Plugin) refuseDuplicateGroupName(reqCtx *RequestContext, vpcID, name string) error {
	if name == "" {
		return nil
	}
	byName, err := p.securityGroupIDsByName(reqCtx, vpcID)
	if err != nil {
		return err
	}
	if _, taken := byName[strings.ToLower(name)]; taken {
		return &AWSError{
			Code: "InvalidGroup.Duplicate",
			Message: "You cannot create a security group with the same name as an existing " +
				"security group in the same VPC.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// defaultVPCGroupByName resolves a [Default VPC] group name, as DeleteSecurityGroup's GroupName
// is published, to the ID of the group of that name in the account's default VPC.
//
// A name that resolves to nothing — including in an account with no default VPC — is
// InvalidGroup.NotFound. It is written by hand rather than through [ec2SecurityGroupIDKind]
// because the kind's message is about an ID, and what failed here is a name; the message is
// substrate's own. The lookup is case-insensitive: "Security group names are not case-sensitive"
// (VPC User Guide, "Security group basics").
func (p *EC2Plugin) defaultVPCGroupByName(reqCtx *RequestContext, name string) (string, error) {
	vpc, _, err := ec2LookupDefaultVPC(context.Background(), p.state, reqCtx, "")
	if err != nil {
		return "", err
	}
	if vpc != nil {
		byName, listErr := p.securityGroupIDsByName(reqCtx, vpc.VPCID)
		if listErr != nil {
			return "", listErr
		}
		if id, ok := byName[strings.ToLower(name)]; ok {
			return id, nil
		}
	}
	return "", &AWSError{
		Code:       "InvalidGroup.NotFound",
		Message:    "No security group named '" + name + "' exists in the default VPC",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2AuthorizeDuplicates reports whether an authorize would add a source a group already allows,
// so the request must be refused with InvalidPermission.Duplicate.
//
// EC2 stores one rule per source — "Each rule has a protocol, from and to ports, and source"
// (API_RevokeSecurityGroupIngress) — so a duplicate is a source already held for the same
// protocol and port range, wherever in the group's stored permissions it sits, and whichever
// other sources the request's permission carries alongside it. That is the granularity
// [ec2RevokePermissions] removes at (#1453), so a rule that one revoke can remove is exactly a
// rule one authorize can duplicate. A source repeated within the request is a duplicate too.
//
// A permission naming no source at all is a duplicate of a stored sourceless permission with the
// same protocol and ports, the reading [ec2RevokePermissions] gives that shape.
func ec2AuthorizeDuplicates(stored, perms []EC2IPPermission) bool {
	held := slices.Clone(stored)
	holds := func(perm EC2IPPermission, has func(EC2IPPermission) bool) bool {
		for _, r := range held {
			if ec2SamePortRange(r, perm) && has(r) {
				return true
			}
		}
		return false
	}
	for _, perm := range perms {
		port := EC2IPPermission{IPProtocol: perm.IPProtocol, FromPort: perm.FromPort, ToPort: perm.ToPort}
		if !ec2PermHasSource(perm) {
			if holds(perm, func(r EC2IPPermission) bool { return !ec2PermHasSource(r) }) {
				return true
			}
			held = append(held, port)
			continue
		}
		for _, cidr := range perm.IPRanges {
			if holds(perm, func(r EC2IPPermission) bool { return ec2HoldsRuleCIDR(r.IPRanges, cidr) }) {
				return true
			}
			one := port
			one.IPRanges = []string{cidr}
			held = append(held, one)
		}
		for _, cidr := range perm.IPv6Ranges {
			if holds(perm, func(r EC2IPPermission) bool { return ec2HoldsRuleCIDR(r.IPv6Ranges, cidr) }) {
				return true
			}
			one := port
			one.IPv6Ranges = []string{cidr}
			held = append(held, one)
		}
		for _, id := range perm.PrefixListIDs {
			if holds(perm, func(r EC2IPPermission) bool { return slices.Contains(r.PrefixListIDs, id) }) {
				return true
			}
			one := port
			one.PrefixListIDs = []string{id}
			held = append(held, one)
		}
		for _, pair := range perm.UserIDGroupPairs {
			if holds(perm, func(r EC2IPPermission) bool {
				return slices.ContainsFunc(r.UserIDGroupPairs, func(s EC2UserIDGroupPair) bool {
					return ec2GroupPairMatches(s, pair)
				})
			}) {
				return true
			}
			one := port
			one.UserIDGroupPairs = []EC2UserIDGroupPair{pair}
			held = append(held, one)
		}
	}
	return false
}

// ec2PermissionDuplicateError is InvalidPermission.Duplicate, with the client-error table's gloss
// as its message, "The specified inbound or outbound rule already exists for that security
// group.", verbatim.
func ec2PermissionDuplicateError() *AWSError {
	return &AWSError{
		Code:       "InvalidPermission.Duplicate",
		Message:    "The specified inbound or outbound rule already exists for that security group.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2RevokePermissions subtracts the rules a RevokeSecurityGroupIngress/Egress request names
// from a group's stored rules, returning what remains and the part of the request that
// matched nothing.
//
// EC2 stores one rule per source — "Each rule has a protocol, from and to ports, and source
// (CIDR range, security group, or prefix list)" (API_RevokeSecurityGroupIngress) — and gives
// each its own security group rule ID, while substrate stores the IpPermission an authorize
// carried, which may hold several sources. Through v0.125.0 a revoke naming one of those
// sources dropped the whole stored permission, so revoking 0.0.0.0/0 from a permission that
// also held 10.1.0.0/16 removed both (#1453). Each source the request names is now removed on
// its own, from every stored permission with the same protocol and port range, and a stored
// permission is dropped only once it holds no source.
//
// A source matched by no stored rule is returned in unknown, grouped back into the
// protocol/port permission it was named under, because the caller's answer depends on it:
// see [EC2Plugin.modifySGRules].
//
// A request permission naming no source at all keeps the behavior it had before #1453: it
// removes the first stored permission with the same protocol and port range. AWS does not
// describe that shape — every rule it documents has a source — so this is substrate's
// reading, kept so a revoke that worked before does not start failing.
func ec2RevokePermissions(stored, targets []EC2IPPermission) (remaining, unknown []EC2IPPermission) {
	rules := make([]EC2IPPermission, len(stored))
	for i, r := range stored {
		rules[i] = r
		rules[i].IPRanges = slices.Clone(r.IPRanges)
		rules[i].IPv6Ranges = slices.Clone(r.IPv6Ranges)
		rules[i].PrefixListIDs = slices.Clone(r.PrefixListIDs)
		rules[i].UserIDGroupPairs = slices.Clone(r.UserIDGroupPairs)
	}
	dropped := make([]bool, len(rules))

	for _, target := range targets {
		if !ec2PermHasSource(target) {
			found := false
			for i := range rules {
				if !dropped[i] && ec2SamePortRange(rules[i], target) {
					dropped[i] = true
					found = true
					break
				}
			}
			if !found {
				unknown = append(unknown, target)
			}
			continue
		}

		missed := EC2IPPermission{IPProtocol: target.IPProtocol, FromPort: target.FromPort, ToPort: target.ToPort}
		// same compares a stored value with the request's: CIDRs canonically (#1553), so a
		// rule stored before canonicalisation still matches; prefix-list IDs exactly.
		removeString := func(get func(*EC2IPPermission) *[]string, value string, same func(stored, target string) bool) bool {
			removed := false
			for i := range rules {
				if dropped[i] || !ec2SamePortRange(rules[i], target) {
					continue
				}
				list := get(&rules[i])
				if before := len(*list); before > 0 {
					*list = slices.DeleteFunc(*list, func(v string) bool { return same(v, value) })
					removed = removed || len(*list) < before
				}
			}
			return removed
		}
		exact := func(stored, target string) bool { return stored == target }
		for _, cidr := range target.IPRanges {
			if !removeString(func(r *EC2IPPermission) *[]string { return &r.IPRanges }, cidr, ec2SameRuleCIDR) {
				missed.IPRanges = append(missed.IPRanges, cidr)
			}
		}
		for _, cidr := range target.IPv6Ranges {
			if !removeString(func(r *EC2IPPermission) *[]string { return &r.IPv6Ranges }, cidr, ec2SameRuleCIDR) {
				missed.IPv6Ranges = append(missed.IPv6Ranges, cidr)
			}
		}
		for _, id := range target.PrefixListIDs {
			if !removeString(func(r *EC2IPPermission) *[]string { return &r.PrefixListIDs }, id, exact) {
				missed.PrefixListIDs = append(missed.PrefixListIDs, id)
			}
		}
		for _, pair := range target.UserIDGroupPairs {
			removed := false
			for i := range rules {
				if dropped[i] || !ec2SamePortRange(rules[i], target) {
					continue
				}
				before := len(rules[i].UserIDGroupPairs)
				rules[i].UserIDGroupPairs = slices.DeleteFunc(rules[i].UserIDGroupPairs,
					func(stored EC2UserIDGroupPair) bool { return ec2GroupPairMatches(stored, pair) })
				removed = removed || len(rules[i].UserIDGroupPairs) < before
			}
			if !removed {
				missed.UserIDGroupPairs = append(missed.UserIDGroupPairs, pair)
			}
		}
		// A stored permission emptied by this target is gone. One that held no source to
		// begin with is untouched by a sourced revoke, so it is not swept up here.
		for i := range rules {
			if !dropped[i] && ec2SamePortRange(rules[i], target) && !ec2PermHasSource(rules[i]) &&
				ec2PermHasSource(stored[i]) {
				dropped[i] = true
			}
		}
		if ec2PermHasSource(missed) {
			unknown = append(unknown, missed)
		}
	}

	for i, r := range rules {
		if !dropped[i] {
			remaining = append(remaining, r)
		}
	}
	return remaining, unknown
}

// ec2PermissionNotFoundError is the refusal for a revoke naming a rule the group does not
// hold, in a group outside the default VPC.
//
// API_RevokeSecurityGroupIngress states the rule by VPC: "For a non-default VPC, if the
// values you specify do not match the existing rule's values, an InvalidPermission.NotFound
// client error is returned, and no rules are revoked." The message is the client-error
// table's gloss for the code: "The specified rule does not exist in this security group".
func ec2PermissionNotFoundError() *AWSError {
	return &AWSError{
		Code:       "InvalidPermission.NotFound",
		Message:    "The specified rule does not exist in this security group.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2IPRangeItem is one ipRanges>item, AWS's IpRange.
type ec2IPRangeItem struct {
	// CidrIP is the IPv4 range.
	CidrIP string `xml:"cidrIp"` //nolint:revive
	// Description is the rule's description.
	Description string `xml:"description,omitempty"`
}

// ec2IPv6RangeItem is one ipv6Ranges>item, AWS's Ipv6Range.
type ec2IPv6RangeItem struct {
	// CidrIPv6 is the IPv6 range.
	CidrIPv6 string `xml:"cidrIpv6"` //nolint:revive
	// Description is the rule's description.
	Description string `xml:"description,omitempty"`
}

// ec2PrefixListItem is one prefixListIds>item, AWS's PrefixListId.
type ec2PrefixListItem struct {
	// PrefixListID is the prefix list's ID.
	PrefixListID string `xml:"prefixListId"`
	// Description is the rule's description.
	Description string `xml:"description,omitempty"`
}

// ec2GroupPairItem is one groups>item, AWS's UserIdGroupPair.
type ec2GroupPairItem struct {
	UserID      string `xml:"userId,omitempty"`
	GroupID     string `xml:"groupId,omitempty"`
	GroupName   string `xml:"groupName,omitempty"`
	Description string `xml:"description,omitempty"`
}

// ec2IPPermissionItem is AWS's IpPermission as a response renders it.
//
// Package-level since #1453, because two responses now render one: DescribeSecurityGroups'
// ipPermissions and ipPermissionsEgress, and a revoke's unknownIpPermissionSet.
type ec2IPPermissionItem struct {
	IPProtocol    string              `xml:"ipProtocol"` //nolint:revive
	FromPort      int                 `xml:"fromPort"`
	ToPort        int                 `xml:"toPort"`
	Groups        []ec2GroupPairItem  `xml:"groups>item,omitempty"`
	IPRanges      []ec2IPRangeItem    `xml:"ipRanges>item"`             //nolint:revive
	IPv6Ranges    []ec2IPv6RangeItem  `xml:"ipv6Ranges>item,omitempty"` //nolint:revive
	PrefixListIDs []ec2PrefixListItem `xml:"prefixListIds>item,omitempty"`
}

// ec2IPPermissionXML renders one stored permission.
func ec2IPPermissionXML(rule EC2IPPermission) ec2IPPermissionItem {
	item := ec2IPPermissionItem{IPProtocol: rule.IPProtocol, FromPort: rule.FromPort, ToPort: rule.ToPort}
	for _, cidr := range rule.IPRanges {
		item.IPRanges = append(item.IPRanges, ec2IPRangeItem{CidrIP: cidr})
	}
	for _, cidr := range rule.IPv6Ranges {
		item.IPv6Ranges = append(item.IPv6Ranges, ec2IPv6RangeItem{CidrIPv6: cidr})
	}
	for _, id := range rule.PrefixListIDs {
		item.PrefixListIDs = append(item.PrefixListIDs, ec2PrefixListItem{PrefixListID: id})
	}
	for _, pair := range rule.UserIDGroupPairs {
		item.Groups = append(item.Groups, ec2GroupPairItem{
			UserID:      pair.UserID,
			GroupID:     pair.GroupID,
			GroupName:   pair.GroupName,
			Description: pair.Description,
		})
	}
	return item
}

// ec2DescribedPermissionXML renders one stored permission of sg with the description each of
// its IPv4, IPv6 and prefix-list sources carries in the group's [EC2SecurityGroup.RuleMeta]
// (#1490), as DescribeSecurityGroups' IpRange, Ipv6Range and PrefixListId shapes publish one.
func ec2DescribedPermissionXML(sg EC2SecurityGroup, rule EC2IPPermission, egress bool) ec2IPPermissionItem {
	item := ec2IPPermissionXML(rule)
	if len(sg.RuleMeta) == 0 {
		return item
	}
	describe := func(kind, value string) string {
		return sg.RuleMeta[ec2SGRuleKey(egress, rule, kind, value)].Description
	}
	for i := range item.IPRanges {
		item.IPRanges[i].Description = describe(ec2SGRuleSourceCIDR, ec2SGRuleSourceValue(rule.IPRanges[i]))
	}
	for i := range item.IPv6Ranges {
		item.IPv6Ranges[i].Description = describe(ec2SGRuleSourceIPv6, ec2SGRuleSourceValue(rule.IPv6Ranges[i]))
	}
	for i := range item.PrefixListIDs {
		item.PrefixListIDs[i].Description = describe(ec2SGRuleSourcePrefixList, rule.PrefixListIDs[i])
	}
	return item
}

// ec2SecurityGroupMatchesFilters reports whether a security group satisfies every supplied
// DescribeSecurityGroups filter.
func ec2SecurityGroupMatchesFilters(sg EC2SecurityGroup, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2SecurityGroupMatchesFilter(sg, name, values) {
			return false
		}
	}
	return true
}

// ec2SecurityGroupMatchesFilter evaluates one DescribeSecurityGroups filter.
//
// Every name [ec2SecurityGroupFilterSpec] accepts is evaluated as of #1440. Before it only
// group-id, group-name and vpc-id were, so tag:<key> and tag-key — accepted by the spec — were
// inert and answered every group, which is how a test of spawn's `tag:spawn:managed=true`
// reaper passed for any group at all.
//
// The rule filters follow the operation's own note: "If using multiple filters for rules, the
// results include security groups for which any combination of rules - not necessarily a
// single rule - match all filters." Each rule filter is therefore evaluated on its own against
// every rule in its direction, and the filters AND across the group rather than within one
// rule — which is what evaluating them independently gives.
//
// The two port filters compare the rendered integer, so `-1` and `22` match as AWS shows
// them. user-id compares the referenced group's owner; a rule naming a CIDR has none.
func ec2SecurityGroupMatchesFilter(sg EC2SecurityGroup, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(sg.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "group-id":
		return ec2FilterAccepts(values, sg.GroupID)
	case "group-name":
		return ec2FilterAccepts(values, sg.GroupName)
	case "vpc-id":
		return ec2FilterAccepts(values, sg.VPCID)
	case "description":
		return ec2FilterAccepts(values, sg.Description)
	case "owner-id":
		return ec2FilterAccepts(values, sg.AccountID)
	}
	rules := sg.IngressRules
	field, ok := strings.CutPrefix(name, "ip-permission.")
	if !ok {
		field, ok = strings.CutPrefix(name, "egress.ip-permission.")
		if !ok {
			return true
		}
		rules = sg.EgressRules
	}
	for _, rule := range rules {
		if ec2RuleMatchesFilter(rule, field, values) {
			return true
		}
	}
	return false
}

// ec2RuleMatchesFilter evaluates one rule-filter field against one stored rule.
func ec2RuleMatchesFilter(rule EC2IPPermission, field string, values []string) bool {
	anyAccepted := func(list []string) bool {
		for _, v := range list {
			if ec2FilterAccepts(values, v) {
				return true
			}
		}
		return false
	}
	switch field {
	case "cidr":
		return anyAccepted(rule.IPRanges)
	case "ipv6-cidr":
		return anyAccepted(rule.IPv6Ranges)
	case "prefix-list-id":
		return anyAccepted(rule.PrefixListIDs)
	case "protocol":
		return ec2FilterAccepts(values, rule.IPProtocol)
	case "from-port":
		return ec2FilterAccepts(values, strconv.Itoa(rule.FromPort))
	case "to-port":
		return ec2FilterAccepts(values, strconv.Itoa(rule.ToPort))
	case "group-id", "group-name", "user-id":
		for _, pair := range rule.UserIDGroupPairs {
			v := pair.GroupID
			switch field {
			case "group-name":
				v = pair.GroupName
			case "user-id":
				v = pair.UserID
			}
			if v != "" && ec2FilterAccepts(values, v) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// modifySGRules applies an Authorize/RevokeSecurityGroupIngress/Egress request.
//
// A revoke removes each named source on its own (#1453) — see [ec2RevokePermissions] — and
// answers a source that matched nothing the way API_RevokeSecurityGroupIngress says it is
// answered, which depends on the group's VPC: "For a default VPC, if the values you specify
// do not match the existing rule's values, no error is returned, and the output describes the
// security group rules that were not revoked. For a non-default VPC ... an
// InvalidPermission.NotFound client error is returned, and no rules are revoked." The
// default-VPC half renders unknownIpPermissionSet; the other refuses before anything is
// written. API_RevokeSecurityGroupEgress publishes only the default-VPC sentence; the
// non-default refusal is applied to egress too, because it is what EC2 answers there: the
// Terraform AWS provider revokes a new group's ::/0 egress rule unconditionally and tolerates
// exactly InvalidPermission.NotFound for a VPC with no IPv6 range (resourceSecurityGroupCreate,
// "If we have a NotFound or InvalidParameterValue, then we are trying to remove the default
// IPv6 egress of a non-IPv6 enabled SG") (#1658).
//
// A revoke may name rules by SecurityGroupRuleId.N instead of, or as well as, by IpPermissions.N
// (#1490). Each ID must name a rule of the group in the request's direction; one that does not
// is InvalidSecurityGroupRuleId.NotFound, and nothing is revoked. The response carries
// revokedSecurityGroupRuleSet, the rules the call removed, as API_RevokedSecurityGroupRule
// shapes them, and an authorize carries securityGroupRuleSet, the rules it added. See
// [ec2SecurityGroupRules] for how a rule and its sgr- ID are derived.
//
// An authorize naming a source the group already allows for the same protocol and ports is
// InvalidPermission.Duplicate, and nothing is written (#1491); the check is per source, the
// granularity the revoke above works at — see [ec2AuthorizeDuplicates]. An authorize records
// the IpRanges, Ipv6Ranges and PrefixListIds descriptions it carries, and the tags of a
// TagSpecification.N whose ResourceType is security-group-rule, on every rule it adds.
//
// Every IPv4 and IPv6 range is canonicalised before it is compared or stored (#1553), as the
// operation pages publish: "AWS canonicalizes IPv4 and IPv6 CIDRs" — see
// [ec2CanonicalizePermissions]. So 100.68.0.18/18 is stored and described as 100.68.0.0/18, and
// authorizing it beside a stored 100.68.0.0/18 is a duplicate.
//
// GroupName is read as both pages publish it — "[Default VPC] The name of the security group.
// For security groups for a default VPC you can specify either the ID or the name of the
// security group" — and resolved among the default VPC's groups by
// [EC2Plugin.defaultVPCGroupByName], the rule DeleteSecurityGroup follows (#1491). GroupId wins
// when both are sent.
func (p *EC2Plugin) modifySGRules(reqCtx *RequestContext, req *AWSRequest, direction string, add bool) (*AWSResponse, error) {
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	egress := direction == "egress"
	sgID := req.Params["GroupId"]
	if sgID == "" && req.Params["GroupName"] != "" {
		resolved, resolveErr := p.defaultVPCGroupByName(reqCtx, req.Params["GroupName"])
		if resolveErr != nil {
			return nil, resolveErr
		}
		sgID = resolved
	}
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifySGRules get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2SecurityGroupIDKind, "GroupId", sgID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var sg EC2SecurityGroup
	if unmarshalErr := json.Unmarshal(data, &sg); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifySGRules unmarshal: %w", unmarshalErr)
	}

	perms := parseSGPermissions(req.Params, reqCtx.AccountID)
	var ruleIDs []string
	if !add {
		ruleIDs = extractIndexedParams(req.Params, "SecurityGroupRuleId")
	}
	if len(perms) == 0 && len(ruleIDs) == 0 {
		return nil, &AWSError{
			Code:       "MissingParameter",
			Message:    "No permissions specified",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	rules := &sg.IngressRules
	if egress {
		rules = &sg.EgressRules
	}
	// A revoke may name a malformed range an earlier substrate stored verbatim; see
	// [ec2CanonicalizePermissions].
	var held []EC2IPPermission
	if !add {
		held = *rules
	}
	if err := ec2CanonicalizePermissions(perms, held); err != nil {
		return nil, err
	}
	// A group named rather than identified is resolved to its ID within the group's VPC, on
	// authorize and revoke alike, so the two forms address one rule.
	if err := p.resolveGroupPairNames(reqCtx, sg.VPCID, perms); err != nil {
		return nil, err
	}
	if add {
		if err := p.requireReferencedGroups(reqCtx, sg.VPCID, perms); err != nil {
			return nil, err
		}
	}

	before := ec2SecurityGroupRules(sg)
	var unknown []EC2IPPermission
	if add {
		if ec2AuthorizeDuplicates(*rules, perms) {
			return nil, ec2PermissionDuplicateError()
		}
		tags := ec2TagSpecificationTags(req.Params, "", "security-group-rule")
		if awsErr := ec2CheckTagRules(tags); awsErr != nil {
			return nil, awsErr
		}
		if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
			return nil, awsErr
		}
		descriptions := ec2RuleDescriptions(req.Params, perms, egress)
		*rules = append(*rules, perms...)
		ec2RecordAddedRules(&sg, ec2SGRuleMetaKeys(before), descriptions, tags)
	} else {
		sourced, sourceless, awsErr := ec2RuleIDTargets(before, ruleIDs, egress, perms)
		if awsErr != nil {
			return nil, awsErr
		}
		// The rules named by ID go first: each names one stored rule exactly, so none can be
		// missed, and no permission the request names can take it away from them first.
		for _, target := range sourceless {
			*rules, _ = ec2DropSourcelessPermission(*rules, target)
		}
		*rules, _ = ec2RevokePermissions(*rules, sourced)
		// A permission naming no source removes a stored permission naming none when the group
		// holds one, so it cannot take a sourced rule on the same ports; only when it holds none
		// does it keep the reading [ec2RevokePermissions] gives it.
		var rest []EC2IPPermission
		for _, perm := range perms {
			if !ec2PermHasSource(perm) {
				if next, dropped := ec2DropSourcelessPermission(*rules, perm); dropped {
					*rules = next
					continue
				}
			}
			rest = append(rest, perm)
		}
		*rules, unknown = ec2RevokePermissions(*rules, rest)
		if len(unknown) > 0 {
			inDefault, lookupErr := p.ec2VPCIsDefault(reqCtx, sg.VPCID)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if !inDefault {
				return nil, ec2PermissionNotFoundError()
			}
		}
	}
	after := ec2SecurityGroupRules(sg)
	ec2PruneRuleMeta(&sg)

	newData, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifySGRules marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 modifySGRules state.Put: %w", err)
	}

	opName := "AuthorizeSecurityGroupIngressResponse"
	if egress && add {
		opName = "AuthorizeSecurityGroupEgressResponse"
	} else if !egress && !add {
		opName = "RevokeSecurityGroupIngressResponse"
	} else if egress && !add {
		opName = "RevokeSecurityGroupEgressResponse"
	}
	type response struct {
		XMLName xml.Name
		XMLNS   string                 `xml:"xmlns,attr"`
		Return  bool                   `xml:"return"`
		Unknown []ec2IPPermissionItem  `xml:"unknownIpPermissionSet>item,omitempty"`
		Added   []ec2SGRuleItem        `xml:"securityGroupRuleSet>item,omitempty"`
		Revoked []ec2RevokedSGRuleItem `xml:"revokedSecurityGroupRuleSet>item,omitempty"`
	}
	resp := response{
		XMLName: xml.Name{Local: opName},
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		Return:  true,
	}
	for _, perm := range unknown {
		resp.Unknown = append(resp.Unknown, ec2IPPermissionXML(perm))
	}
	if add {
		had := ec2SGRuleMetaKeys(before)
		for _, r := range after {
			if r.Egress == egress && !had[r.Key] {
				resp.Added = append(resp.Added, ec2SGRuleXML(r, reqCtx.Region))
			}
		}
	} else {
		remain := ec2SGRuleMetaKeys(after)
		for _, r := range before {
			if r.Egress == egress && !remain[r.Key] {
				resp.Revoked = append(resp.Revoked, ec2RevokedSGRuleXML(r))
			}
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2RecordAddedRules writes the metadata of the rules an authorize added: every rule of sg
// whose key had is missing. A description is recorded for an IPv4, IPv6 or prefix-list rule
// (a referenced group's rides on its pair), and tags for every one. A rule whose ID is not the
// unsalted derivation of [ec2DerivedSGRuleID] has that ID recorded too, so it does not move
// when the rule holding the unsalted one is later revoked.
func ec2RecordAddedRules(sg *EC2SecurityGroup, had map[string]bool, descriptions map[string]string, tags []EC2Tag) {
	for _, r := range ec2SecurityGroupRules(*sg) {
		if had[r.Key] {
			continue
		}
		description := descriptions[r.Key]
		pinned := r.ID != ec2DerivedSGRuleID(sg.GroupID, r.Key, 0)
		if description == "" && len(tags) == 0 && !pinned {
			continue
		}
		ec2SetRuleMeta(sg, r.Key, func(m *EC2SecurityGroupRuleMeta) {
			if r.Kind != ec2SGRuleSourceGroup {
				m.Description = description
			}
			m.Tags = slices.Clone(tags)
			if pinned {
				m.ID = r.ID
			}
		})
	}
}

// ec2RuleIDTargets resolves a revoke's SecurityGroupRuleId.N to the permissions that revoke the
// named rules: sourced holds the single-source permissions [ec2RevokePermissions] removes, and
// sourceless the rules naming no source, which [ec2DropSourcelessPermission] removes, because a
// sourceless target given to the former drops whichever stored permission first shares its
// ports. An ID that is not of the sgr- form is InvalidSecurityGroupRuleId.Malformed; one naming
// no rule of the group in the request's direction is InvalidSecurityGroupRuleId.NotFound.
//
// A rule the request also names in perms is left out, so naming one rule both ways revokes it
// once rather than reporting the second removal as unknown.
func ec2RuleIDTargets(
	rules []ec2SGRule, ids []string, egress bool, perms []EC2IPPermission,
) (sourced, sourceless []EC2IPPermission, err error) {
	if len(ids) == 0 {
		return nil, nil, nil
	}
	filter := newEC2IDFilter(ids, ec2SecurityGroupRuleIDKind)
	if err := filter.validate(); err != nil {
		return nil, nil, err
	}
	named := map[string]bool{}
	for _, perm := range perms {
		for _, r := range ec2SplitPermission(egress, perm) {
			named[r.Key] = true
		}
	}
	for _, r := range rules {
		if r.Egress != egress || !filter.match(r.ID) || named[r.Key] {
			continue
		}
		named[r.Key] = true
		if r.Kind == ec2SGRuleSourceNone {
			sourceless = append(sourceless, r.Perm)
		} else {
			sourced = append(sourced, r.Perm)
		}
	}
	if err := filter.unresolved(); err != nil {
		return nil, nil, err
	}
	return sourced, sourceless, nil
}

// ec2DropSourcelessPermission removes from stored the first permission that names no source and
// shares target's protocol and port range — the stored permission a sourceless rule is, and no
// other — and reports whether one was removed.
func ec2DropSourcelessPermission(stored []EC2IPPermission, target EC2IPPermission) ([]EC2IPPermission, bool) {
	for i, perm := range stored {
		if !ec2PermHasSource(perm) && ec2SamePortRange(perm, target) {
			return slices.Delete(slices.Clone(stored), i, i+1), true
		}
	}
	return stored, false
}
