package emulator

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// ec2PermHasSource reports whether a permission names any source at all: an IPv4 or IPv6
// range, a prefix list, or a referenced security group.
func ec2PermHasSource(perm EC2IPPermission) bool {
	return len(perm.IPRanges) > 0 || len(perm.IPv6Ranges) > 0 ||
		len(perm.PrefixListIDs) > 0 || len(perm.UserIDGroupPairs) > 0
}

// ec2SamePortRange reports whether two permissions share protocol and port range, which is
// the half of a rule's identity a revoke must match before its sources are compared.
func ec2SamePortRange(a, b EC2IPPermission) bool {
	return a.IPProtocol == b.IPProtocol && a.FromPort == b.FromPort && a.ToPort == b.ToPort
}

// ec2GroupPairMatches reports whether a referenced-group source a revoke names is the one a
// stored rule holds. The ID decides when the revoke gives one; a name-only reference — the
// default-VPC form, and the legacy SourceSecurityGroupName parameter — matches by name.
func ec2GroupPairMatches(stored, target EC2UserIDGroupPair) bool {
	if target.GroupID != "" {
		return stored.GroupID == target.GroupID
	}
	return target.GroupName != "" && stored.GroupName == target.GroupName
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
		removeString := func(get func(*EC2IPPermission) *[]string, value string) bool {
			removed := false
			for i := range rules {
				if dropped[i] || !ec2SamePortRange(rules[i], target) {
					continue
				}
				list := get(&rules[i])
				if before := len(*list); before > 0 {
					*list = slices.DeleteFunc(*list, func(v string) bool { return v == value })
					removed = removed || len(*list) < before
				}
			}
			return removed
		}
		for _, cidr := range target.IPRanges {
			if !removeString(func(r *EC2IPPermission) *[]string { return &r.IPRanges }, cidr) {
				missed.IPRanges = append(missed.IPRanges, cidr)
			}
		}
		for _, cidr := range target.IPv6Ranges {
			if !removeString(func(r *EC2IPPermission) *[]string { return &r.IPv6Ranges }, cidr) {
				missed.IPv6Ranges = append(missed.IPv6Ranges, cidr)
			}
		}
		for _, id := range target.PrefixListIDs {
			if !removeString(func(r *EC2IPPermission) *[]string { return &r.PrefixListIDs }, id) {
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
}

// ec2IPv6RangeItem is one ipv6Ranges>item, AWS's Ipv6Range.
type ec2IPv6RangeItem struct {
	// CidrIPv6 is the IPv6 range.
	CidrIPv6 string `xml:"cidrIpv6"` //nolint:revive
}

// ec2PrefixListItem is one prefixListIds>item, AWS's PrefixListId.
type ec2PrefixListItem struct {
	// PrefixListID is the prefix list's ID.
	PrefixListID string `xml:"prefixListId"`
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
