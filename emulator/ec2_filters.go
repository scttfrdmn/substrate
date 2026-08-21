package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// ec2FilterSpec declares the Filter.N.Name values one EC2 describe operation accepts,
// split by whether substrate can answer them.
//
// Before #687 an unrecognized filter name produced three different answers depending on
// which operation received it: dropped (volumes, snapshots, security groups, route
// tables, NAT gateways, fleets, images), matched nothing (instances), or refused
// (instance-type offerings). Real EC2 refuses. A spec per operation makes the refusal one
// shared rule, so a new operation cannot pick a fourth behavior.
//
// The split into two lists is what keeps the refusal from exceeding AWS's. Refusing every
// name substrate does not *evaluate* would deny filters real EC2 accepts —
// ipv6-cidr-block-association.state is a filter, not a typo — so a documented name
// substrate cannot answer is accepted and left inert, and docs/services.md lists those
// names per operation so the gap is visible rather than discovered. Only a name outside
// AWS's documented set is refused.
//
// Each spec's list is transcribed from that operation's own API reference page, named in
// the constructor's doc comment, rather than from a list shared between operations: AWS
// documents different filters for each, and a shared list would refuse a name one
// operation accepts because a sibling does not.
type ec2FilterSpec struct {
	// evaluated are the names substrate applies. A name here still has to be handled by
	// the operation's match function; the spec governs refusal, not matching.
	evaluated []string
	// accepted are the names AWS documents that substrate keeps no state to answer.
	accepted []string
	// tagValueFilter records whether the operation documents the tag:<key> form. Two do
	// not — DescribeFleets and DescribeInstanceTypeOfferings list no tag filter at all —
	// so it cannot be assumed from the presence of tag-key.
	tagValueFilter bool
}

// documents reports whether the operation's reference page lists name as a filter.
//
// tag:<key> is matched by prefix because its key is part of the name, which is why it
// cannot live in either list. AWS's route-table, NAT-gateway and subnet pages render this
// entry as a bare "tag" while the snapshot and security-group pages render it
// "tag:<key>"; the body text is identical on all five and says to "use the tag key in the
// filter name", so the prefix form is what is documented and no literal filter named
// "tag" exists.
func (s ec2FilterSpec) documents(name string) bool {
	if strings.HasPrefix(name, "tag:") {
		return s.tagValueFilter
	}
	return containsStr(s.evaluated, name) || containsStr(s.accepted, name)
}

// check refuses the first filter name, in request order, that the operation does not
// document.
//
// It walks Filter.N itself rather than taking [extractEC2Filters]' map because a map has
// no order: given two undocumented names the map form would name an arbitrary one, and
// two replays of one recorded request could report different refusals. Naming the first
// offending filter in request order is deterministic and is the reading a caller expects.
//
// An empty filter name is skipped rather than refused, because [extractEC2Filters] drops
// it. Refusing here would make the checker and the extractor disagree about what the
// request contains, which is the class of defect #686 fixed.
//
// The three request limits are enforced here too, for the same reason the refusal is: one
// place, inherited by every operation that has a spec. See [ec2FilterLimitError].
//
// Provenance: refusal is real EC2's *observed* behavior, not its documented behavior.
// Neither Using_Filtering nor the Filter type's reference page says what happens to an
// unrecognized name — both are silent — so nothing in AWS's documentation settles it, and
// [ec2InvalidFilterError] carries the reasoning for the code and message. The *limits*, by
// contrast, are documented; only the error they raise is substrate's choice.
func (s ec2FilterSpec) check(params map[string]string) *AWSError {
	totalValues := 0
	for i := 1; ; i++ {
		name, ok := params[fmt.Sprintf("Filter.%d.Name", i)]
		if !ok {
			return nil
		}
		if i > ec2MaxFiltersPerRequest {
			return ec2FilterLimitError(fmt.Sprintf(
				"a request may specify at most %d filters", ec2MaxFiltersPerRequest))
		}
		// Values are counted for every filter, including one whose name is skipped below:
		// AWS's limit is on the request, and an empty name still carries its values on the
		// wire.
		for j := 1; ; j++ {
			v, ok := params[fmt.Sprintf("Filter.%d.Value.%d", i, j)]
			if !ok {
				break
			}
			totalValues++
			if totalValues > ec2MaxFilterValuesPerRequest {
				return ec2FilterLimitError(fmt.Sprintf(
					"a request may specify at most %d filter values in total",
					ec2MaxFilterValuesPerRequest))
			}
			if len(v) > ec2MaxFilterValueLength {
				return ec2FilterLimitError(fmt.Sprintf(
					"a filter value may be at most %d characters", ec2MaxFilterValueLength))
			}
		}
		if name == "" || s.documents(name) {
			continue
		}
		return ec2InvalidFilterError(name)
	}
}

// The request limits EC2 documents for filters, from the "Filtering considerations" list in
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Filtering.html: "You can specify
// up to 50 filters and up to 200 total filter values in a single request" and "Filter strings
// can be up to 255 characters in length".
//
// The 255 is applied to filter *values*, not names. "Filter strings" is not defined on that
// page, but a name is bounded far below 255 by [ec2FilterSpec.documents] — every documented
// name is a fixed short literal — so a length rule on names could only ever fire after the
// refusal already had, and pretending otherwise would advertise a check that cannot be
// reached. The one name that is not a fixed literal, tag:<key>, is bounded by AWS's own
// 128-character tag-key limit.
//
// Length is measured in bytes rather than runes. AWS does not say which it means, and the two
// differ only for a non-ASCII value; bytes is what the wire carries.
const (
	ec2MaxFiltersPerRequest      = 50
	ec2MaxFilterValuesPerRequest = 200
	ec2MaxFilterValueLength      = 255
)

// ec2FilterLimitError returns the error substrate raises when a request exceeds one of the
// documented filter limits. detail states which limit and its value.
//
// Provenance is split, the opposite way round from [ec2InvalidFilterError]: the *limits* are
// documented and the *error* is not. EC2's client-error tables publish no filter-limit code —
// every `*LimitExceeded` code there names a resource quota (KeyPairLimitExceeded,
// NatGatewayLimitExceeded, TrafficMirrorFilterLimitExceeded and so on), none of them a
// request-shape limit — so InvalidParameterValue is the only candidate whose published gloss
// covers this: "A value specified in a parameter is not valid, is unsupported, or cannot be
// used… The returned message provides an explanation of the error value." A consumer must
// dispatch on the code and not on this message.
func ec2FilterLimitError(detail string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    detail,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2InstanceFilterSpec is DescribeInstances' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeInstances.html.
//
// The widest set of the ten, at 136 names. Substrate evaluates ten of them plus
// tag:<key>; the rest describe instance members it does not model.
func ec2InstanceFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"availability-zone", "image-id", "instance-id", "instance-state-code",
			"instance-state-name", "instance-type", "key-name", "subnet-id", "tag-key",
			"vpc-id",
		},
		accepted: []string{
			"affinity", "architecture", "availability-zone-id",
			"block-device-mapping.attach-time",
			"block-device-mapping.delete-on-termination",
			"block-device-mapping.device-name", "block-device-mapping.status",
			"block-device-mapping.volume-id", "boot-mode", "capacity-reservation-id",
			"capacity-reservation-specification.capacity-reservation-preference",
			"capacity-reservation-specification.capacity-reservation-target.capacity-reservation-id",
			"capacity-reservation-specification.capacity-reservation-target.capacity-reservation-resource-group-arn",
			"client-token", "current-instance-boot-mode", "dns-name", "ebs-optimized",
			"ena-support", "enclave-options.enabled", "hibernation-options.configured",
			"host-id", "hypervisor", "iam-instance-profile.arn",
			"iam-instance-profile.id", "instance-lifecycle", "instance.group-id",
			"instance.group-name", "ip-address", "ipv6-address", "kernel-id",
			"launch-index", "launch-time", "maintenance-options.auto-recovery",
			"metadata-options.http-endpoint", "metadata-options.http-protocol-ipv4",
			"metadata-options.http-protocol-ipv6",
			"metadata-options.http-put-response-hop-limit",
			"metadata-options.http-tokens", "metadata-options.instance-metadata-tags",
			"metadata-options.state", "monitoring-state",
			"network-interface.addresses.association.allocation-id",
			"network-interface.addresses.association.association-id",
			"network-interface.addresses.association.carrier-ip",
			"network-interface.addresses.association.customer-owned-ip",
			"network-interface.addresses.association.ip-owner-id",
			"network-interface.addresses.association.public-dns-name",
			"network-interface.addresses.association.public-ip",
			"network-interface.addresses.primary",
			"network-interface.addresses.private-dns-name",
			"network-interface.addresses.private-ip-address",
			"network-interface.association.allocation-id",
			"network-interface.association.association-id",
			"network-interface.association.carrier-ip",
			"network-interface.association.customer-owned-ip",
			"network-interface.association.ip-owner-id",
			"network-interface.association.public-dns-name",
			"network-interface.association.public-ip",
			"network-interface.attachment.attach-time",
			"network-interface.attachment.attachment-id",
			"network-interface.attachment.delete-on-termination",
			"network-interface.attachment.device-index",
			"network-interface.attachment.instance-id",
			"network-interface.attachment.instance-owner-id",
			"network-interface.attachment.network-card-index",
			"network-interface.attachment.status",
			"network-interface.availability-zone",
			"network-interface.deny-all-igw-traffic",
			"network-interface.description", "network-interface.group-id",
			"network-interface.group-name",
			"network-interface.ipv4-prefixes.ipv4-prefix",
			"network-interface.ipv6-address",
			"network-interface.ipv6-addresses.ipv6-address",
			"network-interface.ipv6-addresses.is-primary-ipv6",
			"network-interface.ipv6-native",
			"network-interface.ipv6-prefixes.ipv6-prefix",
			"network-interface.mac-address", "network-interface.network-interface-id",
			"network-interface.operator.managed", "network-interface.operator.principal",
			"network-interface.outpost-arn", "network-interface.owner-id",
			"network-interface.private-dns-name",
			"network-interface.private-ip-address",
			"network-interface.public-dns-name", "network-interface.requester-id",
			"network-interface.requester-managed", "network-interface.source-dest-check",
			"network-interface.status", "network-interface.subnet-id",
			"network-interface.tag-key", "network-interface.tag-value",
			"network-interface.vpc-id",
			"network-performance-options.bandwidth-weighting", "operator.managed",
			"operator.principal", "outpost-arn", "owner-id", "placement-group-name",
			"placement-partition-number", "platform", "platform-details",
			"private-dns-name",
			"private-dns-name-options.enable-resource-name-dns-a-record",
			"private-dns-name-options.enable-resource-name-dns-aaaa-record",
			"private-dns-name-options.hostname-type", "private-ip-address",
			"product-code", "product-code.type", "ramdisk-id", "reason", "requester-id",
			"reservation-id", "root-device-name", "root-device-type",
			"source-dest-check", "spot-instance-request-id", "state-reason-code",
			"state-reason-message", "tenancy", "tpm-support", "usage-operation",
			"usage-operation-update-time", "virtualization-type",
		},
	}
}

// ec2ImageFilterSpec is DescribeImages' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeImages.html.
//
// tag-key joined the evaluated list in #686, which also stopped a valueless tag:<key>
// filter from standing in for it.
func ec2ImageFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"block-device-mapping.snapshot-id", "image-id", "tag-key",
		},
		accepted: []string{
			"architecture", "block-device-mapping.delete-on-termination",
			"block-device-mapping.device-name", "block-device-mapping.encrypted",
			"block-device-mapping.volume-size", "block-device-mapping.volume-type",
			"creation-date", "description", "ena-support", "free-tier-eligible",
			"hypervisor", "image-allowed", "image-type",
			"image-watermark.source-image-creation-time",
			"image-watermark.source-image-id", "image-watermark.source-image-region",
			"image-watermark.watermark-creation-time", "image-watermark.watermark-key",
			"is-public", "kernel-id", "manifest-location", "name", "owner-alias",
			"owner-id", "platform", "product-code", "product-code.type",
			"public-ssm-parameter-name", "ramdisk-id", "root-device-name",
			"root-device-type", "source-image-id", "source-image-region",
			"source-instance-id", "sriov-net-support", "state", "state-reason-code",
			"state-reason-message", "virtualization-type",
		},
	}
}

// ec2VolumeFilterSpec is DescribeVolumes' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeVolumes.html.
//
// Eleven of AWS's twenty names are evaluated, which is the widest coverage of any
// operation here — #670 filled it in.
func ec2VolumeFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"attachment.delete-on-termination", "attachment.device",
			"attachment.instance-id", "availability-zone", "size", "snapshot-id",
			"status", "tag-key", "volume-id", "volume-type",
		},
		accepted: []string{
			"attachment.attach-time", "attachment.status", "availability-zone-id",
			"create-time", "encrypted", "fast-restored", "multi-attach-enabled",
			"operator.managed", "operator.principal",
		},
	}
}

// ec2SnapshotFilterSpec is DescribeSnapshots' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSnapshots.html.
//
// Ten of AWS's fourteen names are evaluated as of #685 — the nine below plus tag:<key>. The
// four that are not name snapshot members substrate does not render, so there is nothing to
// compare a value against.
func ec2SnapshotFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"description", "encrypted", "owner-id", "snapshot-id", "start-time",
			"status", "tag-key", "volume-id", "volume-size",
		},
		accepted: []string{"owner-alias", "progress", "storage-tier", "transfer-type"},
	}
}

// ec2SubnetFilterSpec is DescribeSubnets' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSubnets.html.
//
// Twenty-five names plus five alias spellings the page documents inline, of which #685
// evaluates eleven and four aliases. The aliases are listed individually rather than
// normalized: they are distinct names on the wire, and folding them would make the spec
// disagree with [ec2SubnetMatchesFilter] about what it accepts.
//
// This operation parsed no Filter.N at all before #685, so unlike its siblings it gains
// both halves at once — the refusal from #687 and the matching.
func ec2SubnetFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"availability-zone", "availabilityZone", "cidr", "cidr-block", "cidrBlock",
			"default-for-az", "defaultForAz", "map-public-ip-on-launch", "owner-id",
			"state", "subnet-arn", "subnet-id", "tag-key", "vpc-id",
		},
		accepted: []string{
			"availability-zone-id", "availabilityZoneId", "available-ip-address-count",
			"customer-owned-ipv4-pool", "enable-dns64", "enable-lni-at-device-index",
			"ipv6-cidr-block-association.association-id",
			"ipv6-cidr-block-association.ipv6-cidr-block",
			"ipv6-cidr-block-association.state", "ipv6-native",
			"map-customer-owned-ip-on-launch", "outpost-arn",
			"private-dns-name-options-on-launch.enable-resource-name-dns-a-record",
			"private-dns-name-options-on-launch.enable-resource-name-dns-aaaa-record",
			"private-dns-name-options-on-launch.hostname-type",
		},
	}
}

// ec2SecurityGroupFilterSpec is DescribeSecurityGroups' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSecurityGroups.html.
//
// The twenty rule-scoped filters are accepted and inert. AWS's own note on this
// operation — "if using multiple filters for rules, the results include security groups
// for which any combination of rules, not necessarily a single rule, match all filters" —
// is a semantics substrate would have to implement deliberately rather than incidentally,
// so answering those names is left to a change that can pin that rule.
func ec2SecurityGroupFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated:      []string{"group-id", "group-name", "vpc-id"},
		accepted: []string{
			"description", "egress.ip-permission.cidr",
			"egress.ip-permission.from-port", "egress.ip-permission.group-id",
			"egress.ip-permission.group-name", "egress.ip-permission.ipv6-cidr",
			"egress.ip-permission.prefix-list-id", "egress.ip-permission.protocol",
			"egress.ip-permission.to-port", "egress.ip-permission.user-id",
			"ip-permission.cidr", "ip-permission.from-port", "ip-permission.group-id",
			"ip-permission.group-name", "ip-permission.ipv6-cidr",
			"ip-permission.prefix-list-id", "ip-permission.protocol",
			"ip-permission.to-port", "ip-permission.user-id", "owner-id", "tag-key",
		},
	}
}

// ec2RouteTableFilterSpec is DescribeRouteTables' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeRouteTables.html.
func ec2RouteTableFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"association.route-table-id", "association.subnet-id", "vpc-id",
		},
		accepted: []string{
			"association.gateway-id", "association.main",
			"association.route-table-association-id", "owner-id", "route-table-id",
			"route.destination-cidr-block", "route.destination-ipv6-cidr-block",
			"route.destination-prefix-list-id",
			"route.egress-only-internet-gateway-id", "route.gateway-id",
			"route.instance-id", "route.nat-gateway-id", "route.origin", "route.state",
			"route.transit-gateway-id", "route.vpc-peering-connection-id", "tag-key",
		},
	}
}

// ec2NatGatewayFilterSpec is DescribeNatGateways' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeNatGateways.html.
//
// AWS's narrowest tag-bearing set, at six names.
func ec2NatGatewayFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated:      []string{"state", "vpc-id"},
		accepted:       []string{"nat-gateway-id", "subnet-id", "tag-key"},
	}
}

// ec2FleetFilterSpec is DescribeFleets' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeFleets.html.
//
// The reference documents five filters and **no tag filter at all** — neither tag:<key>
// nor tag-key — even though a fleet carries tags and DescribeFleets renders them. So
// tag:<key> is refused here while it is accepted on every neighboring describe, which is
// AWS's set and not an omission: a caller looking for a fleet by tag uses Resource Groups
// Tagging or DescribeTags.
func ec2FleetFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		evaluated: []string{"activity-status", "fleet-state", "type"},
		accepted: []string{
			"excess-capacity-termination-policy", "replace-unhealthy-instances",
		},
	}
}

// ec2InstanceTypeOfferingFilterSpec is DescribeInstanceTypeOfferings' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeInstanceTypeOfferings.html.
//
// Two names, both evaluated, and no tag filter — an instance type offering is not a
// resource and carries no tags. This operation refused an undocumented name before #687
// through a check of its own, and is where [ec2InvalidFilterError] came from.
func ec2InstanceTypeOfferingFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{evaluated: []string{"instance-type", "location"}}
}
