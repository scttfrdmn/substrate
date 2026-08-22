package emulator

import (
	"strconv"
	"strings"
)

// The filter matchers for the twelve describes that parsed no Filter.N before #695.
//
// Each answers one operation, and each is reached only after that operation's
// [ec2FilterSpec] has refused every name AWS does not document — so a name arriving here is
// either evaluated below or documented-but-inert, and the `default: return true` arm is the
// second of those two cases rather than a hole. That split is [ec2FilterSpec]'s whole
// purpose; see [ec2SubnetMatchesFilter] for the precedent this batch follows.
//
// Filters AND with each other and each one's values OR, AWS's documented rule, and every
// value comparison goes through [ec2FilterAccepts] so wildcards work everywhere (#697).

// ec2TagFilterMatch evaluates the two tag filter forms — tag:<key> and tag-key — against a
// resource's tags, reporting in its second result whether name was one of them.
//
// Comma-ok shaped so a matcher writes one guard above its own switch instead of two cases
// inside it, which is what the six tag-bearing operations in this batch would otherwise
// repeat twelve times between them. The five matchers that predate #695 inline the same two
// blocks; they are left alone here because converting them would be a behavior-neutral edit
// to working code in a change that already touches twelve operations.
//
// A tag: filter compares the *key* exactly and the *value* through [ec2FilterAccepts]: the
// key is part of the filter name, and a filter name is never a pattern — AWS's wildcard rule
// is about values. tag-key compares the key through [ec2FilterAccepts] because there the key
// *is* the value.
func ec2TagFilterMatch(tags []EC2Tag, name string, values []string) (bool, bool) {
	if key, ok := strings.CutPrefix(name, "tag:"); ok {
		for _, t := range tags {
			if t.Key == key && ec2FilterAccepts(values, t.Value) {
				return true, true
			}
		}
		return false, true
	}
	if name != "tag-key" {
		return false, false
	}
	for _, t := range tags {
		if ec2FilterAccepts(values, t.Key) {
			return true, true
		}
	}
	return false, true
}

// ec2SelectedByEither reports whether a resource is named by two identity selectors, given
// each selector's requested list and this resource's value for it. With neither list supplied
// every resource is selected.
//
// The two lists are **unioned**, not intersected, because they are two spellings of the same
// question — DescribeKeyPairs' KeyName.N and KeyPairId.N, DescribeAvailabilityZones' ZoneName.N
// and ZoneId.N, DescribePlacementGroups' GroupName.N and GroupId.N. AWS answers NotFound for a
// name or ID it cannot resolve, which only makes sense if every one a request names is expected
// in the response; intersecting would answer with the empty set while both named resources
// exist. AWS documents no rule for the combination, so this is substrate's reading, recorded
// here.
//
// Membership is exact, never a glob: these are identity lists, and #697 deliberately left them
// out of the wildcard change so that a mistyped ID stays a mistyped ID rather than becoming a
// pattern that matches nothing silently.
func ec2SelectedByEither(listA []string, valueA string, listB []string, valueB string) bool {
	if len(listA) == 0 && len(listB) == 0 {
		return true
	}
	return (len(listA) > 0 && containsStr(listA, valueA)) || (len(listB) > 0 && containsStr(listB, valueB))
}

// ec2VPCMatchesFilters reports whether a VPC satisfies every supplied DescribeVpcs filter.
func ec2VPCMatchesFilters(vpc EC2VPC, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2VPCMatchesFilter(vpc, name, values) {
			return false
		}
	}
	return true
}

// ec2VPCMatchesFilter evaluates a single DescribeVpcs filter against a VPC.
//
// See [ec2VPCFilterSpec] for which of AWS's fifteen names are inert and why.
func ec2VPCMatchesFilter(vpc EC2VPC, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(vpc.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "cidr":
		// AWS: "The primary IPv4 CIDR block of the VPC. The CIDR block you specify must
		// exactly match the VPC's CIDR block for information to be returned for the VPC."
		// Equality against the primary block, not containment — the same reading
		// [ec2SubnetMatchesFilter] records for the subnet cidr filter, and the reason the
		// cidr-block-association family is a separate set of names rather than an alias.
		return ec2FilterAccepts(values, vpc.CIDRBlock)
	case "is-default":
		return ec2FilterAccepts(values, strconv.FormatBool(vpc.IsDefault))
	case "owner-id":
		return ec2FilterAccepts(values, vpc.AccountID)
	case "state":
		return ec2FilterAccepts(values, vpc.State)
	case "vpc-id":
		return ec2FilterAccepts(values, vpc.VPCID)
	default:
		return true
	}
}

// ec2InternetGatewayMatchesFilters reports whether an internet gateway satisfies every
// supplied DescribeInternetGateways filter.
func ec2InternetGatewayMatchesFilters(igw EC2InternetGateway, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2InternetGatewayMatchesFilter(igw, name, values) {
			return false
		}
	}
	return true
}

// ec2InternetGatewayMatchesFilter evaluates a single DescribeInternetGateways filter.
//
// There is no inert arm to explain: all six documented names are evaluated. The default arm
// is unreachable through the plugin — [ec2InternetGatewayFilterSpec] refuses everything
// else — and returns true rather than false so that a future name added to the spec's
// accepted list behaves as the rest of the family does.
//
// Both attachment filters walk the attachment set, so a gateway attached to no VPC matches
// neither, which follows AWS's own gloss on attachment.state — "Present only if a VPC is
// attached".
func ec2InternetGatewayMatchesFilter(igw EC2InternetGateway, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(igw.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "attachment.state":
		for _, a := range igw.Attachments {
			if ec2FilterAccepts(values, a.State) {
				return true
			}
		}
		return false
	case "attachment.vpc-id":
		for _, a := range igw.Attachments {
			if ec2FilterAccepts(values, a.VPCID) {
				return true
			}
		}
		return false
	case "internet-gateway-id":
		return ec2FilterAccepts(values, igw.InternetGatewayID)
	case "owner-id":
		return ec2FilterAccepts(values, igw.AccountID)
	default:
		return true
	}
}

// ec2KeyPairMatchesFilters reports whether a key pair satisfies every supplied
// DescribeKeyPairs filter.
func ec2KeyPairMatchesFilters(kp EC2KeyPair, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2KeyPairMatchesFilter(kp, name, values) {
			return false
		}
	}
	return true
}

// ec2KeyPairMatchesFilter evaluates a single DescribeKeyPairs filter against a key pair.
//
// All five documented names are evaluated. AWS's own Example 2 on this page filters with a
// wildcard — `Filter.1.Name=key-name&Filter.1.Value.1=*Dave*` — which is the shape #697 made
// work for every filter value in the tree.
func ec2KeyPairMatchesFilter(kp EC2KeyPair, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(kp.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "fingerprint":
		return ec2FilterAccepts(values, kp.Fingerprint)
	case "key-name":
		return ec2FilterAccepts(values, kp.KeyName)
	case "key-pair-id":
		return ec2FilterAccepts(values, kp.KeyPairID)
	default:
		return true
	}
}

// ec2PlacementGroupARN returns the ARN of a placement group.
//
// AWS's template is arn:${Partition}:ec2:${Region}:${Account}:placement-group/${PlacementGroupName} —
// by *name*, not by ID, which is why this takes the name and why CreateTags has to translate a
// pg- ID into one (#708).
//
// One spelling for the two readers that compare it as a string: the group-arn filter here and
// a caller's ArnEquals condition, which reaches the same string through
// [ec2Taggable.arn]. [TestEC2_PlacementGroupARN_MatchesTheTaggableResolver] pins that the two
// agree, because a filter and an authorization decision disagreeing about a resource's ARN is
// exactly the defect #674 was.
func ec2PlacementGroupARN(accountID, region, groupName string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":placement-group/" + groupName
}

// ec2PlacementGroupMatchesFilters reports whether a placement group satisfies every supplied
// DescribePlacementGroups filter.
func ec2PlacementGroupMatchesFilters(pg EC2PlacementGroup, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2PlacementGroupMatchesFilter(pg, name, values) {
			return false
		}
	}
	return true
}

// ec2PlacementGroupMatchesFilter evaluates a single DescribePlacementGroups filter.
//
// Six of seven evaluated; spread-level is inert, per [ec2PlacementGroupFilterSpec].
func ec2PlacementGroupMatchesFilter(pg EC2PlacementGroup, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(pg.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "group-arn":
		return ec2FilterAccepts(values, ec2PlacementGroupARN(pg.AccountID, pg.Region, pg.GroupName))
	case "group-name":
		return ec2FilterAccepts(values, pg.GroupName)
	case "state":
		return ec2FilterAccepts(values, pg.State)
	case "strategy":
		return ec2FilterAccepts(values, pg.Strategy)
	default:
		return true
	}
}

// ec2AddressMatchesFilters reports whether an Elastic IP satisfies every supplied
// DescribeAddresses filter.
func ec2AddressMatchesFilters(eip EC2ElasticIP, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2AddressMatchesFilter(eip, name, values) {
			return false
		}
	}
	return true
}

// ec2AddressMatchesFilter evaluates a single DescribeAddresses filter against an Elastic IP.
//
// The five association-shaped names — association-id, instance-id, network-interface-id,
// private-ip-address and public-ip — are compared against the members substrate records,
// which are empty on an unassociated address. So `instance-id=i-x` selects nothing among
// unassociated addresses rather than everything, which is the answer a caller looking for
// "the address on this instance" wants.
//
// The page documents no `domain` filter, so the one member substrate renders that is not
// filterable is the one a caller is most likely to reach for. That is AWS's set: `domain`
// distinguishes EC2-Classic from VPC addresses, and EC2-Classic is retired.
func ec2AddressMatchesFilter(eip EC2ElasticIP, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(eip.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "allocation-id":
		return ec2FilterAccepts(values, eip.AllocationID)
	case "association-id":
		return ec2FilterAccepts(values, eip.AssociationID)
	case "instance-id":
		return ec2FilterAccepts(values, eip.InstanceID)
	case "network-interface-id":
		return ec2FilterAccepts(values, eip.NetworkInterfaceID)
	case "private-ip-address":
		return ec2FilterAccepts(values, eip.PrivateIPAddress)
	case "public-ip":
		return ec2FilterAccepts(values, eip.PublicIP)
	default:
		return true
	}
}

// ec2AvailabilityZoneItem is one availabilityZoneInfo entry in AWS's
// DescribeAvailabilityZones response.
//
// Promoted out of the handler's function body in #695 so the filter matcher and the renderer
// compare and emit the same four values. The members are the four substrate can state without
// inventing: AWS's sample also carries optInStatus, groupName, groupLongName, messageSet,
// networkBorderGroup, geographySet and subGeographySet, and substrate models no zone grouping
// or geography — see [ec2AvailabilityZoneFilterSpec] for what that costs the filter set.
//
// The state element is `zoneState`, which is AWS's spelling in its own sample response and
// not the `state` the filter is named after.
type ec2AvailabilityZoneItem struct {
	// ZoneName is the zone's name, e.g. "us-east-1a".
	ZoneName string `xml:"zoneName"`

	// State is the zone's state. Substrate reports every seeded zone available.
	State string `xml:"zoneState"`

	// RegionName is the region the zone belongs to.
	RegionName string `xml:"regionName"`

	// ZoneID is the zone's ID, e.g. "use1-az1".
	ZoneID string `xml:"zoneId"`
}

// ec2AvailabilityZoneMatchesFilters reports whether a zone satisfies every supplied
// DescribeAvailabilityZones filter.
func ec2AvailabilityZoneMatchesFilters(az ec2AvailabilityZoneItem, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2AvailabilityZoneMatchesFilter(az, name, values) {
			return false
		}
	}
	return true
}

// ec2AvailabilityZoneMatchesFilter evaluates a single DescribeAvailabilityZones filter.
//
// The filter is named `state` and the response element `zoneState`; both read the same value.
func ec2AvailabilityZoneMatchesFilter(az ec2AvailabilityZoneItem, name string, values []string) bool {
	switch name {
	case "region-name":
		return ec2FilterAccepts(values, az.RegionName)
	case "state":
		return ec2FilterAccepts(values, az.State)
	case "zone-id":
		return ec2FilterAccepts(values, az.ZoneID)
	case "zone-name":
		return ec2FilterAccepts(values, az.ZoneName)
	default:
		return true
	}
}

// ec2RegionItem is one regionInfo entry in AWS's DescribeRegions response.
//
// Promoted to package level in #695 for the same reason as [ec2AvailabilityZoneItem]: the
// three filters this operation documents are over exactly these three members, so the matcher
// and the renderer read one struct.
type ec2RegionItem struct {
	// RegionName is the region's name, e.g. "us-east-1".
	RegionName string `xml:"regionName"`

	// RegionEndpoint is the region's EC2 endpoint, the value the endpoint filter compares.
	RegionEndpoint string `xml:"regionEndpoint"`

	// OptInStatus is the region's opt-in status. Every seeded region is
	// opt-in-not-required, so a caller filtering for opted-in or not-opted-in selects
	// nothing — which is the honest answer for an emulator with no disabled regions.
	OptInStatus string `xml:"optInStatus"`
}

// ec2RegionMatchesFilters reports whether a region satisfies every supplied DescribeRegions
// filter. All three documented names are evaluated.
func ec2RegionMatchesFilters(r ec2RegionItem, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2RegionMatchesFilter(r, name, values) {
			return false
		}
	}
	return true
}

// ec2RegionMatchesFilter evaluates a single DescribeRegions filter against a region.
func ec2RegionMatchesFilter(r ec2RegionItem, name string, values []string) bool {
	switch name {
	case "endpoint":
		return ec2FilterAccepts(values, r.RegionEndpoint)
	case "opt-in-status":
		return ec2FilterAccepts(values, r.OptInStatus)
	case "region-name":
		return ec2FilterAccepts(values, r.RegionName)
	default:
		return true
	}
}

// ec2InstanceTypeMatchesFilters reports whether a catalog entry satisfies every supplied
// DescribeInstanceTypes filter.
func ec2InstanceTypeMatchesFilters(info ec2InstanceTypeInfo, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2InstanceTypeMatchesFilter(info, name, values) {
			return false
		}
	}
	return true
}

// ec2InstanceTypeMatchesFilter evaluates a single DescribeInstanceTypes filter against a
// catalog entry.
//
// Five of fifty-seven, which retires the filter half of TODO(#495). The two numeric filters
// compare the decimal rendering of the stored integer: AWS documents no comparison operators
// for filters — "greater than or less than comparison is not supported", as the spot-price
// filter says outright — so `memory-info.size-in-mib=1024` is a string match against 1024 and
// a wildcard is the only way to ask a range-ish question.
//
// The two list-valued filters match if any element matches, which is the same any-of rule
// [ec2InternetGatewayMatchesFilter] applies to the attachment set.
func ec2InstanceTypeMatchesFilter(info ec2InstanceTypeInfo, name string, values []string) bool {
	switch name {
	case "instance-type":
		// AWS documents a wildcard here in the filter's own description — "for example
		// c5.2xlarge or c5*" — which is the only place in EC2's filter documentation
		// where a wildcard appears beside a specific filter rather than in the general
		// rule.
		return ec2FilterAccepts(values, info.InstanceType)
	case "memory-info.size-in-mib":
		return ec2FilterAccepts(values, strconv.Itoa(info.MemoryMiB))
	case "processor-info.supported-architecture":
		for _, arch := range info.SupportedArchs {
			if ec2FilterAccepts(values, arch) {
				return true
			}
		}
		return false
	case "supported-usage-class":
		for _, class := range info.SupportedUsageClasses {
			if ec2FilterAccepts(values, class) {
				return true
			}
		}
		return false
	case "vcpu-info.default-vcpus":
		return ec2FilterAccepts(values, strconv.Itoa(info.VCpus))
	default:
		return true
	}
}

// ec2SpotPriceItem is one spotPriceHistorySet entry in AWS's DescribeSpotPriceHistory
// response.
//
// Promoted to package level in #695 so the matcher compares what the response renders. That
// matters most for `timestamp`: substrate renders RFC3339, and AWS's filter description gives
// its example in a different format ("ddd MMM dd HH:mm:ss UTC YYYY") from the one its own
// sample response emits (2016-11-01T20:56:05.000Z). Comparing against the rendered value is
// the only reading that lets a caller filter on something it can read back — a value in the
// description's format matches nothing here, and would match nothing against AWS's own sample
// either.
type ec2SpotPriceItem struct {
	// InstanceType is the instance type the price is for.
	InstanceType string `xml:"instanceType"`

	// ProductDescription is the platform, always Linux/UNIX in substrate's catalog.
	ProductDescription string `xml:"productDescription"`

	// SpotPrice is the price in USD per hour, as a decimal string.
	SpotPrice string `xml:"spotPrice"`

	// Timestamp is when the price was observed, in RFC3339.
	Timestamp string `xml:"timestamp"`

	// AvailabilityZone is the zone the price applies to.
	AvailabilityZone string `xml:"availabilityZone"`
}

// ec2SpotPriceMatchesFilters reports whether a price observation satisfies every supplied
// DescribeSpotPriceHistory filter.
func ec2SpotPriceMatchesFilters(item ec2SpotPriceItem, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2SpotPriceMatchesFilter(item, name, values) {
			return false
		}
	}
	return true
}

// ec2SpotPriceMatchesFilter evaluates a single DescribeSpotPriceHistory filter.
//
// Five of six; availability-zone-id is inert, per [ec2SpotPriceFilterSpec].
func ec2SpotPriceMatchesFilter(item ec2SpotPriceItem, name string, values []string) bool {
	switch name {
	case "availability-zone":
		return ec2FilterAccepts(values, item.AvailabilityZone)
	case "instance-type":
		return ec2FilterAccepts(values, item.InstanceType)
	case "product-description":
		return ec2FilterAccepts(values, item.ProductDescription)
	case "spot-price":
		// AWS: "The value must match exactly (or use wildcards; greater than or less
		// than comparison is not supported)."
		return ec2FilterAccepts(values, item.SpotPrice)
	case "timestamp":
		return ec2FilterAccepts(values, item.Timestamp)
	default:
		return true
	}
}

// ec2LaunchTemplateMatchesFilters reports whether a launch template satisfies every supplied
// DescribeLaunchTemplates filter.
func ec2LaunchTemplateMatchesFilters(lt EC2LaunchTemplate, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2LaunchTemplateMatchesFilter(lt, name, values) {
			return false
		}
	}
	return true
}

// ec2LaunchTemplateMatchesFilter evaluates a single DescribeLaunchTemplates filter.
//
// All four documented names are evaluated. There is deliberately no launch-template-id arm:
// the page documents no such filter, and [ec2LaunchTemplateFilterSpec] refuses the name rather
// than answering a question AWS's API does not offer.
func ec2LaunchTemplateMatchesFilter(lt EC2LaunchTemplate, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(lt.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "create-time":
		return ec2FilterAccepts(values, lt.CreateTime)
	case "launch-template-name":
		return ec2FilterAccepts(values, lt.LaunchTemplateName)
	default:
		return true
	}
}

// ec2LTVersionMatchesFilters reports whether a launch template version satisfies every
// supplied DescribeLaunchTemplateVersions filter.
//
// Takes the rendered [ec2LTVersionItem] rather than the stored version, because two of the
// four evaluated filters read through it: is-default-version is derived from the parent
// template's default version number, and image-id and instance-type live inside the version's
// data. Filtering the rendered item keeps the answer and the filter over one value.
func ec2LTVersionMatchesFilters(item ec2LTVersionItem, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2LTVersionMatchesFilter(item, name, values) {
			return false
		}
	}
	return true
}

// ec2LTVersionMatchesFilter evaluates a single DescribeLaunchTemplateVersions filter.
//
// Four of fourteen; see [ec2LaunchTemplateVersionFilterSpec] for the ten that are inert.
func ec2LTVersionMatchesFilter(item ec2LTVersionItem, name string, values []string) bool {
	switch name {
	case "create-time":
		return ec2FilterAccepts(values, item.CreateTime)
	case "image-id":
		return ec2FilterAccepts(values, item.LaunchTemplateData.ImageID)
	case "instance-type":
		return ec2FilterAccepts(values, item.LaunchTemplateData.InstanceType)
	case "is-default-version":
		return ec2FilterAccepts(values, strconv.FormatBool(item.DefaultVersion))
	default:
		return true
	}
}
