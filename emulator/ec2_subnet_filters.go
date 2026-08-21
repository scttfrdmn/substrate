package emulator

import (
	"strconv"
	"strings"
)

// ec2SubnetARN returns the ARN of a subnet.
//
// One spelling, shared by the two describe paths and by the authorization resolver, for the
// reason [ec2VolumeStateKey]'s doc comment gives about state keys: a writer that spells a
// resource's identity differently from its readers produces a resource the readers cannot
// find. Here the readers are a caller's ArnEquals condition and the subnet-arn filter, and
// both compare the string literally.
func ec2SubnetARN(accountID, region, subnetID string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":subnet/" + subnetID
}

// ec2SubnetMatchesFilters reports whether a subnet satisfies every supplied DescribeSubnets
// filter. Filters AND with each other and each one's values OR, AWS's documented rule.
//
// Until #685 DescribeSubnets parsed no Filter.N at all: it selected on SubnetId.N and
// nothing else, so "the subnets of vpc-x" answered with every subnet in the region. That is
// the worst shape of the ignored-filter failure — a consumer walking a VPC's subnets got
// its neighbors' too, with nothing in the response to say so.
func ec2SubnetMatchesFilters(subnet EC2Subnet, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2SubnetMatchesFilter(subnet, name, values) {
			return false
		}
	}
	return true
}

// ec2SubnetMatchesFilter evaluates a single DescribeSubnets filter against a subnet.
//
// Eleven of AWS's twenty-five names are answerable from [EC2Subnet], and four of them have
// an alias spelling the reference documents inline ("You can also use availabilityZone as
// the filter name") — the aliases are separate names on the wire, so each is handled here
// beside its canonical form rather than normalized away, which would leave the canonical
// name working and the documented alias silently inert.
//
// The other fourteen are inert, per the rule [ec2FilterSpec] states: substrate models no
// IPv6 association, Outpost, customer-owned pool or private-DNS-on-launch option, so a
// filter over one has nothing to compare against. A name outside all twenty-five never
// arrives — [ec2SubnetFilterSpec] refuses it before the scan (#687).
func ec2SubnetMatchesFilter(subnet EC2Subnet, name string, values []string) bool {
	if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
		for _, t := range subnet.Tags {
			if t.Key == tagKey && containsStr(values, t.Value) {
				return true
			}
		}
		return false
	}

	switch name {
	case "availability-zone", "availabilityZone":
		return containsStr(values, subnet.AvailabilityZone)
	case "cidr-block", "cidr", "cidrBlock":
		// AWS: "The CIDR block you specify must exactly match the subnet's CIDR block
		// for information to be returned for the subnet." So this is equality, not
		// containment — a caller asking for 10.0.0.0/16 does not get 10.0.1.0/24.
		return containsStr(values, subnet.CIDRBlock)
	case "default-for-az", "defaultForAz":
		return containsStr(values, strconv.FormatBool(subnet.IsDefault))
	case "map-public-ip-on-launch":
		return containsStr(values, strconv.FormatBool(subnet.MapPublicIPOnLaunch))
	case "owner-id":
		// Always the requesting account: substrate is single-account, so a caller
		// naming another account is asking a question whose honest answer is "none".
		return containsStr(values, subnet.AccountID)
	case "state":
		return containsStr(values, subnet.State)
	case "subnet-arn":
		return containsStr(values, ec2SubnetARN(subnet.AccountID, subnet.Region, subnet.SubnetID))
	case "subnet-id":
		return containsStr(values, subnet.SubnetID)
	case "vpc-id":
		return containsStr(values, subnet.VPCID)
	case "tag-key":
		for _, t := range subnet.Tags {
			if containsStr(values, t.Key) {
				return true
			}
		}
		return false
	default:
		// A documented filter substrate cannot evaluate is inert; see the doc comment.
		return true
	}
}

// ec2SubnetItem renders a subnet as AWS's Subnet element.
//
// Shared by CreateSubnet and DescribeSubnets because AWS documents one Subnet type for
// both, and before #685 the two rendered different subsets of it — CreateSubnet omitted
// mapPublicIpOnLaunch, so a caller reading the create response saw a different subnet from
// the one the describe reported.
//
// Member order follows AWS's sample responses, which are identical across both operations.
// Five members those samples carry are deliberately absent: availableIpAddressCount,
// availabilityZoneId, assignIpv6AddressOnCreation, ipv6CidrBlockAssociationSet and the
// blockPublicAccessStates/privateDnsNameOptionsOnLaunch structures. Nothing in state backs
// them, and deriving an available-address count from the CIDR would be fabrication — the
// count depends on how many addresses AWS reserves and on every interface in the subnet.
//
// tagSet is omitted entirely when the subnet has no tags, which is what both DescribeSubnets
// samples show for an untagged subnet. That deliberately differs from describeSnapshots'
// present-but-empty tagSet: each follows its own operation's samples. No sample on either
// page shows a *tagged* subnet, so the element's position — last, after the members the
// samples do order — is substrate's choice.
//
// Omission needs the pointer-to-wrapper shape rather than `xml:"tagSet>item,omitempty"`:
// encoding/xml emits the parent element of a nested path even for a nil slice, so the
// omitempty form would render <tagSet></tagSet> on every untagged subnet — the very thing
// the samples say is absent. A nil pointer is omitempty-empty, so it disappears. Callers
// decoding tagSet>item see no difference between the two shapes.
type ec2SubnetItem struct {
	// SubnetID is the subnet's ID.
	SubnetID string `xml:"subnetId"`

	// SubnetARN is the subnet's ARN, the value the subnet-arn filter compares against.
	SubnetARN string `xml:"subnetArn"`

	// State is pending or available. Substrate mints subnets available.
	State string `xml:"state"`

	// OwnerID is the account that owns the subnet.
	OwnerID string `xml:"ownerId"`

	// VpcID is the VPC the subnet belongs to.
	VpcID string `xml:"vpcId"`

	// CIDRBlock is the subnet's IPv4 CIDR block.
	CIDRBlock string `xml:"cidrBlock"`

	// AvailabilityZone is the zone the subnet was created in.
	AvailabilityZone string `xml:"availabilityZone"`

	// DefaultForAz reports whether this is the zone's default subnet.
	DefaultForAz bool `xml:"defaultForAz"`

	// MapPublicIPOnLaunch reports whether a launch in this subnet gets a public IPv4.
	MapPublicIPOnLaunch bool `xml:"mapPublicIpOnLaunch"`

	// Tags are the subnet's tags, absent when it has none.
	Tags *ec2SubnetTagSet `xml:"tagSet,omitempty"`
}

// ec2SubnetTagSet wraps a subnet's tags so an untagged subnet can omit tagSet entirely.
type ec2SubnetTagSet struct {
	// Items are the tags themselves.
	Items []ec2TagItem `xml:"item"`
}

// ec2SubnetXML renders one stored subnet as its response element.
func ec2SubnetXML(subnet EC2Subnet) ec2SubnetItem {
	item := ec2SubnetItem{
		SubnetID:            subnet.SubnetID,
		SubnetARN:           ec2SubnetARN(subnet.AccountID, subnet.Region, subnet.SubnetID),
		State:               subnet.State,
		OwnerID:             subnet.AccountID,
		VpcID:               subnet.VPCID,
		CIDRBlock:           subnet.CIDRBlock,
		AvailabilityZone:    subnet.AvailabilityZone,
		DefaultForAz:        subnet.IsDefault,
		MapPublicIPOnLaunch: subnet.MapPublicIPOnLaunch,
	}
	if tags := ec2TagItems(subnet.Tags); len(tags) > 0 {
		item.Tags = &ec2SubnetTagSet{Items: tags}
	}
	return item
}
