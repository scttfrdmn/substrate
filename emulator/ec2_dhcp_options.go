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

// DHCP option sets (#1614).
//
// The VPC User Guide's "DHCP option set concepts": "Each Region has a default DHCP option set.
// Each VPC uses the default DHCP option set for its Region unless you either create and
// associate a custom DHCP option set with the VPC or configure the VPC with no DHCP option
// set." The default set's domain name servers are "always AmazonProvidedDNS", and its domain
// name is the Region's private DNS suffix — CreateDhcpOptions: "If you're using
// AmazonProvidedDNS in us-east-1, specify ec2.internal. If you're using AmazonProvidedDNS in
// any other Region, specify region.compute.internal."
//
// The options are recorded and rendered, never served: substrate runs no DHCP server, so a set
// is observable only through DescribeDhcpOptions and a VPC's dhcpOptionsId.
//
// # State
//
// A set a caller creates is stored under dopt:{account}/{region}/{dopt-id} as an
// [EC2DHCPOptions]. The Region's default set is not stored until something writes to it: its ID
// is derived from the account and Region ([ec2DefaultDHCPOptionsID]), so every read in every
// process names the same set without drawing from a request's mint, and DescribeDhcpOptions
// renders it from [ec2DefaultDHCPOptions]. A tag write persists it under that ID; deleting it
// writes a tombstone under dopt_default:{account}/{region}, after which it is no longer listed.
//
// A VPC records its set in [EC2VPC.DhcpOptionsID]: a dopt- ID, or "default" for a VPC associated
// with no set, which is the literal AssociateDhcpOptions takes and DescribeVpcs then renders. A
// VPC an older build wrote records nothing and reads as the default set, which is what it used
// on AWS; DeleteDhcpOptions counts it as a user of that set.

// ec2NoDHCPOptions is the DhcpOptionsId that associates a VPC with no DHCP option set, per
// AssociateDhcpOptions' "The ID of the DHCP options set, or default to associate no DHCP
// options with the VPC".
const ec2NoDHCPOptions = "default"

// EC2DHCPOptions is a stored DHCP option set.
type EC2DHCPOptions struct {
	// DHCPOptionsID is the set's dopt- ID.
	DHCPOptionsID string `json:"dhcp_options_id"`

	// Configurations are the set's options, in the order they were given.
	Configurations []EC2DHCPConfiguration `json:"configurations"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`
}

// EC2DHCPConfiguration is one option of a DHCP option set.
type EC2DHCPConfiguration struct {
	// Key is the option name, such as domain-name.
	Key string `json:"key"`

	// Values are the option's values.
	Values []string `json:"values"`
}

// ec2DHCPOptionsIDKind is the DHCP option set ID family for the mutations, with the error
// reference's InvalidDhcpOptionsId.Malformed and InvalidDhcpOptionsID.NotFound.
var ec2DHCPOptionsIDKind = ec2IDKind{
	Prefix: "dopt-", NotFound: "InvalidDhcpOptionsID.NotFound",
	Malformed: "InvalidDhcpOptionsId.Malformed", Noun: "DHCP options set",
}

// ec2DescribeDHCPOptionsIDKind is the same family as DescribeDhcpOptions reports an unknown ID:
// InvalidDhcpOptionID.NotFound, the reference's other spelling, and the one the Terraform AWS
// provider expects from DescribeDhcpOptions (its findDHCPOptionses), where it expects
// InvalidDhcpOptionsID.NotFound from DeleteDhcpOptions.
var ec2DescribeDHCPOptionsIDKind = ec2IDKind{
	Prefix: "dopt-", NotFound: "InvalidDhcpOptionID.NotFound",
	Malformed: "InvalidDhcpOptionsId.Malformed", Noun: "DHCP options set",
}

// ec2DHCPOptionsKey is the state key of a DHCP option set.
func ec2DHCPOptionsKey(accountID, region, id string) string {
	return ec2StatePrefix("dopt", accountID, region) + id
}

// ec2DefaultDHCPOptionsTombstoneKey is the state key recording that the Region's default set was
// deleted.
func ec2DefaultDHCPOptionsTombstoneKey(accountID, region string) string {
	return "dopt_default:" + accountID + "/" + region
}

// ec2DefaultDHCPOptionsID is the ID of the account and Region's default DHCP option set: the
// first draw of a mint seeded with the account and Region.
func ec2DefaultDHCPOptionsID(accountID, region string) string {
	return "dopt-" + NewIDMint("ec2-default-dhcp-options/"+accountID+"/"+region).Hex(8)
}

// ec2DefaultDHCPDomainName is the Region's private DNS suffix, the default set's domain-name.
func ec2DefaultDHCPDomainName(region string) string {
	if region == "us-east-1" {
		return "ec2.internal"
	}
	return region + ".compute.internal"
}

// ec2DefaultDHCPOptions is the account and Region's default set, as DescribeDhcpOptions' first
// sample renders one: domain-name, then domain-name-servers.
func ec2DefaultDHCPOptions(accountID, region string) EC2DHCPOptions {
	return EC2DHCPOptions{
		DHCPOptionsID: ec2DefaultDHCPOptionsID(accountID, region),
		Configurations: []EC2DHCPConfiguration{
			{Key: "domain-name", Values: []string{ec2DefaultDHCPDomainName(region)}},
			{Key: "domain-name-servers", Values: []string{"AmazonProvidedDNS"}},
		},
	}
}

// ec2VPCDHCPOptionsID is the DHCP option set a VPC renders: the one it records, or the Region's
// default set for a VPC an older build wrote.
func ec2VPCDHCPOptionsID(vpc EC2VPC) string {
	if vpc.DhcpOptionsID != "" {
		return vpc.DhcpOptionsID
	}
	return ec2DefaultDHCPOptionsID(vpc.AccountID, vpc.Region)
}

// newVPCDHCPOptionsID is the set a new VPC is associated with: the Region's default set, or none
// once that set has been deleted. The caller holds [EC2Plugin.sgMu] from this read through the
// VPC's write, so a concurrent DeleteDhcpOptions cannot delete the default set between them.
func (p *EC2Plugin) newVPCDHCPOptionsID(ctx context.Context, reqCtx *RequestContext) (string, error) {
	deleted, err := p.state.Get(ctx, ec2Namespace, ec2DefaultDHCPOptionsTombstoneKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return "", fmt.Errorf("ec2 default DHCP options: %w", err)
	}
	if deleted != nil {
		return ec2NoDHCPOptions, nil
	}
	return ec2DefaultDHCPOptionsID(reqCtx.AccountID, reqCtx.Region), nil
}

// loadDHCPOptions returns the account and Region's option sets, the default one included unless
// it was deleted, ordered by ID, and reports which IDs have a record in state.
func (p *EC2Plugin) loadDHCPOptions(ctx context.Context, reqCtx *RequestContext) ([]EC2DHCPOptions, map[string]bool, error) {
	stored := map[string]bool{}
	var sets []EC2DHCPOptions
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("dopt", reqCtx.AccountID, reqCtx.Region), func(_ string, set EC2DHCPOptions) {
		sets = append(sets, set)
		stored[set.DHCPOptionsID] = true
	}); err != nil {
		return nil, nil, fmt.Errorf("ec2 DHCP options: %w", err)
	}
	defaultID := ec2DefaultDHCPOptionsID(reqCtx.AccountID, reqCtx.Region)
	if !stored[defaultID] {
		deleted, err := p.state.Get(ctx, ec2Namespace, ec2DefaultDHCPOptionsTombstoneKey(reqCtx.AccountID, reqCtx.Region))
		if err != nil {
			return nil, nil, fmt.Errorf("ec2 default DHCP options: %w", err)
		}
		if deleted == nil {
			sets = append(sets, ec2DefaultDHCPOptions(reqCtx.AccountID, reqCtx.Region))
		}
	}
	sort.Slice(sets, func(i, j int) bool { return sets[i].DHCPOptionsID < sets[j].DHCPOptionsID })
	return sets, stored, nil
}

// ec2FindDHCPOptions returns the set with the ID among sets, or nil.
func ec2FindDHCPOptions(sets []EC2DHCPOptions, id string) *EC2DHCPOptions {
	for i := range sets {
		if sets[i].DHCPOptionsID == id {
			return &sets[i]
		}
	}
	return nil
}

// syntheticDefaultDHCPOptions returns the Region's default set when ids names it and it is not
// yet stored, or nil. The caller holds [EC2Plugin.sgMu].
func (p *EC2Plugin) syntheticDefaultDHCPOptions(reqCtx *RequestContext, ids []string) (*EC2DHCPOptions, error) {
	defaultID := ec2DefaultDHCPOptionsID(reqCtx.AccountID, reqCtx.Region)
	if !containsStr(ids, defaultID) {
		return nil, nil
	}
	sets, stored, err := p.loadDHCPOptions(context.Background(), reqCtx)
	if err != nil {
		return nil, err
	}
	set := ec2FindDHCPOptions(sets, defaultID)
	if set == nil || stored[defaultID] {
		return nil, nil
	}
	return set, nil
}

// materializeTagTargets persists the synthesized records among a CreateTags request's resource
// IDs — a default network ACL of a VPC an older build wrote, and the Region's default DHCP
// option set — so the tags have a stored record to land on. A synthesized record carries no
// tags, so the request's own tags must fit the 50-tag limit on it; when they do not, the limit's
// refusal is returned and nothing is persisted. The caller holds [EC2Plugin.sgMu], which
// [EC2Plugin.lockTagWrite] takes for both prefixes.
func (p *EC2Plugin) materializeTagTargets(reqCtx *RequestContext, ids []string, tags []EC2Tag) error {
	acls, err := p.syntheticNetworkACLs(reqCtx, ids)
	if err != nil {
		return err
	}
	set, err := p.syntheticDefaultDHCPOptions(reqCtx, ids)
	if err != nil {
		return err
	}
	if len(acls) == 0 && set == nil {
		return nil
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return awsErr
	}
	ctx := context.Background()
	for _, acl := range acls {
		if err := p.putNetworkACL(ctx, reqCtx, acl); err != nil {
			return err
		}
	}
	if set != nil {
		if err := p.putEC2Record(ctx, ec2DHCPOptionsKey(reqCtx.AccountID, reqCtx.Region, set.DHCPOptionsID), *set); err != nil {
			return fmt.Errorf("ec2 default DHCP options: %w", err)
		}
	}
	return nil
}

// --- Rendering ---

// ec2DHCPOptionsItem renders a DHCP option set as AWS's DhcpOptions element, in the member order
// DescribeDhcpOptions' sample shows. tagSet is omitted when the set has none, as that sample
// omits it.
type ec2DHCPOptionsItem struct {
	// DHCPOptionsID is the set's ID.
	DHCPOptionsID string `xml:"dhcpOptionsId"`

	// OwnerID is the account that owns the set.
	OwnerID string `xml:"ownerId"`

	// Configurations are the options.
	Configurations []ec2DHCPConfigurationItem `xml:"dhcpConfigurationSet>item"`

	// Tags are the set's tags, absent when it has none.
	Tags *ec2TagSetXML `xml:"tagSet,omitempty"`
}

// ec2DHCPConfigurationItem renders one DhcpConfiguration.
type ec2DHCPConfigurationItem struct {
	// Key is the option name.
	Key string `xml:"key"`

	// Values are the option's values.
	Values []ec2DHCPValueItem `xml:"valueSet>item"`
}

// ec2DHCPValueItem renders one AttributeValue.
type ec2DHCPValueItem struct {
	// Value is the value.
	Value string `xml:"value"`
}

// ec2DHCPOptionsXML renders set.
func ec2DHCPOptionsXML(set EC2DHCPOptions, owner string) ec2DHCPOptionsItem {
	item := ec2DHCPOptionsItem{DHCPOptionsID: set.DHCPOptionsID, OwnerID: owner, Tags: ec2TagSet(set.Tags)}
	for _, c := range set.Configurations {
		ci := ec2DHCPConfigurationItem{Key: c.Key}
		for _, v := range c.Values {
			ci.Values = append(ci.Values, ec2DHCPValueItem{Value: v})
		}
		item.Configurations = append(item.Configurations, ci)
	}
	return item
}

// --- Operations ---

// ec2DHCPOptionLimits is each option CreateDhcpOptions documents, mapped to the most values it
// may carry: "The IP addresses of up to four DNS servers", "up to eight Network Time Protocol
// (NTP) servers", "up to four NetBIOS name servers", and one value for the rest.
var ec2DHCPOptionLimits = map[string]int{
	"domain-name":                       1,
	"domain-name-servers":               4,
	"ntp-servers":                       8,
	"netbios-name-servers":              4,
	"netbios-node-type":                 1,
	"ipv6-address-preferred-lease-time": 1,
}

// ec2ParseDHCPConfigurations reads DhcpConfiguration.N.Key and DhcpConfiguration.N.Value.M.
//
// At least one option is required. An option CreateDhcpOptions does not document, one given
// twice, one with no value or more than [ec2DHCPOptionLimits] allows, a netbios-node-type other
// than "1, 2, 4, or 8", and an ipv6-address-preferred-lease-time outside "between 140 and
// 2147483647 seconds" are each refused with InvalidParameterValue. The messages are substrate's
// own.
func ec2ParseDHCPConfigurations(params map[string]string) ([]EC2DHCPConfiguration, *AWSError) {
	var out []EC2DHCPConfiguration
	seen := map[string]bool{}
	for n := 1; ; n++ {
		prefix := "DhcpConfiguration." + strconv.Itoa(n) + "."
		key, ok := params[prefix+"Key"]
		if !ok {
			break
		}
		limit, known := ec2DHCPOptionLimits[key]
		if !known {
			return nil, ec2InvalidParameterValue(prefix+"Key", key, "It must be one of domain-name, domain-name-servers, "+
				"ntp-servers, netbios-name-servers, netbios-node-type or ipv6-address-preferred-lease-time.")
		}
		if seen[key] {
			return nil, ec2InvalidParameterValue(prefix+"Key", key, "Each option may be given once.")
		}
		seen[key] = true
		values := extractIndexedParams(params, prefix+"Value")
		if len(values) == 0 || len(values) > limit {
			return nil, ec2InvalidParameterValue(prefix+"Value", strings.Join(values, ","),
				"The "+key+" option takes from 1 to "+strconv.Itoa(limit)+" values.")
		}
		switch key {
		case "netbios-node-type":
			if !containsStr([]string{"1", "2", "4", "8"}, values[0]) {
				return nil, ec2InvalidParameterValue(prefix+"Value.1", values[0], "The NetBIOS node type must be 1, 2, 4 or 8.")
			}
		case "ipv6-address-preferred-lease-time":
			if secs, err := strconv.Atoi(values[0]); err != nil || secs < 140 || secs > 2147483647 {
				return nil, ec2InvalidParameterValue(prefix+"Value.1", values[0], "The lease time must be from 140 to 2147483647 seconds.")
			}
		}
		out = append(out, EC2DHCPConfiguration{Key: key, Values: values})
	}
	if len(out) == 0 {
		return nil, ec2MissingParameter("DhcpConfiguration")
	}
	return out, nil
}

// createDHCPOptions handles CreateDhcpOptions: a new set with the request's options, tagged from
// TagSpecification.N scoped to dhcp-options. Its ID is drawn from the request's mint.
func (p *EC2Plugin) createDHCPOptions(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	configs, awsErr := ec2ParseDHCPConfigurations(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	tags := ec2TagSpecificationTags(req.Params, "", "dhcp-options")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}
	set := EC2DHCPOptions{DHCPOptionsID: "dopt-" + reqCtx.IDs.Hex(8), Configurations: configs, Tags: tags}
	if err := p.putEC2Record(context.Background(), ec2DHCPOptionsKey(reqCtx.AccountID, reqCtx.Region, set.DHCPOptionsID), set); err != nil {
		return nil, fmt.Errorf("ec2 createDhcpOptions: %w", err)
	}
	type response struct {
		XMLName     xml.Name           `xml:"CreateDhcpOptionsResponse"`
		XMLNS       string             `xml:"xmlns,attr"`
		DHCPOptions ec2DHCPOptionsItem `xml:"dhcpOptions"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: ec2XMLNS, DHCPOptions: ec2DHCPOptionsXML(set, reqCtx.AccountID)})
}

// dhcpOptionsInUse reports whether any VPC in the account and Region uses the set with the ID.
func (p *EC2Plugin) dhcpOptionsInUse(ctx context.Context, reqCtx *RequestContext, id string) (bool, error) {
	used := false
	if err := ec2ScanRecords(ctx, p.state, ec2StatePrefix("vpc", reqCtx.AccountID, reqCtx.Region), func(_ string, vpc EC2VPC) {
		used = used || ec2VPCDHCPOptionsID(vpc) == id
	}); err != nil {
		return false, fmt.Errorf("ec2 deleteDhcpOptions vpcs: %w", err)
	}
	return used, nil
}

// deleteDHCPOptions handles DeleteDhcpOptions: "You must disassociate the set of DHCP options
// before you can delete it."
//
// A set any VPC uses is refused with DependencyViolation and EC2's observed message, "The
// dhcpOptions 'dopt-…' has dependencies and cannot be deleted." (quoted in Ashish Gupta's
// "Manually delete an AWS VPC"). An unknown set is InvalidDhcpOptionsID.NotFound, the code the
// Terraform AWS provider's aws_vpc_dhcp_options treats as already deleted. Deleting the Region's
// default set writes its tombstone, so later VPCs are created with no set.
func (p *EC2Plugin) deleteDHCPOptions(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DhcpOptionsId"]
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	sets, stored, err := p.loadDHCPOptions(ctx, reqCtx)
	if err != nil {
		return nil, err
	}
	if reqErr := ec2RequireNamedResource(ec2DHCPOptionsIDKind, "DhcpOptionsId", id, ec2FindDHCPOptions(sets, id) != nil); reqErr != nil {
		return nil, reqErr
	}
	used, err := p.dhcpOptionsInUse(ctx, reqCtx, id)
	if err != nil {
		return nil, err
	}
	if used {
		return nil, &AWSError{
			Code:       "DependencyViolation",
			Message:    "The dhcpOptions '" + id + "' has dependencies and cannot be deleted.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if stored[id] {
		if err := p.state.Delete(ctx, ec2Namespace, ec2DHCPOptionsKey(reqCtx.AccountID, reqCtx.Region, id)); err != nil {
			return nil, fmt.Errorf("ec2 deleteDhcpOptions: %w", err)
		}
	}
	if id == ec2DefaultDHCPOptionsID(reqCtx.AccountID, reqCtx.Region) {
		if err := p.state.Put(ctx, ec2Namespace, ec2DefaultDHCPOptionsTombstoneKey(reqCtx.AccountID, reqCtx.Region), []byte(`{"deleted":true}`)); err != nil {
			return nil, fmt.Errorf("ec2 deleteDhcpOptions tombstone: %w", err)
		}
	}
	return ec2ReturnTrue("DeleteDhcpOptions")
}

// associateDHCPOptions handles AssociateDhcpOptions: "Associates a set of DHCP options (that
// you've previously created) with the specified VPC, or associates no DHCP options with the
// VPC." DhcpOptionsId default records "default", which DescribeVpcs renders as the VPC's
// dhcpOptionsId. An unknown VPC is InvalidVpcID.NotFound and an unknown set
// InvalidDhcpOptionsID.NotFound.
func (p *EC2Plugin) associateDHCPOptions(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id, vpcID := req.Params["DhcpOptionsId"], req.Params["VpcId"]
	if id == "" {
		return nil, ec2MissingParameter("DhcpOptionsId")
	}
	ctx := context.Background()
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	data, err := p.state.Get(ctx, ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 associateDhcpOptions vpc: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VPCIDKind, "VpcId", vpcID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	if id != ec2NoDHCPOptions {
		sets, _, err := p.loadDHCPOptions(ctx, reqCtx)
		if err != nil {
			return nil, err
		}
		if reqErr := ec2RequireResource(ec2DHCPOptionsIDKind, id, ec2FindDHCPOptions(sets, id) != nil); reqErr != nil {
			return nil, reqErr
		}
	}
	var vpc EC2VPC
	if err := json.Unmarshal(data, &vpc); err != nil {
		return nil, fmt.Errorf("ec2 associateDhcpOptions unmarshal: %w", err)
	}
	vpc.DhcpOptionsID = id
	if err := p.putEC2Record(ctx, key, vpc); err != nil {
		return nil, fmt.Errorf("ec2 associateDhcpOptions: %w", err)
	}
	return ec2ReturnTrue("AssociateDhcpOptions")
}

// ec2DHCPOptionsFilterSpec is DescribeDhcpOptions' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeDhcpOptions.html. Every
// documented name is evaluated.
func ec2DHCPOptionsFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated:      []string{"dhcp-options-id", "key", "value", "owner-id", "tag-key"},
	}
}

// ec2DHCPOptionsMatchesFilter evaluates one DescribeDhcpOptions filter. key matches a set holding
// any option with a matching name, and value one holding any matching value, each independently
// — the page's Example 2 pairs them ("a domain-name option whose value includes the string
// example") over a set's options.
func ec2DHCPOptionsMatchesFilter(set EC2DHCPOptions, owner, name string, values []string) bool {
	if matched, isTag := ec2TagFilterMatch(set.Tags, name, values); isTag {
		return matched
	}
	switch name {
	case "dhcp-options-id":
		return ec2FilterAccepts(values, set.DHCPOptionsID)
	case "owner-id":
		return ec2FilterAccepts(values, owner)
	case "key":
		for _, c := range set.Configurations {
			if ec2FilterAccepts(values, c.Key) {
				return true
			}
		}
		return false
	case "value":
		for _, c := range set.Configurations {
			for _, v := range c.Values {
				if ec2FilterAccepts(values, v) {
					return true
				}
			}
		}
		return false
	default:
		return true
	}
}

// describeDHCPOptions handles DescribeDhcpOptions: the account's sets in the Region, the default
// one included, narrowed by DhcpOptionsId.N and Filter.N, ordered by ID and paginated on the
// page's published 5–1000 range.
func (p *EC2Plugin) describeDHCPOptions(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	setIDs := extractIndexedParams(req.Params, "DhcpOptionsId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "DhcpOptionsId", setIDs); awsErr != nil {
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
	ids := newEC2IDFilter(setIDs, ec2DescribeDHCPOptionsIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2DHCPOptionsFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	sets, _, err := p.loadDHCPOptions(context.Background(), reqCtx)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName     xml.Name             `xml:"DescribeDhcpOptionsResponse"`
		XMLNS       string               `xml:"xmlns,attr"`
		DHCPOptions []ec2DHCPOptionsItem `xml:"dhcpOptionsSet>item"`
		NextToken   string               `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: ec2XMLNS}
	for _, set := range sets {
		if !ids.match(set.DHCPOptionsID) {
			continue
		}
		selected := true
		for name, values := range filters {
			if !ec2DHCPOptionsMatchesFilter(set, reqCtx.AccountID, name, values) {
				selected = false
				break
			}
		}
		if selected {
			resp.DHCPOptions = append(resp.DHCPOptions, ec2DHCPOptionsXML(set, reqCtx.AccountID))
		}
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	resp.DHCPOptions, resp.NextToken = ec2Page(resp.DHCPOptions, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}
