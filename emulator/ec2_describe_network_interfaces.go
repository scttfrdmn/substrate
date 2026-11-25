package emulator

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// DescribeNetworkInterfaces (#1519, #1522).
//
// The Terraform AWS provider lists the network interfaces in a subnet, or using a security
// group, before it deletes either, so that it can detach and delete any a Lambda function or a
// load balancer left behind; aws_network_interface and aws_network_interface_attachment read
// their state through it.
//
// The interfaces reported are every stored one (ec2_eni.go), available or attached, and, for an
// instance written before interfaces were stored, the ones [ec2LegacyInterfaces] synthesizes for
// it. Substrate mints no interface for a Lambda function or a load balancer, so those have none
// to report. An ID the caller names that nothing holds is the InvalidNetworkInterfaceID.NotFound
// the EC2 error reference publishes, through [ec2NetworkInterfaceIDKind]. MaxResults and
// NextToken follow every paginated describe (#917): the published range is 5 to 1000.

// ec2NetworkInterfaceIDKind is the eni- family, with both codes from the EC2 error reference.
var ec2NetworkInterfaceIDKind = ec2IDKind{
	Prefix: "eni-", NotFound: "InvalidNetworkInterfaceID.NotFound",
	Malformed: "InvalidNetworkInterfaceID.Malformed", Noun: "network interface",
}

// ec2NetworkInterfaceFilterSpec is DescribeNetworkInterfaces' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeNetworkInterfaces.html.
// The evaluated names are the members a stored interface carries; the rest name state substrate
// does not keep (associations, IPv6 addresses, MAC addresses, requesters) and are inert.
func ec2NetworkInterfaceFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"addresses.primary", "addresses.private-ip-address",
			"attachment.attach-time", "attachment.attachment-id", "attachment.delete-on-termination",
			"attachment.device-index", "attachment.instance-id", "attachment.instance-owner-id",
			"attachment.status", "availability-zone", "description", "group-id", "group-name",
			"interface-type", "network-interface-id", "owner-id", "private-dns-name",
			"private-ip-address", "requester-managed", "source-dest-check", "status", "subnet-id",
			"tag-key", "vpc-id",
		},
		accepted: []string{
			"addresses.association.owner-id", "addresses.association.public-ip",
			"association.allocation-id", "association.association-id",
			"association.ip-owner-id", "association.public-dns-name", "association.public-ip",
			"ipv6-addresses.ipv6-address", "mac-address", "requester-id",
		},
	}
}

// ec2AnyAccepted reports whether the filter accepts any of candidates.
func ec2AnyAccepted(values, candidates []string) bool {
	for _, c := range candidates {
		if ec2FilterAccepts(values, c) {
			return true
		}
	}
	return false
}

// ec2NetworkInterfaceAttachmentValues are the attachment members the attachment.* filters
// compare, keyed by filter name. Every attachment.* name the filter set accepts is evaluated.
func ec2NetworkInterfaceAttachmentValues(rec EC2NetworkInterfaceRecord) map[string]string {
	a := rec.Attachment
	return map[string]string{
		"attachment.attach-time":           a.AttachTime,
		"attachment.attachment-id":         a.AttachmentID,
		"attachment.delete-on-termination": strconv.FormatBool(a.DeleteOnTermination),
		"attachment.device-index":          strconv.Itoa(a.DeviceIndex),
		"attachment.instance-id":           a.InstanceID,
		"attachment.instance-owner-id":     rec.AccountID,
		"attachment.status":                ec2AttachmentStatus(a),
	}
}

// ec2NetworkInterfaceMatches reports whether rec satisfies one evaluated filter. groupNames are
// the names of the interface's groups, for group-name.
func ec2NetworkInterfaceMatches(rec EC2NetworkInterfaceRecord, groupNames []string, name string, values []string) bool {
	if strings.HasPrefix(name, "attachment.") {
		if rec.Attachment == nil {
			return false
		}
		return ec2FilterAccepts(values, ec2NetworkInterfaceAttachmentValues(rec)[name])
	}
	if key, ok := strings.CutPrefix(name, "tag:"); ok {
		for _, t := range rec.Tags {
			if t.Key == key && ec2FilterAccepts(values, t.Value) {
				return true
			}
		}
		return false
	}
	interfaceType := rec.InterfaceType
	if interfaceType == "" {
		interfaceType = "interface"
	}
	var primaryFlags []string
	if rec.PrivateIPAddress != "" {
		primaryFlags = append(primaryFlags, "true")
	}
	if len(rec.SecondaryPrivateIPAddresses) > 0 {
		primaryFlags = append(primaryFlags, "false")
	}
	var tagKeys []string
	for _, t := range rec.Tags {
		tagKeys = append(tagKeys, t.Key)
	}
	single := map[string]string{
		"availability-zone":    rec.AvailabilityZone,
		"description":          rec.Description,
		"interface-type":       interfaceType,
		"network-interface-id": rec.NetworkInterfaceID,
		"owner-id":             rec.AccountID,
		"private-dns-name":     rec.PrivateDNSName,
		"private-ip-address":   rec.PrivateIPAddress,
		"requester-managed":    "false",
		"source-dest-check":    strconv.FormatBool(!rec.SourceDestCheckDisabled),
		"status":               rec.Status,
		"subnet-id":            rec.SubnetID,
		"vpc-id":               rec.VPCID,
	}
	if v, ok := single[name]; ok {
		return ec2FilterAccepts(values, v)
	}
	multi := map[string][]string{
		"addresses.private-ip-address": rec.addresses(),
		"addresses.primary":            primaryFlags,
		"group-id":                     rec.SecurityGroupIDs,
		"group-name":                   groupNames,
		"tag-key":                      tagKeys,
	}
	if v, ok := multi[name]; ok {
		return ec2AnyAccepted(values, v)
	}
	// An accepted, inert name selects every interface.
	return true
}

// describeNetworkInterfaces reports the account's interfaces in the region, narrowed by
// NetworkInterfaceId.N and Filter.N, one page at a time.
func (p *EC2Plugin) describeNetworkInterfaces(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ifcIDs := extractIndexedParams(req.Params, "NetworkInterfaceId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "NetworkInterfaceId", ifcIDs); awsErr != nil {
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
	ids := newEC2IDFilter(ifcIDs, ec2NetworkInterfaceIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2NetworkInterfaceFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)

	// The older instances' state — markers first — is read before the records, the reverse of
	// the order storing them writes in, and a stored record wins over a synthesized one with its
	// ID, so a describe racing a store reports each interface once (see ec2_eni_legacy.go).
	ctx := context.Background()
	legacy, err := p.ec2LegacyStateFor(ctx, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
	}
	records, err := p.ec2NetworkInterfaces(ctx, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
	}
	var synthesized []EC2NetworkInterfaceRecord
	if err := ec2ScanRecords(ctx, p.state, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/", func(_ string, inst EC2Instance) {
		synthesized = append(synthesized, ec2LegacyUnstored(inst, legacy)...)
	}); err != nil {
		return nil, fmt.Errorf("ec2 describeNetworkInterfaces instances: %w", err)
	}
	records = ec2MergeSynthesized(records, synthesized)
	// Ordered by ID, so a page boundary names the same interface on every call.
	sort.Slice(records, func(i, j int) bool {
		return records[i].NetworkInterfaceID < records[j].NetworkInterfaceID
	})

	type response struct {
		XMLName           xml.Name                  `xml:"DescribeNetworkInterfacesResponse"`
		XMLNS             string                    `xml:"xmlns,attr"`
		NetworkInterfaces []ec2NetworkInterfaceItem `xml:"networkInterfaceSet>item"`
		NextToken         string                    `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, rec := range records {
		if !ids.match(rec.NetworkInterfaceID) {
			continue
		}
		item := p.ec2NetworkInterfaceItemFor(reqCtx, rec, false)
		var groupNames []string
		for _, g := range item.Groups {
			groupNames = append(groupNames, g.GroupName)
		}
		selected := true
		for name, values := range filters {
			if !ec2NetworkInterfaceMatches(rec, groupNames, name, values) {
				selected = false
				break
			}
		}
		if selected {
			resp.NetworkInterfaces = append(resp.NetworkInterfaces, item)
		}
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	resp.NetworkInterfaces, resp.NextToken = ec2Page(resp.NetworkInterfaces, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}
