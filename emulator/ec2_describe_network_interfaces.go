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

// DescribeNetworkInterfaces (#1519).
//
// The Terraform AWS provider lists the network interfaces in a subnet, or using a security
// group, before it deletes either, so that it can detach and delete any a Lambda function or a
// load balancer left behind. The call was unrouted, so `terraform destroy` of any configuration
// holding a subnet or a security group failed.
//
// The interfaces reported are the ones substrate holds: those RunInstances recorded on an
// instance from its NetworkInterface.N specifications ([EC2Plugin.ec2AttachInterfaces]), for every
// instance not yet terminated. Substrate mints no interface for an instance launched without one,
// for a Lambda function or for a load balancer, so those have none to report. An ID the caller
// names that no instance holds is the InvalidNetworkInterfaceID.NotFound the EC2 error reference
// publishes, through [ec2NetworkInterfaceIDKind]. MaxResults and NextToken follow every paginated
// describe (#917): the published range is 5 to 1000.

// ec2NetworkInterfaceIDKind is the eni- family, with both codes from the EC2 error reference.
var ec2NetworkInterfaceIDKind = ec2IDKind{
	Prefix: "eni-", NotFound: "InvalidNetworkInterfaceID.NotFound",
	Malformed: "InvalidNetworkInterfaceID.Malformed", Noun: "network interface",
}

// ec2NetworkInterfaceFilterSpec is DescribeNetworkInterfaces' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeNetworkInterfaces.html.
// The evaluated names are the members an instance's recorded interface carries; the rest name
// state substrate does not keep (addresses, associations, attachment times) and are inert.
func ec2NetworkInterfaceFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated: []string{
			"attachment.device-index", "attachment.instance-id", "attachment.instance-owner-id",
			"attachment.status", "availability-zone", "description", "group-id",
			"interface-type", "network-interface-id", "owner-id", "private-dns-name",
			"private-ip-address", "status", "subnet-id", "tag-key", "vpc-id",
		},
		accepted: []string{
			"addresses.association.owner-id", "addresses.association.public-ip", "addresses.primary",
			"addresses.private-ip-address", "association.allocation-id", "association.association-id",
			"association.ip-owner-id", "association.public-dns-name", "association.public-ip",
			"attachment.attach-time", "attachment.attachment-id", "attachment.delete-on-termination",
			"group-name", "ipv6-addresses.ipv6-address", "mac-address", "requester-id",
			"requester-managed", "source-dest-check",
		},
	}
}

// ec2NetworkInterfaceRecord is one interface and the instance it is attached to.
type ec2NetworkInterfaceRecord struct {
	ifc  EC2NetworkInterface
	inst EC2Instance
	item ec2NetworkInterfaceItem
}

// matches reports whether the record satisfies one evaluated filter. Interfaces carry no tags
// of their own, so a tag filter selects nothing.
func (r ec2NetworkInterfaceRecord) matches(name string, values []string) bool {
	switch name {
	case "attachment.device-index":
		return ec2FilterAccepts(values, strconv.Itoa(r.ifc.DeviceIndex))
	case "attachment.instance-id":
		return ec2FilterAccepts(values, r.inst.InstanceID)
	case "attachment.instance-owner-id", "owner-id":
		return ec2FilterAccepts(values, r.inst.AccountID)
	case "attachment.status":
		return ec2FilterAccepts(values, "attached")
	case "availability-zone":
		return ec2FilterAccepts(values, r.inst.AvailabilityZone)
	case "description":
		return ec2FilterAccepts(values, r.ifc.Description)
	case "group-id":
		for _, g := range r.ifc.SecurityGroupIDs {
			if ec2FilterAccepts(values, g) {
				return true
			}
		}
		return false
	case "interface-type":
		return ec2FilterAccepts(values, r.ifc.InterfaceType)
	case "network-interface-id":
		return ec2FilterAccepts(values, r.ifc.NetworkInterfaceID)
	case "private-dns-name":
		return ec2FilterAccepts(values, r.ifc.PrivateDNSName)
	case "private-ip-address":
		return ec2FilterAccepts(values, r.ifc.PrivateIPAddress)
	case "status":
		return ec2FilterAccepts(values, "in-use")
	case "subnet-id":
		return ec2FilterAccepts(values, r.ifc.SubnetID)
	case "vpc-id":
		return ec2FilterAccepts(values, r.inst.VPCID)
	case "tag-key":
		return false
	}
	if strings.HasPrefix(name, "tag:") {
		return false
	}
	// An accepted, inert name selects every interface.
	return true
}

// describeNetworkInterfaces reports the interfaces the account's live instances hold in the
// region, narrowed by NetworkInterfaceId.N and Filter.N, one page at a time.
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

	keys, err := p.state.List(context.Background(), ec2Namespace,
		"instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeNetworkInterfaces list instances: %w", err)
	}
	var records []ec2NetworkInterfaceRecord
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil || inst.State.Name == ec2StateTerminated {
			continue
		}
		// The element is the one RunInstances and DescribeInstances render for the same
		// interface, with the attachment's instance named, which only this response carries.
		items := p.ec2NetworkInterfaceItems(reqCtx, inst)
		for i, ifc := range inst.NetworkInterfaces {
			item := items[i]
			item.Attachment.InstanceID = inst.InstanceID
			item.Attachment.InstanceOwnerID = inst.AccountID
			rec := ec2NetworkInterfaceRecord{ifc: ifc, inst: inst, item: item}
			if !ids.match(ifc.NetworkInterfaceID) {
				continue
			}
			selected := true
			for name, values := range filters {
				if !rec.matches(name, values) {
					selected = false
					break
				}
			}
			if selected {
				records = append(records, rec)
			}
		}
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	// Ordered by ID, so a page boundary names the same interface on every call.
	sort.Slice(records, func(i, j int) bool {
		return records[i].ifc.NetworkInterfaceID < records[j].ifc.NetworkInterfaceID
	})

	type response struct {
		XMLName           xml.Name                  `xml:"DescribeNetworkInterfacesResponse"`
		XMLNS             string                    `xml:"xmlns,attr"`
		NetworkInterfaces []ec2NetworkInterfaceItem `xml:"networkInterfaceSet>item"`
		NextToken         string                    `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, r := range records {
		resp.NetworkInterfaces = append(resp.NetworkInterfaces, r.item)
	}
	resp.NetworkInterfaces, resp.NextToken = ec2Page(resp.NetworkInterfaces, offset, maxResults)
	return ec2XMLResponse(http.StatusOK, resp)
}
