package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// CreateNetworkInterface, AttachNetworkInterface, DetachNetworkInterface, DeleteNetworkInterface
// and ModifyNetworkInterfaceAttribute (#1522), the calls behind Terraform's
// aws_network_interface and aws_network_interface_attachment.
//
// Attachments complete within the request: AttachNetworkInterface leaves the interface in-use
// with its attachment attached, and DetachNetworkInterface leaves it available. AWS passes
// through attaching and detaching; substrate's instances are running by the time RunInstances
// answers, and its attachments are settled by the time these answer, for the same reason.

// ec2NetworkInterfaceInUseError is InvalidNetworkInterface.InUse, the client-error table's "The
// specified interface is currently in use and cannot be deleted or attached to another instance."
// The message is that gloss's first clause with the ID; no page publishes one. The table notes
// that EC2 may answer InvalidParameterValue for the same condition.
func ec2NetworkInterfaceInUseError(eniID string) *AWSError {
	return &AWSError{
		Code:       "InvalidNetworkInterface.InUse",
		Message:    "The interface " + eniID + " is currently in use and cannot be deleted or attached to another instance.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2DeviceIndexInUseError refuses an attachment at a device index the instance already uses,
// with EC2's observed answer, InvalidParameterValue "Instance 'i-…' already has an interface
// attached at device index '0'." (hashicorp/terraform#2998; HashiCorp's "How to import an AWS EC2
// Instance with existing Elastic Network Interfaces").
func ec2DeviceIndexInUseError(instanceID string, deviceIndex int) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: "Instance '" + instanceID + "' already has an interface attached at device index '" +
			strconv.Itoa(deviceIndex) + "'.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2PrimaryDetachError refuses detaching an instance's primary interface. The client-error
// table's OperationNotPermitted names the case — "trying to detach the primary network interface
// (eth0) from an instance" — and the message is EC2's observed one (getmoto/moto#7678).
func ec2PrimaryDetachError() *AWSError {
	return &AWSError{
		Code:       "OperationNotPermitted",
		Message:    "The network interface at device index 0 and networkCard index 0 cannot be detached.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2SecondPrimaryAddressError refuses a CreateNetworkInterface naming two primary addresses. The
// page states the rule: "If you specify an IP address, you cannot indicate any IP addresses
// specified in privateIpAddresses as primary (only one IP address can be designated as
// primary)." It publishes no code or message, so InvalidParameterValue and the wording are
// substrate's own.
func ec2SecondPrimaryAddressError(addr string) *AWSError {
	return ec2InvalidParameterValue("PrivateIpAddresses", addr, "Only one IP address can be designated as primary.")
}

// ec2ParsePrivateAddresses reads CreateNetworkInterface's addressing: the primary address, from
// PrivateIpAddress or the PrivateIpAddresses.N entry marked Primary (the first entry when none
// is), the secondary addresses the other entries name, and SecondaryPrivateIpAddressCount.
//
// An entry repeating PrivateIpAddress names the primary again and is dropped, whether or not it
// is marked Primary; an entry marked Primary that names a different address, or a second entry
// marked Primary, is the refusal [ec2SecondPrimaryAddressError] gives.
func ec2ParsePrivateAddresses(params map[string]string) (primary string, secondary []string, count int, awsErr *AWSError) {
	primary = params["PrivateIpAddress"]
	var listed []string
	primaryAt := -1
	for n := 1; ; n++ {
		addr := params[fmt.Sprintf("PrivateIpAddresses.%d.PrivateIpAddress", n)]
		if addr == "" {
			break
		}
		marked := strings.EqualFold(params[fmt.Sprintf("PrivateIpAddresses.%d.Primary", n)], "true")
		if addr == primary {
			continue
		}
		if marked && (primary != "" || primaryAt >= 0) {
			return "", nil, 0, ec2SecondPrimaryAddressError(addr)
		}
		if marked {
			primaryAt = len(listed)
		}
		listed = append(listed, addr)
	}
	if primary == "" && len(listed) > 0 {
		if primaryAt < 0 {
			primaryAt = 0
		}
		primary = listed[primaryAt]
		listed = append(listed[:primaryAt:primaryAt], listed[primaryAt+1:]...)
	}
	secondary = listed
	if v := params["SecondaryPrivateIpAddressCount"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return "", nil, 0, ec2InvalidParameterValue("SecondaryPrivateIpAddressCount", v, "It must be a non-negative integer.")
		}
		count = n
	}
	return primary, secondary, count, nil
}

// createNetworkInterface handles CreateNetworkInterface: an available interface in the subnet,
// with an address from the subnet's range, the groups the request names or the VPC's default
// group, the description, and TagSpecification.N tags scoped to network-interface.
func (p *EC2Plugin) createNetworkInterface(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ctx := context.Background()
	subnetID := req.Params["SubnetId"]
	interfaceType := req.Params["InterfaceType"]
	switch interfaceType {
	case "":
		interfaceType = "interface"
	case "interface", "efa", "efa-only", "trunk":
	default:
		return nil, ec2InvalidParameterValue("InterfaceType", interfaceType, "Valid values: interface, efa, efa-only, trunk.")
	}
	primaryAddr, secondaryAddrs, secondaryCount, awsErr := ec2ParsePrivateAddresses(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	tags := ec2LaunchTagsForResource(req.Params, "network-interface")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}

	// Held from the subnet and group checks through the write, as a launch holds it, so a
	// concurrent DeleteSubnet or DeleteSecurityGroup cannot remove what this has just checked.
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	subnetData, err := p.state.Get(ctx, ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+subnetID)
	if err != nil {
		return nil, fmt.Errorf("ec2 createNetworkInterface get subnet: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2SubnetIDKind, "SubnetId", subnetID, subnetData != nil); reqErr != nil {
		return nil, reqErr
	}
	var subnet EC2Subnet
	if err := json.Unmarshal(subnetData, &subnet); err != nil {
		return nil, fmt.Errorf("ec2 createNetworkInterface unmarshal subnet: %w", err)
	}
	groups := indexedParams(req.Params, "SecurityGroupId.%d")
	if len(groups) == 0 {
		if sg := ec2LookupDefaultSecurityGroup(ctx, p.state, reqCtx, subnet.VPCID); sg != "" {
			groups = []string{sg}
		}
	}
	if err := p.ec2CheckSecurityGroups(reqCtx, groups, subnet.VPCID); err != nil {
		return nil, err
	}

	txn := p.beginAddrTxn(reqCtx)
	defer txn.close()
	rec := EC2NetworkInterfaceRecord{
		SubnetID:         subnet.SubnetID,
		VPCID:            subnet.VPCID,
		AvailabilityZone: subnet.AvailabilityZone,
		Description:      req.Params["Description"],
		InterfaceType:    interfaceType,
		SecurityGroupIDs: groups,
		Status:           "available",
		Tags:             tags,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}
	// An efa-only interface "do[es] not support IP addresses", so it is given none.
	if interfaceType != "efa-only" {
		addr, err := txn.allocate(subnet, primaryAddr, "")
		if err != nil {
			return nil, err
		}
		rec.PrivateIPAddress = addr
		rec.PrivateDNSName = ec2PrivateDNSName(addr, reqCtx.Region)
		for _, want := range secondaryAddrs {
			addr, err := txn.allocate(subnet, want, "")
			if err != nil {
				return nil, err
			}
			rec.SecondaryPrivateIPAddresses = append(rec.SecondaryPrivateIPAddresses, addr)
		}
		for range secondaryCount {
			addr, err := txn.allocate(subnet, "", "")
			if err != nil {
				return nil, err
			}
			rec.SecondaryPrivateIPAddresses = append(rec.SecondaryPrivateIPAddresses, addr)
		}
	}
	rec.NetworkInterfaceID = generateENIID(reqCtx.IDs)
	rec.EverTagged = len(tags) > 0
	for _, addr := range rec.addresses() {
		txn.setHolder(subnet.SubnetID, addr, rec.NetworkInterfaceID)
	}
	if err := p.putNetworkInterface(ctx, rec); err != nil {
		return nil, fmt.Errorf("ec2 createNetworkInterface: %w", err)
	}
	if err := txn.commit(); err != nil {
		return nil, err
	}
	type response struct {
		XMLName          xml.Name                `xml:"CreateNetworkInterfaceResponse"`
		XMLNS            string                  `xml:"xmlns,attr"`
		NetworkInterface ec2NetworkInterfaceItem `xml:"networkInterface"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:            "http://ec2.amazonaws.com/doc/2016-11-15/",
		NetworkInterface: p.ec2NetworkInterfaceItemFor(reqCtx, rec, false),
	})
}
